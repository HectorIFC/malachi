defmodule Malachi.StreamWindow do
  @moduledoc """
  The window a producer stream may pipeline into (`Malachi.Wire` keys 26 and 27): how many appends, and
  how many inflated bytes, can be in flight at once. NorthGuard's broker defines it, at the handshake and
  again on every ack (meetup transcript, 559 to 560).

  At the handshake the broker grants what the client asked for, capped by its own limits
  (`MALACHI_STREAM_MAX_WINDOW_APPENDS`, `MALACHI_STREAM_MAX_WINDOW_BYTES`). On every ack it scales that
  grant by how loaded the range is: whole while the records it holds in flight for the range are under a
  soft limit, shrinking linearly to nothing at a hard one (`MALACHI_STREAM_INFLIGHT_SOFT`,
  `MALACHI_STREAM_INFLIGHT_HARD`, in records). With group commit on, the load is the node's parked records
  against the most it parks before shedding (`MALACHI_GROUP_COMMIT_MAX_INFLIGHT`), from half of it.

  A window of 0 holds the producer until a later ack grants more. A stream with nothing left in flight is
  always left room for one append of up to the bytes the handshake granted: no later ack would come to
  open it again, and any append the grant allows has to fit.

  Pure: numbers in, numbers out. The broker computes the load and the connection the window.
  """

  @typedoc "A window: appends and inflated bytes."
  @type t :: %{appends: non_neg_integer(), bytes: non_neg_integer()}

  @doc "What the handshake grants: the request, capped by the broker's limits."
  @spec grant(pos_integer(), pos_integer(), pos_integer(), pos_integer()) :: t()
  def grant(requested_appends, requested_bytes, max_appends, max_bytes),
    do: %{appends: min(requested_appends, max_appends), bytes: min(requested_bytes, max_bytes)}

  @doc """
  How much of a grant the load leaves: 1.0 at or under `soft`, 0.0 at or over `hard`, linear in between.
  """
  @spec scale(non_neg_integer(), non_neg_integer(), pos_integer()) :: float()
  def scale(load, soft, _hard) when load <= soft, do: 1.0
  def scale(load, _soft, hard) when load >= hard, do: 0.0
  def scale(load, soft, hard), do: (hard - load) / (hard - soft)

  @doc """
  The window an ack carries: `granted` scaled by `scale`, and, when the stream has nothing left in flight
  (`idle?`), at least one append and every byte of the grant.
  """
  @spec current(t(), float(), boolean()) :: t()
  def current(%{appends: appends, bytes: bytes}, scale, idle?) do
    window = %{appends: floor(appends * scale), bytes: floor(bytes * scale)}
    if idle?, do: %{appends: max(window.appends, 1), bytes: bytes}, else: window
  end

  @doc "The most appends a handshake grants (`MALACHI_STREAM_MAX_WINDOW_APPENDS`)."
  @spec max_appends() :: pos_integer()
  def max_appends, do: Application.get_env(:malachi, :stream_max_window_appends, 64)

  @doc "The most inflated bytes a handshake grants (`MALACHI_STREAM_MAX_WINDOW_BYTES`)."
  @spec max_bytes() :: pos_integer()
  def max_bytes, do: Application.get_env(:malachi, :stream_max_window_bytes, 16_777_216)

  @doc "The records a range may have in flight before its streams' windows shrink (`MALACHI_STREAM_INFLIGHT_SOFT`)."
  @spec inflight_soft() :: non_neg_integer()
  def inflight_soft, do: Application.get_env(:malachi, :stream_inflight_soft, 50_000)

  @doc "The records in flight at which a range's streams' windows reach 0 (`MALACHI_STREAM_INFLIGHT_HARD`)."
  @spec inflight_hard() :: pos_integer()
  def inflight_hard, do: Application.get_env(:malachi, :stream_inflight_hard, 200_000)
end
