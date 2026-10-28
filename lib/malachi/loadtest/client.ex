defmodule Malachi.Loadtest.Client do
  @moduledoc """
  A client of the stream and group keys for short checks against a running node, small enough to drive
  from `bin/malachi rpc` against a release.

  It works as any client does, over the wire. It reads the routes from `cluster_state` and `topic_routes`
  (`Malachi.Loadtest.Router`) and writes and reads each range at the broker that leads it
  (`Malachi.Loadtest.Producer`, `Malachi.Loadtest.Fetcher`). The group keys go through the node it
  connected to, which routes them to the group's coordinator (`Malachi.Consumer.CoordinatorRouter`).
  Nothing here is called on a hot path: each call opens what it needs and closes it.
  """

  alias Malachi.Loadtest.Conn
  alias Malachi.Loadtest.Fetcher
  alias Malachi.Loadtest.Producer
  alias Malachi.Loadtest.Router
  alias Malachi.Log.Record
  alias Malachi.Wire

  @enforce_keys [:router, :conn, :conn_opts]
  defstruct [:router, :conn, :conn_opts]

  @type t :: %__MODULE__{router: Router.t(), conn: Conn.t(), conn_opts: keyword()}

  # How long a read waits out an answer that is no page (routes held back, a connection dropped), between
  # tries.
  @retry_ms 100
  # Refusals that are routing, not reading, as the chaos checker counts them: no broker took the connection,
  # or the routes could not be read yet (a node still recovering): read again and ask again.
  @routing_errors ["connect_failed", "metadata_unavailable"]

  @doc """
  Connects to the node at `opts[:host]` and `opts[:port]`, authenticated as `opts[:user]` with
  `opts[:pass]`, and reads the cluster state there. `{:error, reason}` when the node takes no connection,
  refuses the credentials, or answers no cluster state.
  """
  @spec connect(keyword()) :: {:ok, t()} | {:error, term()}
  def connect(opts) do
    conn_opts = Keyword.take(opts, [:host, :port, :user, :pass])

    with {:ok, conn} <- Conn.connect(conn_opts) do
      # a connection this call could not finish with is closed here, never left to its caller's exit
      case Conn.authenticate(conn, conn_opts) do
        {:ok, conn} -> bootstrap(conn, opts, conn_opts)
        {:error, _reason} = error -> close_with(conn, error)
      end
    end
  end

  defp bootstrap(conn, opts, conn_opts) do
    case Router.bootstrap(Router.new({Keyword.fetch!(opts, :host), Keyword.fetch!(opts, :port)}), conn) do
      {:ok, router, conn} -> {:ok, %__MODULE__{router: router, conn: conn, conn_opts: conn_opts}}
      {:legacy, reason, conn} -> close_with(conn, {:error, reason})
      {:error, _reason} = error -> close_with(conn, error)
    end
  end

  @doc "Closes the connection `connect/1` opened."
  @spec close(t()) :: :ok
  def close(%__MODULE__{conn: conn}), do: Conn.close(conn)

  @doc "Creates `topic` with `keyspace_bits` bits of keyspace: `:ok`, or the refusal (`\"already_exists\"`)."
  @spec create_topic(t(), String.t(), pos_integer()) :: :ok | {:error, String.t()}
  def create_topic(%__MODULE__{} = client, topic, keyspace_bits) do
    client
    |> unary(Wire.create_topic_key(), Wire.encode_create_topic_req(topic, keyspace_bits))
    |> answer(fn _ -> :ok end)
  end

  @doc """
  Appends `records` to `topic` and waits up to `timeout_ms` for their acknowledgements: `{:ok, count}` when
  every one was acknowledged, else `{:error, %{acked: count, failed: %{reason => records}, unanswered: n}}`.
  """
  @spec produce(t(), String.t(), [Record.t()], non_neg_integer()) :: {:ok, non_neg_integer()} | {:error, map()}
  def produce(%__MODULE__{} = client, topic, records, timeout_ms) do
    producer = Producer.new(client.router, client.conn_opts, max_inflight: 16, label: "malachi-client")
    entries = records |> Enum.with_index() |> Enum.map(fn {record, i} -> {i, record} end)
    {producer, events} = Producer.produce(producer, topic, entries, mono_ms())
    {producer, later, _idle_or_timeout} = Producer.await(producer, timeout_ms)
    Producer.close(producer)

    acked = for {:acked, done, _latency} <- events ++ later, reduce: 0, do: (count -> count + length(done))
    failed = Enum.frequencies(for {:failed, done, reason} <- events ++ later, _entry <- done, do: reason)
    unanswered = length(records) - acked - Enum.sum(Map.values(failed))

    if acked == length(records),
      do: {:ok, acked},
      else: {:error, %{acked: acked, failed: failed, unanswered: unanswered}}
  end

  @doc """
  Reads `topic` to its end within `timeout_ms`: every active range, or only `opts[:ranges]` (a group
  member's assignment, as `join/4` hands it), from `opts[:start_from]` (`:earliest`, the default, or
  `{:committed, group}`, that group's checkpoint) until each one answered a page with nothing new.

  `{:ok, %{values: values, positions: positions}}`, where `values` are in the order each range holds them
  and `positions` holds, by range read, where its next page starts: what a member commits once it has read
  them. A member of a group of more than one reads with `ranges:`, since a commit naming a range the group
  did not hand it is refused (`not_assigned`); one handed no range reads nothing; one whose ranges a
  split retired is answered `{:error, :stale_assignment}`, before or during the read, and joins again for
  the ranges made of them. Routing answers (routes moved or held back, a dropped connection, no broker that takes one, routes
  not readable yet) are waited out until `opts[:timeout_ms]` (10 s unless given). `{:error, reason}` on any
  other refusal, or `{:error, :timeout}`.
  """
  @spec read(t(), String.t(), keyword()) :: {:ok, %{values: [binary()], positions: map()}} | {:error, term()}
  def read(%__MODULE__{} = client, topic, opts \\ []) do
    fetcher =
      Fetcher.new(client.router, client.conn_opts,
        at_tail: :wait,
        max: 1_000,
        start_from: Keyword.get(opts, :start_from, :earliest),
        ranges: Keyword.get(opts, :ranges)
      )

    deadline = mono_ms() + Keyword.get(opts, :timeout_ms, 10_000)
    acc = %{values: [], positions: %{}, quiet: %{}, ranges: Keyword.get(opts, :ranges)}
    {fetcher, result} = drain(fetcher, topic, deadline, acc)
    Fetcher.close(fetcher)
    result
  end

  # A page at a time, every range in turn, until every active range answered one with nothing new.
  defp drain(fetcher, topic, deadline, acc) do
    if mono_ms() >= deadline do
      {fetcher, {:error, :timeout}}
    else
      case Fetcher.fetch_page(fetcher, topic, mono_ms()) do
        {fetcher, {:page, page}} -> drained(fetcher, topic, deadline, page_into(acc, page), page)
        {fetcher, :unassigned} -> {fetcher, unassigned(acc)}
        {fetcher, {:held, ms}} -> retry(fetcher, topic, deadline, acc, ms)
        {fetcher, routing} when routing in [:redirected, :dropped] -> retry(fetcher, topic, deadline, acc, @retry_ms)
        {fetcher, {:error, reason}} when reason in @routing_errors -> retry(fetcher, topic, deadline, acc, @retry_ms)
        {fetcher, {:error, _reason} = error} -> {fetcher, error}
      end
    end
  end

  # None of the ranges asked for is active: a member handed none reads nothing; one handed ranges a split
  # has since retired has an assignment the group moved on from, and is told so rather than answered an
  # empty read while their records are in the ranges made of them.
  defp unassigned(%{ranges: []}), do: {:ok, %{values: [], positions: %{}}}
  defp unassigned(_acc), do: {:error, :stale_assignment}

  # Read through once every range it reads (the active ones, or those of `ranges:` among them) answered a
  # page with nothing new since its last one that brought some.
  defp drained(fetcher, topic, deadline, acc, page) do
    reading = if acc.ranges == nil, do: page.active, else: Enum.filter(page.active, &(&1 in acc.ranges))

    if Enum.all?(reading, &Map.has_key?(acc.quiet, &1)),
      do: {fetcher, {:ok, %{values: acc.values, positions: acc.positions}}},
      else: drain(fetcher, topic, deadline, acc)
  end

  defp page_into(acc, %{values: [], range: range, to: to}),
    do: %{acc | quiet: Map.put(acc.quiet, range, true), positions: Map.put(acc.positions, range, to)}

  defp page_into(acc, %{values: values, range: range, to: to}),
    do: %{
      acc
      | values: acc.values ++ values,
        quiet: Map.delete(acc.quiet, range),
        positions: Map.put(acc.positions, range, to)
    }

  defp retry(fetcher, topic, deadline, acc, ms) do
    Process.sleep(min(ms, max(0, deadline - mono_ms())))
    drain(fetcher, topic, deadline, acc)
  end

  @doc """
  Joins `group` for `topic` as `member` (`join_group`): the member's generation, session and ranges (as
  their sequence numbers), or the refusal.
  """
  @spec join(t(), String.t(), String.t(), String.t()) :: {:ok, map()} | {:error, String.t()}
  def join(%__MODULE__{} = client, topic, group, member) do
    req = Wire.encode_join_group_req(%{topic: topic, group: group, member: member})
    client |> unary(Wire.join_group_key(), req) |> answer(&{:ok, Wire.decode_assignment_resp(&1)})
  end

  @doc """
  Commits `positions` (`%{range => position}`, as `read/3` gives them) of `group` for `topic` as `member`,
  at `generation` (`commit_offsets`): `:ok`, or the refusal.
  """
  @spec commit(t(), String.t(), String.t(), String.t(), non_neg_integer(), map()) :: :ok | {:error, String.t()}
  def commit(%__MODULE__{} = client, topic, group, member, generation, positions) do
    positions = for {range, position} <- positions, do: %{range: range, position: position}
    req = %{topic: topic, group: group, member: member, generation: generation, positions: positions}
    client |> unary(Wire.commit_offsets_key(), Wire.encode_commit_offsets_req(req)) |> answer(fn _ -> :ok end)
  end

  @doc "Leaves `group` for `topic` as `member` (`leave_group`): `:ok`, or the refusal."
  @spec leave(t(), String.t(), String.t(), String.t()) :: :ok | {:error, String.t()}
  def leave(%__MODULE__{} = client, topic, group, member) do
    req = Wire.encode_leave_group_req(topic, group, member)
    client |> unary(Wire.leave_group_key(), req) |> answer(fn _ -> :ok end)
  end

  defp unary(client, api_key, payload) do
    case Conn.request(client.conn, api_key, api_key, payload) do
      {:ok, code, body, _conn} -> {code, body}
      {:error, reason} -> {:failed, reason}
    end
  end

  defp answer({0, body}, ok), do: ok.(body)
  defp answer({_error, body}, _ok) when is_binary(body), do: {:error, Wire.decode_error_reason(body)}
  defp answer({:failed, reason}, _ok), do: {:error, reason}

  defp close_with(conn, result) do
    Conn.close(conn)
    result
  end

  defp mono_ms, do: System.monotonic_time(:millisecond)
end
