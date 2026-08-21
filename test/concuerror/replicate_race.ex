defmodule Malachi.Concuerror.ReplicateRace do
  @moduledoc """
  The Concuerror spike scenario (see `scripts/concuerror.sh`): the race between a follower's ack
  and the no-quorum timer of the SAME parked batch, inside the real
  `Malachi.Cluster.ReplicationServer`.

  Invariant under test: **a parked batch is answered exactly once**, never twice (an ack plus a
  no-quorum) and never zero times (both paths deciding the other owns it). The two paths are
  `resolve_batches/2` (ack: cancels the timer and replies `{:ok, last}`) and
  `handle_info({:replicate_timeout, ...})` (timer: replies `{:error, :no_quorum}`), and the window
  between "the timer fired and its message is in the mailbox" and `Process.cancel_timer/1` is
  exactly the interleaving a single-process property test cannot reach.

  The scenario replies through the `{:notify, pid, tag}` target (the non-blocking produce path), so
  this process collects the replies as plain messages and asserts on their multiplicity.

  **Status: blocked by the tool, kept as the reproducible record of the spike.** Concuerror builds
  and runs on our OTP and drives Elixir OTP code (a disk-free GenServer with the same park, ack and
  timer shape explores fine), but this server is disk-backed by design: its init creates the data
  directory and log recovery scans it, and Concuerror's `file_server_2` emulation does not
  implement `read_file_info`, so exploration never starts. This file is NOT compiled into the app
  or the test suite (`elixirc_paths` excludes it); `scripts/concuerror.sh` compiles it on demand.
  See docs/ARCHITECTURE.md for the verdict and what would unblock it.
  """

  alias Malachi.Cluster.ReplicationServer
  alias Malachi.Log.Record

  @segment {{"t", 0}, 0}

  @doc """
  Entry point Concuerror drives (`-m Elixir.Malachi.Concuerror.ReplicateRace -t replicate_race`).

  A primary with a replica set of two (itself plus one follower it must hear from) parks a batch,
  and the follower's ack races the follow timeout. Exits with `{:replied_twice, ...}` or
  `{:never_replied, ...}` if the invariant breaks, which is what Concuerror reports as an error.
  """
  def replicate_race do
    directory = scratch_directory()
    {:ok, primary} = ReplicationServer.start_link(directory: Path.join(directory, "p"), follow_timeout: 1)
    {:ok, follower} = ReplicationServer.start_link(directory: Path.join(directory, "f"))

    tag = :batch
    records = [Record.new("v", key: "k")]
    ReplicationServer.replicate_async(primary, @segment, [primary, follower], 0, records, self(), tag)

    # Blocking receive on purpose: if no path ever answers the batch, this process is stuck and
    # Concuerror reports the deadlock, which IS the "never replied" half of the invariant.
    first =
      receive do
        {:replicate_result, ^tag, result} -> result
      end

    # Stopping the primary first makes the second half sound: once its loop is gone no further
    # reply can be produced, so an empty mailbox here means exactly one reply, not merely "not yet".
    GenServer.stop(primary)
    GenServer.stop(follower)

    receive do
      {:replicate_result, ^tag, second} -> exit({:replied_twice, first, second})
    after
      0 -> :ok
    end

    File.rm_rf!(directory)
    :ok
  end

  defp scratch_directory do
    Path.join(System.tmp_dir!(), "malachi_concuerror_#{:erlang.unique_integer([:positive])}")
  end
end
