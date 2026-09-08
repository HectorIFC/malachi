defmodule Malachi.Cluster.RingBoot do
  @moduledoc """
  The boot-time precedence rule between the **durable ring** and `MALACHI_LOG_VNODES`.

  A cluster that has been resharded and then fully restarted used to come back believing the
  environment, whose even geometry does not match a ring grown by splitting, while the metadata for
  the migrated topics lived in the vnodes the reshard created. The fix is a rule with no room to
  guess, and this module is that rule, kept pure and behind seams so every branch is testable without
  `ra`:

    * a durable ring exists: **it wins**, unconditionally. When the environment disagrees, the
      difference is logged loudly and the environment is ignored;
    * the store **affirms** it has never held a ring: this is a genuinely fresh cluster, so seed from
      the environment exactly as before;
    * the environment asks for no sharding and no ring is recorded: stay unsharded, today's
      single-Raft-group control plane;
    * the store cannot answer: **unknown**. Never seed from the environment here. Guessing is the
      failure this module exists to prevent, and a slow boot is not evidence of a fresh cluster.

  The three-valued read is why the ring lives in `ra` rather than in a per-node file: a missing file
  cannot tell "fresh cluster" from "I lost the record", and only the former may fall back to the
  environment.

  ## Why the environment can never win

  An operator who edits `MALACHI_LOG_VNODES` after a reshard is describing a cluster that no longer
  exists. Honouring that would orphan the migrated metadata, which is the whole bug. So the durable
  ring wins and the divergence is reported at `:warning` with both counts, pointing at
  `mix malachi.ring --show`.
  """

  require Logger

  alias Malachi.Cluster.HashRing
  alias Malachi.Cluster.RingTopology
  alias Malachi.I18n

  @default_timeout_ms 60_000
  @default_retry_interval_ms 500

  @typedoc "What a single read of the durable store can say."
  @type read :: {:ok, RingTopology.t()} | {:ok, :none} | {:error, term()}

  @typedoc """
  The boot decision. `{:durable, topology}` routes by the recorded ring; `{:seed, topology}` is a
  fresh cluster to seed from the environment; `:unsharded` keeps the single-Raft-group control plane;
  `{:error, reason}` means the store could not be read and the caller must refuse to serve.
  """
  @type decision ::
          {:durable, RingTopology.t()} | {:seed, RingTopology.t()} | :unsharded | {:error, term()}

  @doc """
  Reads the durable store, retrying while it is unreachable until `:timeout_ms` elapses.

  The retry exists for the ordinary shape of a full-cluster restart: the first node up cannot reach a
  quorum until a second one joins, and both are in boot at the time. Its Raft server votes as soon as
  distribution is up, well before the supervision tree finishes, so the wait is normally short.

  A definite answer (a recorded ring, or an affirmation that none exists) returns immediately; only
  `{:error, _}` is retried, and the **last** error is what comes back on timeout.

  Options: `:timeout_ms` (default `#{@default_timeout_ms}`), `:retry_interval_ms`
  (default `#{@default_retry_interval_ms}`), and the `:sleep` / `:elapsed_ms` seams tests use to keep
  the clock out of it.
  """
  @spec read_until((-> read()), keyword()) :: read()
  def read_until(read_topology, opts \\ []) do
    timeout_ms = Keyword.get(opts, :timeout_ms, @default_timeout_ms)
    interval_ms = Keyword.get(opts, :retry_interval_ms, @default_retry_interval_ms)
    sleep = Keyword.get(opts, :sleep, &Process.sleep/1)
    started = System.monotonic_time(:millisecond)
    elapsed = Keyword.get(opts, :elapsed_ms, fn -> System.monotonic_time(:millisecond) - started end)

    retry(read_topology, timeout_ms, interval_ms, sleep, elapsed)
  end

  defp retry(read_topology, timeout_ms, interval_ms, sleep, elapsed) do
    case read_topology.() do
      {:error, _reason} = error ->
        if elapsed.() >= timeout_ms do
          error
        else
          sleep.(interval_ms)
          retry(read_topology, timeout_ms, interval_ms, sleep, elapsed)
        end

      answered ->
        answered
    end
  end

  @doc """
  Turns a `read` and the environment's topology (`nil` when `MALACHI_LOG_VNODES` asks for no sharding)
  into the boot `decision`, logging the divergence between the two when there is one.

  Logging lives here rather than at the call site so the loud line cannot be forgotten by a future
  caller: the decision and the announcement are the same step.
  """
  @spec resolve(read(), RingTopology.t() | nil) :: decision()
  def resolve({:ok, %RingTopology{} = durable}, env_topology) do
    warn_if_env_diverges(durable, env_topology)
    {:durable, durable}
  end

  def resolve({:ok, :none}, %RingTopology{} = env_topology) do
    Logger.info(I18n.t(:ring_seeded, env: vnode_count(env_topology)))
    {:seed, env_topology}
  end

  # No ring recorded and the environment asks for none: a clustered but unsharded control plane, the
  # historical single-Raft-group shape. Nothing to seed and nothing to route.
  def resolve({:ok, :none}, nil), do: :unsharded

  def resolve({:error, reason}, _env_topology), do: {:error, reason}

  @doc """
  The topology this node ends up believing after a `{:seed, _}` decision was written through.

  `write` reports `:ok` when this node planted the seed, or `{:error, {:exists, current}}` when
  another node won the concurrent first-boot race, in which case `current` is what the cluster
  actually agreed on and this node adopts it. Any other error is returned for the caller to refuse on:
  a seed that did not land must not be served as though it had.
  """
  @spec confirm_seed(RingTopology.t(), (RingTopology.t() -> :ok | {:error, term()})) ::
          {:ok, RingTopology.t()} | {:error, term()}
  def confirm_seed(%RingTopology{} = seed, write) do
    case write.(seed) do
      :ok ->
        {:ok, seed}

      {:error, {:exists, %RingTopology{} = current}} ->
        Logger.info(I18n.t(:ring_seed_race_lost, version: current.version, durable: vnode_count(current)))
        {:ok, current}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  The message for a durable ring that could not be read within `timeout_ms`. Refusing to boot is the
  deliberate choice: a node that cannot see the ring cannot know which vnodes own which arcs, and
  serving on a guess is precisely the corruption being fixed.
  """
  @spec unreadable_message(term(), non_neg_integer()) :: String.t()
  def unreadable_message(reason, timeout_ms) do
    I18n.t(:ring_unreadable, reason: inspect(reason), timeout: timeout_ms)
  end

  @doc "The number of vnodes a topology's ring carries."
  @spec vnode_count(RingTopology.t()) :: non_neg_integer()
  def vnode_count(%RingTopology{ring: nil}), do: 0
  def vnode_count(%RingTopology{ring: ring}), do: HashRing.size(ring)

  # The environment is only worth mentioning when it describes a different cluster than the record
  # does. A matching count is the normal case and stays quiet; `nil` (sharding switched off in the
  # environment after a reshard) is the most dangerous divergence, so it is reported as 0.
  defp warn_if_env_diverges(durable, env_topology) do
    durable_count = vnode_count(durable)
    env_count = if env_topology, do: vnode_count(env_topology), else: 0

    if env_count != durable_count do
      Logger.warning(I18n.t(:ring_env_ignored, version: durable.version, durable: durable_count, env: env_count))
    end

    :ok
  end
end
