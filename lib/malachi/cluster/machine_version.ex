defmodule Malachi.Cluster.MachineVersion do
  @moduledoc """
  The one versioning rule shared by every control-plane `ra` state machine: metadata, lease, ring,
  users, lockouts, ACLs and cluster flags.

  ## Why a command needs a version

  A Raft group whose members run different code applies the same log with different `apply/3`
  functions. A command that a newer member understands and an older one skips leaves the replicas
  holding different states with no error anywhere. `ra` solves the transport half of this: every
  machine declares `version/0`, and under the `all` upgrade strategy (set explicitly in
  `config/config.exs`) a group only switches to version N once **every** member supports N. The
  switch is a `noop` entry in the log, applied as `{:machine_version, from, to}`, and from then on
  `meta.machine_version` in `apply/3` is N on every replica, including on a replay after a restart
  (the effective version rises at the same log index each time). A member whose code is below the
  effective version stops applying entries until it restarts on newer code: fail-stop, never
  divergence.

  This module is the other half: `apply/5` refuses a command whose tag was introduced **above** the
  effective version, and a tag it does not know at all, with the state unchanged and no effects.
  Because the decision reads only `meta.machine_version`, which is replicated, every member reaches
  the same refusal. Nothing node-local takes part: not `version/0`, not the pin, not membership.

  ## The rule for a new command

  A command introduced by a release goes into its module's `c:command_versions/0` table at
  `code_version() + 1`, and `@code_version` below rises in the same change. Giving an existing command
  another field is a new command by this rule: the table is keyed by shape, so the wider tuple is its
  own entry at the new version, and older members refuse it instead of raising on it or skipping it. With `all`, the command is
  then refused identically on every member until the last one runs and advertises the new version.
  Existing commands stay at the version they were introduced at forever: nothing takes a release
  cursor, so every command ever written is replayed on restart and must stay appliable.

  The seven machines share one version number. That keeps one pin meaningful for all of them, and
  bumping it for one machine only costs the others a no-op `{:machine_version, n - 1, n}`. Version 2
  is where `Malachi.Cluster.ClusterFlags` introduced `{:enable_flag, flag}`, so the other six moved to
  2 with nothing but that no-op.

  ## Holding the version during an upgrade

  `version/0` is `min(code_version(), pin)`, where the pin is `MALACHI_RA_MACHINE_VERSION`
  (`:ra_machine_version_pin`). An operator rolling out a release pins the version in production, so
  the effective version stays put and rolling the build back stays possible at any moment. Removing
  the pin with a rolling restart finalizes the upgrade; from then on a binary below the new version
  stops applying entries, which is the rollback floor. A pin above the code version has no effect.

  ## Moving a topic between vnodes

  A vnode split copies a topic's whole state from one Raft group into another with `:insert_topic`,
  and the two groups may have different members and different effective versions. The command tag
  alone cannot say whether the destination understands that state, so the export carries an
  `export_format`, and an export above the destination's effective version is refused. Whoever
  changes the shape of a topic's exported state raises `Malachi.Metadata`'s export format to the
  version that change ships in.

  ## Seeing a member that stopped

  `check/3` compares the effective version `ra` recorded for a local member with the version this
  node supports. It is pure over its inputs apart from reading `ra`'s counters: the caller keeps the
  last status, so a member that stays stuck is logged once and reported on every call through the
  `[:malachi, :ra, :machine_version]` telemetry event.
  """

  require Logger

  alias Malachi.I18n

  @code_version 2

  @typedoc "A command's shape: its leading atom and the size of the tuple that carries it."
  @type command_key :: {atom(), non_neg_integer()}

  @typedoc "The first machine version that understands each command shape."
  @type command_versions :: %{command_key() => non_neg_integer()}

  @typedoc "A local member's version health, as reported by `check/3`."
  @type status :: :ok | {:stuck, effective :: non_neg_integer(), supported :: non_neg_integer()}

  @typedoc "What changed since the previous status, if anything."
  @type transition :: nil | :stuck | :recovered

  @doc """
  The command shapes a pure state module accepts, each mapped to the version that introduced it.

  Keyed by shape rather than by tag, because a release that gives an existing command another field
  changes what old code can apply just as much as a new tag does. See `apply/5`.
  """
  @callback command_versions() :: command_versions()

  @doc "The newest machine version this code implements, before any pin."
  @spec code_version() :: pos_integer()
  def code_version, do: @code_version

  @doc """
  The machine version this node advertises to `ra`: `code_version/0`, lowered by the operator's pin
  (`:ra_machine_version_pin`) when one is set. Raises on a pin that is not a non-negative integer,
  which `config/runtime.exs` already rejects at boot.
  """
  @spec version() :: non_neg_integer()
  def version, do: pinned(@code_version)

  @doc """
  `code_version` lowered by the operator's pin, if one is set. `version/0` is this applied to
  `code_version/0`; it is public so a test machine that implements a newer version goes through the
  same rule.
  """
  @spec pinned(non_neg_integer()) :: non_neg_integer()
  def pinned(code_version) do
    case Application.get_env(:malachi, :ra_machine_version_pin) do
      nil -> code_version
      pin when is_integer(pin) and pin >= 0 -> min(code_version, pin)
      other -> raise ArgumentError, "invalid :ra_machine_version_pin: #{inspect(other)}"
    end
  end

  @doc """
  Applies `command` to `state` through `apply_fun` when the effective version in `meta` allows it.

  * `{:machine_version, from, to}` answers `:ok` with the state unchanged. This is where a future
    version that changes the state's shape migrates it.
  * A shape missing from `table` answers `{:error, {:unknown_command, {tag, arity}, effective}}`.
    A known tag in an unknown shape lands here too, which is the point: `{:release}` and
    `{:release, holder, fence, reason}` are not the `{:release, holder, fence}` the code implements,
    and dispatching either one would raise inside a pure module that has no clause for it, crashing
    the replica on every replay, or land in a catch-all that skips it while newer members apply it.
  * A shape introduced above the effective version answers
    `{:error, {:unsupported_command, {tag, arity}, introduced, effective}}`.
  * An `{:insert_topic, export}` whose `export_format` is above the effective version answers
    `{:error, {:unsupported_export_format, format, effective}}`. An export without the key predates
    the format and counts as 0.

  Every refusal leaves the state untouched and emits no effects.
  """
  @spec apply(map(), term(), state, command_versions(), (map(), term(), state -> result)) ::
          {state, term()} | result
        when state: term(), result: term()
  def apply(meta, command, state, table, apply_fun)

  def apply(_meta, {:machine_version, _from, _to}, state, _table, _apply_fun), do: {state, :ok}

  def apply(%{machine_version: effective} = meta, command, state, table, apply_fun) do
    key = command_key(command)

    case Map.fetch(table, key) do
      :error ->
        {state, {:error, {:unknown_command, key, effective}}}

      {:ok, introduced} when introduced > effective ->
        {state, {:error, {:unsupported_command, key, introduced, effective}}}

      {:ok, _introduced} ->
        apply_admitted(meta, command, state, apply_fun, effective)
    end
  end

  defp apply_admitted(meta, {:insert_topic, export} = command, state, apply_fun, effective) do
    case export_format(export) do
      format when format > effective -> {state, {:error, {:unsupported_export_format, format, effective}}}
      _format -> apply_fun.(meta, command, state)
    end
  end

  defp apply_admitted(meta, command, state, apply_fun, _effective), do: apply_fun.(meta, command, state)

  defp export_format(%{export_format: format}) when is_integer(format), do: format
  defp export_format(_export), do: 0

  @doc """
  The shape a command is versioned by: the leading atom of a tuple with that tuple's size, a bare atom
  with size 0, or `{:invalid, 0}` for anything else (which no table contains, so it is refused).
  """
  @spec command_key(term()) :: command_key()
  def command_key(command) when is_tuple(command) and tuple_size(command) > 0 and is_atom(elem(command, 0)),
    do: {elem(command, 0), tuple_size(command)}

  def command_key(command) when is_atom(command), do: {command, 0}
  def command_key(_command), do: {:invalid, 0}

  @doc """
  Checks whether the local member `server_id` of a `machine` cluster can still apply its log, given
  the status the caller saw last time. Returns the new status and the transition, if any.

  A member is stuck when the effective version `ra` recorded for it is above what this node supports:
  it has met the `noop` of a newer version and stops applying until it restarts on newer code. A
  member whose counters are not registered (not started yet, or stopped) keeps its last status with no
  transition: there is nothing new to say about it, and the reconcile that owns it will start it.

  Emits `[:malachi, :ra, :machine_version]` on every call where the counters exist, and logs once on
  each transition.
  """
  @spec check(module(), {atom(), node()}, status()) :: {status(), transition()}
  def check(machine, server_id, last_status) do
    case effective_version(server_id) do
      nil ->
        {last_status, nil}

      effective ->
        supported = machine.version()
        status = if effective > supported, do: {:stuck, effective, supported}, else: :ok

        :telemetry.execute(
          [:malachi, :ra, :machine_version],
          %{effective: effective, supported: supported},
          %{server_id: server_id, machine: machine, stuck: status != :ok}
        )

        transition = transition(last_status, status)
        log_transition(transition, machine, server_id, effective, supported)
        {status, transition}
    end
  end

  defp effective_version(server_id) do
    case :ra_counters.counters(server_id, [:effective_machine_version]) do
      %{effective_machine_version: effective} -> effective
      _undefined -> nil
    end
  end

  defp transition(:ok, {:stuck, _effective, _supported}), do: :stuck
  defp transition({:stuck, _effective, _supported}, :ok), do: :recovered
  defp transition(_last_status, _status), do: nil

  defp log_transition(nil, _machine, _server_id, _effective, _supported), do: :ok

  defp log_transition(:stuck, machine, server_id, effective, supported) do
    Logger.error(
      I18n.t(:ra_machine_version_unsupported,
        server: inspect(server_id),
        machine: inspect(machine),
        effective: effective,
        supported: supported
      )
    )
  end

  defp log_transition(:recovered, machine, server_id, effective, _supported) do
    Logger.info(
      I18n.t(:ra_machine_version_recovered, server: inspect(server_id), machine: inspect(machine), effective: effective)
    )
  end
end
