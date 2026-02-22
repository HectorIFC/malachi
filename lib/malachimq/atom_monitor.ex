defmodule MalachiMQ.AtomMonitor do
  @moduledoc """
  Monitors atom table usage to prevent exhaustion attacks.
  Alerts when approaching the 1,048,576 atom limit (BEAM VM default).

  The atom table in the BEAM VM is never garbage collected. Every atom
  created persists for the lifetime of the VM. This monitor watches
  usage levels and emits warnings/critical alerts via Logger and AuditLog.

  ## Configuration

    * `:atom_check_interval_ms` - Check interval (default: 60_000)
    * `:atom_warning_threshold` - Warning at this fraction (default: 0.7)
    * `:atom_critical_threshold` - Critical at this fraction (default: 0.9)
  """

  use GenServer
  require Logger
  alias MalachiMQ.I18n

  @atom_limit 1_048_576

  defstruct [
    :check_interval_ms,
    :warning_threshold,
    :critical_threshold,
    :last_atom_count,
    :warning_sent,
    :critical_sent
  ]

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Returns the current number of atoms in the VM.
  """
  @spec get_atom_count() :: non_neg_integer()
  def get_atom_count do
    :erlang.system_info(:atom_count)
  end

  @doc """
  Returns the maximum atom table size (BEAM default: 1,048,576).
  """
  @spec get_atom_limit() :: pos_integer()
  def get_atom_limit do
    @atom_limit
  end

  @doc """
  Returns atom table usage as a percentage (0.0 - 100.0).
  """
  @spec get_atom_usage_percent() :: float()
  def get_atom_usage_percent do
    count = get_atom_count()
    Float.round(count / @atom_limit * 100, 2)
  end

  @doc """
  Returns a map with atom table statistics.

  ## Returns

      %{
        atom_count: 12345,
        atom_limit: 1_048_576,
        usage_percent: 1.18,
        status: :normal | :warning | :critical
      }
  """
  @spec get_stats() :: map()
  def get_stats do
    count = get_atom_count()
    usage_pct = Float.round(count / @atom_limit * 100, 2)

    %{
      atom_count: count,
      atom_limit: @atom_limit,
      usage_percent: usage_pct,
      status: get_status(count)
    }
  end

  # --- GenServer callbacks ---

  @impl true
  def init(_opts) do
    check_interval = Application.get_env(:malachimq, :atom_check_interval_ms, 60_000)
    warning_threshold = Application.get_env(:malachimq, :atom_warning_threshold, 0.7)
    critical_threshold = Application.get_env(:malachimq, :atom_critical_threshold, 0.9)

    schedule_check(check_interval)

    Logger.info(
      I18n.t(:atom_monitor_started,
        interval_ms: check_interval,
        warning: Float.round(warning_threshold * 100, 0),
        critical: Float.round(critical_threshold * 100, 0)
      )
    )

    {:ok,
     %__MODULE__{
       check_interval_ms: check_interval,
       warning_threshold: warning_threshold,
       critical_threshold: critical_threshold,
       last_atom_count: get_atom_count(),
       warning_sent: false,
       critical_sent: false
     }}
  end

  @impl true
  def handle_info(:check_atoms, state) do
    atom_count = get_atom_count()
    usage_ratio = atom_count / @atom_limit

    new_state = check_and_alert(state, atom_count, usage_ratio)

    schedule_check(state.check_interval_ms)
    {:noreply, %{new_state | last_atom_count: atom_count}}
  end

  # --- Private helpers ---

  defp get_status(atom_count) do
    usage = atom_count / @atom_limit
    warning = Application.get_env(:malachimq, :atom_warning_threshold, 0.7)
    critical = Application.get_env(:malachimq, :atom_critical_threshold, 0.9)

    cond do
      usage >= critical -> :critical
      usage >= warning -> :warning
      true -> :normal
    end
  end

  defp check_and_alert(state, atom_count, usage_ratio) do
    cond do
      usage_ratio >= state.critical_threshold and not state.critical_sent ->
        usage_pct = Float.round(usage_ratio * 100, 1)

        Logger.error(
          "CRITICAL: Atom table usage at #{usage_pct}% " <>
            "(#{atom_count}/#{@atom_limit}). " <>
            "Possible atom exhaustion attack or dynamic atom leak."
        )

        try_audit_log(
          :security_violation,
          %{type: :system},
          "atom_table_critical",
          :critical,
          %{usage_percent: usage_pct, atom_count: atom_count, atom_limit: @atom_limit}
        )

        %{state | critical_sent: true, warning_sent: true}

      usage_ratio >= state.warning_threshold and not state.warning_sent ->
        usage_pct = Float.round(usage_ratio * 100, 1)

        Logger.warning(
          "WARNING: Atom table usage at #{usage_pct}% " <>
            "(#{atom_count}/#{@atom_limit}). " <>
            "Monitor for potential atom leaks."
        )

        try_audit_log(
          :security_violation,
          %{type: :system},
          "atom_table_warning",
          :warning,
          %{usage_percent: usage_pct, atom_count: atom_count, atom_limit: @atom_limit}
        )

        %{state | warning_sent: true}

      usage_ratio < state.warning_threshold ->
        # Reset flags if usage drops below warning threshold
        %{state | warning_sent: false, critical_sent: false}

      true ->
        state
    end
  end

  defp try_audit_log(event_type, context, action, status, metadata) do
    if Process.whereis(MalachiMQ.AuditLog) do
      MalachiMQ.AuditLog.log_event(event_type, context, action, status, metadata)
    end
  rescue
    _ -> :ok
  end

  defp schedule_check(interval) do
    Process.send_after(self(), :check_atoms, interval)
  end
end
