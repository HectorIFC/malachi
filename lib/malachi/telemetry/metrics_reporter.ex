defmodule Malachi.Telemetry.MetricsReporter do
  @moduledoc """
  The default telemetry handler: folds the `Malachi.Telemetry` hot-path events into the ETS
  `Malachi.Metrics` counters, so produce/consume/auth/replication totals show up on the Prometheus
  `/metrics` endpoint without every operator wiring their own handler. Attached once at boot (when
  `Malachi.Metrics` starts); users may still attach their own handlers alongside this one.
  """

  alias Malachi.Metrics

  @handler_id "malachi-metrics-reporter"

  @events [
    [:malachi, :produce],
    [:malachi, :consume],
    [:malachi, :auth],
    [:malachi, :replication, :commit]
  ]

  @doc "Attaches the reporter (idempotent — a previous attachment is replaced)."
  @spec attach() :: :ok
  def attach do
    # Detach first so a Metrics restart re-attaches cleanly instead of hitting :already_exists.
    _ = :telemetry.detach(@handler_id)
    :telemetry.attach_many(@handler_id, @events, &__MODULE__.handle_event/4, nil)
  end

  @doc false
  def handle_event([:malachi, :produce], %{count: count, bytes: bytes}, _metadata, _config) do
    Metrics.record_produce(count, bytes)
  end

  def handle_event([:malachi, :consume], %{count: count}, _metadata, _config) do
    Metrics.record_consume(count)
  end

  def handle_event([:malachi, :auth], _measurements, %{result: result}, _config) do
    Metrics.record_auth(result)
  end

  def handle_event([:malachi, :replication, :commit], _measurements, %{result: result}, _config) do
    Metrics.record_replication(result)
  end
end
