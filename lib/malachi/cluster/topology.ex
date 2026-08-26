defmodule Malachi.Cluster.Topology do
  @moduledoc """
  Builds the libcluster topology from the operator's cluster config.

  Connectivity-only: libcluster discovers and connects peer nodes over Erlang distribution; the SWIM
  membership and the `ra` control plane keep using the configured `:log_nodes` for their initial member
  set, and dynamic `ra` membership rides on the rebalancing coordinator. This module only wires the
  discovery strategy: it does not touch cluster formation.

  Pure: `build/1` maps the config to the keyword list `Cluster.Supervisor` expects, so the strategy wiring
  is unit-testable without opening a multicast or Kubernetes-API socket. Absent `:strategy` => `[]` (no
  clustering supervisor is started; the single-node default is unchanged).

  Supported strategies:

    * `:gossip`. UDP multicast (dev/LAN, near-zero config; `:port` has a default)
    * `:kubernetes`. Pod discovery via the Kubernetes API (`:kubernetes_selector` and
                      `:kubernetes_node_basename` are required)
    * `:epmd`. A static host list (bridges the existing `:log_nodes`), kept connected by libcluster
  """

  # The single topology name Cluster.Supervisor manages for this app.
  @topology :malachi

  @type strategy :: :gossip | :kubernetes | :epmd

  @doc """
  Maps a cluster config map to a libcluster topologies keyword list. Raises `ArgumentError` on a strategy
  with missing required config (fail-fast at boot).
  """
  @spec build(map()) :: keyword()
  def build(%{strategy: nil}), do: []

  def build(%{strategy: strategy} = config) do
    [{@topology, [strategy: strategy_module(strategy), config: strategy_config(strategy, config)]}]
  end

  def build(_config), do: []

  defp strategy_module(:gossip), do: Cluster.Strategy.Gossip
  defp strategy_module(:kubernetes), do: Cluster.Strategy.Kubernetes
  defp strategy_module(:epmd), do: Cluster.Strategy.Epmd

  defp strategy_module(other) do
    raise ArgumentError,
          "unknown cluster strategy #{inspect(other)} (expected :gossip, :kubernetes or :epmd)"
  end

  # gossip: the port has a sane default; secret and multicast_addr are optional (nils dropped).
  defp strategy_config(:gossip, config) do
    [port: Map.get(config, :gossip_port, 45_892)]
    |> put_unless_nil(:secret, Map.get(config, :gossip_secret))
    |> put_unless_nil(:multicast_addr, Map.get(config, :gossip_multicast_addr))
  end

  # kubernetes: selector + node_basename have no sane default, so they are required; namespace optional.
  defp strategy_config(:kubernetes, config) do
    [
      kubernetes_selector: require_field(config, :kubernetes_selector, :kubernetes),
      kubernetes_node_basename: require_field(config, :kubernetes_node_basename, :kubernetes),
      mode: Map.get(config, :kubernetes_mode, :hostname),
      polling_interval: Map.get(config, :kubernetes_polling_interval, 10_000)
    ]
    |> put_unless_nil(:kubernetes_namespace, Map.get(config, :kubernetes_namespace))
  end

  # epmd: a static host list (reuses :log_nodes), which libcluster keeps connected.
  defp strategy_config(:epmd, config) do
    case Map.get(config, :epmd_hosts, []) do
      [] ->
        raise ArgumentError,
              "cluster strategy :epmd requires a non-empty host list (set MALACHI_LOG_NODES)"

      hosts ->
        [hosts: hosts]
    end
  end

  defp require_field(config, key, strategy) do
    case Map.get(config, key) do
      value when value in [nil, ""] ->
        raise ArgumentError, "cluster strategy #{inspect(strategy)} requires #{key}"

      value ->
        value
    end
  end

  defp put_unless_nil(kw, _key, nil), do: kw
  defp put_unless_nil(kw, _key, ""), do: kw
  defp put_unless_nil(kw, key, value), do: Keyword.put(kw, key, value)
end
