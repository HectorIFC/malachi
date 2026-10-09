defmodule Malachi.Cluster.Advertised do
  @moduledoc """
  The address a client reaches this node at, which the node gossips to the rest of the cluster so any
  of them can tell a client where every broker is (`cluster_state`, `Malachi.Wire` key 24). NorthGuard
  spreads exactly this minimal state, each broker's host and port, over its membership dissemination
  (meetup transcript, 537 and 611 to 613).

  The address comes from `MALACHI_ADVERTISED_HOST` and `MALACHI_ADVERTISED_PORT`; the port defaults to the
  one the TCP listener binds (`MALACHI_TCP_PORT`). A node with peers must be told its host, and one a
  client can reach from elsewhere: without it, or with a loopback or unspecified host, every client routed to this node
  would dial itself, so the node refuses to start rather than advertise an address nobody can use. A
  node with no peers is its own whole cluster, and its node name's host is a fine default (in
  Kubernetes the manifest sets the variable to the pod's stable name in the headless service).

  It travels in the SWIM attributes under the reserved atom key `:advertised`, beside
  `Malachi.Cluster.Capabilities`: operator attributes only ever have string keys, so the two cannot
  collide, and a member on an older build gossips a key it does not know onward untouched.
  """

  alias Malachi.IPAddress

  @key :advertised
  @loopback_names ["localhost", "localhost.localdomain", "ip6-localhost", "ip6-loopback"]

  @typedoc "Where a client reaches a node."
  @type t :: %{host: String.t(), port: :inet.port_number()}

  @typedoc "Why a node cannot start with the address it was given."
  @type refusal :: :missing_host | {:loopback_host, String.t()}

  @doc "The attribute key the address travels under."
  @spec key() :: :advertised
  def key, do: @key

  @doc """
  The address this node advertises, from the configured `host` and `port` (either may be nil), the
  port the listener binds (`tcp_port`), this node's name and whether it has peers.
  """
  @spec resolve(String.t() | nil, :inet.port_number() | nil, :inet.port_number(), node(), boolean()) ::
          {:ok, t()} | {:error, refusal()}
  def resolve(host, port, tcp_port, node_name, peers?) do
    # Brackets come off first, so what they held is checked like any host: blank (a template whose
    # variable was unset), padded, loopback.
    with {:ok, host} <- host(host |> blank_to_nil() |> unbracket() |> blank_to_nil(), node_name, peers?),
         do: {:ok, %{host: host, port: port || tcp_port}}
  end

  # An IPv6 address written in brackets, as in a URL, is advertised without them: a client dials the
  # address, and no resolver knows the bracketed form.
  defp unbracket("[" <> rest = host) do
    case String.split(rest, "]") do
      [address, ""] -> address
      _not_bracketed -> host
    end
  end

  defp unbracket(host), do: host

  @doc "Puts `address` into `attributes` under `key/0`."
  @spec put(map(), t()) :: map()
  def put(attributes, address), do: Map.put(attributes, @key, address)

  @doc """
  Keeps the address `current` carries when `attributes` states none: the runtime path that replaces a
  node's attributes (`Malachi.Cluster.MembershipServer.set_attributes/2`) would otherwise erase it, and
  peers would carry a node no client can find.
  """
  @spec ensure(map(), map()) :: map()
  def ensure(attributes, current) do
    case {Map.has_key?(attributes, @key), Map.fetch(current, @key)} do
      {false, {:ok, address}} -> Map.put(attributes, @key, address)
      _stated_or_none -> attributes
    end
  end

  @doc "The address in a member's attributes, or nil when it advertises none (a node on an older build)."
  @spec of(map()) :: t() | nil
  def of(%{@key => %{host: host, port: port} = address}) when is_binary(host) and is_integer(port), do: address
  def of(_attributes), do: nil

  @doc """
  Whether a client elsewhere dialling `host` would reach the client's own machine instead of this node:
  a loopback name or address (IPv4, IPv6, or IPv4 mapped into IPv6), or the unspecified address a
  listener binds (`0.0.0.0`, `::`), which a connection treats as the local host.
  """
  @spec local_only?(String.t()) :: boolean()
  def local_only?(host) do
    host = normalize(host)

    case IPAddress.parse(host) do
      {:ok, address} -> local_only_address?(address)
      :error -> host in @loopback_names
    end
  end

  # How people write the same host: any case, a trailing dot (a fully qualified name), an IPv6 address in
  # brackets, as in a URL, and an IPv6 zone (`::1%lo0`), which names an interface and not another machine.
  # A name that only resolves to loopback (an /etc/hosts entry) cannot be told without resolving it, which
  # a check at boot does not do.
  defp normalize(host) do
    host
    |> String.downcase()
    |> String.trim_trailing(".")
    |> unbracket()
    |> String.split("%", parts: 2)
    |> hd()
  end

  defp local_only_address?({127, _, _, _}), do: true
  defp local_only_address?({0, 0, 0, 0}), do: true
  defp local_only_address?({0, 0, 0, 0, 0, 0, 0, 1}), do: true
  defp local_only_address?({0, 0, 0, 0, 0, 0, 0, 0}), do: true
  # ::ffff:a.b.c.d carries an IPv4 address in its last two groups
  defp local_only_address?({0, 0, 0, 0, 0, 0xFFFF, high, low}),
    do: local_only_address?({div(high, 256), rem(high, 256), div(low, 256), rem(low, 256)})

  defp local_only_address?(_routable), do: false

  defp host(nil, _node_name, true), do: {:error, :missing_host}
  defp host(nil, node_name, false), do: {:ok, node_host(node_name)}

  defp host(host, _node_name, true) do
    if local_only?(host), do: {:error, {:loopback_host, host}}, else: {:ok, host}
  end

  defp host(host, _node_name, false), do: {:ok, host}

  # The host part of a node name; a node started without distribution (`nonode@nohost`) is reached on
  # this machine.
  defp node_host(:nonode@nohost), do: "localhost"
  defp node_host(node_name), do: node_name |> Atom.to_string() |> String.split("@", parts: 2) |> List.last()

  defp blank_to_nil(nil), do: nil

  defp blank_to_nil(value) do
    case String.trim(value) do
      "" -> nil
      trimmed -> trimmed
    end
  end
end
