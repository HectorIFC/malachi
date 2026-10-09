defmodule Malachi.Routing do
  @moduledoc """
  What a routing client asks before it talks to the broker that owns its data: the cluster's minimal
  state (`cluster_state`, `Malachi.Wire` key 24) and a topic's routes (`topic_routes`, key 25).
  NorthGuard's client fetches the same two things and then speaks to the owning broker directly
  (meetup transcript, 609 to 613 and 885 to 886).

  `cluster_state/3` and `topic_routes/2` are pure: they turn the membership view and a vnode's metadata
  into the maps the wire encodes. `read_cluster_state/0` and `read_topic_routes/1` fetch those inputs.

  ## Versions

  Each answer carries a version: the first 64 bits of a SHA-256 of the answer as `Malachi.Wire` encodes
  it with the version set to 0. It changes exactly when the answer does, and because the encoding is
  fixed-width binary with every list in a fixed order, any node on any OTP release computes the same
  one. A client compares two versions for equality; nothing orders them, and nothing has to, because the
  routes are read linearizably from the vnode that owns the topic. The routes live only in the
  replicated metadata, so a version needs no state of its own.
  """

  alias Malachi.Application, as: App
  alias Malachi.BrokerServer
  alias Malachi.Cluster.Advertised
  alias Malachi.Cluster.ClusterFlagsCache
  alias Malachi.Cluster.HashRing
  alias Malachi.Cluster.Membership
  alias Malachi.Cluster.MembershipServer
  alias Malachi.Cluster.RaCluster
  alias Malachi.Consumer.CoordinatorRouter
  alias Malachi.DataPlaneRouter
  alias Malachi.Metadata
  alias Malachi.TCPAcceptorPool
  alias Malachi.Wire

  @flag :producer_streams
  @membership Malachi.LogMembership
  @read_timeout 2_000
  # What a route read projects out of a vnode's state inside the leader: every topic, range and segment the
  # vnode holds and the indexes over them (the whole metadata when the control plane is unsharded), not the
  # offsets and policies beside them. The topic is picked out here, in the caller, because the leader may
  # only apply a standard library function (`Malachi.Cluster.RaCluster.project/3`); what that copy costs
  # per request is measured with the routing clients that ask for it (#275).
  @route_keys [:topics, :ranges, :segments, :topic_ranges, :range_segments]

  @typedoc "A member as the membership view holds it."
  @type member :: {node(), Membership.status(), map()}

  @doc "The cluster flag (and capability) that switches the stream keys on."
  @spec flag() :: :producer_streams
  def flag, do: @flag

  @doc """
  The cluster state from `members` and the ring's vnode tokens, with whether the stream keys are on.
  Brokers are ordered by id; one advertising no address (a node on an older build) has a nil host and
  port 0.
  """
  @spec cluster_state([member()], [non_neg_integer()], boolean()) :: map()
  def cluster_state(members, vnode_tokens, streams_enabled) do
    brokers =
      members
      |> Enum.map(fn {node_name, status, attributes} -> broker(node_name, status, Advertised.of(attributes)) end)
      |> Enum.sort_by(& &1.id)

    vnodes = Enum.sort(vnode_tokens)

    with_version(
      %{streams_enabled: streams_enabled, brokers: brokers, vnodes: vnodes},
      &Wire.encode_cluster_state_resp/1
    )
  end

  @doc """
  `topic`'s routes in `metadata` (the state of the vnode that owns it): every range, active or sealed,
  ordered by its sequence, with its active segment and that segment's primary, or no segment while the
  range has none (a new range takes its first segment on its first append).
  """
  @spec topic_routes(Metadata.t(), Metadata.topic_name()) :: {:ok, map()} | {:error, :no_such_topic}
  def topic_routes(%Metadata{} = metadata, topic) do
    case Metadata.get_topic(metadata, topic) do
      nil ->
        {:error, :no_such_topic}

      topic_meta ->
        bits = bits(topic_meta.keyspace_size)

        ranges =
          metadata
          |> Metadata.ranges_of_topic(topic)
          |> Enum.sort_by(fn %{id: {_topic, seq}} -> seq end)
          |> Enum.map(&route(metadata, &1))

        {:ok, with_version(%{topic: topic, keyspace_bits: bits, ranges: ranges}, &Wire.encode_topic_routes_resp/1)}
    end
  end

  @doc """
  The cluster state as this node sees it: the members of its membership view, the vnode tokens of the
  ring it adopted and whether `flag/0` is on. `{:error, :unavailable}` when the membership server does
  not answer in time, or is not running (between a crash and its restart). A node with no control plane
  (measured in memory) is its own whole cluster: itself and one vnode at token 0.
  """
  @spec read_cluster_state() :: {:ok, map()} | {:error, :unavailable}
  def read_cluster_state do
    if Application.get_env(:malachi, :log_cluster) do
      with {:ok, members} <- members(), {:ok, tokens} <- vnode_tokens() do
        {:ok, cluster_state(members, tokens, ClusterFlagsCache.enabled?(@flag))}
      end
    else
      # No control plane (a single node measured in memory): its own whole cluster.
      {:ok, cluster_state([{node(), :alive, App.membership_attributes()}], [0], ClusterFlagsCache.enabled?(@flag))}
    end
  end

  @doc """
  `topic`'s routes, read linearizably from the vnode that owns it, or from this node's broker when the
  metadata is in memory (a single node measured without a control plane).
  """
  @spec read_topic_routes(Metadata.topic_name()) :: {:ok, map()} | {:error, term()}
  def read_topic_routes(topic) do
    with {:ok, metadata} <- read_metadata(topic), do: topic_routes(metadata, topic)
  end

  defp broker(node_name, status, address) do
    {host, port} = if address, do: {address.host, address.port}, else: {nil, 0}
    %{id: Atom.to_string(node_name), host: host, port: listening_port(node_name, port), status: status}
  end

  # A node whose listener was configured on port 0 (an ephemeral port, in tests) advertises 0 because
  # its membership starts before the listener binds; its own answer gives the port the listener took.
  defp listening_port(node_name, 0) when node_name == node(), do: TCPAcceptorPool.port() || 0
  defp listening_port(_node_name, port), do: port

  defp route(metadata, %{id: {_topic, seq}} = range) do
    %{
      range: seq,
      key_start: range.key_start,
      key_end: range.key_end,
      state: range.state,
      segment: active_segment(metadata, range.id)
    }
  end

  defp active_segment(metadata, range_id) do
    metadata
    |> Metadata.segments_of_range(range_id)
    |> Enum.filter(&(&1.state == :active))
    |> Enum.map(&{Metadata.segment_seq(&1.id), Metadata.broker_ref_string(List.first(&1.replica_set))})
    |> Enum.filter(fn {seq, primary} -> is_integer(seq) and is_binary(primary) end)
    |> Enum.max_by(&elem(&1, 0), fn -> nil end)
    |> case do
      nil -> nil
      {seq, primary} -> %{segment: seq, primary: primary}
    end
  end

  # A keyspace's size is always 2^bits (`Malachi.Keyspace.size_for_bits/1`).
  defp bits(size), do: size |> Integer.digits(2) |> length() |> Kernel.-(1)

  # The answer with its version: the first 64 bits of a SHA-256 of its wire encoding at version 0.
  defp with_version(answer, encode) do
    <<version::64, _rest::binary>> = :crypto.hash(:sha256, encode.(Map.put(answer, :version, 0)))
    Map.put(answer, :version, version)
  end

  @doc "The members of a membership view, as `cluster_state/3` takes them."
  @spec members_of(Membership.t()) :: [member()]
  def members_of(%Membership{members: members}) do
    for {{_name, node_name}, %{status: status, attributes: attributes}} <- members, do: {node_name, status, attributes}
  end

  # A membership server that does not answer in time, or is not running (between a crash and its restart),
  # leaves the brokers unknown: the answer is unavailable rather than this node alone under a version a
  # client would take as real.
  defp members do
    {:ok, members_of(MembershipServer.view(@membership, @read_timeout))}
  catch
    :exit, _reason -> {:error, :unavailable}
  end

  defp vnode_tokens, do: tokens(App.current_vnodes())

  @doc """
  The vnode tokens of a ring read (`Malachi.Application.current_vnodes/0`): an unsharded control plane is
  one vnode at token 0, as `Malachi.Cluster.DSRSM` places it.
  """
  @spec tokens({:ok, [{atom(), non_neg_integer(), [node()]}]} | :error) ::
          {:ok, [non_neg_integer()]} | {:error, :unavailable}
  def tokens({:ok, []}), do: {:ok, [0]}
  def tokens({:ok, vnodes}), do: {:ok, Enum.map(vnodes, fn {_vnode_id, token, _nodes} -> token end)}
  def tokens(:error), do: {:error, :unavailable}

  defp read_metadata(topic) do
    case metadata_server(topic) do
      {:ok, server_id} -> project(server_id)
      :in_memory -> {:ok, BrokerServer.metadata(DataPlaneRouter.shard_for(topic))}
      {:error, _reason} = error -> error
    end
  end

  defp metadata_server(topic),
    do: metadata_source(topic, CoordinatorRouter.topology(), Application.get_env(:malachi, :log_cluster))

  @doc """
  Where `topic`'s metadata is read from: the ra server of the vnode that owns it when the control plane is
  sharded (`topology`, as `Malachi.Consumer.CoordinatorRouter` publishes it), this node's member of the
  single control-plane `cluster` otherwise, or the broker's in-memory metadata when there is no cluster.
  """
  @spec metadata_source(Metadata.topic_name(), map() | nil, atom() | nil) ::
          {:ok, RaCluster.server_id()} | :in_memory | {:error, :unavailable}
  def metadata_source(topic, %{ring: ring, servers: servers}, _cluster) do
    with {:ok, vnode_id} <- HashRing.route(ring, topic),
         {:ok, server_id} <- Map.fetch(servers, vnode_id) do
      {:ok, server_id}
    else
      _unroutable -> {:error, :unavailable}
    end
  end

  def metadata_source(_topic, nil, nil), do: :in_memory
  def metadata_source(_topic, nil, cluster), do: {:ok, {cluster, node()}}

  defp project(server_id) do
    case RaCluster.project(server_id, {:maps, :with, [@route_keys]}, @read_timeout) do
      {:ok, %{topics: topics} = state} when is_map(topics) -> {:ok, struct(Metadata, state)}
      {:ok, _other} -> {:error, :unavailable}
      {:error, _reason} -> {:error, :unavailable}
    end
  end
end
