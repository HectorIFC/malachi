defmodule Malachi.Cluster.Capabilities do
  @moduledoc """
  What this binary can do, said out loud to the rest of the cluster, and the pure check that asks
  whether **every** node can do a given thing.

  ## Why a node has to say what it supports

  `Malachi.Cluster.MachineVersion` already stops a control-plane command from being applied by some
  members and skipped by others: `ra` moves a group to a new machine version only once every member
  supports it. That covers the Raft log and nothing else. A storage frame, a replication message or a
  compression codec never passes through a Raft log, so nothing there tells a newer node to hold off
  while its peers are still old. This module is that missing half: a node advertises a named
  capability set, and a feature asks whether the whole cluster advertises the one it needs before it
  is switched on (`Malachi.Cluster.ClusterFlags`).

  Named capabilities rather than one rising integer, because a release order is not a dependency
  order: a backport reorders integers, and one feature cannot be held back on its own under them.

  ## How it travels

  Inside the SWIM membership attributes, under the reserved atom key `:capabilities`, as a sorted list
  of atoms. That is deliberate:

    * The gossip payload already carries `attributes` as the fourth element of every update
      (`Malachi.Cluster.Membership`), so **no message shape changes**. A member on the old binary
      keeps the key it does not understand and gossips it onward untouched. Widening the update tuple
      instead would raise inside `Membership.merge/2` on that member, which the catch-all of
      `Malachi.UnexpectedMessage` does not cover: it only guards the outer shape of a message.
    * The key cannot collide with an operator's own attributes: `Malachi.Application.parse_attributes/1`
      produces only string keys.
    * Atoms rather than an integer bitmask, because a bitmask index is a second version contract
      alongside the machine version, and a backport that adds a capability out of order breaks reading
      an old advertisement in silence. Erlang distribution caches an atom per connection, so a repeated
      name costs a byte or two. If this list ever passes about sixteen entries, measure the payload
      rather than guess.

  ## One name for both halves

  A flag's name **is** the capability it requires. One registry, one name in the logs, one name on the
  command line. A flag that one day needs two capabilities turns the registry into a map in a change
  of its own; the flag's own name keeps working as the key.

  ## Where the merge lives

  `Malachi.Cluster.Membership.set_attributes/2` replaces the whole attribute map, so any path that
  sets attributes at runtime would erase the capabilities. `attributes/1` is the single function that
  puts them in, and every path that seeds or sets this node's attributes goes through it.
  """

  alias Malachi.Cluster.Membership

  # The capabilities this binary supports. Empty in the bridge release: it ships the mechanism, and
  # the first entry arrives with the first feature that needs a cluster-wide gate (#202). A name is
  # added here only by the release that can genuinely do the thing, because advertising is a promise
  # the rest of the cluster acts on.
  @capabilities []

  @typedoc "A capability, which is also the name of the flag that requires it."
  @type capability :: atom()

  @typedoc "The membership attributes a member gossips (`Malachi.Cluster.Membership`)."
  @type attributes :: Membership.attributes()

  @typedoc "One read of the membership view: what a node's status and attributes are, or `{nil, %{}}`."
  @type reads :: (node() -> {Membership.status() | nil, attributes()})

  @key :capabilities

  @doc "The reserved attribute key this node's capability list is gossiped under."
  @spec key() :: atom()
  def key, do: @key

  @doc """
  Every capability this build knows by name, sorted.

  This is the registry the operator's input is resolved against and the set a flag name must belong
  to. It is the same list as `advertised/0` today; they are separate functions because a future build
  may know a name it cannot yet honour.
  """
  @spec known() :: [capability()]
  def known, do: Enum.sort(@capabilities)

  @doc "The capabilities this node advertises to its peers, sorted so the advertisement is stable."
  @spec advertised() :: [capability()]
  def advertised, do: Enum.sort(@capabilities)

  @doc """
  This node's membership attributes: the operator's own `attributes` with this build's capability list
  merged in under `key/0`.

  The single place the merge happens. An operator key can never shadow it (operator keys are strings),
  and a caller that sets attributes at runtime keeps the capabilities by going through here rather
  than through `Malachi.Cluster.MembershipServer.set_attributes/2` directly.
  """
  @spec attributes(attributes(), [capability()]) :: attributes()
  def attributes(operator_attributes, capabilities \\ advertised()) do
    Map.put(operator_attributes, @key, capabilities)
  end

  @doc """
  The capabilities `attributes` advertises, or `[]` when it advertises none.

  A member on a build that predates this module has no such key, which reads as advertising nothing.
  That is the safe answer: it is exactly the member a flag must not be switched on over.
  """
  @spec of(attributes()) :: [capability()]
  def of(attributes) do
    case Map.get(attributes, @key) do
      list when is_list(list) -> list
      _absent_or_malformed -> []
    end
  end

  @doc """
  Whether every node in `nodes` advertises `capability`, answering `:ok` or naming the ones that do
  not.

  `reads` is the membership view, injected as `node -> {status, attributes}` so this stays pure over
  its inputs. A node counts as advertising only when it is `:alive` **and** its attributes list the
  capability. Anything else is refused and named: `:suspect`, `:dead`, unknown to the view at all
  (which answers `{nil, %{}}`), or simply running a build that does not have the capability.

  `nodes` is the statically configured node set, not the membership's alive set and not a Raft group's
  members. The alive set silently omits a node that has just joined on an old build, and a Raft group
  omits any node whose build does not have that group's machine module at all, which during a rolling
  upgrade is every node not yet upgraded. The configured set is the only population that contains a
  node the cluster has not heard from, and refusing over it is the safe direction: a stale view can
  only withhold a capability, never invent one, because attributes travel with their own member's
  incarnation.

  Monotone by construction: adding a node can only move the answer from `:ok` to a refusal, never the
  other way.
  """
  @spec supported_by_all([node()], capability(), reads()) :: :ok | {:error, {:unsupported, [node()]}}
  def supported_by_all(nodes, capability, reads) do
    case Enum.reject(nodes, &advertises?(&1, capability, reads)) do
      [] -> :ok
      missing -> {:error, {:unsupported, Enum.sort(missing)}}
    end
  end

  defp advertises?(node, capability, reads) do
    {status, attributes} = reads.(node)
    status == :alive and capability in of(attributes)
  end

  @doc """
  Resolves an operator's `input` to one of `known`, or `{:error, :unknown_flag}`.

  Never `String.to_atom/1`: the input is a command-line string, and turning arbitrary text into atoms
  grows a table that is never collected. Matching against `known/0` also means an unknown name is
  refused before anything reaches the Raft log.
  """
  @spec resolve(String.t(), [capability()]) :: {:ok, capability()} | {:error, :unknown_flag}
  def resolve(input, known \\ known()) when is_binary(input) do
    Enum.find_value(known, {:error, :unknown_flag}, fn capability ->
      if Atom.to_string(capability) == input, do: {:ok, capability}
    end)
  end
end
