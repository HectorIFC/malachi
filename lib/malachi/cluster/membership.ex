defmodule Malachi.Cluster.Membership do
  @moduledoc """
  The **pure** state of SWIM-style cluster membership: who is `:alive`, `:suspect`, or `:dead`,
  with per-member **incarnation** numbers. No processes, timers, or network: this is the
  deterministic core that the failure detector and gossip transport
  (`Malachi.Cluster.MembershipServer`) drive.

  Each member owns its incarnation: only that member raises its own, to **refute** a false
  suspicion. An update is `{member, status, incarnation}`, and merging updates is a join on the
  lexicographic order `{incarnation, rank}` where `rank` is `alive < suspect < dead`. That single
  rule yields the SWIM precedence:

    * a higher incarnation always wins (a fresh `:alive` overrides an old `:suspect`/`:dead`);
    * at equal incarnation, `:suspect` overrides `:alive` and `:dead` overrides both;
    * equal `{incarnation, rank}` changes nothing (idempotent).

  Because the merge is a commutative, associative, idempotent join, applying a batch of gossiped
  updates **converges** regardless of order, what makes infection-style dissemination correct.

  The one exception is an update about **self**: a `:suspect`/`:dead` about us is refuted by
  bumping our own incarnation above it and re-announcing `:alive`, so we never stay suspected while
  running. `apply_update/2` returns that refutation as an effect to disseminate.
  """

  @type member :: term()
  @type status :: :alive | :suspect | :dead
  @type incarnation :: non_neg_integer()
  @typedoc "Opaque k/v an admin attaches to a broker (e.g. `rack`, `dc`); disseminated with the member."
  @type attributes :: %{optional(term()) => term()}
  @type member_state :: %{status: status(), incarnation: incarnation(), attributes: attributes()}
  @type update :: {member(), status(), incarnation(), attributes()}
  @typedoc "What an applied update produced: a state change or a self-refutation to disseminate."
  @type effect :: {:applied, update()} | {:refute, update()} | :ignored

  @type t :: %__MODULE__{self: member(), members: %{member() => member_state()}}

  defstruct self: nil, members: %{}

  @ranks %{alive: 0, suspect: 1, dead: 2}

  @doc """
  Builds a membership view local to `self` (which starts `:alive` at incarnation 0). `:peers`
  seeds other members, also `:alive` at incarnation 0. `:attributes` are `self`'s own attributes
  (peers' attributes are learned via gossip).
  """
  @spec new(member(), keyword()) :: t()
  def new(self, opts \\ []) do
    peers = Keyword.get(opts, :peers, [])
    self_attributes = Keyword.get(opts, :attributes, %{})

    members =
      Map.new([self | peers], fn member ->
        attributes = if member == self, do: self_attributes, else: %{}
        {member, %{status: :alive, incarnation: 0, attributes: attributes}}
      end)

    %__MODULE__{self: self, members: members}
  end

  @doc """
  Applies one `{member, status, incarnation}` update under the SWIM precedence rules. Returns the
  new view and an effect: `{:applied, update}` if it changed our state (disseminate it onward),
  `{:refute, update}` if it was a false suspicion about us (announce the refutation), or `:ignored`.
  """
  @spec apply_update(t(), update()) :: {t(), effect()}
  def apply_update(%__MODULE__{self: self} = view, {member, status, incarnation, _attributes}) when member == self do
    # We are the authority on our own attributes, so ignore the peer's copy of them here.
    refute_or_ignore(view, status, incarnation)
  end

  def apply_update(%__MODULE__{} = view, {member, status, incarnation, attributes}) do
    if overrides?(Map.get(view.members, member), status, incarnation) do
      {put_member(view, member, status, incarnation, attributes), {:applied, {member, status, incarnation, attributes}}}
    else
      {view, :ignored}
    end
  end

  @doc """
  Applies a batch of updates (e.g. a received gossip), returning the new view and the list of
  updates that changed state (applied changes plus any self-refutation) to disseminate onward.
  Order-independent: the resulting view is the same for any permutation of `updates`.
  """
  @spec merge(t(), [update()]) :: {t(), [update()]}
  def merge(%__MODULE__{} = view, updates) do
    {view, effects} =
      Enum.reduce(updates, {view, []}, fn update, {view, effects} ->
        case apply_update(view, update) do
          {view, :ignored} -> {view, effects}
          {view, {_kind, change}} -> {view, [change | effects]}
        end
      end)

    {view, Enum.reverse(effects)}
  end

  @doc "Marks `member` `:suspect` at its currently known incarnation (a no-op if already overridden)."
  @spec suspect(t(), member()) :: {t(), effect()}
  def suspect(%__MODULE__{} = view, member) do
    apply_update(view, {member, :suspect, incarnation(view, member) || 0, attributes(view, member)})
  end

  @doc "Confirms `member` `:dead` at its currently known incarnation."
  @spec confirm(t(), member()) :: {t(), effect()}
  def confirm(%__MODULE__{} = view, member) do
    apply_update(view, {member, :dead, incarnation(view, member) || 0, attributes(view, member)})
  end

  @doc "The sorted list of `:alive` members: the live broker set for placement and healing."
  @spec alive_members(t()) :: [member()]
  def alive_members(%__MODULE__{} = view) do
    for({member, %{status: :alive}} <- view.members, do: member) |> Enum.sort()
  end

  @doc "The full view as a list of `{member, status, incarnation, attributes}` updates, for gossiping."
  @spec updates(t()) :: [update()]
  def updates(%__MODULE__{} = view) do
    for {member, %{status: status, incarnation: incarnation, attributes: attributes}} <- view.members do
      {member, status, incarnation, attributes}
    end
  end

  @doc """
  Sets `self`'s `attributes`, raising its own incarnation so the change wins the merge everywhere.
  Returns the new view and `{:applied, update}` to disseminate (attributes are owned by their member).
  """
  @spec set_attributes(t(), attributes()) :: {t(), effect()}
  def set_attributes(%__MODULE__{self: self} = view, attributes) do
    incarnation = Map.fetch!(view.members, self).incarnation + 1
    update = {self, :alive, incarnation, attributes}
    {put_member(view, self, :alive, incarnation, attributes), {:applied, update}}
  end

  @doc "The attributes of `member` (`%{}` if unknown or none set)."
  @spec attributes(t(), member()) :: attributes()
  def attributes(%__MODULE__{} = view, member) do
    case Map.get(view.members, member) do
      nil -> %{}
      %{attributes: attributes} -> attributes
    end
  end

  @doc "The status of `member`, or `nil` if unknown."
  @spec status(t(), member()) :: status() | nil
  def status(%__MODULE__{} = view, member) do
    case Map.get(view.members, member) do
      nil -> nil
      %{status: status} -> status
    end
  end

  @doc "The incarnation of `member`, or `nil` if unknown."
  @spec incarnation(t(), member()) :: incarnation() | nil
  def incarnation(%__MODULE__{} = view, member) do
    case Map.get(view.members, member) do
      nil -> nil
      %{incarnation: incarnation} -> incarnation
    end
  end

  # --- internals ---

  defp refute_or_ignore(view, :alive, incarnation) do
    # An alive about ourselves: only adopt a strictly higher incarnation (normally never happens,
    # since only we raise our own); never needs dissemination. We keep our own attributes.
    self_state = Map.fetch!(view.members, view.self)

    if incarnation > self_state.incarnation do
      {put_member(view, view.self, :alive, incarnation, self_state.attributes), :ignored}
    else
      {view, :ignored}
    end
  end

  defp refute_or_ignore(view, _suspect_or_dead, incarnation) do
    # A suspicion/confirmation about ourselves: refute by jumping past it and re-announcing alive,
    # carrying our own attributes so peers keep them.
    self_state = Map.fetch!(view.members, view.self)
    refuted = max(incarnation, self_state.incarnation) + 1
    update = {view.self, :alive, refuted, self_state.attributes}
    {put_member(view, view.self, :alive, refuted, self_state.attributes), {:refute, update}}
  end

  defp overrides?(nil, _status, _incarnation), do: true

  defp overrides?(%{status: current_status, incarnation: current_incarnation}, status, incarnation) do
    {incarnation, @ranks[status]} > {current_incarnation, @ranks[current_status]}
  end

  defp put_member(view, member, status, incarnation, attributes) do
    member_state = %{status: status, incarnation: incarnation, attributes: attributes}
    %{view | members: Map.put(view.members, member, member_state)}
  end
end
