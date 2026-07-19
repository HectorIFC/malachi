defmodule Malachi.Cluster.ReshardPlan do
  @moduledoc """
  Pure planning of a **grow re-sharding**: the ordered sequence of vnode splits that takes the ring from its
  current vnode count up to a larger target count (P5-follow, "re-sharding — grow"). Deterministic and free
  of `ra`/network — the ring geometry only.

  Decision **split-natural** (not `sharded_vnodes/2` even-spacing): each step splits the vnode owning the
  **largest arc** at that arc's **midpoint**, adding one new vnode there. Repeating `target - current` times
  keeps the ring approximately balanced (splitting the largest arc never raises the maximum arc) **without
  moving any existing token** — so every step is exactly one `Malachi.Cluster.VnodeSplit.split/5` and the
  gossiped ring becomes the source of truth. New vnode ids are derived from the (unique) token, so they are
  unique and deterministic; the count is bounded by the operator's target, so dynamic-atom growth is a
  non-issue (unlike untrusted input).

  `Malachi.Cluster.ReshardCoordinator` executes the steps one at a time (each a resumable split); because
  `plan/3` is a pure function of `(ring, target)`, a crashed reshard resumes by simply re-planning from the
  current ring — completed splits are already reflected, so the remaining plan is what is left.
  """

  alias Malachi.Cluster.HashRing

  @typedoc "One grow step: add `new_vnode_id` at `token`, splitting the vnode `split_from` currently owns it."
  @type step :: %{new_vnode_id: HashRing.vnode_id(), token: HashRing.token(), split_from: HashRing.vnode_id()}

  @default_prefix "vn"

  @doc """
  The ordered grow steps to take `ring` from its current vnode count to `target_count`. Returns `{:ok, []}`
  when already at the target, `{:error, :cannot_shrink}` for a smaller target (grow-only), `{:error,
  :empty_ring}` for an empty ring, or `{:error, :ring_too_dense}` if an arc is too small to split further.

  Options: `:prefix` (the new vnode-id prefix, default `"vn"`) — ids are `:"&lt;prefix&gt;_&lt;token&gt;"`.
  """
  @spec plan(HashRing.t(), pos_integer(), keyword()) :: {:ok, [step()]} | {:error, atom()}
  def plan(%HashRing{} = ring, target_count, opts \\ []) when is_integer(target_count) do
    current = HashRing.size(ring)
    prefix = Keyword.get(opts, :prefix, @default_prefix)

    cond do
      current == 0 -> {:error, :empty_ring}
      target_count < current -> {:error, :cannot_shrink}
      target_count == current -> {:ok, []}
      true -> build_steps(ring, target_count - current, prefix, [])
    end
  end

  # --- internals ---

  defp build_steps(_ring, 0, _prefix, acc), do: {:ok, Enum.reverse(acc)}

  defp build_steps(%HashRing{ring_size: ring_size} = ring, remaining, prefix, acc) do
    {split_from, start, stop} = largest_arc(ring)
    length = arc_length(start, stop, ring_size)

    if length < 2 do
      {:error, :ring_too_dense}
    else
      token = rem(start + div(length, 2), ring_size)
      new_vnode_id = :"#{prefix}_#{token}"

      case HashRing.add_vnode(ring, new_vnode_id, token) do
        {:ok, grown} ->
          step = %{new_vnode_id: new_vnode_id, token: token, split_from: split_from}
          build_steps(grown, remaining - 1, prefix, [step | acc])

        {:error, reason} ->
          {:error, {:ring_step_failed, reason}}
      end
    end
  end

  # The vnode owning the largest arc, as `{vnode_id, start_exclusive, end_inclusive}`. Deterministic: on an
  # arc-length tie the full tuple ordering (length, then vnode id) breaks it the same way on every node.
  defp largest_arc(%HashRing{ring_size: ring_size} = ring) do
    {_length, split_from, start, stop} =
      ring
      |> HashRing.vnode_ids()
      |> Enum.map(fn vnode_id ->
        {:ok, {start, stop}} = HashRing.boundaries(ring, vnode_id)
        {arc_length(start, stop, ring_size), vnode_id, start, stop}
      end)
      |> Enum.max()

    {split_from, start, stop}
  end

  # The size of the arc `(start_exclusive, stop_inclusive]` with wraparound. `start == stop` means a single
  # vnode owning the whole ring.
  defp arc_length(start, stop, ring_size) do
    cond do
      stop > start -> stop - start
      stop < start -> ring_size - start + stop
      true -> ring_size
    end
  end
end
