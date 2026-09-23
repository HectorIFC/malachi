defmodule Malachi.Retention.Orphans do
  @moduledoc """
  The decision half of the orphan sweep: which directories under a data directory belong to no segment
  the control plane still lists, and which of those have been unexplained for long enough to remove.

  Pure, so the one rule that matters can be stated as a property: **a directory of a segment present in
  the metadata is never selected, whatever the sequence of operations that produced that metadata.**

  ## Why it is expected-minus-actual, never a parsed name

  `Malachi.Storage.Layout.segment_directory/2` maps a segment id to a directory name and nothing maps
  back: the readable form is ambiguous (a topic may itself contain `-r0-s0`) and the encoded form is a
  Base64 term. So the expected set is built forward from the metadata and a directory is a candidate by
  being absent from it. A name is never taken apart.

  ## The three guards, and what each one is for

    * **Age.** A replica creates its directory on the first `follow/4`, which can happen before this
      node's metadata view shows the registration. The minimum age has to exceed the worst registration
      lag, so a segment being registered right now is never mistaken for one nobody remembers.
    * **Sightings.** The same directory has to be unexplained on N consecutive passes an interval apart.
      A metadata view that was briefly wrong explains a directory again on the next pass and the count
      restarts, because the count only survives while the directory keeps being a candidate.
    * **A cap per pass.** A guard against this module rather than against the cluster: if the expected
      set ever came out wrong, the operator loses a bounded number of directories and sees the counter
      move, instead of losing the node's data between two ticks.

  Reserved names are excluded before any of that: the data directory's own format marker and, on a
  sharded single-node data plane, the per-shard subdirectories, neither of which is a segment.
  """

  alias Malachi.Metadata
  alias Malachi.Storage.Layout

  @typedoc "A directory found under the data directory: its name and how long ago it was created."
  @type entry :: {name :: String.t(), age_ms :: non_neg_integer()}

  @typedoc "How many consecutive passes each candidate directory has been unexplained for."
  @type sightings :: %{String.t() => pos_integer()}

  @typedoc """
  One pass's decision: the directories to remove now, the ones still short of a guard, the sightings to
  carry into the next pass, and whether tracking hit its cap (which delays a removal, never causes one).
  """
  @type review :: %{
          ready: [String.t()],
          held: [String.t()],
          sightings: sightings(),
          capped?: boolean()
        }

  # Written by `Malachi.Storage.FormatMarker` at the root of the data directory.
  @marker_names ["malachi.format", "malachi.format.tmp"]
  # Written by `Malachi.DataPlaneRouter.shards/1` when a single node runs more than one data-plane shard.
  @shard_name ~r/\A shard_ \d+ \z/x

  @doc """
  The directory names every segment in `metadata` would occupy under `directory`.

  Every segment, not only the ones whose replica set names this node: a directory here for a segment
  that has been healed onto other brokers is a copy this node was asked to keep until the control plane
  says otherwise, and removing it would race a replica set that is still changing.
  """
  @spec expected(Metadata.t(), Path.t()) :: MapSet.t(String.t())
  def expected(%Metadata{} = metadata, directory) do
    for {segment_id, _segment} <- metadata.segments,
        into: MapSet.new(),
        do: Path.basename(Layout.segment_directory(directory, segment_id))
  end

  @doc """
  One pass's decision over `entries`, given what is `expected` and the `sightings` carried from the
  previous pass.

  ## Options

    * `:min_age_ms` - how old a directory must be before it can be a candidate (required);
    * `:sightings` - consecutive passes a candidate must survive before removal (required);
    * `:max_per_pass` - most directories to remove in one pass (required);
    * `:max_tracked` - most candidates to carry sightings for (required).
  """
  @spec review(MapSet.t(String.t()), [entry()], sightings(), keyword()) :: review()
  def review(expected, entries, sightings, opts) do
    required = Keyword.fetch!(opts, :sightings)
    max_tracked = Keyword.fetch!(opts, :max_tracked)

    candidates =
      for {name, age_ms} <- entries,
          candidate?(expected, name, age_ms, Keyword.fetch!(opts, :min_age_ms)),
          do: name

    # Rebuilt from this pass's candidates rather than updated in place, so a directory the metadata
    # explains again is forgotten by construction and its next unexplained pass starts from one.
    seen = Map.new(candidates, &{&1, Map.get(sightings, &1, 0) + 1})
    {tracked, capped?} = cap(seen, max_tracked)

    {eligible, waiting} =
      candidates
      |> Enum.sort()
      |> Enum.split_with(&(Map.get(tracked, &1, 0) >= required))

    ready = Enum.take(eligible, Keyword.fetch!(opts, :max_per_pass))

    %{
      ready: ready,
      # Everything that is a candidate and is not going this pass, whether it is short of a guard or
      # only over the cap. A candidate that vanished from the report would be a leak nobody can see.
      held: Enum.sort(waiting ++ (eligible -- ready)),
      sightings: tracked,
      capped?: capped?
    }
  end

  defp candidate?(expected, name, age_ms, min_age_ms) do
    age_ms >= min_age_ms and not reserved?(name) and not MapSet.member?(expected, name)
  end

  defp reserved?(name), do: name in @marker_names or Regex.match?(@shard_name, name)

  # Dropping the tail of a sorted set of names only makes the dropped ones start counting again on the
  # next pass, which delays a removal. There is no cap that could cause one.
  defp cap(seen, max_tracked) when map_size(seen) <= max_tracked, do: {seen, false}

  defp cap(seen, max_tracked) do
    kept = seen |> Enum.sort() |> Enum.take(max_tracked) |> Map.new()
    {kept, true}
  end
end
