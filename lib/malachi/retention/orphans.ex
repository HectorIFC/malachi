defmodule Malachi.Retention.Orphans do
  @moduledoc """
  The decision half of the orphan sweep: which directories under a data directory belong to no segment
  the control plane still lists, and which of those have been unexplained for long enough to remove.

  Pure, so the one rule that matters can be stated as a property: **a directory the owning vnodes
  report as known is never selected, whatever the sequence of operations that produced it.**

  ## Which segments a name can belong to

  `Malachi.Storage.Layout.segment_directory/2` maps a segment id to a directory name, and the readable
  form is ambiguous: a topic may itself contain `-r0-s0`, so `a-r0-s0-r1-s2` is either segment 2 of
  range 1 of topic `a-r0-s0`, or nothing else the layout could have written. `candidate_ids/1` does not
  pick one reading: it returns every segment id the layout would turn into exactly that name, each one
  witnessed by the round trip, and the sweep asks the owner of each. A name is explained when ANY of
  its readings is a segment its owner knows, so an ambiguous name can only be kept by mistake, never
  removed by one.

  The owner is found from the id itself, through `Malachi.Metadata.segment_routing_topic/1`, the same
  routing every segment command takes (`Malachi.Cluster.ReplicatedDSRSM.known_segments/3`). That is
  what lets the sweep ask the vnode that can answer instead of trusting a copy of every vnode's
  metadata. An id its owner does not list is then asked of every vnode, which can only keep it: a
  vnode can hold metadata outside its arc (`docs/ARCHITECTURE.md`).

  ## The three guards, and what each one is for

    * **Age.** A replica creates its directory on the first `follow/4`, after its segment's registration
      has committed on the owning vnode. The minimum age is kept anyway, for the directories no
      registration explains in the first place: a catch-up or a heal that lands after retention deleted
      the segment, which is a real orphan and can wait.
    * **Sightings.** The same directory has to be unexplained on N consecutive passes an interval apart.
      An answer that was briefly wrong explains a directory again on the next pass and the count
      restarts, because the count only survives while the directory keeps being a candidate.
    * **A cap per pass.** A guard against this module rather than against the cluster: if the known set
      ever came out wrong, the operator loses a bounded number of directories and sees the counter move,
      instead of losing the node's data between two ticks.

  Reserved names are excluded before any of that: the data directory's own format marker and, on a
  sharded single-node data plane, the per-shard subdirectories, neither of which is a segment.

  ## Only a name the layout could have written

  Before the guards, a candidate has to be a name `Malachi.Storage.Layout.segment_directory/2` can
  actually emit, which is exactly a name `candidate_ids/1` finds at least one reading for.

  It cannot hide an orphan, because every directory the replication path creates was named by that
  function. What it does is keep the sweep off everything else that can sit under a data directory an
  operator chose: a `ra` data directory nested inside it, a `lost+found`, a copy taken by hand before an
  upgrade. Removal is an `rm_rf` and there is nothing to undo it with.

  Its one failure mode is the safe one. If `term_to_binary` ever encodes the same term differently, an
  older Base64 directory stops round-tripping and is never swept, which leaks disk instead of losing it.
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

  @typedoc """
  Which of the segment ids it is given the control plane lists (`known`), which it could not route to
  an owner (`unroutable`), which a pending split is moving and neither owner listed (`migrating`), and
  which no owner lists but another vnode does (`misplaced`). The shape
  `Malachi.Cluster.ReplicatedDSRSM.known_segments/3` returns.
  """
  @type lookup :: ([Metadata.segment_id()] ->
                     {:ok,
                      %{
                        known: MapSet.t(),
                        unroutable: [Metadata.segment_id()],
                        migrating: [Metadata.segment_id()],
                        misplaced: [Metadata.segment_id()]
                      }}
                     | {:error, term()})

  # Written by `Malachi.Storage.FormatMarker` at the root of the data directory.
  @marker_names ["malachi.format", "malachi.format.tmp"]
  # Written by `Malachi.DataPlaneRouter.shards/1` when a single node runs more than one data-plane shard.
  @shard_name ~r/\A shard_ \d+ \z/x
  # The tail of `Layout.segment_directory/2`'s readable form, from one `-r` to the end of the name. It is
  # tried at every `-r` in the name, but only the last can match: the tail holds digits after its `-r`,
  # never another `-r`, so a topic that itself contains `-r<n>-s<m>` still gives one readable reading.
  @readable_tail ~r/\A -r (\d+) -s (\d+) \z/x
  # Only the basename of the round trip is compared, so any base does.
  @any_base "/"

  @doc """
  Every segment id `Malachi.Storage.Layout.segment_directory/2` would turn into exactly `name`, and
  nothing else: each reading is kept only when it encodes back to `name`.

  Empty for a name the layout could not have written. At most one readable reading (the tail from the
  last `-r`), plus possibly one more when the same characters also decode as Base64.
  """
  @spec candidate_ids(String.t()) :: [Metadata.segment_id()]
  def candidate_ids(name) when is_binary(name) do
    (readable_ids(name) ++ encoded_ids(name))
    |> Enum.filter(&(Path.basename(Layout.segment_directory(@any_base, &1)) == name))
    |> Enum.uniq()
  end

  @doc """
  The names in `entries` worth asking the control plane about (old enough, not reserved, with at least
  one reading, oldest first and at most `:max_tracked` of them) and how many eligible names the cap
  left out.

  The cap bounds the work of one pass on this node on a disk full of leftovers, and with it how many
  owners get asked. It does not bound what an owner sends back: each one answers with its whole segment
  map (`Malachi.Cluster.MetadataServer.segments/2`). A name left out is asked about on a later pass,
  once the older ones are gone, and the count is what lets the caller say so.

  ## Options

    * `:min_age_ms` - how old a directory must be before it can be a candidate (required);
    * `:max_tracked` - most names to return (required).
  """
  @spec candidates([entry()], keyword()) :: {[String.t()], non_neg_integer()}
  def candidates(entries, opts) do
    min_age_ms = Keyword.fetch!(opts, :min_age_ms)

    eligible =
      entries
      |> Enum.filter(fn {name, age_ms} -> eligible?(name, age_ms, min_age_ms) end)
      |> Enum.sort_by(fn {name, age_ms} -> {-age_ms, name} end)

    {asked, left_out} = Enum.split(eligible, Keyword.fetch!(opts, :max_tracked))
    {Enum.map(asked, &elem(&1, 0)), length(left_out)}
  end

  @doc """
  Which of `names` the control plane accounts for, asking `lookup` about every reading of every name.

  `lookup` takes segment ids and answers as `t:lookup/0` describes, or an error. A name with a known
  reading is `known`. Otherwise a name with a reading that is unroutable, migrating or misplaced is
  `undecided`: the caller must keep it this pass, since nobody could say, the split moving it had not
  settled where it lives, or it lives on a vnode that does not own it. Every other name was asked about and is unexplained. An error from `lookup` is
  returned as-is: a partial answer is not an answer.
  """
  @spec explain([String.t()], lookup()) ::
          {:ok, %{known: MapSet.t(String.t()), undecided: MapSet.t(String.t())}} | {:error, term()}
  def explain(names, lookup) when is_function(lookup, 1) do
    readings = Map.new(names, &{&1, candidate_ids(&1)})
    ids = readings |> Map.values() |> List.flatten() |> Enum.uniq()

    with {:ok, %{known: known, unroutable: unroutable, migrating: migrating, misplaced: misplaced}} <- lookup.(ids) do
      open = MapSet.new(unroutable ++ migrating ++ misplaced)
      known? = fn ids -> Enum.any?(ids, &MapSet.member?(known, &1)) end

      {:ok,
       %{
         known: for({name, ids} <- readings, known?.(ids), into: MapSet.new(), do: name),
         undecided:
           for(
             {name, ids} <- readings,
             not known?.(ids),
             Enum.any?(ids, &MapSet.member?(open, &1)),
             into: MapSet.new(),
             do: name
           )
       }}
    end
  end

  @doc """
  The ids among `ids` that `segments` (one vnode's segment map) lists. Runs in the caller, over the map
  `Malachi.Cluster.MetadataServer.segments/2` returned, never inside the vnode's leader.
  """
  @spec known_among(%{Metadata.segment_id() => term()}, [Metadata.segment_id()]) :: MapSet.t()
  def known_among(segments, ids) when is_map(segments) do
    for id <- ids, Map.has_key?(segments, id), into: MapSet.new(), do: id
  end

  @doc """
  One pass's decision over the `unexplained` names, given the `sightings` carried from the previous pass.

  `unexplained` must be names that were ASKED about and that no owner reported as known: the ones
  `candidates/2` returned, minus the known ones. A name that was never asked about is not unexplained,
  it is unknown, and passing it here would count a sighting nobody made.

  ## Options

    * `:sightings` - consecutive passes a candidate must survive before removal (required);
    * `:max_per_pass` - most directories to remove in one pass (required);
    * `:max_tracked` - most candidates to carry sightings for (required).
  """
  @spec review([String.t()], sightings(), keyword()) :: review()
  def review(unexplained, sightings, opts) do
    required = Keyword.fetch!(opts, :sightings)
    max_tracked = Keyword.fetch!(opts, :max_tracked)
    candidates = Enum.uniq(unexplained)

    # Rebuilt from this pass's candidates rather than updated in place, so a directory explained again
    # is forgotten by construction and its next unexplained pass starts from one.
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

  defp eligible?(name, age_ms, min_age_ms) do
    age_ms >= min_age_ms and not reserved?(name) and candidate_ids(name) != []
  end

  defp reserved?(name), do: name in @marker_names or Regex.match?(@shard_name, name)

  # Every `-r` in the name is a place the topic could end. The round trip in `candidate_ids/1` is what
  # drops a reading the layout would not have produced (a topic outside its allowlist, a leading zero).
  defp readable_ids(name) do
    name
    |> :binary.matches("-r")
    |> Enum.flat_map(fn {start, _length} -> readable_id(name, start) end)
  end

  defp readable_id(_name, 0), do: []

  defp readable_id(name, start) do
    case Regex.run(@readable_tail, binary_part(name, start, byte_size(name) - start)) do
      [_tail, range, segment] -> [{{binary_part(name, 0, start), String.to_integer(range)}, String.to_integer(segment)}]
      nil -> []
    end
  end

  defp encoded_ids(name) do
    case Base.url_decode64(name, padding: false) do
      {:ok, binary} -> decode(binary)
      :error -> []
    end
  end

  # `:safe` refuses to create atoms and refuses funs and references, and the input is bounded by a
  # directory name, so a crafted name can neither grow the atom table nor allocate much. A name that is
  # valid Base64 but not a term at all raises here, which is the common case for a directory an operator
  # created, and means the same thing as decoding to a term that encodes back to something else: the
  # layout did not write this name.
  #
  # Sobelow reports `binary_to_term` as high confidence, which is what fails the build under the
  # repository's `exit: "high"` policy, and it is right in the general case. It is not this case: these
  # bytes are the name of a directory on this node's own disk rather than anything off the wire, and a
  # remote peer reaches them only through a segment id this node already accepted and encoded itself.
  # The exception is per site, which is what `.sobelow-conf` enables `skip` for, so the rule keeps
  # failing the build everywhere else, including on the next `binary_to_term` anyone adds.
  # sobelow_skip ["Misc.BinToTerm"]
  defp decode(binary) do
    [:erlang.binary_to_term(binary, [:safe])]
  rescue
    ArgumentError -> []
  end

  # Dropping the tail of a sorted set of names only makes the dropped ones start counting again on the
  # next pass, which delays a removal. There is no cap that could cause one.
  defp cap(seen, max_tracked) when map_size(seen) <= max_tracked, do: {seen, false}

  defp cap(seen, max_tracked) do
    kept = seen |> Enum.sort() |> Enum.take(max_tracked) |> Map.new()
    {kept, true}
  end
end
