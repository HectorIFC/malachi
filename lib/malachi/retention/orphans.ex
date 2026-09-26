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

  ## Only a name the layout could have written

  Before the guards, a candidate has to be a name `Malachi.Storage.Layout.segment_directory/2` can
  actually emit: the readable `topic-r<range>-s<segment>` form with a topic from the allowlist that
  function screens with, or a Base64 name that decodes to a term the same function encodes back to that
  exact name. The round trip is a witness, so this is not the reverse mapping ruled out above: it says
  nothing about WHICH segment a name belongs to, only that the layout could have produced it.

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

  # Written by `Malachi.Storage.FormatMarker` at the root of the data directory.
  @marker_names ["malachi.format", "malachi.format.tmp"]
  # Written by `Malachi.DataPlaneRouter.shards/1` when a single node runs more than one data-plane shard.
  @shard_name ~r/\A shard_ \d+ \z/x
  # `Layout.segment_directory/2`'s readable form: a topic from the allowlist that function screens with,
  # then the range and segment sequence numbers. The topic charset holds `-`, so the prefix is greedy and
  # a topic that itself ends in `-r0-s0` still matches, which is the ambiguity that rules out parsing.
  @readable_name ~r/\A [A-Za-z0-9._-]+ -r \d+ -s \d+ \z/x
  # Only the basename of the round trip is compared, so any base does.
  @any_base "/"

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
    age_ms >= min_age_ms and producible?(name) and not reserved?(name) and
      not MapSet.member?(expected, name)
  end

  defp reserved?(name), do: name in @marker_names or Regex.match?(@shard_name, name)

  # See the moduledoc section: a name the layout could have written, in either of its two forms.
  defp producible?(name), do: Regex.match?(@readable_name, name) or encoded_name?(name)

  defp encoded_name?(name) do
    case Base.url_decode64(name, padding: false) do
      {:ok, binary} -> round_trips?(binary, name)
      :error -> false
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
  defp round_trips?(binary, name) do
    term = :erlang.binary_to_term(binary, [:safe])
    Path.basename(Layout.segment_directory(@any_base, term)) == name
  rescue
    ArgumentError -> false
  end

  # Dropping the tail of a sorted set of names only makes the dropped ones start counting again on the
  # next pass, which delays a removal. There is no cap that could cause one.
  defp cap(seen, max_tracked) when map_size(seen) <= max_tracked, do: {seen, false}

  defp cap(seen, max_tracked) do
    kept = seen |> Enum.sort() |> Enum.take(max_tracked) |> Map.new()
    {kept, true}
  end
end
