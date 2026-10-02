# The deterministic half of the dependency-updates skill: what a dependency update must pass before
# anything runs it, which checks it needs afterwards, and whether the checks that ran add up to a
# verified group. The skill does the judging (changelogs, decisions, proposals); this script does what
# needs no judgement, so the same rule gives the same answer on every run and is pinned by tests.
#
# Usage, from the repository root (plain `elixir`, never `mix`: any mix command reads mix.lock and
# mix.exs as code, and the point of the floor is to look at them before anything runs them):
#
#   elixir scripts/deps_check.exs plan         <base.lock> <new.lock> [<base mix.exs> <new mix.exs>]
#   elixir scripts/deps_check.exs floor-hex    <base.lock> <new.lock> <registry_dir> <elixir_version>
#   elixir scripts/deps_check.exs uses         <workflow.yml>...
#   elixir scripts/deps_check.exs floor-action <base_uses.tsv> <new_uses.tsv> <api_dir>
#   elixir scripts/deps_check.exs verdict      <tier>[,<tier>...] <results.json> <group head sha> <main sha> <group tree sha>
#
# plan          Diffs the two locks and prints, per group, every package that changed or appeared, the
#               tiers it falls in and the gates those tiers require. A package the table does not name
#               takes the tiers of the packages that require it; one nothing requires and the table does
#               not name gets the full suite. With the two mix.exs files, a changed dependency constraint
#               is printed as a DECISION (the contributor decides, the skill never widens one on its
#               own), and any other change to mix.exs is a STOP.
# floor-hex     The supply chain floor for Hex. Every package that changed or appeared must come from
#               Hex under its own name (or an alias the base already had, whose app hex.pm confirms),
#               from the hexpm repository, with the outer checksum hex.pm
#               publishes for that version, the same build tools as before, no retirement, and an Elixir
#               requirement the given version satisfies, and the requirements hex.pm publishes for it.
#               A package that appeared is a STOP on its own: nobody has looked at it yet. The lock must
#               also be whole: a root of the base lock (an entry nothing in it requires) missing from
#               the new one, or a requirement the new lock does not hold, is a STOP, since mix deps.get
#               would resolve either afresh. <registry_dir>/<package>-<version>.json is the body of
#               https://hex.pm/api/packages/<package>/releases/<version>, fetched by the skill under the
#               name the package is published as; a missing one is reported with its URL.
# uses          Every `uses:` line of the given workflows as TSV: file, line, action, ref, and the
#               version comment (`# v7.0.1`) when there is one. The input to floor-action.
# floor-action  The supply chain floor for GitHub Actions, over the lines that differ between the two
#               TSVs. A changed line must be pinned to a full commit SHA with its tag in a comment, the
#               tag must resolve to that SHA (an annotated tag is followed to its commit, and a tag that
#               now resolves elsewhere has moved), and the repository must be the one named, neither a
#               fork nor archived. An action the base never used is a STOP. <api_dir> holds the
#               `gh api` bodies: <owner>__<repo>.repo.json for repos/<owner>/<repo>,
#               <owner>__<repo>__<tag>.ref.json for .../git/ref/tags/<tag>, and <owner>__<repo>__<sha>.tag.json
#               for .../git/tags/<sha> when the ref is an annotated tag.
# verdict       VERIFIED only when every gate the tiers require is in <results.json> as passed, on
#               Linux, with its evidence: a path that exists (a drill's CHAOS_RESULT_FILE, a log), or a
#               GitHub Actions run URL with its `workflow` and the `sha` it ran named, for the gates
#               that workflow runs as blocking steps, and only when that sha is the group head given,
#               which must not be main itself (before the push, a worktree's HEAD is main's commit). A
#               log counts only with the `tree` it ran on, equal to the group tree given. Malachi is measured on Linux only, so a pass anywhere else counts for
#               nothing.
#
# Exit 0 when the answer is ok, 3 when it is a STOP or NOT VERIFIED (the reasons are printed), 2 on a
# usage error. Nothing read from a file is ever evaluated or executed: locks are parsed as literal
# terms only, and a lock that holds anything else is refused.

defmodule DepsCheck do
  @moduledoc false

  # The suites a tier names, so a failure is attributed to what the bump touches. Each is part of
  # `mix test` as well; naming it makes the gate say which callers were checked.
  @auth_suite "mix test test/malachi/auth/jwt_provider_test.exs test/malachi/auth/jwt_validator_test.exs " <>
                "test/malachi/auth/oidc_config_test.exs test/oidc_auth_test.exs"
  @tracing_suite "mix test test/malachi/log_api_tracing_test.exs test/malachi/cluster/scrubber_tracing_test.exs"
  @runtime_suite "mix test test/malachi/wire_test.exs test/dashboard_test.exs test/acl_enforcement_test.exs " <>
                   "test/auth_test.exs test/malachi/telemetry"
  @http_suite "mix test test/malachi/http test/malachi/dashboard test/malachi/console test/dashboard_security_test.exs " <>
                "test/dashboard_security_headers_test.exs test/dashboard_content_length_test.exs " <>
                "test/http_transport_contract_test.exs"

  # The gates, cheapest first: a group stops at its first failure, so the order is the order they run.
  # Every gate a tier names comes from this list, so a gate is spelled once.
  @gate_order [
    "mix deps.unlock --check-unused",
    "mix compile --warnings-as-errors",
    "mix format --check-formatted",
    "mix credo --strict",
    "MIX_ENV=dev mix compile --warnings-as-errors",
    @auth_suite,
    @tracing_suite,
    @runtime_suite,
    @http_suite,
    "mix test",
    "mix coveralls",
    "mix dialyzer",
    "mix docs --warnings-as-errors",
    "mix sobelow --config",
    "mix deps.audit",
    "scripts/docker-static-assets-check.sh",
    "make docker-build docker-validate",
    "mix test --only multinode",
    "scripts/docker-chaos-test.sh",
    "scripts/docker-config-chaos.sh",
    "scripts/docker-reshard-restart-chaos.sh",
    "scripts/docker-storage-chaos.sh",
    "scripts/docker-upgrade-chaos.sh",
    "actionlint",
    "pull request CI"
  ]

  # Every Hex package runs these, whatever its tier.
  @base [
    "mix deps.unlock --check-unused",
    "mix compile --warnings-as-errors",
    "mix format --check-formatted",
    "mix credo --strict",
    "mix test",
    "mix dialyzer",
    "mix docs --warnings-as-errors",
    "mix sobelow --config",
    "mix deps.audit"
  ]

  @drills [
    "scripts/docker-chaos-test.sh",
    "scripts/docker-config-chaos.sh",
    "scripts/docker-reshard-restart-chaos.sh",
    "scripts/docker-storage-chaos.sh",
    "scripts/docker-upgrade-chaos.sh"
  ]

  @actions_gates ["actionlint", "pull request CI"]

  # What each tier adds to the base, and why it is the check that can catch that kind of dependency.
  @tier_gates %{
    # Build and lint tooling: the tool's own command is what a bump can break, plus the dev-only
    # compile, since benchee and ex_doc are compiled only there.
    tool: ["MIX_ENV=dev mix compile --warnings-as-errors", "mix coveralls"],
    # Libraries on every request path: their callers' suites, named so a failure is attributed.
    runtime: [
      @runtime_suite
    ],
    # They parse untrusted bytes from the network: the HTTP, dashboard and console suites, and the
    # static assets the image serves.
    http: [
      @http_suite,
      "scripts/docker-static-assets-check.sh"
    ],
    # Their start order is pinned in mix.exs (the release lists the exporter before the SDK), and a
    # boot race is not something a unit test sees: the image is built from the worktree and booted.
    # Both make targets are named, because scripts/validate-docker-build.sh alone runs whatever image
    # already carries the version tag, which a dependency update does not change.
    observability: [
      @tracing_suite,
      "make docker-build docker-validate"
    ],
    # Login and token validation: the OIDC and JWT suites, and the image built from the worktree and
    # validated, because argon2_elixir is a NIF compiled for Alpine there.
    auth: [
      @auth_suite,
      "make docker-build docker-validate"
    ],
    # Node discovery: the multinode suite and the node fault drill.
    cluster: ["mix test --only multinode", "scripts/docker-chaos-test.sh"],
    # Every control plane store runs on ra, and its on-disk format has to cross versions: the multinode
    # suite and every drill, the upgrade drill above all.
    consensus: ["mix test --only multinode" | @drills],
    # Nobody classified it, so nothing says which checks are enough: every Hex gate there is, derived
    # from the list rather than written out, so a gate added later is in the full suite too.
    full: @gate_order -- @actions_gates,
    # Workflows run on GitHub, not here: a static lint and the pull request's own CI. Workflows the
    # pull request does not trigger stay unverified until dispatched, which the skill reports.
    actions: @actions_gates
  }

  # The gates a run of each workflow proves, because the workflow runs them as blocking steps on Linux.
  # A run URL is evidence for these only, and only with the workflow named beside it. Everything else
  # needs the log of a run: ci.yml runs credo with continue-on-error and never compiles in dev, so a
  # green run says nothing about either, and sobelow and deps.audit run in security.yml, not ci.yml.
  @proven_by %{
    "ci.yml" => [
      "mix deps.unlock --check-unused",
      "mix compile --warnings-as-errors",
      "mix format --check-formatted",
      @auth_suite,
      @tracing_suite,
      @runtime_suite,
      @http_suite,
      "mix test",
      "mix coveralls",
      "mix dialyzer",
      "mix docs --warnings-as-errors",
      "scripts/docker-static-assets-check.sh",
      "mix test --only multinode",
      "pull request CI"
    ],
    "security.yml" => ["mix deps.unlock --check-unused", "mix sobelow --config", "mix deps.audit"],
    "results.yml" => ["scripts/docker-chaos-test.sh"],
    "storage-chaos.yml" => ["scripts/docker-storage-chaos.sh"],
    "upgrade-chaos.yml" => ["scripts/docker-upgrade-chaos.sh"]
  }

  # The tier of every package the table knows. A package missing from here takes its parents' tiers.
  @tiers %{
    "credo" => :tool,
    "dialyxir" => :tool,
    "ex_doc" => :tool,
    "excoveralls" => :tool,
    "mix_audit" => :tool,
    "sobelow" => :tool,
    "benchee" => :tool,
    "benchee_html" => :tool,
    "stream_data" => :tool,
    "jason" => :runtime,
    "telemetry" => :runtime,
    "inet_cidr" => :runtime,
    "bandit" => :http,
    "plug" => :http,
    "opentelemetry" => :observability,
    "opentelemetry_api" => :observability,
    "opentelemetry_exporter" => :observability,
    "joken" => :auth,
    "jose" => :auth,
    "argon2_elixir" => :auth,
    "libcluster" => :cluster,
    "ra" => :consensus,
    "aten" => :consensus,
    "gen_batch_server" => :consensus,
    "seshat" => :consensus
  }

  @sha ~r/\A[0-9a-f]{40}\z/

  def tiers_table, do: @tiers
  def proven_by, do: @proven_by
  def tier_names, do: Map.keys(@tier_gates)

  # -- Gates and groups ------------------------------------------------------------------------------

  @doc "The gates a set of tiers requires, in the order they run."
  def gates_for(tiers) do
    hex? = Enum.any?(tiers, &(&1 != :actions))
    required = MapSet.new(if(hex?, do: @base, else: []) ++ Enum.flat_map(tiers, &Map.fetch!(@tier_gates, &1)))
    Enum.filter(@gate_order, &MapSet.member?(required, &1))
  end

  @doc """
  The group a package goes to, from its tiers. The one place groups are decided, so the Dependabot
  configuration is checked against this and not against a second list.
  """
  def group_for(tiers) do
    cond do
      :actions in tiers -> "actions"
      :consensus in tiers or :full in tiers -> "ra"
      :auth in tiers -> "hex-auth"
      true -> "hex"
    end
  end

  @doc """
  The tiers of a package: its own when the table names it, otherwise the union of the tiers of every
  package in `lock` that requires it, and the full suite when nothing does. A union rather than the
  "highest" tier, because a package shared by plug and joken needs both suites, not either.
  """
  def tiers_of(name, lock, table \\ @tiers), do: tiers_of(name, lock, table, MapSet.new())

  defp tiers_of(name, lock, table, seen) do
    case Map.fetch(table, name) do
      {:ok, tier} ->
        [tier]

      :error ->
        parents = lock |> parents_of(name) |> Enum.reject(&MapSet.member?(seen, &1))

        case parents do
          [] ->
            [:full]

          _ ->
            parents |> Enum.flat_map(&tiers_of(&1, lock, table, MapSet.put(seen, name))) |> Enum.uniq() |> Enum.sort()
        end
    end
  end

  @doc "The packages in `lock` whose requirements name `name`."
  def parents_of(lock, name) do
    for {parent, entry} <- lock, name in requirements(entry), do: parent
  end

  defp requirements({:hex, _name, _version, _inner, _managers, deps, _repo, _outer}),
    do: Enum.map(deps, &elem(&1, 0))

  defp requirements(_other), do: []

  # -- Lock parsing ----------------------------------------------------------------------------------

  @doc """
  Parses a mix.lock without evaluating it. Atoms are kept as `{:atom, name}` while parsing, so a lock
  never creates an atom, and the whole tree has to be a literal before anything reads it: a call, a
  variable or a sigil anywhere makes it `{:error, :not_literal}`.
  """
  def parse_lock(source) do
    opts = [static_atoms_encoder: fn name, _meta -> {:ok, {:atom, name}} end, emit_warnings: false]

    with {:ok, quoted} <- Code.string_to_quoted(source, opts),
         true <- Macro.quoted_literal?(quoted) || {:error, :not_literal},
         {:%{}, _, pairs} <- quoted,
         {:ok, names} <- key_names(pairs) do
      {:ok, Map.new(Enum.zip(names, pairs), fn {name, {_key, value}} -> {name, entry(value)} end)}
    else
      {:error, reason} when reason in [:not_literal, :string_key, :duplicate_key] -> {:error, reason}
      {:error, _reason} -> {:error, :unparsable}
      _other -> {:error, :not_a_map}
    end
  end

  # Mix writes every key as an atom ("name": ...) and looks a dependency up by its atom alone, so a
  # string key ("name" => ...) is an entry Mix never reads, and a name given twice keeps only one of
  # its entries here. Either way the entry checked would not be the entry installed: both are refused.
  defp key_names(pairs) do
    names = Enum.map(pairs, fn {key, _value} -> key end)

    cond do
      Enum.any?(names, &is_binary/1) -> {:error, :string_key}
      Enum.uniq(names) != names -> {:error, :duplicate_key}
      true -> {:ok, Enum.map(names, fn {:atom, name} -> name end)}
    end
  end

  # A Hex entry is {:hex, name, version, inner_checksum, managers, deps, repo, outer_checksum}. Anything
  # else (a git or path dependency) is kept as {:other, term}, which the floor refuses. The term is kept,
  # without its line numbers, so a git dependency pointed at another URL or ref is a change.
  defp entry({:{}, _, [{:atom, "hex"}, {:atom, name}, version, inner, managers, deps, repo, outer]}) do
    {:hex, name, version, inner, Enum.map(managers, &atom_name/1), Enum.map(deps, &dep/1), repo, outer}
  end

  defp entry(other), do: {:other, Macro.prewalk(other, &Macro.update_meta(&1, fn _ -> [] end))}

  defp atom_name({:atom, name}), do: name

  defp dep({:{}, _, [{:atom, name}, requirement, opts]}), do: {name, requirement, opts}

  defp version({:hex, _, version, _, _, _, _, _}), do: version
  defp version({:other, _term}), do: "?"

  @doc "Packages in `new` that changed or appeared, as {name, :changed | :added}, by name."
  def changes(base, new) do
    new
    |> Enum.flat_map(fn {name, entry} ->
      case Map.fetch(base, name) do
        :error -> [{name, :added}]
        {:ok, ^entry} -> []
        {:ok, _old} -> [{name, :changed}]
      end
    end)
    |> Enum.sort()
  end

  # -- plan ------------------------------------------------------------------------------------------

  @doc "The plan as {lines, stop?}."
  def plan(base, new, mix_exs \\ nil) do
    rows =
      for {name, kind} <- changes(base, new) do
        tiers = tiers_of(name, new)
        from = if kind == :added, do: "new", else: version(Map.fetch!(base, name))
        via = if Map.has_key?(@tiers, name), do: "", else: " (via #{via(name, new)})"

        {group_for(tiers),
         "  #{name} #{from} -> #{version(Map.fetch!(new, name))} tiers=#{Enum.join(tiers, ",")}#{via}", tiers}
      end

    removed = for name <- base |> Map.keys() |> Enum.sort(), not Map.has_key?(new, name), do: "REMOVED #{name}"
    {constraint_lines, mix_stop?} = constraint_lines(mix_exs)

    group_lines =
      rows
      |> Enum.group_by(&elem(&1, 0))
      |> Enum.sort()
      |> Enum.flat_map(fn {group, members} ->
        tiers = members |> Enum.flat_map(&elem(&1, 2)) |> Enum.uniq()
        gates = Enum.map(gates_for(tiers), &"    #{&1}")
        # The tiers the verdict takes for this group, printed so the skill passes them as they are.
        verdict = "  verdict tiers: #{tiers |> Enum.sort() |> Enum.join(",")}"
        ["GROUP #{group}"] ++ Enum.map(members, &elem(&1, 1)) ++ [verdict, "  gates:"] ++ gates
      end)

    lines = if rows == [], do: ["NO CHANGES"], else: group_lines
    {lines ++ removed ++ constraint_lines, mix_stop?}
  end

  defp via(name, lock) do
    case parents_of(lock, name) do
      [] -> "nothing requires it"
      parents -> parents |> Enum.sort() |> Enum.join(",")
    end
  end

  @dep_line ~r/\A(\s*\{:)(\w+)(,\s*")([^"]+)(".*)\z/

  @doc """
  Compares two mix.exs sources line by line. A line may differ only in the constraint string of the
  same dependency; that is a DECISION. Any other difference, a line added or removed included, is a
  STOP: a dependency update has no business elsewhere in mix.exs.
  """
  def constraint_lines(nil), do: {[], false}

  def constraint_lines({base_src, new_src}) do
    base_lines = String.split(base_src, "\n")
    new_lines = String.split(new_src, "\n")

    if length(base_lines) != length(new_lines) do
      {["STOP mix.exs: lines were added or removed, not only a constraint changed"], true}
    else
      base_lines
      |> Enum.zip(new_lines)
      |> Enum.with_index(1)
      |> Enum.reject(fn {{a, b}, _} -> a == b end)
      |> Enum.map(fn {{a, b}, n} -> constraint_change(a, b, n) end)
      |> then(fn results -> {Enum.map(results, &elem(&1, 0)), Enum.any?(results, &elem(&1, 1))} end)
    end
  end

  defp constraint_change(a, b, n) do
    case {Regex.run(@dep_line, a), Regex.run(@dep_line, b)} do
      {[_, pre, name, mid, from, rest], [_, pre, name, mid, to, rest]} ->
        # Only a version requirement on both sides is a constraint change: a string holding #{...}
        # is code that runs when mix.exs is read, and it is a STOP like any other change.
        if requirement?(from) and requirement?(to),
          do: {"DECISION mix.exs:#{n} #{name} constraint #{from} -> #{to}", false},
          else: {"STOP mix.exs:#{n}: a change that is not a dependency constraint", true}

      _ ->
        {"STOP mix.exs:#{n}: a change that is not a dependency constraint", true}
    end
  end

  defp requirement?(text), do: match?({:ok, _}, Version.parse_requirement(text))

  # -- floor-hex -------------------------------------------------------------------------------------

  @doc "Every reason the Hex floor refuses a change, as {name, reason}; empty when it passes."
  def floor_hex(base, new, registry, elixir_version) do
    changed =
      Enum.flat_map(changes(base, new), fn {name, kind} ->
        new_entry = Map.fetch!(new, name)
        added = if kind == :added, do: [{name, "new package: nobody has reviewed it"}], else: []
        added ++ hex_reasons(name, new_entry, Map.get(base, name), registry, elixir_version)
      end)

    changed ++ missing_roots(base, new) ++ missing_requirements(new)
  end

  # A root is an entry nothing else in the base lock requires, optionally aside: only mix.exs brings it in. A dependency
  # update has no reason to drop one, and a lock without it (an empty one, say, exported by a package
  # that ran while resolving) would have mix deps.get resolve it afresh, past every check here. A
  # transitive package nothing requires any more may leave; that is the closure check's business.
  defp missing_roots(base, new) do
    # Only a non-optional requirement makes a package something other than a root: Mix locks an
    # optional one only when mix.exs brings it in, and the closure check skips optional requirements.
    required =
      for {_name, {:hex, _, _, _, _, deps, _, _}} <- base,
          {dep, _, opts} <- deps,
          not optional?(opts),
          into: MapSet.new(),
          do: dep

    reasons =
      for name <- Map.keys(base),
          not MapSet.member?(required, name),
          not Map.has_key?(new, name),
          do: {name, "a root of the base lock is gone: an update does not remove what mix.exs depends on"}

    Enum.sort(reasons)
  end

  # A lock whose entries require a package it does not hold is not a lock this floor has seen whole:
  # `mix deps.get` would resolve the missing package from the registry, fetch it and load it, past every
  # check here. Optional requirements are the exception, since Mix locks them only when something uses
  # them.
  defp missing_requirements(lock) do
    reasons =
      for {name, {:hex, _, _, _, _, deps, _, _}} <- lock,
          {dep, _requirement, opts} <- deps,
          not optional?(opts),
          not Map.has_key?(lock, dep),
          do: {name, "requires #{dep}, which the lock does not hold"}

    Enum.sort(reasons)
  end

  defp optional?(opts), do: option(opts, "optional") == true

  defp option(opts, key) do
    Enum.find_value(opts, fn
      {{:atom, ^key}, value} -> value
      _other -> nil
    end)
  end

  # The requirements an entry records, as {package, requirement, optional}, in the shape hex.pm publishes
  # them for the release, so the two can be compared: a lock entry's dependency list is not covered by
  # the tarball's checksum. Spaces are dropped from requirements, since hex.pm keeps a package's own
  # spelling (~>0.3.0) where the lock has Mix's (~> 0.3.0).
  defp locked_requirements(deps) do
    for {dep, requirement, opts} <- deps, into: MapSet.new() do
      package =
        case option(opts, "hex") do
          {:atom, published} -> published
          nil -> dep
        end

      {package, compact(requirement), optional?(opts)}
    end
  end

  defp published_requirements(release) do
    for {package, spec} <- release["requirements"] || %{}, into: MapSet.new() do
      {package, compact(spec["requirement"]), spec["optional"] == true}
    end
  end

  defp compact(requirement) when is_binary(requirement), do: String.replace(requirement, ~r/\s+/, "")
  defp compact(requirement), do: requirement

  defp hex_reasons(name, {:other, _term}, _old, _registry, _elixir),
    do: [{name, "not a Hex package (git or path source)"}]

  # The registry is read under the name the package is published as, which is not always the lock key:
  # chatterbox is published as ts_chatterbox. Such an alias passes only when the base already locked
  # the key that way and hex.pm names the key as the package's app; a new alias is a rename until
  # someone has looked at it.
  defp hex_reasons(name, {:hex, package, version, _inner, managers, deps, repo, outer}, old, registry, elixir) do
    alias_reason =
      package != name && not same_alias?(old, package) &&
        "published as #{package}, locked as #{name}, and the base did not lock it that way"

    registry_reasons =
      case Map.fetch(registry, "#{package}-#{version}") do
        :error ->
          [
            "no registry record for #{package} #{version}: fetch https://hex.pm/api/packages/#{package}/releases/#{version}"
          ]

        {:ok, release} ->
          [
            package != name && get_in(release, ["meta", "app"]) != name &&
              "hex.pm says #{package} #{version} is the app #{get_in(release, ["meta", "app"])}, not #{name}",
            repo != "hexpm" && "repository #{repo}, not hexpm",
            outer != release["checksum"] && "checksum #{outer} does not match hex.pm #{release["checksum"]}",
            managers_changed?(old, managers) && "build tools changed to #{Enum.join(managers, ",")}",
            release["retirement"] != nil && "version #{version} is retired",
            locked_requirements(deps) != published_requirements(release) &&
              "its requirements in the lock are not the ones hex.pm publishes for #{version}",
            elixir_mismatch(release, elixir)
          ]
      end

    [alias_reason | registry_reasons] |> Enum.filter(&is_binary/1) |> Enum.map(&{name, &1})
  end

  defp same_alias?({:hex, _, _, _, _, _, _, _} = old, package), do: elem(old, 1) == package
  defp same_alias?(_old, _package), do: false

  defp managers_changed?({:hex, _, _, _, old_managers, _, _, _}, managers), do: old_managers != managers
  defp managers_changed?(_old, _managers), do: false

  defp elixir_mismatch(%{"meta" => %{"elixir" => requirement}}, elixir) when is_binary(requirement) do
    if Version.match?(elixir, requirement),
      do: false,
      else: "requires Elixir #{requirement}, the project runs #{elixir}"
  end

  defp elixir_mismatch(_release, _elixir), do: false

  # -- uses and floor-action -------------------------------------------------------------------------

  # Any value after `uses:`, quoted or not, so a `docker://image:tag` or a `./local` action, which carry
  # no `@`, becomes a row too: a line the floor never sees is a line the floor passes.
  @uses ~r/\A\s*-?\s*uses:\s*["']?([^\s#"']+)["']?\s*(?:#\s*(\S+))?/

  @doc """
  The `uses:` lines of one workflow, as {file, line, action, ref, comment}. The ref is what follows the
  first `@`, and empty when there is none.
  """
  def uses(file, source) do
    source
    |> String.split("\n")
    |> Enum.with_index(1)
    |> Enum.flat_map(fn {text, n} ->
      case Regex.run(@uses, text) do
        [_, value | comment] ->
          {action, ref} = split_ref(value)
          [{file, n, action, ref, Enum.join(comment)}]

        nil ->
          []
      end
    end)
  end

  defp split_ref(value) do
    case String.split(value, "@", parts: 2) do
      [action, ref] -> {action, ref}
      [action] -> {action, ""}
    end
  end

  def to_tsv(rows), do: Enum.map(rows, fn row -> row |> Tuple.to_list() |> Enum.join("\t") end)

  # The ref and the comment are the last fields and either may be empty (an unpinned line has no
  # comment, a local or docker action no ref), so a line may end in tabs that an editor or a shell
  # strips: a missing field reads as empty.
  def from_tsv(source) do
    for line <- String.split(source, "\n", trim: true) do
      [file, n, action, ref, comment | _] = String.split(line, "\t") ++ ["", ""]
      {file, String.to_integer(n), action, ref, comment}
    end
  end

  @doc """
  Every reason the Actions floor refuses a change, as {location, reason}; empty when it passes. A row
  is a change when its file holds no identical action, ref and comment in the base, so lines that only
  moved because something above them changed are not re-checked.
  """
  def floor_action(base_rows, new_rows, api, dir \\ nil) do
    unchanged = MapSet.new(base_rows, fn {file, _n, action, ref, comment} -> {file, action, ref, comment} end)
    known = MapSet.new(base_rows, fn {_, _, action, _, _} -> repo_of(action) end)

    new_rows
    |> Enum.reject(fn {file, _n, action, ref, comment} -> MapSet.member?(unchanged, {file, action, ref, comment}) end)
    |> Enum.flat_map(fn {file, n, action, ref, comment} ->
      repo = repo_of(action)
      new_action = if MapSet.member?(known, repo), do: [], else: ["an action the base never used"]
      Enum.map(new_action ++ action_reasons(repo, ref, comment, api, dir), &{"#{file}:#{n} #{action}", &1})
    end)
  end

  # owner/repo/path@ref runs owner/repo. A local (./), same-repository ($/) or docker:// action has no
  # repository to check, and is refused as one rather than passed.
  defp repo_of(action) do
    case String.split(action, "/") do
      [owner, repo | _path] when owner not in [".", "..", "$", "docker:"] -> {owner, repo}
      _other -> {:unsupported, action}
    end
  end

  defp action_reasons({:unsupported, _action}, _ref, _comment, _api, _dir), do: ["not an owner/repo action"]

  defp action_reasons({owner, name}, ref, comment, api, dir) do
    repo = "#{owner}/#{name}"

    with true <- Regex.match?(@sha, ref) || :not_pinned,
         true <- comment != "" || :no_tag,
         {:ok, info} <- fetch(api, dir, "#{owner}__#{name}.repo", "repos/#{repo}"),
         {:ok, commit} <- resolve_tag(api, dir, owner, name, comment) do
      [
        info["full_name"] != repo && "repository is now #{info["full_name"]}",
        info["fork"] == true && "repository is a fork",
        info["archived"] == true && "repository is archived",
        commit != ref && "#{comment} resolves to #{commit}, not #{ref}: the tag moved or the pin is wrong"
      ]
      |> Enum.filter(&is_binary/1)
    else
      :not_pinned -> ["not pinned to a full commit SHA"]
      :no_tag -> ["pinned without its tag in a comment (# vX.Y.Z)"]
      {:missing, what} -> ["not fetched: #{what}"]
      {:unresolved, why} -> ["#{comment} did not resolve: #{why}"]
    end
  end

  # A tag that does not exist upstream still leaves a body behind: `gh api ... > file` writes GitHub's
  # error JSON to stdout, so a body with no commit or tag object is an answer (the tag does not
  # resolve), not a crash.
  defp resolve_tag(api, dir, owner, name, tag) do
    with {:ok, ref} <- fetch(api, dir, "#{owner}__#{name}__#{tag}.ref", "repos/#{owner}/#{name}/git/ref/tags/#{tag}") do
      case ref["object"] do
        %{"type" => "commit", "sha" => sha} ->
          {:ok, sha}

        %{"type" => "tag", "sha" => tag_sha} ->
          with {:ok, annotated} <-
                 fetch(api, dir, "#{owner}__#{name}__#{tag_sha}.tag", "repos/#{owner}/#{name}/git/tags/#{tag_sha}") do
            commit_of(annotated)
          end

        _other ->
          {:unresolved, ref["message"] || "no commit or tag object"}
      end
    end
  end

  defp commit_of(%{"object" => %{"type" => "commit", "sha" => sha}}), do: {:ok, sha}
  defp commit_of(body), do: {:unresolved, body["message"] || "the tag object names no commit"}

  # A missing body is reported as the command that fetches it, into the directory the floor reads when
  # it was given one: a bare file name would land wherever the command is run.
  defp fetch(api, dir, file, endpoint) do
    case Map.fetch(api, file) do
      {:ok, body} -> {:ok, body}
      :error -> {:missing, "gh api #{endpoint} > #{if dir, do: Path.join(dir, file), else: file}.json"}
    end
  end

  # -- verdict ---------------------------------------------------------------------------------------

  @doc """
  Every gate the tiers require that the results do not prove, as {gate, reason}. `head` is the commit
  the group branch stands on and `tree` its tree: a run URL proves a gate only for a run of that
  commit, and a log only for a run on that tree, so a gate that passed before the group changed (a
  package taken out, the lock resolved again) proves nothing about it.
  """
  def verdict(tiers, results, {head, tree}) do
    by_gate = Map.new(results, &{&1["gate"], &1})

    Enum.flat_map(gates_for(tiers), fn gate ->
      case Map.fetch(by_gate, gate) do
        :error -> [{gate, "not run"}]
        {:ok, result} -> result |> result_reasons({head, tree}) |> Enum.map(&{gate, &1})
      end
    end)
  end

  defp result_reasons(result, {head, tree}) do
    [
      result["status"] != "pass" && "status #{inspect(result["status"])}, not pass",
      result["host"] != "linux" && "ran on #{inspect(result["host"])}: Malachi is measured on Linux only",
      evidence_reason(result, {head, tree})
    ]
    |> Enum.filter(&is_binary/1)
  end

  defp evidence_reason(%{"evidence" => "https://github.com/" <> rest} = result, {head, _tree}) do
    workflow = result["workflow"]

    cond do
      not (rest =~ ~r{\A[\w.-]+/[\w.-]+/actions/runs/\d+\z}) or not is_binary(workflow) ->
        no_evidence()

      result["gate"] not in Map.get(@proven_by, workflow, []) ->
        "a run of #{workflow} does not prove it: keep the log of a run on Linux"

      result["sha"] != head ->
        "a run of #{result["sha"] || "nil"}, not of the group head #{head}"

      true ->
        false
    end
  end

  defp evidence_reason(%{"evidence" => path} = result, {_head, tree}) when is_binary(path) and path != "" do
    cond do
      not File.exists?(path) -> no_evidence()
      result["tree"] != tree -> "a log of tree #{result["tree"] || "nil"}, not of the group tree #{tree}"
      true -> false
    end
  end

  defp evidence_reason(_result, _head_and_tree), do: no_evidence()

  defp no_evidence, do: "no evidence: a file that exists, or the run URL of a workflow that proves it"

  # -- main ------------------------------------------------------------------------------------------

  @doc "Runs a subcommand and returns {output_lines, exit_status}, so tests read both without halting."
  def run(["plan", base, new]), do: with_locks(base, new, fn b, n -> plan_output(b, n, nil) end)

  def run(["plan", base, new, base_mix, new_mix]) do
    with_locks(base, new, fn b, n -> plan_output(b, n, {File.read!(base_mix), File.read!(new_mix)}) end)
  end

  def run(["floor-hex", base, new, registry_dir, elixir]) do
    with {:ok, _} <- Version.parse(elixir),
         {registry, []} <- read_json_dir(registry_dir) do
      with_locks(base, new, fn b, n -> b |> floor_hex(n, registry, elixir) |> floor_output("hex floor") end)
    else
      :error -> usage()
      {_registry, unreadable} -> floor_output(unreadable, "hex floor")
    end
  end

  def run(["uses" | [_ | _] = files]) do
    {files |> Enum.flat_map(&uses(&1, File.read!(&1))) |> to_tsv(), 0}
  end

  def run(["floor-action", base, new, api_dir]) do
    case read_json_dir(api_dir) do
      {api, []} ->
        from_tsv(File.read!(base))
        |> floor_action(from_tsv(File.read!(new)), api, api_dir)
        |> floor_output("actions floor")

      {_api, unreadable} ->
        floor_output(unreadable, "actions floor")
    end
  end

  def run(["verdict", tiers, results_file, head, main, tree]) do
    names = String.split(tiers, ",")
    known = Enum.map(tier_names(), &Atom.to_string/1)

    with true <- names != [] and Enum.all?(names, &(&1 in known)),
         true <- Regex.match?(@sha, head) and Regex.match?(@sha, main) and Regex.match?(@sha, tree),
         {:ok, %{"gates" => results}} when is_list(results) <- JSON.decode(File.read!(results_file)) do
      verdict_output(tiers, Enum.map(names, &String.to_existing_atom/1), results, {head, tree}, main)
    else
      _ -> usage()
    end
  end

  def run(_argv), do: usage()

  defp usage, do: {["usage: see the header of scripts/deps_check.exs"], 2}

  defp with_locks(base, new, fun) do
    case {parse_lock(File.read!(base)), parse_lock(File.read!(new))} do
      {{:ok, b}, {:ok, n}} -> fun.(b, n)
      {b, n} -> {["STOP a lock is not a literal map: base #{inspect(b)}, new #{inspect(n)}"], 3}
    end
  end

  defp plan_output(base, new, mix_exs) do
    {lines, stop?} = plan(base, new, mix_exs)
    {lines, if(stop?, do: 3, else: 0)}
  end

  # Before the update is committed and pushed, the worktree's HEAD is main itself, and every run of main
  # would then count as a run of the group.
  defp verdict_output(tiers, _names, _results, {main, _tree}, main),
    do:
      {[
         "NOT VERIFIED #{tiers}",
         "  the group head is main itself: commit and push the group branch, then read CI from its runs"
       ], 3}

  defp verdict_output(tiers, names, results, {head, tree}, _main) do
    case verdict(names, results, {head, tree}) do
      [] -> {["VERIFIED #{tiers}"], 0}
      reasons -> {["NOT VERIFIED #{tiers}" | Enum.map(reasons, fn {gate, why} -> "  #{gate}: #{why}" end)], 3}
    end
  end

  defp floor_output([], label), do: {["OK #{label}"], 0}

  defp floor_output(reasons, label),
    do: {["STOP #{label}" | Enum.map(reasons, fn {who, why} -> "  #{who}: #{why}" end)], 3}

  # Every *.json in the directory, keyed by its name without the extension: <name>-<version> for a
  # registry record, <owner>__<repo>... for an API body. A file that is not JSON (a failed `curl -f`
  # leaves an empty one behind its redirect) is returned apart, so the floor stops on it by name.
  defp read_json_dir(dir) do
    dir
    |> File.ls!()
    |> Enum.filter(&String.ends_with?(&1, ".json"))
    |> Enum.sort()
    |> Enum.reduce({%{}, []}, fn file, {bodies, unreadable} ->
      case JSON.decode(File.read!(Path.join(dir, file))) do
        {:ok, body} -> {Map.put(bodies, String.replace_suffix(file, ".json", ""), body), unreadable}
        {:error, _} -> {bodies, unreadable ++ [{file, "not a JSON record, delete it and fetch it again"}]}
      end
    end)
  end

  def main(argv) do
    {lines, status} = run(argv)
    Enum.each(lines, &IO.puts/1)
    System.halt(status)
  end
end

# The script runs its subcommand everywhere except under `mix test`, which requires the file to call
# the functions above and would otherwise be halted on the way in. Mix.env/0 cannot answer that here,
# because the script runs under plain `elixir`, where Mix is not started; the test runner's own server
# can.
unless Process.whereis(ExUnit.Server) do
  DepsCheck.main(System.argv())
end
