defmodule DepsCheckTest do
  # scripts/deps_check.exs is what the dependency-updates skill trusts to say whether an update may run
  # at all (the supply chain floor), which checks it needs (the gate table), and whether the checks that
  # ran add up to a verified group. A wrong answer here lets a tampered package or a risky bump through
  # on the cheap checks, so every rule has a test of its own, against real lock entries and real hex.pm
  # and GitHub API bodies (test/fixtures/deps, taken from the queue of 2026-10-01).
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Malachi.Test.TmpDir

  @fixtures Path.expand("../fixtures/deps", __DIR__)
  @base_lock Path.join(@fixtures, "base.lock")
  @new_lock Path.join(@fixtures, "new.lock")
  @registry Path.join(@fixtures, "registry")
  @api Path.join(@fixtures, "api")

  # actions/checkout v7.0.1 is a lightweight tag on this commit; v1 is an annotated tag whose tag
  # object points at the second one.
  @v7 "3d3c42e5aac5ba805825da76410c181273ba90b1"
  @v1_commit "50fbc622fc4ef5163becd7fab6573eac35f8462e"

  # The head of the group branch a verdict is about.
  @head "c27d355ed5f9cc2fd0d8b8d65914b9c0639344ce"
  # The origin/main the group branch was cut from.
  @main "fc1c296000000000000000000000000000000000"

  @drills [
    "scripts/docker-chaos-test.sh",
    "scripts/docker-config-chaos.sh",
    "scripts/docker-reshard-restart-chaos.sh",
    "scripts/docker-storage-chaos.sh",
    "scripts/docker-upgrade-chaos.sh"
  ]

  setup_all do
    # Safe to require: the script only runs a subcommand when no ExUnit server is running.
    Code.require_file("scripts/deps_check.exs")
    :ok
  end

  setup do
    dir = TmpDir.path("deps-check")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    %{dir: dir}
  end

  defp write(dir, name, content) do
    path = Path.join(dir, name)
    File.mkdir_p!(Path.dirname(path))
    File.write!(path, content)
    path
  end

  defp new_lock_with(dir, from, to), do: write(dir, "new.lock", String.replace(File.read!(@new_lock), from, to))

  defp registry_with(dir, file, fun) do
    target = Path.join(dir, "registry")
    File.cp_r!(@registry, target)
    path = Path.join(target, file)
    File.write!(path, path |> File.read!() |> JSON.decode!() |> fun.() |> JSON.encode!())
    target
  end

  defp lock!(source) do
    {:ok, lock} = DepsCheck.parse_lock(source)
    lock
  end

  # A minimal Hex lock entry, with the packages it requires.
  defp entry(name, version, requires \\ []) do
    deps = Enum.map_join(requires, ", ", &~s({:#{&1}, "~> 1.0", [hex: :#{&1}, repo: "hexpm", optional: false]}))
    ~s(  "#{name}": {:hex, :#{name}, "#{version}", "inner", [:mix], [#{deps}], "hexpm", "outer-#{name}-#{version}"},)
  end

  defp lock_source(entries), do: "%{\n" <> Enum.join(entries, "\n") <> "\n}\n"

  describe "parse_lock/1" do
    test "reads every Hex entry of a real lock as a tuple" do
      lock = @base_lock |> File.read!() |> lock!()

      assert {:hex, "ra", "3.1.10", _inner, ["rebar3"], deps, "hexpm", outer} = lock["ra"]
      assert outer == "2476e3b7c71d597456a95694bea66d9d203ac6760a8632ac29ebce86994a9f90"
      assert Enum.map(deps, &elem(&1, 0)) == ["aten", "gen_batch_server", "seshat"]
    end

    test "refuses a lock holding anything but literals, and runs none of it", %{dir: dir} do
      marker = Path.join(dir, "ran")
      source = ~s|%{"x": {:hex, :x, "1.0.0", "i", [:mix], [], "hexpm", System.cmd("touch", ["#{marker}"])}}|

      assert DepsCheck.parse_lock(source) == {:error, :not_literal}

      base = write(dir, "base.lock", lock_source([]))
      new = write(dir, "evil.lock", source)
      assert {["STOP a lock is not a literal map" <> _], 3} = DepsCheck.run(["plan", base, new])
      refute File.exists?(marker)
    end

    test "creates no atom from the names in a lock" do
      name = "never_an_atom_#{System.unique_integer([:positive])}"
      lock_source([entry(name, "1.0.0")]) |> lock!()

      assert_raise ArgumentError, fn -> String.to_existing_atom(name) end
    end

    test "refuses what does not parse, and what is not a map" do
      assert DepsCheck.parse_lock("%{") == {:error, :unparsable}
      assert DepsCheck.parse_lock(~s([1, 2])) == {:error, :not_a_map}
    end

    test "keeps a git or path dependency as :other" do
      lock = lock!(~s(%{"evil": {:git, "https://example.com/evil.git", "abc", []}}))
      assert {:other, _term} = lock["evil"]
    end

    test "refuses a string key, which Mix never writes and never reads, and a name given twice" do
      # Mix looks a dependency up by its atom key, so a string key of the same name could carry the
      # entry the floor checks while Mix installs the other.
      base_plug = ~s({:hex, :plug, "1.20.3", "i", [:mix], [], "hexpm", "o"})
      evil_plug = ~s({:hex, :plug, "1.20.9", "i", [:mix], [], "hexpm", "x"})

      assert DepsCheck.parse_lock(~s(%{:plug => #{evil_plug}, "plug" => #{base_plug}})) == {:error, :string_key}
      assert DepsCheck.parse_lock(~s(%{"plug" => #{base_plug}})) == {:error, :string_key}
      assert DepsCheck.parse_lock(~s(%{"plug": #{evil_plug}, "plug": #{base_plug}})) == {:error, :duplicate_key}
      assert DepsCheck.parse_lock(~s(%{:plug => #{evil_plug}, plug: #{base_plug}})) == {:error, :duplicate_key}
    end
  end

  describe "plan" do
    test "groups the queue of 2026-10-01 the way the skill processes it" do
      assert {lines, 0} = DepsCheck.run(["plan", @base_lock, @new_lock])
      headers = Enum.reject(lines, &String.starts_with?(&1, "    "))

      assert headers == [
               "GROUP hex",
               "  dialyxir 1.4.7 -> 1.4.8 tiers=tool",
               "  erlex 0.2.8 -> 0.2.9 tiers=tool (via dialyxir)",
               "  verdict tiers: tool",
               "  gates:",
               "GROUP hex-auth",
               "  joken 2.6.2 -> 2.7.0 tiers=auth",
               "  verdict tiers: auth",
               "  gates:",
               "GROUP ra",
               "  ra 3.1.10 -> 3.2.0 tiers=consensus",
               "  verdict tiers: consensus",
               "  gates:"
             ]

      ra_gates = lines |> Enum.drop_while(&(&1 != "GROUP ra")) |> Enum.filter(&String.starts_with?(&1, "    "))
      assert Enum.map(ra_gates, &String.trim/1) == DepsCheck.gates_for([:consensus])
    end

    test "gives a package nothing requires and nobody classified the full suite, in the ra group" do
      base = lock!(lock_source([]))
      new = lock!(lock_source([entry("mystery", "1.0.0")]))

      assert DepsCheck.tiers_of("mystery", new) == [:full]
      {lines, false} = DepsCheck.plan(base, new)

      assert "GROUP ra" in lines
      assert "  mystery new -> 1.0.0 tiers=full (via nothing requires it)" in lines
      for drill <- @drills, do: assert("    #{drill}" in lines)
      assert "    make docker-build docker-validate" in lines
    end

    test "gives a package shared by two tiers the gates of both, not the higher one" do
      new =
        lock!(
          lock_source([
            entry("plug", "1.0.0", ["shared"]),
            entry("joken", "1.0.0", ["shared"]),
            entry("shared", "1.0.0")
          ])
        )

      assert DepsCheck.tiers_of("shared", new) == [:auth, :http]
      assert DepsCheck.group_for([:auth, :http]) == "hex-auth"

      {lines, false} = DepsCheck.plan(lock!(lock_source([])), new)
      assert lines |> Enum.drop_while(&(&1 != "GROUP hex-auth")) |> Enum.at(3) == "  verdict tiers: auth,http"

      gates = DepsCheck.gates_for([:auth, :http])
      assert "scripts/docker-static-assets-check.sh" in gates
      assert Enum.any?(gates, &(&1 =~ "oidc_auth_test.exs"))
    end

    test "plans a package that moved to a git source, with no version to show" do
      base = lock!(lock_source([entry("plug", "1.0.0")]))
      new = lock!(~s(%{"plug": {:git, "https://example.com/plug.git", "abc", []}}))

      assert {["GROUP hex", "  plug 1.0.0 -> ? tiers=http", "  verdict tiers: http", "  gates:" | _], false} =
               DepsCheck.plan(base, new)
    end

    test "ends a requirement cycle between unclassified packages at the full suite" do
      new = lock!(lock_source([entry("a", "1.0.0", ["b"]), entry("b", "1.0.0", ["a"])]))
      assert DepsCheck.tiers_of("a", new) == [:full]
    end

    test "says NO CHANGES for identical locks, and lists removed packages" do
      lock = @base_lock |> File.read!() |> lock!()
      assert DepsCheck.plan(lock, lock) == {["NO CHANGES"], false}

      {lines, false} = DepsCheck.plan(lock, Map.delete(lock, "seshat"))
      assert lines == ["NO CHANGES", "REMOVED seshat"]
    end

    test "makes a changed constraint a DECISION and any other mix.exs change a STOP", %{dir: dir} do
      mix = File.read!(Path.join(@fixtures, "base.mix.exs"))
      base_mix = write(dir, "base.mix.exs", mix)
      widened = write(dir, "widened.mix.exs", String.replace(mix, ~s({:joken, "~> 2.6.2"}), ~s({:joken, "~> 2.7.0"})))
      tampered = write(dir, "tampered.mix.exs", String.replace(mix, ~s(elixir: "~> 1.19"), ~s(elixir: "~> 1.0")))
      grown = write(dir, "grown.mix.exs", mix <> "\n# one more line\n")

      assert {lines, 0} = DepsCheck.run(["plan", @base_lock, @new_lock, base_mix, widened])
      assert Enum.any?(lines, &(&1 =~ ~r/^DECISION mix.exs:\d+ joken constraint ~> 2.6.2 -> ~> 2.7.0$/))

      assert {lines, 3} = DepsCheck.run(["plan", @base_lock, @new_lock, base_mix, tampered])
      assert Enum.any?(lines, &(&1 =~ ~r/^STOP mix.exs:\d+: a change that is not a dependency constraint$/))

      assert {lines, 3} = DepsCheck.run(["plan", @base_lock, @new_lock, base_mix, grown])
      assert "STOP mix.exs: lines were added or removed, not only a constraint changed" in lines
    end

    test "code inside a constraint string is a STOP, not a decision" do
      payload = ~S|  {:joken, "#{System.cmd(~s(touch), [~s(/tmp/pwned)]); ~s(~> 2.7.0)}"},|
      {[line], true} = DepsCheck.constraint_lines({~s(  {:joken, "~> 2.6.2"},), payload})
      assert line == "STOP mix.exs:1: a change that is not a dependency constraint"

      {[line], true} = DepsCheck.constraint_lines({~s(  {:joken, "~> 2.6.2"},), ~s(  {:joken, "anything at all"},)})
      assert line =~ "STOP"

      assert {[_], false} = DepsCheck.constraint_lines({~s(  {:ra, "~> 3.1"},), ~s(  {:ra, ">= 3.1.0 and < 4.0.0"},)})
    end

    test "a renamed dependency on the same line is a STOP, not a constraint change" do
      {[line], true} = DepsCheck.constraint_lines({~s(  {:joken, "~> 2.6.2"},), ~s(  {:jokex, "~> 2.6.2"},)})
      assert line =~ "STOP"
    end
  end

  describe "gates" do
    test "run cheapest first, with the drills last and after the multinode suite" do
      gates = DepsCheck.gates_for([:consensus])

      assert hd(gates) == "mix deps.unlock --check-unused"
      assert Enum.take(gates, -5) == @drills
      assert Enum.find_index(gates, &(&1 == "mix test")) < Enum.find_index(gates, &(&1 == "mix test --only multinode"))
    end

    test "build the image from the worktree before validating it, for the tiers that boot it" do
      for tier <- [:auth, :observability, :full] do
        gates = DepsCheck.gates_for([tier])
        assert "make docker-build docker-validate" in gates, "#{tier}"
        refute "scripts/validate-docker-build.sh" in gates, "#{tier}"
      end
    end

    test "for Actions are the lint and the pull request's CI, with no mix gate" do
      assert DepsCheck.gates_for([:actions]) == ["actionlint", "pull request CI"]
      assert DepsCheck.group_for([:actions]) == "actions"
    end

    test "the full suite holds every gate of every Hex tier" do
      full = MapSet.new(DepsCheck.gates_for([:full]))

      for tier <- DepsCheck.tier_names(), tier != :actions do
        assert MapSet.subset?(MapSet.new(DepsCheck.gates_for([tier])), full), "#{tier}"
      end
    end

    property "an unclassified package never loses a gate when one more package requires it" do
      table = DepsCheck.tiers_table()
      names = Map.keys(table)

      check all(
              parents <- uniq_list_of(member_of(names), min_length: 1, max_length: 4),
              extra <- member_of(names),
              extra not in parents
            ) do
        before = lock!(lock_source(Enum.map(parents, &entry(&1, "1.0.0", ["child"])) ++ [entry("child", "1.0.0")]))
        later = Map.merge(before, lock!(lock_source([entry(extra, "1.0.0", ["child"])])))

        gates_before = "child" |> DepsCheck.tiers_of(before) |> DepsCheck.gates_for() |> MapSet.new()
        gates_later = "child" |> DepsCheck.tiers_of(later) |> DepsCheck.gates_for() |> MapSet.new()
        assert MapSet.subset?(gates_before, gates_later)
      end
    end
  end

  describe "floor-hex" do
    test "passes the real updates of 2026-10-01" do
      assert DepsCheck.run(["floor-hex", @base_lock, @new_lock, @registry, "1.19.0"]) == {["OK hex floor"], 0}
    end

    test "stops a checksum hex.pm did not publish (the seeded case)", %{dir: dir} do
      good = "8542b71af2fad6b4e92b98c2092beebb6e51e1cada9d51d6d4bb1cdca3dbcd06"
      bad = "0000b71af2fad6b4e92b98c2092beebb6e51e1cada9d51d6d4bb1cdca3dbcd06"
      new = new_lock_with(dir, good, bad)

      assert {["STOP hex floor", line], 3} = DepsCheck.run(["floor-hex", @base_lock, new, @registry, "1.19.0"])
      assert line == "  ra: checksum #{bad} does not match hex.pm #{good}"
    end

    test "stops a package from another repository", %{dir: dir} do
      new = new_lock_with(dir, ~s("hexpm", "8542b7), ~s("mirror", "8542b7))
      assert {["STOP hex floor", "  ra: repository mirror, not hexpm"], 3} = run_floor(new)
    end

    test "stops a package locked under one name and published under another", %{dir: dir} do
      new = new_lock_with(dir, ~s({:hex, :joken, "2.7.0"), ~s({:hex, :jokken, "2.7.0"))

      assert {["STOP hex floor" | lines], 3} = run_floor(new)

      assert lines == [
               "  joken: published as jokken, locked as joken, and the base did not lock it that way",
               "  joken: no registry record for jokken 2.7.0: fetch https://hex.pm/api/packages/jokken/releases/2.7.0"
             ]
    end

    # chatterbox is published as ts_chatterbox, and grpcbox requires it under that name: a legitimate
    # alias this project's lock has always carried. The real 0.16.0 entry and registry record.
    @chatterbox_016 ~s(  "chatterbox": {:hex, :ts_chatterbox, "0.16.0", ) <>
                      ~s("9d062f566235b6deb5ff94c4a4eb332b7cff28d9ccf281b91de10fe74e1f59fe", [:rebar3], ) <>
                      ~s([{:hpack, "~> 0.3.0", [hex: :hpack_erl, repo: "hexpm", optional: false]}], "hexpm", ) <>
                      ~s("34c145c702f3a8d22f49a189eb34579ef3db68f9a98a82d19b5cf6e390aad54f"},)

    # chatterbox requires hpack, published as hpack_erl; it is the same on both sides, so the floor never
    # reads its registry record, but the lock has to hold it.
    @hpack ~s(  "hpack": {:hex, :hpack_erl, "0.3.0", "inner", [:rebar3], [], "hexpm", "outer"},)

    defp chatterbox_base(package) do
      ~s(  "chatterbox": {:hex, :#{package}, "0.15.1", "inner", [:rebar3], ) <>
        ~s([{:hpack, "~> 0.3.0", [hex: :hpack_erl, repo: "hexpm", optional: false]}], "hexpm", "outer"},)
    end

    test "passes an update of an alias the base already locked, read under its published name", %{dir: dir} do
      base = write(dir, "base.lock", lock_source([chatterbox_base("ts_chatterbox"), @hpack]))
      new = write(dir, "new.lock", lock_source([@chatterbox_016, @hpack]))

      assert DepsCheck.run(["floor-hex", base, new, @registry, "1.19.0"]) == {["OK hex floor"], 0}
    end

    test "stops an alias the base did not have, and one whose registry app is another", %{dir: dir} do
      base = write(dir, "base.lock", lock_source([chatterbox_base("chatterbox"), @hpack]))
      new = write(dir, "new.lock", lock_source([@chatterbox_016, @hpack]))

      assert {["STOP hex floor", line], 3} = DepsCheck.run(["floor-hex", base, new, @registry, "1.19.0"])

      assert line ==
               "  chatterbox: published as ts_chatterbox, locked as chatterbox, and the base did not lock it that way"

      base = write(dir, "base.lock", lock_source([chatterbox_base("ts_chatterbox"), @hpack]))
      registry = registry_with(dir, "ts_chatterbox-0.16.0.json", &put_in(&1, ["meta", "app"], "evil"))

      assert DepsCheck.run(["floor-hex", base, new, registry, "1.19.0"]) ==
               {["STOP hex floor", "  chatterbox: hex.pm says ts_chatterbox 0.16.0 is the app evil, not chatterbox"], 3}
    end

    test "stops new build tools on a package", %{dir: dir} do
      source = File.read!(@new_lock)
      [joken_line] = source |> String.split("\n") |> Enum.filter(&String.starts_with?(&1, ~s(  "joken")))

      new =
        write(
          dir,
          "make.lock",
          String.replace(source, joken_line, String.replace(joken_line, "[:mix]", "[:mix, :make]"))
        )

      assert {["STOP hex floor", "  joken: build tools changed to mix,make"], 3} = run_floor(new)
    end

    test "stops a retired version", %{dir: dir} do
      registry = registry_with(dir, "ra-3.2.0.json", &Map.put(&1, "retirement", %{"reason" => "security"}))

      assert {["STOP hex floor", "  ra: version 3.2.0 is retired"], 3} =
               DepsCheck.run(["floor-hex", @base_lock, @new_lock, registry, "1.19.0"])
    end

    test "stops a version whose Elixir requirement the project does not meet" do
      assert {["STOP hex floor", "  joken: requires Elixir ~> 1.16, the project runs 1.15.0"], 3} =
               DepsCheck.run(["floor-hex", @base_lock, @new_lock, @registry, "1.15.0"])
    end

    test "stops a package nobody has reviewed, and one with no registry record", %{dir: dir} do
      new =
        write(
          dir,
          "new.lock",
          String.replace(File.read!(@new_lock), "%{\n", "%{\n" <> entry("left_pad", "1.0.0") <> "\n")
        )

      assert {["STOP hex floor" | lines], 3} = run_floor(new)

      assert lines == [
               "  left_pad: new package: nobody has reviewed it",
               "  left_pad: no registry record for left_pad 1.0.0: fetch https://hex.pm/api/packages/left_pad/releases/1.0.0"
             ]
    end

    test "stops a git or path source", %{dir: dir} do
      source = File.read!(@new_lock)
      [ra_line] = source |> String.split("\n") |> Enum.filter(&String.starts_with?(&1, ~s(  "ra")))

      new =
        write(
          dir,
          "git.lock",
          String.replace(source, ra_line, ~s(  "ra": {:git, "https://example.com/ra.git", "abc", []},))
        )

      assert {["STOP hex floor", "  ra: not a Hex package (git or path source)"], 3} = run_floor(new)
    end

    test "stops a git dependency that moved to another source, not only one that left Hex", %{dir: dir} do
      base = write(dir, "base.lock", ~s(%{"foo": {:git, "https://github.com/good/foo.git", "aaa", [tag: "v1"]}}))
      new = write(dir, "new.lock", ~s(%{\n  "foo": {:git, "https://github.com/evil/foo.git", "bbb", [tag: "v2"]}\n}))
      same = write(dir, "same.lock", ~s(%{\n\n  "foo": {:git, "https://github.com/good/foo.git", "aaa", [tag: "v1"]}}))

      assert {["STOP hex floor", "  foo: not a Hex package (git or path source)"], 3} =
               DepsCheck.run(["floor-hex", base, new, @registry, "1.19.0"])

      assert {["GROUP ra", "  foo ? -> ? tiers=full (via nothing requires it)" | _], 0} =
               DepsCheck.run(["plan", base, new])

      # The same entry on other lines is not a change: positions are not part of what is compared.
      assert DepsCheck.run(["floor-hex", base, same, @registry, "1.19.0"]) == {["OK hex floor"], 0}
    end

    test "stops on a registry file it cannot read, as a failed curl leaves behind", %{dir: dir} do
      registry = Path.join(dir, "registry")
      File.cp_r!(@registry, registry)
      File.write!(Path.join(registry, "joken-2.7.0.json"), "")

      assert DepsCheck.run(["floor-hex", @base_lock, @new_lock, registry, "1.19.0"]) ==
               {["STOP hex floor", "  joken-2.7.0.json: not a JSON record, delete it and fetch it again"], 3}
    end

    test "stops a lock that requires a package it does not hold, as a forged lock would" do
      # A script run while resolving deletes its own entry and leaves its parent's as published: the
      # parent matches hex.pm, and only the missing entry gives the forgery away.
      base = lock!(lock_source([entry("parent", "1.0.0")]))
      new = lock!(lock_source([entry("parent", "1.0.0", ["hidden"])]))

      assert {"parent", "requires hidden, which the lock does not hold"} in DepsCheck.floor_hex(
               base,
               new,
               %{},
               "1.19.0"
             )
    end

    test "does not ask an optional requirement to be locked" do
      optional =
        ~s(  "parent": {:hex, :parent, "1.0.0", "inner", [:mix], ) <>
          ~s([{:extra, "~> 1.0", [hex: :extra, repo: "hexpm", optional: true]}], "hexpm", "outer"},)

      lock = lock!(lock_source([optional]))
      assert DepsCheck.floor_hex(lock, lock, %{}, "1.19.0") == []
    end

    test "stops an entry whose requirements are not the ones hex.pm publishes for that version", %{dir: dir} do
      new = new_lock_with(dir, ~s([{:jose, "~> 1.11.12", [hex: :jose), ~s([{:jose, "~> 1.11.0", [hex: :jose))

      assert {["STOP hex floor", "  joken: its requirements in the lock are not the ones hex.pm publishes for 2.7.0"],
              3} =
               run_floor(new)
    end

    test "refuses an Elixir version it cannot read" do
      assert {_usage, 2} = DepsCheck.run(["floor-hex", @base_lock, @new_lock, @registry, "1.19"])
    end

    defp run_floor(new), do: DepsCheck.run(["floor-hex", @base_lock, new, @registry, "1.19.0"])
  end

  describe "uses" do
    test "reads every uses: line with its ref and version comment", %{dir: dir} do
      workflow =
        write(dir, "ci.yml", """
        jobs:
          test:
            steps:
              - uses: actions/checkout@v4
              - name: Upload
                uses: github/codeql-action/upload-sarif@#{@v7} # v4.1.0
              # uses: commented/out@v1 is not a step
              - run: echo uses: nothing@here
        """)

      assert {lines, 0} = DepsCheck.run(["uses", workflow])

      assert lines == [
               "#{workflow}\t4\tactions/checkout\tv4\t",
               "#{workflow}\t6\tgithub/codeql-action/upload-sarif\t#{@v7}\tv4.1.0"
             ]
    end

    test "round-trips through TSV, a stripped trailing tab included" do
      rows = [{"a.yml", 3, "actions/checkout", "v4", ""}, {"a.yml", 9, "x/y", @v7, "v1.0.0"}]
      assert rows |> DepsCheck.to_tsv() |> Enum.join("\n") |> DepsCheck.from_tsv() == rows
      assert DepsCheck.from_tsv("a.yml\t3\tactions/checkout\tv4\n") == [{"a.yml", 3, "actions/checkout", "v4", ""}]
      assert DepsCheck.from_tsv("a.yml\t3\t./.github/actions/x\n") == [{"a.yml", 3, "./.github/actions/x", "", ""}]
    end
  end

  describe "floor-action" do
    @base [{"ci.yml", 10, "actions/checkout", "v4", ""}, {"ci.yml", 20, "actions/checkout", "v4", ""}]

    defp action_floor(new, api \\ nil), do: DepsCheck.floor_action(@base, new, api || api_bodies())

    defp api_bodies do
      for file <- File.ls!(@api), into: %{} do
        {String.replace_suffix(file, ".json", ""), JSON.decode!(File.read!(Path.join(@api, file)))}
      end
    end

    test "passes a line pinned to the commit its tag resolves to" do
      assert action_floor([{"ci.yml", 10, "actions/checkout", @v7, "v7.0.1"}]) == []
    end

    test "follows an annotated tag to its commit" do
      assert action_floor([{"ci.yml", 10, "actions/checkout", @v1_commit, "v1"}]) == []
    end

    test "stops a pin that is not the commit the tag resolves to now" do
      moved = String.duplicate("a", 40)

      assert action_floor([{"ci.yml", 10, "actions/checkout", moved, "v7.0.1"}]) == [
               {"ci.yml:10 actions/checkout",
                "v7.0.1 resolves to #{@v7}, not #{moved}: the tag moved or the pin is wrong"}
             ]
    end

    test "stops a changed line left on a tag, or pinned without its tag" do
      assert action_floor([{"ci.yml", 10, "actions/checkout", "v7", ""}]) ==
               [{"ci.yml:10 actions/checkout", "not pinned to a full commit SHA"}]

      assert action_floor([{"ci.yml", 10, "actions/checkout", @v7, ""}]) ==
               [{"ci.yml:10 actions/checkout", "pinned without its tag in a comment (# vX.Y.Z)"}]
    end

    test "stops a fork, an archived repository, and one that now answers under another name" do
      pinned = [{"ci.yml", 10, "actions/checkout", @v7, "v7.0.1"}]
      api = api_bodies()

      for {change, reason} <- [
            {%{"fork" => true}, "repository is a fork"},
            {%{"archived" => true}, "repository is archived"},
            {%{"full_name" => "someone/checkout"}, "repository is now someone/checkout"}
          ] do
        api = Map.update!(api, "actions__checkout.repo", &Map.merge(&1, change))
        assert action_floor(pinned, api) == [{"ci.yml:10 actions/checkout", reason}]
      end
    end

    test "stops an action the base never used, and says what to fetch when a body is missing" do
      assert action_floor([{"ci.yml", 30, "evil/checkout", @v7, "v7.0.1"}]) == [
               {"ci.yml:30 evil/checkout", "an action the base never used"},
               {"ci.yml:30 evil/checkout", "not fetched: gh api repos/evil/checkout > evil__checkout.repo.json"}
             ]
    end

    test "stops a local or docker action, which has no repository to check" do
      assert action_floor([{"ci.yml", 30, "./.github/actions/x", @v7, "v1"}]) == [
               {"ci.yml:30 ./.github/actions/x", "an action the base never used"},
               {"ci.yml:30 ./.github/actions/x", "not an owner/repo action"}
             ]
    end

    test "stops a docker or local action added to a workflow, read by uses as the skill reads it", %{dir: dir} do
      base_wf = write(dir, "base/ci.yml", "    steps:\n      - uses: actions/checkout@v4\n")

      new_wf =
        write(dir, "new/ci.yml", """
            steps:
              - uses: actions/checkout@v4
              - uses: docker://ghcr.io/someone/image:latest
              - uses: ./.github/actions/setup
              - uses: "docker://alpine@sha256:abc"
        """)

      {base_rows, 0} = DepsCheck.run(["uses", base_wf])
      {new_rows, 0} = DepsCheck.run(["uses", new_wf])
      base = write(dir, "base.tsv", Enum.join(base_rows, "\n"))
      new = write(dir, "new.tsv", Enum.join(new_rows, "\n"))

      assert {["STOP actions floor" | reasons], 3} = DepsCheck.run(["floor-action", base, new, @api])

      for action <- ["docker://ghcr.io/someone/image:latest", "./.github/actions/setup", "docker://alpine"] do
        assert Enum.any?(reasons, &(&1 =~ "#{action}: not an owner/repo action")), action
      end
    end

    test "stops a tag that does not resolve, as gh api saves a 404, instead of crashing" do
      not_found = %{"message" => "Not Found", "status" => "404"}
      api = Map.put(api_bodies(), "actions__checkout__v7.0.1.ref", not_found)

      assert action_floor([{"ci.yml", 10, "actions/checkout", @v7, "v7.0.1"}], api) == [
               {"ci.yml:10 actions/checkout", "v7.0.1 did not resolve: Not Found"}
             ]
    end

    test "stops an annotated tag whose tag object names no commit" do
      api =
        Map.put(api_bodies(), "actions__checkout__544eadc6bf3d226fd7a7a9f0dc5b5bf7ca0675b9.tag", %{
          "message" => "Not Found"
        })

      assert action_floor([{"ci.yml", 10, "actions/checkout", @v1_commit, "v1"}], api) == [
               {"ci.yml:10 actions/checkout", "v1 did not resolve: Not Found"}
             ]
    end

    test "stops an annotated tag whose tag object was never fetched, and says what to fetch" do
      api = Map.delete(api_bodies(), "actions__checkout__544eadc6bf3d226fd7a7a9f0dc5b5bf7ca0675b9.tag")

      assert action_floor([{"ci.yml", 10, "actions/checkout", @v1_commit, "v1"}], api) == [
               {"ci.yml:10 actions/checkout",
                "not fetched: gh api repos/actions/checkout/git/tags/544eadc6bf3d226fd7a7a9f0dc5b5bf7ca0675b9 > " <>
                  "actions__checkout__544eadc6bf3d226fd7a7a9f0dc5b5bf7ca0675b9.tag.json"}
             ]
    end

    test "names the file a missing body goes to inside the directory the floor reads", %{dir: dir} do
      api = Path.join(dir, "api")
      File.mkdir_p!(api)
      base = write(dir, "base.tsv", Enum.join(DepsCheck.to_tsv(@base), "\n"))

      new =
        write(dir, "new.tsv", Enum.join(DepsCheck.to_tsv([{"ci.yml", 10, "actions/checkout", @v7, "v7.0.1"}]), "\n"))

      assert DepsCheck.run(["floor-action", base, new, api]) ==
               {[
                  "STOP actions floor",
                  "  ci.yml:10 actions/checkout: not fetched: gh api repos/actions/checkout > #{api}/actions__checkout.repo.json"
                ], 3}
    end

    test "stops on an API body it cannot read, as a failed gh api leaves behind", %{dir: dir} do
      api = Path.join(dir, "api")
      File.cp_r!(@api, api)
      File.write!(Path.join(api, "actions__checkout.repo.json"), "")
      base = write(dir, "base.tsv", Enum.join(DepsCheck.to_tsv(@base), "\n"))

      new =
        write(dir, "new.tsv", Enum.join(DepsCheck.to_tsv([{"ci.yml", 10, "actions/checkout", @v7, "v7.0.1"}]), "\n"))

      assert DepsCheck.run(["floor-action", base, new, api]) ==
               {[
                  "STOP actions floor",
                  "  actions__checkout.repo.json: not a JSON record, delete it and fetch it again"
                ], 3}
    end

    test "does not re-check a line that only moved" do
      assert action_floor([{"ci.yml", 11, "actions/checkout", "v4", ""}, {"ci.yml", 21, "actions/checkout", "v4", ""}]) ==
               []
    end

    test "runs end to end over TSV files", %{dir: dir} do
      base = write(dir, "base.tsv", @base |> DepsCheck.to_tsv() |> Enum.join("\n"))

      good =
        write(dir, "good.tsv", Enum.join(DepsCheck.to_tsv([{"ci.yml", 10, "actions/checkout", @v7, "v7.0.1"}]), "\n"))

      bad = write(dir, "bad.tsv", Enum.join(DepsCheck.to_tsv([{"ci.yml", 10, "actions/checkout", "v7", ""}]), "\n"))

      assert DepsCheck.run(["floor-action", base, good, @api]) == {["OK actions floor"], 0}

      assert DepsCheck.run(["floor-action", base, bad, @api]) ==
               {["STOP actions floor", "  ci.yml:10 actions/checkout: not pinned to a full commit SHA"], 3}
    end
  end

  describe "verdict" do
    defp results(dir, gates, overrides \\ %{}) do
      evidence = write(dir, "evidence/result.json", "{}")

      rows =
        for gate <- gates do
          Map.merge(
            %{"gate" => gate, "status" => "pass", "host" => "linux", "evidence" => evidence},
            Map.get(overrides, gate, %{})
          )
        end

      write(dir, "results.json", JSON.encode!(%{"gates" => rows}))
    end

    test "verifies the ra group only with every gate passed on Linux with evidence", %{dir: dir} do
      file = results(dir, DepsCheck.gates_for([:consensus]))
      assert DepsCheck.run(["verdict", "consensus", file, @head, @main]) == {["VERIFIED consensus"], 0}
    end

    test "refuses the ra group without the upgrade drill", %{dir: dir} do
      file = results(dir, DepsCheck.gates_for([:consensus]) -- ["scripts/docker-upgrade-chaos.sh"])

      assert DepsCheck.run(["verdict", "consensus", file, @head, @main]) ==
               {["NOT VERIFIED consensus", "  scripts/docker-upgrade-chaos.sh: not run"], 3}
    end

    test "refuses an unclassified package with only the cheap checks", %{dir: dir} do
      file = results(dir, DepsCheck.gates_for([:tool]))
      assert {["NOT VERIFIED full" | missing], 3} = DepsCheck.run(["verdict", "full", file, @head, @main])
      assert "  scripts/docker-upgrade-chaos.sh: not run" in missing
      assert "  make docker-build docker-validate: not run" in missing
    end

    test "refuses a pass off Linux, a failure, and a pass with no evidence", %{dir: dir} do
      file =
        results(dir, DepsCheck.gates_for([:actions]), %{
          "actionlint" => %{"host" => "darwin"},
          "pull request CI" => %{"status" => "fail", "evidence" => Path.join(dir, "missing.log")}
        })

      assert DepsCheck.run(["verdict", "actions", file, @head, @main]) ==
               {[
                  "NOT VERIFIED actions",
                  ~s(  actionlint: ran on "darwin": Malachi is measured on Linux only),
                  ~s(  pull request CI: status "fail", not pass),
                  "  pull request CI: no evidence: a file that exists, or the run URL of a workflow that proves it"
                ], 3}
    end

    test "refuses a result with no evidence field at all", %{dir: dir} do
      file = results(dir, DepsCheck.gates_for([:actions]), %{"actionlint" => %{"evidence" => nil}})

      assert DepsCheck.run(["verdict", "actions", file, @head, @main]) ==
               {[
                  "NOT VERIFIED actions",
                  "  actionlint: no evidence: a file that exists, or the run URL of a workflow that proves it"
                ], 3}
    end

    test "accepts a run URL only from a workflow that runs that gate as a blocking step", %{dir: dir} do
      run_url = "https://github.com/HectorIFC/malachi/actions/runs/123456"
      from_ci = %{"evidence" => run_url, "sha" => @head, "workflow" => "ci.yml"}

      ok = results(dir, DepsCheck.gates_for([:actions]), %{"pull request CI" => from_ci})
      assert {["VERIFIED actions"], 0} = DepsCheck.run(["verdict", "actions", ok, @head, @main])

      # The same URL with no workflow named, or one that is not a run, proves nothing.
      for evidence <- [%{"evidence" => run_url}, %{"evidence" => "https://github.com/HectorIFC/malachi/pull/278"}] do
        bad = results(dir, DepsCheck.gates_for([:actions]), %{"pull request CI" => evidence})
        assert {["NOT VERIFIED actions", _], 3} = DepsCheck.run(["verdict", "actions", bad, @head, @main])
      end
    end

    test "refuses a run of another commit, or one that does not say which commit it ran", %{dir: dir} do
      run_url = "https://github.com/HectorIFC/malachi/actions/runs/123456"
      main = String.duplicate("b", 40)

      for {evidence, reason} <- [
            {%{"evidence" => run_url, "workflow" => "ci.yml", "sha" => main},
             "a run of #{main}, not of the group head #{@head}"},
            {%{"evidence" => run_url, "workflow" => "ci.yml"}, "a run of nil, not of the group head #{@head}"}
          ] do
        file = results(dir, DepsCheck.gates_for([:actions]), %{"pull request CI" => evidence})

        assert DepsCheck.run(["verdict", "actions", file, @head, @main]) ==
                 {["NOT VERIFIED actions", "  pull request CI: #{reason}"], 3}
      end
    end

    test "refuses a group head or a main that is not a full commit SHA", %{dir: dir} do
      file = results(dir, DepsCheck.gates_for([:actions]))
      assert {_usage, 2} = DepsCheck.run(["verdict", "actions", file, "main", @main])
      assert {_usage, 2} = DepsCheck.run(["verdict", "actions", file, @head, "main"])
      assert {_usage, 2} = DepsCheck.run(["verdict", "actions", file, @head])
    end

    test "refuses a group head that is main itself, before the update was committed and pushed", %{dir: dir} do
      run_url = "https://github.com/HectorIFC/malachi/actions/runs/1"

      file =
        results(dir, DepsCheck.gates_for([:actions]), %{
          "pull request CI" => %{"evidence" => run_url, "sha" => @main, "workflow" => "ci.yml"}
        })

      assert DepsCheck.run(["verdict", "actions", file, @main, @main]) ==
               {[
                  "NOT VERIFIED actions",
                  "  the group head is main itself: commit and push the group branch, then read CI from its runs"
                ], 3}
    end

    test "refuses CI as evidence for a gate CI runs without failing on it, or never runs", %{dir: dir} do
      run_url = "https://github.com/HectorIFC/malachi/actions/runs/123456"
      # ci.yml runs credo with continue-on-error and never compiles in dev: a green run proves neither.
      overrides =
        for gate <- ["mix credo --strict", "MIX_ENV=dev mix compile --warnings-as-errors"],
            into: %{},
            do: {gate, %{"evidence" => run_url, "sha" => @head, "workflow" => "ci.yml"}}

      file = results(dir, DepsCheck.gates_for([:tool]), overrides)
      assert {["NOT VERIFIED tool" | reasons], 3} = DepsCheck.run(["verdict", "tool", file, @head, @main])

      assert reasons == [
               "  mix credo --strict: a run of ci.yml does not prove it: keep the log of a run on Linux",
               "  MIX_ENV=dev mix compile --warnings-as-errors: a run of ci.yml does not prove it: keep the log of a run on Linux"
             ]
    end

    test "takes sobelow and deps.audit from the security workflow, and drills from their own", %{dir: dir} do
      run_url = "https://github.com/HectorIFC/malachi/actions/runs/7"

      overrides = %{
        "mix sobelow --config" => %{"evidence" => run_url, "sha" => @head, "workflow" => "security.yml"},
        "mix deps.audit" => %{"evidence" => run_url, "sha" => @head, "workflow" => "security.yml"},
        "scripts/docker-upgrade-chaos.sh" => %{"evidence" => run_url, "sha" => @head, "workflow" => "upgrade-chaos.yml"},
        "scripts/docker-chaos-test.sh" => %{"evidence" => run_url, "sha" => @head, "workflow" => "results.yml"},
        "mix test --only multinode" => %{"evidence" => run_url, "sha" => @head, "workflow" => "ci.yml"}
      }

      file = results(dir, DepsCheck.gates_for([:consensus]), overrides)
      assert DepsCheck.run(["verdict", "consensus", file, @head, @main]) == {["VERIFIED consensus"], 0}

      wrong =
        results(dir, DepsCheck.gates_for([:consensus]), %{
          "mix deps.audit" => %{"evidence" => run_url, "sha" => @head, "workflow" => "ci.yml"}
        })

      assert {["NOT VERIFIED consensus", "  mix deps.audit: a run of ci.yml does not prove it" <> _], 3} =
               DepsCheck.run(["verdict", "consensus", wrong, @head, @main])
    end

    test "takes several tiers for a group that mixes them", %{dir: dir} do
      file = results(dir, DepsCheck.gates_for([:auth, :http]))
      assert DepsCheck.run(["verdict", "auth,http", file, @head, @main]) == {["VERIFIED auth,http"], 0}

      assert {["NOT VERIFIED auth,http,consensus" | _], 3} =
               DepsCheck.run(["verdict", "auth,http,consensus", file, @head, @main])
    end

    test "refuses an unknown tier and results that are not a gate list", %{dir: dir} do
      file = results(dir, [])
      assert {_usage, 2} = DepsCheck.run(["verdict", "nonsense", file, @head, @main])
      assert {_usage, 2} = DepsCheck.run(["verdict", "tool", write(dir, "bad.json", ~s({"gates": 1})), @head, @main])
      assert {_usage, 2} = DepsCheck.run(["verdict", "tool", write(dir, "broken.json", "{"), @head, @main])
    end
  end

  test "an unknown subcommand is a usage error" do
    assert DepsCheck.run([]) == {["usage: see the header of scripts/deps_check.exs"], 2}
    assert {_usage, 2} = DepsCheck.run(["uses"])
  end

  test "the script runs from the command line and exits with the status it reports" do
    {out, status} =
      System.cmd("elixir", ["scripts/deps_check.exs", "floor-hex", @base_lock, @new_lock, @registry, "1.19.0"])

    assert {out, status} == {"OK hex floor\n", 0}
  end
end
