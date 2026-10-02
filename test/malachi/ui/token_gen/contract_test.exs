defmodule Malachi.UI.TokenGen.ContractTest do
  @moduledoc """
  The cross language contract: each generated file, read the way its own language reads it, holds
  exactly the tokens the snapshot holds, with the same values. Removing a token from one side and
  adding one that the snapshot lacks each fail, for the web stylesheet, the terminal palette and the
  Elixir module alike.
  """
  use ExUnit.Case, async: true

  import Malachi.TokenFixture, only: [minimal: 0, encode!: 1]

  alias Malachi.UI.TokenGen
  alias Malachi.UI.TokenGen.Contract
  alias Malachi.UI.Tokens

  setup_all do
    {:ok, outputs} = TokenGen.generate(encode!(minimal()))
    outputs = Map.new(outputs)

    %{
      css: outputs["assets/src/styles/tokens.css"],
      rust: outputs["tui/src/theme/generated.rs"],
      elixir: outputs["lib/malachi/ui/tokens.ex"],
      snapshot: Jason.decode!(outputs["docs/design/tokens.snapshot.json"])
    }
  end

  defp assert_fails(result, fragment) do
    assert {:error, errors} = result

    assert Enum.any?(errors, &String.contains?(&1, fragment)),
           "expected a contract error containing #{inspect(fragment)}, got:\n" <> Enum.join(errors, "\n")
  end

  describe "the generated files" do
    test "all three agree with the snapshot", context do
      assert Contract.css(context.css, context.snapshot) == :ok
      assert Contract.rust(context.rust, context.snapshot) == :ok
      assert Contract.elixir(context.elixir, context.snapshot) == :ok
    end
  end

  describe "the web stylesheet" do
    test "a token removed from :root", %{css: css, snapshot: snapshot} do
      css = String.replace(css, "  --state-on: oklch(0.627 0.165 149.2);\n", "", global: false)
      assert_fails(Contract.css(css, snapshot), "tokens.css :root: missing --state-on")
    end

    test "a token the snapshot does not have", %{css: css, snapshot: snapshot} do
      css = String.replace(css, ":root {\n", ":root {\n  --state-extra: oklch(0.5 0.1 20);\n", global: false)
      assert_fails(Contract.css(css, snapshot), "tokens.css :root: --state-extra is not in the snapshot")
    end

    test "a value that differs", %{css: css, snapshot: snapshot} do
      css = String.replace(css, "  --space-1: 4px;\n", "  --space-1: 5px;\n", global: false)
      assert_fails(Contract.css(css, snapshot), "tokens.css :root: --space-1 is 5px, the snapshot says 4px")
    end

    test "a dark value, a density class, the reduced motion block and the Tailwind theme", context do
      dark = String.replace(context.css, "  --background: oklch(0.161 0.004 265);\n", "")
      assert_fails(Contract.css(dark, context.snapshot), "tokens.css .dark: missing --background")

      mode = String.replace(context.css, "  --row-height: var(--row-compact);\n", "")
      assert_fails(Contract.css(mode, context.snapshot), "tokens.css .density-compact: missing --row-height")

      motion = String.replace(context.css, "    --motion-fast: 0ms;\n", "")
      assert_fails(Contract.css(motion, context.snapshot), "tokens.css reduced motion: missing --motion-fast")

      theme = String.replace(context.css, "  --color-state-on: var(--state-on);\n", "")
      assert_fails(Contract.css(theme, context.snapshot), "tokens.css @theme inline: missing --color-state-on")
    end
  end

  describe "the terminal palette" do
    test "a field removed from the struct", %{rust: rust, snapshot: snapshot} do
      rust = String.replace(rust, "    pub state_on: Color,\n", "")
      assert_fails(Contract.rust(rust, snapshot), "generated.rs Palette: missing field state_on")
    end

    test "a field the snapshot does not have", %{rust: rust, snapshot: snapshot} do
      rust = String.replace(rust, "pub struct Palette {\n", "pub struct Palette {\n    pub state_extra: Color,\n")
      assert_fails(Contract.rust(rust, snapshot), "generated.rs Palette: state_extra is not a terminal token")
    end

    test "a const removed or added", %{rust: rust, snapshot: snapshot} do
      removed = Regex.replace(~r/pub const ANSI16: Palette = Palette \{.*?\};\n/s, rust, "")
      assert_fails(Contract.rust(removed, snapshot), "generated.rs: missing const ANSI16")

      added = rust <> "\npub const ANSI8: Palette = ANSI16;\n"
      assert_fails(Contract.rust(added, snapshot), "generated.rs: const ANSI8 is not part of the contract")
    end

    test "a value that differs, in a palette or a painted surface", %{rust: rust, snapshot: snapshot} do
      changed = String.replace(rust, "    state_on: Color::LightGreen,\n", "    state_on: Color::Green,\n")

      assert_fails(
        Contract.rust(changed, snapshot),
        "generated.rs ANSI16.state_on is Color::Green, the snapshot says Color::LightGreen"
      )

      painted = String.replace(rust, "    background: Color::Black,\n", "    background: Color::Blue,\n")
      assert_fails(Contract.rust(painted, snapshot), "generated.rs PAINTED_ANSI16_DARK.background is Color::Blue")
    end
  end

  describe "tokens redeclared because they depend on a moded token" do
    setup do
      gap = Malachi.TokenFixture.object([{"$value", "{density.row.height}"}])

      own =
        Malachi.TokenFixture.object([
          {"$value", "{density.row.height}"},
          {"$modes", Malachi.TokenFixture.object([{"compact", "{density.row.default}"}])}
        ])

      file = minimal() |> Malachi.TokenFixture.put(["density", "row", "gap"], gap)
      file = Malachi.TokenFixture.put(file, ["density", "row", "own"], own)
      {:ok, outputs} = TokenGen.generate(encode!(file))
      outputs = Map.new(outputs)

      %{
        dcss: outputs["assets/src/styles/tokens.css"],
        dsnap: Jason.decode!(outputs["docs/design/tokens.snapshot.json"])
      }
    end

    test "the generated stylesheet agrees, own mode values included", %{dcss: css, dsnap: snapshot} do
      assert css =~ "  --row-gap: var(--row-height);\n"
      assert css =~ "  --row-own: var(--row-default);\n"
      assert Contract.css(css, snapshot) == :ok
    end

    test "a dependent redeclaration removed or added fails", %{dcss: css, dsnap: snapshot} do
      removed = String.replace(css, "  --row-gap: var(--row-height);\n", "")
      assert_fails(Contract.css(removed, snapshot), "tokens.css .density-compact: missing --row-gap")

      added = String.replace(css, ".density-compact {\n", ".density-compact {\n  --space-1: var(--space-1);\n")
      assert_fails(Contract.css(added, snapshot), "tokens.css .density-compact: --space-1 is not in the snapshot")
    end
  end

  describe "the shapes the other tests do not break" do
    test "a Surface with a field more or a field less", %{rust: rust, snapshot: snapshot} do
      surface = "pub struct Surface {\n    pub background: Color,\n    pub foreground: Color,\n}"
      assert rust =~ surface

      fewer = String.replace(rust, surface, "pub struct Surface {\n    pub background: Color,\n}")
      assert_fails(Contract.rust(fewer, snapshot), "generated.rs Surface: has exactly background and foreground")

      more =
        String.replace(
          rust,
          surface,
          "pub struct Surface {\n    pub background: Color,\n    pub foreground: Color,\n    pub border: Color,\n}"
        )

      assert_fails(Contract.rust(more, snapshot), "generated.rs Surface: has exactly background and foreground")
    end

    test "a palette struct that is missing altogether", %{rust: rust, snapshot: snapshot} do
      gone = Regex.replace(~r/pub struct Palette \{.*?\}\n/s, rust, "")
      assert_fails(Contract.rust(gone, snapshot), "generated.rs Palette: missing field state_on")
    end

    test "one field dropped from a palette that still exists", %{rust: rust, snapshot: snapshot} do
      [head, dark] = String.split(rust, "pub const TRUECOLOR_DARK: Palette = Palette {\n", parts: 2)
      dark = Regex.replace(~r/    state_on: Color::Rgb\([0-9, ]+\),\n/, dark, "", global: false)
      changed = head <> "pub const TRUECOLOR_DARK: Palette = Palette {\n" <> dark
      assert_fails(Contract.rust(changed, snapshot), "generated.rs TRUECOLOR_DARK: missing state_on")
    end

    test "a block that is never closed, and text that is not a block", %{css: css, snapshot: snapshot} do
      assert_fails(
        Contract.css(css <> "\n.extra {\n  --x: 1;\n", snapshot),
        "tokens.css: the block .extra is never closed"
      )

      assert_fails(Contract.css(css <> "\nstray text\n", snapshot), "tokens.css: unreadable text")
    end
  end

  describe "the Elixir module" do
    test "an entry removed or added", %{elixir: elixir, snapshot: snapshot} do
      removed = Regex.replace(~r/\s*"state-on" => %\{[^}]*\},/, elixir, "", global: false)
      assert_fails(Contract.elixir(removed, snapshot), "tokens.ex: missing state-on")

      added =
        String.replace(elixir, "@tokens %{", ~s(@tokens %{"state-extra" => %{type: :color, light: "x", dark: "x"}, ),
          global: false
        )

      assert_fails(Contract.elixir(added, snapshot), "tokens.ex: state-extra is not in the snapshot")
    end

    test "a value that differs, and a module the contract cannot read", %{elixir: elixir, snapshot: snapshot} do
      changed = String.replace(elixir, ~s(light: "4px"), ~s(light: "5px"), global: false)
      assert_fails(Contract.elixir(changed, snapshot), "tokens.ex: space-1 light is \"5px\", the snapshot says \"4px\"")

      assert_fails(Contract.elixir("defmodule X do end", snapshot), "tokens.ex: no @tokens literal")
      assert_fails(Contract.elixir("defmodule X do", snapshot), "tokens.ex: does not parse")
    end
  end

  describe "the committed outputs" do
    test "the compiled Malachi.UI.Tokens agrees with the committed snapshot" do
      snapshot = "docs/design/tokens.snapshot.json" |> File.read!() |> Jason.decode!()
      assert Contract.elixir_module(Tokens, snapshot) == :ok
      assert {:ok, "oklch(0.985 0.002 265)"} = Tokens.get("background", :light)
      assert Tokens.get("not-a-token", :dark) == :error
      assert hd(Tokens.names()) == "background"
    end

    test "the committed stylesheet and palette agree with the committed snapshot" do
      snapshot = "docs/design/tokens.snapshot.json" |> File.read!() |> Jason.decode!()
      assert Contract.css(File.read!("assets/src/styles/tokens.css"), snapshot) == :ok
      assert Contract.rust(File.read!("tui/src/theme/generated.rs"), snapshot) == :ok
      assert Contract.elixir(File.read!("lib/malachi/ui/tokens.ex"), snapshot) == :ok
    end
  end
end
