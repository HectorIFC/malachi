defmodule Mix.Tasks.Malachi.TokensTest do
  # Writes into a tmp_dir and reads Mix.shell messages. Not async only because Mix.shell/1 is global
  # process state.
  use ExUnit.Case, async: false

  import Malachi.TokenFixture, only: [minimal: 0, encode!: 1, put: 3]

  alias Mix.Tasks.Malachi.Tokens

  @moduletag :tmp_dir

  @outputs [
    "assets/src/styles/tokens.css",
    "tui/src/theme/generated.rs",
    "lib/malachi/ui/tokens.ex",
    "docs/design/tokens.snapshot.json"
  ]

  setup %{tmp_dir: root} do
    shell = Mix.shell()
    Mix.shell(Mix.Shell.Process)
    on_exit(fn -> Mix.shell(shell) end)

    File.mkdir_p!(Path.join(root, "docs/design"))
    File.mkdir_p!(Path.join(root, "assets/src"))
    File.mkdir_p!(Path.join(root, "tui/src"))
    write_tokens(root, minimal())
    %{root: root}
  end

  defp write_tokens(root, file), do: File.write!(Path.join(root, "docs/design/design-tokens.json"), encode!(file))

  defp generate(root), do: Tokens.run(["--root", root])
  defp check(root), do: Tokens.run(["--root", root, "--check"])

  defp check_error(root) do
    error = assert_raise Mix.Error, fn -> check(root) end
    error.message
  end

  defp messages do
    receive do
      {:mix_shell, :info, [message]} -> [message | messages()]
    after
      0 -> []
    end
  end

  test "writes the four outputs, and a second run changes nothing", %{root: root} do
    assert generate(root) == :ok
    for path <- @outputs, do: assert(File.exists?(Path.join(root, path)), path)
    assert Enum.sort(messages()) == Enum.sort(Enum.map(@outputs, &"wrote #{&1}"))

    before = Map.new(@outputs, &{&1, File.read!(Path.join(root, &1))})
    assert generate(root) == :ok
    assert Enum.sort(messages()) == Enum.sort(Enum.map(@outputs, &"unchanged #{&1}"))
    assert before == Map.new(@outputs, &{&1, File.read!(Path.join(root, &1))})
  end

  test "--check passes on a tree that matches its token file", %{root: root} do
    generate(root)
    messages()
    assert check(root) == :ok
    assert messages() == ["design tokens: every output matches docs/design/design-tokens.json"]
  end

  test "--check fails on every output a colour edit changes, until it is regenerated", %{root: root} do
    generate(root)
    write_tokens(root, put(minimal(), ["color", "state", "on", "light"], "oklch(0.600 0.165 149.2)"))

    message = check_error(root)
    for path <- @outputs, do: assert(message =~ "#{path}: differs from what docs/design/design-tokens.json generates")

    generate(root)
    assert check(root) == :ok
  end

  test "--check fails on outputs that were never generated", %{root: root} do
    message = check_error(root)
    for path <- @outputs, do: assert(message =~ "#{path}: missing; run mix malachi.tokens")
  end

  test "--check fails on a raw colour literal in a component", %{root: root} do
    generate(root)
    File.mkdir_p!(Path.join(root, "assets/src/components"))
    File.write!(Path.join(root, "assets/src/components/Badge.tsx"), "export const tint = '#ff0000';\n")

    assert check_error(root) =~ "assets/src/components/Badge.tsx:1: raw color literal #ff0000"
  end

  test "--check fails when a scanned directory is missing", %{root: root} do
    generate(root)
    File.rm_rf!(Path.join(root, "tui/src"))
    assert check_error(root) =~ "tui/src: the raw color literal check scans this directory, and it does not exist"
  end

  test "--check reports the contract break, not only the stale file, when a token is deleted from the Rust side",
       %{root: root} do
    generate(root)
    path = Path.join(root, "tui/src/theme/generated.rs")
    File.write!(path, String.replace(File.read!(path), "    pub state_on: Color,\n", ""))

    message = check_error(root)
    assert message =~ "tui/src/theme/generated.rs: differs from"
    assert message =~ "generated.rs Palette: missing field state_on"
  end

  test "--check and a plain run both refuse a token file that fails a gate, and write nothing", %{root: root} do
    write_tokens(root, put(minimal(), ["color", "state", "on", "light"], "oklch(0.900 0.050 149.2)"))

    assert check_error(root) =~ "$contrast: color.state.on on color.base.background in light"

    error = assert_raise Mix.Error, fn -> generate(root) end
    assert error.message =~ "$contrast: color.state.on"
    for path <- @outputs, do: refute(File.exists?(Path.join(root, path)), path)
  end

  test "a missing token file is reported, not crashed on", %{root: root} do
    File.rm!(Path.join(root, "docs/design/design-tokens.json"))
    assert check_error(root) =~ "docs/design/design-tokens.json: cannot be read"
    error = assert_raise Mix.Error, fn -> generate(root) end
    assert error.message =~ "docs/design/design-tokens.json: cannot be read"
  end

  test "an unknown option is refused" do
    assert_raise OptionParser.ParseError, fn -> Tokens.run(["--fix"]) end
  end
end
