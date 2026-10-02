defmodule Mix.Tasks.Malachi.Tokens do
  @shortdoc "Generates the design token outputs from docs/design/design-tokens.json, or checks them"

  @moduledoc """
  #{@shortdoc}.

      mix malachi.tokens          # write the four generated files
      mix malachi.tokens --check  # fail unless they match the token file, as CI does

  `docs/design/design-tokens.json` is the one place the web console, the terminal interface and any
  server rendered page take a color, a size or a duration from. This task generates, from it:

    * `assets/src/styles/tokens.css`, the web console's custom properties;
    * `tui/src/theme/generated.rs`, the terminal palette in truecolor, 256 and 16 colors;
    * `lib/malachi/ui/tokens.ex`, `Malachi.UI.Tokens`;
    * `docs/design/tokens.snapshot.json`, the flat view the contract tests compare the others against.

  Nothing is written when the token file breaks a rule or fails a gate (contrast, color vision,
  gamut, terminal distinctness); every reason is listed instead. See `Malachi.UI.TokenGen`.

  `--check` regenerates in memory and fails on any output that is missing or differs from what the
  token file produces, on a committed output that breaks the cross language contract, and on a raw
  color literal anywhere in `assets/src` or `tui/src` outside the generated files. The fix for the
  first two is to run the task without `--check` and commit the result; for the third, use a token.

  `--root` points the task at another checkout. It exists for the tests, which run it against a
  scratch tree; everywhere else the task runs from the repository root without it.
  """

  use Mix.Task

  alias Malachi.UI.TokenGen

  @switches [check: :boolean, root: :string]

  @impl Mix.Task
  def run(argv) do
    {opts, _rest} = OptionParser.parse!(argv, strict: @switches)
    root = Keyword.get(opts, :root, File.cwd!())

    if Keyword.get(opts, :check, false), do: check(root), else: write(root)
  end

  defp check(root) do
    case TokenGen.check(root) do
      [] ->
        Mix.shell().info("design tokens: every output matches #{TokenGen.source()}")
        :ok

      errors ->
        Mix.raise("design tokens do not match #{TokenGen.source()}:\n\n" <> Enum.map_join(errors, "\n", &("  " <> &1)))
    end
  end

  defp write(root) do
    text = read_source(root)

    case TokenGen.generate(text) do
      {:ok, outputs} ->
        Enum.each(outputs, fn {path, content} -> write_output(root, path, content) end)

      {:error, errors} ->
        Mix.raise("#{TokenGen.source()} cannot be generated:\n\n" <> Enum.map_join(errors, "\n", &("  " <> &1)))
    end
  end

  defp read_source(root) do
    case root |> Path.join(TokenGen.source()) |> File.read() do
      {:ok, text} -> text
      {:error, reason} -> Mix.raise("#{TokenGen.source()}: cannot be read (#{:file.format_error(reason)})")
    end
  end

  defp write_output(root, path, content) do
    full = Path.join(root, path)

    if File.read(full) == {:ok, content} do
      Mix.shell().info("unchanged #{path}")
    else
      File.mkdir_p!(Path.dirname(full))
      File.write!(full, content)
      Mix.shell().info("wrote #{path}")
    end
  end
end
