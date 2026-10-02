defmodule Malachi.UI.TokenGen do
  @moduledoc """
  One token file, four generated outputs, and the gates that prove the three interfaces share it.

  `docs/design/design-tokens.json` is the single source of every color, type, spacing, density,
  motion and easing value the web console, the terminal interface and any server rendered page use
  (operator-interfaces.md section 4). From it this generates:

  | Output | For |
  |---|---|
  | `assets/src/styles/tokens.css` | The web console (#231) |
  | `tui/src/theme/generated.rs` | The terminal interface (#233) |
  | `lib/malachi/ui/tokens.ex` | `Malachi.UI.Tokens`, any server rendered page |
  | `docs/design/tokens.snapshot.json` | The cross language contract tests |

  The pipeline is `Malachi.UI.TokenGen.Source` (parse and validate the file), then
  `Malachi.UI.TokenGen.Model` (compute every value once), then `Malachi.UI.TokenGen.Gates` (gamut,
  contrast, color vision, terminal distinctness), then `Malachi.UI.TokenGen.Emit`. Nothing is
  written unless every step passes. `check/1` is what CI runs, through `mix malachi.tokens --check`.
  """

  alias Malachi.UI.TokenGen.{Contract, Emit, Gates, Lint, Model, Source}

  @source "docs/design/design-tokens.json"
  @css "assets/src/styles/tokens.css"
  @rust "tui/src/theme/generated.rs"
  @elixir "lib/malachi/ui/tokens.ex"
  @snapshot "docs/design/tokens.snapshot.json"
  @outputs [@css, @rust, @elixir, @snapshot]

  # The interface sources the raw color literal gate scans. Both directories exist from this change on,
  # holding the generated file each one consumes, so a missing one is an error rather than a silent pass.
  @scanned ["assets/src", "tui/src"]

  @doc "The token file, relative to the repository root."
  @spec source() :: String.t()
  def source, do: @source

  @doc "The generated files, relative to the repository root, in the order they are written."
  @spec outputs() :: [String.t()]
  def outputs, do: @outputs

  @doc "Generates every output from the text of a token file, or returns every error that prevents it."
  @spec generate(String.t()) :: {:ok, [{String.t(), String.t()}]} | {:error, [String.t()]}
  def generate(text) do
    with {:ok, source} <- Source.load(text),
         model = Model.build(source),
         [] <- Gates.check(model) do
      {:ok,
       [
         {@css, Emit.css(model)},
         {@rust, Emit.rust(model)},
         {@elixir, Emit.elixir(model)},
         {@snapshot, Emit.snapshot(model)}
       ]}
    else
      {:error, errors} -> {:error, errors}
      errors when is_list(errors) -> {:error, errors}
    end
  end

  @doc """
  Every reason the repository under `root` does not match its token file: an output that is missing
  or differs from what the token file generates, a committed output that breaks the cross language
  contract, and a raw color literal in an interface source. An empty list means the gate passes.
  """
  @spec check(Path.t()) :: [String.t()]
  def check(root) do
    case root |> Path.join(@source) |> File.read() do
      {:ok, text} -> check_generated(root, text)
      {:error, reason} -> ["#{@source}: cannot be read (#{:file.format_error(reason)})"]
    end
  end

  defp check_generated(root, text) do
    case generate(text) do
      {:ok, outputs} ->
        snapshot = outputs |> List.keyfind(@snapshot, 0) |> elem(1) |> Jason.decode!()
        stale(root, outputs) ++ contract(root, snapshot) ++ Enum.map(lint(root), &Lint.describe/1)

      {:error, errors} ->
        errors
    end
  end

  defp stale(root, outputs) do
    for {path, content} <- outputs, message = stale_message(root, path, content), do: message
  end

  defp stale_message(root, path, content) do
    case File.read(Path.join(root, path)) do
      {:ok, ^content} -> nil
      {:ok, _other} -> "#{path}: differs from what #{@source} generates; run mix malachi.tokens and commit the result"
      {:error, _reason} -> "#{path}: missing; run mix malachi.tokens and commit the result"
    end
  end

  defp contract(root, snapshot) do
    for {path, check} <- [{@css, &Contract.css/2}, {@rust, &Contract.rust/2}, {@elixir, &Contract.elixir/2}],
        {:ok, text} <- [File.read(Path.join(root, path))],
        {:error, errors} <- [check.(text, snapshot)],
        error <- errors,
        do: error
  end

  @doc "The raw color literal hits under `root`."
  @spec lint(Path.t()) :: [Lint.hit()]
  def lint(root), do: Lint.scan(root, @scanned, [@css, @rust])
end
