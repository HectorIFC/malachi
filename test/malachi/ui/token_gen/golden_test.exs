defmodule Malachi.UI.TokenGen.GoldenTest do
  @moduledoc """
  Holds the color math to values computed by an independent implementation.

  `test/fixtures/tokens/golden.json` was written by culori from the token file, with the command it
  records. The vectors are frozen on purpose: a change in this module that moves a byte, a contrast
  ratio or a terminal index shows up here, against numbers this code did not produce.
  """
  use ExUnit.Case, async: true

  alias Malachi.UI.TokenGen.{Color, Contrast, Cvd, Model, Quantize, Source, Token}

  @golden "test/fixtures/tokens/golden.json" |> File.read!() |> Jason.decode!()

  # The fixture is frozen, so nothing else would notice it going stale: these two tests tie it to the token
  # file as it is now, and a changed, added, renamed or removed color or pair fails until it is regenerated.
  test "the fixture holds exactly the token file's own colors, in both themes" do
    {:ok, source} = Source.load(File.read!("docs/design/design-tokens.json"))

    expected =
      for %Token{type: :color, themed: true} = token <- source.tokens, theme <- [:light, :dark], into: MapSet.new() do
        {token.path, Atom.to_string(theme), Map.fetch!(token.raw, theme)}
      end

    actual = MapSet.new(@golden["colors"], &{&1["path"], &1["theme"], &1["oklch"]})
    assert MapSet.difference(expected, actual) == MapSet.new(), "colors the fixture lacks or has stale"
    assert MapSet.difference(actual, expected) == MapSet.new(), "colors the fixture has and the token file does not"
  end

  test "the fixture holds exactly the token file's contrast pairs, in both themes, with their resolved colors" do
    {:ok, source} = Source.load(File.read!("docs/design/design-tokens.json"))
    model = Model.build(source)
    value = fn path, theme -> Model.fetch!(model, path).value[String.to_existing_atom(theme)] end

    expected =
      for %{foreground: fg, background: bg} <- source.contrast, theme <- ~w(light dark), into: MapSet.new() do
        {fg, bg, theme, value.(fg, theme), value.(bg, theme)}
      end

    actual =
      MapSet.new(@golden["contrast"], fn pair ->
        {pair["foreground"], pair["background"], pair["theme"], pair["foregroundOklch"], pair["backgroundOklch"]}
      end)

    assert MapSet.difference(expected, actual) == MapSet.new(), "pairs the fixture lacks or has stale"
    assert MapSet.difference(actual, expected) == MapSet.new(), "pairs the fixture has and the token file does not"
  end

  for %{"path" => path, "theme" => theme} = vector <- @golden["colors"] do
    @vector vector
    test "#{path} in #{theme}" do
      vector = @vector
      {:ok, color} = Color.parse(vector["oklch"])
      bytes = Color.to_srgb8(color)

      for {got, want} <- Enum.zip(Tuple.to_list(bytes), vector["srgb8"]) do
        assert abs(got - want) <= 1, "srgb8 #{inspect(bytes)} vs culori #{inspect(vector["srgb8"])}"
      end

      assert_in_delta Color.clip_delta(color), vector["clipDeltaEOK"], 1.0e-3
      assert Quantize.nearest(bytes) == vector["ansi256"]

      {l, a, b} = Cvd.deuteranopia(bytes)
      [wl, wa, wb] = vector["deuteranopiaOklab"]
      assert_in_delta l, wl, 2.0e-3
      assert_in_delta a, wa, 2.0e-3
      assert_in_delta b, wb, 2.0e-3
    end
  end

  test "every contrast ratio matches within a hundredth" do
    for %{"foreground" => fg, "background" => bg, "theme" => theme, "ratio" => want} = pair <- @golden["contrast"] do
      ratio = Contrast.ratio(srgb8(pair["foregroundOklch"]), srgb8(pair["backgroundOklch"]))
      assert_in_delta ratio, want, 0.01, "#{fg} on #{bg} in #{theme}"
    end
  end

  defp srgb8(oklch) do
    {:ok, color} = Color.parse(oklch)
    Color.to_srgb8(color)
  end
end
