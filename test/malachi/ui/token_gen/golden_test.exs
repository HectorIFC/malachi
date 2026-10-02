defmodule Malachi.UI.TokenGen.GoldenTest do
  @moduledoc """
  Holds the color math to values computed by an independent implementation.

  `test/fixtures/tokens/golden.json` was written by culori from the token file, with the command it
  records. The vectors are frozen on purpose: a change in this module that moves a byte, a contrast
  ratio or a terminal index shows up here, against numbers this code did not produce.
  """
  use ExUnit.Case, async: true

  alias Malachi.UI.TokenGen.{Color, Contrast, Cvd, Quantize}

  @golden "test/fixtures/tokens/golden.json" |> File.read!() |> Jason.decode!()

  test "the fixture covers both themes of every color with its own value" do
    assert length(@golden["colors"]) >= 100
    assert length(@golden["contrast"]) >= 60
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
    colors = Map.new(@golden["colors"], &{{&1["path"], &1["theme"]}, &1["oklch"]})

    for %{"foreground" => fg, "background" => bg, "theme" => theme, "ratio" => want} <- @golden["contrast"] do
      ratio = Contrast.ratio(srgb8(colors, fg, theme), srgb8(colors, bg, theme))
      assert_in_delta ratio, want, 0.01, "#{fg} on #{bg} in #{theme}"
    end
  end

  defp srgb8(colors, path, theme) do
    {:ok, color} = Color.parse(Map.fetch!(colors, {path, theme}))
    Color.to_srgb8(color)
  end
end
