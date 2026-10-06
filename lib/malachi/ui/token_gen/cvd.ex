defmodule Malachi.UI.TokenGen.Cvd do
  @moduledoc """
  Color vision deficiency simulation, for the distinguishability check in `$cvd`.

  Deuteranopia is simulated with the Machado, Oliveira and Fernandes (2009) matrix at severity 1.0,
  applied in linear sRGB, then clipped. The generator compares the simulated colors in OKLab, so a
  set of colors that only differ along the red to green axis is caught.
  """

  alias Malachi.UI.TokenGen.Color

  @deuteranopia {
    {0.367322, 0.860646, -0.227968},
    {0.280085, 0.672501, 0.047413},
    {-0.011820, 0.042940, 0.968881}
  }

  @doc "How the color appears to a deuteranope, in OKLab."
  @spec deuteranopia(Color.srgb8()) :: Color.oklab()
  def deuteranopia(rgb) do
    {r, g, b} = Color.srgb8_to_linear(rgb)
    {row1, row2, row3} = @deuteranopia

    {apply_row(row1, r, g, b), apply_row(row2, r, g, b), apply_row(row3, r, g, b)}
    |> Color.clip()
    |> Color.linear_srgb_to_oklab()
  end

  defp apply_row({x, y, z}, r, g, b), do: x * r + y * g + z * b
end
