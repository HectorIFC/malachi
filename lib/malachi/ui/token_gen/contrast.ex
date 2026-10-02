defmodule Malachi.UI.TokenGen.Contrast do
  @moduledoc """
  WCAG 2.2 contrast between two sRGB colors.

  The ratio is computed on bytes, the color a screen actually shows after the gamut clip, rather than
  on the authored OKLCH: that is what a reader sees and what an accessibility audit would measure.
  """

  alias Malachi.UI.TokenGen.Color

  @doc "Relative luminance, 0 for black and 1 for white."
  @spec luminance(Color.srgb8()) :: float()
  def luminance(rgb) do
    {r, g, b} = Color.srgb8_to_linear(rgb)
    0.2126 * r + 0.7152 * g + 0.0722 * b
  end

  @doc "The contrast ratio, from 1 (identical luminance) to 21 (black on white). Symmetric."
  @spec ratio(Color.srgb8(), Color.srgb8()) :: float()
  def ratio(a, b) do
    la = luminance(a)
    lb = luminance(b)
    (max(la, lb) + 0.05) / (min(la, lb) + 0.05)
  end
end
