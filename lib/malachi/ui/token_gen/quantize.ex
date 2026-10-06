defmodule Malachi.UI.TokenGen.Quantize do
  @moduledoc """
  The 256 color tier of the terminal palette: the nearest xterm index, matched in OKLab.

  Only indexes 16 to 255 are candidates, the 6x6x6 cube and the 24 step gray ramp, whose colors are
  fixed by convention. Indexes 0 to 15 are the terminal theme's own colors, so their appearance is
  unknown at generate time; that tier is declared by hand in `$ansi16` instead.

  Matching is in OKLab rather than by sRGB distance, because sRGB distance is not perceptual: it
  treats a step in blue as large as an equal step in green. A tie keeps the lowest index, so the
  result never depends on enumeration order. The palette is computed once, when this module compiles.
  """

  alias Malachi.UI.TokenGen.Color

  @levels {0, 95, 135, 175, 215, 255}

  @palette (for index <- 16..255 do
              rgb =
                if index < 232 do
                  n = index - 16
                  {elem(@levels, div(n, 36)), elem(@levels, rem(div(n, 6), 6)), elem(@levels, rem(n, 6))}
                else
                  v = 8 + 10 * (index - 232)
                  {v, v, v}
                end

              {index, rgb}
            end)

  @labs Enum.map(@palette, fn {index, rgb} -> {index, Color.srgb8_to_oklab(rgb)} end)

  @doc "The candidate palette, index and sRGB bytes, in index order."
  @spec palette() :: [{16..255, Color.srgb8()}]
  def palette, do: @palette

  @doc "The palette index perceptually nearest to the given color."
  @spec nearest(Color.srgb8()) :: 16..255
  def nearest(rgb) do
    lab = Color.srgb8_to_oklab(rgb)
    {index, _lab} = Enum.min_by(@labs, fn {_index, candidate} -> Color.delta_e_ok(lab, candidate) end)
    index
  end
end
