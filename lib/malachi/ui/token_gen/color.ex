defmodule Malachi.UI.TokenGen.Color do
  @moduledoc """
  An authored OKLCH color and the conversions the token generator needs from it.

  Tokens are authored in OKLCH, and the web stylesheet and `Malachi.UI.Tokens` carry them as written,
  since both are CSS a browser draws. The terminal palette, the contrast and color vision checks and
  the snapshot's `srgb8` receive sRGB instead. This module is that conversion,
  OKLCH to OKLab to linear sRGB, a clip into the unit cube in linear light, and the sRGB transfer
  function to bytes, with Björn Ottosson's published matrices.

  The clip is deliberately the simple one. A color sRGB cannot show is moved channel by channel to
  the nearest face of the cube, and `clip_delta/1` reports how far in OKLab that moved it, so the
  generator can refuse an authored color that sRGB cannot approximate (`$gamut` in the token file)
  instead of hiding the error inside a gamut mapping search.
  """

  @enforce_keys [:l, :c, :h, :alpha]
  defstruct [:l, :c, :h, :alpha]

  @typedoc "An authored OKLCH color: lightness 0 to 1, chroma 0 to 0.4, hue in degrees, alpha 0 to 1."
  @type t :: %__MODULE__{l: float(), c: float(), h: float(), alpha: float()}

  @typedoc "A point in OKLab: lightness, a, b."
  @type oklab :: {float(), float(), float()}

  @typedoc "Linear light sRGB, nominally 0 to 1 per channel and outside it when out of gamut."
  @type linear :: {float(), float(), float()}

  @typedoc "An sRGB color as the bytes a screen shows."
  @type srgb8 :: {0..255, 0..255, 0..255}

  @max_chroma 0.4

  # The authored form only: lowercase `oklch(`, plain decimals with a leading digit, single spaces, and
  # an optional ` / alpha`. Anything looser would let two spellings of one color into the file.
  @pattern ~r/\Aoklch\((\d(?:\.\d+)?) (\d(?:\.\d+)?) (\d{1,3}(?:\.\d+)?)(?: \/ (\d(?:\.\d+)?))?\)\z/

  @doc """
  Parses an authored `oklch(L C H)` or `oklch(L C H / A)` string.

  Lightness must lie in 0..1, chroma in 0..#{@max_chroma}, hue in 0 up to but excluding 360, and
  alpha in 0..1. Percentages, commas, missing components and any other color syntax are refused.
  """
  @spec parse(String.t()) :: {:ok, t()} | {:error, String.t()}
  def parse(text) when is_binary(text) do
    case Regex.run(@pattern, text, capture: :all_but_first) do
      [l, c, h] -> build(number(l), number(c), number(h), 1.0)
      [l, c, h, alpha] -> build(number(l), number(c), number(h), number(alpha))
      nil -> {:error, "#{inspect(text)} is not an authored oklch(L C H) or oklch(L C H / A) color"}
    end
  end

  defp build(l, _c, _h, _alpha) when l > 1.0, do: {:error, "lightness #{l} is above 1"}
  defp build(_l, c, _h, _alpha) when c > @max_chroma, do: {:error, "chroma #{c} is above #{@max_chroma}"}
  defp build(_l, _c, h, _alpha) when h >= 360.0, do: {:error, "hue #{h} is not below 360"}
  defp build(_l, _c, _h, alpha) when alpha > 1.0, do: {:error, "alpha #{alpha} is above 1"}
  defp build(l, c, h, alpha), do: {:ok, %__MODULE__{l: l, c: c, h: h, alpha: alpha}}

  defp number(text) do
    {value, ""} = Float.parse(text)
    value
  end

  @doc "The color in OKLab."
  @spec to_oklab(t()) :: oklab()
  def to_oklab(%__MODULE__{l: l, c: c, h: h}) do
    radians = h * :math.pi() / 180
    {l, c * :math.cos(radians), c * :math.sin(radians)}
  end

  @doc "OKLab to linear sRGB, unclipped."
  @spec oklab_to_linear_srgb(oklab()) :: linear()
  def oklab_to_linear_srgb({l, a, b}) do
    l_ = cube(l + 0.3963377774 * a + 0.2158037573 * b)
    m_ = cube(l - 0.1055613458 * a - 0.0638541728 * b)
    s_ = cube(l - 0.0894841775 * a - 1.2914855480 * b)

    {
      4.0767416621 * l_ - 3.3077115913 * m_ + 0.2309699292 * s_,
      -1.2684380046 * l_ + 2.6097574011 * m_ - 0.3413193965 * s_,
      -0.0041960863 * l_ - 0.7034186147 * m_ + 1.7076147010 * s_
    }
  end

  @doc "Linear sRGB to OKLab."
  @spec linear_srgb_to_oklab(linear()) :: oklab()
  def linear_srgb_to_oklab({r, g, b}) do
    l_ = cbrt(0.4122214708 * r + 0.5363325363 * g + 0.0514459929 * b)
    m_ = cbrt(0.2119034982 * r + 0.6806995451 * g + 0.1073969566 * b)
    s_ = cbrt(0.0883024619 * r + 0.2817188376 * g + 0.6299787005 * b)

    {
      0.2104542553 * l_ + 0.7936177850 * m_ - 0.0040720468 * s_,
      1.9779984951 * l_ - 2.4285922050 * m_ + 0.4505937099 * s_,
      0.0259040371 * l_ + 0.7827717662 * m_ - 0.8086757660 * s_
    }
  end

  @doc "Clamps every channel of a linear sRGB color into 0..1."
  @spec clip(linear()) :: linear()
  def clip({r, g, b}), do: {clamp(r), clamp(g), clamp(b)}

  @doc "The sRGB transfer function, linear light to the encoded value."
  @spec encode(float()) :: float()
  def encode(x) when x <= 0.0031308, do: 12.92 * x
  def encode(x), do: 1.055 * :math.pow(x, 1 / 2.4) - 0.055

  @doc "The inverse sRGB transfer function, encoded value to linear light."
  @spec decode(float()) :: float()
  def decode(x) when x <= 0.04045, do: x / 12.92
  def decode(x), do: :math.pow((x + 0.055) / 1.055, 2.4)

  @doc "The color as the sRGB bytes a screen shows, after the clip."
  @spec to_srgb8(t()) :: srgb8()
  def to_srgb8(%__MODULE__{} = color), do: color |> to_oklab() |> oklab_to_srgb8()

  @doc "An OKLab point as sRGB bytes, after the clip."
  @spec oklab_to_srgb8(oklab()) :: srgb8()
  def oklab_to_srgb8(lab) do
    {r, g, b} = lab |> oklab_to_linear_srgb() |> clip()
    {byte(r), byte(g), byte(b)}
  end

  @doc "sRGB bytes in linear light."
  @spec srgb8_to_linear(srgb8()) :: linear()
  def srgb8_to_linear({r, g, b}), do: {decode(r / 255), decode(g / 255), decode(b / 255)}

  @doc "sRGB bytes in OKLab."
  @spec srgb8_to_oklab(srgb8()) :: oklab()
  def srgb8_to_oklab(rgb), do: rgb |> srgb8_to_linear() |> linear_srgb_to_oklab()

  @doc "How far, in OKLab, the clip into sRGB moves this color. Zero for a color inside the gamut."
  @spec clip_delta(t()) :: float()
  def clip_delta(%__MODULE__{} = color) do
    lab = to_oklab(color)
    clipped = lab |> oklab_to_linear_srgb() |> clip() |> linear_srgb_to_oklab()
    delta_e_ok(lab, clipped)
  end

  @doc "The Euclidean distance between two OKLab points."
  @spec delta_e_ok(oklab(), oklab()) :: float()
  def delta_e_ok({l1, a1, b1}, {l2, a2, b2}) do
    :math.sqrt((l1 - l2) * (l1 - l2) + (a1 - a2) * (a1 - a2) + (b1 - b2) * (b1 - b2))
  end

  defp byte(x), do: round(encode(x) * 255)

  defp clamp(x) when x < 0.0, do: 0.0
  defp clamp(x) when x > 1.0, do: 1.0
  defp clamp(x), do: x * 1.0

  defp cube(x), do: x * x * x

  # A real cube root: an out of gamut color has negative cone responses, and :math.pow/2 would refuse a
  # negative base with a fractional exponent.
  defp cbrt(x) when x < 0, do: -:math.pow(-x, 1 / 3)
  defp cbrt(x), do: :math.pow(x, 1 / 3)
end
