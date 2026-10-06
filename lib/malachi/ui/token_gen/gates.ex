defmodule Malachi.UI.TokenGen.Gates do
  @moduledoc """
  The checks on what the tokens look like, as opposed to how the file is written.

    * **Gamut.** Every authored color is clipped into sRGB for the consumers that take sRGB (the
      terminal palette, these checks, the snapshot's `srgb8`), and a clip that moves it further than `$gamut.maxDeltaEOK` in OKLab fails: sRGB cannot show what was
      authored, so the authored value is wrong.
    * **Contrast.** Every pair in `$contrast`, in both themes, against its own threshold, on the
      bytes a screen shows. A pair with alpha on either side is refused rather than composited,
      because its contrast depends on what it is drawn over.
    * **Apart.** Every member of each `$apart` set's `these` stays at least `$apart.minDeltaEOK`
      from every member of its `from`, in OKLab, in both themes, so a color with one meaning (a
      cluster's identity) never looks like a color with another (a state).
    * **Color vision.** Every two members of a `$cvd` set, in both themes, simulated for a
      deuteranope, must stay at least `$cvd.minDeltaEOK` apart in OKLab.
    * **Terminal distinctness.** Every two tokens in `$terminal.distinct` land on different 256 color
      indexes in each theme. When quantisation puts two on one index, the fix is declared by hand in
      `$ansi256`; this module never picks a second choice. A declaration equal to what quantisation
      picks anyway is refused, so the file only carries the exceptions it needs. The sixteen color
      tier is checked by `Malachi.UI.TokenGen.Source`, where it is declared.
  """

  alias Malachi.UI.TokenGen.{Color, Contrast, Cvd, Model}

  @themes [:light, :dark]

  @doc "Runs every gate. An empty list means the tokens pass."
  @spec check(Model.t()) :: [String.t()]
  def check(%Model{} = model) do
    gamut(model) ++ contrast(model) ++ cvd(model) ++ apart(model) ++ distinct(model) ++ redundant(model)
  end

  defp gamut(%Model{entries: entries, source: source}) do
    max = source.gamut.max

    for %{type: :color, themed: true} = entry <- entries,
        theme <- @themes,
        delta = entry.color[theme].clip_delta,
        delta > max do
      "$gamut: #{entry.path} in #{theme} moves #{fixed(delta, 3)} in OKLab when clipped to sRGB, above #{max}"
    end
  end

  defp contrast(%Model{source: source} = model) do
    Enum.flat_map(source.contrast, fn %{foreground: fg, background: bg, min: min} ->
      foreground = Model.fetch!(model, fg)
      background = Model.fetch!(model, bg)

      if Enum.any?(@themes, &(foreground.color[&1].alpha < 1.0 or background.color[&1].alpha < 1.0)) do
        [
          "$contrast: #{fg} on #{bg} carries alpha, so its contrast depends on what it is drawn over; pair opaque tokens"
        ]
      else
        for theme <- @themes,
            ratio = Contrast.ratio(foreground.color[theme].srgb8, background.color[theme].srgb8),
            ratio < min do
          "$contrast: #{fg} on #{bg} in #{theme} is #{fixed(ratio, 2)}, below #{Model.number(min)}"
        end
      end
    end)
  end

  defp cvd(%Model{source: source} = model) do
    min = source.cvd.min

    for set <- source.cvd.sets,
        {a, i} <- Enum.with_index(set),
        b <- Enum.drop(set, i + 1),
        theme <- @themes,
        distance = cvd_distance(model, a, b, theme),
        distance < min do
      "$cvd: #{a} and #{b} in #{theme} are #{fixed(distance, 3)} apart for a deuteranope, below #{min}"
    end
  end

  defp apart(%Model{source: source} = model) do
    min = source.apart.min

    for %{these: these, from: from} <- source.apart.sets,
        a <- these,
        b <- from,
        theme <- @themes,
        distance = oklab_distance(model, a, b, theme),
        distance < min do
      "$apart: #{a} and #{b} in #{theme} are #{fixed(distance, 3)} apart, below #{min}"
    end
  end

  defp oklab_distance(model, a, b, theme) do
    Color.delta_e_ok(
      Color.srgb8_to_oklab(Model.fetch!(model, a).color[theme].srgb8),
      Color.srgb8_to_oklab(Model.fetch!(model, b).color[theme].srgb8)
    )
  end

  defp cvd_distance(model, a, b, theme) do
    Color.delta_e_ok(
      Cvd.deuteranopia(Model.fetch!(model, a).color[theme].srgb8),
      Cvd.deuteranopia(Model.fetch!(model, b).color[theme].srgb8)
    )
  end

  defp distinct(%Model{source: source} = model) do
    entries = Enum.map(source.terminal.distinct, &Model.fetch!(model, &1))

    for theme <- @themes,
        {a, i} <- Enum.with_index(entries),
        b <- Enum.drop(entries, i + 1),
        a.ansi256[theme] == b.ansi256[theme] do
      "$ansi256: #{a.path} and #{b.path} in #{theme} both quantise to #{a.ansi256[theme]}; " <>
        "declare one of them by hand in $ansi256.#{theme}"
    end
  end

  defp redundant(%Model{source: source} = model) do
    for theme <- @themes,
        {path, index} <- Enum.sort(source.ansi256[theme]),
        Model.fetch!(model, path).quantised[theme] == index do
      "$ansi256.#{theme}: #{path} declares #{index}, which quantisation picks anyway; remove the declaration"
    end
  end

  defp fixed(value, decimals), do: :erlang.float_to_binary(value * 1.0, decimals: decimals)
end
