defmodule Malachi.UI.TokenGen.ColorTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Malachi.UI.TokenGen.Color

  # The sRGB primaries and white in OKLab, as published with the space by Björn Ottosson (2020) and
  # reproduced in the CSS Color 4 sample code: red is oklch(62.8% 0.2577 29.23), green
  # oklch(86.64% 0.2948 142.5), blue oklch(45.2% 0.3132 264.05).
  @published [
    {{1.0, 1.0, 1.0}, {1.0, 0.0, 0.0}},
    {{0.0, 0.0, 0.0}, {0.0, 0.0, 0.0}},
    {{1.0, 0.0, 0.0}, {0.627955, 0.224863, 0.125846}},
    {{0.0, 1.0, 0.0}, {0.866440, -0.233888, 0.179498}},
    {{0.0, 0.0, 1.0}, {0.452014, -0.032457, -0.311528}}
  ]

  describe "parse/1" do
    test "reads lightness, chroma, hue and an optional alpha" do
      assert {:ok, %Color{l: 0.985, c: 0.002, h: 265.0, alpha: 1.0}} = Color.parse("oklch(0.985 0.002 265)")
      assert {:ok, %Color{l: 0.769, c: 0.157, h: 70.1, alpha: 0.14}} = Color.parse("oklch(0.769 0.157 70.1 / 0.14)")
      assert {:ok, %Color{l: +0.0, c: +0.0, h: +0.0, alpha: 0.5}} = Color.parse("oklch(0.000 0.000 0 / 0.5)")
      assert {:ok, %Color{l: 1.0}} = Color.parse("oklch(1 0 0)")
    end

    test "refuses anything that is not the authored OKLCH form" do
      for bad <- [
            "#ffffff",
            "rgb(0 0 0)",
            "oklch(0.5 0.1)",
            "oklch(0.5, 0.1, 20)",
            "oklch(50% 0.1 20)",
            "oklch(1.2 0.1 20)",
            "oklch(0.5 0.6 20)",
            "oklch(0.5 0.1 360)",
            "oklch(0.5 0.1 20 / 1.5)",
            "oklch(0.5 0.1 20 /0.5)",
            " oklch(0.5 0.1 20)",
            "oklch(0.5 0.1 20) ",
            "OKLCH(0.5 0.1 20)",
            "oklch(-0.5 0.1 20)",
            "oklch(.5 0.1 20)",
            ""
          ] do
        assert {:error, _reason} = Color.parse(bad), "expected #{inspect(bad)} to be refused"
      end
    end
  end

  describe "the OKLab transform" do
    test "matches the published values for white, black and the sRGB primaries" do
      for {linear, {el, ea, eb}} <- @published do
        {l, a, b} = Color.linear_srgb_to_oklab(linear)
        assert_in_delta l, el, 1.0e-4
        assert_in_delta a, ea, 1.0e-4
        assert_in_delta b, eb, 1.0e-4

        {r, g, bl} = Color.oklab_to_linear_srgb({el, ea, eb})
        {er, eg, ebl} = linear
        assert_in_delta r, er, 1.0e-4
        assert_in_delta g, eg, 1.0e-4
        assert_in_delta bl, ebl, 1.0e-4
      end
    end

    test "an out of gamut linear color, with a negative channel, still converts" do
      {l, _a, _b} = Color.linear_srgb_to_oklab({-0.5, 0.1, 0.0})
      assert l < 0
    end

    test "converts polar OKLCH to OKLab" do
      {:ok, red} = Color.parse("oklch(0.627955 0.257683 29.2339)")
      {l, a, b} = Color.to_oklab(red)
      assert_in_delta l, 0.627955, 1.0e-6
      assert_in_delta a, 0.224863, 1.0e-5
      assert_in_delta b, 0.125846, 1.0e-5
    end

    test "the sRGB transfer function is the piecewise one, linear below the knee" do
      assert Color.decode(0.0) == 0.0
      assert_in_delta Color.decode(0.04045), 0.04045 / 12.92, 1.0e-12
      assert_in_delta Color.decode(0.5), 0.21404114, 1.0e-8
      assert_in_delta Color.encode(0.0031308), 0.0031308 * 12.92, 1.0e-12
      assert_in_delta Color.encode(0.21404114), 0.5, 1.0e-7
      assert_in_delta Color.encode(1.0), 1.0, 1.0e-12
    end
  end

  describe "to_srgb8/1 and clip_delta/1" do
    test "an in gamut color is not moved by the clip" do
      {:ok, white} = Color.parse("oklch(1.000 0.000 0)")
      assert Color.to_srgb8(white) == {255, 255, 255}
      assert Color.clip_delta(white) < 1.0e-6
    end

    test "an out of gamut color is clipped channel by channel and reports how far it moved" do
      # Pure sRGB red with its chroma pushed well beyond the gamut.
      {:ok, color} = Color.parse("oklch(0.628 0.350 29.2)")
      {r, g, b} = Color.to_srgb8(color)
      assert r == 255 and g == 0 and b in 0..40
      assert Color.clip_delta(color) > 0.05
    end
  end

  describe "properties" do
    property "sRGB8 survives a round trip through OKLab" do
      check all(r <- integer(0..255), g <- integer(0..255), b <- integer(0..255)) do
        lab = Color.srgb8_to_oklab({r, g, b})
        assert Color.oklab_to_srgb8(lab) == {r, g, b}
      end
    end

    property "the clip is idempotent and always lands inside the unit cube" do
      check all(
              r <- float(min: -0.5, max: 1.5),
              g <- float(min: -0.5, max: 1.5),
              b <- float(min: -0.5, max: 1.5)
            ) do
        clipped = Color.clip({r, g, b})
        assert Color.clip(clipped) == clipped
        assert clipped |> Tuple.to_list() |> Enum.all?(&(&1 >= 0.0 and &1 <= 1.0))
      end
    end

    property "the OKLab distance is a metric: zero on itself, symmetric" do
      check all(
              a <- tuple({float(min: 0.0, max: 1.0), float(min: -0.4, max: 0.4), float(min: -0.4, max: 0.4)}),
              b <- tuple({float(min: 0.0, max: 1.0), float(min: -0.4, max: 0.4), float(min: -0.4, max: 0.4)})
            ) do
        assert Color.delta_e_ok(a, a) == 0.0
        assert_in_delta Color.delta_e_ok(a, b), Color.delta_e_ok(b, a), 1.0e-12
      end
    end
  end
end
