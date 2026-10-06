defmodule Malachi.UI.TokenGen.PerceptionTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Malachi.UI.TokenGen.{Color, Contrast, Cvd, Quantize}

  describe "Contrast" do
    test "black on white is 21 and a color on itself is 1" do
      assert_in_delta Contrast.ratio({0, 0, 0}, {255, 255, 255}), 21.0, 1.0e-9
      assert_in_delta Contrast.ratio({118, 118, 118}, {118, 118, 118}), 1.0, 1.0e-12
    end

    test "matches the WCAG reference value for #767676 on white, the lightest gray that passes 4.5" do
      assert_in_delta Contrast.ratio({0x76, 0x76, 0x76}, {255, 255, 255}), 4.54, 0.005
    end

    test "luminance weighs the channels by the sRGB coefficients" do
      assert_in_delta Contrast.luminance({255, 0, 0}), 0.2126, 1.0e-6
      assert_in_delta Contrast.luminance({0, 255, 0}), 0.7152, 1.0e-6
      assert_in_delta Contrast.luminance({0, 0, 255}), 0.0722, 1.0e-6
    end

    property "contrast is symmetric and bounded by 1 and 21" do
      check all(a <- rgb8(), b <- rgb8()) do
        ratio = Contrast.ratio(a, b)
        assert ratio == Contrast.ratio(b, a)
        assert ratio >= 1.0 and ratio <= 21.0 + 1.0e-9
      end
    end
  end

  describe "Quantize" do
    test "the palette is the xterm cube and gray ramp, indexes 16 to 255" do
      palette = Quantize.palette()
      assert length(palette) == 240
      assert {16, {0, 0, 0}} = hd(palette)
      assert {231, {255, 255, 255}} = List.keyfind(palette, 231, 0)
      assert {196, {255, 0, 0}} = List.keyfind(palette, 196, 0)
      assert {232, {8, 8, 8}} = List.keyfind(palette, 232, 0)
      assert {255, {238, 238, 238}} = List.last(palette)
    end

    test "never answers with one of the sixteen terminal themed colors" do
      for rgb <- [{0, 0, 0}, {128, 0, 0}, {192, 192, 192}, {255, 255, 255}] do
        assert Quantize.nearest(rgb) in 16..255
      end
    end

    test "a near miss lands on the perceptually closest entry" do
      assert Quantize.nearest({250, 2, 1}) == 196
      assert Quantize.nearest({90, 92, 96}) == 59
    end

    property "every palette entry quantises to itself" do
      check all({index, rgb} <- member_of(Quantize.palette())) do
        assert Quantize.nearest(rgb) == index
      end
    end

    property "every color quantises into 16 to 255" do
      check all(rgb <- rgb8()) do
        assert Quantize.nearest(rgb) in 16..255
      end
    end
  end

  describe "Cvd" do
    test "a deuteranope still tells red from blue" do
      red = Cvd.deuteranopia({255, 0, 0})
      blue = Cvd.deuteranopia({0, 0, 255})
      assert Color.delta_e_ok(red, blue) > 0.3
    end

    test "grays are left where they are" do
      for v <- [0, 64, 128, 255] do
        assert_in_delta Color.delta_e_ok(Cvd.deuteranopia({v, v, v}), Color.srgb8_to_oklab({v, v, v})), 0.0, 2.0e-3
      end
    end

    test "red and green collapse toward each other" do
      normal = Color.delta_e_ok(Color.srgb8_to_oklab({200, 40, 40}), Color.srgb8_to_oklab({40, 160, 40}))
      simulated = Color.delta_e_ok(Cvd.deuteranopia({200, 40, 40}), Cvd.deuteranopia({40, 160, 40}))
      assert simulated < normal / 2
    end
  end

  defp rgb8, do: tuple({integer(0..255), integer(0..255), integer(0..255)})
end
