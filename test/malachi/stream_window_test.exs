defmodule Malachi.StreamWindowTest do
  use ExUnit.Case, async: true
  use ExUnitProperties
  import Bitwise

  alias Malachi.StreamWindow

  test "the handshake grants the request, capped by the broker's limits" do
    assert StreamWindow.grant(10, 1_000, 64, 16_777_216) == %{appends: 10, bytes: 1_000}
    assert StreamWindow.grant(1_000, 1 <<< 30, 64, 16_777_216) == %{appends: 64, bytes: 16_777_216}
  end

  test "the load scales a grant: whole to the soft limit, nothing from the hard one, linear between" do
    assert StreamWindow.scale(0, 100, 200) == 1.0
    assert StreamWindow.scale(100, 100, 200) == 1.0
    assert StreamWindow.scale(150, 100, 200) == 0.5
    assert StreamWindow.scale(200, 100, 200) == 0.0
    assert StreamWindow.scale(10_000, 100, 200) == 0.0
  end

  test "the ack's window is the grant scaled, rounded down" do
    assert StreamWindow.current(%{appends: 10, bytes: 1_000}, 0.55, false) == %{appends: 5, bytes: 550}
    assert StreamWindow.current(%{appends: 10, bytes: 1_000}, 0.0, false) == %{appends: 0, bytes: 0}
  end

  test "a stream with nothing in flight keeps room for one append of any size the grant allows" do
    assert StreamWindow.current(%{appends: 10, bytes: 1_000}, 0.0, true) == %{appends: 1, bytes: 1_000}
    # under heavy load the bytes are not scaled down below the next append it may send
    assert StreamWindow.current(%{appends: 64, bytes: 16_777_216}, 0.02, true) == %{appends: 1, bytes: 16_777_216}
    # and a lighter load never grants less than a heavier one
    assert StreamWindow.current(%{appends: 64, bytes: 16_777_216}, 0.5, true) == %{appends: 32, bytes: 16_777_216}
  end

  property "a window never exceeds its grant, and shrinks as the load grows" do
    check all(
            appends <- positive_integer(),
            bytes <- positive_integer(),
            soft <- integer(0..1_000),
            span <- integer(1..1_000),
            load_a <- integer(0..3_000),
            load_b <- integer(0..3_000)
          ) do
      grant = %{appends: appends, bytes: bytes}
      [low, high] = Enum.sort([load_a, load_b])
      at_low = StreamWindow.current(grant, StreamWindow.scale(low, soft, soft + span), false)
      at_high = StreamWindow.current(grant, StreamWindow.scale(high, soft, soft + span), false)

      assert at_low.appends <= appends and at_low.bytes <= bytes
      assert at_high.appends <= at_low.appends and at_high.bytes <= at_low.bytes
    end
  end
end
