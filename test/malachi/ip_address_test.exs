defmodule Malachi.IPAddressTest do
  # The pure layer: no socket, no OS, no network. Deterministic everywhere, which is why it carries no
  # tag. The socket-level cases live in ip_address_socket_test.exs, tagged :linux.
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Malachi.IPAddress

  doctest Malachi.IPAddress

  describe "format/1 on addresses" do
    # The suite had no IPv6 coverage at all before this: no "::1", no eight-element tuple, nothing.
    # All six IPv6 clauses in the old copies were unexecuted by any test.
    for {tuple, expected} <- [
          {{127, 0, 0, 1}, "127.0.0.1"},
          {{0, 0, 0, 0}, "0.0.0.0"},
          {{255, 255, 255, 255}, "255.255.255.255"},
          {{10, 0, 0, 7}, "10.0.0.7"},
          {{0, 0, 0, 0, 0, 0, 0, 1}, "::1"},
          {{0, 0, 0, 0, 0, 0, 0, 0}, "::"},
          {{0x2001, 0xDB8, 0, 0, 0, 0, 0, 1}, "2001:db8::1"},
          {{0xFE80, 0, 0, 0, 0, 0, 0, 1}, "fe80::1"}
        ] do
      test "#{inspect(tuple)} formats as #{expected}" do
        assert IPAddress.format(unquote(Macro.escape(tuple))) == unquote(expected)
      end
    end

    test "an IPv4-mapped IPv6 address takes the canonical mapped form" do
      # The case that matters most in practice: a listener on :: accepting an IPv4 client gets an
      # eight-element tuple. The old manual formatter wrote it as 0:0:0:0:0:FFFF:7F00:1, which shares
      # no substring with the address an operator would search for.
      assert IPAddress.format({0, 0, 0, 0, 0, 0xFFFF, 0x7F00, 1}) == "::ffff:127.0.0.1"
      assert IPAddress.format({0, 0, 0, 0, 0, 0xFFFF, 0xC0A8, 0x101}) == "::ffff:192.168.1.1"
    end

    test "the IPv6 form is lowercase and zero-compressed, not the expanded uppercase one" do
      formatted = IPAddress.format({0xFE80, 0, 0, 0, 0, 0, 0, 1})

      assert formatted == "fe80::1"
      refute formatted =~ ~r/[A-F]/
      refute formatted == "FE80:0:0:0:0:0:0:1"
    end
  end

  describe "format/1 on a binary" do
    test "passes an already formatted address through unchanged" do
      assert IPAddress.format("192.168.1.1") == "192.168.1.1"
      assert IPAddress.format("::ffff:127.0.0.1") == "::ffff:127.0.0.1"
    end

    test "passes a sentinel through, so formatting twice never changes the answer" do
      # The TCP path formats once at the edge and carries the binary from there, so every downstream
      # helper sees a binary. Re-formatting must be a no-op or the key would drift.
      for value <- ["unknown", "invalid", "not an address at all"] do
        assert IPAddress.format(value) == value
      end
    end
  end

  describe "format/1 sentinels" do
    test "a tuple that is shaped like an address but is not one answers invalid" do
      # :inet.ntoa/1 rejects out-of-range elements, not just the wrong arity. The previous copies
      # guarded on tuple_size/1 alone and then called to_string/1 on {:error, :einval}, which raised
      # Protocol.UndefinedError instead of answering a sentinel.
      assert IPAddress.format({999, 0, 0, 1}) == "invalid"
      assert IPAddress.format({-1, 0, 0, 1}) == "invalid"
      assert IPAddress.format({0, 0, 0, 0, 0, 0, 0, 0x1FFFF}) == "invalid"
      assert IPAddress.format({:a, 0, 0, 1}) == "invalid"
      assert IPAddress.format({1, 2}) == "invalid"
      assert IPAddress.format({}) == "invalid"
    end

    test "anything that is not even shaped like an address answers unknown" do
      assert IPAddress.format(nil) == "unknown"
      assert IPAddress.format(:einval) == "unknown"
      assert IPAddress.format(42) == "unknown"
      assert IPAddress.format([]) == "unknown"
      assert IPAddress.format(%{}) == "unknown"
    end

    test "the two sentinels are distinct, so the audit log tells a bad address from no address" do
      refute IPAddress.format({1, 2}) == IPAddress.format(nil)
    end
  end

  describe "parse/1" do
    test "reads back both families from a binary" do
      assert IPAddress.parse("127.0.0.1") == {:ok, {127, 0, 0, 1}}
      assert IPAddress.parse("::1") == {:ok, {0, 0, 0, 0, 0, 0, 0, 1}}
      assert IPAddress.parse("::ffff:127.0.0.1") == {:ok, {0, 0, 0, 0, 0, 0xFFFF, 0x7F00, 1}}
    end

    test "accepts a tuple that is already an address" do
      assert IPAddress.parse({10, 0, 0, 7}) == {:ok, {10, 0, 0, 7}}
      assert IPAddress.parse({0, 0, 0, 0, 0, 0, 0, 1}) == {:ok, {0, 0, 0, 0, 0, 0, 0, 1}}
    end

    test "refuses a malformed binary, a malformed tuple and a non-address term" do
      assert IPAddress.parse("not an address") == :error
      assert IPAddress.parse("999.0.0.1") == :error
      assert IPAddress.parse("") == :error
      assert IPAddress.parse({999, 0, 0, 1}) == :error
      assert IPAddress.parse({1, 2}) == :error
      assert IPAddress.parse(nil) == :error
      assert IPAddress.parse(42) == :error
    end

    test "refuses the sentinels, so a malformed address never parses back into a real one" do
      assert IPAddress.parse("unknown") == :error
      assert IPAddress.parse("invalid") == :error
    end
  end

  describe "round-trip property" do
    # The property that makes :inet.ntoa the right canonical form: what it writes is what
    # :inet.parse_address reads back. A CIDR allowlist and the trusted-proxy check both depend on it.
    property "an IPv4 address survives format then parse" do
      check all(
              a <- integer(0..255),
              b <- integer(0..255),
              c <- integer(0..255),
              d <- integer(0..255)
            ) do
        ip = {a, b, c, d}
        assert IPAddress.parse(IPAddress.format(ip)) == {:ok, ip}
      end
    end

    property "an IPv6 address survives format then parse" do
      check all(groups <- list_of(integer(0..65_535), length: 8)) do
        ip = List.to_tuple(groups)
        assert IPAddress.parse(IPAddress.format(ip)) == {:ok, ip}
      end
    end

    property "formatting is idempotent, because the binary clause passes through" do
      check all(groups <- list_of(integer(0..65_535), length: 8)) do
        once = IPAddress.format(List.to_tuple(groups))
        assert IPAddress.format(once) == once
      end
    end
  end
end
