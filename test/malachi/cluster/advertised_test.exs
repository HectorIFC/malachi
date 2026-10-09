defmodule Malachi.Cluster.AdvertisedTest do
  use ExUnit.Case, async: true

  alias Malachi.Cluster.Advertised

  describe "resolve/5" do
    test "a configured host and port are what the node advertises" do
      assert Advertised.resolve("malachi-0.malachi-headless", 5050, 4040, :"malachi@malachi-0", true) ==
               {:ok, %{host: "malachi-0.malachi-headless", port: 5050}}
    end

    test "the port defaults to the one the listener binds" do
      assert {:ok, %{port: 4040}} = Advertised.resolve("10.0.0.7", nil, 4040, :"malachi@10.0.0.7", true)
    end

    test "a node with peers and no host is refused, blank counting as none" do
      for host <- [nil, "", "   "] do
        assert Advertised.resolve(host, nil, 4040, :malachi@a, true) == {:error, :missing_host}
      end
    end

    test "a node with peers and a loopback host is refused" do
      for host <- [
            "127.0.0.1",
            "127.1.2.3",
            "::1",
            "0.0.0.0",
            "::",
            "::ffff:127.0.0.1",
            "::ffff:0.0.0.0",
            "localhost",
            "LOCALHOST",
            "localhost.localdomain",
            "localhost.",
            "[::1]",
            "ip6-localhost",
            "ip6-loopback"
          ] do
        assert {:error, {:loopback_host, _host}} = Advertised.resolve(host, nil, 4040, :malachi@a, true), host
      end
    end

    test "a node with no peers falls back to its node name's host, and may be loopback" do
      assert {:ok, %{host: "malachi-0.svc", port: 4040}} =
               Advertised.resolve(nil, nil, 4040, :"malachi@malachi-0.svc", false)

      assert {:ok, %{host: "127.0.0.1"}} = Advertised.resolve(nil, nil, 4040, :"m@127.0.0.1", false)
      assert {:ok, %{host: "localhost"}} = Advertised.resolve(nil, nil, 4040, :nonode@nohost, false)
      assert {:ok, %{host: "127.0.0.1"}} = Advertised.resolve("127.0.0.1", nil, 4040, :m@h, false)
    end

    test "an IPv6 address in brackets is advertised without them, so a client can dial it" do
      assert {:ok, %{host: "2001:db8::1"}} = Advertised.resolve("[2001:db8::1]", nil, 4040, :m@h, true)
      assert {:ok, %{host: "[2001:db8::1"}} = Advertised.resolve("[2001:db8::1", nil, 4040, :m@h, true)
    end

    test "what brackets hold is checked like any host: blank is no host, padding is trimmed" do
      for host <- ["[]", "[ ]"] do
        assert Advertised.resolve(host, nil, 4040, :m@h, true) == {:error, :missing_host}, host
      end

      assert {:ok, %{host: "h"}} = Advertised.resolve("[]", nil, 4040, :m@h, false)
      assert {:error, {:loopback_host, "::1"}} = Advertised.resolve("[ ::1 ]", nil, 4040, :m@h, true)
      assert {:ok, %{host: "2001:db8::1"}} = Advertised.resolve("[ 2001:db8::1 ]", nil, 4040, :m@h, true)
    end

    test "a loopback IPv6 address with a zone is still loopback" do
      for host <- ["::1%lo0", "::1%1", "[::1%lo0]"] do
        assert {:error, {:loopback_host, _}} = Advertised.resolve(host, nil, 4040, :m@h, true), host
      end

      assert {:ok, %{host: "fe80::1%eth0"}} = Advertised.resolve("fe80::1%eth0", nil, 4040, :m@h, true)
    end

    test "a configured host is trimmed" do
      assert {:ok, %{host: "broker.example"}} = Advertised.resolve("  broker.example ", nil, 4040, :m@h, true)
    end
  end

  test "local_only?/1 tells a routable address or name from one that stays on this machine" do
    for host <- [
          "10.0.0.1",
          "192.168.1.5",
          "::ffff:10.0.0.1",
          "fe80::1",
          "[fe80::1]",
          "broker.example",
          "broker.example.",
          "localhost.example",
          "127.example.com"
        ] do
      refute Advertised.local_only?(host), host
    end
  end

  describe "the attribute" do
    test "put/2 and of/1 round trip under the reserved atom key" do
      attributes = Advertised.put(%{"rack" => "a"}, %{host: "h", port: 1})
      assert attributes[Advertised.key()] == %{host: "h", port: 1}
      assert Advertised.of(attributes) == %{host: "h", port: 1}
    end

    test "of/1 is nil for a member that advertises nothing usable" do
      for attributes <- [
            %{},
            %{advertised: :garbage},
            %{advertised: %{host: nil, port: 1}},
            %{advertised: %{host: "h"}}
          ] do
        assert Advertised.of(attributes) == nil
      end
    end

    test "ensure/2 keeps the current address when the new attributes state none, and a stated one wins" do
      current = %{"rack" => "a", advertised: %{host: "h", port: 1}}

      assert Advertised.ensure(%{"rack" => "b"}, current) == %{"rack" => "b", advertised: %{host: "h", port: 1}}
      assert Advertised.ensure(%{advertised: %{host: "x", port: 2}}, current) == %{advertised: %{host: "x", port: 2}}
      assert Advertised.ensure(%{"rack" => "b"}, %{}) == %{"rack" => "b"}
    end
  end
end
