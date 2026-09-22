defmodule Malachi.Auth.SessionBindingCanonicalTest do
  @moduledoc """
  Session IP binding compares the canonical form of both sides, not the raw values.

  The two listeners used to hand `Malachi.Auth.SessionManager` different representations of the same
  address: the TCP acceptor formats at the edge and passes a binary, the dashboard passed the tuple
  straight from `:inet.peername/1`, and both mint sessions into the same table. `binding_mismatches/3`
  compared them raw, so a token minted on one listener and presented on the other from *the same
  address* could never satisfy the binding. It came back `:session_hijack_attempt` and raised the
  theft signal the moduledoc asks operators to alert on.

  That was a type accident rather than a defence, and these cases pin the fix without weakening what
  the binding is for: a genuinely different address is still refused.
  """
  use ExUnit.Case, async: false

  alias Malachi.Auth.SessionManager

  setup do
    binding = Application.get_env(:malachi, :session_ip_binding, true)
    ranges = Application.get_env(:malachi, :trusted_proxy_ranges, [])
    Application.put_env(:malachi, :session_ip_binding, true)
    Application.put_env(:malachi, :trusted_proxy_ranges, [])

    on_exit(fn ->
      Application.put_env(:malachi, :session_ip_binding, binding)
      Application.put_env(:malachi, :trusted_proxy_ranges, ranges)
    end)

    {:ok, username: "binding_canonical_#{System.unique_integer([:positive])}"}
  end

  defp session_for(username, ip) do
    {:ok, token} = SessionManager.create_session(username, [:admin], ip, "")
    on_exit(fn -> SessionManager.revoke_session(token) end)
    token
  end

  describe "the same address in two representations" do
    test "a session minted from a tuple validates against the equivalent binary", ctx do
      # The dashboard shape minting, the TCP acceptor shape validating.
      token = session_for(ctx.username, {192, 168, 1, 1})

      assert {:ok, session} = SessionManager.validate_session(token, "192.168.1.1")
      assert session.username == ctx.username
    end

    test "a session minted from a binary validates against the equivalent tuple", ctx do
      token = session_for(ctx.username, "192.168.1.1")

      assert {:ok, session} = SessionManager.validate_session(token, {192, 168, 1, 1})
      assert session.username == ctx.username
    end

    test "an IPv6 session matches across representations, including the compressed form", ctx do
      token = session_for(ctx.username, {0, 0, 0, 0, 0, 0, 0, 1})

      assert {:ok, _session} = SessionManager.validate_session(token, "::1")
    end

    test "an IPv4-mapped IPv6 peer matches its own canonical string", ctx do
      mapped = {0, 0, 0, 0, 0, 0xFFFF, 0x7F00, 1}
      token = session_for(ctx.username, mapped)

      assert {:ok, _session} = SessionManager.validate_session(token, "::ffff:127.0.0.1")
    end
  end

  describe "the binding still refuses a genuinely different address" do
    test "a different IPv4 address is a hijack attempt, in either representation", ctx do
      token = session_for(ctx.username, {192, 168, 1, 1})

      assert {:error, :session_hijack_attempt} = SessionManager.validate_session(token, {10, 0, 0, 99})
      assert {:error, :session_hijack_attempt} = SessionManager.validate_session(token, "10.0.0.99")
    end

    test "a different IPv6 address is a hijack attempt", ctx do
      token = session_for(ctx.username, {0, 0, 0, 0, 0, 0, 0, 1})

      assert {:error, :session_hijack_attempt} = SessionManager.validate_session(token, "2001:db8::1")
    end

    test "an unreadable peer does not collapse into matching a real address", ctx do
      # "unknown" is what from_socket/2 answers for a dead socket. It must not validate a session that
      # was bound to a real address, or a closed socket would become a way past the binding.
      token = session_for(ctx.username, {192, 168, 1, 1})

      assert {:error, :session_hijack_attempt} = SessionManager.validate_session(token, "unknown")
      assert {:error, :session_hijack_attempt} = SessionManager.validate_session(token, nil)
    end

    test "the original address keeps working after a refused attempt", ctx do
      token = session_for(ctx.username, {192, 168, 1, 1})

      assert {:error, :session_hijack_attempt} = SessionManager.validate_session(token, {10, 0, 0, 99})
      assert {:ok, _session} = SessionManager.validate_session(token, "192.168.1.1")
    end
  end

  describe "trusted proxy exemption reads either representation" do
    setup do
      Application.put_env(:malachi, :trusted_proxy_ranges, ["10.0.0.0/8"])
      :ok
    end

    test "a binary address inside the range disables the binding", ctx do
      token = session_for(ctx.username, "10.1.2.3")

      # Exempt at creation, so a different address validates.
      assert {:ok, _session} = SessionManager.validate_session(token, {203, 0, 113, 9})
    end

    test "a tuple address inside the range disables the binding", ctx do
      token = session_for(ctx.username, {10, 1, 2, 3})

      assert {:ok, _session} = SessionManager.validate_session(token, "203.0.113.9")
    end

    test "an address outside the range keeps the binding on", ctx do
      token = session_for(ctx.username, {192, 168, 1, 1})

      assert {:error, :session_hijack_attempt} = SessionManager.validate_session(token, {203, 0, 113, 9})
    end
  end
end
