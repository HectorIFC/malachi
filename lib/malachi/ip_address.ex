defmodule Malachi.IPAddress do
  @moduledoc """
  The one canonical string form of a client address, and its inverse.

  A client address is not display material: the string this module produces is the per-IP key of
  `Malachi.ConnectionLimiter`, the auth key of `Malachi.RateLimiter`, half of the replicated lockout
  key in `Malachi.Auth.LockoutManager`, the value compared by session binding in
  `Malachi.Auth.SessionManager`, and the `ip` field of every `Malachi.AuditLog` event. Two modules
  that format the same address differently split one client into two limiter buckets, stop the
  lockout manager recognising repeated attempts, and write one address two ways in the audit log.

  Six modules used to format addresses three different ways, and for one IPv6 address they disagreed
  (`0:0:0:0:0:FFFF:7F00:1` against `::ffff:127.0.0.1`). Everything goes through here now.

  ## The canonical form

  `:inet.ntoa/1`, which is the RFC 5952 form: lowercase hex, zero runs compressed to `::`, and an
  IPv4-mapped address written as `::ffff:127.0.0.1`. It is the form that round-trips through
  `:inet.parse_address/1` (see `parse/1`), which is what a CIDR allowlist and an operator grepping
  the audit log both expect.

  ## The sentinel rule

  Written once, so no caller invents its own:

    * a binary passes through unchanged, so a caller that already formatted once is never
      reformatted;
    * `:inet.ntoa/1` accepted it, so the canonical form;
    * it was a tuple and `:inet.ntoa/1` rejected it, so `"invalid"`: it was shaped like an address
      and was not one;
    * anything else, `nil` included, so `"unknown"`: there was no address at all.

  `:inet.ntoa/1` decides what is an address: it answers `{:error, :einval}` for a wrong arity *and*
  for out-of-range elements, and never raises. So no element-range guard is duplicated here. The
  previous copies guarded on `tuple_size/1` alone and then called `to_string/1` on the result, which
  raised `Protocol.UndefinedError` for a correctly sized tuple holding out-of-range elements.

  The tuple test lives in the clause head rather than after the `:inet.ntoa/1` call because dialyzer
  narrows the argument to `t:inet.ip_address/0` once the call is made, which would make a later
  `is_tuple/1` test look unreachable.

  ## What this does not do

  It does not decide *which* address a request came from. There is no forwarded-header handling
  here: the peer address is the socket's own, and the trusted-proxy check in
  `Malachi.Auth.SessionManager` compares that against configured CIDR ranges.
  """

  alias Malachi.SocketHelper

  @invalid "invalid"
  @unknown "unknown"

  @typedoc """
  What a caller may hand to `format/1`: an address tuple, an already formatted binary, or anything
  else (which is answered with a sentinel rather than a crash).
  """
  @type formattable :: :inet.ip_address() | String.t() | nil | term()

  @doc """
  The canonical string form of `ip`.

  ## Examples

      iex> Malachi.IPAddress.format({127, 0, 0, 1})
      "127.0.0.1"

      iex> Malachi.IPAddress.format({0, 0, 0, 0, 0, 0, 0, 1})
      "::1"

  An IPv4 client reaching a listener bound to `::` arrives as an IPv4-mapped eight-element tuple,
  which is where the old implementations diverged most confusingly:

      iex> Malachi.IPAddress.format({0, 0, 0, 0, 0, 0xFFFF, 0x7F00, 1})
      "::ffff:127.0.0.1"

  A binary is passed through, so formatting twice is the same as formatting once:

      iex> Malachi.IPAddress.format("10.0.0.7")
      "10.0.0.7"

  The sentinels tell a malformed address apart from no address:

      iex> Malachi.IPAddress.format({999, 0, 0, 1})
      "invalid"

      iex> Malachi.IPAddress.format(nil)
      "unknown"
  """
  @spec format(formattable()) :: String.t()
  def format(ip) when is_binary(ip), do: ip

  def format(ip) when is_tuple(ip) do
    case :inet.ntoa(ip) do
      {:error, :einval} -> @invalid
      chars -> List.to_string(chars)
    end
  end

  def format(_ip), do: @unknown

  @doc """
  The address tuple behind `ip`, for callers that need to compare rather than record it.

  Accepts either representation, because the two listeners produce different ones: the TCP acceptor
  formats at the edge and carries a binary, while a tuple arrives straight from `:inet.peername/1`.

  ## Examples

      iex> Malachi.IPAddress.parse("::1")
      {:ok, {0, 0, 0, 0, 0, 0, 0, 1}}

      iex> Malachi.IPAddress.parse({10, 0, 0, 7})
      {:ok, {10, 0, 0, 7}}

      iex> Malachi.IPAddress.parse("not an address")
      :error
  """
  @spec parse(formattable()) :: {:ok, :inet.ip_address()} | :error
  def parse(ip) when is_binary(ip) do
    case :inet.parse_address(String.to_charlist(ip)) do
      {:ok, parsed} -> {:ok, parsed}
      {:error, _reason} -> :error
    end
  end

  def parse(ip) when is_tuple(ip) do
    case :inet.ntoa(ip) do
      {:error, :einval} -> :error
      _chars -> {:ok, ip}
    end
  end

  def parse(_ip), do: :error

  @doc """
  The client address behind `socket`, in canonical form, for either transport.

  Answers `"unknown"` when the peer cannot be read, which is the ordinary outcome when the client
  closed the connection between the accept and this call.
  """
  @spec from_socket(:gen_tcp.socket() | :ssl.sslsocket(), :gen_tcp | :ssl) :: String.t()
  def from_socket(socket, transport) do
    case SocketHelper.socket_peername(socket, transport) do
      {:ok, {address, _port}} -> format(address)
      {:error, _reason} -> @unknown
    end
  end
end
