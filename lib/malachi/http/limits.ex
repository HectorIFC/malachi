defmodule Malachi.HTTP.Limits do
  @moduledoc """
  The request limits every HTTP listener on the node enforces, read from one set of configuration keys.

  Two listeners parse HTTP today: the legacy dashboard (`Malachi.Dashboard`, by hand on `:gen_tcp`) and
  the console endpoint (`Malachi.Console.Endpoint`, on Bandit). Both take their header bounds and their
  read deadline from here, so an operator who tightens a limit tightens it on both, and the console
  cannot drift away from the bounds the dashboard's security suite pins down.

  | Key | Default | Bounds |
  |---|---|---|
  | `:dashboard_max_header_count` | 50 | header lines per request, repeats included |
  | `:dashboard_max_header_line_size` | 10,000 | bytes in the request line and in each header line, at most 1 MiB |
  | `:dashboard_max_header_size` | 32,768 | bytes of names plus values across all headers |
  | `:dashboard_recv_timeout_ms` | 5,000 | the request line and the whole header block |

  The keys keep their `dashboard_` prefix because the environment variables operators already set
  (`MALACHI_DASHBOARD_MAX_HEADER_*`) map to them.
  """

  alias Malachi.Config

  # Bandit's defaults for the count and the line. A line is bounded at the socket; the total is what
  # keeps fifty maximal lines, half a megabyte per connection, from being acceptable.
  @default_max_header_count 50
  @default_max_header_line_size 10_000
  @default_max_header_size 32_768

  # The line limit also sizes the dashboard's driver buffer, which is allocated whole on the first read
  # of a partial line, so it is a memory cost per connection rather than only a ceiling. It is capped
  # here: past 2^31 the socket options wrap (buffer 1, and at 2^32 packet_size 0, which is no line limit
  # at all).
  @max_header_line_size_ceiling 1_048_576

  @default_recv_timeout_ms 5_000

  @typedoc "Header bounds: lines per request, bytes per line, and bytes of names plus values in total."
  @type headers :: %{count: pos_integer(), line: pos_integer(), total: pos_integer()}

  @doc """
  The header bounds. Read when a listener starts, so a bad value is reported once rather than on every
  request; an invalid value falls back to its default through `Malachi.Config.checked/4`.
  """
  @spec headers() :: headers()
  def headers do
    %{
      count: setting(:dashboard_max_header_count, @default_max_header_count, &(&1 > 0)),
      line:
        setting(
          :dashboard_max_header_line_size,
          @default_max_header_line_size,
          &(&1 > 0 and &1 <= @max_header_line_size_ceiling)
        ),
      total: setting(:dashboard_max_header_size, @default_max_header_size, &(&1 > 0))
    }
  end

  @doc """
  Deadline, in milliseconds, for the request line and the whole header block together. Without it a
  client that connects and sends nothing pins a process and a socket indefinitely (slowloris), and a
  per-read timeout alone would let a client that sends one header just inside it hold the connection
  for as many reads as it is allowed headers. On the console it is also how long an idle keep alive
  connection is held.
  """
  @spec recv_timeout_ms() :: non_neg_integer()
  def recv_timeout_ms do
    Application.get_env(:malachi, :dashboard_recv_timeout_ms, @default_recv_timeout_ms)
  end

  defp setting(key, default, valid?) do
    :malachi
    |> Application.get_env(key, default)
    |> Config.checked(key, default, &(is_integer(&1) and valid?.(&1)))
  end
end
