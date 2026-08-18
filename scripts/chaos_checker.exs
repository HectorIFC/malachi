# Jepsen-style acked-durability checker for the chaos certification harness.
#
# produce mode: connects to the cluster (multi-host round-robin), creates the topic, and produces
# sequential values c-1, c-2, ... at a steady rate for the whole window, RETRYING through errors and
# dropped connections (chaos is happening on purpose). A value is appended to the acked file ONLY
# when its produce was confirmed, so the file is the exact set of writes the cluster acknowledged.
#
# verify mode: fetches the whole topic and asserts every acked value is present. The invariant under
# test: an acknowledged write survives any single-node kill, partition, or stall (rf=3 quorum
# durability). Exit 0 on success, 1 with a summary of missing values otherwise.
#
# Usage (run inside the cluster network or anywhere that reaches the hosts):
#   mix run --no-start scripts/chaos_checker.exs produce host1,host2,host3 topic duration_s acked_file
#   mix run --no-start scripts/chaos_checker.exs verify  host1,host2,host3 topic acked_file

defmodule ChaosChecker do
  alias Malachi.Loadtest.Conn
  alias Malachi.Log.Record
  alias Malachi.Wire

  @rate_sleep_ms 20
  @retry_sleep_ms 250

  def main(["produce", hosts, topic, duration_s, acked_file]) do
    hosts = parse_hosts(hosts)
    deadline = System.monotonic_time(:millisecond) + String.to_integer(duration_s) * 1000
    {:ok, out} = File.open(acked_file, [:write])

    conn = connect_retry(hosts, 0, deadline)
    ensure_topic(conn, topic)
    produce_loop(conn, hosts, topic, out, deadline, 1, 2, 0)
  end

  def main(["verify", hosts, topic, acked_file]) do
    hosts = parse_hosts(hosts)
    acked = acked_file |> File.read!() |> String.split("\n", trim: true) |> MapSet.new()

    conn = connect_retry(hosts, 0, System.monotonic_time(:millisecond) + 30_000)
    read = fetch_all(conn, topic, nil, MapSet.new(), 2)
    missing = MapSet.difference(acked, read)

    IO.puts("acked=#{MapSet.size(acked)} read=#{MapSet.size(read)} missing=#{MapSet.size(missing)}")

    if MapSet.size(missing) == 0 do
      IO.puts("VERIFY OK: every acknowledged write survived")
      System.halt(0)
    else
      IO.puts("VERIFY FAILED, first missing: #{missing |> Enum.take(10) |> inspect()}")
      System.halt(1)
    end
  end

  def main(_argv) do
    IO.puts("usage: chaos_checker.exs produce <hosts> <topic> <duration_s> <acked_file>")
    IO.puts("       chaos_checker.exs verify  <hosts> <topic> <acked_file>")
    System.halt(2)
  end

  defp parse_hosts(hosts), do: hosts |> String.split(",", trim: true) |> Enum.map(&String.trim/1)

  # --- produce ---

  defp produce_loop(conn, hosts, topic, out, deadline, i, corr, host_idx) do
    if System.monotonic_time(:millisecond) >= deadline do
      IO.puts("produced through #{i - 1} attempts; done")
    else
      value = "c-#{i}"
      record = %Record{value: value, key: "k#{i}", timestamp: 0, headers: []}

      case try_produce(conn, topic, record, corr) do
        {:ok, conn} ->
          # Confirmed by the cluster: this write must now survive anything the chaos does.
          IO.write(out, value <> "\n")
          Process.sleep(@rate_sleep_ms)
          produce_loop(conn, hosts, topic, out, deadline, i + 1, corr + 1, host_idx)

        {:retry, _dead} ->
          # Error or dropped connection mid-chaos: reconnect (next host) and RETRY THE SAME value;
          # it was never acked, so it may legitimately be absent or duplicated, both fine.
          Process.sleep(@retry_sleep_ms)
          conn = connect_retry(hosts, host_idx + 1, deadline)
          produce_loop(conn, hosts, topic, out, deadline, i, corr + 1, host_idx + 1)
      end
    end
  end

  defp try_produce(conn, topic, record, corr) do
    case Conn.request(conn, Wire.produce_key(), corr, Wire.encode_produce_req(topic, [record])) do
      {:ok, 0, _resp, conn} -> {:ok, conn}
      {:ok, _code, _resp, conn} -> {:retry, conn}
      {:error, _reason} -> {:retry, conn}
    end
  rescue
    _any -> {:retry, conn}
  end

  # --- verify ---

  defp fetch_all(conn, topic, cursor, acc, corr) do
    payload = Wire.encode_fetch_req(topic, cursor, nil, nil, 500, 0)

    case Conn.request(conn, Wire.fetch_key(), corr, payload) do
      {:ok, 0, resp, conn} ->
        {records, next_cursor} = Wire.decode_fetch_resp(resp)

        case records do
          [] -> acc
          _ -> fetch_all(conn, topic, next_cursor, Enum.into(records, acc, & &1.value), corr + 1)
        end

      other ->
        IO.puts("fetch failed: #{inspect(other)}")
        System.halt(1)
    end
  end

  # --- connection plumbing ---

  defp connect_retry(hosts, idx, deadline) do
    if System.monotonic_time(:millisecond) >= deadline do
      IO.puts("could not connect before the deadline")
      System.halt(1)
    end

    host = Enum.at(hosts, rem(idx, length(hosts)))

    with {:ok, conn} <- Conn.connect(host: host, port: 4040),
         {:ok, conn} <- Conn.authenticate(conn, user: "admin", pass: "admin123") do
      conn
    else
      _err ->
        Process.sleep(@retry_sleep_ms)
        connect_retry(hosts, idx + 1, deadline)
    end
  rescue
    _any ->
      Process.sleep(@retry_sleep_ms)
      connect_retry(hosts, idx + 1, deadline)
  end

  defp ensure_topic(conn, topic) do
    {:ok, _code, _resp, _conn} = Conn.request(conn, Wire.create_topic_key(), 1, Wire.encode_create_topic_req(topic, 8))
    :ok
  end
end

ChaosChecker.main(System.argv())
