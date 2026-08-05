# Wire-protocol benchmark: JSON+base64 (today's tcp_protocol) vs binary framing (Record.encode, already
# used on disk), over 1M records. Standalone; does NOT touch lib/ beyond calling Record.
#
# Measures, for both encodings: on-wire bytes, encode throughput + reductions, decode throughput +
# reductions. Records are paged (1000/page) like the real protocol. base64 inflates the payload ~33% and
# JSON adds parse/quote overhead; the binary frame is what a NorthGuard/Kafka-style protocol would use.
#
# Run: mix run benchmark/protocol_bench.exs

defmodule ProtoBench do
  alias Malachi.Log.Record

  @total 1_000_000
  @page 1_000
  @value_bytes 100

  # today's tcp_protocol serialization (copied from record_to_json/1)
  defp record_to_json(r), do: %{"key" => r.key, "value" => Base.encode64(r.value), "headers" => Map.new(r.headers), "timestamp" => r.timestamp}

  defp measure(fun) do
    {r0, _} = :erlang.statistics(:reductions)
    t0 = System.monotonic_time(:microsecond)
    acc = fun.()
    wall = System.monotonic_time(:microsecond) - t0
    {r1, _} = :erlang.statistics(:reductions)
    {acc, Float.round(wall / 1_000_000, 2), round(@total / (wall / 1_000_000)), r1 - r0}
  end

  def run do
    value = :binary.copy("x", @value_bytes)
    # one representative page, with offsets assigned (binary framing needs an offset)
    page = for i <- 1..@page, do: %Record{offset: i, key: "k#{rem(i, 1000)}", value: value, timestamp: 1_700_000_000_000, headers: []}
    pages = div(@total, @page)

    # ---- JSON + base64 ----
    {json_blobs, json_enc_s, json_enc_thr, json_enc_red} =
      measure(fn -> for _ <- 1..pages, do: Jason.encode!(%{"records" => Enum.map(page, &record_to_json/1)}) end)

    json_bytes = json_blobs |> Enum.map(&byte_size/1) |> Enum.sum()
    one_json = List.first(json_blobs)

    {_, json_dec_s, json_dec_thr, json_dec_red} =
      measure(fn ->
        for _ <- 1..pages do
          %{"records" => recs} = Jason.decode!(one_json)
          for r <- recs, do: Base.decode64!(r["value"])
        end
      end)

    # ---- binary framing (Record.encode / decode_all) ----
    {bin_blobs, bin_enc_s, bin_enc_thr, bin_enc_red} =
      measure(fn -> for _ <- 1..pages, do: page |> Enum.map(&Record.encode/1) |> IO.iodata_to_binary() end)

    bin_bytes = bin_blobs |> Enum.map(&byte_size/1) |> Enum.sum()
    one_bin = List.first(bin_blobs)

    {_, bin_dec_s, bin_dec_thr, bin_dec_red} =
      measure(fn -> for _ <- 1..pages, do: Record.decode_all(one_bin) end)

    mb = fn b -> Float.round(b / 1_048_576, 1) end

    IO.puts("""

    ============ Wire protocol: JSON+base64 vs binary framing (#{@total} records, #{@value_bytes}B value) ============
                    on-wire        encode                          decode
                    (MB)   B/rec   time    thr(rec/s)   reductions   time    thr(rec/s)   reductions
    JSON+base64     #{pad(mb.(json_bytes), 6)} #{pad(round(json_bytes / @total), 6)}  #{pad(json_enc_s, 5)}s  #{pad(json_enc_thr, 9)}   #{pad(json_enc_red, 11)}  #{pad(json_dec_s, 5)}s  #{pad(json_dec_thr, 9)}   #{json_dec_red}
    binary frame    #{pad(mb.(bin_bytes), 6)} #{pad(round(bin_bytes / @total), 6)}  #{pad(bin_enc_s, 5)}s  #{pad(bin_enc_thr, 9)}   #{pad(bin_enc_red, 11)}  #{pad(bin_dec_s, 5)}s  #{pad(bin_dec_thr, 9)}   #{bin_dec_red}

    binary vs JSON:  #{Float.round((json_bytes - bin_bytes) / json_bytes * 100, 1)}% fewer bytes | encode #{Float.round(json_enc_red / max(bin_enc_red, 1), 1)}x fewer reductions | decode #{Float.round(json_dec_red / max(bin_dec_red, 1), 1)}x fewer reductions
    ==========================================================================================================
    """)
  end

  defp pad(v, n), do: String.pad_trailing("#{v}", n)
end

ProtoBench.run()
