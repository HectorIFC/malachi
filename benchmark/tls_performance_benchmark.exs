#!/usr/bin/env elixir

# TLS Performance Benchmark
# Run with: mix run benchmark/tls_performance_benchmark.exs
#
# Measures TLS overhead vs plain TCP for:
# - Handshake latency (TLS 1.3 vs 1.2 vs plain TCP)
# - Message throughput (1KB messages)
# - Certificate validation overhead

Code.require_file("utils/benchmark_helpers.ex", "benchmark")

defmodule TLSPerformanceBenchmark do
  @warmup_iterations 100
  @benchmark_iterations 1000
  @message_size 1024

  def run do
    IO.puts("=" |> String.duplicate(70))
    IO.puts("  MalachiMQ TLS Performance Benchmark")
    IO.puts("=" |> String.duplicate(70))
    IO.puts("")

    Application.ensure_all_started(:ssl)

    results = %{
      "benchmark" => "tls_performance",
      "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601(),
      "system_info" => get_system_info(),
      "results" => %{
        "handshake_latency" => benchmark_handshake_latency(),
        "throughput" => benchmark_throughput(),
        "cert_validation" => benchmark_cert_validation()
      }
    }

    IO.puts("")
    IO.puts("=" |> String.duplicate(70))
    IO.puts("  Results Summary")
    IO.puts("=" |> String.duplicate(70))
    print_results(results)

    # Save results
    filename = "benchmark/results/tls_performance_#{Date.to_iso8601(Date.utc_today())}.json"
    File.mkdir_p!("benchmark/results")
    File.write!(filename, Jason.encode!(results, pretty: true))
    IO.puts("\nResults saved to: #{filename}")
  end

  defp get_system_info do
    %{
      "otp_release" => to_string(:erlang.system_info(:otp_release)),
      "elixir_version" => System.version(),
      "schedulers" => :erlang.system_info(:schedulers_online),
      "os" => to_string(:os.type() |> elem(1))
    }
  end

  defp benchmark_handshake_latency do
    IO.puts("Benchmarking handshake latency...")

    # Generate test certificates
    {cert_path, key_path} = generate_test_certs()

    try do
      tcp_latency = benchmark_tcp_handshake()
      tls_13_latency = benchmark_tls_handshake(cert_path, key_path, [:"tlsv1.3"])
      tls_12_latency = benchmark_tls_handshake(cert_path, key_path, [:"tlsv1.2"])

      %{
        "tcp_us" => tcp_latency,
        "tls_1_3_us" => tls_13_latency,
        "tls_1_2_us" => tls_12_latency,
        "overhead_1_3_pct" => if(tcp_latency > 0, do: Float.round((tls_13_latency - tcp_latency) / tcp_latency * 100, 1), else: 0.0),
        "overhead_1_2_pct" => if(tcp_latency > 0, do: Float.round((tls_12_latency - tcp_latency) / tcp_latency * 100, 1), else: 0.0)
      }
    after
      File.rm(cert_path)
      File.rm(key_path)
    end
  end

  defp benchmark_tcp_handshake do
    {:ok, listen} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
    {:ok, {_ip, port}} = :inet.sockname(listen)

    # Warmup
    for _ <- 1..@warmup_iterations do
      do_tcp_handshake(port, listen)
    end

    # Benchmark
    times =
      for _ <- 1..@benchmark_iterations do
        {time, _} = :timer.tc(fn -> do_tcp_handshake(port, listen) end)
        time
      end

    :gen_tcp.close(listen)
    Enum.sum(times) / length(times)
  end

  defp do_tcp_handshake(port, listen) do
    {:ok, client} = :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false], 5000)
    {:ok, server} = :gen_tcp.accept(listen, 1000)
    :gen_tcp.close(client)
    :gen_tcp.close(server)
  end

  defp benchmark_tls_handshake(cert_path, key_path, versions) do
    opts = [
      :binary,
      active: false,
      reuseaddr: true,
      certfile: cert_path,
      keyfile: key_path,
      versions: versions
    ]

    {:ok, listen} = :ssl.listen(0, opts)
    {:ok, {_ip, port}} = :ssl.sockname(listen)

    # Warmup
    for _ <- 1..min(@warmup_iterations, 50) do
      do_tls_handshake(port, listen)
    end

    # Benchmark
    times =
      for _ <- 1..min(@benchmark_iterations, 200) do
        {time, _} = :timer.tc(fn -> do_tls_handshake(port, listen) end)
        time
      end

    :ssl.close(listen)
    Enum.sum(times) / length(times)
  end

  defp do_tls_handshake(port, listen) do
    {:ok, client} = :ssl.connect({127, 0, 0, 1}, port, [:binary, active: false, verify: :verify_none], 5000)
    {:ok, transport_socket} = :ssl.transport_accept(listen, 2000)
    {:ok, _server} = :ssl.handshake(transport_socket, 2000)
    :ssl.close(client)
    :ssl.close(transport_socket)
  end

  defp benchmark_throughput do
    IO.puts("Benchmarking message throughput...")

    {cert_path, key_path} = generate_test_certs()
    message = String.duplicate("x", @message_size)

    try do
      tcp_throughput = benchmark_tcp_throughput(message)
      tls_throughput = benchmark_tls_throughput(cert_path, key_path, message)

      %{
        "message_size_bytes" => @message_size,
        "tcp_msgs_per_sec" => tcp_throughput,
        "tls_msgs_per_sec" => tls_throughput,
        "overhead_pct" =>
          if(tcp_throughput > 0,
            do: Float.round((tcp_throughput - tls_throughput) / tcp_throughput * 100, 1),
            else: 0
          )
      }
    after
      File.rm(cert_path)
      File.rm(key_path)
    end
  end

  defp benchmark_tcp_throughput(message) do
    {:ok, listen} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
    {:ok, {_ip, port}} = :inet.sockname(listen)

    {:ok, client} = :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false], 5000)
    {:ok, server} = :gen_tcp.accept(listen, 1000)

    count = 10_000

    {time, _} =
      :timer.tc(fn ->
        for _ <- 1..count do
          :gen_tcp.send(client, message)
          :gen_tcp.recv(server, 0, 1000)
        end
      end)

    :gen_tcp.close(client)
    :gen_tcp.close(server)
    :gen_tcp.close(listen)

    Float.round(count / (time / 1_000_000), 0)
  end

  defp benchmark_tls_throughput(cert_path, key_path, message) do
    opts = [
      :binary,
      active: false,
      reuseaddr: true,
      certfile: cert_path,
      keyfile: key_path,
      versions: [:"tlsv1.3"]
    ]

    {:ok, listen} = :ssl.listen(0, opts)
    {:ok, {_ip, port}} = :ssl.sockname(listen)

    {:ok, client} = :ssl.connect({127, 0, 0, 1}, port, [:binary, active: false, verify: :verify_none], 5000)
    {:ok, transport_socket} = :ssl.transport_accept(listen, 2000)
    {:ok, server} = :ssl.handshake(transport_socket, 2000)

    count = 5_000

    {time, _} =
      :timer.tc(fn ->
        for _ <- 1..count do
          :ssl.send(client, message)
          :ssl.recv(server, 0, 1000)
        end
      end)

    :ssl.close(client)
    :ssl.close(server)
    :ssl.close(listen)

    Float.round(count / (time / 1_000_000), 0)
  end

  defp benchmark_cert_validation do
    IO.puts("Benchmarking certificate validation overhead...")

    {cert_pem, key_pem} = generate_cert_pem()

    # Benchmark parsing
    {parse_time, _} =
      :timer.tc(fn ->
        for _ <- 1..1000 do
          MalachiMQ.TLSValidator.parse_certificate_validity(cert_pem)
        end
      end)

    # Benchmark key size parsing
    {key_time, _} =
      :timer.tc(fn ->
        for _ <- 1..1000 do
          MalachiMQ.TLSValidator.parse_key_size(key_pem)
        end
      end)

    # Benchmark key-cert match
    {match_time, _} =
      :timer.tc(fn ->
        for _ <- 1..100 do
          MalachiMQ.TLSValidator.do_validate_key_cert_match(cert_pem, key_pem)
        end
      end)

    %{
      "cert_parse_avg_us" => Float.round(parse_time / 1000, 1),
      "key_parse_avg_us" => Float.round(key_time / 1000, 1),
      "key_cert_match_avg_us" => Float.round(match_time / 100, 1)
    }
  end

  defp generate_test_certs do
    {cert_pem, key_pem} = generate_cert_pem()

    dir = System.tmp_dir!()
    cert_path = Path.join(dir, "bench_cert_#{:rand.uniform(1_000_000)}.pem")
    key_path = Path.join(dir, "bench_key_#{:rand.uniform(1_000_000)}.pem")

    File.write!(cert_path, cert_pem)
    File.write!(key_path, key_pem)

    {cert_path, key_path}
  end

  defp generate_cert_pem do
    private_key = :public_key.generate_key({:rsa, 2048, 65537})
    {:RSAPrivateKey, _, modulus, pub_exp, _, _, _, _, _, _, _} = private_key
    public_key = {:RSAPublicKey, modulus, pub_exp}

    now = :calendar.universal_time()
    now_seconds = :calendar.datetime_to_gregorian_seconds(now)
    not_before = now
    not_after = :calendar.gregorian_seconds_to_datetime(now_seconds + 365 * 86400)

    serial = :rand.uniform(1_000_000)

    tbs_cert =
      {:OTPTBSCertificate, :v3, serial,
       {:SignatureAlgorithm, {1, 2, 840, 113_549, 1, 1, 11}, :NULL},
       {:rdnSequence, [[{:AttributeTypeAndValue, {2, 5, 4, 3}, {:utf8String, "Benchmark"}}]]},
       {:Validity, {:utcTime, format_utc(not_before)}, {:utcTime, format_utc(not_after)}},
       {:rdnSequence, [[{:AttributeTypeAndValue, {2, 5, 4, 3}, {:utf8String, "Benchmark"}}]]},
       {:OTPSubjectPublicKeyInfo, {:PublicKeyAlgorithm, {1, 2, 840, 113_549, 1, 1, 1}, :NULL}, public_key},
       :asn1_NOVALUE, :asn1_NOVALUE, :asn1_NOVALUE}

    tbs_der = :public_key.pkix_encode(:OTPTBSCertificate, tbs_cert, :otp)
    signature = :public_key.sign(tbs_der, :sha256, private_key)

    cert = {:OTPCertificate, tbs_cert, {:SignatureAlgorithm, {1, 2, 840, 113_549, 1, 1, 11}, :NULL}, signature}
    cert_der = :public_key.pkix_encode(:OTPCertificate, cert, :otp)

    cert_pem = :public_key.pem_encode([{:Certificate, cert_der, :not_encrypted}])
    key_pem = :public_key.pem_encode([{:RSAPrivateKey, :public_key.der_encode(:RSAPrivateKey, private_key), :not_encrypted}])

    {cert_pem, key_pem}
  end

  defp format_utc({{year, month, day}, {hour, min, sec}}) do
    short_year = rem(year, 100)

    :io_lib.format(~c"~2..0B~2..0B~2..0B~2..0B~2..0B~2..0BZ", [short_year, month, day, hour, min, sec])
    |> IO.iodata_to_binary()
    |> String.to_charlist()
  end

  defp print_results(results) do
    hl = results["results"]["handshake_latency"]
    tp = results["results"]["throughput"]
    cv = results["results"]["cert_validation"]

    IO.puts("")
    IO.puts("Handshake Latency:")
    IO.puts("  TCP (baseline):  #{Float.round(hl["tcp_us"], 0)} us")
    IO.puts("  TLS 1.3:         #{Float.round(hl["tls_1_3_us"], 0)} us (+#{hl["overhead_1_3_pct"]}%)")
    IO.puts("  TLS 1.2:         #{Float.round(hl["tls_1_2_us"], 0)} us (+#{hl["overhead_1_2_pct"]}%)")
    IO.puts("")
    IO.puts("Message Throughput (#{tp["message_size_bytes"]}B messages):")
    IO.puts("  TCP (baseline):  #{tp["tcp_msgs_per_sec"]} msgs/sec")
    IO.puts("  TLS 1.3:         #{tp["tls_msgs_per_sec"]} msgs/sec (-#{tp["overhead_pct"]}%)")
    IO.puts("")
    IO.puts("Certificate Validation:")
    IO.puts("  Cert parse:       #{cv["cert_parse_avg_us"]} us/op")
    IO.puts("  Key parse:        #{cv["key_parse_avg_us"]} us/op")
    IO.puts("  Key-cert match:   #{cv["key_cert_match_avg_us"]} us/op")
  end
end

TLSPerformanceBenchmark.run()
