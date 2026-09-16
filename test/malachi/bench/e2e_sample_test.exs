Code.require_file("../../../benchmark/support/e2e_sample.exs", __DIR__)

defmodule Malachi.Bench.E2ESampleTest do
  use ExUnit.Case, async: true

  alias Malachi.Bench.E2ESample

  @benchmark Path.expand("../../../benchmark", __DIR__)

  # The report throughput_1m.exs prints, as captured from a real run, with the mix log lines around it.
  @report """
  12:13:33.648 [notice] candidate -> leader in term: 1
  Producing 1000000 records (100B value each, batches of 1000)...
  Consuming 1000000 records (fetch by cursor, pages of 1000)...

  ============ 1M-message end-to-end (BrokerServer + ReplicationServer, single node) ============
  PRODUCE   1000000 recs in 1.83s  =>  546688 rec/s, 74.8 MB/s
    batch latency (1000/batch) us:  p50=1734  p99=3932  max=21442
  CONSUME   1000000 recs in 2.22s  =>  450050 rec/s
    page latency (1000/page) us:    p50=1956  p99=11571  max=29497

  DISK      136.8 MB on disk  =>  143.4 bytes/record (payload 100B)
  ==============================================================================================
  """

  @regime "REGIME    batch 1000 x 100B (97.7KB of values per request, group commit off), " <>
            "segment preallocation off, on ext4"

  test "reads the produce p50 and p99 from a real report" do
    assert E2ESample.parse(@report) == %{p50: 1734, p99: 3932}
  end

  test "a regime line printed before the latency line is not read in its place" do
    output = String.replace(@report, "PRODUCE", @regime <> "\nPRODUCE")

    assert E2ESample.parse(output) == %{p50: 1734, p99: 3932}
  end

  test "a line naming a batch and percentiles that is not the produce latency line is not read" do
    output = @report |> drop_produce_line() |> Kernel.<>("REGIME batch 1000 x 100B p50=1 p99=2\n")

    assert_raise RuntimeError, ~r/without a produce latency line/, fn -> E2ESample.parse(output) end
  end

  test "text that the earlier unanchored pattern accepted is refused" do
    output = drop_produce_line(@report) <> "note: batch latency drift p50=1 p99=2\n"

    assert_raise RuntimeError, ~r/without a produce latency line/, fn -> E2ESample.parse(output) end
  end

  test "the consume page latency alone is not a produce sample" do
    assert_raise RuntimeError, ~r/without a produce latency line/, fn ->
      E2ESample.parse(drop_produce_line(@report))
    end
  end

  test "a run that died before its report raises with the output attached" do
    output = "** (MatchError) no match of right hand side value: {:error, :enospc}\n"

    error = assert_raise RuntimeError, fn -> E2ESample.parse(output) end
    assert error.message =~ "{:error, :enospc}"
  end

  test "two produce latency lines are ambiguous and raise" do
    output = @report <> "  batch latency (1000/batch) us:  p50=1  p99=2  max=3\n"

    assert_raise RuntimeError, ~r/2 produce latency lines, expected one/, fn -> E2ESample.parse(output) end
  end

  test "store_error_path_ab.sh copies every support file store_error_path_ab.exs loads into the baseline tree" do
    # The baseline tree is an older checkout, so a support file the harness loads and the shell script does
    # not copy only fails in the benchmark workflow, tens of minutes in.
    required =
      ~r/Code\.require_file\("(support\/[^"]+)"/
      |> Regex.scan(File.read!(Path.join(@benchmark, "store_error_path_ab.exs")), capture: :all_but_first)
      |> List.flatten()

    assert "support/e2e_sample.exs" in required

    shell = File.read!(Path.join(@benchmark, "store_error_path_ab.sh"))

    for file <- required do
      assert shell =~ ~s(cp "$BRANCH/benchmark/#{file}" "$BASELINE/benchmark/#{file}"),
             "store_error_path_ab.sh does not copy #{file} into the baseline tree"
    end
  end

  defp drop_produce_line(output) do
    output |> String.split("\n") |> Enum.reject(&(&1 =~ "batch latency")) |> Enum.join("\n")
  end
end
