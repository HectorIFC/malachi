defmodule DockerClusterSummaryTest do
  # benchmark/docker-cluster-summary.jq turns the dispatch job's case lines into the published verdict,
  # so it decides whether a disk against tmpfs difference is claimed. Only jq is needed to check it.
  use ExUnit.Case, async: true

  @moduletag :tmp_dir

  @program Path.expand("../../benchmark/docker-cluster-summary.jq", __DIR__)

  setup_all do
    %{jq: System.find_executable("jq") || flunk("jq is required to test benchmark/docker-cluster-summary.jq")}
  end

  test "reports median and range per RF and mode, and claims a difference only for disjoint ranges", ctx do
    lines =
      summarize!(ctx, [
        kase("tmpfs", 1, 300_000, 5),
        kase("disk", 1, 250_000, 7),
        kase("tmpfs", 1, 320_000, 4),
        kase("disk", 1, 310_000, 8),
        kase("tmpfs", 1, 310_000, 6),
        kase("disk", 1, 260_000, 9),
        kase("tmpfs", 3, 90_000, 20),
        kase("disk", 3, 20_000, 80),
        kase("tmpfs", 3, 100_000, 22),
        kase("disk", 3, 25_000, 90)
      ])

    assert "| 1 | disk | 3 | 260000 (250000 to 310000) | 1 (1 to 1) | 8 (7 to 9) | 100 (100 to 100) | 400 (400 to 400) | ext4 |" in lines

    assert "| 1 | tmpfs | 3 | 310000 (300000 to 320000) | 1 (1 to 1) | 5 (4 to 6) | 100 (100 to 100) | 400 (400 to 400) | tmpfs |" in lines

    # An even count takes the mean of the two middle values.
    assert "| 3 | disk | 2 | 22500 (20000 to 25000) | 1 (1 to 1) | 85 (80 to 90) | 100 (100 to 100) | 400 (400 to 400) | ext4 |" in lines

    assert ("- RF 1: rec/s: ranges overlap, no difference is claimed (tmpfs 310000 (300000 to 320000), " <>
              "disk 260000 (250000 to 310000)); p99 ms: ranges are disjoint (tmpfs 5 (4 to 6), disk 8 (7 to 9)); " <>
              "flush p50 us: ranges overlap, no difference is claimed (tmpfs 100 (100 to 100), disk 100 (100 to 100)); " <>
              "flush p99 us: ranges overlap, no difference is claimed (tmpfs 400 (400 to 400), disk 400 (400 to 400))") in lines

    assert Enum.any?(lines, &String.starts_with?(&1, "- RF 3: rec/s: ranges are disjoint"))
    assert "Host: Linux 6.8.0 x86_64" in lines
    assert "Volumes: ext4 rw,relatime on /dev/sda1 (disk sda rota=false model=Stub Disk)" in lines
    refute "Failed cases:" in lines
  end

  test "a single run of a mode claims nothing, however far apart the numbers are", ctx do
    lines =
      summarize!(ctx, [
        kase("tmpfs", 1, 450_000, 70),
        kase("disk", 1, 220_000, 150),
        kase("tmpfs", 1, 440_000, 72)
      ])

    assert ("- RF 1: rec/s: a single run of a mode has no noise floor, no difference is claimed " <>
              "(tmpfs 445000 (440000 to 450000), disk 220000 (220000 to 220000)); p99 ms: a single run of a mode " <>
              "has no noise floor, no difference is claimed (tmpfs 71 (70 to 72), disk 150 (150 to 150)); " <>
              "flush p50 us: a single run of a mode has no noise floor, no difference is claimed " <>
              "(tmpfs 100 (100 to 100), disk 100 (100 to 100)); flush p99 us: a single run of a mode has no noise " <>
              "floor, no difference is claimed (tmpfs 400 (400 to 400), disk 400 (400 to 400))") in lines
  end

  test "a failed check or a missing result is listed and never measured", ctx do
    lines =
      summarize!(ctx, [
        kase("tmpfs", 1, 300_000, 5),
        %{kase("disk", 1, 1, 1) | "outcome" => "preallocation missing"},
        %{kase("disk", 3, 1, 1) | "outcome" => "timeout after 300s", "loadtest" => nil},
        %{kase("tmpfs", 3, 1, 1) | "outcome" => "no json", "loadtest" => nil}
      ])

    assert "| 1 | tmpfs | 1 | 300000 (300000 to 300000) | 1 (1 to 1) | 5 (5 to 5) | 100 (100 to 100) | 400 (400 to 400) | tmpfs |" in lines
    refute Enum.any?(lines, &String.starts_with?(&1, "| 1 | disk"))
    assert Enum.any?(lines, &String.starts_with?(&1, "- RF 1: rec/s: not comparable, one mode has no result"))
    refute Enum.any?(lines, &String.starts_with?(&1, "- RF 3:"))

    assert [
             "Failed cases:",
             "- RF 1, disk: preallocation missing",
             "- RF 3, disk: timeout after 300s",
             "- RF 3, tmpfs: no json"
           ] ==
             Enum.drop_while(lines, &(&1 != "Failed cases:"))
  end

  test "errors lower the number but the case still counts", ctx do
    lines = summarize!(ctx, [%{kase("disk", 3, 5_000, 900) | "outcome" => "ok with errors"}])

    assert "| 3 | disk | 1 | 5000 (5000 to 5000) | 1 (1 to 1) | 900 (900 to 900) | 100 (100 to 100) | 400 (400 to 400) | ext4 |" in lines
  end

  test "a run where nothing produced a result says so", ctx do
    lines = summarize!(ctx, [%{kase("disk", 1, 1, 1) | "outcome" => "unhealthy", "loadtest" => nil, "nodes" => %{}}])
    assert "- no case produced a result" in lines
    assert "- RF 1, disk: unhealthy" in lines
  end

  describe "per-flush latency" do
    test "compares the modes' flush p50 and p99, claiming only disjoint ranges", ctx do
      lines =
        summarize!(ctx, [
          flushed(kase("tmpfs", 1, 300_000, 5), 20.4e-6, 60.0e-6),
          flushed(kase("disk", 1, 250_000, 7), 180.0e-6, 381.0e-6),
          flushed(kase("tmpfs", 1, 310_000, 5), 21.0e-6, 70.0e-6),
          flushed(kase("disk", 1, 255_000, 7), 190.0e-6, 60.0e-6)
        ])

      assert "| 1 | disk | 2 | 252500 (250000 to 255000) | 1 (1 to 1) | 7 (7 to 7) | 185 (180 to 190) | 220.5 (60 to 381) | ext4 |" in lines

      assert "| 1 | tmpfs | 2 | 305000 (300000 to 310000) | 1 (1 to 1) | 5 (5 to 5) | 20.5 (20 to 21) | 65 (60 to 70) | tmpfs |" in lines

      [verdict] = Enum.filter(lines, &String.starts_with?(&1, "- RF 1:"))
      assert verdict =~ "flush p50 us: ranges are disjoint (tmpfs 20.5 (20 to 21), disk 185 (180 to 190))"

      assert verdict =~
               "flush p99 us: ranges overlap, no difference is claimed (tmpfs 65 (60 to 70), disk 220.5 (60 to 381))"
    end

    test "a measured case without flush latency keeps its throughput and is listed with the reason", ctx do
      lines =
        summarize!(ctx, [
          kase("tmpfs", 1, 300_000, 5),
          %{kase("disk", 1, 250_000, 7) | "flush" => %{"nodes" => %{}, "all" => nil, "error" => "malachi2: HTTP 403"}},
          %{kase("disk", 1, 260_000, 7) | "flush" => nil},
          %{kase("disk", 3, 1, 1) | "outcome" => "no json", "loadtest" => nil, "flush" => nil}
        ])

      assert "| 1 | disk | 2 | 255000 (250000 to 260000) | 1 (1 to 1) | 7 (7 to 7) | n/a | n/a | ext4 |" in lines

      [verdict] = Enum.filter(lines, &String.starts_with?(&1, "- RF 1:"))
      assert verdict =~ "flush p99 us: not comparable, one mode has no result"

      assert ["Measured cases without flush latency:", "- RF 1, disk: malachi2: HTTP 403", "- RF 1, disk: not scraped"] ==
               lines |> Enum.drop_while(&(&1 != "Measured cases without flush latency:")) |> Enum.take(3)
    end

    test "a window without flushes has no latency to compare", ctx do
      lines = summarize!(ctx, [flushed(kase("disk", 1, 250_000, 7), nil, nil)])
      assert "| 1 | disk | 1 | 250000 (250000 to 250000) | 1 (1 to 1) | 7 (7 to 7) | n/a | n/a | ext4 |" in lines
      refute "Measured cases without flush latency:" in lines
    end
  end

  defp flushed(kase, p50, p99), do: put_in(kase, ["flush", "all"], %{"p50" => p50, "p99" => p99, "flushes" => 10})

  defp kase(mode, rf, rate, p99) do
    fstype = if mode == "disk", do: "ext4", else: "tmpfs"

    %{
      "data_mode" => mode,
      "rf" => rf,
      "outcome" => "ok",
      "host" => "Linux 6.8.0 x86_64",
      "docker" => "Ubuntu 24.04, kernel 6.8.0, storage driver overlay2",
      "docker_root_backing" => "ext4 rw,relatime on /dev/sda1 (disk sda rota=false model=Stub Disk)",
      "nodes" => Map.new(~w(malachi1 malachi2 malachi3), &{&1, %{"fstype" => fstype, "mount_options" => "rw"}}),
      "loadtest" => %{"records_per_s" => rate, "latency_ms" => %{"p50" => 1, "p99" => p99}},
      "flush" => %{"nodes" => %{}, "all" => %{"p50" => 100.0e-6, "p99" => 400.0e-6, "flushes" => 10}, "error" => nil}
    }
  end

  defp summarize!(ctx, cases) do
    input = Path.join(ctx.tmp_dir, "cases.jsonl")
    File.write!(input, Enum.map_join(cases, "\n", &Jason.encode!/1) <> "\n")
    {output, 0} = System.cmd(ctx.jq, ["-rs", "-f", @program, input], stderr_to_stdout: true)
    String.split(output, "\n", trim: true)
  end
end
