defmodule Malachi.HTTP.LimitsTest do
  # Not async: every case rewrites application env the running dashboard and other suites also read.
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Malachi.HTTP.Limits

  @keys [
    :dashboard_max_header_count,
    :dashboard_max_header_line_size,
    :dashboard_max_header_size,
    :dashboard_recv_timeout_ms
  ]

  setup do
    originals = Map.new(@keys, &{&1, Application.fetch_env(:malachi, &1)})
    Enum.each(@keys, &Application.delete_env(:malachi, &1))

    on_exit(fn ->
      Enum.each(originals, fn
        {key, {:ok, value}} -> Application.put_env(:malachi, key, value)
        {key, :error} -> Application.delete_env(:malachi, key)
      end)
    end)
  end

  describe "headers/0" do
    test "unset keys give the documented defaults" do
      assert Limits.headers() == %{count: 50, line: 10_000, total: 32_768}
    end

    test "valid values are taken as configured" do
      Application.put_env(:malachi, :dashboard_max_header_count, 5)
      Application.put_env(:malachi, :dashboard_max_header_line_size, 512)
      Application.put_env(:malachi, :dashboard_max_header_size, 1_024)

      assert Limits.headers() == %{count: 5, line: 512, total: 1_024}
    end

    for {key, bad} <- [
          dashboard_max_header_count: 0,
          dashboard_max_header_count: "50",
          dashboard_max_header_line_size: -1,
          dashboard_max_header_size: 0,
          dashboard_max_header_size: 1.5
        ] do
      test "#{key} = #{inspect(bad)} falls back to its default and names the key" do
        Application.put_env(:malachi, unquote(key), unquote(bad))

        log = capture_log(fn -> assert Limits.headers() == %{count: 50, line: 10_000, total: 32_768} end)

        assert log =~ Atom.to_string(unquote(key))
      end
    end

    test "the line limit is accepted at the 1 MiB ceiling and refused one byte past it" do
      Application.put_env(:malachi, :dashboard_max_header_line_size, 1_048_576)
      assert Limits.headers().line == 1_048_576

      Application.put_env(:malachi, :dashboard_max_header_line_size, 1_048_577)
      log = capture_log(fn -> assert Limits.headers().line == 10_000 end)
      assert log =~ "dashboard_max_header_line_size"
    end
  end

  describe "recv_timeout_ms/0" do
    test "defaults to five seconds" do
      assert Limits.recv_timeout_ms() == 5_000
    end

    test "follows the configured value" do
      Application.put_env(:malachi, :dashboard_recv_timeout_ms, 150)
      assert Limits.recv_timeout_ms() == 150
    end
  end
end
