defmodule DockerStaticAssetsCheckTest do
  # scripts/docker-static-assets-check.sh is what stops the image from shipping without priv/static
  # again (#226), and what catches a copy of priv as a whole carrying development keys into it. The
  # Docker smoke test in CI and scripts/docker-regression-test.sh both run it against a real container;
  # here it runs for real against a loopback HTTP stub standing in for the dashboard, with a PATH whose
  # `docker` answers `exec` with whatever listing the case needs, so every way it can fail is exercised
  # without Docker. The script bounds its exec with the real coreutils `timeout`, which is why this module
  # is Linux only.
  use ExUnit.Case, async: true

  alias Malachi.Test.TmpDir

  @moduletag :linux

  @script Path.expand("../../scripts/docker-static-assets-check.sh", __DIR__)
  @logo File.read!(Path.expand("../../priv/static/logo.svg", __DIR__))
  @svg "image/svg+xml; charset=utf-8"

  setup_all do
    # Missing tools fail loudly instead of skipping: a skipped harness test reads as a passing one.
    for tool <- ~w(bash curl cmp timeout) do
      System.find_executable(tool) || flunk("#{tool} is required to test scripts/docker-static-assets-check.sh")
    end

    :ok
  end

  setup do
    dir = TmpDir.path("docker-static-assets-check")
    # Exclusive: a directory already there is a leftover, and building on it would test the leftover.
    File.mkdir!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)

    stub_bin = Path.join(dir, "stub-bin")
    File.mkdir_p!(stub_bin)
    stub_docker!(stub_bin)

    %{stub_bin: stub_bin}
  end

  # A `docker` that, for `exec <container> sh -c <command>`, prints STUB_PRIV_LISTING (one entry per
  # line, given with `|` as the separator) and exits with STUB_DOCKER_STATUS, and records its arguments
  # so a case can assert what it was asked. With STUB_DOCKER_SLEEP it instead hangs that many seconds,
  # as a container that does not answer exec would; `exec` so the check's timeout stops the sleep itself
  # rather than leaving it holding the output pipe.
  defp stub_docker!(stub_bin) do
    path = Path.join(stub_bin, "docker")

    File.write!(path, """
    #!/bin/sh
    printf '%s\\n' "$*" > "#{stub_bin}/docker-args"
    [ -n "$STUB_DOCKER_SLEEP" ] && exec sleep "$STUB_DOCKER_SLEEP"
    [ -n "$STUB_PRIV_LISTING" ] && printf '%s\\n' "$STUB_PRIV_LISTING" | tr '|' '\\n'
    exit "${STUB_DOCKER_STATUS:-0}"
    """)

    File.chmod!(path, 0o755)
  end

  # Serves one canned HTTP response to every connection on a loopback ephemeral port, and returns the
  # base URL. The listener belongs to the test process, so it closes with the test.
  defp serve!(status, content_type, body) do
    {:ok, listen} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true, ip: {127, 0, 0, 1}])
    {:ok, port} = :inet.port(listen)

    response = [
      "HTTP/1.1 #{status}\r\nContent-Type: #{content_type}\r\n",
      "Content-Length: #{byte_size(body)}\r\nConnection: close\r\n\r\n",
      body
    ]

    # Any process may accept on a listen socket; the test process keeps owning it, so it closes (and
    # the acceptor returns) when the test ends.
    spawn_link(fn -> accept_loop(listen, response) end)

    "http://127.0.0.1:#{port}"
  end

  # Accepts connections on a loopback ephemeral port and never answers, as a dashboard that stalls
  # mid-request would. Returns the base URL.
  defp serve_nothing! do
    {:ok, listen} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true, ip: {127, 0, 0, 1}])
    {:ok, port} = :inet.port(listen)
    spawn_link(fn -> hold_loop(listen, []) end)
    "http://127.0.0.1:#{port}"
  end

  defp hold_loop(listen, held) do
    case :gen_tcp.accept(listen) do
      {:ok, socket} -> hold_loop(listen, [socket | held])
      {:error, :closed} -> :ok
    end
  end

  defp accept_loop(listen, response) do
    case :gen_tcp.accept(listen) do
      {:ok, socket} ->
        {:ok, _request} = :gen_tcp.recv(socket, 0, 5_000)
        :ok = :gen_tcp.send(socket, response)
        :gen_tcp.close(socket)
        accept_loop(listen, response)

      {:error, :closed} ->
        :ok
    end
  end

  defp run(ctx, args, env \\ []) do
    path = "#{ctx.stub_bin}:#{System.get_env("PATH")}"
    System.cmd("bash", [@script | args], env: [{"PATH", path} | env], stderr_to_stdout: true)
  end

  defp run_against(ctx, url, env \\ []),
    do: run(ctx, ["malachi-test", url], [{"STUB_PRIV_LISTING", "static"} | env])

  test "passes when the logo is served as the repository has it and priv holds only static", ctx do
    url = serve!("200 OK", @svg, @logo)

    assert {out, 0} = run_against(ctx, url)
    assert out =~ "logo served: 200, #{@svg}, identical to the repository copy"
    assert out =~ "release priv directory holds only static"

    assert File.read!(Path.join(ctx.stub_bin, "docker-args")) ==
             "exec malachi-test sh -c ls -1A /app/lib/malachi-*/priv\n"
  end

  test "a trailing slash on the dashboard URL is tolerated", ctx do
    url = serve!("200 OK", @svg, @logo)
    assert {_out, 0} = run_against(ctx, url <> "/")
  end

  test "fails on a 404, which is what an image without priv/static answers", ctx do
    url = serve!("404 Not Found", "application/json", ~s({"s":"err","reason":"not_found"}))

    assert {out, 1} = run_against(ctx, url)
    assert out =~ "GET /logo.svg answered status 404, expected 200"
  end

  test "fails when the logo comes back with another content type", ctx do
    url = serve!("200 OK", "text/html", @logo)

    assert {out, 1} = run_against(ctx, url)
    assert out =~ "GET /logo.svg answered content type 'text/html', expected image/svg+xml"
  end

  test "fails when the body is not the repository's logo", ctx do
    url = serve!("200 OK", @svg, "<svg></svg>")

    assert {out, 1} = run_against(ctx, url)
    assert out =~ "GET /logo.svg body differs from"
  end

  test "fails when priv carries anything besides static, such as development keys", ctx do
    url = serve!("200 OK", @svg, @logo)

    assert {out, 1} = run_against(ctx, url, [{"STUB_PRIV_LISTING", "dist_cert|static"}])
    assert out =~ "release priv directory holds 'dist_cert static', expected only static"
  end

  test "fails when priv is empty", ctx do
    url = serve!("200 OK", @svg, @logo)

    assert {out, 1} = run_against(ctx, url, [{"STUB_PRIV_LISTING", ""}])
    assert out =~ "release priv directory holds '', expected only static"
  end

  test "fails when the container cannot be inspected", ctx do
    url = serve!("200 OK", @svg, @logo)

    assert {out, 1} = run_against(ctx, url, [{"STUB_DOCKER_STATUS", "1"}])
    assert out =~ "could not list the release priv directory in container malachi-test"
  end

  test "fails when nothing answers at the dashboard URL", ctx do
    {:ok, listen} = :gen_tcp.listen(0, ip: {127, 0, 0, 1})
    {:ok, port} = :inet.port(listen)
    :ok = :gen_tcp.close(listen)

    assert {out, 1} = run_against(ctx, "http://127.0.0.1:#{port}")
    assert out =~ "could not reach http://127.0.0.1:#{port}/logo.svg"
  end

  test "fails within its limit when the dashboard accepts the request and never answers", ctx do
    url = serve_nothing!()

    {elapsed_us, result} = :timer.tc(fn -> run_against(ctx, url, [{"STATIC_ASSETS_HTTP_TIMEOUT", "1"}]) end)

    assert {out, 1} = result
    assert out =~ "GET /logo.svg did not complete within 1s"
    assert elapsed_us < 5_000_000, "took #{div(elapsed_us, 1000)} ms against a 1 s limit"
  end

  test "fails within its limit when the container does not answer exec", ctx do
    url = serve!("200 OK", @svg, @logo)
    env = [{"STUB_DOCKER_SLEEP", "30"}, {"STATIC_ASSETS_EXEC_TIMEOUT", "1"}]

    {elapsed_us, result} = :timer.tc(fn -> run_against(ctx, url, env) end)

    assert {out, 1} = result
    assert out =~ "listing the release priv directory in container malachi-test did not finish within 1s"
    assert elapsed_us < 5_000_000, "took #{div(elapsed_us, 1000)} ms against a 1 s limit"
  end

  test "refuses 0, which both curl and timeout read as no limit, and a leading zero", ctx do
    for var <- ~w(STATIC_ASSETS_HTTP_TIMEOUT STATIC_ASSETS_EXEC_TIMEOUT), value <- ["0", "05"] do
      assert {out, 2} = run(ctx, ["malachi-test", "http://127.0.0.1:1"], [{var, value}])
      assert out =~ "#{var} must be a whole number of seconds from 1 to 99999 without leading zeros, got '#{value}'"
    end
  end

  test "refuses a timeout longer than five digits, which curl rejects outright", ctx do
    for var <- ~w(STATIC_ASSETS_HTTP_TIMEOUT STATIC_ASSETS_EXEC_TIMEOUT), value <- ["100000", "99999999999999999999"] do
      assert {out, 2} = run(ctx, ["malachi-test", "http://127.0.0.1:1"], [{var, value}])
      assert out =~ "#{var} must be a whole number of seconds from 1 to 99999 without leading zeros, got '#{value}'"
    end
  end

  test "accepts the largest allowed timeout", ctx do
    url = serve!("200 OK", @svg, @logo)
    env = [{"STATIC_ASSETS_HTTP_TIMEOUT", "99999"}, {"STATIC_ASSETS_EXEC_TIMEOUT", "99999"}]
    assert {_out, 0} = run_against(ctx, url, env)
  end

  test "refuses a timeout that is not a whole number of seconds", ctx do
    for var <- ~w(STATIC_ASSETS_HTTP_TIMEOUT STATIC_ASSETS_EXEC_TIMEOUT), value <- ["abc", "1.5", "-3"] do
      assert {out, 2} = run(ctx, ["malachi-test", "http://127.0.0.1:1"], [{var, value}])
      assert out =~ "#{var} must be a whole number of seconds from 1 to 99999 without leading zeros, got '#{value}'"
    end
  end

  test "names the missing tool when coreutils timeout is not on PATH", ctx do
    # Only the stub bin: bash itself is resolved by System.cmd, and the script stops before any other tool.
    assert {out, 2} =
             System.cmd("bash", [@script, "malachi-test", "http://127.0.0.1:1"],
               env: [{"PATH", ctx.stub_bin}],
               stderr_to_stdout: true
             )

    assert out =~ "this check requires coreutils timeout, which is not on PATH"
    refute out =~ "could not list"
  end

  test "refuses to run without both arguments", ctx do
    for args <- [[], ["malachi-test"], ["", "http://127.0.0.1:1"], ["a", "b", "c"]] do
      assert {out, 2} = run(ctx, args)
      assert out =~ "usage:"
    end
  end
end
