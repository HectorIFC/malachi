defmodule DockerImageHealthCheckTest do
  # scripts/docker-image-health-check.sh is what stops the image's own HEALTHCHECK from breaking unseen
  # again (#282): every compose file overrode the probe, so nothing ever ran the image's. The Docker smoke
  # test in CI and scripts/docker-regression-test.sh run it against a real container; here it runs for
  # real against a PATH whose `docker` answers each `inspect` the script makes with whatever the case
  # needs, so every way it can fail is exercised without Docker. The script bounds its docker calls with
  # the real coreutils `timeout`, which is why this module is Linux only.
  use ExUnit.Case, async: true

  alias Malachi.Test.TmpDir

  @moduletag :linux

  @script Path.expand("../../scripts/docker-image-health-check.sh", __DIR__)
  @probe ~s(["CMD-SHELL","wget -q -O /dev/null http://127.0.0.1:4041/health || exit 1"])
  # start period, interval and timeout in nanoseconds, then the probe: the image's real timings.
  @image_healthcheck "30000000000 30000000000 10000000000 #{@probe}"
  @refused "connect to [::1]:4041: Connection refused"

  setup_all do
    # Missing tools fail loudly instead of skipping: a skipped harness test reads as a passing one.
    for tool <- ~w(bash timeout sleep) do
      System.find_executable(tool) || flunk("#{tool} is required to test scripts/docker-image-health-check.sh")
    end

    :ok
  end

  setup do
    dir = TmpDir.path("docker-image-health-check")
    # Exclusive: a directory already there is a leftover, and building on it would test the leftover.
    File.mkdir!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)

    stub_bin = Path.join(dir, "stub-bin")
    File.mkdir_p!(stub_bin)
    stub_docker!(stub_bin)

    %{stub_bin: stub_bin}
  end

  # A `docker` that answers the script's inspections, told apart by their format templates:
  #
  #   * `{{.Image}}` prints STUB_IMAGE;
  #   * the container's `.Config.Healthcheck` prints STUB_CONTAINER_TEST;
  #   * `image inspect` prints STUB_IMAGE_HEALTHCHECK;
  #   * `.State.Status` prints the next entry of STUB_STATES (`|` separated), repeating the last one once
  #     the list is used up, as a container that stays in a state would;
  #   * `.Log` prints STUB_PROBE_LOG, entries `|` separated, each behind the marker the script splits on.
  #
  # STUB_FAIL_ON names the call (image, container_test, image_healthcheck, state, log) that exits 1, and
  # STUB_HANG_ON the one that hangs instead, as a daemon that does not answer would; `exec` so the check's
  # timeout stops the sleep itself rather than leaving it holding the output pipe. Every call's arguments
  # are appended to docker-args, so a case can assert what was asked.
  defp stub_docker!(stub_bin) do
    path = Path.join(stub_bin, "docker")

    File.write!(path, """
    #!/bin/sh
    printf '%s\\n' "$*" >> "#{stub_bin}/docker-args"
    case "$*" in
      'image inspect'*) call=image_healthcheck ;;
      *'{{.Image}}'*) call=image ;;
      *'.Config.Healthcheck'*) call=container_test ;;
      *'.State.Status'*) call=state ;;
      *'.Log'*) call=log ;;
      *) echo "unexpected docker call: $*" >&2; exit 99 ;;
    esac
    [ "$STUB_HANG_ON" = "$call" ] && exec sleep 30
    [ "$STUB_FAIL_ON" = "$call" ] && { echo "Error: No such object" >&2; exit 1; }
    case "$call" in
      image) printf '%s\\n' "${STUB_IMAGE-sha256:abc}" ;;
      container_test) printf '%s\\n' "$STUB_CONTAINER_TEST" ;;
      image_healthcheck) printf '%s\\n' "$STUB_IMAGE_HEALTHCHECK" ;;
      state)
        count_file="#{stub_bin}/state-count"
        n=$(( $(cat "$count_file" 2>/dev/null || echo 0) + 1 ))
        echo "$n" > "$count_file"
        total=$(printf '%s' "$STUB_STATES" | tr '|' '\\n' | wc -l)
        total=$((total + 1))
        [ "$n" -le "$total" ] || n=$total
        printf '%s' "$STUB_STATES" | tr '|' '\\n' | sed -n "${n}p"
        ;;
      log) [ -n "$STUB_PROBE_LOG" ] && printf '<probe>%s' "$STUB_PROBE_LOG" | sed 's/|/<probe>/g' ;;
    esac
    exit 0
    """)

    File.chmod!(path, 0o755)
  end

  defp run(ctx, args, env \\ []) do
    path = "#{ctx.stub_bin}:#{System.get_env("PATH")}"
    System.cmd("bash", [@script | args], env: [{"PATH", path} | env], stderr_to_stdout: true)
  end

  # A container built from the image as it is, with no healthcheck override, going through `states`.
  defp run_container(ctx, states, env \\ []) do
    defaults = [
      {"STUB_CONTAINER_TEST", @probe},
      {"STUB_IMAGE_HEALTHCHECK", @image_healthcheck},
      {"STUB_STATES", Enum.join(states, "|")},
      {"IMAGE_HEALTH_POLL", "1"}
    ]

    run(ctx, ["malachi-test"], defaults |> Map.new() |> Map.merge(Map.new(env)) |> Enum.to_list())
  end

  defp state_reads(ctx) do
    ctx.stub_bin
    |> Path.join("docker-args")
    |> File.read!()
    |> String.split("\n", trim: true)
    |> Enum.count(&String.contains?(&1, ".State.Status"))
  end

  test "passes once the container reports healthy under the image's own probe", ctx do
    assert {out, 0} = run_container(ctx, ["running starting", "running healthy"])
    assert out =~ "waiting up to 70s for container malachi-test to report healthy under the image HEALTHCHECK"
    assert out =~ "container malachi-test reported healthy after"
    assert state_reads(ctx) == 2

    args = File.read!(Path.join(ctx.stub_bin, "docker-args"))
    assert args =~ "inspect -f {{.Image}} malachi-test\n"
    assert args =~ "image inspect -f"
    assert args =~ "sha256:abc\n"
  end

  test "fails at once when the container reports unhealthy, with the last probe's output", ctx do
    env = [{"STUB_PROBE_LOG", "first try|#{@refused}"}]

    assert {out, 1} = run_container(ctx, ["running starting", "running unhealthy", "running healthy"], env)
    assert out =~ "image health check failed: container malachi-test reported unhealthy after"
    assert out =~ "last probe output: #{@refused}"
    refute out =~ "first try"
    assert state_reads(ctx) == 2
  end

  test "fails at once when the container stops running", ctx do
    assert {out, 1} = run_container(ctx, ["running starting", "exited "])
    assert out =~ "image health check failed: container malachi-test is exited, not running, after"
  end

  test "fails at once when the container is restarting after a crash", ctx do
    assert {out, 1} = run_container(ctx, ["restarting starting"])
    assert out =~ "container malachi-test is restarting, not running"
  end

  test "fails within its budget when the container never leaves starting, which is the bug on main", ctx do
    env = [{"STUB_PROBE_LOG", @refused}, {"IMAGE_HEALTH_TIMEOUT", "1"}]

    {elapsed_us, result} = :timer.tc(fn -> run_container(ctx, ["running starting"], env) end)

    assert {out, 1} = result
    assert out =~ "waiting up to 1s"
    assert out =~ ~r/container malachi-test is still starting after \ds, past the 1s budget/
    assert out =~ "last probe output: #{@refused}"
    assert elapsed_us < 5_000_000, "took #{div(elapsed_us, 1000)} ms against a 1 s budget"
  end

  test "says so when no probe has run by the time the budget is spent", ctx do
    assert {out, 1} = run_container(ctx, ["running starting"], [{"IMAGE_HEALTH_TIMEOUT", "1"}])
    assert out =~ "no probe has run yet"
  end

  test "names a container without a health status rather than printing nothing", ctx do
    assert {out, 1} = run_container(ctx, ["running "], [{"IMAGE_HEALTH_TIMEOUT", "1"}])
    assert out =~ "is still without a health status after"
  end

  test "still fails with its reason when the probe log cannot be read", ctx do
    env = [{"STUB_FAIL_ON", "log"}]

    assert {out, 1} = run_container(ctx, ["running unhealthy"], env)
    assert out =~ "reported unhealthy after"
    refute out =~ "last probe output"
    # An unreadable log is not an empty one: saying no probe ran would be a false diagnostic.
    refute out =~ "no probe has run yet"
  end

  test "budgets the image's start period plus one interval plus one probe timeout", ctx do
    env = [{"STUB_IMAGE_HEALTHCHECK", "5000000000 2000000000 1500000000 #{@probe}"}]

    assert {out, 0} = run_container(ctx, ["running healthy"], env)
    # 8.5 s, rounded up so a fraction of a second is still waited for.
    assert out =~ "waiting up to 9s"
  end

  test "reads zero timings as Docker's defaults, interval 30s and timeout 30s with no start period", ctx do
    env = [{"STUB_IMAGE_HEALTHCHECK", "0 0 0 #{@probe}"}]

    assert {out, 0} = run_container(ctx, ["running healthy"], env)
    assert out =~ "waiting up to 60s"
  end

  test "fails naming the timings when the image's are not whole nanoseconds", ctx do
    # One case per field, plus a template value that `read` splits into two words. Before the check, a word
    # in the interval or the timeout fell back to Docker's default and the run passed on the wrong budget.
    cases = [
      {"abc 30000000000 10000000000", "abc 30000000000 10000000000"},
      {"30000000000 x 10000000000", "30000000000 x 10000000000"},
      {"30000000000 30000000000 x", "30000000000 30000000000 x"},
      {"30000000000 <no value> 10000000000", "30000000000 <no value>"},
      # Past the int64 range, by length and at the maximum's own length, and a leading zero bash reads as
      # octal: each used to reach the zero test and fall over to Docker's 30s default.
      {"30000000000 99999999999999999999 10000000000", "30000000000 99999999999999999999 10000000000"},
      {"30000000000 30000000000 9223372036854775808", "30000000000 30000000000 9223372036854775808"},
      {"30000000000 010 10000000000", "30000000000 010 10000000000"}
    ]

    for {timings, shown} <- cases do
      env = [{"STUB_IMAGE_HEALTHCHECK", "#{timings} #{@probe}"}]

      assert {out, 1} = run_container(ctx, ["running healthy"], env)

      assert out =~
               "image health check failed: could not read the HEALTHCHECK timings of image sha256:abc, got '#{shown}'"

      refute out =~ "waiting up to"
      refute out =~ "overrides the image HEALTHCHECK"
    end
  end

  test "budgets a start period at the int64 maximum without overflowing", ctx do
    # Docker accepts a start period this long. Summed in nanoseconds it wrapped to a negative budget and the
    # check failed at once; per timing in seconds it is 9223372037 + 30 + 10.
    env = [{"STUB_IMAGE_HEALTHCHECK", "9223372036854775807 30000000000 10000000000 #{@probe}"}]

    assert {out, 0} = run_container(ctx, ["running healthy"], env)
    assert out =~ "waiting up to 9223372077s"
  end

  test "accepts a zero timing, which is not a leading zero", ctx do
    env = [{"STUB_IMAGE_HEALTHCHECK", "0 30000000000 10000000000 #{@probe}"}]

    assert {out, 0} = run_container(ctx, ["running healthy"], env)
    assert out =~ "waiting up to 40s"
  end

  test "IMAGE_HEALTH_TIMEOUT replaces the budget read from the image", ctx do
    assert {out, 0} = run_container(ctx, ["running healthy"], [{"IMAGE_HEALTH_TIMEOUT", "99999"}])
    assert out =~ "waiting up to 99999s"
  end

  test "refuses a container whose probe overrides the image's, which would prove nothing", ctx do
    override = ~s(["CMD","wget","-q","-O","/dev/null","http://127.0.0.1:4041/health"])

    assert {out, 1} = run_container(ctx, ["running healthy"], [{"STUB_CONTAINER_TEST", override}])
    assert out =~ "image health check failed: container malachi-test overrides the image HEALTHCHECK"
    assert out =~ "it probes #{override}, the image probes #{@probe}"
    assert state_reads(ctx) == 0
  end

  test "refuses a container started with --no-healthcheck", ctx do
    assert {out, 1} = run_container(ctx, ["running healthy"], [{"STUB_CONTAINER_TEST", ~s(["NONE"])}])
    assert out =~ ~s(it probes ["NONE"], the image probes)
  end

  test "fails when the image has no HEALTHCHECK at all", ctx do
    for image_healthcheck <- ["", ~s(0 0 0 ["NONE"])] do
      env = [{"STUB_IMAGE_HEALTHCHECK", image_healthcheck}, {"STUB_CONTAINER_TEST", ""}]

      assert {out, 1} = run_container(ctx, ["running healthy"], env)
      assert out =~ "image health check failed: image sha256:abc has no HEALTHCHECK"
    end
  end

  test "fails naming what it could not read when a docker call errors", ctx do
    cases = [
      {"image", "could not read the image of container malachi-test"},
      {"container_test", "could not read the healthcheck of container malachi-test"},
      {"image_healthcheck", "could not read the HEALTHCHECK of image sha256:abc"},
      {"state", "could not read the health status of container malachi-test"}
    ]

    for {call, message} <- cases do
      assert {out, 1} = run_container(ctx, ["running healthy"], [{"STUB_FAIL_ON", call}])
      assert out =~ "image health check failed: #{message}"
      refute out =~ "reported healthy"
    end
  end

  test "fails within its limit when docker does not answer", ctx do
    env = [{"STUB_HANG_ON", "state"}, {"IMAGE_HEALTH_EXEC_TIMEOUT", "1"}]

    {elapsed_us, result} = :timer.tc(fn -> run_container(ctx, ["running healthy"], env) end)

    assert {out, 1} = result
    assert out =~ "the health status of container malachi-test did not finish within 1s"
    assert elapsed_us < 5_000_000, "took #{div(elapsed_us, 1000)} ms against a 1 s limit"
  end

  test "refuses 0, a leading zero, a fraction, a negative, text and more than five digits", ctx do
    vars = ~w(IMAGE_HEALTH_TIMEOUT IMAGE_HEALTH_EXEC_TIMEOUT IMAGE_HEALTH_POLL)

    for var <- vars, value <- ["0", "05", "1.5", "-3", "abc", "100000"] do
      assert {out, 2} = run(ctx, ["malachi-test"], [{var, value}])
      assert out =~ "#{var} must be a whole number of seconds from 1 to 99999 without leading zeros, got '#{value}'"
    end
  end

  test "refuses a limit that is set but empty rather than falling back to the default", ctx do
    # Set inside the shell: System.cmd reads an empty env value as "unset the variable", which would
    # test the default instead.
    for var <- ~w(IMAGE_HEALTH_TIMEOUT IMAGE_HEALTH_EXEC_TIMEOUT IMAGE_HEALTH_POLL) do
      path = "#{ctx.stub_bin}:#{System.get_env("PATH")}"
      command = ~s(export "$1="; exec bash "$2" malachi-test)

      assert {out, 2} =
               System.cmd("bash", ["-c", command, "bash", var, @script],
                 env: [{"PATH", path}],
                 stderr_to_stdout: true
               )

      assert out =~ "#{var} must be a whole number of seconds from 1 to 99999 without leading zeros, got ''"
    end
  end

  test "names the missing tool when coreutils timeout is not on PATH", ctx do
    # Only the stub bin: bash itself is resolved by System.cmd, and the script stops before any other tool.
    assert {out, 2} =
             System.cmd("bash", [@script, "malachi-test"], env: [{"PATH", ctx.stub_bin}], stderr_to_stdout: true)

    assert out =~ "this check requires coreutils timeout, which is not on PATH"
    refute File.exists?(Path.join(ctx.stub_bin, "docker-args"))
  end

  test "finds its helper when run by bare name from the scripts directory", ctx do
    # $0 has no slash then, which is the one way to reach the fallback that resolves the helper from `.`.
    path = "#{ctx.stub_bin}:#{System.get_env("PATH")}"

    env = [
      {"PATH", path},
      {"STUB_CONTAINER_TEST", @probe},
      {"STUB_IMAGE_HEALTHCHECK", @image_healthcheck},
      {"STUB_STATES", "running healthy"}
    ]

    assert {out, 0} =
             System.cmd("bash", [Path.basename(@script), "malachi-test"],
               cd: Path.dirname(@script),
               env: env,
               stderr_to_stdout: true
             )

    assert out =~ "container malachi-test reported healthy after"
  end

  test "refuses to run without exactly one container", ctx do
    for args <- [[], [""], ["a", "b"]] do
      assert {out, 2} = run(ctx, args)
      assert out =~ "usage:"
    end
  end
end
