defmodule Malachi.Test.LoadtestProbesTest do
  # The generator tests count connections and order events through this probe, so a probe that miscounts
  # would make them pass or fail for the wrong reason. Pinned here without a server.
  use ExUnit.Case, async: true

  alias Malachi.Test.LoadtestProbes

  @moduletag :tmp_dir

  test "counts admitted authentications only, oldest first, and leaves the mailbox empty" do
    send(self(), {:auth, :ok, 10})
    send(self(), {:auth, :error, 11})
    send(self(), {:auth, :ok, 12})

    assert LoadtestProbes.successful_auths() == [10, 12]
    refute_received {:auth, _result, _ms}
  end

  test "forwards the server's authentication telemetry to the watching process" do
    probe = LoadtestProbes.watch_auth()

    :telemetry.execute([:malachi, :auth], %{count: 1}, %{result: :ok})
    :telemetry.execute([:malachi, :auth], %{count: 1}, %{result: :error})

    assert [_admitted] = LoadtestProbes.successful_auths()
    assert :ok = LoadtestProbes.stop_auth(probe)
    assert {:error, :not_found} = LoadtestProbes.stop_auth(probe)
  end

  test "reports when a watched file appears, not before", %{tmp_dir: dir} do
    path = Path.join(dir, "marker")
    LoadtestProbes.watch_file(path)

    refute_receive {:file_appeared, ^path, _ms}, 3 * LoadtestProbes.poll_ms()
    File.write!(path, "")
    assert_receive {:file_appeared, ^path, ms}, 1_000
    assert is_integer(ms)
  end
end
