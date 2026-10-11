defmodule Malachi.ApplicationGroupOptsTest do
  # What every group coordinator is started with (`Malachi.Application.group_coordinator_opts/1`): the
  # session `MALACHI_GROUP_SESSION_MS` sets, checked, and the control plane's refusal of the group fence's
  # commands before an upgrade is finalized, answered as `unsupported`.
  #
  # async: false. The session is read from the application environment, which this suite sets.
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Malachi.Application, as: App
  alias Malachi.Cluster.MetadataMachine
  alias Malachi.Metadata

  setup do
    previous = Application.fetch_env(:malachi, :group_session_ms)

    on_exit(fn ->
      case previous do
        {:ok, value} -> Application.put_env(:malachi, :group_session_ms, value)
        :error -> Application.delete_env(:malachi, :group_session_ms)
      end
    end)
  end

  defp session_ms, do: App.group_coordinator_opts(:any_name)[:session_ms]

  test "the session is the configured one, and the default without one" do
    Application.put_env(:malachi, :group_session_ms, 1_234)
    assert session_ms() == 1_234
    assert App.group_coordinator_opts(:coordinator_name)[:name] == :coordinator_name

    Application.delete_env(:malachi, :group_session_ms)
    assert session_ms() == 30_000
  end

  test "a session that is not a positive count fitting the wire's 32 bits is the default, and says so" do
    for bad <- [0, -1, 0x100000000, "5000"] do
      Application.put_env(:malachi, :group_session_ms, bad)
      log = capture_log(fn -> assert session_ms() == 30_000 end)
      assert log =~ "group_session_ms", "#{inspect(bad)}"
    end

    Application.put_env(:malachi, :group_session_ms, 0xFFFFFFFF)
    assert session_ms() == 0xFFFFFFFF
  end

  test "every coordinator gets the ranges, the ownership guard and the epoch opener" do
    opts = App.group_coordinator_opts(:any_name)
    assert is_function(opts[:ranges_fun], 1)
    assert is_function(opts[:owns_fun], 1)
    assert is_function(opts[:open_fun], 2)
  end

  test "a control plane below the group fence's version is unsupported to the group keys" do
    # what the metadata machine of a group still at 6 answers both commands
    meta = %{machine_version: 6, index: 1, term: 1, system_time: 1_700_000_000_000}

    for command <- [{:open_group, "g", "t"}, {:commit_offset, "g", "t", %{}, 1}] do
      {_state, refused} = MetadataMachine.apply(meta, command, Metadata.new())
      assert App.group_command_reply(refused) == {:error, :unsupported}, inspect(command)
    end

    assert App.group_command_reply({:ok, 3}) == {:ok, 3}
    assert App.group_command_reply({:error, :timeout}) == {:error, :timeout}
  end
end
