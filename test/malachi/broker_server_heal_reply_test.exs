defmodule Malachi.BrokerServerHealReplyTest do
  @moduledoc """
  The scrubber deletes a copy only after `BrokerServer.apply_heal/2` answers `:ok`, so the reply a caller gets
  must be the control plane's own answer, not an unconditional `:ok`. This goes through the real GenServer call
  and the real control plane: a set change for a segment the control plane does not know is refused by it.
  """
  use ExUnit.Case, async: true

  alias Malachi.BrokerServer

  @moduletag :tmp_dir

  test "apply_heal answers the caller the control plane's refusal, not :ok", %{tmp_dir: directory} do
    {:ok, server} = BrokerServer.start_link(directory, name: unique_name())

    assert BrokerServer.apply_heal(server, [{:set_segment_replicas, {{"events", 0}, 0}, [:b, :a]}]) ==
             {:error, :no_such_segment}
  end

  defp unique_name, do: :"broker_heal_reply_#{System.unique_integer([:positive])}"
end
