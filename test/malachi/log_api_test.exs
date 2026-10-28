defmodule Malachi.LogApiTest do
  # `Malachi.LogApi.create_topic/2`, what the `create_topic` key (`Malachi.Wire` 1) runs: the keyspace is
  # the broker's choice, never the client's.
  use ExUnit.Case, async: true

  import Malachi.Test.TeardownHelper

  alias Malachi.BrokerServer
  alias Malachi.LogApi
  alias Malachi.Metadata

  @moduletag :tmp_dir

  setup %{tmp_dir: directory} do
    {:ok, server} = BrokerServer.start_link(directory)
    on_exit(fn -> stop_quietly(server) end)
    %{server: server}
  end

  test "a topic is created with one active range over a keyspace of 2^8 keys", %{server: server} do
    assert LogApi.create_topic(server, "events") == :ok

    assert [{"events", 0} = root] = BrokerServer.active_range_ids(server, "events")
    metadata = BrokerServer.metadata(server)
    assert Metadata.get_topic(metadata, "events").keyspace_size == 256
    assert %{key_start: 0, key_end: 256} = Metadata.get_range(metadata, root)
  end

  test "a topic is created once: a second create is the broker's refusal", %{server: server} do
    :ok = LogApi.create_topic(server, "events")
    assert LogApi.create_topic(server, "events") == {:error, :already_exists}
  end
end
