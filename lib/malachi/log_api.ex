defmodule Malachi.LogApi do
  @moduledoc """
  Creating a topic over a `Malachi.BrokerServer`, as the `create_topic` key (`Malachi.Wire` 1) does it.

  A client never chooses a topic's keyspace size: it is internal, and it is what lets a topic's ranges split
  as it grows. Writing and reading go through the stream keys (`Malachi.ProducerStreams`,
  `Malachi.ConsumeStreams`).
  """

  alias Malachi.BrokerServer
  alias Malachi.Metadata

  # Keyspace size 2^8 = 256, leaving room for the topic's range to split as it grows. The client
  # does not choose this (no "partition count" leaks to it).
  @default_keyspace_bits 8

  @doc "Creates `topic`. The client does not specify partitions: the keyspace is internal."
  @spec create_topic(GenServer.server(), Metadata.topic_name()) :: :ok | {:error, term()}
  def create_topic(server, topic) do
    case BrokerServer.create_topic(server, topic, @default_keyspace_bits) do
      {:ok, _root_id} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end
end
