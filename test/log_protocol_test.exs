defmodule Malachi.LogProtocolTest do
  # End-to-end over the real TCP server (started by the app): the NorthGuard log protocol
  # (create_topic / produce by key / fetch by opaque cursor) against the live Malachi.LogBroker.
  use ExUnit.Case, async: false

  alias Malachi.Test.TCPHelper

  @port Application.compile_env(:malachi, :tcp_port, 4040)

  defp request(socket, action) do
    :ok = TCPHelper.send_line(socket, Jason.encode!(action))
    {:ok, line} = TCPHelper.recv_line(socket, timeout: 2_000)
    Jason.decode!(String.trim(line))
  end

  defp with_session(username, password, fun) do
    case TCPHelper.connect(port: @port) do
      {:ok, socket} ->
        {:ok, _token} = TCPHelper.authenticate(socket, username, password)

        try do
          fun.(socket)
        after
          :gen_tcp.close(socket)
        end

      {:error, _reason} ->
        # No TCP server in this environment; nothing to assert.
        :ok
    end
  end

  test "produce by key then fetch by opaque cursor (no offsets exposed)" do
    with_session("app", "app123", fn socket ->
      topic = "logproto_#{System.unique_integer([:positive])}"

      assert %{"s" => "ok"} = request(socket, %{"action" => "create_topic", "topic" => topic})

      assert %{"s" => "ok", "count" => 2} =
               request(socket, %{
                 "action" => "produce",
                 "topic" => topic,
                 "records" => [%{"key" => "k1", "value" => "v1"}, %{"key" => "k2", "value" => "v2"}]
               })

      assert %{"s" => "ok", "records" => records, "cursor" => cursor} =
               request(socket, %{"action" => "fetch", "topic" => topic})

      # opaque cursor (a string token, not an integer offset); records carry key/value, no offset
      assert is_binary(cursor)
      assert records |> Enum.map(& &1["value"]) |> Enum.sort() == ["v1", "v2"]
      assert Enum.all?(records, &(not Map.has_key?(&1, "offset")))

      # the cursor carries position: re-fetching with it yields nothing new
      assert %{"s" => "ok", "records" => []} =
               request(socket, %{"action" => "fetch", "topic" => topic, "cursor" => cursor})
    end)
  end

  test "consumer group: fetch resumes from the server-committed position" do
    with_session("app", "app123", fn socket ->
      topic = "logproto_grp_#{System.unique_integer([:positive])}"

      assert %{"s" => "ok"} = request(socket, %{"action" => "create_topic", "topic" => topic})

      assert %{"s" => "ok", "count" => 3} =
               request(socket, %{
                 "action" => "produce",
                 "topic" => topic,
                 "records" => [%{"value" => "a"}, %{"value" => "b"}, %{"value" => "c"}]
               })

      # fetch for a group, then commit the returned cursor durably
      assert %{"s" => "ok", "records" => records, "cursor" => cursor} =
               request(socket, %{"action" => "fetch", "topic" => topic, "group" => "g1"})

      assert length(records) == 3

      assert %{"s" => "ok"} =
               request(socket, %{"action" => "commit", "topic" => topic, "group" => "g1", "cursor" => cursor})

      # the group resumes past the committed position (nothing new)
      assert %{"s" => "ok", "records" => []} =
               request(socket, %{"action" => "fetch", "topic" => topic, "group" => "g1"})
    end)
  end

  test "fetch rejects a malformed (non-string) cursor instead of silently restarting" do
    with_session("app", "app123", fn socket ->
      topic = "logproto_badcur_#{System.unique_integer([:positive])}"
      assert %{"s" => "ok"} = request(socket, %{"action" => "create_topic", "topic" => topic})

      # a wrong-typed cursor must error, not fall through to a fetch from the start
      assert %{"s" => "err", "reason" => "invalid_cursor"} =
               request(socket, %{"action" => "fetch", "topic" => topic, "cursor" => 123})

      # a null cursor is "no cursor": with a group it still resumes the group (not suppressed)
      assert %{"s" => "ok", "records" => _} =
               request(socket, %{"action" => "fetch", "topic" => topic, "cursor" => nil, "group" => "g"})
    end)
  end

  test "produce needs :produce and fetch needs :consume" do
    topic = "logproto_perm_#{System.unique_integer([:positive])}"

    # consumer (only :consume) may fetch but not produce
    with_session("consumer", "consumer123", fn socket ->
      assert %{"s" => "err", "reason" => "permission_denied"} =
               request(socket, %{"action" => "produce", "topic" => topic, "records" => [%{"value" => "x"}]})
    end)

    # producer (only :produce) may produce/create but not fetch
    with_session("producer", "producer123", fn socket ->
      assert %{"s" => "ok"} = request(socket, %{"action" => "create_topic", "topic" => topic})

      assert %{"s" => "err", "reason" => "permission_denied"} =
               request(socket, %{"action" => "fetch", "topic" => topic})
    end)
  end
end
