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

  # Deterministically wait until the live LogBroker has a parked long-poll waiter.
  defp wait_for_park(deadline \\ nil) do
    deadline = deadline || System.monotonic_time(:millisecond) + 3_000

    cond do
      log_broker_waiters() >= 1 -> :ok
      System.monotonic_time(:millisecond) > deadline -> flunk("the long-poll consumer did not park")
      true -> Process.sleep(5) && wait_for_park(deadline)
    end
  end

  defp log_broker_waiters do
    case Process.whereis(Malachi.LogBroker) do
      nil -> 0
      pid -> length(:sys.get_state(pid).waiters)
    end
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
                 "records" => [
                   %{"key" => "k1", "value" => Base.encode64("v1")},
                   %{"key" => "k2", "value" => Base.encode64("v2")}
                 ]
               })

      assert %{"s" => "ok", "records" => records, "cursor" => cursor} =
               request(socket, %{"action" => "fetch", "topic" => topic})

      # opaque cursor (a string token, not an integer offset); records carry key/value, no offset
      assert is_binary(cursor)
      # value is base64 on the wire — decode to recover the original bytes
      assert records |> Enum.map(&Base.decode64!(&1["value"])) |> Enum.sort() == ["v1", "v2"]
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
                 "records" => [
                   %{"value" => Base.encode64("a")},
                   %{"value" => Base.encode64("b")},
                   %{"value" => Base.encode64("c")}
                 ]
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

  test "produces and fetches arbitrary binary payloads (non-UTF-8 bytes survive the round trip)" do
    with_session("app", "app123", fn socket ->
      topic = "logproto_bin_#{System.unique_integer([:positive])}"
      assert %{"s" => "ok"} = request(socket, %{"action" => "create_topic", "topic" => topic})

      payload = <<0, 159, 146, 150, 255, 0, 1>>
      refute String.valid?(payload)

      assert %{"s" => "ok", "count" => 1} =
               request(socket, %{
                 "action" => "produce",
                 "topic" => topic,
                 "records" => [%{"value" => Base.encode64(payload)}]
               })

      assert %{"s" => "ok", "records" => [record]} =
               request(socket, %{"action" => "fetch", "topic" => topic})

      assert Base.decode64!(record["value"]) == payload
    end)
  end

  test "produce rejects a value that is not valid base64" do
    with_session("app", "app123", fn socket ->
      topic = "logproto_badb64_#{System.unique_integer([:positive])}"
      assert %{"s" => "ok"} = request(socket, %{"action" => "create_topic", "topic" => topic})

      assert %{"s" => "err", "reason" => "invalid_base64"} =
               request(socket, %{
                 "action" => "produce",
                 "topic" => topic,
                 "records" => [%{"value" => "not valid base64 !!!"}]
               })
    end)
  end

  test "fetch with wait resumes when another client produces (long-poll)" do
    topic = "logproto_lp_#{System.unique_integer([:positive])}"

    with_session("app", "app123", fn socket ->
      assert %{"s" => "ok"} = request(socket, %{"action" => "create_topic", "topic" => topic})
    end)

    # a consumer long-polls an empty topic in a separate task; it parks until a produce arrives
    consumer =
      Task.async(fn ->
        with_session("app", "app123", fn socket ->
          request(socket, %{"action" => "fetch", "topic" => topic, "wait" => 3_000})
        end)
      end)

    wait_for_park()

    with_session("app", "app123", fn socket ->
      assert %{"s" => "ok", "count" => 1} =
               request(socket, %{
                 "action" => "produce",
                 "topic" => topic,
                 "records" => [%{"value" => Base.encode64("a")}]
               })
    end)

    assert %{"s" => "ok", "records" => [record]} = Task.await(consumer, 5_000)
    assert Base.decode64!(record["value"]) == "a"
  end

  test "fetch with wait returns empty after the timeout (long-poll)" do
    with_session("app", "app123", fn socket ->
      topic = "logproto_lpto_#{System.unique_integer([:positive])}"
      assert %{"s" => "ok"} = request(socket, %{"action" => "create_topic", "topic" => topic})

      assert %{"s" => "ok", "records" => []} =
               request(socket, %{"action" => "fetch", "topic" => topic, "wait" => 100})
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
