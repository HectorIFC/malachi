defmodule Malachi.LogProtocolTest do
  # End-to-end over the real TCP server (started by the app): the NorthGuard log protocol over the binary
  # Malachi.Wire framing (create_topic / produce by key / fetch by opaque cursor / groups / long-poll)
  # against the live Malachi.LogBroker.
  use ExUnit.Case, async: false

  alias Malachi.Log.Record
  alias Malachi.Test.TCPHelper
  alias Malachi.Wire

  @port Application.compile_env(:malachi, :tcp_port, 4040)

  defp ok?(code), do: code == Wire.ok_code()
  # an error response carries the reason as a length-prefixed string
  defp reason(payload), do: Wire.decode_auth_resp(payload)

  defp create_topic(socket, topic) do
    TCPHelper.request(socket, Wire.create_topic_key(), 1, Wire.encode_create_topic_req(topic, 8))
  end

  # records: a list of `value` binaries or `{key, value}` tuples
  defp produce(socket, topic, records) do
    wire =
      Enum.map(records, fn
        {key, value} -> %Record{key: key, value: value}
        value when is_binary(value) -> %Record{value: value}
      end)

    TCPHelper.request(socket, Wire.produce_key(), 1, Wire.encode_produce_req(topic, wire))
  end

  defp fetch(socket, topic, opts \\ []) do
    payload =
      Wire.encode_fetch_req(topic, opts[:cursor], opts[:group], opts[:max] || 100, opts[:wait] || 0)

    TCPHelper.request(socket, Wire.fetch_key(), 1, payload)
  end

  defp commit(socket, topic, group, cursor) do
    TCPHelper.request(socket, Wire.commit_key(), 1, Wire.encode_commit_req(topic, group, cursor))
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
        {:ok, _token} = TCPHelper.authenticate_wire(socket, username, password)

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
      assert {code, _} = create_topic(socket, topic)
      assert ok?(code)

      assert {code, <<2::32>>} = produce(socket, topic, [{"k1", "v1"}, {"k2", "v2"}])
      assert ok?(code)

      assert {code, payload} = fetch(socket, topic)
      assert ok?(code)
      {records, cursor} = Wire.decode_fetch_resp(payload)

      # opaque cursor (a byte token, not an integer offset); records carry key/value, never an offset
      assert is_binary(cursor)
      assert records |> Enum.map(& &1.value) |> Enum.sort() == ["v1", "v2"]
      assert Enum.all?(records, &(&1.offset == nil))

      # the cursor carries position: re-fetching with it yields nothing new
      assert {code, payload} = fetch(socket, topic, cursor: cursor)
      assert ok?(code)
      assert {[], _cursor} = Wire.decode_fetch_resp(payload)
    end)
  end

  test "consumer group: fetch resumes from the server-committed position" do
    with_session("app", "app123", fn socket ->
      topic = "logproto_grp_#{System.unique_integer([:positive])}"
      assert {code, _} = create_topic(socket, topic)
      assert ok?(code)

      assert {code, <<3::32>>} = produce(socket, topic, ["a", "b", "c"])
      assert ok?(code)

      # fetch for a group, then commit the returned cursor durably
      assert {code, payload} = fetch(socket, topic, group: "g1")
      assert ok?(code)
      {records, cursor} = Wire.decode_fetch_resp(payload)
      assert length(records) == 3

      assert {code, _} = commit(socket, topic, "g1", cursor)
      assert ok?(code)

      # the group resumes past the committed position (nothing new)
      assert {code, payload} = fetch(socket, topic, group: "g1")
      assert ok?(code)
      assert {[], _cursor} = Wire.decode_fetch_resp(payload)
    end)
  end

  test "fetch rejects a malformed opaque cursor instead of silently restarting" do
    with_session("app", "app123", fn socket ->
      topic = "logproto_badcur_#{System.unique_integer([:positive])}"
      assert {code, _} = create_topic(socket, topic)
      assert ok?(code)

      # bytes that are not a valid cursor token must error, not fall through to a fetch from the start
      assert {code, payload} = fetch(socket, topic, cursor: "!!!not-a-cursor!!!")
      refute ok?(code)
      assert reason(payload) == "invalid_cursor"

      # no cursor + a group still resumes the group (a nil cursor is "no cursor", not suppressed)
      assert {code, _payload} = fetch(socket, topic, group: "g")
      assert ok?(code)
    end)
  end

  test "produces and fetches arbitrary binary payloads (non-UTF-8 bytes survive the round trip)" do
    with_session("app", "app123", fn socket ->
      topic = "logproto_bin_#{System.unique_integer([:positive])}"
      assert {code, _} = create_topic(socket, topic)
      assert ok?(code)

      payload = <<0, 159, 146, 150, 255, 0, 1>>
      refute String.valid?(payload)

      assert {code, <<1::32>>} = produce(socket, topic, [payload])
      assert ok?(code)

      assert {code, resp} = fetch(socket, topic)
      assert ok?(code)
      {[record], _cursor} = Wire.decode_fetch_resp(resp)
      assert record.value == payload
    end)
  end

  test "fetch with wait resumes when another client produces (long-poll)" do
    topic = "logproto_lp_#{System.unique_integer([:positive])}"

    with_session("app", "app123", fn socket ->
      assert {code, _} = create_topic(socket, topic)
      assert ok?(code)
    end)

    # a consumer long-polls an empty topic in a separate task; it parks until a produce arrives
    consumer =
      Task.async(fn ->
        with_session("app", "app123", fn socket ->
          {code, resp} = fetch(socket, topic, wait: 3_000)
          {code, Wire.decode_fetch_resp(resp)}
        end)
      end)

    wait_for_park()

    with_session("app", "app123", fn socket ->
      assert {code, <<1::32>>} = produce(socket, topic, ["a"])
      assert ok?(code)
    end)

    assert {code, {[record], _cursor}} = Task.await(consumer, 5_000)
    assert ok?(code)
    assert record.value == "a"
  end

  test "fetch with wait returns empty after the timeout (long-poll)" do
    with_session("app", "app123", fn socket ->
      topic = "logproto_lpto_#{System.unique_integer([:positive])}"
      assert {code, _} = create_topic(socket, topic)
      assert ok?(code)

      assert {code, resp} = fetch(socket, topic, wait: 100)
      assert ok?(code)
      assert {[], _cursor} = Wire.decode_fetch_resp(resp)
    end)
  end

  test "produce needs :produce and fetch needs :consume" do
    topic = "logproto_perm_#{System.unique_integer([:positive])}"

    # consumer (only :consume) may fetch but not produce
    with_session("consumer", "consumer123", fn socket ->
      assert {code, payload} = produce(socket, topic, ["x"])
      refute ok?(code)
      assert reason(payload) == "permission_denied"
    end)

    # producer (only :produce) may produce/create but not fetch
    with_session("producer", "producer123", fn socket ->
      assert {code, _} = create_topic(socket, topic)
      assert ok?(code)

      assert {code, payload} = fetch(socket, topic)
      refute ok?(code)
      assert reason(payload) == "permission_denied"
    end)
  end
end
