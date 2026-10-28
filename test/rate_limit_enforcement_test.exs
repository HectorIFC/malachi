defmodule Malachi.RateLimitEnforcementTest do
  # async: false. Toggles the global publish/subscribe limits and shares the running acceptor.
  #
  # End-to-end over the real TCP server: the configured publish/subscribe quotas are applied on the
  # stream keys, an append on a producer stream (publish) and an open_consume (subscribe), keyed by the
  # authenticated username and enforced per node. What is
  # pinned here is the operator-visible contract: the wire reason, the counter that feeds
  # `rate_limit_blocked{action=...}`, the key the quota is counted by, and that an unconfigured limit
  # changes nothing.
  use ExUnit.Case, async: false

  import Malachi.Test.QuotaForensics, only: [within_one_window: 2, snapshot: 2, assert_refused: 4]

  alias Malachi.Auth
  alias Malachi.Metrics
  alias Malachi.Test.StreamsOn
  alias Malachi.Test.StreamTCP
  alias Malachi.Test.TCPHelper
  alias Malachi.Wire

  @moduletag :security

  @limit_keys [
    :publish_rate_limit,
    :publish_rate_window_ms,
    :publish_bytes_rate_limit,
    :publish_bytes_rate_window_ms,
    :subscribe_rate_limit,
    :subscribe_rate_window_ms
  ]

  setup do
    :ok = StreamsOn.setup(%{})
    prior = for key <- @limit_keys, into: %{}, do: {key, Application.get_env(:malachi, key)}
    on_exit(fn -> for {key, value} <- prior, do: restore(key, value) end)
    :ok
  end

  defp restore(key, nil), do: Application.delete_env(:malachi, key)
  defp restore(key, value), do: Application.put_env(:malachi, key, value)

  # A window long enough that no token is refilled mid-test, so a blocked request stays blocked. It is still
  # a fixed window aligned to the limiter's clock, so every test that expects a refusal runs its body
  # inside `within_one_window/2` and asserts the refusal with `assert_refused/4`: a boundary crossed
  # mid-test is rerun, and any other over-limit admission fails with a report of why (issue #151).
  @window_ms 60_000

  defp limit_publish(limit), do: put_limit(:publish_rate_limit, :publish_rate_window_ms, limit)
  defp limit_publish_bytes(limit), do: put_limit(:publish_bytes_rate_limit, :publish_bytes_rate_window_ms, limit)
  defp limit_subscribe(limit), do: put_limit(:subscribe_rate_limit, :subscribe_rate_window_ms, limit)

  defp put_limit(limit_key, window_key, limit) do
    Application.put_env(:malachi, limit_key, limit)
    Application.put_env(:malachi, window_key, @window_ms)
  end

  @password "Rate-Pass-1!"

  # A fresh user, so its quota starts unspent.
  defp new_user(prefix \\ "rl") do
    username = "#{prefix}_#{System.unique_integer([:positive])}"
    Auth.add_user(username, @password, [:produce, :consume])
    on_exit(fn -> Auth.remove_user(username) end)
    username
  end

  # A fresh user with an authenticated connection.
  defp connect_as_new_user do
    username = new_user()
    {username, connect_as(username, @password)}
  end

  defp connect_as(username, password) do
    {:ok, socket} = TCPHelper.connect()
    {:ok, _token} = TCPHelper.authenticate_wire(socket, username, password)
    on_exit(fn -> :gen_tcp.close(socket) end)
    socket
  end

  defp create_topic(socket, topic) do
    reply(TCPHelper.request(socket, Wire.create_topic_key(), 1, Wire.encode_create_topic_req(topic, 8)))
  end

  # Appends `values` on this connection's producer stream on `topic`, opened on its first append, at the
  # stream's next sequence: `:ok` when acknowledged, else the refusal, of the open or of the append at its
  # sequence (the stream goes on after a refused append).
  defp produce(socket, topic, values \\ ["v"]) do
    with {:ok, id, sequence} <- stream_for(socket, topic) do
      StreamTCP.send_append(socket, id, sequence, StreamTCP.batch(values))
      {_ack, errors} = StreamTCP.acked_through(socket, sequence)
      Process.put({:stream, socket, topic}, {id, sequence + 1})

      case Enum.find(errors, &(&1.sequence == sequence)) do
        nil -> :ok
        %{reason: reason} -> {:error, reason}
      end
    end
  end

  defp stream_for(socket, topic) do
    case Process.get({:stream, socket, topic}) do
      {id, sequence} ->
        {:ok, id, sequence}

      nil ->
        with {:ok, %{stream_id: id}} <- StreamTCP.open(socket, topic) do
          Process.put({:stream, socket, topic}, {id, 0})
          {:ok, id, 0}
        end
    end
  end

  # An open_consume on `topic`: `:ok` when the stream opened, else the refusal.
  defp subscribe(socket, topic) do
    case StreamTCP.open_consume(socket, topic) do
      {:ok, _stream} -> :ok
      {:error, _reason} = refused -> refused
    end
  end

  defp reply({code, payload}) do
    if code == Wire.ok_code(), do: :ok, else: {:error, Wire.decode_error_reason(payload)}
  end

  defp blocked_count(action) do
    Metrics.get_system_metrics().rate_limiting[action]
  end

  defp new_topic, do: "rl_topic_#{System.unique_integer([:positive])}"

  describe "unconfigured (the default): no behaviour change" do
    test "a limit of zero does not apply, however many produces are sent" do
      limit_publish(0)
      {_user, socket} = connect_as_new_user()
      topic = new_topic()
      assert :ok = create_topic(socket, topic)

      before = blocked_count(:publish_blocked)

      for _ <- 1..25, do: assert(:ok = produce(socket, topic))

      # not merely "allowed": the limiter was never consulted, so the counter cannot have moved
      assert blocked_count(:publish_blocked) == before
    end

    test "an unset limit does not apply" do
      Application.delete_env(:malachi, :publish_rate_limit)
      Application.delete_env(:malachi, :subscribe_rate_limit)

      {_user, socket} = connect_as_new_user()
      topic = new_topic()
      assert :ok = create_topic(socket, topic)

      for _ <- 1..10, do: assert(:ok = produce(socket, topic))
    end
  end

  describe "publish quota" do
    test "produce over the limit is refused as rate_limited and the counter advances" do
      limit_publish(3)

      within_one_window(@window_ms, fn ->
        {user, socket} = connect_as_new_user()
        topic = new_topic()
        assert :ok = create_topic(socket, topic)

        before = blocked_count(:publish_blocked)
        spent = snapshot(user, :publish)

        # create_topic is gated by :produce but is not a publish, and neither is opening the stream, so
        # neither spends a token
        for _ <- 1..3, do: assert(:ok = produce(socket, topic))

        # `overloaded` is the group-commit valve (see test/malachi/group_commit_test.exs); a client must be
        # able to tell "you are over your quota" from "the broker is saturated", so the two never collide,
        # which the exact match on `rate_limited` pins
        assert_refused(produce(socket, topic), spent, user, :publish)

        assert blocked_count(:publish_blocked) == before + 1
      end)
    end

    test "the quota is per user, not per connection: a second connection shares the bucket" do
      limit_publish(2)

      within_one_window(@window_ms, fn ->
        username = new_user("rl_fanout")
        first = connect_as(username, @password)
        second = connect_as(username, @password)
        topic = new_topic()
        assert :ok = create_topic(first, topic)

        spent = snapshot(username, :publish)
        assert :ok = produce(first, topic)
        assert :ok = produce(second, topic)

        # the two tokens are gone whichever connection spent them: a fan-out client cannot buy more quota
        # by opening more connections
        assert_refused(produce(first, topic), spent, username, :publish)
        assert_refused(produce(second, topic), spent, username, :publish)
      end)
    end

    test "one user exhausting its quota does not affect another user" do
      limit_publish(1)

      within_one_window(@window_ms, fn ->
        {starved, starved_socket} = connect_as_new_user()
        {_other, other_socket} = connect_as_new_user()
        topic = new_topic()
        assert :ok = create_topic(starved_socket, topic)

        spent = snapshot(starved, :publish)
        assert :ok = produce(starved_socket, topic)
        assert_refused(produce(starved_socket, topic), spent, starved, :publish)

        assert :ok = produce(other_socket, topic)
      end)
    end

    test "a request the user was never allowed to make spends no token" do
      limit_publish(1)
      topic = new_topic()

      {_owner, owner_socket} = connect_as_new_user()
      assert :ok = create_topic(owner_socket, topic)

      # consumer holds :consume only, so its stream is refused before the quota is consulted
      consumer = connect_as("consumer", "consumer123")
      before = blocked_count(:publish_blocked)

      for _ <- 1..5, do: assert({:error, "permission_denied"} = produce(consumer, topic))

      assert blocked_count(:publish_blocked) == before
    end

    test "the limit is not applied when rate limiting is switched off entirely" do
      limit_publish(1)
      prior = Application.get_env(:malachi, :rate_limit_enabled)
      Application.put_env(:malachi, :rate_limit_enabled, false)
      on_exit(fn -> restore(:rate_limit_enabled, prior) end)

      {_user, socket} = connect_as_new_user()
      topic = new_topic()
      assert :ok = create_topic(socket, topic)

      for _ <- 1..5, do: assert(:ok = produce(socket, topic))
    end
  end

  describe "publish quotas count records and bytes" do
    test "a produce spends one unit per record" do
      limit_publish(5)

      within_one_window(@window_ms, fn ->
        {user, socket} = connect_as_new_user()
        topic = new_topic()
        assert :ok = create_topic(socket, topic)

        spent = snapshot(user, :publish)
        assert :ok = produce(socket, topic, ["a", "b", "c"])
        # 3 spent, 3 more would make 6
        assert_refused(produce(socket, topic, ["d", "e", "f"]), spent, user, :publish)
        assert :ok = produce(socket, topic, ["d", "e"])
      end)
    end

    test "a produce spends its bytes under the bytes quota, which refuses on its own" do
      limit_publish_bytes(1_000)

      # the counter is read inside each attempt, so an attempt a window boundary reruns is not counted twice
      within_one_window(@window_ms, fn ->
        {user, socket} = connect_as_new_user()
        topic = new_topic()
        assert :ok = create_topic(socket, topic)

        before = blocked_count(:publish_bytes_blocked)
        spent = snapshot(user, :publish_bytes)
        assert :ok = produce(socket, topic, [String.duplicate("x", 600)])
        assert_refused(produce(socket, topic, [String.duplicate("y", 600)]), spent, user, :publish_bytes)
        assert blocked_count(:publish_bytes_blocked) == before + 1
      end)
    end

    test "a produce bigger than a whole window is quota_too_small, counted as blocked, and spends nothing" do
      limit_publish(2)

      within_one_window(@window_ms, fn ->
        {_user, socket} = connect_as_new_user()
        topic = new_topic()
        assert :ok = create_topic(socket, topic)

        before = blocked_count(:publish_blocked)
        assert {:error, "quota_too_small"} = produce(socket, topic, ["a", "b", "c"])
        assert blocked_count(:publish_blocked) == before + 1
        assert :ok = produce(socket, topic, ["a", "b"])
      end)
    end
  end

  describe "subscribe quota" do
    test "subscribe over the limit is refused as rate_limited and the counter advances" do
      limit_subscribe(1)

      within_one_window(@window_ms, fn ->
        username = new_user("rl_sub")
        setup_socket = connect_as(username, @password)
        topic = new_topic()
        assert :ok = create_topic(setup_socket, topic)

        before = blocked_count(:subscribe_blocked)
        spent = snapshot(username, :subscribe)

        # a second connection of the same user: the same bucket
        streaming = connect_as(username, @password)
        :ok = subscribe(streaming, topic)

        refused = connect_as(username, @password)
        assert_refused(subscribe(refused, topic), spent, username, :subscribe)

        assert blocked_count(:subscribe_blocked) == before + 1
      end)
    end

    test "an unconfigured subscribe limit does not apply" do
      limit_subscribe(0)
      {_user, socket} = connect_as_new_user()
      topic = new_topic()
      assert :ok = create_topic(socket, topic)

      before = blocked_count(:subscribe_blocked)
      for _ <- 1..5, do: assert(:ok = subscribe(socket, topic))

      # every stream opened, so nothing was blocked
      assert blocked_count(:subscribe_blocked) == before
    end

    test "the publish and subscribe quotas are independent" do
      # Both limits are POSITIVE and the subscribe runs as the SAME user whose publish quota is spent.
      # An earlier version of this test set the subscribe limit to 0 and subscribed as a new user, so it
      # passed for two reasons that had nothing to do with independence: the subscribe quota was never
      # consulted, and the bucket it would have used belonged to somebody else. Verified against a mutant
      # that counts both actions on one bucket, which this version fails and that one did not.
      limit_publish(1)
      limit_subscribe(1)

      within_one_window(@window_ms, fn ->
        username = new_user("rl_indep")
        socket = connect_as(username, @password)
        topic = new_topic()
        assert :ok = create_topic(socket, topic)

        spent = snapshot(username, :publish)
        assert :ok = produce(socket, topic)
        assert_refused(produce(socket, topic), spent, username, :publish)

        # Same user, publish quota gone: the subscribe must still be admitted on its own bucket, and the
        # record produced above, pushed on the stream it opened, is positive proof it was admitted, not
        # merely an absence of refusal.
        streaming = connect_as(username, @password)

        case subscribe(streaming, topic) do
          :ok -> :ok
          {:error, reason} -> flunk("the subscribe was refused (#{reason}), so the quotas share a bucket")
        end

        assert {:records, page} = StreamTCP.push(streaming, StreamTCP.consumer_corr())
        assert [{_position, "v"}] = StreamTCP.page_records(page)
      end)
    end
  end
end
