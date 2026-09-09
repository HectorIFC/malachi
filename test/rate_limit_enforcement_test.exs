defmodule Malachi.RateLimitEnforcementTest do
  # async: false. Toggles the global publish/subscribe limits and shares the running acceptor.
  #
  # End-to-end over the real TCP server: the configured publish/subscribe quotas are applied on the
  # produce and subscribe paths, keyed by the authenticated username and enforced per node. What is
  # pinned here is the operator-visible contract: the wire reason, the counter that feeds
  # `rate_limit_blocked{action=...}`, the key the quota is counted by, and that an unconfigured limit
  # changes nothing.
  use ExUnit.Case, async: false

  alias Malachi.Auth
  alias Malachi.Metrics
  alias Malachi.Test.TCPHelper
  alias Malachi.Wire

  @moduletag :security

  @limit_keys [:publish_rate_limit, :publish_rate_window_ms, :subscribe_rate_limit, :subscribe_rate_window_ms]

  setup do
    prior = for key <- @limit_keys, into: %{}, do: {key, Application.get_env(:malachi, key)}
    on_exit(fn -> for {key, value} <- prior, do: restore(key, value) end)
    :ok
  end

  defp restore(key, nil), do: Application.delete_env(:malachi, key)
  defp restore(key, value), do: Application.put_env(:malachi, key, value)

  # A window long enough that no token is refilled mid-test, so a blocked request stays blocked.
  defp limit_publish(limit), do: put_limit(:publish_rate_limit, :publish_rate_window_ms, limit)
  defp limit_subscribe(limit), do: put_limit(:subscribe_rate_limit, :subscribe_rate_window_ms, limit)

  defp put_limit(limit_key, window_key, limit) do
    Application.put_env(:malachi, limit_key, limit)
    Application.put_env(:malachi, window_key, 60_000)
  end

  # A fresh user (so its bucket starts empty) with an authenticated connection.
  defp connect_as_new_user do
    username = "rl_#{System.unique_integer([:positive])}"
    password = "Rate-Pass-1!"
    Auth.add_user(username, password, [:produce, :consume])
    on_exit(fn -> Auth.remove_user(username) end)

    {username, connect_as(username, password)}
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

  defp produce(socket, topic) do
    payload = Wire.encode_produce_req(topic, [%Malachi.Log.Record{value: "v"}])
    reply(TCPHelper.request(socket, Wire.produce_key(), 1, payload))
  end

  # A blocked subscribe answers an error frame instead of entering stream mode, so unlike an accepted
  # one it has a response to read.
  defp subscribe(socket, topic) do
    :ok = TCPHelper.subscribe(socket, topic, nil, 100, 100, 7)
    {:ok, frame_body} = TCPHelper.recv_frame(socket)
    {7, code, payload} = Wire.decode_response(frame_body)
    reply({code, payload})
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
      {_user, socket} = connect_as_new_user()
      topic = new_topic()
      assert :ok = create_topic(socket, topic)

      before = blocked_count(:publish_blocked)

      # create_topic is gated by :produce but is not a publish, so it spends no token
      for _ <- 1..3, do: assert(:ok = produce(socket, topic))

      assert {:error, reason} = produce(socket, topic)
      assert reason == "rate_limited"

      # `overloaded` is the group-commit valve (see test/malachi/group_commit_test.exs); a client must be
      # able to tell "you are over your quota" from "the broker is saturated", so the two never collide
      refute reason == "overloaded"

      assert blocked_count(:publish_blocked) == before + 1
    end

    test "the quota is per user, not per connection: a second connection shares the bucket" do
      limit_publish(2)
      username = "rl_fanout_#{System.unique_integer([:positive])}"
      password = "Rate-Pass-1!"
      Auth.add_user(username, password, [:produce, :consume])
      on_exit(fn -> Auth.remove_user(username) end)

      first = connect_as(username, password)
      second = connect_as(username, password)
      topic = new_topic()
      assert :ok = create_topic(first, topic)

      assert :ok = produce(first, topic)
      assert :ok = produce(second, topic)

      # the two tokens are gone whichever connection spent them: a fan-out client cannot buy more quota
      # by opening more connections
      assert {:error, "rate_limited"} = produce(first, topic)
      assert {:error, "rate_limited"} = produce(second, topic)
    end

    test "one user exhausting its quota does not affect another user" do
      limit_publish(1)
      {_starved, starved_socket} = connect_as_new_user()
      {_other, other_socket} = connect_as_new_user()
      topic = new_topic()
      assert :ok = create_topic(starved_socket, topic)

      assert :ok = produce(starved_socket, topic)
      assert {:error, "rate_limited"} = produce(starved_socket, topic)

      assert :ok = produce(other_socket, topic)
    end

    test "a request the user was never allowed to make spends no token" do
      limit_publish(1)
      topic = new_topic()

      {_owner, owner_socket} = connect_as_new_user()
      assert :ok = create_topic(owner_socket, topic)

      # consumer holds :consume only, so its produce is denied before the quota is consulted
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

  describe "subscribe quota" do
    test "subscribe over the limit is refused as rate_limited and the counter advances" do
      limit_subscribe(1)
      username = "rl_sub_#{System.unique_integer([:positive])}"
      password = "Rate-Pass-1!"
      Auth.add_user(username, password, [:produce, :consume])
      on_exit(fn -> Auth.remove_user(username) end)

      setup_socket = connect_as(username, password)
      topic = new_topic()
      assert :ok = create_topic(setup_socket, topic)

      before = blocked_count(:subscribe_blocked)

      # an accepted subscribe turns its connection into a push stream, so the second one needs its own
      # connection: same user, so the same bucket
      streaming = connect_as(username, password)
      :ok = TCPHelper.subscribe(streaming, topic, nil, 100, 100, 7)

      refused = connect_as(username, password)
      assert {:error, "rate_limited"} = subscribe(refused, topic)

      assert blocked_count(:subscribe_blocked) == before + 1
    end

    test "an unconfigured subscribe limit does not apply" do
      limit_subscribe(0)
      {_user, socket} = connect_as_new_user()
      topic = new_topic()
      assert :ok = create_topic(socket, topic)

      before = blocked_count(:subscribe_blocked)
      :ok = TCPHelper.subscribe(socket, topic, nil, 100, 100, 7)

      # the subscribe was accepted (it entered stream mode), so nothing was blocked
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

      username = "rl_indep_#{System.unique_integer([:positive])}"
      password = "Rate-Pass-1!"
      Auth.add_user(username, password, [:produce, :consume])
      on_exit(fn -> Auth.remove_user(username) end)

      socket = connect_as(username, password)
      topic = new_topic()
      assert :ok = create_topic(socket, topic)

      assert :ok = produce(socket, topic)
      assert {:error, "rate_limited"} = produce(socket, topic)

      # Same user, publish quota gone: the subscribe must still be admitted on its own bucket. An accepted
      # subscribe switches the connection to stream mode and immediately pushes the backlog, so the record
      # produced above coming back is positive proof it was admitted, not merely an absence of refusal. A
      # refusal would answer an error frame carrying `rate_limited` on the same correlation id.
      streaming = connect_as(username, password)
      :ok = TCPHelper.subscribe(streaming, topic, nil, 100, 100, 7)

      assert {:ok, frame_body} = TCPHelper.recv_frame(streaming, timeout: 2_000)
      {7, code, payload} = Wire.decode_response(frame_body)

      # Named rather than asserted bare, because the failure that matters here is a refusal, and the
      # reason is only decodable once we know this IS an error frame.
      if code != Wire.ok_code() do
        flunk("the subscribe was refused (#{Wire.decode_error_reason(payload)}), so the quotas share a bucket")
      end

      assert {[%{value: "v"}], _cursor} = Wire.decode_fetch_resp(payload)
    end
  end
end
