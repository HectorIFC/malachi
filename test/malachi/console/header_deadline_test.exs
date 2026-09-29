defmodule Malachi.Console.HeaderDeadlineTest do
  use ExUnit.Case, async: true

  alias Malachi.Console.HeaderDeadline

  # Stands in for the protocol handler Bandit.DelegatingHandler routes to: it answers with the
  # callback it received, so each test can see that HeaderDeadline passed the call through untouched.
  defmodule Recorder do
    def handle_error(error, socket, state), do: {:handle_error, error, socket, state}
    def handle_shutdown(socket, state), do: {:handle_shutdown, socket, state}
    def handle_timeout(socket, state), do: {:handle_timeout, socket, state}
    def handle_call(msg, from, state), do: {:handle_call, msg, from, state}
    def handle_cast(msg, state), do: {:handle_cast, msg, state}
  end

  @state %{handler_module: Recorder}

  test "Thousand Island callbacks reach Bandit's handler unchanged" do
    assert HeaderDeadline.handle_error(:boom, :socket, @state) == {:handle_error, :boom, :socket, @state}
    assert HeaderDeadline.handle_shutdown(:socket, @state) == {:handle_shutdown, :socket, @state}
    assert HeaderDeadline.handle_timeout(:socket, @state) == {:handle_timeout, :socket, @state}
  end

  test "GenServer calls and casts reach Bandit's handler unchanged" do
    state = {:socket, @state}
    assert HeaderDeadline.handle_call(:ping, :from, state) == {:handle_call, :ping, :from, state}
    assert HeaderDeadline.handle_cast(:ping, state) == {:handle_cast, :ping, state}
  end
end
