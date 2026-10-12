defmodule Malachi.Test.StreamStub do
  @moduledoc """
  A wire server that speaks the stream keys in their simplest form, for the client tests that need a
  broker they can steer: the load generator's setup refusals and the routing producer's moves.

  Authentication and topic creation succeed. `cluster_state` says the stream keys are served by the stub
  itself, `topic_routes` gives the routes set with `set_routes/3` (one range, 0, until then), an
  `open_stream` opens the next stream id, and every `append` is acknowledged unless `set_appends/2`
  says otherwise. `overrides`, given at start and changed with `set_override/3`, replace the answer to a
  key: `:close` (the connection), `:silent` (no answer), `{:error, reason}`, or for an append
  `{:refuse_append, reason}` (an acknowledgement naming it as failed, counted by `refused/1`). Routes can
  also change from one read to the next (`set_routes_sequence/2`); an `open_stream` answers with the
  version last read, by any connection (the stub keeps one for all), and `opened/1` counts the opens.

  The appends the stub took are kept, in arrival order, with their values (`appends/1`), and a test can
  write any frame to the connections that opened streams (`push/2`): a `moved` push, or the error frames
  of appends the broker did not take.
  """

  alias Malachi.Wire
  alias Malachi.Wire.Batch

  @enforce_keys [:port, :state, :listen]
  defstruct [:port, :state, :listen]

  @type t :: %__MODULE__{}

  @doc "Starts a stub on an ephemeral port. Stop it with `stop/1`."
  @spec start(map()) :: t()
  def start(overrides \\ %{}) do
    {:ok, listen} = :gen_tcp.listen(0, [:binary, packet: 4, active: false, reuseaddr: true])
    {:ok, port} = :inet.port(listen)

    {:ok, state} =
      Agent.start(fn ->
        %{
          overrides: overrides,
          routes: [%{version: 1, ranges: [{0, 0, 256}]}],
          served: 1,
          refused: 0,
          appends: :ack,
          next_stream: 1,
          taken: [],
          streamers: []
        }
      end)

    stub = %__MODULE__{port: port, state: state, listen: listen}
    spawn(fn -> accept(stub) end)
    stub
  end

  @doc "Stops the stub: its listener, its connections' state and its record of appends."
  @spec stop(t()) :: :ok
  def stop(%__MODULE__{} = stub) do
    :gen_tcp.close(stub.listen)
    Agent.stop(stub.state)
  end

  @doc "The routes `topic_routes` answers from now on: `ranges` as `{range, key_start, key_end}`, all led by the stub."
  @spec set_routes(t(), pos_integer(), [{non_neg_integer(), non_neg_integer(), pos_integer()}]) :: :ok
  def set_routes(%__MODULE__{} = stub, version, ranges), do: set_routes_sequence(stub, [{version, ranges}])

  @doc """
  Routes `topic_routes` answers in turn: the first to the next read, the second to the one after, and the
  last to every read from then on (routes read mid-split, say, then whole).
  """
  @spec set_routes_sequence(t(), [{pos_integer(), [{non_neg_integer(), non_neg_integer(), pos_integer()}]}]) :: :ok
  def set_routes_sequence(%__MODULE__{state: state}, [_ | _] = sequence) do
    routes = Enum.map(sequence, fn {version, ranges} -> %{version: version, ranges: ranges} end)
    Agent.update(state, &%{&1 | routes: routes})
  end

  @doc "Replaces the answer to `api_key` from now on, as `overrides` does at start (`:default` puts it back)."
  @spec set_override(t(), non_neg_integer(), term()) :: :ok
  def set_override(%__MODULE__{state: state}, api_key, reply),
    do: Agent.update(state, &%{&1 | overrides: Map.put(&1.overrides, api_key, reply)})

  @doc "Whether appends are acknowledged (`:ack`) or left unanswered (`:silent`) from now on."
  @spec set_appends(t(), :ack | :silent) :: :ok
  def set_appends(%__MODULE__{state: state}, mode) when mode in [:ack, :silent],
    do: Agent.update(state, &%{&1 | appends: mode})

  @doc "How many appends were answered as refused (`{:refuse_append, reason}`)."
  @spec refused(t()) :: non_neg_integer()
  def refused(%__MODULE__{state: state}), do: Agent.get(state, & &1.refused)

  @doc "How many streams were opened so far."
  @spec opened(t()) :: non_neg_integer()
  def opened(%__MODULE__{state: state}), do: Agent.get(state, &(&1.next_stream - 1))

  @doc "Every append taken so far, in arrival order: `{stream_id, sequence, values}`."
  @spec appends(t()) :: [{pos_integer(), non_neg_integer(), [binary()]}]
  def appends(%__MODULE__{state: state}), do: state |> Agent.get(& &1.taken) |> Enum.reverse()

  @doc "Writes `frame` (a whole response frame, length prefix included) to every connection that opened a stream."
  @spec push(t(), binary()) :: :ok
  def push(%__MODULE__{state: state}, <<_len::32, body::binary>>) do
    state |> Agent.get(& &1.streamers) |> Enum.each(&:gen_tcp.send(&1, body))
  end

  defp accept(stub) do
    case :gen_tcp.accept(stub.listen) do
      {:ok, sock} ->
        pid = spawn(fn -> receive(do: (:go -> serve(stub, sock))) end)
        :ok = :gen_tcp.controlling_process(sock, pid)
        send(pid, :go)
        accept(stub)

      {:error, _closed} ->
        :ok
    end
  end

  defp serve(stub, sock) do
    case :gen_tcp.recv(sock, 0) do
      {:ok, <<api_key::16, corr::32, payload::binary>>} ->
        case Agent.get(stub.state, &Map.get(&1.overrides, api_key, :default)) do
          :close ->
            :gen_tcp.close(sock)

          :silent ->
            serve(stub, sock)

          {:error, reason} ->
            :gen_tcp.send(sock, error_body(corr, reason))
            serve(stub, sock)

          reply ->
            answer(stub, sock, api_key, corr, payload, reply)
            serve(stub, sock)
        end

      {:error, _closed} ->
        :ok
    end
  catch
    # the stub was stopped while this connection was still open
    :exit, _noproc -> :gen_tcp.close(sock)
  end

  defp answer(stub, sock, 24, corr, _payload, _reply) do
    broker = %{id: "stub@127.0.0.1", host: "127.0.0.1", port: stub.port, status: :alive}
    state = %{version: 1, streams_enabled: true, brokers: [broker], vnodes: [0]}
    :gen_tcp.send(sock, ok_body(corr, Wire.encode_cluster_state_resp(state)))
  end

  defp answer(stub, sock, 25, corr, payload, _reply) do
    topic = Wire.decode_topic_routes_req(payload)

    %{version: version, ranges: ranges} =
      Agent.get_and_update(stub.state, fn
        %{routes: [only]} = state -> {only, %{state | served: only.version}}
        %{routes: [next | rest]} = state -> {next, %{state | routes: rest, served: next.version}}
      end)

    routes =
      for {range, key_start, key_end} <- ranges do
        %{
          range: range,
          key_start: key_start,
          key_end: key_end,
          state: :active,
          segment: %{segment: 0, primary: "stub@127.0.0.1"}
        }
      end

    resp = %{topic: topic, version: version, keyspace_bits: 8, ranges: routes}
    :gen_tcp.send(sock, ok_body(corr, Wire.encode_topic_routes_resp(resp)))
  end

  defp answer(stub, sock, 26, corr, _payload, _reply) do
    id =
      Agent.get_and_update(stub.state, fn s ->
        {s.next_stream, %{s | next_stream: s.next_stream + 1, streamers: Enum.uniq([sock | s.streamers])}}
      end)

    # the version the client last read, as a broker names the routes it opened the stream against
    version = Agent.get(stub.state, & &1.served)
    opened = %{stream_id: id, segment: 0, window_appends: 64, window_bytes: 16_777_216, routes_version: version}
    :gen_tcp.send(sock, ok_body(corr, Wire.encode_open_stream_resp(opened)))
  end

  defp answer(stub, sock, 27, corr, payload, reply) do
    {stream_id, seq, batch} = Wire.decode_append_req(payload)
    {:ok, entries} = Batch.decode(batch, max_inflated_bytes: 16_777_216)
    values = Enum.map(entries, fn {record, _tombstone} -> record.value end)
    mode = Agent.get_and_update(stub.state, &{&1.appends, %{&1 | taken: [{stream_id, seq, values} | &1.taken]}})

    errors =
      case reply do
        {:refuse_append, reason} ->
          Agent.update(stub.state, &%{&1 | refused: &1.refused + 1})
          [%{sequence: seq, reason: reason}]

        _acknowledge ->
          []
      end

    if mode == :ack do
      ack = %{
        stream_id: stream_id,
        acked_sequence: seq + 1,
        window_appends: 64,
        window_bytes: 16_777_216,
        errors: errors
      }

      :gen_tcp.send(sock, ok_body(corr, IO.iodata_to_binary(Wire.encode_push(:append_ack, ack))))
    end
  end

  # authentication, topic creation and anything else: an empty ok
  defp answer(_stub, sock, _api_key, corr, _payload, _reply), do: :gen_tcp.send(sock, ok_body(corr, <<>>))

  defp ok_body(corr, payload), do: <<corr::32, Wire.ok_code()::16, payload::binary>>

  # An unframed error-response body: the reason as the wire's present-string (tag 1, 32-bit length).
  defp error_body(corr, reason), do: <<corr::32, Wire.error_code()::16, 1, byte_size(reason)::32, reason::binary>>
end
