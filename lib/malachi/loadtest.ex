defmodule Malachi.Loadtest do
  @moduledoc """
  A multi-core wire-protocol load generator: one BEAM process per connection, so N connections spread
  across all schedulers (unlike a single-threaded event loop). It drives produce/fetch/mixed/stream load
  and control-plane (user/acl) load against a running Malachi server, measuring throughput and latency.

  `run/1` is the entry point (the `mix malachi.loadtest` task is a thin wrapper). Metrics are collected
  lock-free: an `:counters` array for ops/records/errors plus backpressure events (dropped connections,
  server-shed `overloaded` produces, quota-refused `rate_limited` produces, and reconnects) and a
  `Malachi.Histogram` for latency. A genuine error (any refusal other than those two) is also counted
  under the reason the server gave, in a table the report returns as `error_reasons`, so a run that
  records errors says which ones; the reasons always add up to `errors`. A worker connects and
  authenticates, waits at a barrier so all connections start together, runs its scenario for
  `warmup + duration`, and records only during the measured window. It is resilient: a shed produce
  backs off and continues, and a dropped connection reconnects (capped) rather than aborting.

  How the connections are OPENED is a strategy (`:connect_strategy`), because each one pays a full
  credential verification on the server and opening hundreds at once is a self-inflicted auth storm:

    * `:bounded` (default) - at most `:connect_concurrency` (default 32) connections inside
      connect+auth at once; the next starts when one finishes. Keeps each auth's individual latency
      near `concurrency / server_verify_rate` instead of the whole queue's drain time.
    * `:stagger` - connection `i` starts `i * :connect_stagger_ms` (default 100) after the first, with
      no in-flight bound. A time ramp that does not adapt to the server's verify rate.
    * `:all_at_once` - every connection starts simultaneously (the storm, kept on purpose for
      reproducing it and for comparing with historical runs).

  A connection that fails to open under any strategy fails the run with a `SetupError` naming how many
  of how many failed, rather than crashing a linked worker.
  """

  alias Malachi.Histogram
  alias Malachi.Loadtest.Conn
  alias Malachi.Log.Record
  alias Malachi.Wire

  defmodule SetupError do
    @moduledoc "A load test could not establish its connections; the message says how many and why."
    defexception [:message]
  end

  @scenarios [:produce, :fetch, :mixed, :stream, :user, :acl]
  @connect_strategies [:bounded, :stagger, :all_at_once]

  @ops 1
  @records 2
  @errors 3
  @dropped 4
  @overloaded 5
  @reconnects 6
  @rate_limited 7
  @counters 7

  # Backpressure: on a shed produce the worker backs off briefly and keeps going; on a dropped
  # connection it reconnects with a short backoff, capped so a server that is truly down stops the worker
  # instead of spinning. A shed is either `:overloaded` (the broker's group-commit valve) or
  # `:rate_limited` (this user over the configured publish quota); they are counted apart because they
  # tell an operator different things, and a run that hides one behind the other is a silent zero.
  @shed_backoff_ms 5
  @reconnect_backoff_ms 20
  @max_reconnect_tries 10

  # Metrics + measurement window + the bits needed to reconnect/re-prime (conn_opts, pipeline), bundled so
  # the per-op loops take a single context rather than many args.
  @typep m :: %{
           ops: :counters.counters_ref(),
           hist: term(),
           warmup_end: integer(),
           measure_end: integer(),
           conn_opts: keyword(),
           pipeline: pos_integer(),
           reasons: :ets.tid()
         }

  @doc """
  Runs a load test and returns the report map. See the moduledoc / the `mix malachi.loadtest` task for
  options; all are optional with sensible defaults.
  """
  @spec run(keyword()) :: map()
  def run(opts \\ []) do
    cfg = normalize(opts)
    setup(cfg)

    # Genuine errors by reason. A table rather than a `:counters` slot because the reasons are only known
    # when the server sends them; errors are rare, so the shared writes cost nothing on the hot path.
    reasons = :ets.new(:loadtest_error_reasons, [:set, :public, write_concurrency: true])

    try do
      measure(cfg, reasons)
    after
      :ets.delete(reasons)
    end
  end

  defp measure(cfg, reasons) do
    ops = :counters.new(@counters, [:write_concurrency])
    hist = Histogram.new()
    parent = self()

    # The gate only exists for the bounded strategy; the other strategies pace themselves.
    cfg =
      cfg
      |> Map.put(:gate, if(cfg.connect_strategy == :bounded, do: start_gate(cfg.connect_concurrency)))
      |> Map.put(:reasons, reasons)

    workers =
      for index <- 0..(cfg.connections - 1) do
        spawn_link(fn -> worker(parent, index, cfg, ops, hist) end)
      end

    failures =
      Enum.reduce(workers, [], fn _, acc ->
        receive do
          {:ready, _} -> acc
          {:connect_failed, _pid, reason} -> [reason | acc]
        end
      end)

    if cfg.gate, do: send(cfg.gate, :stop)

    # Fail the whole run rather than measuring fewer connections than the report would claim. The ready
    # workers are killed explicitly: a caller that rescues this exception (a test, IEx) never sends them
    # an exit signal through the link, and they would otherwise wait for :go forever, holding sockets.
    if failures != [] do
      Enum.each(workers, fn w ->
        Process.unlink(w)
        Process.exit(w, :kill)
      end)

      raise SetupError,
            "#{length(failures)} of #{cfg.connections} connections failed to connect and authenticate " <>
              "(first error: #{inspect(List.last(failures))}). A server saturated verifying credentials " <>
              "is the usual cause: lower --connections, or use --connect-strategy bounded with a smaller " <>
              "--connect-concurrency."
    end

    now = mono_ms()
    warmup_end = now + cfg.warmup * 1000
    measure_end = warmup_end + cfg.duration * 1000
    Enum.each(workers, fn w -> send(w, {:go, warmup_end, measure_end}) end)
    mark_measure_start(cfg.measure_marker, warmup_end)

    Enum.each(workers, fn _ -> receive(do: ({:done, _} -> :ok)) end)

    report = build_report(cfg, ops, hist)
    print(cfg, report)
    report
  end

  @doc false
  # The reproduce command for a set of options, without running anything. Exposed for the tests: the
  # branches worth pinning (TLS on, a topic a shell would misread) either need infrastructure to reach
  # through `run/1` or need a run that fails on purpose, and neither tells you more about the string
  # than calling the builder does. Not part of the API; `run/1` puts this in the report.
  @spec reproduce_command(keyword()) :: String.t()
  def reproduce_command(opts), do: opts |> normalize() |> command()

  # --- config ---

  defp normalize(opts) do
    scenario = Keyword.get(opts, :scenario, :produce)
    unless scenario in @scenarios, do: raise(ArgumentError, "unknown scenario #{inspect(scenario)}")
    backlog? = scenario in [:fetch, :mixed, :stream]

    %{
      scenario: scenario,
      connect_strategy: connect_strategy!(opts),
      connect_concurrency: positive!(opts, :connect_concurrency, 32),
      connect_stagger_ms: positive!(opts, :connect_stagger_ms, 100),
      connections: positive!(opts, :connections, 128),
      duration: positive!(opts, :duration, 10),
      warmup: non_negative!(opts, :warmup, 2),
      batch: positive!(opts, :batch, 10),
      record_size: positive!(opts, :record_size, 256),
      keys: positive!(opts, :keys, 1000),
      pipeline: max(1, Keyword.get(opts, :pipeline, 1)),
      max: Keyword.get(opts, :max, 100),
      window: Keyword.get(opts, :window, 100),
      prepopulate: Keyword.get(opts, :prepopulate, if(backlog?, do: 10_000, else: 0)),
      topic: Keyword.get(opts, :topic) || "loadtest_#{System.system_time(:millisecond)}",
      # Number of distinct topics to spread the connections over. 1 (default) keeps every connection on one
      # topic; N > 1 fans out `<topic>_0..<topic>_(N-1)`, so data-plane sharding (which pins a topic to a
      # shard) actually spreads load across shards. Connection `i` uses topic `rem(i, topics)`.
      topics: max(1, Keyword.get(opts, :topics, 1)),
      json: Keyword.get(opts, :json, false),
      measure_marker: measure_marker!(opts),
      # `--host` accepts a comma-separated list (a cluster): connection `i` targets `hosts[rem(i, n)]`,
      # spreading the load across every node's broker mailbox. A single host keeps today's behavior.
      hosts: opts |> Keyword.get(:host, "127.0.0.1") |> String.split(",", trim: true) |> Enum.map(&String.trim/1),
      conn_opts: Keyword.take(opts, [:port, :user, :pass, :token, :tls, :cacert, :cert, :key, :insecure])
    }
  end

  # The connection options for worker `index`: the shared opts plus its round-robin host.
  defp conn_opts_for(cfg, index) do
    [{:host, Enum.at(cfg.hosts, rem(index, length(cfg.hosts)))} | cfg.conn_opts]
  end

  # Validates the connect strategy and that each pacing knob is only passed with the strategy that reads
  # it: silently ignoring `connect_stagger_ms` under `:bounded` (or the concurrency under `:stagger`)
  # would let a run claim a pacing it never applied.
  defp connect_strategy!(opts) do
    strategy = Keyword.get(opts, :connect_strategy, :bounded)

    unless strategy in @connect_strategies do
      raise ArgumentError,
            "unknown connect_strategy #{inspect(strategy)} (expected one of: #{inspect(@connect_strategies)})"
    end

    if strategy != :bounded and Keyword.has_key?(opts, :connect_concurrency) do
      raise ArgumentError, "connect_concurrency only applies to the :bounded connect strategy"
    end

    if strategy != :stagger and Keyword.has_key?(opts, :connect_stagger_ms) do
      raise ArgumentError, "connect_stagger_ms only applies to the :stagger connect strategy"
    end

    strategy
  end

  # A zero or negative count would fail late and confusingly (an ArithmeticError after the whole run,
  # or an instantly-empty measurement): reject it up front with a named error instead.
  defp positive!(opts, key, default) do
    case Keyword.get(opts, key, default) do
      value when is_integer(value) and value > 0 -> value
      value -> raise ArgumentError, "#{key} must be a positive integer, got: #{inspect(value)}"
    end
  end

  # The file a harness waits for before sampling CPU. Checked before any connection opens: finding out
  # after hundreds of authentications that the marker cannot be written would lose the whole run, and
  # creating the file early to prove it would fire the signal before the window it marks.
  defp measure_marker!(opts) do
    case Keyword.get(opts, :measure_marker) do
      nil ->
        nil

      path when is_binary(path) and path != "" ->
        check_marker!(path)

      other ->
        raise ArgumentError, "measure_marker must be a non-empty path, got: #{inspect(other)}"
    end
  end

  # A marker that already exists is overwritten when the window opens, so it has to be a writable
  # regular file; a directory or a file owned by someone else passes a check of the parent alone and
  # then fails inside `File.write!/2`, after every connection has authenticated. Mirrored by
  # `markerProblem` in scripts/loadtest.js.
  defp check_marker!(path) do
    case File.stat(path) do
      {:ok, %File.Stat{type: :regular, access: access}} when access in [:write, :read_write] ->
        path

      {:ok, %File.Stat{type: type, access: access}} ->
        raise ArgumentError,
              "measure_marker #{inspect(path)} exists and is not a writable regular file " <>
                "(type #{type}, access #{access})"

      {:error, :enoent} ->
        check_marker_directory!(path)

      {:error, reason} ->
        raise ArgumentError, "measure_marker #{inspect(path)} cannot be used: #{inspect(reason)}"
    end
  end

  defp check_marker_directory!(path) do
    dir = Path.dirname(path)

    case File.stat(dir) do
      {:ok, %File.Stat{type: :directory, access: access}} when access in [:write, :read_write] ->
        path

      _missing_or_unwritable ->
        raise ArgumentError, "measure_marker directory #{inspect(dir)} does not exist or is not writable"
    end
  end

  defp non_negative!(opts, key, default) do
    case Keyword.get(opts, key, default) do
      value when is_integer(value) and value >= 0 -> value
      value -> raise ArgumentError, "#{key} must be a non-negative integer, got: #{inspect(value)}"
    end
  end

  defp setup(cfg) do
    admin =
      case connect_auth(conn_opts_for(cfg, 0)) do
        {:ok, conn} ->
          conn

        {:error, reason} ->
          raise SetupError,
                "could not connect and authenticate to create the topic (#{inspect(reason)}); " <>
                  "is the server up and are the credentials right?"
      end

    admin =
      Enum.reduce(topic_names(cfg), admin, fn topic, conn ->
        conn |> create_topic(topic) |> prepopulate(cfg, topic)
      end)

    Conn.close(admin)
  end

  # All topic names the run uses: the base topic for `topics: 1`, else `<base>_0..<base>_(topics-1)`.
  defp topic_names(%{topic: base, topics: 1}), do: [base]
  defp topic_names(%{topic: base, topics: n}), do: for(i <- 0..(n - 1), do: "#{base}_#{i}")

  # The topic connection `index` drives (round-robin over the topic set), so connections spread evenly.
  defp topic_for(%{topic: base, topics: 1}, _index), do: base
  defp topic_for(%{topic: base, topics: n}, index), do: "#{base}_#{rem(index, n)}"

  defp connect_auth(conn_opts) do
    with {:ok, conn} <- Conn.connect(conn_opts), do: Conn.authenticate(conn, conn_opts)
  end

  # Opens worker `index`'s connection under the configured strategy (see the moduledoc). Mid-run
  # reconnects bypass this on purpose: they are rare singles, not a storm, and the gate is gone by then.
  defp ramp_connect(%{connect_strategy: :bounded, gate: gate}, _index, conn_opts) do
    send(gate, {:acquire, self()})
    receive(do: (:granted -> :ok))
    result = connect_auth(conn_opts)
    send(gate, :release)
    result
  end

  defp ramp_connect(%{connect_strategy: :stagger} = cfg, index, conn_opts) do
    Process.sleep(index * cfg.connect_stagger_ms)
    connect_auth(conn_opts)
  end

  defp ramp_connect(%{connect_strategy: :all_at_once}, _index, conn_opts), do: connect_auth(conn_opts)

  # A tiny counting semaphore for the bounded strategy: at most `slots` grants outstanding, FIFO beyond
  # that. Linked to the run process and stopped explicitly once every worker is past connect.
  defp start_gate(slots), do: spawn_link(fn -> gate_loop(slots, :queue.new()) end)

  defp gate_loop(slots, waiting) do
    receive do
      {:acquire, pid} when slots > 0 ->
        send(pid, :granted)
        gate_loop(slots - 1, waiting)

      {:acquire, pid} ->
        gate_loop(slots, :queue.in(pid, waiting))

      :release ->
        case :queue.out(waiting) do
          {{:value, pid}, rest} ->
            send(pid, :granted)
            gate_loop(slots, rest)

          {:empty, rest} ->
            gate_loop(slots + 1, rest)
        end

      :stop ->
        :ok
    end
  end

  # keyspace_bits 8 (server default). A topic that already exists is fine, so a rerun works; any other
  # refusal fails the setup here, where it names its cause, rather than as a run full of produce errors.
  defp create_topic(conn, topic) do
    conn
    |> Conn.request(Wire.create_topic_key(), 1, Wire.encode_create_topic_req(topic, 8))
    |> expect_setup_ok("create topic", topic, ["already_exists"])
  end

  defp prepopulate(conn, %{prepopulate: n}, _topic) when n <= 0, do: conn

  defp prepopulate(conn, cfg, topic) do
    value = :binary.copy("x", cfg.record_size)
    batch = for i <- 1..cfg.batch, do: %Record{value: value, key: "k#{i}", timestamp: 0, headers: []}
    payload = Wire.encode_produce_req(topic, batch)

    # //1 keeps the range empty when prepopulate < batch (1..0 without a step enumerates DOWN and
    # would send two spurious batches).
    Enum.reduce(1..div(cfg.prepopulate, cfg.batch)//1, {conn, 2}, fn _, {conn, corr} ->
      conn = conn |> Conn.request(Wire.produce_key(), corr, payload) |> expect_setup_ok("prepopulate", topic, [])
      {conn, corr + 1}
    end)
    |> elem(0)
  end

  # A setup request either succeeded, or was refused with one of the `tolerated` reasons, or fails the run
  # with a SetupError naming the step, the topic and the reason. A backlog shorter than asked for, or a
  # topic that was never created, would otherwise be measured as if the setup had worked.
  defp expect_setup_ok({:ok, 0, _resp, conn}, _step, _topic, _tolerated), do: conn

  defp expect_setup_ok({:ok, _code, resp, conn}, step, topic, tolerated) do
    reason = Wire.decode_error_reason(resp)

    if reason in tolerated do
      conn
    else
      Conn.close(conn)
      raise SetupError, "could not #{step} #{inspect(topic)}: the server answered #{inspect(reason)}"
    end
  end

  defp expect_setup_ok({:error, reason}, step, topic, _tolerated) do
    raise SetupError, "could not #{step} #{inspect(topic)}: the connection failed (#{inspect(reason)})"
  end

  # Signals that the measured window has begun, for a harness that samples CPU over it. The harness
  # cannot know this on its own: every connection authenticates first, which took 25s at 512
  # connections on the CI runner, so a sampler started from the spawn measured the server verifying
  # credentials and the generator waiting on it. The workers already hold the deadlines, so the run
  # process has nothing else to do until warmup ends.
  #
  # The instant this marks is `warmup_end`, which is also the instant every worker starts recording
  # (`measuring = now >= m.warmup_end`), and that is why it is written here rather than before the
  # `:go` messages: sending first costs 810us to 512 workers and the write itself 363us, while writing
  # before them would put the marker at the START of the warmup and hand the sampler a window that
  # includes it, which is the error this whole mechanism exists to remove. With `warmup: 0` the two
  # coincide and a worker can record about a millisecond before the file appears, against a harness
  # that polls for it every 100ms.
  defp mark_measure_start(nil, _warmup_end), do: :ok

  defp mark_measure_start(path, warmup_end) do
    Process.sleep(max(0, warmup_end - mono_ms()))
    File.write!(path, "")
  end

  # --- worker ---

  defp worker(parent, index, cfg, ops, hist) do
    conn_opts = conn_opts_for(cfg, index)

    # A failed connect is reported to the parent and the worker ends normally: the parent decides the
    # run's fate (SetupError naming every failure) instead of a linked MatchError crash taking it down.
    case ramp_connect(cfg, index, conn_opts) do
      {:ok, conn} -> worker_loop(parent, index, cfg, ops, hist, conn_opts, conn)
      {:error, reason} -> send(parent, {:connect_failed, self(), reason})
    end
  end

  defp worker_loop(parent, index, cfg, ops, hist, conn_opts, conn) do
    ctx = build_ctx(cfg, index)
    send(parent, {:ready, self()})
    {warmup_end, measure_end} = receive(do: ({:go, w, e} -> {w, e}))

    # conn_opts carries this worker's host, so a reconnect goes back to the same node.
    m = %{
      ops: ops,
      hist: hist,
      warmup_end: warmup_end,
      measure_end: measure_end,
      conn_opts: conn_opts,
      pipeline: cfg.pipeline,
      reasons: cfg.reasons
    }

    conn = drive(cfg, conn, ctx, m)

    Conn.close(conn)
    send(parent, {:done, self()})
  end

  # Per-connection context. produce pre-encodes its payload once (zero per-op encoding); the others carry
  # the mutable state the closed loop threads (fetch/stream cursor, admin op counter).
  defp build_ctx(cfg, index) do
    topic = topic_for(cfg, index)
    base = %{topic: topic, batch: cfg.batch, max: cfg.max, window: cfg.window}

    case scenario_for(cfg.scenario, index) do
      :produce ->
        value = :binary.copy("x", cfg.record_size)

        records =
          for i <- 1..cfg.batch do
            %Record{value: value, key: "k#{rem(index * cfg.batch + i, cfg.keys)}", timestamp: 0, headers: []}
          end

        Map.put(base, :op, {:produce, Wire.encode_produce_req(topic, records)})

      :fetch ->
        Map.put(base, :op, {:fetch, %{group: "grp_#{index}", member: "mem_#{index}", cursor: nil}})

      :stream ->
        Map.put(base, :op, {:stream, %{group: "sgrp_#{index}", member: "smem_#{index}"}})

      :user ->
        Map.put(base, :op, {:user, %{seq: index * 1_000_000}})

      :acl ->
        Map.put(base, :op, {:acl, %{seq: index * 1_000_000}})
    end
  end

  # mixed: even connections produce, odd fetch. Others run uniformly.
  defp scenario_for(:mixed, index), do: if(rem(index, 2) == 0, do: :produce, else: :fetch)
  defp scenario_for(other, _index), do: other

  @spec drive(map(), Conn.t(), map(), m()) :: Conn.t()
  defp drive(_cfg, conn, %{op: {:stream, s}} = ctx, m), do: stream_loop(conn, ctx, s, m)
  defp drive(cfg, conn, %{op: {:produce, _}} = ctx, m) when cfg.pipeline > 1, do: pipelined(conn, ctx, m)
  defp drive(_cfg, conn, ctx, m), do: closed_loop(conn, ctx, m, 2)

  # --- closed loop (threads ctx so fetch/admin state advances) ---

  defp closed_loop(conn, ctx, m, corr) do
    now = mono_ms()

    if now >= m.measure_end do
      conn
    else
      t0 = mono_us()
      {status, conn, ctx} = do_op(ctx, conn, corr)
      measuring = now >= m.warmup_end

      case status do
        # The server refused this produce (backpressure or quota): back off briefly, keep the connection.
        shed_status when shed_status in [:overloaded, :rate_limited] ->
          shed(m, shed_status, measuring)
          closed_loop(conn, ctx, m, corr + 1)

        # Transport error: the connection dropped. Reconnect and keep going within the window.
        :halt ->
          case after_drop(m) do
            {:ok, conn} -> closed_loop(conn, ctx, m, corr + 1)
            :give_up -> conn
          end

        _ ->
          record(m, status, mono_us() - t0, measuring)
          closed_loop(conn, ctx, m, corr + 1)
      end
    end
  end

  # --- pipelined loop (produce only; W in flight per connection) ---

  defp pipelined(conn, ctx, m) do
    result =
      Enum.reduce_while(1..m.pipeline, {conn, %{}, 2}, fn _, {conn, inflight, corr} ->
        case send_produce(ctx, conn, corr) do
          {:ok, conn} -> {:cont, {conn, Map.put(inflight, corr, mono_us()), corr + 1}}
          {:error, conn} -> {:halt, {:send_failed, conn}}
        end
      end)

    case result do
      # A send error is a broken socket, the same as a recv error: count the drop and reconnect,
      # instead of silently draining the pipeline and ending the worker early with no drop recorded.
      {:send_failed, conn} -> reconnect_pipelined(conn, ctx, m)
      {conn, inflight, next} -> pipe_loop(conn, ctx, m, inflight, next)
    end
  end

  # On :give_up the dead conn is returned so the worker's final Conn.close stays shape-safe.
  defp reconnect_pipelined(dead_conn, ctx, m) do
    case after_drop(m) do
      {:ok, conn} -> pipelined(conn, ctx, m)
      :give_up -> dead_conn
    end
  end

  defp pipe_loop(conn, _ctx, _m, inflight, _next) when map_size(inflight) == 0, do: conn

  defp pipe_loop(conn, ctx, m, inflight, next) do
    case Conn.recv_frame(conn) do
      {:ok, body, conn} ->
        {corr, code, resp} = Wire.decode_response(body)
        now = mono_ms()
        measuring = now >= m.warmup_end
        {t0, inflight} = Map.pop(inflight, corr)
        status = produce_status(code, resp)
        if shed?(status), do: shed(m, status, measuring)
        if t0 && not shed?(status), do: record(m, status, mono_us() - t0, measuring)

        refill =
          if now < m.measure_end do
            send_produce(ctx, conn, next)
          else
            {:idle, conn}
          end

        case refill do
          {:ok, conn} -> pipe_loop(conn, ctx, m, Map.put(inflight, next, mono_us()), next + 1)
          {:idle, conn} -> pipe_loop(conn, ctx, m, inflight, next)
          # Broken socket on send: same treatment as a recv error below.
          {:error, conn} -> reconnect_pipelined(conn, ctx, m)
        end

      # The connection dropped: reconnect and re-prime the pipeline within the window.
      {:error, _reason} ->
        reconnect_pipelined(conn, ctx, m)
    end
  end

  defp send_produce(%{op: {:produce, payload}}, conn, corr) do
    case Conn.send_frame(conn, Wire.produce_key(), corr, payload) do
      :ok -> {:ok, conn}
      {:error, _} -> {:error, conn}
    end
  end

  # --- stream loop (server push + credit acks) ---
  #
  # A successful subscribe sends no response: the server just starts pushing frames tagged with the
  # subscribe correlation id (payload = a fetch_resp), and the client returns credit + advances via
  # stream_ack. So we fire the subscribe and then read pushes, recving with the time left in the window
  # so we stop promptly at the deadline. A non-ok frame means the subscribe was rejected.

  defp stream_loop(conn, ctx, s, m) do
    payload = Wire.encode_subscribe_req(ctx.topic, s.group, s.member, ctx.window, ctx.max)

    case Conn.send_frame(conn, Wire.subscribe_key(), 2, payload) do
      :ok -> stream_recv(conn, ctx, s, m)
      {:error, _} -> conn
    end
  end

  defp stream_recv(conn, ctx, s, m) do
    remaining = m.measure_end - mono_ms()

    if remaining <= 0 do
      conn
    else
      case Conn.recv_frame(conn, remaining) do
        {:ok, body, conn} ->
          handle_push(conn, ctx, s, m, Wire.decode_response(body))

        # The connection dropped: reconnect and re-subscribe within the window.
        {:error, _reason} ->
          case after_drop(m) do
            {:ok, conn} -> stream_loop(conn, ctx, s, m)
            :give_up -> conn
          end
      end
    end
  end

  defp handle_push(conn, ctx, s, m, {_corr, 0, resp}) do
    {records, next_cursor} = Wire.decode_fetch_resp(resp)
    n = length(records)
    if mono_ms() >= m.warmup_end and n > 0, do: record_push(m, n)
    ack = Wire.encode_stream_ack_req(ctx.topic, s.group, s.member, next_cursor, n)
    _ = Conn.send_frame(conn, Wire.stream_ack_key(), 3, ack)
    stream_recv(conn, ctx, s, m)
  end

  # A refused subscribe. The server sends no frame for a subscription it did not accept, so there is
  # nothing left to wait for and this worker's stream ends here. That makes the refusal terminal, like a
  # dropped connection, and it is counted whatever the window `after_drop/1` counts drops in: the
  # subscribe goes out the moment the barrier lifts, so a warmup (2s by default) would otherwise swallow
  # every refusal, and the worker never gets a second chance to record one. No backoff either, for the
  # same reason: there is nothing left to back off from.
  defp handle_push(conn, _ctx, _s, m, {_corr, _code, resp}) do
    case error_status(resp) do
      {:error, reason} -> count_error(m, reason)
      shed_status -> :counters.add(m.ops, shed_counter(shed_status), 1)
    end

    conn
  end

  # --- ops (closed-loop round trips); each returns {status, conn, ctx} ---

  defp do_op(%{op: {:produce, payload}} = ctx, conn, corr) do
    case Conn.request(conn, Wire.produce_key(), corr, payload) do
      {:ok, 0, <<count::32>>, conn} -> {{:ok, count}, conn, ctx}
      {:ok, _code, resp, conn} -> {error_status(resp), conn, ctx}
      {:error, _reason} -> {:halt, conn, ctx}
    end
  end

  defp do_op(%{op: {:fetch, st}} = ctx, conn, corr) do
    payload = Wire.encode_fetch_req(ctx.topic, st.cursor, st.group, st.member, ctx.max, 0)

    case Conn.request(conn, Wire.fetch_key(), corr, payload) do
      {:ok, 0, resp, conn} ->
        {records, next_cursor} = Wire.decode_fetch_resp(resp)
        # at the tail (nothing new) reset to start so the reader keeps pulling the growing backlog
        st = %{st | cursor: if(records == [], do: nil, else: next_cursor)}
        {{:ok, length(records)}, conn, %{ctx | op: {:fetch, st}}}

      {:ok, _code, resp, conn} ->
        {genuine_error(resp), conn, ctx}

      {:error, _reason} ->
        {:halt, conn, ctx}
    end
  end

  # Admin: user create/delete cycle (control-plane, ra-backed). One op = one round trip pair.
  defp do_op(%{op: {:user, st}} = ctx, conn, corr) do
    user = "loaduser_#{st.seq}"

    case Conn.request(conn, Wire.create_user_key(), corr, Wire.encode_create_user_req(user, "pw", [:produce])) do
      {:ok, 0, _resp, conn} ->
        conn
        |> Conn.request(Wire.delete_user_key(), corr + 1, Wire.encode_delete_user_req(user))
        |> admin_cleanup(conn, ctx, {:user, %{st | seq: st.seq + 1}})

      {:ok, _code, resp, conn} ->
        {genuine_error(resp), conn, ctx}

      {:error, _reason} ->
        {:halt, conn, ctx}
    end
  end

  # Admin: acl grant/revoke cycle.
  defp do_op(%{op: {:acl, st}} = ctx, conn, corr) do
    user = "acluser_#{st.seq}"
    acl = Wire.encode_acl_req(user, "produce", "topic-*")

    case Conn.request(conn, Wire.grant_acl_key(), corr, acl) do
      {:ok, 0, _resp, conn} ->
        conn
        |> Conn.request(Wire.revoke_acl_key(), corr + 1, acl)
        |> admin_cleanup(conn, ctx, {:acl, %{st | seq: st.seq + 1}})

      {:ok, _code, resp, conn} ->
        {genuine_error(resp), conn, ctx}

      {:error, _reason} ->
        {:halt, conn, ctx}
    end
  end

  # The second half of an admin cycle (delete the user, revoke the grant), with the same three outcomes as
  # the first half. Ignoring this response counted a refused cleanup as a completed op, left the user or
  # the grant behind in replicated state, and turned a transport failure into a MatchError that took the
  # worker down. `sent_on` is the connection the request went out on, for the transport case, which
  # answers no connection of its own. The sequence advances either way, so a retry after a cleanup that
  # never happened does not keep colliding with what the previous attempt left behind.
  defp admin_cleanup({:ok, 0, _resp, conn}, _sent_on, ctx, op), do: {{:ok, 1}, conn, %{ctx | op: op}}
  defp admin_cleanup({:ok, _code, resp, conn}, _sent_on, ctx, op), do: {genuine_error(resp), conn, %{ctx | op: op}}
  defp admin_cleanup({:error, _reason}, sent_on, ctx, op), do: {:halt, sent_on, %{ctx | op: op}}

  # Produce response payload is `<<count::32>>`; count it as records (pipelined path).
  defp produce_status(0, <<count::32>>), do: {:ok, count}
  defp produce_status(_code, resp), do: error_status(resp)

  # A produce error response is one of the server's two refusals, both light events the worker backs off
  # on (`:overloaded` from the group-commit valve, `:rate_limited` from the configured publish quota), or a
  # genuine error that keeps its reason.
  defp error_status(resp) do
    case Wire.decode_error_reason(resp) do
      "overloaded" -> :overloaded
      "rate_limited" -> :rate_limited
      reason -> {:error, reason}
    end
  end

  # Any other operation's error response: always genuine, and always with the reason the server gave.
  defp genuine_error(resp), do: {:error, Wire.decode_error_reason(resp)}

  # A refusal the worker retries through, as opposed to a completed op or a genuine error.
  defp shed?(status), do: status in [:overloaded, :rate_limited]

  # --- recording ---

  defp record(_m, _status, _dt, false), do: :ok

  defp record(m, {:ok, n}, dt, true) do
    :counters.add(m.ops, @ops, 1)
    if n > 0, do: :counters.add(m.ops, @records, n)
    Histogram.record(m.hist, dt)
  end

  defp record(m, {:error, reason}, _dt, true), do: count_error(m, reason)

  # The error counter and the reason move together, so the breakdown always adds up to `errors`.
  defp count_error(m, reason) do
    :counters.add(m.ops, @errors, 1)
    :ets.update_counter(m.reasons, reason, 1, {reason, 0})
  end

  # The server refused a produce: count it under its own reason (during the measured window) and back off
  # briefly so the worker does not immediately re-flood the broker.
  defp shed(m, status, measuring) do
    if measuring, do: :counters.add(m.ops, shed_counter(status), 1)
    Process.sleep(@shed_backoff_ms)
  end

  defp shed_counter(:overloaded), do: @overloaded
  defp shed_counter(:rate_limited), do: @rate_limited

  # A transport error dropped the connection. Count the drop (always visible, an event not a rate) and try
  # to recover: `{:ok, conn}` with a fresh authenticated connection to continue on, or `:give_up` when the
  # server is down (retry cap hit) or the window has closed.
  defp after_drop(m) do
    :counters.add(m.ops, @dropped, 1)

    case reconnect(m, 0) do
      {:ok, conn} ->
        :counters.add(m.ops, @reconnects, 1)
        {:ok, conn}

      :give_up ->
        :give_up
    end
  end

  defp reconnect(m, tries) do
    cond do
      mono_ms() >= m.measure_end -> :give_up
      tries >= @max_reconnect_tries -> :give_up
      true -> reconnect_attempt(m, tries)
    end
  end

  defp reconnect_attempt(m, tries) do
    Process.sleep(@reconnect_backoff_ms)

    case connect_auth(m.conn_opts) do
      {:ok, conn} -> {:ok, conn}
      {:error, _} -> reconnect(m, tries + 1)
    end
  end

  # stream pushes count as one op per page plus its records.
  # No latency sample: a server push carries no client-side start timestamp, and recording 0 made
  # every stream run report p50=0.0 as if it measured sub-microsecond latency. Stream latency is
  # simply not measured client-side; the histogram stays honest (empty).
  defp record_push(m, n) do
    :counters.add(m.ops, @ops, 1)
    :counters.add(m.ops, @records, n)
  end

  # --- report ---

  defp build_report(cfg, ops, hist) do
    error_reasons = Map.new(:ets.tab2list(cfg.reasons))
    op_count = :counters.get(ops, @ops)
    records = :counters.get(ops, @records)
    errors = :counters.get(ops, @errors)
    dropped = :counters.get(ops, @dropped)
    overloaded = :counters.get(ops, @overloaded)
    rate_limited = :counters.get(ops, @rate_limited)
    reconnects = :counters.get(ops, @reconnects)
    secs = cfg.duration

    %{
      meta: meta(cfg),
      scenario: cfg.scenario,
      connections: cfg.connections,
      # The flush regime the throughput describes, as fields of their own rather than only inside
      # meta.command, whose syntax differs between the two generators. Mirrored by scripts/loadtest.js.
      batch: cfg.batch,
      record_size: cfg.record_size,
      pipeline: cfg.pipeline,
      duration_s: secs,
      ops: op_count,
      records: records,
      errors: errors,
      error_reasons: error_reasons,
      dropped: dropped,
      overloaded: overloaded,
      rate_limited: rate_limited,
      reconnects: reconnects,
      ops_per_s: round(op_count / secs),
      records_per_s: round(records / secs),
      mb_per_s: Float.round(records * cfg.record_size / 1_048_576 / secs, 2),
      latency_ms: %{p50: ms(hist, 50), p99: ms(hist, 99), p99_9: ms(hist, 99.9), p99_99: ms(hist, 99.99)}
    }
  end

  # --- reproduce metadata ---

  # What turns a number into a result: when it was taken, from which commit, on what. Deliberately the
  # same shape the Node generator writes (`buildMeta` in scripts/loadtest.js), so the published pages can
  # render a run from either tool through one path instead of two.
  #
  # The version and the git ref describe THIS tree, the one the generator ran from, not the server it
  # drove. They coincide in every setup that matters (CI, a local run) and the distinction only appears
  # when someone points the generator at a host running another build, which the recorded host makes
  # visible.
  defp meta(cfg) do
    %{
      timestamp: DateTime.utc_now() |> DateTime.to_iso8601(),
      command: command(cfg),
      git_ref: git_ref(),
      git_ref_date: git(["show", "-s", "--format=%cI", "HEAD"]),
      malachi_version: version(),
      hardware: hardware()
    }
  end

  # Rebuilt from the EFFECTIVE config rather than quoted from `System.argv/0`, which is what the Node
  # generator records. Two reasons: `run/1` is called directly as well (the mix task is a thin wrapper),
  # so there is not always an argv to quote; and a reproduction needs the defaults too, while an argv
  # shows only what was typed. Credential VALUES are deliberately absent: this string gets committed
  # and published, and `--pass` or `--token` has no business in either. File paths are not values and
  # do come along, see `tls_options/1`.
  defp command(cfg) do
    options =
      [
        scenario: cfg.scenario,
        connections: cfg.connections,
        duration: cfg.duration,
        warmup: cfg.warmup,
        batch: cfg.batch,
        "record-size": cfg.record_size,
        keys: cfg.keys,
        pipeline: cfg.pipeline,
        max: cfg.max,
        window: cfg.window,
        prepopulate: cfg.prepopulate,
        topics: cfg.topics,
        # The topic and the port were both missing once, and both broke the reproduction in the same
        # quiet way: a run against port 5040 published a command that connects to 4040, and a fetch
        # run against an existing topic published one that would invent a new empty topic instead.
        # The port default matches `Conn.connect/1`, so the recorded value is the one actually
        # dialed.
        topic: cfg.topic,
        host: Enum.join(cfg.hosts, ","),
        port: Keyword.get(cfg.conn_opts, :port, 4040)
      ] ++
        connect_options(cfg) ++ auth_options(cfg.conn_opts) ++ tls_options(cfg.conn_opts) ++ json_option(cfg)

    Enum.join(["mix malachi.loadtest" | Enum.map(options, &render_option/1)], " ")
  end

  # The connect strategy shapes the run (it is what tames the auth storm), so it must be reproducible
  # from the record. Its pacing knob travels only with the strategy that reads it: the up-front
  # validation rejects a cross-strategy knob, so a rebuilt command carrying both would refuse to run.
  defp connect_options(cfg) do
    case cfg.connect_strategy do
      :bounded -> ["connect-strategy": "bounded", "connect-concurrency": cfg.connect_concurrency]
      :stagger -> ["connect-strategy": "stagger", "connect-stagger-ms": cfg.connect_stagger_ms]
      :all_at_once -> ["connect-strategy": "all-at-once"]
    end
  end

  # `--name=value` rather than two arguments, for every option rather than only the ones that look
  # risky. `OptionParser` reads a separate value beginning with a hyphen as another switch, so a topic
  # named `-weird` produced a command that does not merely target the wrong thing but refuses to run,
  # complaining that --topic is missing its argument and that -w, -e, -i, -r and -d are unknown
  # options. That name passes the server's allowlist, which permits hyphens, so this is reachable with
  # a topic the broker accepts rather than only with one it rejects.
  #
  # Uniform because the alternative is deciding per option which values could start with a hyphen, and
  # that judgement is what has already been wrong three times in this function.
  defp render_option({flag, true}), do: "--#{flag}"
  defp render_option({flag, value}), do: "--#{flag}=#{shell_arg(value)}"

  # Transport last, and only when TLS is on, so an ordinary plaintext command is unchanged. Omitting
  # it was the third thing this function got wrong in the same way: a run over TLS published a command
  # that reconnects in plaintext, which does not reproduce the run and does not say so. It measures a
  # different thing too, since the handshake and the record layer are part of what was timed.
  #
  # The certificate paths come along. They are paths, not secrets, which is the line this function
  # already draws: `--pass` and `--token` are excluded because they carry the secret VALUE, and a
  # command that names a key file leaks no key. Dropping them would produce a command that runs and
  # quietly reproduces a weaker configuration, which is the bug being fixed rather than a smaller
  # version of it.
  # The identity the run authenticated as, which decides what it was allowed to do: a user without a
  # produce ACL on the topic measures rejections, and the published command would reproduce it as
  # admin and disagree with its own numbers. The name is not the secret, which is the line
  # `tls_options/1` already draws: `--pass` and `--token` stay out because they carry the VALUE.
  # Nothing is recorded for a token or certificate run, where there is no username in play and naming
  # one would describe a different handshake than the one measured.
  defp auth_options(conn_opts) do
    cond do
      Keyword.get(conn_opts, :token) -> []
      Keyword.get(conn_opts, :cert) -> []
      true -> [user: Keyword.get(conn_opts, :user, "admin")]
    end
  end

  # The flag that produced the artifact the command is printed inside. Without it the reproduction
  # prints a human-readable summary to the terminal and writes no JSON, so the one thing it cannot do
  # is regenerate the page it is quoted on.
  defp json_option(%{json: true}), do: [json: true]
  defp json_option(_cfg), do: []

  defp tls_options(conn_opts) do
    if Keyword.get(conn_opts, :tls) do
      # --insecure travels with the rest: it turns off server verification, so a recorded command that
      # omitted it would reproduce a STRONGER configuration than the run and disagree with its numbers,
      # the same reasoning that put the certificate paths here.
      insecure = if Keyword.get(conn_opts, :insecure), do: [insecure: true], else: []

      [tls: true] ++
        for(
          option <- [:cacert, :cert, :key],
          path = Keyword.get(conn_opts, option),
          do: {option, path}
        ) ++ insecure
    else
      []
    end
  end

  # The free-text values above are the ones a shell can misread. A topic with a space records as
  # `--topic has a space`, which on replay parses as `--topic has` and silently targets a different
  # topic; the host list is not validated anywhere at all. The server's allowlist would reject that
  # topic, but the command is recorded even for a run that failed, and a string this module emits
  # should not depend on a downstream validator to be correct.
  #
  # Quoted only when it has to be, so an ordinary command stays readable. Single quotes because they
  # are the one POSIX quoting with no escapes inside; an embedded single quote is closed, escaped and
  # reopened, which is the standard idiom for exactly this.
  defp shell_arg(value) do
    string = to_string(value)

    if string != "" and string =~ ~r{\A[A-Za-z0-9._,:/@=+-]+\z} do
      string
    else
      "'" <> String.replace(string, "'", "'\\''") <> "'"
    end
  end

  defp version do
    case Application.spec(:malachi, :vsn) do
      nil -> nil
      vsn -> to_string(vsn)
    end
  end

  defp git_ref do
    case git(["rev-parse", "--short", "HEAD"]) do
      nil -> nil
      ref -> if git(["status", "--porcelain"]) in [nil, ""], do: ref, else: ref <> "-dirty"
    end
  end

  # nil rather than a raise when git is missing or this is not a checkout: the generator has to run from
  # a container and from a release tarball too, and a blank provenance field is a smaller problem than a
  # load test that refuses to start over it.
  defp git(args) do
    case System.cmd("git", args, stderr_to_stdout: true) do
      {output, 0} -> String.trim(output)
      {_output, _status} -> nil
    end
  rescue
    _error -> nil
  end

  # The BEAM knows an architecture triple, not a marketing CPU name, and reports `:unknown` for the
  # processor count on some platforms. Recording only what it actually knows beats filling the gap:
  # a missing field reads as missing, an invented one reads as fact.
  defp hardware do
    %{
      cpu: to_string(:erlang.system_info(:system_architecture)),
      cores: logical_processors(),
      schedulers: :erlang.system_info(:schedulers_online),
      os: os_description()
    }
  end

  defp logical_processors do
    case :erlang.system_info(:logical_processors_available) do
      count when is_integer(count) -> count
      _unknown -> nil
    end
  end

  defp os_description do
    {_family, name} = :os.type()

    version =
      case :os.version() do
        {major, minor, release} -> "#{major}.#{minor}.#{release}"
        other -> to_string(other)
      end

    "#{name} #{version}"
  end

  defp ms(hist, p), do: Float.round(Histogram.percentile(hist, p) / 1000, 2)

  defp print(%{json: true}, report) do
    IO.puts(json(report))
    warn_if_empty(report)
  end

  defp print(_cfg, r) do
    l = r.latency_ms

    IO.puts("""

    #{r.scenario} (#{r.connections} conns, pipeline #{r.pipeline}) over #{r.duration_s}s
      #{r.records_per_s} rec/s  #{r.ops_per_s} ops/s  #{r.mb_per_s} MB/s
      errors=#{r.errors}  dropped=#{r.dropped}  reconnects=#{r.reconnects}
      server-refused: overloaded=#{r.overloaded}  rate_limited=#{r.rate_limited}
      latency ms: p50=#{l.p50} p99=#{l.p99} p99.9=#{l.p99_9} p99.99=#{l.p99_99}
    """)

    print_error_reasons(r.error_reasons)

    warn_if_empty(r)
  end

  # Only when there is something to name, most frequent first, so a clean run prints what it always did.
  defp print_error_reasons(reasons) when map_size(reasons) == 0, do: :ok

  defp print_error_reasons(reasons) do
    listed =
      reasons
      |> Enum.sort_by(fn {reason, n} -> {-n, reason} end)
      |> Enum.map_join("  ", fn {reason, n} -> "#{reason}=#{n}" end)

    IO.puts("  error reasons: #{listed}")
  end

  # A run that recorded nothing is not "server idle": name the likely cause so it is never a silent zero.
  defp warn_if_empty(%{ops: 0} = r) do
    cause =
      if r.dropped > 0 do
        "#{r.dropped} connection(s) dropped under load (the server likely timed out the produce call)"
      else
        "per-op latency likely exceeds warmup + duration"
      end

    IO.puts(
      :stderr,
      "warning: no op completed in the measured window (#{cause}); lower --connections/--batch or raise --duration"
    )
  end

  defp warn_if_empty(_r), do: :ok

  # Encoded rather than concatenated. The report now carries free text from the environment (the CPU
  # architecture string, a git ref, a topic name), and hand-built JSON has no escaping: one quote or
  # backslash in any of them would emit a document no parser accepts, which is a bad way for a published
  # result to fail. Jason is already a dependency.
  defp json(report), do: Jason.encode!(report)

  defp mono_ms, do: System.monotonic_time(:millisecond)
  defp mono_us, do: System.monotonic_time(:microsecond)
end
