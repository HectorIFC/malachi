defmodule Malachi.Loadtest do
  @moduledoc """
  A multi-core wire-protocol load generator: one BEAM process per connection, so N connections spread
  across all schedulers (unlike a single-threaded event loop). It drives produce/fetch/mixed/stream load
  and control-plane (user/acl) load against a running Malachi server, measuring throughput and latency.

  `run/1` is the entry point (the `mix malachi.loadtest` task is a thin wrapper). Metrics are collected
  lock-free: an `:counters` array for ops/records/errors plus backpressure events (dropped connections,
  server-shed `overloaded` produces, and reconnects) and a `Malachi.Loadtest.Histogram` for latency. A
  worker connects and authenticates, waits at a barrier so all connections start together, runs its
  scenario for `warmup + duration`, and records only during the measured window. It is resilient: a shed
  produce backs off and continues, and a dropped connection reconnects (capped) rather than aborting.
  """

  alias Malachi.Loadtest.Conn
  alias Malachi.Loadtest.Histogram
  alias Malachi.Log.Record
  alias Malachi.Wire

  @scenarios [:produce, :fetch, :mixed, :stream, :user, :acl]

  @ops 1
  @records 2
  @errors 3
  @dropped 4
  @overloaded 5
  @reconnects 6
  @counters 6

  # Backpressure: on an `:overloaded` shed the worker backs off briefly and keeps going; on a dropped
  # connection it reconnects with a short backoff, capped so a server that is truly down stops the worker
  # instead of spinning.
  @overloaded_backoff_ms 5
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
           pipeline: pos_integer()
         }

  @doc """
  Runs a load test and returns the report map. See the moduledoc / the `mix malachi.loadtest` task for
  options; all are optional with sensible defaults.
  """
  @spec run(keyword()) :: map()
  def run(opts \\ []) do
    cfg = normalize(opts)
    setup(cfg)

    ops = :counters.new(@counters, [:write_concurrency])
    hist = Histogram.new()
    parent = self()

    workers =
      for index <- 0..(cfg.connections - 1) do
        spawn_link(fn -> worker(parent, index, cfg, ops, hist) end)
      end

    Enum.each(workers, fn _ -> receive(do: ({:ready, _} -> :ok)) end)

    now = mono_ms()
    warmup_end = now + cfg.warmup * 1000
    measure_end = warmup_end + cfg.duration * 1000
    Enum.each(workers, fn w -> send(w, {:go, warmup_end, measure_end}) end)

    Enum.each(workers, fn _ -> receive(do: ({:done, _} -> :ok)) end)

    report = build_report(cfg, ops, hist)
    print(cfg, report)
    report
  end

  # --- config ---

  defp normalize(opts) do
    scenario = Keyword.get(opts, :scenario, :produce)
    unless scenario in @scenarios, do: raise(ArgumentError, "unknown scenario #{inspect(scenario)}")
    backlog? = scenario in [:fetch, :mixed, :stream]

    %{
      scenario: scenario,
      connections: Keyword.get(opts, :connections, 128),
      duration: Keyword.get(opts, :duration, 10),
      warmup: Keyword.get(opts, :warmup, 2),
      batch: Keyword.get(opts, :batch, 10),
      record_size: Keyword.get(opts, :record_size, 256),
      keys: Keyword.get(opts, :keys, 1000),
      pipeline: max(1, Keyword.get(opts, :pipeline, 1)),
      max: Keyword.get(opts, :max, 100),
      window: Keyword.get(opts, :window, 100),
      prepopulate: Keyword.get(opts, :prepopulate, if(backlog?, do: 10_000, else: 0)),
      topic: Keyword.get(opts, :topic) || "loadtest_#{System.system_time(:millisecond)}",
      json: Keyword.get(opts, :json, false),
      conn_opts: Keyword.take(opts, [:host, :port, :user, :pass, :token, :tls, :cacert, :cert, :key])
    }
  end

  defp setup(cfg) do
    {:ok, admin} = connect_auth(cfg.conn_opts)
    admin = create_topic(admin, cfg.topic)
    admin = prepopulate(admin, cfg)
    Conn.close(admin)
  end

  defp connect_auth(conn_opts) do
    with {:ok, conn} <- Conn.connect(conn_opts), do: Conn.authenticate(conn, conn_opts)
  end

  # keyspace_bits 8 (server default); ignore already-exists so re-runs work.
  defp create_topic(conn, topic) do
    {:ok, _code, _resp, conn} = Conn.request(conn, Wire.create_topic_key(), 1, Wire.encode_create_topic_req(topic, 8))
    conn
  end

  defp prepopulate(conn, %{prepopulate: n}) when n <= 0, do: conn

  defp prepopulate(conn, cfg) do
    value = :binary.copy("x", cfg.record_size)
    batch = for i <- 1..cfg.batch, do: %Record{value: value, key: "k#{i}", timestamp: 0, headers: []}
    payload = Wire.encode_produce_req(cfg.topic, batch)

    Enum.reduce(1..div(cfg.prepopulate, cfg.batch), {conn, 2}, fn _, {conn, corr} ->
      {:ok, _code, _resp, conn} = Conn.request(conn, Wire.produce_key(), corr, payload)
      {conn, corr + 1}
    end)
    |> elem(0)
  end

  # --- worker ---

  defp worker(parent, index, cfg, ops, hist) do
    {:ok, conn} = connect_auth(cfg.conn_opts)
    ctx = build_ctx(cfg, index)
    send(parent, {:ready, self()})
    {warmup_end, measure_end} = receive(do: ({:go, w, e} -> {w, e}))

    m = %{
      ops: ops,
      hist: hist,
      warmup_end: warmup_end,
      measure_end: measure_end,
      conn_opts: cfg.conn_opts,
      pipeline: cfg.pipeline
    }

    conn = drive(cfg, conn, ctx, m)

    Conn.close(conn)
    send(parent, {:done, self()})
  end

  # Per-connection context. produce pre-encodes its payload once (zero per-op encoding); the others carry
  # the mutable state the closed loop threads (fetch/stream cursor, admin op counter).
  defp build_ctx(cfg, index) do
    base = %{topic: cfg.topic, batch: cfg.batch, max: cfg.max, window: cfg.window}

    case scenario_for(cfg.scenario, index) do
      :produce ->
        value = :binary.copy("x", cfg.record_size)

        records =
          for i <- 1..cfg.batch do
            %Record{value: value, key: "k#{rem(index * cfg.batch + i, cfg.keys)}", timestamp: 0, headers: []}
          end

        Map.put(base, :op, {:produce, Wire.encode_produce_req(cfg.topic, records)})

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
        # The server shed this produce (backpressure): back off briefly and keep the connection.
        :overloaded ->
          shed(m, measuring)
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
    {conn, inflight, next} =
      Enum.reduce(1..m.pipeline, {conn, %{}, 2}, fn _, {conn, inflight, corr} ->
        case send_produce(ctx, conn, corr) do
          {:ok, conn} -> {conn, Map.put(inflight, corr, mono_us()), corr + 1}
          {:error, conn} -> {conn, inflight, corr}
        end
      end)

    pipe_loop(conn, ctx, m, inflight, next)
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
        if status == :overloaded, do: shed(m, measuring)
        if t0 && status != :overloaded, do: record(m, status, mono_us() - t0, measuring)

        {conn, inflight, next} =
          if now < m.measure_end do
            case send_produce(ctx, conn, next) do
              {:ok, conn} -> {conn, Map.put(inflight, next, mono_us()), next + 1}
              {:error, conn} -> {conn, inflight, next}
            end
          else
            {conn, inflight, next}
          end

        pipe_loop(conn, ctx, m, inflight, next)

      # The connection dropped: reconnect and re-prime the pipeline within the window.
      {:error, _reason} ->
        case after_drop(m) do
          {:ok, conn} -> pipelined(conn, ctx, m)
          :give_up -> conn
        end
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

  defp handle_push(conn, _ctx, _s, _m, _rejected), do: conn

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

      {:ok, _code, _resp, conn} ->
        {:error, conn, ctx}

      {:error, _reason} ->
        {:halt, conn, ctx}
    end
  end

  # Admin: user create/delete cycle (control-plane, ra-backed). One op = one round trip pair.
  defp do_op(%{op: {:user, st}} = ctx, conn, corr) do
    user = "loaduser_#{st.seq}"

    case Conn.request(conn, Wire.create_user_key(), corr, Wire.encode_create_user_req(user, "pw", [:produce])) do
      {:ok, 0, _resp, conn} ->
        {:ok, _c, _r, conn} = Conn.request(conn, Wire.delete_user_key(), corr + 1, Wire.encode_delete_user_req(user))
        {{:ok, 1}, conn, %{ctx | op: {:user, %{st | seq: st.seq + 1}}}}

      {:ok, _code, _resp, conn} ->
        {:error, conn, ctx}

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
        {:ok, _c, _r, conn} = Conn.request(conn, Wire.revoke_acl_key(), corr + 1, acl)
        {{:ok, 1}, conn, %{ctx | op: {:acl, %{st | seq: st.seq + 1}}}}

      {:ok, _code, _resp, conn} ->
        {:error, conn, ctx}

      {:error, _reason} ->
        {:halt, conn, ctx}
    end
  end

  # Produce response payload is `<<count::32>>`; count it as records (pipelined path).
  defp produce_status(0, <<count::32>>), do: {:ok, count}
  defp produce_status(_code, resp), do: error_status(resp)

  # An error response is either the server's backpressure shed (`:overloaded`, a light event the worker
  # backs off on) or a genuine error (`:error`).
  defp error_status(resp) do
    if Wire.decode_error_reason(resp) == "overloaded", do: :overloaded, else: :error
  end

  # --- recording ---

  defp record(_m, _status, _dt, false), do: :ok

  defp record(m, {:ok, n}, dt, true) do
    :counters.add(m.ops, @ops, 1)
    if n > 0, do: :counters.add(m.ops, @records, n)
    Histogram.record(m.hist, dt)
  end

  defp record(m, :error, _dt, true), do: :counters.add(m.ops, @errors, 1)

  # The server shed a produce under backpressure: count it (during the measured window) and back off
  # briefly so the worker does not immediately re-flood the overloaded broker.
  defp shed(m, measuring) do
    if measuring, do: :counters.add(m.ops, @overloaded, 1)
    Process.sleep(@overloaded_backoff_ms)
  end

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
  defp record_push(m, n) do
    :counters.add(m.ops, @ops, 1)
    :counters.add(m.ops, @records, n)
    Histogram.record(m.hist, 0)
  end

  # --- report ---

  defp build_report(cfg, ops, hist) do
    op_count = :counters.get(ops, @ops)
    records = :counters.get(ops, @records)
    errors = :counters.get(ops, @errors)
    dropped = :counters.get(ops, @dropped)
    overloaded = :counters.get(ops, @overloaded)
    reconnects = :counters.get(ops, @reconnects)
    secs = cfg.duration

    %{
      scenario: cfg.scenario,
      connections: cfg.connections,
      pipeline: cfg.pipeline,
      duration_s: secs,
      ops: op_count,
      records: records,
      errors: errors,
      dropped: dropped,
      overloaded: overloaded,
      reconnects: reconnects,
      ops_per_s: round(op_count / secs),
      records_per_s: round(records / secs),
      mb_per_s: Float.round(records * cfg.record_size / 1_048_576 / secs, 2),
      latency_ms: %{p50: ms(hist, 50), p99: ms(hist, 99), p99_9: ms(hist, 99.9), p99_99: ms(hist, 99.99)}
    }
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
      errors=#{r.errors}  dropped=#{r.dropped}  overloaded=#{r.overloaded}  reconnects=#{r.reconnects}
      latency ms: p50=#{l.p50} p99=#{l.p99} p99.9=#{l.p99_9} p99.99=#{l.p99_99}
    """)

    warn_if_empty(r)
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

  defp json(r) do
    l = r.latency_ms

    ~s({"scenario":"#{r.scenario}","connections":#{r.connections},"pipeline":#{r.pipeline},) <>
      ~s("duration_s":#{r.duration_s},"ops":#{r.ops},"records":#{r.records},"errors":#{r.errors},) <>
      ~s("dropped":#{r.dropped},"overloaded":#{r.overloaded},"reconnects":#{r.reconnects},) <>
      ~s("ops_per_s":#{r.ops_per_s},"records_per_s":#{r.records_per_s},"mb_per_s":#{r.mb_per_s},) <>
      ~s("latency_ms":{"p50":#{l.p50},"p99":#{l.p99},"p99_9":#{l.p99_9},"p99_99":#{l.p99_99}}})
  end

  defp mono_ms, do: System.monotonic_time(:millisecond)
  defp mono_us, do: System.monotonic_time(:microsecond)
end
