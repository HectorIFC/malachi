defmodule Malachi.Console.Endpoint do
  @moduledoc """
  The HTTP endpoint that serves the operator console: Bandit with a plain Plug pipeline
  (`Malachi.Console.Router`), on its own port beside the legacy dashboard (`Malachi.Dashboard`).

  It exists because the dashboard's hand written HTTP layer closes the socket after every response and
  has no keep alive, no conditional requests and no content negotiation, which a bundled single page
  application needs. The serving contract is section 10.6 of `docs/design/operator-interfaces.md`:
  the bundle is never committed (`priv/static/console/` holds only a `.gitignore`) and is to be
  built and injected by CI once the web console lands, which no pipeline does yet; a missing bundle
  answers 503 instead of failing the boot, and every asset is hashed at startup.

  ## Limits

  Bandit enforces the header count and the length of the request line and of each header line, and
  `Malachi.Console.Router` enforces the total, all read from `Malachi.HTTP.Limits`, the same settings
  the dashboard uses. Three differences are Bandit's, because it refuses while parsing, before any
  plug runs, and offers no hook for the answer; each gets a bare response, with no body and none of
  the security headers: a request line past the limit gets a 414 and a header line past it a 431,
  where the dashboard's socket driver closes the connection unanswered, and a header past the count
  gets a 431 where the dashboard answers with its JSON reason. Past the byte budget both
  answer the same JSON, because the console checks that one in its router. The transport contract
  suite asserts each side of each difference.

  The request line and the header block share one deadline, `Malachi.HTTP.Limits.recv_timeout_ms/0`,
  as on the dashboard: Bandit's own timeout is per read, so `Malachi.Console.HeaderDeadline` enforces
  the whole block, and closes the connection silently when it passes. The same deadline bounds how
  long an idle keep alive connection waits for its next request. Bandit's per-read timeout for the
  request line and the headers is set to twice that, so it never races the deadline. It does not
  bound a request body: Bandit reads a body with the `:read_timeout` given to `Plug.Conn.read_body/2`,
  15 seconds unless the caller says otherwise. The console reads no body today; the first route that
  does (#230) must pass its own. Acceptors and connections are bounded explicitly, so idle
  keep alive connections cannot pile up without limit: 10 acceptors (`:console_num_acceptors`) and
  1024 connections in total (`:console_max_connections`). Thousand Island counts connections per
  acceptor, so the total is split evenly and rounded up (103 each by default). A connection past its
  acceptor's share waits for one to close, up to five seconds, and is then closed unanswered. Compression at request time is off: assets ship
  precompressed, and Bandit would not compress a response carrying a strong ETag anyway.

  ## Authentication during the transition

  Nothing the endpoint serves today is authenticated: the application shell holds no data. The
  session store stays the one `Malachi.Auth` keeps, and the cookie stays `malachi_token`. A browser
  sends a cookie to every port of the host that set it, so a login on the dashboard already reaches
  this endpoint and nobody logs in twice. When the read API lands (#230), the access rules move out of
  `Malachi.Dashboard` into a module both endpoints call, and `Authorization: Bearer` is the primary
  mechanism.

  ## A port that cannot be opened

  If the listener cannot start (the port is taken, or not permitted), the error is logged and
  `start_link/1` returns `:ignore`: the broker boots and serves data without a console. That follows
  the NorthGuard talk, where a broker starts moving data within seconds of starting (transcript line
  626) and needs no other system propped up to run (lines 693 and 694); an operator surface is not
  a reason to stop a broker. Bandit itself also logs the taken port, in English and outside
  `Malachi.I18n`, just before this module's own translated line.

  ## Cut over

  `Malachi.Dashboard` is removed in one commit, once every route it serves has moved here, the
  transport contract suite (`test/http_transport_contract_test.exs`) passes against this endpoint
  alone, the Docker smoke test and `HEALTHCHECK` point here, and the Prometheus scrapes in `deploy/`
  have moved. Reverting is swapping the child spec back.
  """

  require Logger

  alias Malachi.Config
  alias Malachi.Console.Assets
  alias Malachi.HTTP.Limits
  alias Malachi.I18n

  @default_num_acceptors 10
  @default_max_connections 1_024

  @doc false
  def child_spec(arg) do
    %{id: child_id(arg), start: {__MODULE__, :start_link, [arg]}, type: :supervisor}
  end

  defp child_id({_port, opts}), do: {__MODULE__, Keyword.get(opts, :name, __MODULE__)}
  defp child_id(_port), do: {__MODULE__, __MODULE__}

  @doc """
  Starts the endpoint on `port`. `{port, name: name, static_dir: dir}` names it (so a test can run one
  beside the application's) and reads the bundle from `dir` instead of `Malachi.Console.Assets.static_dir/0`.
  Port 0 asks the operating system for a free port; `port/1` answers which. Returns `:ignore`, after
  logging why, when the port cannot be opened.
  """
  @spec start_link(:inet.port_number() | {:inet.port_number(), keyword()}) :: Supervisor.on_start() | :ignore
  def start_link({port, opts}) do
    name = Keyword.get(opts, :name, __MODULE__)
    manifest = Assets.build(Keyword.get_lazy(opts, :static_dir, &Assets.static_dir/0))

    case Bandit.start_link(bandit_options(port, manifest)) do
      {:ok, pid} ->
        {:ok, {_address, bound}} = ThousandIsland.listener_info(pid)
        :persistent_term.put({__MODULE__, name}, bound)
        Logger.info(I18n.t(:console_started, port: bound))
        {:ok, pid}

      {:error, reason} ->
        Logger.error(I18n.t(:console_listen_failed, port: port, reason: inspect(reason)))
        :ignore
    end
  end

  def start_link(port), do: start_link({port, []})

  @doc """
  The port the endpoint registered as `name` bound, which differs from the configured one when that was
  0; nil if it never started. The same contract as `Malachi.Dashboard.port/1`.
  """
  @spec port(atom()) :: :inet.port_number() | nil
  def port(name \\ __MODULE__), do: :persistent_term.get({__MODULE__, name}, nil)

  @doc """
  Every option handed to Bandit, built in one place. The scheme is plain HTTP: TLS for the console is
  #70, and when it lands it is a change to this function.
  """
  @spec bandit_options(:inet.port_number(), Assets.t()) :: keyword()
  def bandit_options(port, manifest) do
    headers = Limits.headers()
    acceptors = positive_setting(:console_num_acceptors, @default_num_acceptors)
    max_connections = positive_setting(:console_max_connections, @default_max_connections)

    [
      plug: {Malachi.Console.Router, %{manifest: manifest, max_header_bytes: headers.total}},
      scheme: :http,
      port: port,
      startup_log: false,
      http_1_options: [
        max_request_line_length: headers.line,
        max_header_length: headers.line,
        max_header_count: headers.count
      ],
      http_options: [compress: false, log_protocol_errors: false],
      # HTTP/2 over cleartext is never offered by a browser, and HeaderDeadline relies on a request
      # running in its connection's process, which is true of HTTP/1 only. TLS (#70) revisits this.
      http_2_options: [enabled: false],
      thousand_island_options: [
        handler_module: Malachi.Console.HeaderDeadline,
        num_acceptors: acceptors,
        num_connections: div(max_connections + acceptors - 1, acceptors),
        # Twice the deadline, so the header deadline (HeaderDeadline) always fires first and the two
        # never race. Bandit applies it to the request line and header reads only, not to a body.
        read_timeout: 2 * Limits.recv_timeout_ms()
      ]
    ]
  end

  defp positive_setting(key, default) do
    :malachi
    |> Application.get_env(key, default)
    |> Config.checked(key, default, &(is_integer(&1) and &1 > 0))
  end
end
