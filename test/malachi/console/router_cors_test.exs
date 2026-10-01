defmodule Malachi.Console.RouterCorsTest do
  # Not async: CORS is switched on in application env, which the async router tests must not see.
  use ExUnit.Case, async: false

  import Plug.Conn
  import Plug.Test

  alias Malachi.Console.Assets
  alias Malachi.Console.Router
  alias Malachi.Dashboard.SecurityHeaders
  alias Malachi.Test.ConsoleFixture

  setup do
    originals = Map.new([:dashboard_cors_enabled, :dashboard_cors_origins], &{&1, Application.fetch_env(:malachi, &1)})

    on_exit(fn ->
      Enum.each(originals, fn
        {key, {:ok, value}} -> Application.put_env(:malachi, key, value)
        {key, :error} -> Application.delete_env(:malachi, key)
      end)
    end)

    Application.put_env(:malachi, :dashboard_cors_enabled, true)
    Application.put_env(:malachi, :dashboard_cors_origins, ["*"])
    %{opts: Router.init(%{manifest: Assets.build(ConsoleFixture.bundle!()), max_header_bytes: 32_768})}
  end

  test "the console sends no CORS headers even where the dashboard would", %{opts: opts} do
    # The dashboard grants CORS on /metrics and /stream when it is on; the console serves a page there.
    assert {"access-control-allow-origin", "*"} in SecurityHeaders.headers("/metrics")

    for path <- ["/metrics", "/stream"] do
      conn = Router.call(conn(:get, path) |> put_req_header("origin", "https://elsewhere"), opts)
      assert conn.status == 200, path
      assert get_resp_header(conn, "access-control-allow-origin") == [], path
      assert get_resp_header(conn, "access-control-allow-methods") == [], path
    end
  end
end
