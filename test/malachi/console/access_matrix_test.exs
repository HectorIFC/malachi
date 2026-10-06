defmodule Malachi.Console.AccessMatrixTest do
  @moduledoc """
  Both HTTP endpoints decide through `Malachi.Console.Access`, and nothing else: for every row of its route
  table and every kind of caller, a real request to the endpoint that serves the row is refused exactly
  when `Access.authorize/2` refuses it, and with the role it names. A row changed in the table changes the
  expected and the observed outcome together, on whichever endpoint serves it; an endpoint that decided on
  its own would drift from the table and fail here.
  """

  use ExUnit.Case, async: false

  alias Malachi.Console.Access
  alias Malachi.Test.AccessHelper

  # The caller kinds: no credentials, a valid session with no role, each role, and a wire superuser.
  @callers [
    none: nil,
    wire_only: {[:produce, :consume], nil},
    viewer: {[], :viewer},
    editor: {[], :editor},
    admin: {[], :admin},
    wire_admin: {[:admin], nil}
  ]

  # A row's pattern made concrete. Parameters name a user, policy and topic that do not exist, so an
  # admitted mutation finds nothing to change.
  defp concrete(:any), do: "/metrics"

  defp concrete(pattern),
    do: "/" <> Enum.map_join(pattern, "/", fn segment -> if segment == :param, do: "matrix_absent", else: segment end)

  defp endpoint(["api", "v1" | _rest]), do: :console
  defp endpoint(_pattern), do: :dashboard

  setup do
    Malachi.RateLimiter.reset_bucket("127.0.0.1", :dashboard_auth)

    subjects =
      for {kind, spec} <- @callers, into: %{} do
        case spec do
          nil ->
            {kind, {nil, nil}}

          {permissions, role} ->
            username = "matrix_#{kind}"
            token = AccessHelper.account!(username, permissions, role)
            {:ok, subject} = Access.resolve(token, "127.0.0.1", "")
            {kind, {token, subject}}
        end
      end

    %{subjects: subjects}
  end

  test "every route on both endpoints answers as the table decides, for every caller", %{subjects: subjects} do
    for {method, pattern, level} <- Access.routes(), {kind, {token, subject}} <- subjects do
      path = concrete(pattern)
      # A public row is the same for every caller, and GET /logout would end the caller's session.
      response = AccessHelper.call(endpoint(pattern), method, path, if(level == :public, do: nil, else: token))
      label = "#{kind} #{method} #{path} (requires #{level})"

      case Access.authorize(subject, level) do
        :ok ->
          refute response.status in [401, 403], "#{label}: refused with #{response.status}"

        {:error, :authentication_required} ->
          # The two page routes send a browser without a session to the login form.
          if path in ["/", "/stream"],
            do: assert(response.status == 302, label),
            else: assert(%{status: 401, json: %{"type" => "errors.auth.unauthenticated"}} = response, label)

        {:error, {:missing_role, required, role}} ->
          assert response.status == 403, "#{label}: got #{response.status}"

          assert response.json == %{
                   "type" => "errors.auth.missing_role",
                   "status" => 403,
                   "required_role" => Atom.to_string(required),
                   "role" => role && Atom.to_string(role)
                 },
                 label
      end
    end
  end

  test "the table covers both endpoints" do
    endpoints = Access.routes() |> Enum.map(fn {_method, pattern, _level} -> endpoint(pattern) end) |> Enum.uniq()
    assert Enum.sort(endpoints) == [:console, :dashboard]
  end
end
