defmodule ComposeEnvTest do
  # docker-compose.yml is the dev stack, and two worktrees must be able to run it at once: no container
  # name fixed in the file (compose then names containers after the project) and every host port taken
  # from the environment, defaulting to the value the README documents. Read as text, like the file is:
  # the checks are about what is written, and CI's test job has no Docker to render it with.
  use ExUnit.Case, async: true

  alias Malachi.Test.DevCompose

  # Host port variable => its default, which is also the container port it maps to.
  @ports %{
    "MALACHI_TCP_PORT" => 4040,
    "MALACHI_DASHBOARD_PORT" => 4041,
    "JAEGER_UI_PORT" => 16_686,
    "OTLP_PORT" => 4318,
    "PROMETHEUS_PORT" => 9090
  }

  test "fixes no container name" do
    refute File.read!(DevCompose.path()) =~ ~r/^\s*container_name:/m
  end

  test "publishes every host port from its variable, on loopback, defaulting to today's port" do
    mappings =
      for mapping <- DevCompose.mappings() do
        assert {var, default, container} = DevCompose.parse(mapping),
               "not a loopback mapping with a variable host port: #{mapping}"

        assert default == container, "#{var} defaults to #{default} but maps to #{container}"
        {var, default}
      end

    assert length(mappings) == map_size(@ports)
    assert Map.new(mappings) == @ports
  end

  test "parse/1 refuses a fixed host port and a mapping open on every interface" do
    assert DevCompose.parse("127.0.0.1:4040:4040") == :error
    assert DevCompose.parse("${MALACHI_TCP_PORT:-4040}:4040") == :error
  end
end
