defmodule Malachi.Test.DevCompose do
  @moduledoc """
  The host ports `docker-compose.yml` (the dev stack) publishes, read from the file as text. Shared by the
  test of the file itself and the test of `scripts/worktree-env.sh`, which must write exactly the
  variables the file reads.
  """

  @compose Path.expand("../../docker-compose.yml", __DIR__)

  @doc "The file's path."
  @spec path() :: Path.t()
  def path, do: @compose

  @doc """
  Every quoted port mapping under a `ports:` list, as written (`"127.0.0.1:${VAR:-4040}:4040"` gives
  `127.0.0.1:${VAR:-4040}:4040`).
  """
  @spec mappings() :: [String.t()]
  def mappings do
    ~r/^\s+-\s+"([^"]*:\d+)"\s*$/m
    |> Regex.scan(File.read!(@compose), capture: :all_but_first)
    |> List.flatten()
  end

  @doc """
  Parses one mapping of the shape `127.0.0.1:${VAR:-default}:container` into `{var, default, container}`,
  or `:error` for any other shape.
  """
  @spec parse(String.t()) :: {String.t(), pos_integer(), pos_integer()} | :error
  def parse(mapping) do
    case Regex.run(~r/\A127\.0\.0\.1:\$\{([A-Z_]+):-(\d+)\}:(\d+)\z/, mapping) do
      [_all, var, default, container] -> {var, String.to_integer(default), String.to_integer(container)}
      nil -> :error
    end
  end
end
