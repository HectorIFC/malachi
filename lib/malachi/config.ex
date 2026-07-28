defmodule Malachi.Config do
  @moduledoc """
  Helpers for normalizing operator-supplied runtime configuration.

  Extracted from `config/runtime.exs` so the rules can be tested directly. That file is skipped
  entirely under `config_env() == :test`, so anything defined inline in it is unreachable from the
  suite; a pure function here is not.
  """

  @doc """
  Normalizes an on-disk data directory taken from an environment variable.

  Returns the trimmed path, or `nil` when the value is absent or blank (including all-whitespace), in
  which case the caller falls back to its own default. In `:prod` a relative path is rejected rather
  than accepted: it resolves against the process working directory, so the node would write its durable
  segments to ephemeral container storage instead of the mounted volume, and the loss would only surface
  on the next restart. Other environments stay permissive, so a relative path is fine for local work.

  `var` names the source variable and appears in the error, so the operator sees which one to fix.

  ## Examples

      iex> Malachi.Config.data_dir("MALACHI_LOG_DATA_DIR", "/mnt/vol/log", :prod)
      "/mnt/vol/log"

      iex> Malachi.Config.data_dir("MALACHI_LOG_DATA_DIR", "  ", :prod)
      nil

      iex> Malachi.Config.data_dir("MALACHI_LOG_DATA_DIR", "data/log", :dev)
      "data/log"
  """
  def data_dir(var, value, env) when is_binary(var) do
    case value |> to_string() |> String.trim() do
      "" ->
        nil

      dir ->
        if env == :prod and Path.type(dir) != :absolute do
          raise "#{var} must be an absolute path, got: #{inspect(dir)}"
        end

        dir
    end
  end
end
