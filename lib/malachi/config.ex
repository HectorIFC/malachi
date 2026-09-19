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

  @doc """
  Normalizes the OpenTelemetry sampling ratio taken from `MALACHI_TRACING_SAMPLE_RATIO`.

  Returns `{:ok, ratio}` for a number in `0.0..1.0`, and `:invalid` for anything else, leaving the
  caller to warn and fall back. Absent is `{:ok, 1.0}`: tracing is opt-in, so a deployment that turned
  it on without naming a ratio asked to see everything.

  Strict on purpose, unlike the lenient float parsing used for most settings. `Float.parse/1` returns
  the leading number and discards the rest, so `"0,1"` would become `0.0` and trace nothing, while
  `"ten"` would fail to parse and take the default, tracing *everything*. Both are silent, and the
  second is silent in the direction that puts real work on a production node. Requiring the parse to
  consume the whole string is what separates a value from a typo.

  ## Examples

      iex> Malachi.Config.sampling_ratio(nil)
      {:ok, 1.0}

      iex> Malachi.Config.sampling_ratio("0.25")
      {:ok, 0.25}

      iex> Malachi.Config.sampling_ratio("0")
      {:ok, 0.0}

      iex> Malachi.Config.sampling_ratio("0,1")
      :invalid

      iex> Malachi.Config.sampling_ratio("ten")
      :invalid

      iex> Malachi.Config.sampling_ratio("1.5")
      :invalid
  """
  @spec sampling_ratio(String.t() | nil) :: {:ok, float()} | :invalid
  def sampling_ratio(nil), do: {:ok, 1.0}

  def sampling_ratio(raw) when is_binary(raw) do
    case Float.parse(String.trim(raw)) do
      {ratio, ""} when ratio >= 0.0 and ratio <= 1.0 -> {:ok, ratio}
      _malformed_or_out_of_range -> :invalid
    end
  end

  @doc """
  Normalizes the `ra` machine version pin taken from `MALACHI_RA_MACHINE_VERSION`.

  Returns `nil` when the value is absent or blank (no pin: the node advertises the newest version its
  code implements) and the integer when it is a whole non-negative number. Anything else raises, so the
  node refuses to boot. Silently ignoring a typo would be the worst outcome here: an operator who pinned
  the version to keep a rollback possible would find the pin gone and the upgrade finalized, which
  moves the rollback floor. See `Malachi.Cluster.MachineVersion` for what the pin holds back.

  ## Examples

      iex> Malachi.Config.ra_machine_version_pin(nil)
      nil

      iex> Malachi.Config.ra_machine_version_pin(" ")
      nil

      iex> Malachi.Config.ra_machine_version_pin(" 1 ")
      1
  """
  @spec ra_machine_version_pin(String.t() | nil) :: non_neg_integer() | nil
  def ra_machine_version_pin(nil), do: nil

  def ra_machine_version_pin(raw) when is_binary(raw) do
    case String.trim(raw) do
      "" ->
        nil

      trimmed ->
        case Integer.parse(trimmed) do
          {pin, ""} when pin >= 0 ->
            pin

          _malformed_or_negative ->
            raise "MALACHI_RA_MACHINE_VERSION must be a non-negative integer, got: #{inspect(raw)}"
        end
    end
  end
end
