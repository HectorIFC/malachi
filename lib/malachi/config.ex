defmodule Malachi.Config do
  @moduledoc """
  Helpers for normalizing operator-supplied runtime configuration.

  Extracted from `config/runtime.exs` so the rules can be tested directly. That file is skipped
  entirely under `config_env() == :test`, so anything defined inline in it is unreachable from the
  suite; a pure function here is not.

  `checked/4` is the other half of the same idea, applied where the value is finally used rather than
  where it is parsed: the config layer turns an environment variable into a term without judging it,
  and the process that needs it decides whether the term can do the job.
  """

  require Logger

  alias Malachi.I18n

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

  @doc """
  The whole number in `raw`, or `default` when the variable is absent or blank. Anything else raises,
  so the node refuses to boot.

  The counterpart of `checked/4`, and what separates them is what being wrong costs. `checked/4` judges
  a value that parsed: `MALACHI_SCRUB_INTERVAL_MS=0` is a number, the operator's intent is legible, and
  the documented default is a reasonable stand-in, so refusing to boot there would turn one lost knob
  into a lost node. This one judges whether there is a number at all.

  `Integer.parse/1` keeps the leading digits and discards the rest, which is worse than refusing and
  worse than defaulting: `600_000` arrives as `600` and `10m` as `10`, values the operator never wrote
  and that no default can stand in for, because nothing here knows what was meant. The ones that hurt
  are the ones that come out smaller. `MALACHI_RETENTION_MAX_AGE_MS=7_776_000_000` would expire
  everything sealed more than seven milliseconds ago, on every replica, with no way back.

  `var` is the environment variable's own name, because that is what the operator has to go and fix.

  ## Examples

      iex> Malachi.Config.integer("MALACHI_X", nil, 5)
      5

      iex> Malachi.Config.integer("MALACHI_X", " ", 5)
      5

      iex> Malachi.Config.integer("MALACHI_X", " 12 ", 5)
      12
  """
  @spec integer(String.t(), String.t() | nil, value) :: integer() | value when value: term()
  def integer(var, raw, default), do: parsed(var, raw, default, &Integer.parse/1, "a whole number")

  @doc """
  The number in `raw` as a float, or `default` when the variable is absent or blank. Anything else
  raises, for the reason given in `integer/3`.

  `Float.parse/1` truncates the same way, and a decimal comma is the everyday case: `0,5` arrives as
  `0.0`, which as a memory threshold means the alarm fires immediately and forever.

  ## Examples

      iex> Malachi.Config.float("MALACHI_X", nil, 0.7)
      0.7

      iex> Malachi.Config.float("MALACHI_X", "0.5", 0.7)
      0.5
  """
  @spec float(String.t(), String.t() | nil, value) :: float() | value when value: term()
  def float(var, raw, default), do: parsed(var, raw, default, &Float.parse/1, "a number")

  defp parsed(_var, nil, default, _parse, _shape), do: default

  defp parsed(var, raw, default, parse, shape) when is_binary(var) and is_binary(raw) do
    case String.trim(raw) do
      "" ->
        default

      trimmed ->
        case parse.(trimmed) do
          {value, ""} -> value
          _malformed -> raise "#{var} must be #{shape}, got: #{inspect(raw)}"
        end
    end
  end

  @doc """
  `value` when `valid?` accepts it, otherwise `default`, saying out loud which setting was refused.

  Environment variables reach a process already parsed but not judged: `MALACHI_SCRUB_INTERVAL_MS=0`
  is a valid integer and a busy loop, and `MALACHI_RETENTION_SKIP_LEDGER_MAX=0` is a valid integer and
  a `FunctionClauseError` inside a server the application supervisor starts. Refusing to boot over an
  operator's typo turns one lost knob into a lost node, so the documented default is used and the line
  names the setting, what arrived and what is being used instead.

  `setting` appears in the log, so it should be the name the operator can act on.

  ## Examples

      iex> Malachi.Config.checked(5_000, :scrubber_interval, 60_000, &(is_integer(&1) and &1 > 0))
      5_000

  """
  @spec checked(value, atom(), value, (value -> boolean())) :: value when value: term()
  def checked(value, setting, default, valid?) do
    if valid?.(value) do
      value
    else
      Logger.warning(I18n.t(:setting_invalid, setting: setting, value: inspect(value), default: inspect(default)))
      default
    end
  end

  @doc """
  Normalizes the orphan sweep mode taken from `MALACHI_RETENTION_ORPHAN_SWEEP`.

  `nil` or a blank value is `:delete`, the documented default. `delete`, `report` and `off` are
  accepted whatever their case and surrounding whitespace. Anything else raises.

  Raising rather than falling back is the same rule as `ra_machine_version_pin/1`, and for the same
  reason: this is a knob whose wrong value is not a lost knob. `checked/4` exists for an interval,
  where the cost of refusing the value is one cadence; here `MALACHI_RETENTION_ORPHAN_SWEEP=Report`
  from an operator who meant NOT to delete would have selected the mode that deletes, and said
  nothing. A node that refuses to start is a problem an operator sees; directories that are gone are
  not.

  ## Examples

      iex> Malachi.Config.retention_orphan_sweep("  Report ")
      :report

      iex> Malachi.Config.retention_orphan_sweep(nil)
      :delete

  """
  @spec retention_orphan_sweep(String.t() | nil) :: :delete | :report | :off
  def retention_orphan_sweep(nil), do: :delete

  def retention_orphan_sweep(raw) when is_binary(raw) do
    case raw |> String.trim() |> String.downcase() do
      "" -> :delete
      "delete" -> :delete
      "report" -> :report
      "off" -> :off
      _other -> raise "MALACHI_RETENTION_ORPHAN_SWEEP must be delete, report or off, got: #{inspect(raw)}"
    end
  end
end
