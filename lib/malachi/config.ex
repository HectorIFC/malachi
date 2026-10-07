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

  alias Malachi.Auth
  alias Malachi.Auth.ConsoleRole
  alias Malachi.Cluster.Policy
  alias Malachi.I18n

  # The control plane a single node runs when MALACHI_LOG_CLUSTER is not set (`log_cluster/2`).
  @default_log_cluster :malachi_log

  # 7 days, Kafka's `segment.ms` default: long enough that a topic busy enough to fill a segment rolls by
  # size first, short enough that a quiet one is not held back from age retention for weeks.
  @default_segment_max_age_ms 7 * 24 * 60 * 60 * 1000

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
  A retention bound from `raw`: `nil` when the variable is absent or blank (the rule is off), otherwise a
  whole number from 0 through 2^64 - 1. Anything else stops the node, for the reason `integer/3` gives
  and two more. A negative age or byte budget expires everything the rule can see. A bound above
  2^64 - 1 is stored whole but travels in 64 bits (`Malachi.Wire`), so `get_topic_policy` would report
  it truncated, 2^64 as a zero budget, while the sweep applies the real value.

  ## Examples

      iex> Malachi.Config.retention_bound("MALACHI_RETENTION_MAX_BYTES", nil)
      nil

      iex> Malachi.Config.retention_bound("MALACHI_RETENTION_MAX_BYTES", "10737418240")
      10737418240
  """
  @spec retention_bound(String.t(), String.t() | nil) :: non_neg_integer() | nil
  def retention_bound(var, raw) do
    case integer(var, raw, nil) do
      nil -> nil
      bound when bound in 0..0xFFFF_FFFF_FFFF_FFFF -> bound
      _out_of_range -> raise "#{var} must be a whole number from 0 through 18446744073709551615, got: #{inspect(raw)}"
    end
  end

  @doc """
  How old an active segment may get before the retention sweep rolls it, from
  `MALACHI_SEGMENT_MAX_AGE_MS`: 7 days when the variable is absent or blank, otherwise a whole number of
  milliseconds from the field's floor (`retention.segment_max_age_ms` in `Malachi.Cluster.Policy`, one
  minute) through 2^64 - 1. Anything else stops the node, for the reasons `retention_bound/2` gives and
  one more: below the floor, the sweep that asks for the roll runs too rarely to honor the value, and
  every roll is one more segment in the metadata.

  ## Examples

      iex> Malachi.Config.segment_max_age_ms(nil)
      604_800_000

      iex> Malachi.Config.segment_max_age_ms("3600000")
      3_600_000
  """
  @spec segment_max_age_ms(String.t() | nil) :: pos_integer()
  def segment_max_age_ms(raw) do
    var = "MALACHI_SEGMENT_MAX_AGE_MS"
    %{min: floor} = Policy.field("retention.segment_max_age_ms")

    case integer(var, raw, @default_segment_max_age_ms) do
      age when age in floor..0xFFFF_FFFF_FFFF_FFFF//1 ->
        age

      _out_of_range ->
        raise "#{var} must be a whole number from #{floor} through 18446744073709551615, got: #{inspect(raw)}"
    end
  end

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

  @doc """
  The default users in `MALACHI_DEFAULT_USERS`: entries separated by `;`, each
  `user:password:permissions[:role]`, permissions separated by `,` and possibly empty (an operator with a
  console role and no wire permission, `ops:secret::viewer`). Returns `{username, password, permissions,
  role}` tuples, `role` `nil` when absent. An unknown permission or role, an empty username or password, or
  an entry of the wrong shape stops the node: a typo here would otherwise seed an account that cannot do
  what the operator meant, or one that silently can do more, such as an admin with an empty password. A
  password cannot contain `:` or `;`, the separators. Every error names the entry by its position alone, its
  place among the `;` separated segments with empty ones counted, and prints no field, the username
  included, since a mistyped separator can move part of a password into any of them.

  ## Examples

      iex> Malachi.Config.default_users("admin:s3cret:admin;ops:pw::viewer")
      [{"admin", "s3cret", [:admin], nil}, {"ops", "pw", [], :viewer}]

  """
  @spec default_users(String.t()) :: [{String.t(), String.t(), [atom()], ConsoleRole.t() | nil}]
  def default_users(raw) when is_binary(raw) do
    raw
    |> String.split(";")
    # Numbered before the empty segments are dropped, so the position an error gives is the segment an
    # operator counts between separators, stray ones included.
    |> Enum.with_index(1)
    |> Enum.reject(fn {entry, _position} -> entry == "" end)
    |> Enum.map(&default_user/1)
  end

  defp default_user({entry, position}) do
    {username, password, perms, role} =
      case String.split(entry, ":") do
        [username, password, perms] ->
          {username, password, perms, nil}

        [username, password, perms, role] ->
          {username, password, perms, role}

        fields ->
          raise "MALACHI_DEFAULT_USERS entry #{position} has #{length(fields)} fields; entries must be " <>
                  "user:password:permissions[:role], and a password cannot contain ':' or ';'"
      end

    if username == "" or password == "" do
      raise "MALACHI_DEFAULT_USERS entry #{position} has an empty username or password"
    end

    with {:ok, permissions} <- Auth.parse_permissions(String.split(perms, ",", trim: true)),
         {:ok, role} <- ConsoleRole.parse(role) do
      {username, password, permissions, role}
    else
      :error ->
        raise "MALACHI_DEFAULT_USERS entry #{position} has an unknown permission or role " <>
                "(permissions: admin, produce, consume; roles: viewer, editor, admin)"
    end
  end

  @doc """
  The cluster's identity as `GET /api/v1/me` reports it, from `MALACHI_CLUSTER_DISPLAY_NAME`,
  `MALACHI_CLUSTER_COLOR` and `MALACHI_CLUSTER_ICON`. Each is `nil` when absent or blank. The color is a
  `#RRGGBB` hex value and the icon a short slug the console maps to an icon (`^[a-z0-9-]{1,32}$`);
  anything else stops the node, since a console that cannot render the identity defeats its purpose,
  which is telling clusters apart at a glance. Every node should carry the same values.

  ## Examples

      iex> Malachi.Config.cluster_identity(" prod-eu ", "#A1B2C3", "globe")
      %{name: "prod-eu", color: "#A1B2C3", icon: "globe"}

      iex> Malachi.Config.cluster_identity(nil, "", nil)
      %{name: nil, color: nil, icon: nil}

  """
  @spec cluster_identity(String.t() | nil, String.t() | nil, String.t() | nil) :: %{
          name: String.t() | nil,
          color: String.t() | nil,
          icon: String.t() | nil
        }
  def cluster_identity(name, color, icon) do
    %{
      name: blank_to_nil(name),
      color: matching("MALACHI_CLUSTER_COLOR", blank_to_nil(color), ~r/\A#[0-9A-Fa-f]{6}\z/, "a #RRGGBB color"),
      icon:
        matching("MALACHI_CLUSTER_ICON", blank_to_nil(icon), ~r/\A[a-z0-9-]{1,32}\z/, "a slug of 1 to 32 a-z, 0-9 or -")
    }
  end

  defp blank_to_nil(nil), do: nil

  defp blank_to_nil(raw) do
    case String.trim(raw) do
      "" -> nil
      trimmed -> trimmed
    end
  end

  defp matching(_var, nil, _pattern, _shape), do: nil

  defp matching(var, value, pattern, shape) do
    if value =~ pattern, do: value, else: raise("#{var} must be #{shape}, got: #{inspect(value)}")
  end

  @doc """
  The control plane cluster this node runs, from `MALACHI_LOG_CLUSTER` and the data-plane shard count.

    * A name given: that cluster, whatever the shard count (a clustered node runs one data-plane
      shard, `Malachi.DataPlaneRouter.shard_count/0`).
    * No name and one shard (the default): `:malachi_log`, so a single node runs its metadata as a
      one-member `ra` cluster and keeps it across a restart. Before #273 this was `nil`, in-memory
      metadata that a restart forgot, after which the orphan sweep deleted every segment written
      before the boot.
    * No name and more than one shard: `nil`, the in-memory data-plane sharding measurement mode, whose
      shards hold their metadata in memory only and run no orphan sweep.

  The name comes from a trusted operator (deploy config), so creating the atom is fine.

  ## Examples

      iex> Malachi.Config.log_cluster(nil, 1)
      :malachi_log

      iex> Malachi.Config.log_cluster(" ", 4)
      nil

      iex> Malachi.Config.log_cluster("orders_meta", 4)
      :orders_meta

  """
  @spec log_cluster(String.t() | nil, integer()) :: atom() | nil
  def log_cluster(raw, data_shards) when is_integer(data_shards) do
    case raw && String.trim(raw) do
      blank when blank in [nil, ""] -> if data_shards > 1, do: nil, else: @default_log_cluster
      name -> String.to_atom(name)
    end
  end
end
