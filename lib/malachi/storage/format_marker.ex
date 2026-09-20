defmodule Malachi.Storage.FormatMarker do
  @moduledoc """
  The data-directory format marker: a small plain-text file at the root of the log data directory
  that records which on-disk format the directory holds, so a binary that cannot read that format
  refuses to start instead of opening it.

  Without it, an older release started on data a newer one wrote does not fail. Recovery reads the
  unknown frame as damage, preserves it, and the node carries on. The marker turns that into a refusal
  before any process opens a segment.

  ## The file

      format=1
      written_by=0.12.0
      requires=0.12.0

  `format` is the level, `written_by` the release that wrote the file, and `requires` the oldest
  release that understands `format`. The last one is recorded by the WRITER because only the writer
  can know it: a binary refusing a marker from its future has no other way to name the release the
  operator should go back to. Unknown keys are ignored, so a later release may add some.

  The parser is strict about everything else. All three keys must be present exactly once and the
  file must end with a newline, so a marker cut short anywhere reads as invalid rather than as a lower
  level: `format=12` truncated to `format=1` must never let a release that only understands 1 start.
  An invalid marker refuses, which is the conservative answer.

  ## The rule

  A binary starts on a directory only when the marker is at or below the highest format it
  understands (`supported_format/0`). No marker means either a fresh directory or one written before
  the marker existed; both get a marker at `baseline_format/0`, whatever this release reads. The
  marker only rises, and it rises when a format-changing feature is switched on through the cluster
  flag, never because a newer binary merely started. `raise_to/3` is that mechanism.

  ## Durability

  Both writes, creating the marker and raising it, go the same way: the content into a temporary file
  written with `:sync`, a rename over the marker, then an fsync of the directory
  (`Malachi.Storage.Directory.sync/1`), because the rename is a change to the directory that the file's
  own fsync does not persist. A crash anywhere along that path leaves either the old marker or the new
  one, never a mix: the rename is atomic, and a torn temporary file is not the marker. Only after the
  directory fsync does the call return `:ok`, which is what lets `raise_to/3` promise the new level is
  durable before the first byte of a new format is written.

  A path that TRUSTS an existing marker fsyncs the directory too, rather than assuming an earlier write
  did: a write whose rename landed and whose fsync failed leaves the marker visible but not durable, and
  both the startup gate and a repeated `raise_to/3` would otherwise take it at face value. The fsync is
  cheap and idempotent; skipping it is what is expensive.
  """

  require Logger

  alias Malachi.I18n
  alias Malachi.Storage.Directory

  @file_name "malachi.format"
  @temp_name "malachi.format.tmp"

  # The level a directory starts at, and the only level the startup gate ever writes. It is the format
  # of every byte written before the marker existed, so it is what an unmarked directory holds whether
  # it is empty or full. A release that raises the on-disk format must NOT raise this: a node joining a
  # cluster whose format-changing flag has not been flipped still writes the baseline, and claiming a
  # higher level here would refuse a rollback that is still free and label old bytes as new ones. Only
  # `raise_to/3`, called when the flag flips, moves a directory above it.
  @baseline_format 1
  # The highest level this release can read.
  @supported_format 1

  # The oldest release that understands each format. The bridge release is the first that reads the
  # marker at all, so it is the floor for format 1 as far as the marker can express one.
  @first_release %{1 => "0.12.0"}

  # sysexits EX_CONFIG: the node cannot run with what it was given. A distinct status so a crash loop
  # under a restart policy is recognizable, and so a service manager can be told not to restart on it.
  @exit_status 78

  @required_keys ["format", "written_by", "requires"]

  for format <- @baseline_format..@supported_format do
    unless Map.has_key?(@first_release, format),
      do: raise(CompileError, description: "no first release recorded for format #{format}")
  end

  @typedoc "A parsed marker."
  @type marker :: %{format: pos_integer(), written_by: String.t(), requires: String.t()}

  @typedoc "Why a marker could not be parsed."
  @type parse_error ::
          :missing_newline
          | :malformed_line
          | {:duplicate_key, String.t()}
          | {:missing_key, String.t()}
          | {:bad_format, String.t()}

  @typedoc "What was found where the marker should be."
  @type observation :: {:absent, :fresh | :existing} | {:ok, marker()} | {:error, parse_error()}

  @typedoc "Why a directory is refused, with the marker path for the message."
  @type refusal ::
          {:too_new, marker(), pos_integer(), Path.t()}
          | {:invalid, parse_error(), Path.t()}
          | {:io, atom(), Path.t()}

  @doc "The marker's path under `dir`."
  @spec path(Path.t()) :: Path.t()
  def path(dir), do: Path.join(dir, @file_name)

  @doc "The format level a directory without a marker starts at."
  @spec baseline_format() :: pos_integer()
  def baseline_format, do: @baseline_format

  @doc "The highest format level this release can read."
  @spec supported_format() :: pos_integer()
  def supported_format, do: @supported_format

  @doc "The exit status of a refused start (78, EX_CONFIG)."
  @spec exit_status() :: non_neg_integer()
  def exit_status, do: @exit_status

  @doc """
  Renders `marker` as the file's content.

  ## Examples

      iex> Malachi.Storage.FormatMarker.render(%{format: 1, written_by: "0.12.0", requires: "0.12.0"})
      "format=1\\nwritten_by=0.12.0\\nrequires=0.12.0\\n"
  """
  @spec render(marker()) :: String.t()
  def render(%{format: format, written_by: written_by, requires: requires}) do
    "format=#{format}\nwritten_by=#{written_by}\nrequires=#{requires}\n"
  end

  @doc """
  Parses the file's content. Never raises: anything that is not a complete marker is an error.

  ## Examples

      iex> Malachi.Storage.FormatMarker.parse("format=2\\nwritten_by=0.13.0\\nrequires=0.13.0\\n")
      {:ok, %{format: 2, written_by: "0.13.0", requires: "0.13.0"}}

      iex> Malachi.Storage.FormatMarker.parse("format=2\\nwritten_by=0.13.0\\nrequires=0.1")
      {:error, :missing_newline}
  """
  @spec parse(binary()) :: {:ok, marker()} | {:error, parse_error()}
  def parse(content) when is_binary(content) do
    with :ok <- terminated(content),
         {:ok, pairs} <- pairs(content),
         :ok <- required(pairs),
         {:ok, format} <- format_level(Map.fetch!(pairs, "format")) do
      {:ok, %{format: format, written_by: Map.fetch!(pairs, "written_by"), requires: Map.fetch!(pairs, "requires")}}
    end
  end

  defp terminated(content) do
    if String.ends_with?(content, "\n"), do: :ok, else: {:error, :missing_newline}
  end

  defp pairs(content) do
    content
    |> String.split("\n")
    |> Enum.map(&String.trim/1)
    |> Enum.reject(&(&1 == ""))
    |> Enum.reduce_while({:ok, %{}}, fn line, {:ok, acc} ->
      case String.split(line, "=", parts: 2) do
        [key, value] ->
          key = String.trim(key)

          if Map.has_key?(acc, key),
            do: {:halt, {:error, {:duplicate_key, key}}},
            else: {:cont, {:ok, Map.put(acc, key, String.trim(value))}}

        [_no_equals] ->
          {:halt, {:error, :malformed_line}}
      end
    end)
  end

  defp required(pairs) do
    case Enum.find(@required_keys, &(not Map.has_key?(pairs, &1))) do
      nil -> :ok
      key -> {:error, {:missing_key, key}}
    end
  end

  defp format_level(value) do
    case Integer.parse(value) do
      {level, ""} when level >= 1 -> {:ok, level}
      _other -> {:error, {:bad_format, value}}
    end
  end

  @doc """
  What to do about `observation` for a binary that reads up to `supported`. Pure.

    * `{:write, level, kind}` - no marker: write one at `baseline_format/0` (`kind` says whether the
      directory held data already, for the log line, and never changes the level).
    * `:ok` - the marker is readable by this binary.
    * `{:refuse, reason}` - the marker is from a newer format, or is not a marker.
  """
  @spec decide(observation(), pos_integer()) ::
          :ok
          | {:write, pos_integer(), :fresh | :existing}
          | {:refuse, {:too_new, marker(), pos_integer()} | {:invalid, parse_error()}}
  def decide({:absent, kind}, _supported), do: {:write, @baseline_format, kind}
  def decide({:ok, %{format: format}}, supported) when format <= supported, do: :ok
  def decide({:ok, marker}, supported), do: {:refuse, {:too_new, marker, supported}}
  def decide({:error, reason}, _supported), do: {:refuse, {:invalid, reason}}

  @doc """
  Reads the marker in `dir`. A missing marker is reported with whether the directory already holds
  anything else (segments, shard subdirectories), which only changes the log line.
  """
  @spec read(Path.t()) :: {:ok, observation()} | {:error, {:io, atom()}}
  def read(dir) do
    case File.read(path(dir)) do
      {:ok, content} -> {:ok, parse(content)}
      {:error, :enoent} -> with {:ok, kind} <- data_state(dir), do: {:ok, {:absent, kind}}
      {:error, posix} -> {:error, {:io, posix}}
    end
  end

  # Anything at the root other than the marker and its temporary file counts as data. A missing
  # directory is a fresh one.
  defp data_state(dir) do
    case File.ls(dir) do
      {:ok, entries} ->
        {:ok, if(Enum.any?(entries, &(&1 not in [@file_name, @temp_name])), do: :existing, else: :fresh)}

      {:error, :enoent} ->
        {:ok, :fresh}

      {:error, posix} ->
        {:error, {:io, posix}}
    end
  end

  @doc """
  Creates the marker in `dir` at `format`, written by release `version`, durably (see the moduledoc).
  A leftover temporary file from an earlier crash is overwritten.
  """
  @spec write(Path.t(), pos_integer(), String.t()) :: :ok | {:error, {:io, atom()}}
  def write(dir, format, version \\ release_version()) do
    with :ok <- mkdir(dir) do
      replace(dir, render(marker(format, version)))
    end
  end

  defp mkdir(dir) do
    case File.mkdir_p(dir) do
      :ok -> :ok
      {:error, posix} -> {:error, {:io, posix}}
    end
  end

  defp sync_directory(dir) do
    case Directory.sync(dir) do
      :ok -> :ok
      {:error, posix} -> {:error, {:io, posix}}
    end
  end

  # The one way the marker file changes: temporary file with `:sync`, rename over the marker, fsync of
  # the directory. Nothing before the directory fsync counts as written.
  defp replace(dir, content) do
    temp = Path.join(dir, @temp_name)

    with :ok <- File.write(temp, content, [:sync]),
         :ok <- File.rename(temp, path(dir)) do
      sync_directory(dir)
    else
      {:error, posix} -> {:error, {:io, posix}}
    end
  end

  @doc """
  Raises the marker in `dir` to `format`, durably (see the moduledoc). The marker only rises: the same
  level is a no-op, a lower one is refused, and so is a level above what this binary reads, since the
  node would then refuse its own next start. There must already be a marker: `enforce/2` writes one on
  every start, so a directory without one is a directory the startup gate never ran on.

  Must return `:ok` BEFORE the first byte of the new format is written.

  Options (for tests, since this release reads only format 1 and so has nothing to raise to):
  `:supported` (default `supported_format/0`), `:requires` (default the recorded first release of
  `format`) and `:version` (default this release's version).
  """
  @spec raise_to(Path.t(), pos_integer(), keyword()) ::
          :ok
          | {:error,
             :no_marker
             | {:lower, pos_integer(), pos_integer()}
             | {:unsupported, pos_integer()}
             | {:no_first_release, pos_integer()}
             | {:invalid, parse_error()}
             | {:io, atom()}}
  def raise_to(dir, format, opts \\ []) do
    supported = Keyword.get(opts, :supported, @supported_format)

    if format > supported do
      {:error, {:unsupported, format}}
    else
      raise_within(dir, format, opts)
    end
  end

  defp raise_within(dir, format, opts) do
    case read(dir) do
      # Already at the level asked for, which includes the retry of a raise whose rename landed and
      # whose directory fsync did not: the marker is visible and may still be lost. The fsync is
      # repeated rather than assumed, because the caller takes this `:ok` as permission to write the
      # first byte of the new format.
      {:ok, {:ok, %{format: ^format}}} ->
        sync_directory(dir)

      {:ok, {:ok, %{format: current}}} when current > format ->
        {:error, {:lower, current, format}}

      {:ok, {:ok, _lower}} ->
        raise_over(dir, format, opts)

      {:ok, {:absent, _kind}} ->
        {:error, :no_marker}

      {:ok, {:error, reason}} ->
        {:error, {:invalid, reason}}

      {:error, _io} = error ->
        error
    end
  end

  # A level with no recorded first release cannot be written: the marker would have no `requires` to
  # name, and an older binary refusing it could not tell the operator where to go.
  defp raise_over(dir, format, opts) do
    case first_release(format, opts) do
      {:ok, requires} ->
        marker = %{format: format, written_by: Keyword.get(opts, :version, release_version()), requires: requires}
        replace(dir, render(marker))

      :error ->
        {:error, {:no_first_release, format}}
    end
  end

  defp first_release(format, opts) do
    case Keyword.fetch(opts, :requires) do
      {:ok, _requires} = given -> given
      :error -> Map.fetch(@first_release, format)
    end
  end

  defp marker(format, version),
    do: %{format: format, written_by: version, requires: Map.fetch!(@first_release, format)}

  @doc """
  The startup gate: reads the marker in `dir`, writes one when there is none, and answers whether the
  node may start. Options (for tests): `:supported` (default `supported_format/0`) and `:version`
  (default this release's version).
  """
  @spec enforce(Path.t(), keyword()) :: :ok | {:refuse, refusal()}
  def enforce(dir, opts \\ []) do
    supported = Keyword.get(opts, :supported, @supported_format)
    version = Keyword.get(opts, :version, release_version())
    file = path(dir)

    with {:ok, observation} <- read(dir),
         :ok <- act(decide(observation, supported), dir, version) do
      :ok
    else
      {:refuse, {:too_new, marker, supported}} -> {:refuse, {:too_new, marker, supported, file}}
      {:refuse, {:invalid, reason}} -> {:refuse, {:invalid, reason, file}}
      {:error, {:io, posix}} -> {:refuse, {:io, posix, file}}
    end
  end

  # An existing marker this binary can read: accepted, but its directory entry is fsynced first, for
  # the same reason the raise path repeats it. A marker left visible by a write whose fsync failed
  # would otherwise be trusted for the rest of the node's life without ever being made durable.
  defp act(:ok, dir, _version), do: sync_directory(dir)
  defp act({:refuse, _reason} = refusal, _dir, _version), do: refusal

  defp act({:write, format, kind}, dir, version) do
    with :ok <- write(dir, format, version) do
      log_created(kind, format, path(dir))
    end
  end

  defp log_created(:fresh, format, file),
    do: Logger.info(I18n.t(:data_format_marker_created_fresh, format: format, path: file))

  defp log_created(:existing, format, file),
    do: Logger.info(I18n.t(:data_format_marker_created_existing, format: format, path: file))

  @doc """
  Refuses the start: logs one line through I18n, prints the same line on stderr (a container log
  shows it even when the logger has not flushed), and halts with `exit_status/0` through `halt_fun`.
  """
  @spec refuse!(refusal(), (non_neg_integer() -> any())) :: any()
  def refuse!(reason, halt_fun \\ &System.halt/1) do
    detail = refusal_detail(reason)
    Logger.error(I18n.t(:data_format_refused, detail: detail))
    IO.puts(:stderr, I18n.t(:data_format_refused, detail: detail))
    halt_fun.(@exit_status)
  end

  defp refusal_detail({:too_new, marker, supported, file}) do
    I18n.t(:data_format_too_new,
      format: marker.format,
      supported: supported,
      requires: marker.requires,
      written_by: marker.written_by,
      path: file
    )
  end

  defp refusal_detail({:invalid, reason, file}),
    do: I18n.t(:data_format_marker_invalid, reason: inspect(reason), path: file)

  defp refusal_detail({:io, posix, file}),
    do: I18n.t(:data_format_marker_io_failed, reason: inspect(posix), path: file)

  defp release_version, do: :malachi |> Application.spec(:vsn) |> to_string()
end
