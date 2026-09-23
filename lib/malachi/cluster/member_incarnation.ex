defmodule Malachi.Cluster.MemberIncarnation do
  @moduledoc """
  The incarnation a node resumes at after a restart, kept on disk so a restarted member outranks
  everything its peers still remember about the member it was.

  ## The problem this solves

  A member owns its incarnation, and SWIM's merge is a join on `{incarnation, rank}` that ignores an
  equal or lower pair. A node that restarts from scratch therefore announces itself **below** whatever
  its peers retained, and every peer ignores the announcement and keeps the old record: the old status
  and, which is what makes this more than a curiosity, the old **attributes**.

  So a node that comes back on a build advertising fewer capabilities is still remembered as
  advertising the old set (`Malachi.Cluster.Capabilities`), and a cluster flag can be switched on over
  a node that cannot honour it. The same staleness silently feeds rack-aware placement, which reads the
  same attributes.

  An incarnation rises only on a refutation or a deliberate attribute change, so it cannot be derived
  from anything the node already keeps. It has to be remembered.

  ## How

  A single decimal number in `malachi.incarnation`, at the root of the log data directory beside
  `malachi.format`, written the same durable way (`Malachi.Storage.FormatMarker`): a temporary file with
  `:sync`, a rename over the target, then an fsync of the directory, because a rename is a change to the
  directory that the file's own fsync does not persist.

  The number is a **ceiling**, not the current value. `reserve/2` starts the node one above the recorded
  ceiling and immediately records a ceiling a whole block higher, so the node can raise its incarnation
  that many times without touching the disk again. Only crossing the block writes, through `extend/3`.
  That keeps the write off the refutation path, which is where an fsync would sit in the way of the
  failure detector.

  A missing, empty or unparsable file reads as ceiling 0. That is the first boot of a node, and it is
  also the safe reading of a damaged one: starting low costs a node one round of being ignored by peers
  that remember more, which the next refutation corrects, while starting high on a guess would let this
  node override records it has no right to.
  """

  alias Malachi.Storage.Directory

  @file_name "malachi.incarnation"
  @temp_name "malachi.incarnation.tmp"

  # How many times a node can raise its own incarnation before it has to write again. An incarnation
  # rises only on a refutation or an attribute change, so a thousand is a long life for one boot, and
  # the cost of reserving too many is nothing: the numbers are unbounded and only their order matters.
  @block 1_024

  @typedoc "The value this node starts at, and the ceiling it may reach before it must write again."
  @type reservation :: %{start: pos_integer(), ceiling: pos_integer()}

  @doc "The path the ceiling is kept at, inside `dir`."
  @spec path(Path.t()) :: Path.t()
  def path(dir), do: Path.join(dir, @file_name)

  @doc "The size of the block `reserve/2` hands out."
  @spec block() :: pos_integer()
  def block, do: @block

  @doc """
  Claims the next block of incarnations for this node in `dir`.

  Answers `{:ok, %{start: start, ceiling: ceiling}}` once the new ceiling is durably recorded, so a
  crash immediately after cannot hand the same numbers out twice. `{:error, reason}` when the file
  cannot be written, which the caller decides what to do about: this module never guesses.
  """
  @spec reserve(Path.t(), pos_integer()) :: {:ok, reservation()} | {:error, term()}
  def reserve(dir, block \\ @block) do
    recorded = read(dir)
    ceiling = recorded + block

    case write(dir, ceiling) do
      :ok -> {:ok, %{start: recorded + 1, ceiling: ceiling}}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Records a ceiling a block above `incarnation`, for a node that has used up the block it reserved.

  Answers the new ceiling, or `{:error, reason}`. The caller keeps serving either way: a ceiling that
  could not be written costs a future restart one round of being ignored, which is strictly less bad
  than a membership server that stops to retry a disk.
  """
  @spec extend(Path.t(), pos_integer(), pos_integer()) :: {:ok, pos_integer()} | {:error, term()}
  def extend(dir, incarnation, block \\ @block) do
    ceiling = incarnation + block

    case write(dir, ceiling) do
      :ok -> {:ok, ceiling}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  The ceiling recorded in `dir`, or 0 when there is none to read.

  Missing, empty, unparsable or unreadable all answer 0: see the moduledoc for why starting low is the
  safe direction.
  """
  @spec read(Path.t()) :: non_neg_integer()
  def read(dir) do
    with {:ok, content} <- File.read(path(dir)),
         {value, rest} when value >= 0 <- Integer.parse(String.trim(content)),
         true <- String.trim(rest) == "" do
      value
    else
      _missing_or_damaged -> 0
    end
  end

  # The one way the file changes, and the same path `Malachi.Storage.FormatMarker` uses for the format
  # marker: nothing before the directory fsync counts as written.
  defp write(dir, ceiling) do
    temp = Path.join(dir, @temp_name)

    with :ok <- File.write(temp, "#{ceiling}\n", [:sync]),
         :ok <- File.rename(temp, path(dir)) do
      Directory.sync(dir)
    end
  end
end
