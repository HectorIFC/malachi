defmodule Malachi.StartupRefusal do
  @moduledoc """
  The one surface a boot precondition uses to refuse a start: one line through `Malachi.I18n`, the
  same line on stderr, and a halt with a distinct exit status.

  Two gates need it. `Malachi.Storage.FormatMarker` refuses a data directory written in a format this
  binary cannot read (#189), and `Malachi.Cluster.ClusterFlags` refuses a node whose capability set
  lacks a flag the cluster has already enabled (#193). Both say the same three things in the same
  order, so they say them here.

  The line goes to the logger **and** to stderr because a container log shows the second even when the
  logger has not flushed, which is the only output an operator gets from a node that halts during boot.

  `halt_fun` is the seam: production passes `System.halt/1`, a test passes a function that records the
  status instead of taking the VM down with it.
  """

  require Logger

  alias Malachi.I18n

  # sysexits EX_CONFIG: the node cannot run with what it was given. A distinct status so a crash loop
  # under a restart policy is recognizable, and so a service manager can be told not to restart on it.
  @exit_status 78

  @doc "The exit status of a refused start (78, EX_CONFIG)."
  @spec exit_status() :: non_neg_integer()
  def exit_status, do: @exit_status

  @doc """
  Refuses the start: logs `detail` inside the standard envelope, prints the same line on stderr, and
  halts with `exit_status/0` through `halt_fun`.

  `detail` is already a rendered I18n string, because only the caller knows which key describes its
  own precondition. The envelope around it is the same for every gate, which is what makes a refused
  start recognizable by its first words whatever refused it.
  """
  @spec refuse!(String.t(), (non_neg_integer() -> any())) :: any()
  def refuse!(detail, halt_fun \\ &System.halt/1) do
    Logger.error(I18n.t(:startup_refused, detail: detail))
    IO.puts(:stderr, I18n.t(:startup_refused, detail: detail))
    halt_fun.(@exit_status)
  end
end
