defmodule Malachi.Test.TmpDir do
  @moduledoc """
  Paths for a test's scratch files and directories that no other test run on this host can produce.

  `System.unique_integer/1` is unique within one VM only, and every run starts from the same small
  values, so two `mix test` runs on one host (two worktrees) that named a directory
  `System.tmp_dir!()` plus that integer could pick the same one: one run's cleanup would then delete the
  files the other is using. Adding the operating system pid, which no other live process on the host
  holds, makes the name unique per host, as `Malachi.Test.Distribution` does for node names.

  Only the name is made unique; nothing is created, so a caller that wants the directory creates it
  (`File.mkdir_p!/1`) and removes it (`on_exit`) as before.
  """

  @doc """
  A path under `base` (the system temporary directory unless given) named `<prefix>_<os pid>_<integer>`,
  distinct from every other path this function returns on this host while this VM lives.
  """
  @spec path(String.t(), Path.t()) :: Path.t()
  def path(prefix, base \\ System.tmp_dir!()) do
    Path.join(base, "#{prefix}_#{:os.getpid()}_#{System.unique_integer([:positive])}")
  end
end
