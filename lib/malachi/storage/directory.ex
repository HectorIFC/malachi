defmodule Malachi.Storage.Directory do
  @moduledoc """
  Makes a directory's entries durable.

  Creating or renaming a file changes its directory, and an fsync of the file itself does not persist
  that change: a power failure can lose the new name while keeping the content. The directory has to be
  fsynced too. Erlang opens a directory for that with the `:directory` mode (a plain `:file.open/2` on a
  directory answers `:eisdir`), which OTP 28, the release this project builds on, supports.
  """

  @doc """
  Fsyncs `dir`, so every entry created, renamed or removed in it so far survives a power failure.
  Answers the error when the directory cannot be opened or synced.
  """
  @spec sync(Path.t()) :: :ok | {:error, term()}
  def sync(dir) do
    case :file.open(dir, [:directory, :read, :raw]) do
      {:ok, fd} ->
        try do
          :file.sync(fd)
        after
          _ = :file.close(fd)
        end

      {:error, _reason} = error ->
        error
    end
  end
end
