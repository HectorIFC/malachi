defmodule Malachi.SourceFile do
  @moduledoc """
  Reads a source file that a Mix task builds from, and stops with a message naming it when it cannot be read.

  The design token task and the landing page staging both read a source this way, so an unreadable source is
  reported the same way in both.
  """

  @doc "The file's text. Raises a `Mix.Error` that names `label` and the reason when the file cannot be read."
  @spec read!(Path.t(), String.t()) :: String.t()
  def read!(path, label) do
    case File.read(path) do
      {:ok, text} -> text
      {:error, reason} -> unreadable!(label, reason)
    end
  end

  @doc "Raises a `Mix.Error` saying that `label` cannot be read, with the reason. The one wording for an unreadable file."
  @spec unreadable!(String.t(), term()) :: no_return()
  def unreadable!(label, reason), do: Mix.raise("#{label}: cannot be read (#{:file.format_error(reason)})")
end
