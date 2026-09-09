defmodule Malachi.CLI.Options do
  @moduledoc """
  Argument parsing shared by the operator mix tasks (`mix malachi.ring`, `mix malachi.reshard`,
  `mix malachi.user`, `mix malachi.acl`).

  `OptionParser.parse/2` reports an unknown option in its third element and simply leaves it out of
  the parsed options. Every one of these tasks used to discard that element, so `--nod malachi@other`
  parsed cleanly into no `:node` at all and the task went on to talk to `$MALACHI_NODE` or the default
  as though that had been asked for. For a mistyped `--node` in particular that is the worst possible
  recovery: the command succeeds against a **different cluster** and reports the answer as yours.

  Kept free of `Mix.*` for the same reason as `Malachi.CLI.Rpc`: it returns `{:error, message}` rather
  than raising, so the tasks own their own output and exit behavior and this stays testable without a
  Mix shell.
  """

  @doc """
  Parses `argv` against `switches` (strict), returning `{:ok, {opts, args}}` or an `{:error, message}`
  naming the options that were not recognized. Positional arguments are returned untouched: whether a
  task accepts any is the task's own business.
  """
  @spec parse([String.t()], keyword()) :: {:ok, {keyword(), [String.t()]}} | {:error, String.t()}
  def parse(argv, switches) do
    case OptionParser.parse(argv, strict: switches) do
      {opts, args, []} -> {:ok, {opts, args}}
      {_opts, _args, invalid} -> {:error, "unknown option(s): " <> names(invalid)}
    end
  end

  # `invalid` entries are `{name, value}`, where the value is nil for a flag the switches do not list
  # and the raw string when the value could not be cast to the declared type. The name is what the
  # operator typed and the only part worth echoing back.
  defp names(invalid), do: Enum.map_join(invalid, ", ", fn {name, _value} -> name end)
end
