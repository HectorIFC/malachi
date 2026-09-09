defmodule Malachi.CLI.OptionsTest do
  @moduledoc """
  The guard that keeps a mistyped option from becoming a command against the wrong cluster. Every
  operator task discarded `OptionParser`'s invalid list, so `--nod other@host` parsed into no `:node`
  at all and the task went on to talk to `$MALACHI_NODE` or the default.
  """
  use ExUnit.Case, async: true

  alias Malachi.CLI.Options

  @switches [node: :string, cookie: :string, to: :integer, show: :boolean]

  test "returns the parsed options and positional arguments when everything is recognised" do
    assert Options.parse(["--node", "malachi@h", "grant", "alice"], @switches) ==
             {:ok, {[node: "malachi@h"], ["grant", "alice"]}}
  end

  test "an empty argv parses to nothing, not to an error" do
    assert Options.parse([], @switches) == {:ok, {[], []}}
  end

  test "an unknown option is refused and named, rather than silently dropped" do
    assert {:error, message} = Options.parse(["--nod", "malachi@other"], @switches)
    assert message =~ "unknown option(s): --nod"
  end

  test "every unrecognised option is named, not just the first" do
    assert {:error, message} = Options.parse(["--nod", "x", "--cooky", "y"], @switches)
    assert message =~ "--nod"
    assert message =~ "--cooky"
  end

  test "a value that does not match the declared type is refused too" do
    # --to is an integer switch; a word cannot be cast, so OptionParser reports it as invalid
    assert {:error, message} = Options.parse(["--to", "sixteen"], @switches)
    assert message =~ "--to"
  end

  test "a recognised boolean switch parses without a value" do
    assert {:ok, {opts, []}} = Options.parse(["--show"], @switches)
    assert opts[:show] == true
  end
end
