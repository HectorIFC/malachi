defmodule Malachi.SourceFileTest do
  use ExUnit.Case, async: true

  alias Malachi.SourceFile

  @moduletag :tmp_dir

  test "returns the file's text", %{tmp_dir: dir} do
    path = Path.join(dir, "source.json")
    File.write!(path, "{}")

    assert SourceFile.read!(path, "source.json") == "{}"
  end

  test "a missing file raises a Mix.Error that names the label and the reason", %{tmp_dir: dir} do
    error = assert_raise Mix.Error, fn -> SourceFile.read!(Path.join(dir, "absent.json"), "site/absent.json") end

    assert error.message == "site/absent.json: cannot be read (no such file or directory)"
  end
end
