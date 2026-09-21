defmodule Malachi.Storage.FormatMarkerTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  import ExUnit.CaptureIO
  import ExUnit.CaptureLog

  require Logger

  alias Malachi.Storage.FormatMarker

  doctest FormatMarker

  @moduletag :tmp_dir

  @v1 %{format: 1, written_by: "0.12.0", requires: "0.12.0"}
  @v2 %{format: 2, written_by: "0.13.0", requires: "0.13.0"}

  defp put_marker(dir, content), do: File.write!(FormatMarker.path(dir), content)

  # The refusal line naming `path`, picked out of whatever else shared the captured device.
  defp refusal_line(output, path) do
    output
    |> String.split("\n", trim: true)
    |> Enum.find(&(String.contains?(&1, "REFUSING TO START (exit 78):") and String.contains?(&1, path)))
  end

  test "the levels and the exit status this release is built with" do
    assert FormatMarker.baseline_format() == 1
    assert FormatMarker.supported_format() == 1
    assert FormatMarker.exit_status() == 78
  end

  describe "decide/2" do
    test "no marker writes the current format, whether or not the directory held data" do
      assert FormatMarker.decide({:absent, :fresh}, 1) == {:write, 1, :fresh}
      assert FormatMarker.decide({:absent, :existing}, 1) == {:write, 1, :existing}
    end

    test "a marker below or at the supported format starts" do
      assert FormatMarker.decide({:ok, @v1}, 2) == :ok
      assert FormatMarker.decide({:ok, @v2}, 2) == :ok
    end

    test "a marker above the supported format refuses, carrying the marker" do
      assert FormatMarker.decide({:ok, @v2}, 1) == {:refuse, {:too_new, @v2, 1}}
    end

    test "an unparseable marker refuses" do
      assert FormatMarker.decide({:error, :missing_newline}, 1) == {:refuse, {:invalid, :missing_newline}}
    end
  end

  describe "parse/1" do
    test "reads a rendered marker back" do
      assert FormatMarker.parse(FormatMarker.render(@v2)) == {:ok, @v2}
    end

    test "ignores unknown keys, blank lines, surrounding spaces and CRLF" do
      content = "format = 2\r\n\nwritten_by=0.13.0\r\nfuture_key=anything\nrequires= 0.13.0 \n"
      assert FormatMarker.parse(content) == {:ok, @v2}
    end

    test "refuses what is not a complete marker" do
      assert FormatMarker.parse("") == {:error, :missing_newline}
      assert FormatMarker.parse("format=1\nwritten_by=a\nrequires=a") == {:error, :missing_newline}

      assert FormatMarker.parse("format=1\nformat=2\nwritten_by=a\nrequires=a\n") ==
               {:error, {:duplicate_key, "format"}}

      assert FormatMarker.parse("written_by=a\nrequires=a\n") == {:error, {:missing_key, "format"}}
      assert FormatMarker.parse("format=1\nrequires=a\n") == {:error, {:missing_key, "written_by"}}
      assert FormatMarker.parse("format=1\nwritten_by=a\n") == {:error, {:missing_key, "requires"}}
      assert FormatMarker.parse("format=0\nwritten_by=a\nrequires=a\n") == {:error, {:bad_format, "0"}}
      assert FormatMarker.parse("format=abc\nwritten_by=a\nrequires=a\n") == {:error, {:bad_format, "abc"}}
      assert FormatMarker.parse("format=2x\nwritten_by=a\nrequires=a\n") == {:error, {:bad_format, "2x"}}
      assert FormatMarker.parse("format=1\nwritten_by\nrequires=a\n") == {:error, :malformed_line}
    end

    property "render then parse is the identity" do
      check all(
              format <- positive_integer(),
              written_by <- string(:alphanumeric, min_length: 1),
              requires <- string(:alphanumeric, min_length: 1)
            ) do
        marker = %{format: format, written_by: written_by, requires: requires}
        assert FormatMarker.parse(FormatMarker.render(marker)) == {:ok, marker}
      end
    end

    property "never raises on arbitrary bytes" do
      check all(content <- binary()) do
        assert match?({:ok, _}, FormatMarker.parse(content)) or match?({:error, _}, FormatMarker.parse(content))
      end
    end

    property "every strict prefix of a marker is refused, so a truncation never reads as a lower level" do
      check all(
              format <- integer(1..10_000),
              written_by <- string(:alphanumeric, min_length: 1)
            ) do
        content = FormatMarker.render(%{format: format, written_by: written_by, requires: written_by})

        for cut <- 0..(byte_size(content) - 1) do
          assert {:error, _reason} = FormatMarker.parse(binary_part(content, 0, cut))
        end
      end
    end
  end

  describe "enforce/2" do
    test "a fresh directory, even one that does not exist yet, gets a marker at the current format", %{tmp_dir: tmp} do
      dir = Path.join(tmp, "not_yet")

      log = capture_log(fn -> assert FormatMarker.enforce(dir, version: "9.9.9") == :ok end)

      assert {:ok, {:ok, %{format: 1, written_by: "9.9.9", requires: "0.12.0"}}} = FormatMarker.read(dir)
      assert log =~ "fresh directory"
    end

    test "a directory written before the marker existed is treated as format 1", %{tmp_dir: dir} do
      File.mkdir_p!(Path.join(dir, "shard_0"))

      log = capture_log(fn -> assert FormatMarker.enforce(dir) == :ok end)

      assert {:ok, {:ok, %{format: 1}}} = FormatMarker.read(dir)
      assert log =~ "written before the marker existed"
    end

    test "a leftover temporary file is not data and is overwritten", %{tmp_dir: dir} do
      File.write!(Path.join(dir, "malachi.format.tmp"), "format=9\n")

      assert {:ok, {:absent, :fresh}} = FormatMarker.read(dir)
      capture_log(fn -> assert FormatMarker.enforce(dir) == :ok end)

      assert {:ok, {:ok, %{format: 1}}} = FormatMarker.read(dir)
      refute File.exists?(Path.join(dir, "malachi.format.tmp"))
    end

    test "starting again leaves the marker byte for byte unchanged", %{tmp_dir: dir} do
      capture_log(fn -> assert FormatMarker.enforce(dir, version: "0.12.0") == :ok end)
      before = File.read!(FormatMarker.path(dir))

      assert FormatMarker.enforce(dir, version: "0.13.0") == :ok
      assert File.read!(FormatMarker.path(dir)) == before
    end

    test "a marker above the supported format refuses without touching it", %{tmp_dir: dir} do
      content = FormatMarker.render(@v2)
      put_marker(dir, content)

      assert FormatMarker.enforce(dir) == {:refuse, {:too_new, @v2, 1, FormatMarker.path(dir)}}
      assert File.read!(FormatMarker.path(dir)) == content
    end

    test "a marker at or below the supported format starts", %{tmp_dir: dir} do
      put_marker(dir, FormatMarker.render(@v1))
      assert FormatMarker.enforce(dir, supported: 2) == :ok
    end

    test "a marker truncated at any byte refuses", %{tmp_dir: dir} do
      content = FormatMarker.render(@v1)

      for cut <- 0..(byte_size(content) - 1) do
        put_marker(dir, binary_part(content, 0, cut))
        assert {:refuse, {:invalid, _reason, _path}} = FormatMarker.enforce(dir)
      end
    end

    test "a marker path that is a directory refuses as an IO failure", %{tmp_dir: dir} do
      File.mkdir_p!(FormatMarker.path(dir))
      assert {:refuse, {:io, :eisdir, _path}} = FormatMarker.enforce(dir)
    end

    test "a directory the node cannot write refuses as an IO failure", %{tmp_dir: dir} do
      File.mkdir_p!(Path.join(dir, "shard_0"))
      File.chmod!(dir, 0o500)
      on_exit(fn -> File.chmod(dir, 0o700) end)

      # Root ignores the permission bits, so the refusal can only be observed as a regular user.
      if File.write(Path.join(dir, "probe"), "") == {:error, :eacces} do
        assert {:refuse, {:io, :eacces, _path}} = FormatMarker.enforce(dir)
      end
    end

    test "a data directory that cannot be created refuses as an IO failure", %{tmp_dir: tmp} do
      # The parent exists and cannot be written: reading the marker and listing the directory both find
      # nothing, so the failure comes from creating it.
      File.chmod!(tmp, 0o500)
      on_exit(fn -> File.chmod(tmp, 0o700) end)

      # Root ignores the permission bits, so the refusal can only be observed as a regular user.
      if File.mkdir(Path.join(tmp, "probe")) == {:error, :eacces} do
        assert {:refuse, {:io, :eacces, _path}} = FormatMarker.enforce(Path.join(tmp, "log"))
      end
    end

    test "a data directory that cannot be listed refuses as an IO failure", %{tmp_dir: dir} do
      File.chmod!(dir, 0o300)
      on_exit(fn -> File.chmod(dir, 0o700) end)

      if File.ls(dir) == {:error, :eacces} do
        assert {:refuse, {:io, :eacces, _path}} = FormatMarker.enforce(dir)
      end
    end
  end

  describe "raise_to/3" do
    setup %{tmp_dir: dir} do
      :ok = FormatMarker.write(dir, 1, "0.12.0")
      :ok
    end

    test "the same level is a no-op", %{tmp_dir: dir} do
      before = File.read!(FormatMarker.path(dir))
      assert FormatMarker.raise_to(dir, 1) == :ok
      assert File.read!(FormatMarker.path(dir)) == before
    end

    test "a level this binary cannot read is refused", %{tmp_dir: dir} do
      assert FormatMarker.raise_to(dir, FormatMarker.supported_format() + 1) ==
               {:error, {:unsupported, FormatMarker.supported_format() + 1}}
    end

    test "there must already be a marker", %{tmp_dir: tmp} do
      assert FormatMarker.raise_to(Path.join(tmp, "empty"), 1) == {:error, :no_marker}
    end

    test "an invalid marker is not raised over", %{tmp_dir: dir} do
      put_marker(dir, "format=1\n")
      assert FormatMarker.raise_to(dir, 1) == {:error, {:invalid, {:missing_key, "written_by"}}}
    end

    test "a marker that cannot be read is reported", %{tmp_dir: tmp} do
      dir = Path.join(tmp, "marker_is_a_dir")
      File.mkdir_p!(FormatMarker.path(dir))
      assert FormatMarker.raise_to(dir, 1) == {:error, {:io, :eisdir}}
    end
  end

  describe "raising above the current format" do
    # This release reads only format 1, so there is nothing real to raise to yet; `:supported` and
    # `:requires` stand in for the release that will (#207), exactly as it will call this.
    @raise_opts [supported: 2, requires: "0.13.0", version: "0.13.0"]

    test "overwrites the marker in place with the new level", %{tmp_dir: dir} do
      :ok = FormatMarker.write(dir, 1, "0.12.0")

      assert FormatMarker.raise_to(dir, 2, @raise_opts) == :ok
      assert File.read!(FormatMarker.path(dir)) == FormatMarker.render(@v2)
      assert FormatMarker.enforce(dir) == {:refuse, {:too_new, @v2, 1, FormatMarker.path(dir)}}
    end

    test "keeps no stale bytes when the new content is shorter", %{tmp_dir: dir} do
      put_marker(dir, "format=1\nwritten_by=an-old-and-much-longer-version-string\nrequires=0.12.0\n")

      assert FormatMarker.raise_to(dir, 2, @raise_opts) == :ok
      assert File.read!(FormatMarker.path(dir)) == FormatMarker.render(@v2)
    end

    test "a level with no recorded first release is not written", %{tmp_dir: dir} do
      :ok = FormatMarker.write(dir, 1, "0.12.0")
      before = File.read!(FormatMarker.path(dir))

      assert FormatMarker.raise_to(dir, 2, supported: 2) == {:error, {:no_first_release, 2}}
      assert File.read!(FormatMarker.path(dir)) == before
    end

    test "never lowers", %{tmp_dir: dir} do
      put_marker(dir, FormatMarker.render(@v2))
      assert FormatMarker.raise_to(dir, 1, @raise_opts) == {:error, {:lower, 2, 1}}
      assert File.read!(FormatMarker.path(dir)) == FormatMarker.render(@v2)
    end

    test "a directory the node cannot write is reported, and the marker is left as it was", %{tmp_dir: dir} do
      :ok = FormatMarker.write(dir, 1, "0.12.0")
      before = File.read!(FormatMarker.path(dir))
      File.chmod!(dir, 0o500)
      on_exit(fn -> File.chmod(dir, 0o700) end)

      # Root ignores the permission bits, so the refusal can only be observed as a regular user.
      if File.write(Path.join(dir, "probe"), "") == {:error, :eacces} do
        assert FormatMarker.raise_to(dir, 2, @raise_opts) == {:error, {:io, :eacces}}
        assert File.read!(FormatMarker.path(dir)) == before
      end
    end

    test "a leftover temporary file from a crashed raise does not stop the next one", %{tmp_dir: dir} do
      :ok = FormatMarker.write(dir, 1, "0.12.0")
      File.write!(Path.join(dir, "malachi.format.tmp"), "format=2\nwrit")

      assert FormatMarker.raise_to(dir, 2, @raise_opts) == :ok
      assert File.read!(FormatMarker.path(dir)) == FormatMarker.render(@v2)
      refute File.exists?(Path.join(dir, "malachi.format.tmp"))
    end
  end

  describe "refuse!/2" do
    for {name, reason} <- [
          too_new: {:too_new, @v2, 1, "/data/malachi.format"},
          invalid: {:invalid, :missing_newline, "/data/malachi.format"},
          io: {:io, :eacces, "/data/malachi.format"}
        ] do
      test "halts with 78 and prints the same line to the log and to stderr (#{name})" do
        reason = unquote(Macro.escape(reason))
        parent = self()

        stderr =
          capture_io(:stderr, fn ->
            log = capture_log(fn -> FormatMarker.refuse!(reason, &send(parent, {:halted, &1})) end)
            send(parent, {:log, log})
          end)

        assert_received {:halted, 78}
        assert_received {:log, log}

        line = refusal_line(stderr, "/data/malachi.format")
        assert line =~ "REFUSING TO START (exit 78):"
        assert log =~ line
      end
    end

    test "a line another test writes at the same moment does not break the comparison" do
      # Both captures replace a process-global device, so whatever another async test prints while this
      # one runs lands in the same output. Taking the whole capture as one line made this assertion
      # depend on nothing else in the suite refusing to start at that instant.
      parent = self()
      decoy = "REFUSING TO START (exit 78): the format marker /other/node/malachi.format is not valid"

      stderr =
        capture_io(:stderr, fn ->
          log =
            capture_log(fn ->
              IO.puts(:stderr, decoy)
              Logger.error(decoy)
              FormatMarker.refuse!({:invalid, :missing_newline, "/data/malachi.format"}, &send(parent, {:halted, &1}))
            end)

          send(parent, {:log, log})
        end)

      assert_received {:halted, 78}
      assert_received {:log, log}

      line = refusal_line(stderr, "/data/malachi.format")
      assert line =~ "REFUSING TO START (exit 78):"
      refute line =~ "/other/node"
      assert log =~ line
    end

    test "the too-new line names both levels, the writer and the release to go back to" do
      stderr =
        capture_io(:stderr, fn -> capture_log(fn -> FormatMarker.refuse!({:too_new, @v2, 1, "/d"}, & &1) end) end)

      assert stderr =~ "format 2"
      assert stderr =~ "at most format 1"
      assert stderr =~ "release 0.13.0"
      assert stderr =~ "Start release 0.13.0 or newer"
    end
  end
end
