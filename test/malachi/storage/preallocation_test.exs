defmodule Malachi.Storage.PreallocationTest do
  use ExUnit.Case, async: true

  alias Malachi.Storage.Preallocation

  @moduletag :tmp_dir

  # Every strategy has to end up at the same OBSERVABLE state, which is the point of having them
  # behind one function: the file is `to_byte` long and the new region reads back as zeros. What
  # differs is only what the filesystem had to do to get there, which is what the benchmark
  # measures and what no test can assert portably.
  @strategies [:sparse, :allocate, :zeros]

  defp open_file(directory, name \\ "seg.log") do
    path = Path.join(directory, name)
    File.touch!(path)
    {:ok, file_descriptor} = :file.open(path, [:read, :write, :raw, :binary])
    {path, file_descriptor}
  end

  for strategy <- @strategies do
    describe "extend/4 with #{inspect(strategy)}" do
      @describetag strategy: strategy

      test "sizes an empty file and leaves the region reading as zeros", %{tmp_dir: directory, strategy: strategy} do
        {path, file_descriptor} = open_file(directory)

        assert :ok = Preallocation.extend(file_descriptor, 0, 8192, strategy)

        assert File.stat!(path).size == 8192
        assert {:ok, chunk} = :file.pread(file_descriptor, 0, 8192)
        assert chunk == :binary.copy(<<0>>, 8192)
        :ok = :file.close(file_descriptor)
      end

      test "extends past existing bytes without disturbing them", %{tmp_dir: directory, strategy: strategy} do
        {path, file_descriptor} = open_file(directory)
        :ok = :file.pwrite(file_descriptor, 0, "already here")
        written = byte_size("already here")

        assert :ok = Preallocation.extend(file_descriptor, written, 4096, strategy)

        assert File.stat!(path).size == 4096
        assert {:ok, "already here"} = :file.pread(file_descriptor, 0, written)
        assert {:ok, tail} = :file.pread(file_descriptor, written, 4096 - written)
        assert tail == :binary.copy(<<0>>, 4096 - written)
        :ok = :file.close(file_descriptor)
      end

      # Asking for room must never take any away. Recovery re-extends a segment whose valid bytes
      # may already exceed the configured target (the target was lowered between runs), and if that
      # call shrank the file it would delete committed records while claiming to preallocate.
      test "never shrinks a file that is already longer", %{tmp_dir: directory, strategy: strategy} do
        {path, file_descriptor} = open_file(directory)
        :ok = Preallocation.extend(file_descriptor, 0, 8192, :zeros)

        assert :ok = Preallocation.extend(file_descriptor, 8192, 4096, strategy)
        assert :ok = Preallocation.extend(file_descriptor, 8192, 8192, strategy)

        assert File.stat!(path).size == 8192
        :ok = :file.close(file_descriptor)
      end
    end
  end

  describe "the :zeros chunk loop" do
    # The chunked write loop is the only strategy with arithmetic of its own, and a size that is
    # not a whole number of chunks is where an off-by-one in the final slice would land.
    test "writes a size that is not a whole number of chunks", %{tmp_dir: directory} do
      {path, file_descriptor} = open_file(directory)
      size = 1_048_576 + 3

      assert :ok = Preallocation.extend(file_descriptor, 0, size, :zeros)

      assert File.stat!(path).size == size
      assert {:ok, tail} = :file.pread(file_descriptor, size - 3, 3)
      assert tail == <<0, 0, 0>>
      :ok = :file.close(file_descriptor)
    end
  end

  describe "a file operation that fails" do
    # Every strategy has to surface the error rather than answer :ok on a file it did not extend.
    # A closed descriptor is the portable way to make each underlying call fail: the caller
    # (Malachi.Storage.ElixirStore) opens a segment for preallocation and must not go on to write
    # into a region it only believes exists.
    test "is reported, whichever strategy hit it", %{tmp_dir: directory} do
      for strategy <- @strategies do
        {_path, file_descriptor} = open_file(directory, "closed_#{strategy}.log")
        :ok = :file.close(file_descriptor)

        assert {:error, _reason} = Preallocation.extend(file_descriptor, 0, 4096, strategy)
      end
    end
  end

  describe "extend_or_restore/4" do
    test "answers :extended once the extension is in place", %{tmp_dir: directory} do
      {path, file_descriptor} = open_file(directory)

      assert Preallocation.extend_or_restore(file_descriptor, 0, 4096, :zeros) == :extended
      assert File.stat!(path).size == 4096

      :ok = :file.close(file_descriptor)
    end

    test "answers :restored and puts the file back when the extension fails", %{tmp_dir: directory} do
      {path, file_descriptor} = open_file(directory)
      :ok = :file.pwrite(file_descriptor, 0, "record")

      # A position no file can reach makes the extension fail on any filesystem, while the descriptor stays
      # good for the restore.
      assert Preallocation.extend_or_restore(file_descriptor, 6, Bitwise.bsl(1, 64), :sparse) == :restored
      assert File.read!(path) == "record"

      :ok = :file.close(file_descriptor)
    end

    test "answers the error when the file cannot be put back either", %{tmp_dir: directory} do
      # The case that must not pass for a degraded success (`Malachi.Storage.ElixirStore` refuses the handle):
      # a closed descriptor fails the extension and the restore alike.
      {_path, file_descriptor} = open_file(directory)
      :ok = :file.close(file_descriptor)

      assert {:error, _reason} = Preallocation.extend_or_restore(file_descriptor, 0, 4096, :zeros)
    end
  end

  describe "file_size/1" do
    test "reports the descriptor's own size", %{tmp_dir: directory} do
      {_path, file_descriptor} = open_file(directory)

      assert {:ok, 0} = Preallocation.file_size(file_descriptor)
      :ok = Preallocation.extend(file_descriptor, 0, 1234, :zeros)
      assert {:ok, 1234} = Preallocation.file_size(file_descriptor)

      :ok = :file.close(file_descriptor)
    end
  end
end
