defmodule Malachi.Test.StorageFaults do
  @moduledoc """
  Real filesystem faults for storage tests.

  `Malachi.Test.FaultySegmentStore` fakes a failure at the store boundary, which is what the layers above
  the store need. These reach the store's OWN file operations instead, which is what proves the store
  hands an error back rather than raising it.
  """

  import ExUnit.Assertions

  @doc """
  Takes every permission away from `path` for the rest of the test and proves it took. Must be called
  from the test process (it registers the restore with `ExUnit.Callbacks.on_exit/1`).
  """
  @spec make_unreadable!(Path.t()) :: :ok
  def make_unreadable!(path) do
    File.chmod!(path, 0o000)
    ExUnit.Callbacks.on_exit(fn -> File.chmod(path, 0o644) end)

    # Permissions do not bind root. A test that carried on would pass for the wrong reason, so it fails here
    # instead, loudly.
    assert {:error, :eacces} = :file.open(path, [:read, :raw]),
           "#{path} is still readable after chmod 000; is the suite running as root?"

    :ok
  end

  @doc """
  Closes a `Malachi.Storage.ElixirStore` handle's descriptor behind its back, so the next operation on it
  fails the way a descriptor on a failing device does. Measured on macOS and Linux (OTP 28 and 29): every
  raw file operation on a closed descriptor answers `{:error, :einval}` and none raises.
  """
  @spec close_descriptor!(%{file_descriptor: :file.fd()}) :: :ok
  def close_descriptor!(%{file_descriptor: file_descriptor}) do
    :ok = :file.close(file_descriptor)
  end
end
