defmodule Malachi.Test.EchoTransport do
  @moduledoc """
  A transport for `Malachi.TCPProtocol` that hands each frame back to the process it is given instead
  of writing it to a socket, so a test reads the exact bytes the protocol boundary would have sent: a
  transport is anything answering `send/2`.
  """

  @doc "Sends `{:frame, frame}` to `pid`."
  @spec send(pid(), iodata()) :: :ok
  def send(pid, frame) do
    Kernel.send(pid, {:frame, frame})
    :ok
  end
end
