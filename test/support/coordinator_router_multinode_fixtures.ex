defmodule Malachi.Consumer.CoordinatorRouterMultinodeFixtures do
  @moduledoc """
  Fixtures for `Malachi.Consumer.CoordinatorRouterMultinodeTest`. Lives under `test/support` (compiled to
  the test ebin, so it is on the code path) rather than inline in the `_test.exs` file — the multinode test
  ships this module's `&ranges/1` capture to peer nodes as the coordinator's `ranges_fun`, and only modules
  on the code path can be loaded remotely.
  """

  @doc "A fixed two-range assignment source, used as a coordinator `ranges_fun` in the multinode test."
  def ranges(_topic), do: [:r0, :r1]
end
