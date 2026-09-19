defmodule Malachi.Retention.SkipLedgerTest do
  use ExUnit.Case, async: true

  alias Malachi.Retention.SkipLedger

  @window 1_000

  defp ledger(max \\ 10), do: SkipLedger.new(max, @window)

  test "the first sighting of a skip is counted and logged" do
    assert {_ledger, {:log, 0}} = SkipLedger.observe(ledger(), :skip_a, :reader, 0)
  end

  test "the same skip read again (a poll before the commit) is a duplicate" do
    {ledger, {:log, 0}} = SkipLedger.observe(ledger(), :skip_a, :reader, 0)

    assert {_ledger, :duplicate} = SkipLedger.observe(ledger, :skip_a, :reader, 10)
  end

  test "a new skip of the same reader inside the window is counted but not logged" do
    {ledger, {:log, 0}} = SkipLedger.observe(ledger(), :skip_a, :reader, 0)

    assert {_ledger, :count} = SkipLedger.observe(ledger, :skip_b, :reader, 10)
  end

  test "after the window the reader is logged again, with how many lines were held back" do
    {ledger, {:log, 0}} = SkipLedger.observe(ledger(), :skip_a, :reader, 0)
    {ledger, :count} = SkipLedger.observe(ledger, :skip_b, :reader, 10)
    {ledger, :count} = SkipLedger.observe(ledger, :skip_c, :reader, 20)

    assert {ledger, {:log, 2}} = SkipLedger.observe(ledger, :skip_d, :reader, @window)
    # and the count of held-back lines starts over
    assert {_ledger, :count} = SkipLedger.observe(ledger, :skip_e, :reader, @window + 1)
  end

  test "readers are rate limited independently" do
    {ledger, {:log, 0}} = SkipLedger.observe(ledger(), :skip_a, :reader_a, 0)

    assert {_ledger, {:log, 0}} = SkipLedger.observe(ledger, :skip_b, :reader_b, 1)
  end

  test "memory is bounded: past max the oldest skip is forgotten, and counted again if it returns" do
    {ledger, _} = SkipLedger.observe(ledger(2), :skip_a, :reader, 0)
    {ledger, _} = SkipLedger.observe(ledger, :skip_b, :reader, 1)
    {ledger, _} = SkipLedger.observe(ledger, :skip_c, :reader, 2)

    assert SkipLedger.size(ledger) == 2
    assert {_ledger, :count} = SkipLedger.observe(ledger, :skip_a, :reader, 3)
    assert {_ledger, :duplicate} = SkipLedger.observe(ledger, :skip_c, :reader, 3)
  end

  test "the per-reader log state is bounded the same way" do
    {ledger, {:log, 0}} = SkipLedger.observe(ledger(2), :skip_a, :reader_a, 0)
    {ledger, {:log, 0}} = SkipLedger.observe(ledger, :skip_b, :reader_b, 1)
    {ledger, {:log, 0}} = SkipLedger.observe(ledger, :skip_c, :reader_c, 2)

    # reader_a was forgotten, so its next skip opens a new window instead of being held back
    assert {_ledger, {:log, 0}} = SkipLedger.observe(ledger, :skip_d, :reader_a, 3)
  end

  test "a non-positive bound is refused" do
    assert_raise FunctionClauseError, fn -> SkipLedger.new(0, @window) end
  end
end
