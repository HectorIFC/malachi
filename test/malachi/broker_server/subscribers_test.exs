defmodule Malachi.BrokerServer.SubscribersTest do
  # The subscriber bookkeeping of `Malachi.BrokerServer`, alone: who is owed a read, how much it may
  # take, and what its position and credit become when the read ends. The reads themselves run in the
  # subscribers' processes now, so a wake that lands while a read is out must not hand out a second one,
  # and the credit has to hold on the push side without leaning on the clamp in the ack path.
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Malachi.BrokerServer.Subscribers

  defp sub(ref, topic, opts \\ []) do
    %{
      pid: Keyword.get(opts, :pid, self()),
      ref: ref,
      topic: topic,
      group: "g",
      positions: %{},
      window: Keyword.get(opts, :window, 10),
      in_flight: Keyword.get(opts, :in_flight, 0),
      max: Keyword.get(opts, :max, 4),
      member: nil,
      ranges: nil,
      coordinator: nil
    }
  end

  defp refs(reads), do: Enum.map(reads, fn {sub, _budget} -> sub.ref end)

  describe "one read at a time" do
    test "a subscriber gets its first read when it is added, bounded by max and its credit" do
      ref = make_ref()
      assert {[{%{ref: ^ref}, 4}], _subs} = Subscribers.add(Subscribers.new(), sub(ref, "t"))

      ref2 = make_ref()
      assert {[{_, 2}], _subs} = Subscribers.add(Subscribers.new(), sub(ref2, "t", window: 2))
    end

    test "a wake during a read hands out no second read, and the read is handed out again when it ends" do
      ref = make_ref()
      {[_first], subs} = Subscribers.add(Subscribers.new(), sub(ref, "t"))

      assert {[], subs} = Subscribers.wake(subs, "t")
      assert {[], subs} = Subscribers.wake(subs, "t")
      assert [%{reading: true, wake_pending: true}] = Subscribers.list(subs, "t")

      assert {[{%{ref: ^ref, positions: %{r: 4}}, 4}], subs} = Subscribers.read_done(subs, ref, {:ok, 4, %{r: 4}})
      assert [%{reading: true, wake_pending: false, in_flight: 4}] = Subscribers.list(subs, "t")
    end

    test "a read that ended with no wake meanwhile leaves the subscriber idle" do
      ref = make_ref()
      {_, subs} = Subscribers.add(Subscribers.new(), sub(ref, "t"))
      assert {[], subs} = Subscribers.read_done(subs, ref, {:ok, 0, %{r: 0}})
      assert [%{reading: false, in_flight: 0, positions: %{r: 0}}] = Subscribers.list(subs, "t")
    end

    test "a failed read moves neither the position nor the credit" do
      ref = make_ref()
      {_, subs} = Subscribers.add(Subscribers.new(), sub(ref, "t") |> Map.put(:positions, %{r: 7}))
      assert {[], subs} = Subscribers.read_done(subs, ref, :error)
      assert [%{reading: false, in_flight: 0, positions: %{r: 7}}] = Subscribers.list(subs, "t")
    end

    test "the end of a read for a subscription with no read out is ignored" do
      ref = make_ref()
      {_, subs} = Subscribers.add(Subscribers.new(), sub(ref, "t"))
      {[], subs} = Subscribers.read_done(subs, ref, {:ok, 2, %{r: 2}})
      assert {[], ^subs} = Subscribers.read_done(subs, ref, {:ok, 3, %{r: 5}})
    end

    test "the end of a read for a subscription that is gone is ignored" do
      ref = make_ref()
      {_, subs} = Subscribers.add(Subscribers.new(), sub(ref, "t"))
      {_removed, subs} = Subscribers.remove_ref(subs, ref)
      assert {[], ^subs} = Subscribers.read_done(subs, ref, {:ok, 3, %{}})
    end

    test "a subscriber out of credit is not read for, and an ack that returns credit reads again" do
      ref = make_ref()
      {[_], subs} = Subscribers.add(Subscribers.new(), sub(ref, "t", window: 2, max: 10))
      {[], subs} = Subscribers.read_done(subs, ref, {:ok, 2, %{}})

      assert {[], subs} = Subscribers.wake(subs, "t")
      assert {[{_, 1}], _subs} = Subscribers.ack(subs, "t", self(), 1, nil, nil)
    end
  end

  describe "the ref index" do
    test "a :DOWN removes exactly one subscription and leaves every other topic untouched" do
      {a, b, c} = {make_ref(), make_ref(), make_ref()}
      other = spawn(fn -> :ok end)

      subs =
        Enum.reduce([sub(a, "t"), sub(b, "u"), sub(c, "t", pid: other)], Subscribers.new(), fn s, acc ->
          acc |> Subscribers.add(s) |> elem(1)
        end)

      assert {%{ref: ^a}, subs} = Subscribers.remove_ref(subs, a)
      assert refs_of(subs, "t") == [c]
      assert refs_of(subs, "u") == [b]
      assert subs.by_ref == %{b => "u", c => "t"}
      assert {nil, ^subs} = Subscribers.remove_ref(subs, a)
    end

    test "unsubscribing a pid removes its refs from the index too" do
      {a, b} = {make_ref(), make_ref()}
      subs = Enum.reduce([sub(a, "t"), sub(b, "u")], Subscribers.new(), &(&2 |> Subscribers.add(&1) |> elem(1)))

      assert {[%{ref: ^a}], subs} = Subscribers.remove_pid(subs, "t", self())
      assert subs.by_ref == %{b => "u"}
      assert {nil, _} = Subscribers.remove_ref(subs, a)
    end
  end

  describe "rotate" do
    test "each read starts the range list one further along" do
      assert Subscribers.rotate([:a, :b, :c], %{turn: 0}) == [:a, :b, :c]
      assert Subscribers.rotate([:a, :b, :c], %{turn: 1}) == [:b, :c, :a]
      assert Subscribers.rotate([:a, :b, :c], %{turn: 5}) == [:c, :a, :b]
      assert Subscribers.rotate([], %{turn: 3}) == []
    end
  end

  # --- the invariants, under any interleaving ---

  # The model the property drives: the reads handed out and not yet ended, by ref, with their budgets,
  # and the pids each ref belongs to. `Subscribers` is checked against it after every step.
  defp command(refs) do
    one_of([
      tuple({constant(:wake), member_of(["t", "u"])}),
      tuple({constant(:ack), member_of(refs), integer(0..12)}),
      tuple({constant(:read_done), member_of(refs), float(min: 0.0, max: 1.0)}),
      tuple({constant(:fail), member_of(refs)}),
      tuple({constant(:down), member_of(refs)}),
      tuple({constant(:unsubscribe), member_of(refs)}),
      tuple({constant(:orphan_done), member_of(refs)})
    ])
  end

  defp setup_subs do
    gen all(
          specs <- list_of(tuple({member_of(["t", "u"]), integer(1..12), integer(1..6)}), min_length: 1, max_length: 6)
        ) do
      Enum.map(specs, fn {topic, window, max} -> {make_ref(), topic, window, max} end)
    end
  end

  property "one read at a time, credit within the window, and an index that matches the lists" do
    check all(
            specs <- setup_subs(),
            refs = Enum.map(specs, &elem(&1, 0)),
            commands <- list_of(command(refs), max_length: 60),
            max_runs: 300
          ) do
      pids = Map.new(specs, fn {ref, _, _, _} -> {ref, :c.pid(0, System.unique_integer([:positive]), 0)} end)

      {subs, outstanding} =
        Enum.reduce(specs, {Subscribers.new(), %{}}, fn {ref, topic, window, max}, {subs, out} ->
          {reads, subs} = Subscribers.add(subs, sub(ref, topic, window: window, max: max, pid: pids[ref]))
          {subs, record_reads(out, reads)}
        end)

      check_invariants(subs, outstanding)

      # `orphans`: refs removed while a read of theirs was out; that read can still end, and must change nothing.
      Enum.reduce(commands, {subs, outstanding, MapSet.new()}, fn command, {subs, out, orphans} ->
        {subs, out, orphans} = step(command, subs, out, orphans, pids)
        check_invariants(subs, out)
        {subs, out, orphans}
      end)
    end
  end

  defp step({:orphan_done, ref}, subs, out, orphans, _pids) do
    if MapSet.member?(orphans, ref) do
      assert Subscribers.read_done(subs, ref, {:ok, 1, %{ref => 1}}) == {[], subs}
      {subs, out, MapSet.delete(orphans, ref)}
    else
      {subs, out, orphans}
    end
  end

  defp step(command, subs, out, orphans, pids) do
    {subs, new_out} = step(command, subs, out, pids)

    # a ref whose read was out and is no longer subscribed now has an orphaned read
    gone = for {ref, _budget} <- out, not Map.has_key?(subs.by_ref, ref), into: MapSet.new(), do: ref
    {subs, Map.drop(new_out, MapSet.to_list(gone)), MapSet.union(orphans, gone)}
  end

  defp step({:wake, topic}, subs, out, _pids) do
    {reads, subs} = Subscribers.wake(subs, topic)
    {subs, record_reads(out, reads)}
  end

  defp step({:ack, ref, count}, subs, out, pids) do
    case find(subs, ref) do
      nil ->
        {subs, out}

      sub ->
        {reads, subs} = Subscribers.ack(subs, sub.topic, pids[ref], count, nil, nil)
        {subs, record_reads(out, reads)}
    end
  end

  defp step({:read_done, ref, fraction}, subs, out, _pids) do
    case Map.pop(out, ref) do
      {nil, _} ->
        {subs, out}

      {budget, out} ->
        pushed = trunc(fraction * budget)
        {reads, subs} = Subscribers.read_done(subs, ref, {:ok, pushed, %{ref => pushed}})
        {subs, record_reads(out, reads)}
    end
  end

  defp step({:fail, ref}, subs, out, _pids) do
    case Map.pop(out, ref) do
      {nil, _} ->
        {subs, out}

      {_budget, out} ->
        {reads, subs} = Subscribers.read_done(subs, ref, :error)
        {subs, record_reads(out, reads)}
    end
  end

  defp step({:down, ref}, subs, out, _pids) do
    {_removed, subs} = Subscribers.remove_ref(subs, ref)
    {subs, out}
  end

  defp step({:unsubscribe, ref}, subs, out, pids) do
    case find(subs, ref) do
      nil ->
        {subs, out}

      sub ->
        {_removed, subs} = Subscribers.remove_pid(subs, sub.topic, pids[ref])
        {subs, out}
    end
  end

  # Every read handed out is recorded; one handed out to a ref that already has one out is the bug.
  defp record_reads(out, reads) do
    Enum.reduce(reads, out, fn {sub, budget}, out ->
      refute Map.has_key?(out, sub.ref), "a second read was handed out while one was in progress"
      assert budget > 0 and budget <= sub.max and budget <= sub.window - sub.in_flight
      Map.put(out, sub.ref, budget)
    end)
  end

  defp check_invariants(subs, out) do
    all = for topic <- Subscribers.topics(subs), sub <- Subscribers.list(subs, topic), do: {topic, sub}

    for {topic, sub} <- all do
      # the credit, checked here and not through the ack path's clamp
      assert sub.in_flight <= sub.window
      assert sub.reading == Map.has_key?(out, sub.ref)
      assert sub.topic == topic
      assert Map.fetch!(subs.by_ref, sub.ref) == topic
    end

    assert map_size(subs.by_ref) == length(all)

    # each topic's push order names exactly the subscribers it holds
    for {_topic, %{order: order, by_ref: topic_by_ref}} <- subs.by_topic do
      assert Enum.sort(order) == Enum.sort(Map.keys(topic_by_ref))
      assert length(order) == length(Enum.uniq(order))
    end
  end

  defp find(subs, ref) do
    Enum.find_value(Subscribers.topics(subs), fn topic -> Enum.find(Subscribers.list(subs, topic), &(&1.ref == ref)) end)
  end

  defp refs_of(subs, topic), do: subs |> Subscribers.list(topic) |> Enum.map(& &1.ref)

  test "a wake hands out its reads in push order, newest subscriber first" do
    {a, b} = {make_ref(), make_ref()}
    subs = Enum.reduce([sub(a, "t"), sub(b, "t")], Subscribers.new(), &(&2 |> Subscribers.add(&1) |> elem(1)))
    {[], subs} = Subscribers.read_done(subs, a, {:ok, 0, %{}})
    {[], subs} = Subscribers.read_done(subs, b, {:ok, 0, %{}})
    {reads, _} = Subscribers.wake(subs, "t")
    assert refs(reads) == [b, a]
  end
end
