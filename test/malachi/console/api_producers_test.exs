defmodule Malachi.Console.ApiProducersTest do
  # `GET /api/v1/producers`: the producer streams open on the node that answers, at the viewer role, from
  # stand-in connections registered in `Malachi.ProducerDirectory`.
  #
  # async: false. The directory is the node's, and the async suites open producer streams of their own.
  use ExUnit.Case, async: false

  alias Malachi.ProducerDirectory
  alias Malachi.Test.AccessHelper

  setup do
    Malachi.RateLimiter.reset_bucket("127.0.0.1", :dashboard_auth)
    :ok
  end

  defp producers(token), do: AccessHelper.call(:console, "GET", "/api/v1/producers", token, [])

  # A connection holding producer stream 1, answering listings with `streams`, or never (`:silent`).
  defp connection(streams) do
    parent = self()

    pid =
      spawn(fn ->
        ProducerDirectory.track()
        send(parent, {:tracked, self()})
        answer(streams)
      end)

    assert_receive {:tracked, ^pid}
    on_exit(fn -> Process.exit(pid, :kill) end)
    pid
  end

  defp answer(streams) do
    receive do
      {:describe_producer_streams, from, ref} when streams != :silent ->
        send(from, {ref, self(), streams})
        answer(streams)
    end
  end

  @stream %{
    stream_id: 4,
    topic: "api_producers_topic",
    range: 2,
    segment: 9,
    primary: "malachi@10.0.0.1",
    label: "billing-writer",
    client_ip: "10.0.0.9",
    window: %{appends: 64, bytes: 1_024},
    next_sequence: 12,
    acked_sequence: 10,
    in_flight: %{appends: 2, bytes: 300},
    opened_at: 1_791_720_000_000
  }

  test "a viewer is shown each open producer stream of this node, and the total" do
    connection([@stream])
    token = AccessHelper.account!("producers_viewer", [], :viewer)
    response = producers(token)

    assert response.status == 200
    assert AccessHelper.header(response, "cache-control") == "no-store"
    assert %{"node" => node_name, "streams" => streams, "total" => total, "unanswered_connections" => 0} = response.json
    assert node_name == Atom.to_string(node())
    assert [listed] = Enum.filter(streams, &(&1["topic"] == "api_producers_topic"))

    assert listed == %{
             "stream_id" => 4,
             "topic" => "api_producers_topic",
             "range" => 2,
             "segment" => 9,
             "primary" => "malachi@10.0.0.1",
             "label" => "billing-writer",
             "client_ip" => "10.0.0.9",
             "window" => %{"appends" => 64, "bytes" => 1_024},
             "next_sequence" => 12,
             "acked_sequence" => 10,
             "in_flight" => %{"appends" => 2, "bytes" => 300},
             "opened_at" => "2026-10-11T12:00:00.000Z"
           }

    assert total == %{"count" => length(streams), "relation" => "eq"}
  end

  test "a connection that does not answer makes the total a lower bound" do
    connection([@stream])
    connection(:silent)
    token = AccessHelper.account!("producers_lower_bound", [], :viewer)

    assert %{status: 200, json: %{"total" => %{"relation" => "gte"}, "unanswered_connections" => 1}} =
             producers(token)
  end

  defp page(token, query), do: AccessHelper.call(:console, "GET", "/api/v1/producers?" <> query, token, [])

  test "pages walk the streams in order, each token resuming after the last stream it returned" do
    streams = for n <- 1..5, do: %{@stream | stream_id: n, topic: "api_paged_topic", opened_at: 1_791_720_000_000 + n}
    connection(streams)
    token = AccessHelper.account!("producers_pager", [], :viewer)

    walk = fn walk, query, acc ->
      %{status: 200, json: json} = page(token, query)
      acc = acc ++ for(s <- json["streams"], s["topic"] == "api_paged_topic", do: s["stream_id"])

      case json["next_page_token"] do
        "" -> {acc, json["total"]}
        next -> walk.(walk, "page_size=2&page_token=" <> next, acc)
      end
    end

    {ids, total} = walk.(walk, "page_size=2", [])
    assert ids == [1, 2, 3, 4, 5]
    assert total["count"] >= 5
  end

  test "streams opened in the same millisecond are walked whole, a page boundary inside the tie included" do
    tied = for n <- 1..3, do: %{@stream | stream_id: n, topic: "api_tied_topic", opened_at: 1_791_720_000_000}
    connection(tied)
    connection(Enum.map(tied, &%{&1 | stream_id: &1.stream_id + 10}))
    token = AccessHelper.account!("producers_tied", [], :viewer)

    walk = fn walk, query, acc ->
      %{status: 200, json: json} = page(token, query)
      acc = acc ++ for(s <- json["streams"], s["topic"] == "api_tied_topic", do: s["stream_id"])

      case json["next_page_token"] do
        "" -> acc
        next -> walk.(walk, "page_size=1&page_token=" <> next, acc)
      end
    end

    ids = walk.(walk, "page_size=1&page_token=", [])
    assert Enum.sort(ids) == [1, 2, 3, 11, 12, 13]
    assert length(Enum.uniq(ids)) == 6
  end

  test "a size of 0 is the default and one past the maximum is lowered to it, as AIP-158 asks" do
    connection([@stream])
    token = AccessHelper.account!("producers_sizes", [], :viewer)

    for query <- ["page_size=0", "page_size=5000"] do
      assert %{status: 200, json: %{"streams" => [_ | _]}} = page(token, query), query
    end
  end

  test "a negative size, or a size or token that does not parse, is a 400 problem" do
    token = AccessHelper.account!("producers_bad_page", [], :viewer)

    for query <- [
          "page_size=-1",
          "page_size=x",
          "page_size[]=1",
          "page_token=%%%",
          "page_token[x]=y",
          "page_token=" <> Base.url_encode64("x:y")
        ] do
      assert %{status: 400, json: %{"type" => "errors.http.invalid_request"}} = page(token, query), query
    end
  end

  # A connection that does not answer the first listing it is asked for, and answers every later one.
  defp late_connection(streams) do
    parent = self()

    pid =
      spawn(fn ->
        ProducerDirectory.track()
        send(parent, {:tracked, self()})

        receive do
          {:describe_producer_streams, _from, _ref} -> answer(streams)
        end
      end)

    assert_receive {:tracked, ^pid}
    on_exit(fn -> Process.exit(pid, :kill) end)
    pid
  end

  test "a walk a connection did not answer once is not complete, though that connection answers later" do
    connection([%{@stream | stream_id: 1, topic: "api_late_topic", opened_at: 1_791_720_000_010}])
    connection([%{@stream | stream_id: 3, topic: "api_late_topic", opened_at: 1_791_720_000_030}])
    # its stream sorts before the first page's position: once missed, no later page can return it
    late_connection([%{@stream | stream_id: 2, topic: "api_late_topic", opened_at: 1_791_720_000_005}])
    token = AccessHelper.account!("producers_late", [], :viewer)

    walk = fn walk, query, acc ->
      %{status: 200, json: json} = page(token, query)
      acc = [json | acc]

      case json["next_page_token"] do
        "" -> Enum.reverse(acc)
        next -> walk.(walk, "page_size=1&page_token=" <> next, acc)
      end
    end

    [first | _] = pages = walk.(walk, "page_size=1", [])
    assert first["unanswered_connections"] == 1
    # every page after the one the late connection missed says so, the last one included
    assert Enum.all?(pages, &(&1["complete"] == false))
    seen = for json <- pages, s <- json["streams"], s["topic"] == "api_late_topic", do: s["stream_id"]
    refute 2 in seen
  end

  test "a first page no connection answered is not complete" do
    connection(:silent)
    token = AccessHelper.account!("producers_silent_only", [], :viewer)
    assert %{status: 200, json: %{"complete" => false, "next_page_token" => ""}} = page(token, "page_size=100")
  end

  test "a walk every connection answered is complete on every page" do
    connection(for n <- 1..3, do: %{@stream | stream_id: n, topic: "api_whole_topic", opened_at: 1_791_720_000_000 + n})
    token = AccessHelper.account!("producers_whole", [], :viewer)

    walk = fn walk, query, acc ->
      %{status: 200, json: json} = page(token, query)

      case json["next_page_token"] do
        "" -> Enum.reverse([json | acc])
        next -> walk.(walk, "page_size=1&page_token=" <> next, [json | acc])
      end
    end

    pages = walk.(walk, "page_size=1", [])
    assert length(pages) >= 3
    assert Enum.all?(pages, &(&1["complete"] == true and &1["unanswered_connections"] == 0))
  end

  test "a page of more than the maximum size is cut to it" do
    connection(
      for n <- 1..1_001, do: %{@stream | stream_id: n, topic: "api_big_topic", opened_at: 1_791_720_000_000 + n}
    )

    token = AccessHelper.account!("producers_big", [], :viewer)

    %{status: 200, json: json} = page(token, "page_size=5000")
    assert length(json["streams"]) == 1_000
    assert json["next_page_token"] != ""
    assert json["complete"] == true
  end

  test "it takes a viewer: no credentials is a 401, an account with no console role a 403" do
    assert %{status: 401} = producers(nil)
    token = AccessHelper.account!("producers_wire_only", [:produce], nil)
    assert %{status: 403} = producers(token)
  end
end
