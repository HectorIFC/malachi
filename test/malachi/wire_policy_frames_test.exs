defmodule Malachi.WirePolicyFramesTest do
  @moduledoc """
  The storage policy api keys (17 to 21) against the golden frames both clients read
  (`test/support/fixtures/wire/policy_frames.json`; the Node side is `scripts/lib/wire.selftest.js`). A
  shipped frame is frozen, so a difference here is a wire break, not a fixture to regenerate.
  """
  use ExUnit.Case, async: true

  alias Malachi.Wire

  @fixture "test/support/fixtures/wire/policy_frames.json"
  @external_resource @fixture
  @cases @fixture |> File.read!() |> Jason.decode!() |> Map.fetch!("cases")

  defp fields(list), do: Enum.map(list, &List.to_tuple/1)

  defp encode("define_policy_req", v), do: Wire.encode_define_policy_req(v["name"], fields(v["fields"]))
  defp encode("delete_policy_req", v), do: Wire.encode_delete_policy_req(v["name"], v["force"])
  defp encode("list_policies_req", nil), do: <<>>
  defp encode("list_policies_resp", v), do: Wire.encode_list_policies_resp(Enum.map(v, fn [n, f] -> {n, fields(f)} end))
  defp encode("bind_topic_policy_req", v), do: Wire.encode_bind_topic_policy_req(v["topic"], v["name"])
  defp encode("get_topic_policy_req", v), do: Wire.encode_get_topic_policy_req(v)
  defp encode("topic_policy_resp", v), do: Wire.encode_topic_policy_resp(topic_policy(v))

  defp decode("define_policy_req", bin) do
    {name, fields} = Wire.decode_define_policy_req(bin)
    %{"name" => name, "fields" => Enum.map(fields, &Tuple.to_list/1)}
  end

  defp decode("delete_policy_req", bin) do
    {name, force} = Wire.decode_delete_policy_req(bin)
    %{"name" => name, "force" => force}
  end

  defp decode("list_policies_req", bin) do
    :ok = Wire.decode_list_policies_req(bin)
    nil
  end

  defp decode("list_policies_resp", bin),
    do: Enum.map(Wire.decode_list_policies_resp(bin), fn {n, f} -> [n, Enum.map(f, &Tuple.to_list/1)] end)

  defp decode("bind_topic_policy_req", bin) do
    {topic, name} = Wire.decode_bind_topic_policy_req(bin)
    %{"topic" => topic, "name" => name}
  end

  defp decode("get_topic_policy_req", bin), do: Wire.decode_get_topic_policy_req(bin)
  defp decode("topic_policy_resp", bin), do: bin |> Wire.decode_topic_policy_resp() |> to_json()

  defp topic_policy(v) do
    %{
      topic: v["topic"],
      policy: v["policy"],
      resolution: String.to_existing_atom(v["resolution"]),
      definition: v["definition"] && fields(v["definition"]),
      effective: Enum.map(v["effective"], fn [n, x, o] -> {n, x, String.to_existing_atom(o)} end)
    }
  end

  defp to_json(resp) do
    %{
      "topic" => resp.topic,
      "policy" => resp.policy,
      "resolution" => Atom.to_string(resp.resolution),
      "definition" => resp.definition && Enum.map(resp.definition, &Tuple.to_list/1),
      "effective" => Enum.map(resp.effective, fn {n, x, o} -> [n, x, Atom.to_string(o)] end)
    }
  end

  test "the fixture covers every policy codec, both directions" do
    codecs = @cases |> Enum.map(& &1["codec"]) |> Enum.uniq() |> Enum.sort()

    assert codecs == ~w(bind_topic_policy_req define_policy_req delete_policy_req get_topic_policy_req
                        list_policies_req list_policies_resp topic_policy_resp)
  end

  for %{"name" => name, "codec" => codec, "value" => value, "hex" => hex} <- @cases do
    test "#{codec}: #{name}" do
      bytes = Base.decode16!(unquote(hex), case: :lower)
      assert encode(unquote(codec), unquote(Macro.escape(value))) == bytes
      assert decode(unquote(codec), bytes) == unquote(Macro.escape(value))
    end
  end

  test "the api keys are the next free numbers after the ACL keys" do
    assert {Wire.define_policy_key(), Wire.delete_policy_key(), Wire.list_policies_key()} == {17, 18, 19}
    assert {Wire.bind_topic_policy_key(), Wire.get_topic_policy_key()} == {20, 21}
  end

  describe "a malformed payload raises, which the connection answers as malformed_request" do
    test "trailing bytes, an unknown value tag and a force byte that is not a boolean" do
      define = Wire.encode_define_policy_req("p", [])
      assert_raise MatchError, fn -> Wire.decode_define_policy_req(define <> <<0>>) end

      unknown_tag = <<1, 1::32, "p", 1::16, 1, 1::32, "x", 9>>
      assert_raise FunctionClauseError, fn -> Wire.decode_define_policy_req(unknown_tag) end

      assert_raise MatchError, fn -> Wire.decode_delete_policy_req(<<1, 1::32, "p", 2>>) end
      assert_raise FunctionClauseError, fn -> Wire.decode_list_policies_req(<<0>>) end
    end

    test "an unknown resolution or origin code" do
      bad_resolution = <<1, 1::32, "t", 0, 7, 0, 0::16>>
      assert_raise ArgumentError, fn -> Wire.decode_topic_policy_resp(bad_resolution) end

      bad_origin = <<1, 1::32, "t", 0, 0, 0, 1::16, 1, 1::32, "x", 0, 9>>
      assert_raise ArgumentError, fn -> Wire.decode_topic_policy_resp(bad_origin) end
    end
  end
end
