defmodule Malachi.Cluster.MemberIncarnationTest do
  @moduledoc """
  The incarnation a node resumes at, and the reason it has to survive a restart at all: without it a
  peer keeps the attributes it remembers, and the capability gate reads them as current.
  """
  use ExUnit.Case, async: true

  alias Malachi.Cluster.Capabilities
  alias Malachi.Cluster.MemberIncarnation
  alias Malachi.Cluster.Membership

  @moduletag :tmp_dir

  describe "reserve/2" do
    test "a first boot starts at 1 and records a whole block", %{tmp_dir: dir} do
      assert {:ok, %{start: 1, ceiling: ceiling}} = MemberIncarnation.reserve(dir, 8)

      assert ceiling == 8
      assert MemberIncarnation.read(dir) == 8
    end

    test "a restart resumes above the block the previous boot reserved", %{tmp_dir: dir} do
      {:ok, first} = MemberIncarnation.reserve(dir, 8)
      {:ok, second} = MemberIncarnation.reserve(dir, 8)

      # The point of the whole module: the second boot is above everything the first could have reached,
      # so a peer that remembers the first cannot outrank the second.
      assert second.start > first.ceiling
      assert second == %{start: 9, ceiling: 16}
    end

    test "the ceiling is durable before the numbers are handed out", %{tmp_dir: dir} do
      # A crash between handing out and recording would hand the same numbers out twice, which is the
      # one thing a reservation must not do.
      {:ok, %{ceiling: ceiling}} = MemberIncarnation.reserve(dir, 4)

      assert File.read!(MemberIncarnation.path(dir)) |> String.trim() == to_string(ceiling)
    end

    test "answers the error when the directory cannot be written" do
      assert {:error, _reason} = MemberIncarnation.reserve("/nonexistent/malachi/#{System.unique_integer()}")
    end
  end

  describe "extend/3" do
    test "records a block above where the node has got to", %{tmp_dir: dir} do
      {:ok, _reservation} = MemberIncarnation.reserve(dir, 4)

      assert {:ok, 68} = MemberIncarnation.extend(dir, 4, 64)
      assert MemberIncarnation.read(dir) == 68
    end

    test "answers the error rather than raising, so a membership server keeps serving" do
      assert {:error, _reason} = MemberIncarnation.extend("/nonexistent/malachi/#{System.unique_integer()}", 1)
    end
  end

  describe "read/1" do
    test "a directory with no file reads as zero, which is a first boot", %{tmp_dir: dir} do
      assert MemberIncarnation.read(dir) == 0
    end

    test "a damaged file reads as zero rather than as a guess", %{tmp_dir: dir} do
      # Starting low costs one round of being ignored, which the next refutation corrects. Starting high
      # on a guess would let this node override records it has no right to.
      for content <- ["", "   ", "not a number", "12 34", "-5", "9\nrubbish"] do
        File.write!(MemberIncarnation.path(dir), content)
        assert MemberIncarnation.read(dir) == 0, "read #{inspect(content)} as something other than 0"
      end
    end

    test "a value with surrounding whitespace still reads", %{tmp_dir: dir} do
      File.write!(MemberIncarnation.path(dir), "  42\n")
      assert MemberIncarnation.read(dir) == 42
    end
  end

  describe "why it exists" do
    test "without a resumed incarnation a peer keeps the capabilities of the build that restarted" do
      cap = :batch_format

      # A peer that has seen node :b refute a suspicion holds it at incarnation 3, advertising the
      # capability. :b then restarts on a build that advertises nothing.
      peer = Membership.new(:a, peers: [:b])
      {peer, _effect} = Membership.apply_update(peer, {:b, :alive, 3, Capabilities.attributes(%{}, [cap])})

      from_scratch = announcement_of(Membership.new(:b, peers: [:a], attributes: Capabilities.attributes(%{}, [])))
      {stale, effect} = Membership.apply_update(peer, from_scratch)

      assert effect == :ignored

      assert Capabilities.supported_by_all([:b], cap, reads(stale)) == :ok,
             "expected the stale view to still vouch for the capability"

      # Resumed above what the peer remembers, the announcement lands and the gate tells the truth.
      resumed =
        announcement_of(Membership.new(:b, peers: [:a], attributes: Capabilities.attributes(%{}, []), incarnation: 4))

      {current, effect} = Membership.apply_update(peer, resumed)

      assert effect == {:applied, resumed}
      assert Capabilities.supported_by_all([:b], cap, reads(current)) == {:error, {:unsupported, [:b]}}
    end
  end

  defp announcement_of(view) do
    view |> Membership.updates() |> Enum.find(&(elem(&1, 0) == :b))
  end

  defp reads(view) do
    fn member -> {Membership.status(view, member), Membership.attributes(view, member)} end
  end
end
