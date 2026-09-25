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

  describe "reserve/3" do
    test "a first boot starts one above the seed and records a whole block", %{tmp_dir: dir} do
      assert {:ok, %{start: 1, ceiling: ceiling}} = MemberIncarnation.reserve(dir, 8, 0)

      assert ceiling == 8
      assert MemberIncarnation.read(dir) == {:ok, 8}
    end

    test "a restart resumes above the block the previous boot reserved", %{tmp_dir: dir} do
      {:ok, first} = MemberIncarnation.reserve(dir, 8, 0)
      {:ok, second} = MemberIncarnation.reserve(dir, 8, 0)

      # The point of the whole module: the second boot is above everything the first could have reached,
      # so a peer that remembers the first cannot outrank the second.
      assert second.start > first.ceiling
      assert second == %{start: 9, ceiling: 16}
    end

    test "the ceiling is durable before the numbers are handed out", %{tmp_dir: dir} do
      # A crash between handing out and recording would hand the same numbers out twice, which is the
      # one thing a reservation must not do.
      {:ok, %{ceiling: ceiling}} = MemberIncarnation.reserve(dir, 4, 0)

      assert File.read!(MemberIncarnation.path(dir)) |> String.trim() == to_string(ceiling)
    end

    test "answers the error when the directory cannot be written" do
      assert {:error, _reason} = MemberIncarnation.reserve("/nonexistent/malachi/#{System.unique_integer()}")
    end
  end

  describe "the wall clock floor" do
    test "a first boot with no file starts above the current second", %{tmp_dir: dir} do
      # The upgrade case: no file to read, and peers that may remember this node well above zero. An
      # incarnation counts refutations, so seconds since the epoch clear anything an older build reached.
      before = MemberIncarnation.seed()

      assert {:ok, %{start: start, ceiling: ceiling}} = MemberIncarnation.reserve(dir, 8)

      assert start > before
      assert ceiling == start - 1 + 8
    end

    test "a recorded ceiling above the clock still wins, so a clock moving back cannot lower a node", %{
      tmp_dir: dir
    } do
      far_ahead = MemberIncarnation.seed() * 2
      File.write!(MemberIncarnation.path(dir), "#{far_ahead}\n")

      assert {:ok, %{start: start}} = MemberIncarnation.reserve(dir, 8)
      assert start == far_ahead + 1
    end

    test "the seed is a floor, not the value: a higher record is kept", %{tmp_dir: dir} do
      {:ok, first} = MemberIncarnation.reserve(dir, 8)
      {:ok, second} = MemberIncarnation.reserve(dir, 8, 0)

      # Seed 0 on the second call, and it still resumes above the first block rather than at 1.
      assert second.start == first.ceiling + 1
    end

    test "a clock before the epoch costs the floor, not the reservation", %{tmp_dir: dir} do
      assert {:ok, %{start: 1}} = MemberIncarnation.reserve(dir, 8, 0)
      assert MemberIncarnation.seed() >= 0
    end
  end

  describe "extend/3" do
    test "records a block above where the node has got to", %{tmp_dir: dir} do
      {:ok, _reservation} = MemberIncarnation.reserve(dir, 4, 0)

      assert {:ok, 68} = MemberIncarnation.extend(dir, 4, 64)
      assert MemberIncarnation.read(dir) == {:ok, 68}
    end

    test "answers the error rather than raising, leaving the caller to decide what it means" do
      assert {:error, _reason} = MemberIncarnation.extend("/nonexistent/malachi/#{System.unique_integer()}", 1)
    end
  end

  describe "read/1" do
    test "a directory with no file reads as zero, which is a first boot", %{tmp_dir: dir} do
      assert MemberIncarnation.read(dir) == {:ok, 0}
    end

    test "a damaged file is an error, not a reset to zero", %{tmp_dir: dir} do
      # Resetting would start this node below what its peers remember, and nothing corrects that: a live
      # node is never suspected, so it never refutes, so the peers keep the old record forever.
      for content <- ["", "   ", "not a number", "12 34", "-5", "9\nrubbish"] do
        File.write!(MemberIncarnation.path(dir), content)
        assert {:error, {:damaged, _seen}} = MemberIncarnation.read(dir), "read #{inspect(content)} as usable"
      end
    end

    test "a damaged file stops a reservation rather than overwriting it", %{tmp_dir: dir} do
      File.write!(MemberIncarnation.path(dir), "not a number")

      assert {:error, {:damaged, _seen}} = MemberIncarnation.reserve(dir, 8, 0)
      # And the ceiling that could not be read is still there to be recovered, not replaced by a guess.
      assert File.read!(MemberIncarnation.path(dir)) == "not a number"
    end

    test "an unreadable file is an error of its own", %{tmp_dir: dir} do
      File.mkdir_p!(MemberIncarnation.path(dir))
      assert {:error, {:io, :eisdir}} = MemberIncarnation.read(dir)
    end

    test "a value with surrounding whitespace still reads", %{tmp_dir: dir} do
      File.write!(MemberIncarnation.path(dir), "  42\n")
      assert MemberIncarnation.read(dir) == {:ok, 42}
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

    test "a node upgraded from a build without the file still outranks what its peers remember" do
      cap = :batch_format
      dir = Path.join(System.tmp_dir!(), "inc#{System.unique_integer([:positive])}")
      File.mkdir_p!(dir)
      on_exit(fn -> File.rm_rf!(dir) end)

      # :b ran for a long time on a build that never kept this file, refuting suspicions as it went, so
      # its peer holds it at 7 and still credits it with the capability.
      peer = Membership.new(:a, peers: [:b])
      {peer, _effect} = Membership.apply_update(peer, {:b, :alive, 7, Capabilities.attributes(%{}, [cap])})

      # It is upgraded and boots on the new build, with no ceiling to read.
      assert {:ok, %{start: start}} = MemberIncarnation.reserve(dir)

      resumed =
        announcement_of(
          Membership.new(:b, peers: [:a], attributes: Capabilities.attributes(%{}, []), incarnation: start)
        )

      {current, effect} = Membership.apply_update(peer, resumed)

      assert effect == {:applied, resumed}, "the upgraded node was ignored, and nothing would correct it"
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
