defmodule Malachi.ConfigTest do
  use ExUnit.Case, async: true

  alias Malachi.Config

  doctest Malachi.Config

  describe "data_dir/3" do
    test "returns an absolute path unchanged in production" do
      assert Config.data_dir("MALACHI_LOG_DATA_DIR", "/mnt/vol/log", :prod) == "/mnt/vol/log"
    end

    test "raises in production when the path is relative" do
      # The failure this guards: a relative path resolves against the process working directory, so a
      # container would write durable segments to ephemeral storage and lose them on the next restart.
      assert_raise RuntimeError, ~r/MALACHI_LOG_DATA_DIR must be an absolute path/, fn ->
        Config.data_dir("MALACHI_LOG_DATA_DIR", "app/data/log", :prod)
      end
    end

    test "names the offending variable in the error" do
      assert_raise RuntimeError, ~r/MALACHI_RA_DATA_DIR must be an absolute path/, fn ->
        Config.data_dir("MALACHI_RA_DATA_DIR", "data/ra", :prod)
      end
    end

    test "allows a relative path outside production" do
      assert Config.data_dir("MALACHI_LOG_DATA_DIR", "data/log", :dev) == "data/log"
      assert Config.data_dir("MALACHI_LOG_DATA_DIR", "data/log", :test) == "data/log"
    end

    test "treats nil as absent" do
      assert Config.data_dir("MALACHI_LOG_DATA_DIR", nil, :prod) == nil
    end

    test "treats an empty string as absent" do
      assert Config.data_dir("MALACHI_LOG_DATA_DIR", "", :prod) == nil
    end

    test "treats an all-whitespace value as absent, so it never reaches the absolute-path check" do
      assert Config.data_dir("MALACHI_LOG_DATA_DIR", "   ", :prod) == nil
    end

    test "trims surrounding whitespace from a real path" do
      assert Config.data_dir("MALACHI_LOG_DATA_DIR", "  /mnt/vol/log  ", :prod) == "/mnt/vol/log"
    end
  end

  describe "sampling_ratio/1" do
    test "accepts the whole valid range, including both ends" do
      assert Config.sampling_ratio("0") == {:ok, 0.0}
      assert Config.sampling_ratio("0.0") == {:ok, 0.0}
      assert Config.sampling_ratio("0.001") == {:ok, 0.001}
      assert Config.sampling_ratio("1") == {:ok, 1.0}
      assert Config.sampling_ratio("1.0") == {:ok, 1.0}
    end

    test "an absent value means sample everything" do
      # Tracing is opt-in, so a deployment that switched it on without naming a ratio asked to see it
      # all. Zero would be the other reading and would make the switch do nothing.
      assert Config.sampling_ratio(nil) == {:ok, 1.0}
    end

    test "rejects a value outside the range in either direction" do
      # Negative reaches :trace_id_ratio_based and breaks the sampler at startup; above 1.0 used to
      # fall through to :always_on, which looks like a deliberate setting rather than a rejected one.
      assert Config.sampling_ratio("-0.5") == :invalid
      assert Config.sampling_ratio("1.5") == :invalid
      assert Config.sampling_ratio("100") == :invalid
    end

    test "rejects a value the parser would only partly understand" do
      # This is the case worth the strictness. Float.parse takes the leading number and discards the
      # rest, so a decimal comma would have become 0.0 and traced nothing, and a trailing typo would
      # have been accepted at its prefix. Both silently, neither the number anyone typed.
      assert Config.sampling_ratio("0,1") == :invalid
      assert Config.sampling_ratio("0.1bad") == :invalid
      assert Config.sampling_ratio("0.5 percent") == :invalid
    end

    test "rejects a value that does not parse at all, rather than taking the default" do
      # The dangerous one: unparseable used to fall back to 1.0, so a typo in the variable name's
      # VALUE turned full sampling on in production without a word.
      assert Config.sampling_ratio("ten") == :invalid
      assert Config.sampling_ratio("") == :invalid
      assert Config.sampling_ratio("   ") == :invalid
    end

    test "tolerates surrounding whitespace around a real value" do
      assert Config.sampling_ratio("  0.25  ") == {:ok, 0.25}
    end
  end
end
