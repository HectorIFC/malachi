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
end
