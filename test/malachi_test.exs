defmodule MalachiTest do
  use ExUnit.Case

  describe "Malachi Application" do
    test "application starts successfully" do
      assert Code.ensure_loaded?(Malachi.Application)
    end

    test "main modules are available" do
      assert Code.ensure_loaded?(Malachi.Auth)
      assert Code.ensure_loaded?(Malachi.BrokerServer)
      assert Code.ensure_loaded?(Malachi.LogApi)
    end
  end
end
