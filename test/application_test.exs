defmodule Malachi.ApplicationTest do
  use ExUnit.Case, async: false

  describe "application startup" do
    test "application is started" do
      assert Process.whereis(Malachi.Supervisor) != nil
    end

    test "all critical services are running" do
      assert Process.whereis(Malachi.PartitionManager) != nil
      assert Process.whereis(Malachi.Metrics) != nil
      assert Process.whereis(Malachi.Auth) != nil
      assert Process.whereis(Malachi.AckManager) != nil
      assert Process.whereis(Malachi.ConnectionRegistry) != nil
    end

    test "QueueRegistry is available" do
      assert Registry.whereis_name({Malachi.QueueRegistry, {:test, 0}}) == :undefined
    end

    test "QueueSupervisor is running" do
      assert Process.whereis(Malachi.QueueSupervisor) != nil
    end

    test "TaskSupervisor is running" do
      assert Process.whereis(Malachi.TaskSupervisor) != nil
    end
  end

  describe "configuration" do
    test "TCP port is configured" do
      port = Application.get_env(:malachi, :tcp_port, 4040)
      assert is_integer(port)
      assert port > 0
    end

    test "dashboard port is configured" do
      port = Application.get_env(:malachi, :dashboard_port, 4041)
      assert is_integer(port)
      assert port > 0
    end

    test "schedulers are configured" do
      schedulers = System.schedulers_online()
      assert schedulers > 0
    end
  end
end
