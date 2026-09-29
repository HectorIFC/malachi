defmodule Malachi.HttpTransportContract.DashboardTest do
  use Malachi.Test.HttpContract, target: :dashboard
end

defmodule Malachi.HttpTransportContract.ConsoleTest do
  use Malachi.Test.HttpContract, target: :console
end
