# Start the application for all tests
# Individual tests handle their own state isolation via setup/on_exit blocks
{:ok, _} = Application.ensure_all_started(:malachi)

# Remove this run's isolated log-broker data dir (config/test.exs) once the suite finishes.
ExUnit.after_suite(fn _result ->
  case Application.get_env(:malachi, :log_data_dir) do
    nil -> :ok
    dir -> File.rm_rf(dir)
  end
end)

# Multi-node tests spin up peer BEAM nodes (need epmd/distribution); opt in with
# `mix test --include multinode`.
ExUnit.start(exclude: [:multinode])
