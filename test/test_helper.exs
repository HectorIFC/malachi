# Start Erlang distribution BEFORE the app, with the same node name the multinode tests use. The app forms
# an ra cluster for the replicated user store at boot; if a later test renamed the node
# (`:net_kernel.start`), that cluster (formed under the old name) would be orphaned. Naming the node up
# front keeps it stable. The multinode tests then see `:already_started` and do not rename it.
_ = System.cmd("epmd", ["-daemon"])

case :net_kernel.start([:"malachi_primary@127.0.0.1", :longnames]) do
  {:ok, _pid} -> :ok
  {:error, {:already_started, _pid}} -> :ok
end

# Start the application for all tests
# Individual tests handle their own state isolation via setup/on_exit blocks
{:ok, _} = Application.ensure_all_started(:malachi)

# Remove this run's isolated log-broker and ra data dirs (config/test.exs) once the suite finishes.
ExUnit.after_suite(fn _result ->
  for key <- [:log_data_dir, :ra_data_dir] do
    case Application.get_env(:malachi, key) do
      nil -> :ok
      dir -> File.rm_rf(dir)
    end
  end
end)

# Multi-node tests spin up peer BEAM nodes (need epmd/distribution); opt in with
# `mix test --include multinode`.
ExUnit.start(exclude: [:multinode])
