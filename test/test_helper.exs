# Start the application for all tests
# Individual tests handle their own state isolation via setup/on_exit blocks
{:ok, _} = Application.ensure_all_started(:malachi)

# Multi-node tests spin up peer BEAM nodes (need epmd/distribution); opt in with
# `mix test --include multinode`.
ExUnit.start(exclude: [:multinode])
