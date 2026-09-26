# Name the node once, before any test runs, so no test renames it later: the multinode tests' own
# `Distribution.ensure_started/0` then leaves the name alone. Note that `mix test` (no `--no-start`) has
# already started the application by the time this file runs, so this comes after the app's boot, not
# before it. The name is derived from the OS pid, so a second `mix test` on the same host (another
# worktree) does not fight this one for it in epmd.
:ok = Malachi.Test.Distribution.ensure_started()

# Record every message a long-lived server drops (Malachi.UnexpectedMessage) from here on, the application's
# own servers included, so the run can fail on any drop no test asked for (see the check at the end).
{:ok, _} = Application.ensure_all_started(:telemetry)
:ok = Malachi.Test.UnknownMessages.start_guard()

# Start the application for all tests
# Individual tests handle their own state isolation via setup/on_exit blocks
{:ok, _} = Application.ensure_all_started(:malachi)

# The rules table the storage-failure tests inject faults through. Scoped by directory, so async tests
# never see each other's rules.
:ok = Malachi.Test.FaultySegmentStore.start()

# A drop nobody asked for is what used to be a crash, and the catch-alls would otherwise let it pass
# silently: ExUnit has no way to fail a run from here once every test passed, so the VM exits with a
# failing status instead. Registered BEFORE the cleanup below, because `after_suite` callbacks run in
# REVERSE registration order (`ExUnit.after_suite/1`): registering this last would halt the VM before the
# cleanup ran and leave the run's data directories behind.
ExUnit.after_suite(fn _result ->
  case Malachi.Test.UnknownMessages.report_guard() do
    :ok -> :ok
    {:error, _violations} -> System.halt(1)
  end
end)

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
#
# Malachi and its harnesses target Linux only, and `:linux` tests drive them with Linux tools (coreutils
# `timeout`, util-linux). They run wherever the suite runs on Linux, CI included; on any other host they
# are excluded rather than rewritten for it.
linux_only = if :os.type() == {:unix, :linux}, do: [], else: [:linux]
ExUnit.start(exclude: [:multinode | linux_only])
