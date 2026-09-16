defmodule Malachi.Test.BenchScript do
  @moduledoc """
  Runs a script under `benchmark/` for real, for the tests that pin what those scripts print and refuse.

  The environment is set through `env(1)` rather than `System.cmd/3`'s `:env`, because the port driver
  treats an empty value as unsetting the variable, and an empty knob is one of the inputs a script has to
  refuse. Every knob a test does not set is removed, so a value exported in the developer's shell cannot
  change what is being checked.

  Scripts run with `mix run --no-start`: the suite already runs the application, and a second one would
  fight it for its ports.
  """

  @project Path.expand("../..", __DIR__)
  @knobs ~w(BENCH_DIR BENCH_ALLOW_TMPFS SCALE_NS SCALE_BATCHES)

  @doc "The `env` and `mix` executables, failing the calling test when either is missing."
  def executables! do
    for tool <- ~w(env mix), into: %{} do
      {String.to_atom(tool), System.find_executable(tool) || ExUnit.Assertions.flunk("#{tool} is required")}
    end
  end

  @doc """
  Runs `script` (relative to the project root) with exactly the `knobs` given, as `{name, value}`
  pairs, and returns `{output, exit_status}` with stderr folded into the output.
  """
  def run(%{env: env, mix: mix}, script, knobs) do
    unknown = for {name, _} <- knobs, name not in @knobs, do: name
    if unknown != [], do: raise(ArgumentError, "not a benchmark knob: #{Enum.join(unknown, ", ")}")

    unset = for name <- @knobs, not List.keymember?(knobs, name, 0), do: ["-u", name]
    set = for {name, value} <- knobs, do: "#{name}=#{value}"
    args = List.flatten(unset) ++ ["MIX_ENV=test"] ++ set ++ [mix, "run", "--no-start", script]

    System.cmd(env, args, cd: @project, stderr_to_stdout: true)
  end
end
