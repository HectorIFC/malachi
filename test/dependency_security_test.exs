defmodule DependencySecurityTest do
  use ExUnit.Case, async: true

  @moduledoc """
  Tests to verify dependency security configuration.
  Ensures all dependencies are properly pinned and security tools are present.
  """

  test "mix.lock file exists" do
    assert File.exists?("mix.lock"),
           "mix.lock must exist for reproducible builds"
  end

  test "all runtime dependencies are pinned to patch level" do
    {:ok, content} = File.read("mix.exs")

    # Runtime dependencies (not dev/test only)
    runtime_deps = [
      ~r/{:jason, "~> \d+\.\d+\.\d+"/,
      ~r/{:argon2_elixir, "~> \d+\.\d+\.\d+"/,
      ~r/{:inet_cidr, "~> \d+\.\d+\.\d+"/
    ]

    for pattern <- runtime_deps do
      assert Regex.match?(pattern, content),
             "Runtime dependency must be pinned to patch level (e.g., ~> x.y.z): #{inspect(pattern)}"
    end
  end

  test "security tools are present in dependencies" do
    {:ok, content} = File.read("mix.exs")

    assert content =~ ~r/{:mix_audit,/,
           "mix_audit must be present in dependencies"

    assert content =~ ~r/{:sobelow,/,
           "sobelow must be present in dependencies"
  end

  test "dev/test dependencies have runtime: false" do
    {:ok, content} = File.read("mix.exs")

    dev_test_deps = ["credo", "dialyxir", "mix_audit", "sobelow"]

    for dep <- dev_test_deps do
      # Find the line with this dependency
      lines = String.split(content, "\n")

      dep_line =
        Enum.find(lines, fn line ->
          String.contains?(line, ":#{dep},")
        end)

      if dep_line do
        assert String.contains?(dep_line, "runtime: false"),
               "#{dep} should have runtime: false"
      end
    end
  end

  test "SECURITY.md exists" do
    assert File.exists?("SECURITY.md"),
           "SECURITY.md must exist with vulnerability disclosure policy"
  end
end
