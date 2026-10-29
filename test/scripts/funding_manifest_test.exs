defmodule FundingManifestTest do
  # funding.json is read by programs (FLOSS/fund, GitHub's Sponsor button) that reject a manifest they cannot parse
  # or whose fields disagree with the project. The schema is v1.1.0 of fundingjson.org; the required fields and enums
  # below are copied from it, so a field dropped or renamed in an edit fails here rather than in a directory review.
  # The project facts (URLs, license) are read from mix.exs, not repeated, so a version or URL change in one place
  # cannot leave the manifest stale.
  use ExUnit.Case, async: true

  @root Path.expand("../..", __DIR__)

  defp manifest do
    @root |> Path.join("funding.json") |> File.read!() |> Jason.decode!()
  end

  defp funding_yml, do: @root |> Path.join(".github/FUNDING.yml") |> File.read!()
  defp security_md, do: @root |> Path.join("SECURITY.md") |> File.read!()

  # Every object the schema requires a field on, with the fields it requires.
  defp assert_required!(object, keys, where) do
    for key <- keys do
      assert Map.has_key?(object, key), "#{where} is missing the required field #{inspect(key)}"
    end
  end

  describe "required fields (schema v1.1.0)" do
    test "the root declares the entity and the funding section" do
      assert_required!(manifest(), ["entity", "funding"], "the root")
    end

    test "the entity declares its type, role, name, email, description and webpage" do
      entity = manifest()["entity"]
      assert_required!(entity, ["type", "role", "name", "email", "description", "webpageUrl"], "entity")
      assert_required!(entity["webpageUrl"], ["url"], "entity.webpageUrl")
    end

    test "each project declares its identity, URLs, licenses and tags" do
      projects = manifest()["projects"]
      assert projects != [], "a funding manifest with no project has nothing to fund"

      for project <- projects do
        assert_required!(
          project,
          ["guid", "name", "description", "webpageUrl", "repositoryUrl", "licenses", "tags"],
          "project #{inspect(project["guid"])}"
        )

        assert_required!(project["webpageUrl"], ["url"], "project webpageUrl")
        assert_required!(project["repositoryUrl"], ["url"], "project repositoryUrl")
      end
    end

    test "each channel and each plan declares what the schema requires" do
      funding = manifest()["funding"]
      assert_required!(funding, ["channels", "plans"], "funding")

      for channel <- funding["channels"] do
        assert_required!(channel, ["guid", "type"], "channel #{inspect(channel["guid"])}")
      end

      for plan <- funding["plans"] do
        assert_required!(
          plan,
          ["guid", "status", "name", "amount", "currency", "frequency", "channels"],
          "plan #{inspect(plan["guid"])}"
        )
      end
    end
  end

  describe "enumerated values and formats" do
    test "the entity type and role are in the schema's enums" do
      entity = manifest()["entity"]
      assert entity["type"] in ["individual", "group", "organisation", "other"]
      assert entity["role"] in ["owner", "steward", "maintainer", "contributor", "other"]
    end

    test "every channel type, plan status and plan frequency is in the schema's enums" do
      funding = manifest()["funding"]

      for channel <- funding["channels"] do
        assert channel["type"] in ["bank", "payment-provider", "cheque", "cash", "other"],
               "channel #{inspect(channel["guid"])} has type #{inspect(channel["type"])}"
      end

      for plan <- funding["plans"] do
        assert plan["status"] in ["active", "inactive"]
        assert plan["frequency"] in ["one-time", "weekly", "fortnightly", "monthly", "yearly", "other"]
        assert is_number(plan["amount"]) and plan["amount"] >= 0, "amount is a number, 0 meaning any amount"
        assert plan["currency"] =~ ~r/\A[A-Z]{3}\z/, "currency is an ISO 4217 code, got #{inspect(plan["currency"])}"
      end
    end
  end

  describe "identifiers and references" do
    test "every guid is unique across projects, channels and plans" do
      manifest = manifest()
      funding = manifest["funding"]

      guids =
        Enum.map(manifest["projects"], & &1["guid"]) ++
          Enum.map(funding["channels"], & &1["guid"]) ++
          Enum.map(funding["plans"], & &1["guid"])

      assert length(guids) == length(Enum.uniq(guids)), "duplicate guid in #{inspect(guids)}"
    end

    test "every plan names only channels that the manifest declares, and names at least one" do
      funding = manifest()["funding"]
      channel_guids = MapSet.new(funding["channels"], & &1["guid"])

      for plan <- funding["plans"] do
        assert plan["channels"] != [], "plan #{inspect(plan["guid"])} has no channel"

        for guid <- plan["channels"] do
          assert MapSet.member?(channel_guids, guid),
                 "plan #{inspect(plan["guid"])} refers to the undeclared channel #{inspect(guid)}"
        end
      end
    end
  end

  describe "agreement with the rest of the repository" do
    test "the project's repository, site and license are the ones mix.exs declares" do
      config = Mix.Project.config()
      project = hd(manifest()["projects"])

      assert project["repositoryUrl"]["url"] == config[:source_url]
      assert project["webpageUrl"]["url"] == config[:site_url] <> "/"
      # The schema writes a standard license as its SPDX identifier with the "spdx:" prefix (its examples use spdx:MIT).
      assert project["licenses"] == Enum.map(config[:package][:licenses], &("spdx:" <> &1))
    end

    test "names the v1.1.0 schema it follows, as the schema's own examples do" do
      assert manifest()["$schema"] == "https://fundingjson.org/schema/v1.1.0.json"
    end

    test "the entity e-mail is the one SECURITY.md publishes for reports" do
      [_, reported] = Regex.run(~r/Send details to `([^`]+)`/, security_md())
      assert manifest()["entity"]["email"] == reported
    end

    test "the GitHub Sponsors channel names the account that FUNDING.yml sponsors" do
      [_, account] = Regex.run(~r/^github: (\S+)$/m, funding_yml())
      channel = Enum.find(manifest()["funding"]["channels"], &(&1["guid"] == "github-sponsors"))

      assert channel["address"] == "https://github.com/sponsors/#{account}"
    end

    test "FUNDING.yml keeps no commented placeholder for a channel that is not used" do
      placeholder = ~r/^#\s*(patreon|open_collective|ko_fi|buy_me_a_coffee|liberapay|issuehunt|polar|custom):/m
      refute Regex.match?(placeholder, funding_yml())
    end
  end
end
