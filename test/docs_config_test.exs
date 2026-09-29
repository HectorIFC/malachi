defmodule DocsConfigTest do
  # The ExDoc sidebar is built from `docs:` in mix.exs. ExDoc draws every extra that no group claims
  # above the first group, and on load it scrolls the sidebar so the current page is at the top, which
  # for the landing page (Introduction, the first guide) hides whatever sits above it. The Docker Hub
  # link sat there, ungrouped, and was visible only after scrolling the sidebar back up.
  use ExUnit.Case, async: true

  @docker_hub "https://hub.docker.com/r/hectorcardoso/malachi"

  # Read at run time: the config holds an anonymous function (the before_closing_body_tag hook), which a
  # module attribute cannot carry.
  defp docs, do: Mix.Project.config()[:docs]

  # What a group entry is matched against: the path for a page, the url for an external link (ExDoc's
  # Config.match_extra compares a URLNode's url with the pattern).
  defp pattern({key, opts}), do: Keyword.get(opts, :url, to_string(key))

  test "every extra belongs to a group, so none is drawn above the guides" do
    grouped = docs()[:groups_for_extras] |> Keyword.values() |> List.flatten()

    ungrouped = docs()[:extras] |> Enum.map(&pattern/1) |> Enum.reject(&(&1 in grouped))

    assert ungrouped == []
  end

  test "the Docker Hub link is listed right after the introduction, in the guides and in the extras" do
    guides = docs()[:groups_for_extras][:Guides]
    assert Enum.take(guides, 2) == ["docs/guides/introduction.md", @docker_hub]

    order = Enum.map(docs()[:extras], &pattern/1)
    introduction = Enum.find_index(order, &(&1 == "docs/guides/introduction.md"))
    assert Enum.at(order, introduction + 1) == @docker_hub
  end
end
