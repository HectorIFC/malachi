defmodule Mix.Tasks.Malachi.Docs.Site do
  @shortdoc "Stages the landing page as doc/index.html, or checks that it is staged"

  @moduledoc """
  #{@shortdoc}, with its fonts, `llms.txt`, `robots.txt` and `sitemap.xml`.

      mix malachi.docs.site          # stage into doc/, after mix docs has written it
      mix malachi.docs.site --check  # fail when a check of the staged tree fails

  Not wired yet: neither the `docs` alias nor CI runs this task. The page source is `site/`, which arrives
  with the design kit (#263), and the wiring arrives with it. Every value the page takes (the version, the
  licence, the base URL) comes from `mix.exs` through `Malachi.Site`, which says what is staged and what
  `--check` proves.

  `--root` points the task at another checkout. It exists for the tests; everywhere else the task runs from
  the repository root without it.
  """

  use Mix.Task

  alias Malachi.Site

  @switches [check: :boolean, root: :string]

  @impl Mix.Task
  def run(argv) do
    {opts, rest} = OptionParser.parse!(argv, strict: @switches)

    if rest != [], do: Mix.raise("unexpected arguments: #{Enum.join(rest, " ")}")

    root = Keyword.get(opts, :root, File.cwd!())
    config = Site.from_project(Mix.Project.config())

    if Keyword.get(opts, :check, false), do: check(root, config), else: stage(root, config)
  end

  defp stage(root, config) do
    :ok = Site.stage(root, config)
    Mix.shell().info("staged the landing page at doc/index.html")
  end

  defp check(root, config) do
    case Site.verify(root, config) do
      [] ->
        Mix.shell().info(
          "landing page: staged and filled; every sitemap URL and every link to this site in llms.txt resolves"
        )

        :ok

      errors ->
        Mix.raise("the staged landing page is incomplete:\n\n" <> Enum.map_join(errors, "\n", &("  " <> &1)))
    end
  end
end
