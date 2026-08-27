# Benchmark results page

A self-contained static page that renders the three recorded results: the Node.js load test, the Elixir load
test, and the chaos certification. No build step and no dependencies: `index.html` inlines its CSS and JS and
draws the latency charts as inline SVG.

It reads `loadtest-node.json`, `loadtest-elixir.json` and `chaos-node.json` from beside it. Those are copies
of `benchmark/published/`, which `mix docs` stages here (see `copy_benchmarks/1` in `mix.exs`), and which the
[Publish results](../../.github/workflows/results.yml) workflow rewrites on every push to main. So the page
is as current as the last merge.

That is the whole reason it works this way. The page used to render a single sample captured by hand into a
`data.json` of its own, regenerated only when someone remembered to dispatch a workflow, and it spent months
advertising 0.7.8 numbers while the project shipped 0.8.x. Reading the same files the generated documentation
pages read means one pipeline keeps both current, and there is no second copy to forget.

The headline number at the top is the Node.js produce rate, which is what the page has always led with. The
Elixir generator records different fields (backpressure counters, four latency percentiles rather than a full
histogram), so its section shows what that run recorded and omits the rest: a percentile that was not
measured is left out of the chart rather than plotted as zero, because a curve that dives to the floor reads
as a measured latency of zero.

A file that is missing, unreadable or half-written leaves a note in its own section. The other sections still
render, since a page that goes blank tells a reader less than one that says which part it could not load.

The Pages workflow publishes this directory at
`https://hectorifc.github.io/malachi/benchmarks/`.

## Preview locally

`mix docs` stages the page and its data together, and the browser blocks `fetch` over `file://`, so serve the
staged directory over HTTP:

```bash
mix docs
python3 -m http.server 8000 --directory doc/benchmarks
# then open http://localhost:8000/
```

Serving `benchmark/dashboard` directly shows the page with all three sections reporting that they could not
load their data, because the JSON only sits next to `index.html` after `mix docs` copies it.

## Refreshing the numbers

Nothing to do by hand. A push to main runs both load tests and the chaos drill on a CI runner and commits the
results to `benchmark/published/`, which republishes this page. To see what a change does to the numbers
before merging, open a pull request: the same workflow runs there without committing anything, because
runner-to-runner variance would otherwise put noise in every diff.

From a branch in this repository it also posts the measurements as a comment. From a fork it does not:
`GITHUB_TOKEN` is read-only there, so the comment step is skipped rather than failing the run, and the
numbers come back as the run's uploaded artifacts instead.

To measure locally instead, see the guides for
[the Node.js load test](../../docs/guides/running-the-node-loadtest.md),
[the Elixir load test](../../docs/guides/running-the-elixir-loadtest.md) and
[the chaos drills](../../docs/guides/running-chaos-drills.md).
