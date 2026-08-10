# Benchmark results page

A self-contained static page that renders one load-test result: the throughput headline, the latency
percentile curve (P50 to P99.99), and the reproduce metadata. No build step and no dependencies: `index.html`
inlines its CSS and JS and draws the chart as inline SVG, reading `data.json` next to it.

The Pages workflow copies this directory into the published docs at `benchmarks/`, so it is served at
`https://hectorifc.github.io/malachi/benchmarks/`.

## Regenerate `data.json`

`data.json` is one run of `scripts/loadtest.js --json` (its `meta` block already records the command, git ref,
version, and hardware, so the result is self-describing). To refresh it, start a server and capture a run:

```bash
# one terminal
MIX_ENV=dev mix run --no-halt

# another terminal
MALACHI_USER=admin MALACHI_PASS=admin123 \
  node scripts/loadtest.js --scenario produce --connections 8 --duration 6 --batch 10 --json \
  > benchmark/dashboard/data.json
```

Any scenario works (`produce`, `fetch`, `mixed`); `stream` reports throughput only (no latency block), so the
chart is skipped for it.

## Preview locally

`index.html` fetches `data.json`, which the browser blocks over `file://`, so serve the directory over HTTP:

```bash
python3 -m http.server 8000 --directory benchmark/dashboard
# then open http://localhost:8000/
```
