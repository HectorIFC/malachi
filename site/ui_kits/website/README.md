# Website UI kit: landing page

Static HTML for the Malachi landing page (issue #263). Rules: `guidelines/marketing.md`.

- `index.html`: hook (node fails, nothing acknowledged is lost), hero, guarantees, thesis canvas, 3D topic
  model, what you get (tabs), how it works (three scenes), vs Kafka / Redpanda / Pulsar (tabs, one scene each),
  CTA, footer. All copy is in the HTML in its final state; without JavaScript tabs render stacked and canvases
  stay blank beside their captions.
- `site.js`: theme switch, tabs, copy buttons, thesis canvas, and the scene controller (`MSite.Scene`: runs
  only while visible, own pause button, end-state frame under reduced motion).
- `scenes.js`: hook, split, replicate, cursor, kafka, redpanda, pulsar scenes (canvas 2D, shapes only).
- `model.js`: the 3D topic model (small canvas renderer; drag or arrow keys to rotate; Topic / Range / Segment /
  Record highlight). Geometry follows the "Topic EN" model.
- `comparisons.md`: the official source for every claim about another system, and the claims removed.
- Weight: about 90 KB of first-party HTML, CSS and JS before fonts (budget 250 KB).

No throughput or chaos numbers on this page; the guarantees section links to the benchmarks page.
