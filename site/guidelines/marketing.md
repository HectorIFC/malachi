# Marketing chapter: the landing page

Scope: the public landing page for Malachi (issue #263). Everything in README.md applies unless this chapter
names an exception. There are exactly three exceptions: display type, motion and illustration. Colour,
spacing, radii, borders, vocabulary, voice, state shapes and the honest-number rules are unchanged. The
console, the TUI and the workspace never use anything in this chapter.

Tokens: `tokens/marketing.css`, loaded after `styles.css`, only by the landing page.

## 1. Display type is a webfont

- **Family:** Bricolage Grotesque (variable, weight 200 to 800), by Mathieu Triay, SIL Open Font License 1.1.
- **Hosting:** self-hosted, one file: `assets/fonts/bricolage-grotesque-latin-wght-normal.woff2` (41 KB, Latin
  subset, weight axis only), from Fontsource. No font CDN, no Google Fonts request.
- **Loading:** `font-display: swap`, `<link rel="preload" as="font" crossorigin>` on the page that uses it.
  The fallback is `--font-sans`, so a blocked file costs looks, never text.
- **Where:** `h1`, `h2`, `h3` of the landing page only, through `--font-display` at `--weight-display` (700).
- **Not:** body copy, buttons, labels, navigation, tables, numbers. Body stays on `--font-sans`; every
  quantity, identifier and command stays on `--font-mono` with tabular numerals.

## 2. Motion

The console rule (nothing above 150 ms, only on operator action) still holds for controls: buttons, tabs,
links, inputs. Scenes are the exception.

- **Allowed in every section:** scenes that run on their own while visible, scenes started by scrolling into
  view, and hover (or focus) animations on cards and illustrations.
- **A scene** is a canvas, SVG or HTML element that draws the model. It runs only while at least part of it
  is on screen and the tab is visible; off screen it is stopped, not throttled. Frame cap
  `--scene-fps` (30). Durations up to `--scene-loop-max` (12 s) per loop; easing `--ease-standard`.
- **Pause:** no visible button. Every scene that runs on its own is the control (WCAG 2.2.2): it is
  focusable (`tabindex="0"`, `role="img"`), its `aria-label` carries its description plus "Click or press
  Space to pause" ("... to play" when paused), and a click, Space or Enter pauses and resumes it. Focus is
  drawn with `--ring`. Pausing one scene does not pause the others. Hover animations stop when the pointer
  or focus leaves.
- **`prefers-reduced-motion: reduce`:** no scene runs, rotates or takes focus. Each scene draws one
  fixed, deterministic frame (for the hero: the first story, node-2 unknown, all three lines visible).
- **Rotation:** the hero hook rotates three stories. All three are in the HTML; rotation only changes
  which one is visible. Without JavaScript all three show, stacked. Hover animations are off. Changing the preference while the page is open switches mode.
- **No JavaScript:** every scene's text is already in the HTML in its final state; canvases stay blank next
  to their captions.
- **Never:** video files, parallax, scroll-jacking, number roll-ups, text drawn only inside a canvas, motion
  behind body text.

## 3. Illustration and generative art

Generative art means drawings computed from a model (the thesis canvas, the 3D topic model, the
how-it-works and comparison scenes).

- **May:** any section of the landing page, as an illustration of how Malachi or a compared system works,
  next to the text it illustrates. It needs no visible label; it carries an accessible description
  (`role="img"` with `aria-label`, or `aria-describedby` pointing at its caption).
- **May not:** behind text, as a page or section background, as texture, in the console, TUI, workspace,
  docs or benchmark pages, or anywhere a reader could take it for a measurement. No numbers are drawn.
- **Colour:** existing tokens only, with their meaning: `--state-active`, `--state-sealed`,
  `--state-behind`, `--state-damaged`, `--brand-accent` for the one thing to look at (cursor, split point),
  `--muted-foreground` for fences and boundaries, `--border` for empty space. No gradients.
- **Text:** none only inside a canvas. Labels, legends and captions are HTML next to the scene.
- **No CSS-hidden copy.** Facts taken off the page (license, runtime, the TLS and not-production-ready
  notice) go to the JSON-LD (`license`, `programmingLanguage`, `description`), the meta description and
  `llms.txt`. The only screen-reader-only text is a scene's image description.

## 4. Copy

Benefit-first and provable: short headings that say what the operator gets, each backed by the code. No
throughput or chaos numbers on the landing page (they are CI measurements); link to the benchmarks page
instead. The "in active development, not production-ready" note stays visible. Claims about other systems
must be checked against their official docs and recorded in `ui_kits/website/comparisons.md`; no logos, no
wording that suggests endorsement.

## Page budget

- **HTML + CSS + JS: 250 KB uncompressed, first party only.** No third-party scripts or libraries (the 3D
  model is drawn with a small canvas renderer, not three.js).
- **Fonts: 60 KB** on top (one woff2 today, 41 KB).
- One `h1`; legible in light and dark; no horizontal scroll at 360 px. The theme switch in the footer offers
  system, light, dark (`localStorage` key `malachi-theme`).
