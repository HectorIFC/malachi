// Browser checks for the landing page, run against the staged doc/ served over HTTP. Each check prints one line;
// any failure exits 1, so the CI job fails. Scores come from Lighthouse, the rest from Playwright.
import * as chromeLauncher from "chrome-launcher";
import lighthouse from "lighthouse";
import { chromium } from "playwright";

const base = process.argv[2];
if (!base) {
  console.error("usage: node run.mjs <url of the staged landing page>");
  process.exit(2);
}

const MIN_SCORE = 95;
const failures = [];

function report(name, ok, detail = "") {
  console.log(`${ok ? "ok  " : "FAIL"} ${name}${detail ? `: ${detail}` : ""}`);
  if (!ok) failures.push(name);
}

async function lighthouseScores() {
  const chrome = await chromeLauncher.launch({ chromeFlags: ["--headless=new", "--no-sandbox"] });
  try {
    const result = await lighthouse(base, {
      port: chrome.port,
      output: "json",
      logLevel: "error",
      onlyCategories: ["performance", "accessibility", "best-practices", "seo"],
    });
    for (const [id, category] of Object.entries(result.lhr.categories)) {
      const score = Math.round(category.score * 100);
      report(`lighthouse ${id} >= ${MIN_SCORE}`, score >= MIN_SCORE, `${score}`);
    }
    // Below the bar, print what pulled the performance score down, so the next change can be aimed at it.
    if (Math.round(result.lhr.categories.performance.score * 100) < MIN_SCORE) {
      for (const id of ["first-contentful-paint", "largest-contentful-paint", "total-blocking-time", "cumulative-layout-shift", "speed-index"]) {
        const audit = result.lhr.audits[id];
        console.log(`  metric ${id}: ${audit.displayValue ?? audit.numericValue}`);
      }
      const worst = result.lhr.categories.performance.auditRefs
        .filter((ref) => ref.weight > 0 && (result.lhr.audits[ref.id].score ?? 1) < 0.9)
        .sort((a, b) => (1 - (result.lhr.audits[b.id].score ?? 1)) * b.weight - (1 - (result.lhr.audits[a.id].score ?? 1)) * a.weight)
        .slice(0, 8);
      for (const ref of worst) {
        const audit = result.lhr.audits[ref.id];
        console.log(`  audit ${ref.id}: score ${audit.score}, ${audit.displayValue ?? ""}`);
      }
    }
  } finally {
    await chrome.kill();
  }
}

async function browserChecks() {
  const browser = await chromium.launch();
  try {
    // 360px wide: the page must not scroll sideways.
    const narrow = await browser.newPage({ viewport: { width: 360, height: 800 } });
    await narrow.goto(base, { waitUntil: "load" });
    const overflow = await narrow.evaluate(
      () => document.documentElement.scrollWidth - document.documentElement.clientWidth,
    );
    report("no horizontal scroll at 360px", overflow <= 0, `overflow ${overflow}px`);
    await narrow.close();

    // JavaScript off: every heading and paragraph of the page must still be visible.
    const noScript = await browser.newContext({ javaScriptEnabled: false });
    const plain = await noScript.newPage();
    await plain.goto(base, { waitUntil: "load" });
    const headings = plain.locator("h1, h2, h3");
    const paragraphs = plain.locator("main p");
    const hidden = [];
    for (const locator of [headings, paragraphs]) {
      const count = await locator.count();
      for (let i = 0; i < count; i++) {
        if (!(await locator.nth(i).isVisible())) hidden.push(await locator.nth(i).innerText().catch(() => "?"));
      }
    }
    report("headings and paragraphs visible with JavaScript off", hidden.length === 0, hidden.join(" | ").slice(0, 200));
    await noScript.close();

    // Reduced motion: no CSS animation may keep running once the page has loaded.
    const reduced = await browser.newContext({ reducedMotion: "reduce" });
    const still = await reduced.newPage();
    await still.goto(base, { waitUntil: "load" });
    await still.waitForTimeout(1500);
    const running = await still.evaluate(
      () => document.getAnimations().filter((animation) => animation.playState === "running").length,
    );
    report("no CSS animation runs under prefers-reduced-motion", running === 0, `${running} running`);
    await reduced.close();
  } finally {
    await browser.close();
  }
}

await lighthouseScores();
await browserChecks();

if (failures.length) {
  console.error(`\n${failures.length} check(s) failed`);
  process.exit(1);
}
console.log("\nall landing page checks passed");
