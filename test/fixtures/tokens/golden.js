// Writes test/fixtures/tokens/golden.json from docs/design/design-tokens.json with culori, an
// implementation independent of Malachi.UI.TokenGen.
const c = require("culori");
const fs = require("fs");
const [tokensPath, outPath] = process.argv.slice(2);
const t = JSON.parse(fs.readFileSync(tokensPath, "utf8"));

const toRgb = c.converter("rgb");
const toLab = c.converter("oklab");
const clamp = (x) => Math.min(1, Math.max(0, x));
const srgb8 = (s) => {
  const rgb = toRgb(c.parse(s));
  return [rgb.r, rgb.g, rgb.b].map((v) => Math.round(clamp(v) * 255));
};
const clipped = (s) => {
  const rgb = toRgb(c.parse(s));
  return { mode: "rgb", r: clamp(rgb.r), g: clamp(rgb.g), b: clamp(rgb.b) };
};
const fromBytes = ([r, g, b]) => ({ mode: "rgb", r: r / 255, g: g / 255, b: b / 255 });
const dE = c.differenceEuclidean("oklab");
const levels = [0, 95, 135, 175, 215, 255];
const palette = [];
for (let i = 16; i < 256; i++) {
  let rgb;
  if (i < 232) {
    const n = i - 16;
    rgb = [levels[Math.floor(n / 36)], levels[Math.floor(n / 6) % 6], levels[n % 6]];
  } else {
    const v = 8 + 10 * (i - 232);
    rgb = [v, v, v];
  }
  palette.push([i, fromBytes(rgb)]);
}
const nearest = (bytes) => {
  const col = fromBytes(bytes);
  let best = null;
  for (const [i, p] of palette) {
    const d = dE(col, p);
    if (best === null || d < best[1]) best = [i, d];
  }
  return best[0];
};
// culori's own filterDeficiencyDeuter applies the matrix to gamma encoded sRGB. Machado, Oliveira and
// Fernandes (2009) define it on linear RGB, which is what the generator does, so the reference applies
// the same severity 1.0 matrix (row for row the last entry of culori's DEUTER table) through culori's
// linear sRGB and OKLab converters instead.
const toLrgb = c.converter("lrgb");
const DEUTER_1 = [0.367322, 0.860646, -0.227968, 0.280085, 0.672501, 0.047413, -0.01182, 0.04294, 0.968881];
const deuter = (color) => {
  const { r, g, b } = toLrgb(color);
  const m = DEUTER_1;
  return {
    mode: "lrgb",
    r: clamp(m[0] * r + m[1] * g + m[2] * b),
    g: clamp(m[3] * r + m[4] * g + m[5] * b),
    b: clamp(m[6] * r + m[7] * g + m[8] * b),
  };
};

const colors = [];
for (const [group, members] of Object.entries(t.color)) {
  if (group.startsWith("$")) continue;
  for (const [key, v] of Object.entries(members)) {
    if (key.startsWith("$") || !v.light) continue;
    for (const theme of ["light", "dark"]) {
      const s = v[theme];
      const bytes = srgb8(s);
      colors.push({
        path: `color.${group}.${key}`,
        theme,
        oklch: s,
        srgb8: bytes,
        clipDeltaEOK: +dE(c.parse(s), clipped(s)).toFixed(6),
        ansi256: nearest(bytes),
        deuteranopiaOklab: (() => {
          const l = toLab(deuter(fromBytes(bytes)));
          return [+l.l.toFixed(6), +l.a.toFixed(6), +l.b.toFixed(6)];
        })(),
      });
    }
  }
}

const pair = (a, b) => +c.wcagContrast(fromBytes(srgb8(a)), fromBytes(srgb8(b))).toFixed(4);
// Follows a {path} reference to the color it names, so a pair on an alias has a reference value too.
const lookup = (path, theme) => {
  const [, group, key] = path.split(".");
  const token = t.color[group][key];
  if (token[theme]) return token[theme];
  const ref = /^\{(.+)\}$/.exec(token.$value || "");
  return ref ? lookup(ref[1], theme) : undefined;
};
const contrast = [];
for (const p of t.$contrast.pairs) {
  for (const theme of ["light", "dark"]) {
    const fg = lookup(p.foreground, theme);
    const bg = lookup(p.background, theme);
    if (!fg || !bg) continue;
    contrast.push({ foreground: p.foreground, background: p.background, theme, foregroundOklch: fg, backgroundOklch: bg, ratio: pair(fg, bg) });
  }
}

const out = {
  $description:
    "Reference values computed by culori, independently of Malachi.UI.TokenGen, from the token file at the time this was written. Regenerate only by running the command below; never edit by hand.",
  $source: {
    library: `culori ${require("culori/package.json").version}`,
    command: "npm install culori@4.0.2 && node test/fixtures/tokens/golden.js docs/design/design-tokens.json test/fixtures/tokens/golden.json",
  },
  colors,
  contrast,
};
fs.writeFileSync(outPath, JSON.stringify(out, null, 2) + "\n");
console.log(colors.length, "colors,", contrast.length, "pairs");
