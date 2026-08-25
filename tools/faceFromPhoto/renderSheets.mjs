// Contact sheets for one faces.js slot, cropped to the feature so the options
// are actually comparable. This is where the "What the shapes actually look
// like" groupings in PROMPT.md come from - they were written by LOOKING at
// these, not guessed from the id names, and they should be re-derived here if
// facesjs ever changes its artwork.
//
//   node tools/faceFromPhoto/renderSheets.mjs eye
//   node tools/faceFromPhoto/renderSheets.mjs head --size 380 --cols 5 --base hair=none
//   node tools/faceFromPhoto/renderSheets.mjs facialHair --from 0 --to 24
//   node tools/faceFromPhoto/renderSheets.mjs mouth --crop 130,340,150,130
//
// Options, all optional:
//   --size N        cell height in px (default 190). The default is fine for a
//                   quick look and far too small to tell two jawlines apart -
//                   raise it when the difference you are chasing is subtle.
//   --cols N        cells per row (default: as many as fit)
//   --from N --to N slice of the id list, for slots too long to read at once
//                   (facialHair has 90). One image per chunk beats one image
//                   scaled down until every beard looks the same.
//   --crop x,y,w,h  override the window from faceCrops.ts
//   --base k=v,...  override the base face, e.g. hair=none,fatness=0.6. Values
//                   parse as numbers when they look like numbers, else as an id.
//   --label X       suffix for the output filename
//
// Needs facesjs and playwright (see README) and must run from the repo root.

import { faceToSvgString, svgsIndex } from "facesjs";
import { chromium } from "playwright";
// Same crop windows the in-game face editor uses for its thumbnails.
import { FACE_CROPS, FULL_FACE } from "../../src/common/faceCrops.ts";

const argv = process.argv.slice(2);
const slot = argv[0];
const flag = (name, fallback) => {
	const i = argv.indexOf(`--${name}`);
	return i === -1 ? fallback : argv[i + 1];
};

const OUT = process.env.OUT_DIR ?? ".";
const size = Number(flag("size", 190));
const cols = flag("cols") ? Number(flag("cols")) : undefined;
const from = Number(flag("from", 0));
const to = Number(flag("to", Infinity));
const label = flag("label", "");

const base = {
	fatness: 0.4,
	teamColors: ["#89bfd3", "#7a1319", "#07364f"],
	hairBg: { id: "none" },
	body: { id: "body3", color: "#ddb7a0", size: 1 },
	jersey: { id: "jersey" },
	ear: { id: "ear2", size: 1 },
	head: { id: "head5", shave: "rgba(0,0,0,0)" },
	eyeLine: { id: "none" },
	smileLine: { id: "none", size: 1 },
	miscLine: { id: "none" },
	facialHair: { id: "none" },
	eye: { id: "eye1", angle: 0 },
	eyebrow: { id: "eyebrow1", angle: 0 },
	hair: { id: "short", color: "#272421", flip: false },
	mouth: { id: "straight", flip: false },
	nose: { id: "nose1", flip: false, size: 1 },
	glasses: { id: "none" },
	accessories: { id: "none" },
};

// --base hair=none,fatness=0.6 - an id string for object slots, a number for
// the scalar ones.
for (const pair of (flag("base", "") || "").split(",").filter(Boolean)) {
	const [k, v] = pair.split("=");
	const n = Number(v);
	if (typeof base[k] === "object") {
		base[k] = { ...base[k], id: v };
	} else {
		base[k] = Number.isNaN(n) ? v : n;
	}
}

// The female-only options are exactly the ids named "female*" (checked against
// svgsGenders), and the package export map blocks importing that table, so
// filter by name.
const allIds = svgsIndex[slot].filter((id) => !id.startsWith("female"));
const ids = allIds.slice(from, to === Infinity ? undefined : to);
const cropArg = flag("crop");
const [x, y, w, h] = cropArg
	? cropArg.split(",").map(Number)
	: (FACE_CROPS[slot] ?? FULL_FACE);
const cw = Math.round((w / h) * size);

const cells = ids
	.map((id) => {
		const f = structuredClone(base);
		f[slot] = { ...f[slot], id };
		if (slot === "hair" && (id === "bald" || id === "short-bald"))
			f.head.shave = "rgba(0,0,0,0.1)";
		const svg = faceToSvgString(f).replace(
			'viewBox="0 0 400 600"',
			`viewBox="${x} ${y} ${w} ${h}"`,
		);
		return `<div style="text-align:center;font:13px monospace;border:1px solid #ddd"><div style="width:${cw}px;height:${size}px">${svg}</div>${id}</div>`;
	})
	.join("");

const width = cols ? cols * (cw + 8) + 24 : 1500;
const browser = await chromium.launch({
	executablePath: process.env.PW_CHROME,
	proxy: { server: "direct://" },
	args: ["--no-proxy-server"],
});
const page = await browser.newPage({ viewport: { width, height: 400 } });
await page.setContent(
	`<body style="margin:0;background:#fff"><div style="display:flex;flex-wrap:wrap;gap:6px;padding:8px">${cells}</div></body>`,
);
await page.waitForTimeout(500);
const suffix = label ? `-${label}` : from || to !== Infinity ? `-${from}` : "";
await page.screenshot({
	path: `${OUT}/sheet-${slot}${suffix}.png`,
	fullPage: true,
});
await browser.close();
console.log(slot, `${ids.length}/${allIds.length}`);
