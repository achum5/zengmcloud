// Contact sheets for one faces.js slot, cropped to the feature so the options
// are actually comparable. This is where the "What the shapes actually look
// like" groupings in PROMPT.md come from - they were written by looking at
// these, not guessed from the id names, and they should be re-derived here if
// facesjs ever changes its artwork.
//
//   node tools/faceFromPhoto/renderSheets.mjs eye
//
// Slots worth rendering: head, eye, eyebrow, nose, mouth, hair, facialHair.
// Needs facesjs and playwright (see README) and must run from the repo root.

import { faceToSvgString, svgsIndex } from "facesjs";
import { chromium } from "playwright";
// Same crop windows the in-game face editor uses for its thumbnails.
import { FACE_CROPS, FULL_FACE } from "../../src/common/faceCrops.ts";

const OUT = process.env.OUT_DIR ?? ".";
const slot = process.argv[2];
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
// The female-only options are exactly the ids named "female*" (checked against
// svgsGenders), and the package export map blocks importing that table, so
// filter by name.
const ids = svgsIndex[slot].filter((id) => !id.startsWith("female"));
const [x, y, w, h] = FACE_CROPS[slot] ?? FULL_FACE;
const cw = Math.round((w / h) * 190);

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
		return `<div style="text-align:center;font:11px monospace;border:1px solid #ddd"><div style="width:${cw}px;height:190px">${svg}</div>${id}</div>`;
	})
	.join("");

const browser = await chromium.launch({
	executablePath: process.env.PW_CHROME,
	proxy: { server: "direct://" },
	args: ["--no-proxy-server"],
});
const page = await browser.newPage({ viewport: { width: 1500, height: 400 } });
await page.setContent(
	`<body style="margin:0;background:#fff"><div style="display:flex;flex-wrap:wrap;gap:6px;padding:8px">${cells}</div></body>`,
);
await page.waitForTimeout(500);
await page.screenshot({ path: `${OUT}/sheet-${slot}.png`, fullPage: true });
await browser.close();
console.log(slot, ids.length);
