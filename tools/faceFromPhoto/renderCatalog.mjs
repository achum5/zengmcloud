// Renders EVERY faces.js option, one slot at a time, as a labeled contact
// sheet. This is the "menu" a vision model picks from when matching a photo:
// instead of guessing an id like `hair.id = "crop-fade"` blind, it sees the
// thumbnail next to its label and chooses the nearest.
//
//   node tools/faceFromPhoto/renderCatalog.mjs
//   -> tools/faceFromPhoto/out/catalog.html   (open in a browser)
//
// Only the "identity-critical" slots are sheeted by default (the ones the eye
// actually reads); pass --all to include the subtle line/jersey slots too.

import { writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import { faceToSvgString, generate, svgsIndex } from "facesjs";

const outDir = join(dirname(fileURLToPath(import.meta.url)), "out");

// Slots that carry recognizable identity, most-important first. The rest
// (eyeLine, smileLine, miscLine, jersey, hairBg, body) are fine left random.
const IDENTITY_SLOTS = [
	"head",
	"hair",
	"facialHair",
	"glasses",
	"eye",
	"eyebrow",
	"nose",
	"mouth",
	"ear",
	"accessories",
];

const ALL = process.argv.includes("--all");
const slots = ALL ? Object.keys(svgsIndex) : IDENTITY_SLOTS;

// A neutral, well-formed male base so the ONLY thing changing across a sheet is
// the slot under test. Clean-shaven, plain short hair, light skin, no glasses.
const base = generate(
	{
		facialHair: { id: "none" },
		glasses: { id: "none" },
		accessories: { id: "none" },
		hair: { id: "short", color: "#4a3520" },
		body: { color: "#e8b98f" },
		fatness: 0.35,
		eye: { angle: 0 },
		eyebrow: { angle: 0 },
	},
	{ gender: "male", race: "white" },
);

const thumb = (overrides, label) => {
	const svg = faceToSvgString(base, overrides);
	return `<figure><div class="face">${svg}</div><figcaption>${label}</figcaption></figure>`;
};

let sections = "";
for (const slot of slots) {
	const ids = svgsIndex[slot];
	const cards = ids
		.map((id) => {
			// Skip female-named variants in this male-base sheet to cut noise.
			if (!ALL && id.startsWith("female")) {
				return "";
			}
			return thumb({ [slot]: { id } }, id);
		})
		.filter(Boolean)
		.join("\n");
	sections += `<section><h2>${slot} <small>(${ids.length} options)</small></h2><div class="grid">${cards}</div></section>\n`;
}

const html = `<!doctype html><meta charset="utf8"><title>faces.js option catalog</title>
<style>
 body{font:14px/1.4 system-ui,sans-serif;margin:0;background:#111;color:#eee}
 h1{padding:16px 20px;margin:0;position:sticky;top:0;background:#111;z-index:1}
 section{padding:8px 20px 24px;border-top:1px solid #333}
 h2{font-size:18px} h2 small{color:#888;font-weight:400}
 .grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(96px,1fr));gap:10px}
 figure{margin:0;background:#1c1c1c;border-radius:8px;padding:6px;text-align:center}
 .face{width:100%;aspect-ratio:2/3} .face svg{width:100%;height:100%}
 figcaption{font-size:11px;color:#bbb;margin-top:4px;word-break:break-all}
</style>
<h1>faces.js option catalog — pick the nearest per slot</h1>
${sections}`;

writeFileSync(join(outDir, "catalog.html"), html);
const total = slots.reduce((n, s) => n + svgsIndex[s].length, 0);
console.log(
	`Wrote catalog.html — ${slots.length} slots, ${total} options rendered.`,
);
