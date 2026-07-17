// Turns a "perception spec" (what the color-sampler + vision step produce for
// one photo) into a real faces.js FaceConfig, renders it, and writes a preview.
//
//   node tools/faceFromPhoto/buildFace.mjs <spec.json>
//   -> out/<name>.svg           the face on its own (this is what p.face renders)
//   -> out/<name>.face.json     the FaceConfig to store on the player
//   -> out/<name>.html          photo (if given) beside the generated face
//
// The spec is the ONLY thing a real pipeline needs to produce per player:
//   {
//     "name": "mike-miller",
//     "photo": "mike.jpg",              // optional, for the side-by-side
//     "gender": "male", "race": "white",// seeds coherent defaults for unset slots
//     "colors": { "skin": "#e8b98f", "hair": "#4a3520", "shave": "rgba(0,0,0,0)" },
//     "shape":  { "fatness": 0.3, "nose": 1.0, "ear": 0.9 },
//     "slots":  { "head":"head7", "hair":{"id":"short"}, "facialHair":"none",
//                 "glasses":"none", "eye":"eye8", "eyebrow":"eyebrow3",
//                 "nose":"nose5", "mouth":"smile-closed", "accessories":"none" }
//   }
// `colors`/`shape` map to free numeric/hex fields (set exactly, no quantizing);
// `slots` are the discrete menu picks. Anything omitted stays as generate()'s
// coherent random default, so faces never come out malformed.

import { readFileSync, writeFileSync, existsSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join, resolve } from "node:path";
import { faceToSvgString, generate } from "facesjs";

const here = dirname(fileURLToPath(import.meta.url));
const outDir = join(here, "out");

const specPath = process.argv[2];
if (!specPath) {
	console.error("usage: node buildFace.mjs <spec.json>");
	process.exit(1);
}
const spec = JSON.parse(readFileSync(resolve(specPath), "utf8"));
const name = spec.name ?? "face";

// Normalize a slot value: accept either "head7" (id only) or {id, angle, ...}.
const asObj = (v) => (typeof v === "string" ? { id: v } : v);

// Build the override object from the perception spec.
const overrides = {};
for (const [slot, v] of Object.entries(spec.slots ?? {})) {
	overrides[slot] = asObj(v);
}
if (spec.colors?.skin) {
	overrides.body = { ...(overrides.body ?? {}), color: spec.colors.skin };
}
if (spec.colors?.hair) {
	overrides.hair = { ...(overrides.hair ?? {}), color: spec.colors.hair };
}
if (spec.colors?.shave !== undefined) {
	overrides.head = { ...(overrides.head ?? {}), shave: spec.colors.shave };
}
if (spec.shape?.fatness !== undefined) {
	overrides.fatness = spec.shape.fatness;
}
if (spec.shape?.nose !== undefined) {
	overrides.nose = { ...(overrides.nose ?? {}), size: spec.shape.nose };
}
if (spec.shape?.ear !== undefined) {
	overrides.ear = { ...(overrides.ear ?? {}), size: spec.shape.ear };
}

// generate() fills every unset slot with a coherent default; our overrides then
// stamp in the identity we perceived. One call, deterministic given the spec.
const face = generate(overrides, {
	gender: spec.gender ?? "male",
	race: spec.race ?? "white",
});

const svg = faceToSvgString(face);
writeFileSync(join(outDir, `${name}.svg`), svg);
writeFileSync(join(outDir, `${name}.face.json`), JSON.stringify(face, null, 2));

// Side-by-side preview. Embeds the reference photo as a data URI if present, so
// the HTML is self-contained and viewable anywhere.
let photoImg = "<div class='ph'>no photo provided</div>";
const photoPath = spec.photo ? resolve(dirname(resolve(specPath)), spec.photo) : undefined;
if (photoPath && existsSync(photoPath)) {
	const ext = photoPath.split(".").pop().toLowerCase();
	const mime = ext === "png" ? "image/png" : ext === "webp" ? "image/webp" : "image/jpeg";
	const b64 = readFileSync(photoPath).toString("base64");
	photoImg = `<img src="data:${mime};base64,${b64}" alt="reference">`;
}

const html = `<!doctype html><meta charset="utf8"><title>${name} — photo vs faces.js</title>
<style>
 body{font:14px/1.5 system-ui,sans-serif;margin:0;background:#111;color:#eee;padding:20px}
 h1{font-size:18px;margin:0 0 16px}
 .row{display:flex;gap:20px;flex-wrap:wrap;align-items:flex-start}
 .col{background:#1c1c1c;border-radius:12px;padding:12px;width:280px}
 .col h2{font-size:13px;color:#9ab;margin:0 0 8px;text-transform:uppercase;letter-spacing:.05em}
 img,.face svg{width:256px;height:auto;border-radius:8px;display:block}
 .ph{width:256px;height:256px;display:grid;place-items:center;color:#666;background:#151515;border-radius:8px}
 pre{background:#151515;border-radius:8px;padding:10px;font-size:11px;overflow:auto;color:#cde}
</style>
<h1>${name}: reference photo → auto-built faces.js face</h1>
<div class="row">
 <div class="col"><h2>reference</h2>${photoImg}</div>
 <div class="col"><h2>generated</h2><div class="face">${svg}</div></div>
 <div class="col"><h2>perception spec</h2><pre>${escapeHtml(JSON.stringify(spec, null, 1))}</pre></div>
</div>`;
writeFileSync(join(outDir, `${name}.html`), html);

function escapeHtml(s) {
	return s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}

console.log(`Wrote ${name}.svg, ${name}.face.json, ${name}.html`);
