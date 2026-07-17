// Batch-converts a { "<srID>": spec } file — what a Claude chat hands back for
// a batch of players — into the { "<srID>": FaceConfig } dataset that
// applyFaces.mjs consumes. Every id is validated; anything bad falls back to a
// coherent default rather than breaking. Accepts either compact specs OR
// already-full FaceConfigs (passed through), so it's safe to run on mixed input.
//
//   node tools/faceFromPhoto/specsToFaces.mjs <specs.json> [faces-out.json]

import { readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { specToFace } from "./specToFace.mjs";

const [specsPath, outArg] = process.argv.slice(2);
if (!specsPath) {
	console.error("usage: node specsToFaces.mjs <specs.json> [faces-out.json]");
	process.exit(1);
}

const specs = JSON.parse(readFileSync(resolve(specsPath), "utf8"));
const faces = {};
let ok = 0;
let warned = 0;

// A value that already looks like a full FaceConfig (has fatness + head) is
// taken as-is; otherwise it's a compact spec to expand.
const isFullFace = (v) =>
	v && typeof v === "object" && "fatness" in v && "head" in v && "body" in v;

for (const [srID, spec] of Object.entries(specs)) {
	if (isFullFace(spec)) {
		faces[srID] = spec;
		ok += 1;
		continue;
	}
	let hadWarning = false;
	faces[srID] = specToFace(spec, {
		onWarn: (m) => {
			hadWarning = true;
			console.warn(`  ! ${srID}: ${m}`);
		},
	});
	ok += 1;
	if (hadWarning) {
		warned += 1;
	}
}

const outPath = outArg ? resolve(outArg) : resolve(specsPath).replace(/\.json$/, ".faces.json");
writeFileSync(outPath, JSON.stringify(faces));
console.log(`Converted ${ok} players (${warned} had id warnings).`);
console.log(`Wrote ${outPath} — feed this to applyFaces.mjs.`);
