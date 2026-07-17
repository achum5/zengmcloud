// Mass-applies a faces.js dataset to an exported BBGM league file, matching
// players by their Basketball-Reference id (srID). For every player that has a
// face in the dataset, its `face` is overwritten and any `imgURL` photo is
// cleared (so the cartoon face is the one that shows). Nothing else on the
// player is touched.
//
//   node tools/faceFromPhoto/applyFaces.mjs <league-export.json> <faces-by-srid.json> [out.json]
//
// The dataset is a plain { "<srID>": FaceConfig } map — league-independent, so
// the same file re-skins any league or re-import. This tool needs NO app
// changes: export your league, run this, re-import the result.

import { readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";

const [leaguePath, datasetPath, outPathArg] = process.argv.slice(2);
if (!leaguePath || !datasetPath) {
	console.error(
		"usage: node applyFaces.mjs <league-export.json> <faces-by-srid.json> [out.json]",
	);
	process.exit(1);
}

const league = JSON.parse(readFileSync(resolve(leaguePath), "utf8"));
const faces = JSON.parse(readFileSync(resolve(datasetPath), "utf8"));

const players = Array.isArray(league.players) ? league.players : [];
if (players.length === 0) {
	console.error(
		"No players array found in the league file — is this a full league export?",
	);
	process.exit(1);
}

let applied = 0;
let noSrID = 0;
let noMatch = 0;
for (const p of players) {
	if (!p.srID) {
		noSrID += 1;
		continue;
	}
	const face = faces[p.srID];
	if (!face) {
		noMatch += 1;
		continue;
	}
	p.face = face;
	// A photo URL would render instead of the cartoon face, so drop it.
	delete p.imgURL;
	delete p.imgURLSmall;
	applied += 1;
}

const outPath = outPathArg
	? resolve(outPathArg)
	: resolve(leaguePath).replace(/\.json$/, ".faces.json");
writeFileSync(outPath, JSON.stringify(league));

console.log(`Players: ${players.length}`);
console.log(`  faces applied:        ${applied}`);
console.log(`  no srID (fictional):  ${noSrID}  (kept their face)`);
console.log(`  srID not in dataset:  ${noMatch}  (kept their face)`);
console.log(`Wrote ${outPath} — re-import this into BBGM.`);
