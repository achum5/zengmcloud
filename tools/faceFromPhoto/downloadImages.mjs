// Downloads every player's photo (imgURL) out of an exported BBGM league file
// into a folder, named by srID, so you can attach batches to a Claude chat.
// Run this on YOUR machine — the image host is usually blocked from sandboxes.
//
//   node tools/faceFromPhoto/downloadImages.mjs <league-export.json> [out-dir]
//
// Resumable: files that already exist are skipped, so you can re-run after an
// interruption. Prints a manifest (srID, name) you can keep alongside a batch.

import { readFileSync, writeFileSync, existsSync, mkdirSync } from "node:fs";
import { resolve, join } from "node:path";

const [leaguePath, outDirArg] = process.argv.slice(2);
if (!leaguePath) {
	console.error(
		"usage: node downloadImages.mjs <league-export.json> [out-dir]",
	);
	process.exit(1);
}
const outDir = resolve(outDirArg ?? "player-images");
mkdirSync(outDir, { recursive: true });

const league = JSON.parse(readFileSync(resolve(leaguePath), "utf8"));
const players = (league.players ?? []).filter((p) => p.srID && p.imgURL);
console.log(`${players.length} players have both an srID and an imgURL.`);

const extOf = (url) => {
	const m = /\.(jpe?g|png|webp|gif)(?:$|\?)/i.exec(url);
	return m ? m[1].toLowerCase().replace("jpeg", "jpg") : "jpg";
};

const manifest = [];
let got = 0;
let skipped = 0;
let failed = 0;

// Small concurrency so a full-history download doesn't hammer the host.
const queue = [...players];
const worker = async () => {
	while (queue.length > 0) {
		const p = queue.shift();
		const file = join(outDir, `${p.srID}.${extOf(p.imgURL)}`);
		manifest.push({
			srID: p.srID,
			name: `${p.firstName ?? ""} ${p.lastName ?? ""}`.trim(),
		});
		if (existsSync(file)) {
			skipped += 1;
			continue;
		}
		try {
			const res = await fetch(p.imgURL);
			if (!res.ok) {
				throw new Error(`HTTP ${res.status}`);
			}
			const buf = Buffer.from(await res.arrayBuffer());
			writeFileSync(file, buf);
			got += 1;
			if (got % 50 === 0) {
				console.log(`  …${got} downloaded`);
			}
		} catch (error) {
			failed += 1;
			console.warn(`  ! ${p.srID} (${p.imgURL}): ${error.message}`);
		}
	}
};

await Promise.all(Array.from({ length: 6 }, worker));

writeFileSync(
	join(outDir, "_manifest.json"),
	JSON.stringify(manifest, null, 1),
);
console.log(
	`Done: ${got} downloaded, ${skipped} already present, ${failed} failed.`,
);
console.log(
	`Images in ${outDir} (named <srID>.<ext>); manifest at _manifest.json.`,
);
