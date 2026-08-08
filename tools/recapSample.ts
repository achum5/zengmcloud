// Print auto recaps from captured real-league data, so the prose can be judged
// by reading a lot of it at once instead of by guessing.
//
//   node tools/recapSample.ts <data.json> [--days 4] [--games 12] [--from 0]
//
// The JSON is a capture of getDayGamesForRecap output:
//   [{ day, games: RecapGame[], standings: RecapDayStandings[] }, ...]
// Not part of the app; a development aid for working on getAutoRecap.

import fs from "node:fs";
import {
	beginRecapBatch,
	endRecapBatch,
	getAutoDayRecap,
	getAutoRecap,
} from "../src/worker/util/getAutoRecap.ts";

const args = process.argv.slice(2);
const file = args[0];
const flag = (name: string, fallback: number) => {
	const i = args.indexOf(`--${name}`);
	return i === -1 ? fallback : Number.parseInt(args[i + 1]!);
};
if (!file) {
	console.error("usage: node tools/recapSample.ts <data.json> [--days N]");
	process.exit(1);
}

const data = JSON.parse(fs.readFileSync(file, "utf8")) as {
	day: number;
	games: any[];
	standings: any[];
}[];

const numDays = flag("days", 3);
const numGames = flag("games", 10);
const from = flag("from", 0);

const rule = (s: string) => `\n${"=".repeat(78)}\n${s}\n${"=".repeat(78)}`;

for (const entry of data.slice(from, from + numDays)) {
	const { day, games } = entry;
	const standings = entry.standings?.find?.((s: any) => s.day === day);

	console.log(rule(`DAY ${day} - ${games.length} games`));

	beginRecapBatch();
	let dayRecap = "";
	const notes: string[] = [];
	try {
		for (const g of games) {
			notes.push(getAutoRecap(g));
		}
	} finally {
		endRecapBatch();
	}
	dayRecap = getAutoDayRecap({
		season: 2026,
		day,
		playoffs: games.some((g: any) => g.playoffs),
		games,
		standings,
	});

	console.log(`\n--- DAY WRAP ---\n${dayRecap}\n`);
	console.log(`--- ${Math.min(numGames, notes.length)} GAME RECAPS ---`);
	for (const note of notes.slice(0, numGames)) {
		console.log(`\n${note}`);
	}
}
