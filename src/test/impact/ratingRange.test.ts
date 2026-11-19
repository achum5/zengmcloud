import { test } from "vitest";
import { IMPACT_RATINGS } from "../../worker/core/realisticFrontOffice/playerImpact.ts";
import { idb } from "../../worker/db/index.ts";
import { createArenaLeague } from "../arena/runLeague.ts";
import { loadLeagueFile } from "../smart/loadLeague.ts";

// Each impact rating's spread among rostered players: in random players'
// leagues (RR_ARENAS, arena seeds: what the roster model was fit on) and in
// league files (RR_LEAGUES) - for FIT_SUPPORT (strength.ts).
//   RR_OUT=<file> [RR_ARENAS=4242,1,2,3,4] [RR_LEAGUES=<league.json>,...] \
//     npx vitest --run --project basketball src/test/impact/ratingRange.test.ts
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

test.skipIf(!env.RR_OUT)("rating range", { timeout: 900_000 }, async () => {
	await importNode("fake-indexeddb/auto");
	const fs = await importNode("node:fs");
	const out: Record<string, Record<string, number[]>> = {};
	const sources = [
		...(env.RR_ARENAS ?? "4242").split(",").map((s) => `arena:${s}`),
		...(env.RR_LEAGUES ?? "").split(",").filter(Boolean),
	];
	for (const src of sources) {
		if (src.startsWith("arena:")) {
			await createArenaLeague(Number(src.slice(6)));
		} else {
			await loadLeagueFile(src);
		}
		const players = await idb.cache.players.indexGetAll("playersByTid", [
			0,
			Infinity,
		]);
		const by: Record<string, number[]> = {};
		for (const key of [...IMPACT_RATINGS, "ovr"]) {
			by[key] = players
				.map((p) => (p.ratings.at(-1) as any)[key])
				.sort((a, b) => a - b);
		}
		out[src] = by;
	}
	fs.writeFileSync(env.RR_OUT!, JSON.stringify(out));
});
