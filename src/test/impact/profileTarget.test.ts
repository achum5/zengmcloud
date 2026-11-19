import { test } from "vitest";
import {
	playerLine,
	strengthSettings,
	typicalRatings,
	valueOverReplacement,
	type ValueRatings,
} from "../../worker/core/realisticFrontOffice/strength.ts";
import { idb } from "../../worker/db/index.ts";
import { loadLeagueFile } from "../smart/loadLeague.ts";

// What a league's players of each overall look like to the roster model,
// against an even profile of that overall (strength.ts, typicalRatings):
// each player's line, believed in full, less the even profile's - for
// AVERAGE_OVER_EVEN (strength.ts), fit on Leagues 10, 11 and 34.
//   PT_LEAGUES=<league.json>,... PT_OUT=<file> npx vitest --run \
//     --project basketball src/test/impact/profileTarget.test.ts
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

test.skipIf(!env.PT_LEAGUES)(
	"profile target",
	{ timeout: 600_000 },
	async () => {
		await importNode("fake-indexeddb/auto");
		const fs = await importNode("node:fs");
		strengthSettings.profile = 1;
		strengthSettings.profileSynergy = false;
		const out: any[] = [];
		for (const league of env.PT_LEAGUES!.split(",")) {
			await loadLeagueFile(league);
			const players = await idb.cache.players.indexGetAll("playersByTid", [
				-1,
				Infinity,
			]);
			for (const p of players) {
				const r = p.ratings.at(-1)! as unknown as ValueRatings;
				const raw = playerLine(r, p.ratings.at(-1)!.pos);
				const typ = playerLine(typicalRatings(r.ovr), p.ratings.at(-1)!.pos);
				out.push({
					league,
					tid: p.tid,
					ovr: r.ovr,
					pos: p.ratings.at(-1)!.pos,
					roleFree: raw.roleFree - typ.roleFree,
					offense: raw.offense - typ.offense,
					usage: raw.usage - typ.usage,
					vor: valueOverReplacement(raw) - valueOverReplacement(typ),
					synergy: Object.fromEntries(
						Object.entries(raw.synergy).map(([k, v]) => [
							k,
							(v as number) - (typ.synergy as any)[k],
						]),
					),
				});
			}
		}
		fs.writeFileSync(env.PT_OUT!, JSON.stringify(out));
	},
);
