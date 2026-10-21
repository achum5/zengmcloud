import { test } from "vitest";
import { makeSeededRandom } from "../../common/random.ts";
import { idb } from "../../worker/db/index.ts";
import { g } from "../../worker/util/index.ts";
import {
	buildOutlook,
	franchiseValue,
	lineMeasure,
	PATIENCE,
	retentionOf,
} from "../../worker/core/realisticFrontOffice/outlook.ts";
import { DEFAULT_LEVEL } from "../../common/budgetLevels.ts";
import { last } from "../../common/utils.ts";
import genPlayersWithoutSaving from "../../worker/core/draft/genPlayersWithoutSaving.ts";
import { projectRatings } from "../../worker/core/realisticFrontOffice/projection.ts";
import {
	playerValue,
	REPLACEMENT_OVR,
} from "../../worker/core/realisticFrontOffice/strength.ts";
import {
	bestDealWith,
	loadTeam,
} from "../../worker/core/realisticFrontOffice/market.ts";
import { league } from "../../worker/core/index.ts";
import { createArenaLeague } from "../arena/runLeague.ts";

// A look inside the new front offices' heads, on an arena league. Never
// part of the normal test run:
//
//   FO_OUTLOOK=/tmp/fo npx vitest --run --project basketball src/test/frontOffice/frontOffice.test.ts

const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};

const importNode = (name: string) => import(/* @vite-ignore */ name);

test.runIf(env.FO_OUTLOOK !== undefined)(
	"front office: league outlook",
	{ timeout: 60 * 60 * 1000 },
	async () => {
		await importNode("fake-indexeddb/auto");
		const realRandom = Math.random;
		Math.random = makeSeededRandom(1);
		try {
			await createArenaLeague(1);
			await league.setGameAttributes({ realisticFrontOffices: true });
			const started = Date.now();
			const outlook = await buildOutlook();
			const seconds = (Date.now() - started) / 1000;
			const teams = await idb.cache.teams.getAll();
			const rows = [...outlook.teams.values()]
				.map((o) => ({
					abbrev: teams.find((t) => t.tid === o.tid)!.abbrev,
					strength: o.strength.map((x) => Math.round(x * 10) / 10),
					title: o.title.map((x) => Math.round(x * 1000) / 10),
					value: Math.round(franchiseValue(outlook, o.strength) * 1000) / 1000,
				}))
				.sort((a, b) => b.value - a.value);
			const fs = await importNode("node:fs/promises");
			await fs.mkdir(env.FO_OUTLOOK!, { recursive: true });
			await fs.writeFile(
				`${env.FO_OUTLOOK}/outlook.json`,
				JSON.stringify(
					{
						season: g.get("season"),
						seconds,
						titleCurves: outlook.titleCurves,
						rows,
						// Chance of re-signing with his own team, by overall.
						retention: (
							await idb.cache.players.indexGetAll("playersByTid", [0, Infinity])
						).map((p) => [
							last(p.ratings).ovr,
							retentionOf(outlook, p.pid, p.tid),
						]),
					},
					null,
					1,
				),
			);
		} finally {
			Math.random = realRandom;
		}
	},
);

// What a pick turns into, by slot: generated draft classes under the arena's
// settings, each prospect's career played out with the game's own development,
// the class taken in order of what each prospect adds over the minimum,
// discounted. Writes picks.json, the source of the table in pickValue.ts:
//
//   FO_PICKS=/tmp/fo FO_CLASSES=50 npx vitest --run --project basketball src/test/frontOffice/frontOffice.test.ts
test.runIf(env.FO_PICKS !== undefined)(
	"front office: pick values",
	{ timeout: 6 * 60 * 60 * 1000 },
	async () => {
		await importNode("fake-indexeddb/auto");
		const realRandom = Math.random;
		Math.random = makeSeededRandom(Number(env.FO_SEED ?? 1));
		try {
			await createArenaLeague(1);
			const season = g.get("season");
			const numClasses = Number(env.FO_CLASSES ?? 50);
			const numPicks = g.get("numDraftRounds") * g.get("numActiveTeams");
			const years = 10;
			// Per pick and season after the draft: the line summed over the
			// players taken there who are worth a roster place then, and how many
			// are - the rest are cut.
			const sums = Array.from({ length: numPicks }, () =>
				Array.from({ length: years + 1 }, () => [0, 0, 0, 0, 0]),
			);
			for (let c = 0; c < numClasses; c++) {
				const prospects = await genPlayersWithoutSaving(
					season,
					DEFAULT_LEVEL,
					[],
				);
				const board = prospects
					.map((p, i) => {
						const projection = projectRatings({
							ratings: last(p.ratings),
							age: season - p.born.year,
							years,
							samples: 16,
							seed: c * 1000 + i,
							measure: lineMeasure,
						});
						let score = 0;
						for (let y = 0; y <= years; y++) {
							const [roleFree, offense] = projection.measures[y]!;
							score +=
								PATIENCE ** y *
								Math.max(
									0,
									playerValue({ roleFree: roleFree!, offense: offense! }),
								);
						}
						return { projection, score };
					})
					.sort((a, b) => b.score - a.score);
				for (let k = 0; k < numPicks && k < board.length; k++) {
					for (let y = 0; y <= years; y++) {
						const { measures, ovr } = board[k]!.projection;
						if (ovr[y]! < REPLACEMENT_OVR) {
							continue;
						}
						const row = sums[k]![y]!;
						for (let i = 0; i < 3; i++) {
							row[i]! += measures[y]![i]!;
						}
						row[3]! += ovr[y]!;
						row[4]! += 1;
					}
				}
			}
			const round = (x: number) => Math.round(x * 1000) / 1000;
			const fs = await importNode("node:fs/promises");
			await fs.mkdir(env.FO_PICKS!, { recursive: true });
			await fs.writeFile(
				`${env.FO_PICKS}/picks.json`,
				JSON.stringify({
					numClasses,
					// Per pick, per season after the draft: roleFree, offense, usage
					// and overall of the players worth a roster place, and the share
					// of players taken there who are.
					picks: sums.map((rows) =>
						rows.map((row) => {
							const kept = row[4]!;
							return kept > 0
								? [
										...row.slice(0, 4).map((x) => round(x / kept)),
										round(kept / numClasses),
									]
								: [0, 0, 0, 0, 0];
						}),
					),
				}),
			);
		} finally {
			Math.random = realRandom;
		}
	},
);

// How long the market takes, piece by piece, on an arena league - and
// what deals it would make. Writes profile.json:
//
//   FO_PROFILE=/tmp/fo npx vitest --run --project basketball src/test/frontOffice/frontOffice.test.ts
test.runIf(env.FO_PROFILE !== undefined)(
	"front office: profile the market",
	{ timeout: 60 * 60 * 1000 },
	async () => {
		await importNode("fake-indexeddb/auto");
		const realRandom = Math.random;
		Math.random = makeSeededRandom(1);
		try {
			await createArenaLeague(1);
			let started = Date.now();
			const outlook = await buildOutlook();
			const outlookSeconds = (Date.now() - started) / 1000;
			started = Date.now();
			const teams = await idb.cache.teams.getAll();
			const states = [];
			for (const t of teams) {
				states.push(await loadTeam(outlook, t.tid));
			}
			const loadSeconds = (Date.now() - started) / 1000;
			const single = new Map<string, number>();
			const pairs = [];
			for (let i = 1; i <= 10; i++) {
				started = Date.now();
				const deal = bestDealWith(outlook, states[0]!, states[i]!, single);
				pairs.push({
					tids: [states[0]!.tid, states[i]!.tid],
					seconds: (Date.now() - started) / 1000,
					deal,
				});
			}
			const fs = await importNode("node:fs/promises");
			await fs.mkdir(env.FO_PROFILE!, { recursive: true });
			await fs.writeFile(
				`${env.FO_PROFILE}/profile.json`,
				JSON.stringify({ outlookSeconds, loadSeconds, pairs }, null, 1),
			);
		} finally {
			Math.random = realRandom;
		}
	},
);
