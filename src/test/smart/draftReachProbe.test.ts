import { test } from "vitest";
import { makeSeededRandom } from "../../common/random.ts";
import type { Player } from "../../common/types.ts";
import { idb } from "../../worker/db/index.ts";
import { g } from "../../worker/util/index.ts";
import { projectRatings } from "../../worker/core/realisticFrontOffice/projection.ts";
import {
	advanceSeasons,
	createArenaLeague,
	phaseClock,
} from "../arena/runLeague.ts";

// How far CPU teams reach in the draft with the smart CPU on vanilla's rules:
// each pick's place on the board (the prospects left, by value) - arena
// leagues, DR_SEEDS=<seed,...> DR_SEASONS=<n> DR_OUT=<file>. Seconds in each
// phase go to <file>.clock.json. DR_CAREERS=<n> (outlookSettings.
// youngCareers) and DR_BOARD=0|1 (smartDraftSettings.scoutedBoard) for the
// draft as it was: 24 and 0.
//
// And how the picks turn out: each prospect weighed played out 400 times
// with the game's own development code (outcome: his average best overall,
// his chance of reaching 60 and 65), against the best of them still there by
// that (bestPeak). The game's potential is the 75th percentile of 20 such
// careers, and can be well off: a value board's top prospect who slides
// usually reads far better on it than he plays out.
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

const CAREERS = 400;
const outcomes = new Map<string, { peak: number; p60: number; p65: number }>();
const outcomeOf = (p: Player) => {
	const season = g.get("season");
	const r = p.ratings.at(-1)!;
	const key = `${g.get("lid")}:${season}:${p.pid}`;
	let o = outcomes.get(key);
	if (!o) {
		const age = season - p.born.year;
		const proj = projectRatings({
			ratings: r,
			age,
			years: 12,
			samples: CAREERS,
			seed: p.pid * 7 + 3,
			keepCareers: true,
			measure: () => [0],
		});
		const peaks = new Array<number>(CAREERS).fill(0);
		for (const season of proj.careers!) {
			for (const [s, c] of season.entries()) {
				peaks[s] = Math.max(peaks[s]!, c.ovr);
			}
		}
		const share = (x: number) => peaks.filter((v) => v >= x).length / CAREERS;
		o = {
			peak: Math.round((peaks.reduce((a, b) => a + b, 0) / CAREERS) * 10) / 10,
			p60: share(60),
			p65: share(65),
		};
		outcomes.set(key, o);
	}
	return o;
};

test.skipIf(!env.DR_OUT)("draft reach", { timeout: 21_600_000 }, async () => {
	await importNode("fake-indexeddb/auto");
	const fs = await importNode("node:fs");
	const { smartDraftWatchers, smartDraftSettings } =
		await import("../../worker/core/smartCpu/draft.ts");
	const { outlookSettings } =
		await import("../../worker/core/realisticFrontOffice/outlook.ts");
	if (env.DR_CAREERS) {
		outlookSettings.youngCareers = Number(env.DR_CAREERS);
	}
	if (env.DR_BOARD) {
		smartDraftSettings.scoutedBoard = env.DR_BOARD === "1";
	}
	const rows: unknown[] = [];
	let seed = 0;
	smartDraftWatchers.push((dp, worths) => {
		const choice = worths[0]!;
		const board = [...worths].sort(
			(a, b) => b.p.value - a.p.value || a.p.pid - b.p.pid,
		);
		const rank = board.indexOf(choice) + 1;
		const r = (p: any) => p.ratings.at(-1);
		const bestPeak = Math.max(...worths.map((x) => outcomeOf(x.p).peak));
		rows.push({
			seed,
			season: g.get("season"),
			round: dp.round,
			pick: dp.pick,
			tid: dp.tid,
			rank,
			bestPeak,
			chosen: {
				pid: choice.p.pid,
				age: g.get("season") - choice.p.born.year,
				ovr: r(choice.p).ovr,
				pot: r(choice.p).pot,
				value: choice.p.value,
				...outcomeOf(choice.p),
			},
			top: {
				pid: board[0]!.p.pid,
				age: g.get("season") - board[0]!.p.born.year,
				ovr: r(board[0]!.p).ovr,
				pot: r(board[0]!.p).pot,
				value: board[0]!.p.value,
				...outcomeOf(board[0]!.p),
			},
		});
		fs.writeFileSync(
			env.DR_OUT!,
			rows.map((x) => JSON.stringify(x)).join("\n") + "\n",
		);
	});
	for (const s of (env.DR_SEEDS ?? "9177").split(",").map(Number)) {
		seed = s;
		Math.random = makeSeededRandom(s);
		(idb as any).league = undefined;
		await createArenaLeague(s);
		const { league } = await import("../../worker/core/index.ts");
		await league.setGameAttributes({
			realisticFrontOffices: false,
			smartCpu: true,
		});
		await advanceSeasons(Number(env.DR_SEASONS ?? 2));
		fs.writeFileSync(`${env.DR_OUT}.clock.json`, JSON.stringify(phaseClock));
	}
});
