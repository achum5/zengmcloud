import { afterAll, assert, beforeAll, describe, test } from "vitest";
import { resetCache, resetG } from "../../test/helpers.ts";
import { simRecapLeague, withSeededRandom } from "../../test/recapLeague.ts";
import { idb } from "../db/index.ts";
import {
	getAutoRecapsForDay,
	recapGamesForDay,
	type RecapGame,
	type RecapTeam,
} from "./getDayGamesForRecap.ts";
import { verifyRecap } from "./recapAccuracy.ts";

// RECAPS OF REAL GAMES, CHECKED IN CI.
//
// The hand-built fixtures pin individual sentences; the corpus run
// (recapCorpus.test.ts) is read by a person and skipped in CI. This sits
// between them: a week of a seeded league played with the real engine,
// every recap written by the real path, and each one held to the rules a
// copy desk would hold it to - claims the game's own log can check. Each
// rule here is one a corpus read found broken.

const DAYS = 8;
const SEED = 11;

type Pair = { gid: number; recap: string; game: RecapGame };
const pairs: Pair[] = [];
let savedLeague: unknown;

beforeAll(async () => {
	savedLeague = (idb as any).league;
	await withSeededRandom(SEED, async () => {
		const { season, lastDay } = await simRecapLeague({
			days: DAYS,
			seed: SEED,
			playoffs: false,
		});
		for (let day = 1; day <= lastDay; day++) {
			const { notes } = await getAutoRecapsForDay({ season, day });
			for (const game of await recapGamesForDay({ season, day })) {
				const recap = notes[game.gid];
				if (recap) {
					pairs.push({ gid: game.gid, recap, game });
				}
			}
		}
	});
}, 120_000);

afterAll(async () => {
	(idb as any).league = savedLeague;
	resetG();
	await resetCache();
});

const sides = (game: RecapGame): { w: RecapTeam; l: RecapTeam } => {
	const [a, b] = game.teams;
	return game.winnerTid === a.tid ? { w: a, l: b } : { w: b, l: a };
};

const ORD = ["first", "second", "third", "fourth"] as const;

describe("simmed recaps", () => {
	test("there are enough of them to mean something", () => {
		assert.isAtLeast(pairs.length, 60);
	});

	test("nothing the box score and the score log contradict", () => {
		const bad = pairs.flatMap(({ gid, recap, game }) =>
			verifyRecap(recap, game).map(
				(v) => `gid ${gid} [${v.kind}] ${v.detail}: ${v.sentence}`,
			),
		);
		assert.deepStrictEqual(bad, []);
	});

	// "breaking it open with a 28-19 second quarter" of a quarter that turned
	// an eight-point deficit into a one-point halftime lead.
	test("a quarter only broke it open when the winners came out of it clear", () => {
		let seen = 0;
		for (const { gid, recap, game } of pairs) {
			const { w, l } = sides(game);
			for (const m of recap.matchAll(
				/(?:breaking it open with an?|An?) (\d+)-(\d+) (first|second|third|fourth) quarter(?: broke it open)?/g,
			)) {
				if (!/break/.test(m[0])) {
					continue;
				}
				seen += 1;
				const q = ORD.indexOf(m[3] as (typeof ORD)[number]);
				const wq = w.ptsQtrs ?? [];
				const lq = l.ptsQtrs ?? [];
				const sum = (xs: number[], n: number) =>
					xs.slice(0, n).reduce((a, b) => a + b, 0);
				const before = sum(wq, q) - sum(lq, q);
				const after = sum(wq, q + 1) - sum(lq, q + 1);
				assert.isTrue(
					before <= 6 && after >= 10,
					`gid ${gid}: ${m[0]} (lead ${before} -> ${after})\n${recap}`,
				);
			}
		}
		assert.isAtLeast(seen, 0);
	});

	// "The Nets led by as many as 14 in the first, but... The Nets led by as
	// many as 14."
	test("the losers' biggest lead is told once", () => {
		for (const { gid, recap, game } of pairs) {
			const { l } = sides(game);
			const peak = game.flow?.maxLead[game.teams[0] === l ? 0 : 1] ?? 0;
			if (peak < 10) {
				continue;
			}
			const told = [
				...recap.matchAll(
					new RegExp(
						String.raw`${l.name}[^.]*\b(?:as many as|led by|up by) ${peak}\b`,
						"g",
					),
				),
			];
			assert.isAtMost(told.length, 1, `gid ${gid}\n${recap}`);
		}
	});

	// Behind after three and won: the game was won in the fourth, and the
	// story says so with the quarter's score - not just "the second half
	// belonged to them".
	test("a game won in the last quarter says what the last quarter was", () => {
		let seen = 0;
		for (const { gid, recap, game } of pairs) {
			const { w, l } = sides(game);
			const wq = w.ptsQtrs ?? [];
			const lq = l.ptsQtrs ?? [];
			if (game.overtimes > 0 || wq.length !== 4 || lq.length !== 4) {
				continue;
			}
			const wIn = wq[0]! + wq[1]! + wq[2]!;
			const lIn = lq[0]! + lq[1]! + lq[2]!;
			if (wIn > lIn) {
				continue;
			}
			seen += 1;
			const q = `${wq[3]}-${lq[3]}`;
			assert.isTrue(
				recap.includes(`${q} fourth quarter`) ||
					recap.includes(`fourth quarter ${q}`),
				`gid ${gid}: no ${q} fourth quarter\n${recap}`,
			);
		}
		assert.isAtLeast(seen, 3);
	});
});
