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

	// "Vince Carter's 11 points and 10 rebounds lead the Bulls past the
	// Celtics" in a 43-point rout where a teammate scored 20. The man the
	// lede is about was outscored by eight or more only when his line is one
	// nobody outscores, or he made the winning shot.
	test("the lede goes to the night's scorer unless the line is rare", () => {
		for (const { gid, recap, game } of pairs) {
			const { w } = sides(game);
			const [headline, , lede = ""] = recap.split("\n");
			const named = w.players
				.map((p) => ({ p, at: lede.indexOf(p.name) }))
				.filter((x) => x.at >= 0)
				.sort((a, b) => a.at - b.at)[0]?.p;
			if (!named) {
				continue;
			}
			const top = [...w.players].sort((a, b) => b.pts - a.pts)[0]!;
			const tens = [named.pts, named.reb, named.ast, named.stl, named.blk];
			const rare =
				tens.filter((x) => x >= 10).length >= 3 ||
				named.reb >= 15 ||
				named.ast >= 13 ||
				named.blk >= 6 ||
				named.stl >= 6;
			const shot =
				headline!.includes(named.name) &&
				/three|layup|jumper|free throw|hook|buzzer|winner|basket|dunk/.test(
					headline!,
				);
			if (rare || shot) {
				continue;
			}
			assert.isTrue(
				top.pts < 18 || top.pts < named.pts + 8,
				`gid ${gid}: lede ${named.name} ${named.pts} under ${top.name} ${top.pts}\n${recap}`,
			);
		}
	});

	// "Jalen King's 24 points and 10 rebounds not enough as the Jazz handle
	// the 76ers" - a 19-point loss. A losing line headlines a game only when
	// it is big for the margin.
	test("a losing line headlines only a game it could have swung", () => {
		for (const { gid, recap, game } of pairs) {
			const { w, l } = sides(game);
			const headline = recap.split("\n")[0]!;
			if (!/not enough|despite/.test(headline)) {
				continue;
			}
			const p = l.players.find((x) => headline.includes(x.name));
			if (!p) {
				continue;
			}
			const tens = [p.pts, p.reb, p.ast, p.stl, p.blk];
			const size =
				p.pts +
				(p.reb >= 20 ? 10 : p.reb >= 15 ? 6 : 0) +
				(p.ast >= 12 ? 6 : 0) +
				(tens.filter((x) => x >= 10).length >= 3 ? 10 : 0);
			const margin = w.pts - l.pts;
			assert.isAtLeast(
				size,
				22 + Math.max(0, margin - 5),
				`gid ${gid}: ${headline} (by ${margin})`,
			);
		}
	});

	// Phrases that said something the game did not: "though nothing else
	// fell - 35.9% for the game" after a man's line (the rest of his team
	// scored 80), "Nothing fell from deep - 7-of-29" (seven did), "after
	// going 4-of-7 from three" (the threes were part of the points, not
	// before them), "18 points, 3 steals for the Trail Blazers, and 2
	// blocks", and an "including" aside left open before "as".
	test("no phrase says what the numbers do not", () => {
		for (const { gid, recap } of pairs) {
			for (const bad of [
				/nothing else fell/,
				/Nothing fell from deep/,
				/after going \d+-of-\d+/,
				/\d+ [a-z]+ for the [A-Z][\w ]*, and \d+ [a-z]/,
				/including \d+-of-\d+ from (?:three|the line) (?:as|in)\b/,
			]) {
				assert.notMatch(recap, bad, `gid ${gid}\n${recap}`);
			}
		}
	});

	// A 10-0 run happens most games; "ran off 11 straight points in the
	// second" was in most recaps. The winners' run is told when it is 15 or
	// more, when it came in a quarter that turned the game or broke it open
	// (12 or more), or in a last quarter the game was won in.
	test("the winners' run is told only when it is news", () => {
		let told = 0;
		for (const { gid, recap, game } of pairs) {
			const run = game.flow?.run;
			const { w, l } = sides(game);
			if (!run || run.side !== (game.teams[0] === w ? 0 : 1)) {
				continue;
			}
			if (
				!new RegExp(
					String.raw`\b${run.pts} straight points|\b${run.pts} unanswered|${run.pts}-0 (?:run|burst)`,
				).test(recap)
			) {
				continue;
			}
			told += 1;
			const wq = w.ptsQtrs ?? [];
			const lq = l.ptsQtrs ?? [];
			const after = (n: number) =>
				wq.slice(0, n).reduce((a, b) => a + b, 0) -
				lq.slice(0, n).reduce((a, b) => a + b, 0);
			const reg = wq.length - game.overtimes;
			const news =
				run.pts >= 15 ||
				(run.pts >= 12 &&
					((after(run.period - 1) <= 4 && after(run.period) >= 6) ||
						(after(run.period - 1) < 0 && after(run.period) > 0))) ||
				(run.pts >= 10 &&
					run.period === reg &&
					game.overtimes === 0 &&
					after(reg - 1) <= 0);
			assert.isTrue(news, `gid ${gid}: a ${run.pts}-0 run told\n${recap}`);
		}
		assert.isAtLeast(told, 5);
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
