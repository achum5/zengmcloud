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
const dayRecaps: string[] = [];
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
			const { notes, dayRecap } = await getAutoRecapsForDay({ season, day });
			if (dayRecap) {
				dayRecaps.push(dayRecap);
			}
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
		assert.isAtLeast(dayRecaps.length, DAYS);
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
				/\d+ [a-z]+ for the [A-Z][\w ]*, and \d+ (?:points|rebounds|assists|steals|blocks)\b/,
				/including \d+-of-\d+ from (?:three|the line) (?:as|in)\b/,
				// The side that tied it at the end may have been the other one.
				/after the break to force overtime/,
				// Soccer talk, and "and a loss" as an afterthought.
				/aggregate favors|was no help to .*, and a loss/,
				// Backwards, and never said where.
				/are next on the schedule for/,
				// The next game is its own sentence: "...beaten at the line, 3
				// attempts to 24, and play the Pacers next, at home."
				/, and (?:host|visit|play|get) the [A-Z][\w ]* next\b/,
				/\b[Ss]ore achilles|with (?:bruised|sore|strained) quadriceps/,
				// Six in double figures is a third of all winners here.
				/had six players score in double figures|got double figures out of six men\.|Six [A-Z][\w ]* reached double figures|- six in double figures/,
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

	// "Marcus Green pours in 40" in the deck, and never a word of him in the
	// wrap under it - for whom, against whom, won or lost. Every one of 73
	// deck performers in a 90-day corpus run.
	test("a day wrap tells every performer its deck names", () => {
		let seen = 0;
		for (const recap of dayRecaps) {
			const deck = /^\*([^*].*)\*$/m.exec(recap);
			if (!deck) {
				continue;
			}
			const body = recap.slice(deck.index + deck[0].length);
			for (const item of deck[1]!.split(" · ")) {
				const who = /^(.+?) (?:pours in|triple-doubles|goes for)\b/.exec(item);
				if (who) {
					seen += 1;
					assert.include(body, who[1]!, recap);
				}
			}
		}
		assert.isAtLeast(seen, 1);
	});

	// "Payton Mathis goes for 42, Jalen Hayes for 44" over a deck reading
	// "Payton Mathis pours in 42".
	test("a day wrap's deck does not repeat its headline", () => {
		for (const recap of dayRecaps) {
			const headline = recap.split("\n")[0]!;
			const deck = /^\*([^*].*)\*$/m.exec(recap)?.[1] ?? "";
			for (const item of deck.split(" · ")) {
				const who = /^(.+?) (?:pours in|triple-doubles|goes for)\b/.exec(item);
				if (who) {
					assert.notInclude(headline, who[1]!, recap);
				}
			}
		}
	});

	// "Elsewhere, the 76ers routed the Celtics and the Raptors routed the
	// Heat. Also on the night, the Hornets routed the Rockets."
	test("a day wrap does not repeat a result verb", () => {
		for (const recap of dayRecaps) {
			const body = recap.split("\n\n").slice(1).join(" ");
			for (const verb of [
				"routed",
				"blew out",
				"ran away from",
				"rolled past",
				"edged",
				"held off",
				"slipped past",
			]) {
				const n = body.split(` ${verb} `).length - 1;
				assert.isAtMost(n, 1, `${verb}\n${recap}`);
			}
		}
	});

	// The record in parentheses is the lede's; hung on the next mention down
	// the piece it read as a score: "they outscored the Raptors (0-4) 52-34".
	// And a beat of the game's course stands on its own: "They did not trail
	// again and forced 24 turnovers." (A whole-game "never trailed" may join.)
	test("records sit in the lede, and the game's course is not glued to a stat", () => {
		for (const { gid, recap } of pairs) {
			const body = recap.split("\n\n").slice(2).join("\n\n");
			assert.notMatch(body, /\(\d+-\d+\)/, `gid ${gid}\n${recap}`);
			assert.notMatch(
				recap,
				/(?:trail(?:ed)? again|going into the \w+) and (?:forced|hit|made|got|finished|piled|racked|shared|knocked|won the glass|out-rebounded|controlled|turned|hounded)/,
				`gid ${gid}\n${recap}`,
			);
		}
	});

	// "Jalen Green collected 5 steals for the Clippers" closed the losers'
	// paragraph: a note goes with the side it is about.
	test("a winner's steals or blocks are not told among the losers", () => {
		for (const { gid, recap, game } of pairs) {
			const { w, l } = sides(game);
			for (const para of recap.split("\n\n").slice(2)) {
				const first = para.split(/(?<=\.)\s+/)[0] ?? "";
				const losersPara =
					l.players.some((p) => first.includes(p.name)) &&
					!w.players.some((p) => first.includes(p.name));
				if (!losersPara) {
					continue;
				}
				assert.notMatch(
					para,
					new RegExp(
						String.raw`\d+ steals for the ${w.name}|The ${w.name} (?:blocked|sent back|finished with \d+ blocks)|(?:${w.players.map((p) => p.name).join("|")}) protected the rim`,
					),
					`gid ${gid}\n${recap}`,
				);
			}
		}
	});

	// "The Suns were up 1 at the half, 56-55." and nothing after it, in a game
	// the Suns won by seven. A story that reaches the break goes past it.
	test("the story never stops at halftime", () => {
		const after =
			/second half|after the break|after halftime|going into the|entering the|fourth|third|overtime|extra period|with \d|seconds|for good|ahead for the last time|never|did not trail|pulled away|lead reached|as many as|at their biggest|from there|had led|nearly|let it go|got as close/i;
		for (const { gid, recap } of pairs) {
			const story = recap.split("\n\n").slice(1, 3).join(" ");
			const sents = story.split(/(?<=[!.?])\s+/);
			const at = sents.findIndex((t) =>
				/halftime|at the half\b|at the break|into the break/.test(t),
			);
			// The lede may have told the second half already: "breaking it
			// open with a 31-16 fourth quarter".
			if (at < 0 || /(?:third|fourth) quarter|second half/.test(sents[0]!)) {
				continue;
			}
			assert.match(sents.slice(at).join(" "), after, `gid ${gid}\n${recap}`);
		}
	});

	// "...before the Lakers won the second half 69-59. The Lakers piled up 30
	// assists and ran off 15 straight points in the first."
	test("a first-half run is told before the second half", () => {
		for (const { gid, recap } of pairs) {
			// The lede's first sentence summarizes ("breaking it open with a
			// 31-16 fourth quarter"); the story is told from the one after it.
			const sents = recap
				.split("\n\n")
				.slice(1, 3)
				.join(" ")
				.split(/(?<=[!.?])\s+/)
				.slice(1);
			const second = sents.findIndex((t) =>
				/second half|after the break|after halftime|going into the fourth|fourth quarter/i.test(
					t,
				),
			);
			if (second < 0) {
				continue;
			}
			for (const t of sents.slice(second + 1)) {
				assert.notMatch(
					t,
					/straight points in the (?:first|second)\b|-0 (?:run|burst) in the (?:first|second)\b/,
					`gid ${gid}\n${recap}`,
				);
			}
		}
	});

	// "Walker Green has 22 points and 10 assists as the Grizzlies stun the
	// Pacers" over "Walker Green totaled 22 points and 10 assists as the
	// Grizzlies..."
	test("the lede does not restate the headline's line", () => {
		for (const { gid, recap } of pairs) {
			const headline = recap.split("\n")[0]!;
			const lede = recap.split("\n\n")[1]!.split(/(?<=\.)\s/)[0]!;
			for (const m of headline.matchAll(
				/\d+ points and \d+ (?:rebounds|assists|steals|blocks)/g,
			)) {
				assert.notInclude(lede, m[0], `gid ${gid}\n${recap}`);
			}
		}
	});

	// "They racked up 28 assists" - a quarter of all winners have 28.
	test("a team number gets a sentence only when it is unusual", () => {
		for (const { gid, recap, game } of pairs) {
			const { w } = sides(game);
			const sum = (k: "tp" | "ast") =>
				w.players.reduce((a, p) => a + (p[k] ?? 0), 0);
			if (
				new RegExp(
					String.raw`${w.name} (?:piled up|finished with|racked up) \d+ assists|${w.name} shared it, \d+ assists`,
				).test(recap)
			) {
				assert.isAtLeast(sum("ast"), 30, `gid ${gid}\n${recap}`);
			}
			if (
				new RegExp(
					String.raw`${w.name} (?:knocked down \d+ threes|hit \d+ from deep|made \d+ three-pointers|got \w+ to fall from deep)`,
				).test(recap)
			) {
				assert.isAtLeast(sum("tp"), 16, `gid ${gid}\n${recap}`);
			}
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
