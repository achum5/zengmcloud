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
			const [headline, , rawLede = ""] = recap.split("\n");
			// The late go-ahead basket the lede ends on names its shooter,
			// who is not the man the lede is about.
			const lede = rawLede.replace(/ on [A-Z][^.]*? go-ahead [^.]*/, "");
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
				/(?:hitting|making) \d+ of \d+ (?:threes|free throws) (?:as|in)\b/,
				// A shooting line is not part of a points total.
				/points, including \d+-of-\d+/,
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
				// "Picked up a sore ankle that will cost him", "went down with
				// plantar fasciitis and is out around four games".
				/is out around|that will cost him|picked up an? (?:sore|sprained|strained|bruised)/,
				// "They shared it, 31 assists on the night", "It was 55-46 the
				// Raptors at the half", "The 20 were 13 more than the 7.0 a game
				// he had been averaging".
				/shared it, \d+ assists|[Ii]t was \d+-\d+ the [A-Z]|were \d+ more than the [\d.]+ a game/,
				/let all of it go before winning it/,
				// "The best the Pelicans could offer was 32 points" undersold
				// the best line on the floor; "got out with 2" was a bettor's.
				/could offer was (?:a [\w-]+ - )?(?:2[5-9]|[3-9]\d) points|got out with \d/,
				// A recap is read the next day: no "tonight".
				/\btonight\b/i,
				// Headlines: a passer's verb on a steals line, a pronoun, a
				// stock phrase.
				/picks the [A-Z][\w ]* apart: \d+ points and \d+ steals|score in double figures as they|come out on top of/,
				// "It was a 11-point game", "the end of a eight-game run", "a
				// 81-point edge", "a 22th straight game".
				/\b[Aa] (?:8|1[18]\b)|\ba (?:eight|eleven|eighteen)|\b\d*(?:[^1]|^)(?:1th|2th|3th)\b/,
				// Prose spells out a streak: "a six-game", not "a 6-game".
				/(?:ending|snapping|snapped|ended) (?:the [\w ]+'s? |an? )[2-9]-game/,
				// "...a 56-52 lead into the break. After the break, ..."
				/halftime\.[^.]*\bafter halftime|\bbreak\.[^.]*\bafter the break/i,
				// Six in double figures is a third of all winners here.
				/had six players score in double figures|got double figures out of six men\.|Six [A-Z][\w ]* reached double figures|- six in double figures/,
				// "Top-scored" is not American; "got 21 to fall from deep" said
				// neither of what nor from how many; nobody is "0 games off the
				// top".
				/top-scored|to fall from deep|\b0 games\b/,
				// A count in prose is a word: "had lost six in a row".
				/have now lost \d|had lost \d+ in a row|skid is over at \d/,
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
	// "Bennedict Brooks' 27 points and 11 rebounds came in a 119-96 loss to
	// the Mavericks" never said whose loss it was.
	test("a day wrap's losing line says whose loss", () => {
		for (const recap of dayRecaps) {
			for (const x of recap.split(/(?<=[!.?])\s+/)) {
				if (/ loss to the /.test(x) && !/losing effort/.test(x)) {
					assert.match(
						x,
						/(?:the [A-Z][\w ]*'s? |for the [A-Z][\w ]* in a )(?:\d+-\d+(?: \(\w+\))? )?loss to/,
						x,
					);
				}
			}
		}
	});

	// "Marcus Green had a triple-double for the Clippers" - the deck's word
	// and none of the line, which was 38 points, 12 rebounds and 10 assists.
	test("a triple-double in a day wrap comes with its line", () => {
		for (const recap of dayRecaps) {
			const body = recap.split("\n\n").slice(1).join(" ");
			assert.notMatch(body, /\bhad a triple-double(?! \()/, recap);
		}
	});

	// "The Nuggets got a double-double - 14 points and 10 assists from
	// Tyrese Ellis": the line, then the man it belonged to, dangling.
	test("a man comes before his line, not after a dash", () => {
		for (const { recap } of pairs) {
			assert.notMatch(
				recap,
				// Nor any line hung off a dash: "leaned on Scoot Carter for a
				// double-double - 17 points and 11 rebounds, and Xavier Carter
				// contributed 16" ran the next man into it.
				/(?:double|triple)-double - \d/,
				recap,
			);
		}
	});

	// "Darius Hayes had 31 points as the Heat beat the Lakers 121-88. ...
	// Jaden Green's 22 points came in the Lakers' 121-88 loss to the Heat."
	test("a day wrap gives a game's score once", () => {
		for (const recap of dayRecaps) {
			const body = recap
				.split("\n\n")
				.filter((p) => !p.startsWith("*"))
				.join(" ");
			for (const m of body.matchAll(
				/came in the [A-Z][\w ]*?'s? (\d+-\d+) loss/g,
			)) {
				assert.equal(body.split(m[1]!).length, 2, recap);
			}
		}
	});

	// "Lonnie Green gave the Lakers 22 points off the bench. That finished a
	// 2-0 season sweep"; "That was Xavier King in his first game since..."
	test("no closing line points 'That' at the sentence before it", () => {
		for (const { recap } of pairs) {
			assert.notMatch(
				recap,
				/(?:^|\. )That (?:lifted|finished|gave|put|squared|made it)\b|That was(?: [A-Z][a-z]+){2} in his first game/,
				recap,
			);
		}
	});

	// "Keegan Lowry scores 30, and the Kings take Game 1" over a teammate's
	// 32; "Xavier Ingram's 24 not enough" over one's 28.
	test("a headline's points belong to the man who scored the most", () => {
		for (const { recap, game } of pairs) {
			const headline = recap.split("\n")[0]!;
			for (const t of game.teams) {
				for (const p of t.players) {
					const m = new RegExp(
						String.raw`${p.name}(?:'s?| scores| goes for| pours in) (\d+)\b(?!-| points and| points,| and \d)`,
					).exec(headline);
					if (m && Number(m[1]) === p.pts) {
						assert.ok(
							t.players.every((q) => q.pts <= p.pts) ||
								/ picks /.test(headline),
							recap,
						);
					}
				}
			}
		}
	});

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

	// "...winning the second half 65-43. They were up by 21 at their
	// biggest. The Celtics led by as many as 15." - a first-half lead told
	// after the second half.
	test("the losers' first-half lead is told before the second half", () => {
		for (const { gid, recap, game } of pairs) {
			const { l } = sides(game);
			const at = game.flow?.maxLeadAt?.[game.teams[0] === l ? 0 : 1];
			if (at === undefined || at > 2) {
				continue;
			}
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
					new RegExp(
						String.raw`^(?:The ${l.name} (?:led by as many as|had been up by)|At one point the ${l.name} led by)`,
					),
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

	// An American box score, not a football table: "level at 54", "the two
	// sides", "top of the table", "away from home" were all in the copy.
	test("the copy is American sports copy", () => {
		for (const recap of [...pairs.map((p) => p.recap), ...dayRecaps]) {
			assert.notMatch(
				recap,
				/\blevel (?:at|with|for|going)\b|break level|drew level|pull(?:ed)? level|the two sides|both sides|\btable\b|away from home|home side|, away\./,
				recap,
			);
		}
	});

	// "The Trail Blazers assisted on 31 of their 39 baskets." and then "The
	// ball found the open man all night for the Trail Blazers - 31 assists to
	// 16.": one team number, told twice.
	test("a team number is told once", () => {
		const KW = {
			ast: /assist/,
			tp: /threes|three-pointers|from deep|from three/,
			reb: /glass|boards|rebounds/,
			tov: /turnovers|giveaways|gave it away|coughed it up/,
		} as const;
		for (const { gid, recap, game } of pairs) {
			const names = game.teams.flatMap((t) => t.players.map((p) => p.name));
			const sentences = recap
				.replaceAll("\n\n", " ")
				.split(/(?<=[!.?])\s+/)
				.filter((x) => !names.some((n) => x.includes(n)));
			for (const [k, re] of Object.entries(KW)) {
				const totals = new Set(
					game.teams.map((t) =>
						t.players.reduce(
							(a, p) => a + (p[k as keyof typeof KW] as number),
							0,
						),
					),
				);
				const told = sentences.filter(
					(x) =>
						re.test(x) &&
						[...x.matchAll(/\b\d+\b/g)].some((m) => totals.has(Number(m[0]))),
				);
				assert.isAtMost(told.length, 1, `gid ${gid} (${k})\n${recap}`);
			}
		}
	});

	// "Foul trouble cost Lonnie Dunn, who finished with 9 points" after a
	// sentence about the other team: a man these notes introduce comes with
	// his team.
	test("a foul-out or a minutes note says whose man it was", () => {
		for (const { gid, recap, game } of pairs) {
			const nicks = game.teams.map((t) => t.name);
			for (const x of recap.split(/(?<=[!.?])\s+/)) {
				if (
					/fouling out with|fouled out with|sixth foul, done|Foul trouble cost|logged \d+ minutes/.test(
						x,
					)
				) {
					assert.ok(
						nicks.some((n) => x.includes(n)),
						`gid ${gid}: ${x}`,
					);
				}
			}
		}
	});

	// "Walker Mathis added 17 points in defeat. Walker Mathis returned from a
	// three-game absence (sore foot) with 17 points." A man's points are
	// given once in the body.
	test("a man's points are given once", () => {
		for (const { gid, recap, game } of pairs) {
			const body = recap.split("\n\n").slice(1).join(" ");
			const sentences = body.split(/(?<=[!.?])\s+/);
			for (const t of game.teams) {
				for (const p of t.players) {
					if (p.pts < 10) {
						continue;
					}
					const told = sentences.filter(
						(x) => x.includes(p.name) && x.includes(`${p.pts} points`),
					);
					assert.isAtMost(told.length, 1, `gid ${gid}: ${p.name}\n${recap}`);
				}
			}
		}
	});

	// "There were 21 lead changes and 5 ties. He had not scored more than 17
	// in two earlier games against the Suns." - a pronoun with no one to
	// point at.
	test("a sentence opening on He follows one that names him", () => {
		for (const { gid, recap, game } of pairs) {
			const names = game.teams.flatMap((t) => t.players.map((p) => p.name));
			const body = recap.split("\n\n").slice(1).join(" ");
			const sentences = body.split(/(?<=[!.?])\s+/);
			sentences.forEach((x, i) => {
				if (/^(?:He|His)\b/.test(x)) {
					const prev = sentences[i - 1] ?? "";
					assert.ok(
						names.filter((n) => prev.includes(n)).length === 1,
						`gid ${gid}: ${prev} || ${x}`,
					);
				}
			});
		}
	});

	// "They were 14 up going into the fourth. They made 25 free throws to 11
	// for the Trail Blazers. The Hornets had led by 18, and very nearly let
	// it go." A team's number dropped into the middle of the game's last
	// minutes broke the clock; it goes with the team's men.
	test("no team number interrupts the finish", () => {
		const STAT =
			/^(?:The [A-Z][\w ]*|They) (?:knocked down|hit|made|got|piled up|finished with|racked up|assisted on|forced|turned|hounded|won the glass|controlled the boards|out-rebounded|blocked|sent back|shot)\b/;
		const CLOCK =
			/with [\d.:]+ (?:seconds? )?(?:left|to go|to play)|at the buzzer|two minutes/;
		for (const { gid, recap } of pairs) {
			for (const para of recap.split("\n\n").slice(1)) {
				const sentences = para.split(/(?<=[!.?])\s+/);
				const lastClock = sentences.findLastIndex((x) => CLOCK.test(x));
				sentences.slice(0, Math.max(0, lastClock)).forEach((x) => {
					assert.notMatch(x, STAT, `gid ${gid}\n${para}`);
				});
			}
		}
	});

	// A headline cut down to "Herb Hayes scores 25" leaves his whole line to
	// the lede.
	test("a headline's trimmed line is given in full underneath", () => {
		for (const { gid, recap } of pairs) {
			const m = / scores (\d+)(?: as |, and )/.exec(recap.split("\n")[0]!);
			if (m) {
				assert.include(
					recap.split("\n\n")[1],
					`${m[1]} points`,
					`gid ${gid}\n${recap}`,
				);
			}
		}
	});

	// "But 18 turnovers did them in" of a game the winners turned it over
	// 16 times: a reason they lost has to be something they did worse.
	test("a reason the losers lost is a difference between the teams", () => {
		const total = (t: RecapTeam, k: "tov" | "fg" | "fga") =>
			t.players.reduce((a, p) => a + p[k], 0);
		let seen = 0;
		for (const { gid, recap, game } of pairs) {
			const { w, l } = sides(game);
			if (
				/did them in|proved costly|kept undoing|were far too many|coughed it up \d+|gave it away \d+ times|undone by \d+ turnovers|turnovers that beat|could not hold on to it/.test(
					recap,
				)
			) {
				seen += 1;
				assert.isAtLeast(
					total(l, "tov") - total(w, "tov"),
					5,
					`gid ${gid}\n${recap}`,
				);
			}
			if (
				/shot just [\d.]+% as a team|shot [\d.]+% from the floor|% shooting night for the team|they shot [\d.]+% from the floor|never found the range|Shooting [\d.]+% was never/.test(
					recap,
				)
			) {
				const pct = (t: RecapTeam) => (100 * total(t, "fg")) / total(t, "fga");
				assert.isAtLeast(pct(w) - pct(l), 4.9, `gid ${gid}\n${recap}`);
			}
		}
		assert.isAtLeast(seen, 3);
	});

	// "Isaiah King strained his hamstring" at the bottom of a recap that had
	// never named him left the reader to guess whose loss it was.
	test("a man hurt in the game is told with his team unless already named", () => {
		let seen = 0;
		for (const { gid, recap, game } of pairs) {
			for (const t of game.teams) {
				for (const p of t.players) {
					if (!p.injury?.newThisGame) {
						continue;
					}
					const at = recap.search(
						new RegExp(
							`${p.name} (?:sprained|strained|bruised|fractured|dislocated|tore|broke|left with)[^.;]* and (?:is expected to miss|will miss|will be out for|will be out as long)`,
						),
					);
					if (at < 0) {
						continue;
					}
					seen += 1;
					if (recap.indexOf(p.name) === at) {
						assert.match(
							recap.slice(0, at),
							new RegExp(`(?:the|teammate) ${t.name}'s? $|teammate $`, "i"),
							`gid ${gid}: ${p.name}\n${recap}`,
						);
					}
				}
			}
		}
		assert.isAtLeast(seen, 2);
	});

	// "30 points, 8 rebounds, and 7 assists on 4 threes" put the rebounds on
	// the threes; "No one scored more than Amen Dunn, who had 16" was the long
	// way to a team high.
	test("threes are something he hit, and a team high is said straight", () => {
		for (const text of [...pairs.map((p) => p.recap), ...dayRecaps]) {
			assert.notMatch(text, /\bon \d+ (?:threes|three-pointers)\b/, text);
			assert.notMatch(text, /\bno one scored more than\b/i, text);
		}
	});

	// "Elsewhere, the Warriors edged the Pacers 101-98. Around the league,
	// the Heat beat the Spurs 101-83." Three results or fewer are one list.
	// And "No game topped the Bucks' 253-point shootout with the Pistons"
	// after the wrap had given that 129-124 itself.
	test("a wrap's short list of results is one sentence, and a total is not re-added", () => {
		const OPENER =
			/^(?:Elsewhere|Also on the night|There was drama elsewhere too|Around the league|In (?:the )?other games?)\b/;
		for (const wrap of dayRecaps) {
			const sentences = wrap
				.split("\n\n")
				.filter((p) => !p.startsWith("*"))
				.join(" ")
				.split(/(?<=[!.?])\s+/);
			const lists = sentences.filter((s) => OPENER.test(s));
			const results = lists.reduce(
				(n, s) => n + (s.match(/\b\d{2,3}-\d{2,3}\b/g) ?? []).length,
				0,
			);
			assert.ok(lists.length < 2 || results > 3, wrap);
			for (const m of wrap.matchAll(
				/(\d{3})-point shootout|combined for (\d{3}) points/g,
			)) {
				const total = Number(m[1] ?? m[2]);
				const said = [...wrap.matchAll(/\b(\d{2,3})-(\d{2,3})\b/g)].some(
					(s) => Number(s[1]) + Number(s[2]) === total,
				);
				assert.ok(!said || total >= 260, wrap);
			}
		}
	});

	// "The Grizzlies got to the line more than twice as often, 32 attempts to
	// 11. Cade Brooks sprained his shoulder..." - and he plays for the Hawks.
	test("a man right after a sentence about the other club comes with his club", () => {
		for (const { gid, recap, game } of pairs) {
			const sideOf = new Map<string, RecapTeam>();
			for (const t of game.teams) {
				for (const p of t.players) {
					sideOf.set(p.name, t);
				}
			}
			const sentences = recap
				.split("\n\n")
				.slice(1)
				.join(" ")
				.split(/(?<=[!.?])\s+(?=[A-Z])/);
			for (let i = 1; i < sentences.length; i++) {
				const cur = sentences[i]!;
				const prev = sentences[i - 1]!;
				const who = [...sideOf.keys()].find((n) => cur.startsWith(`${n} `));
				const mine = who ? sideOf.get(who) : undefined;
				const other = game.teams.find((t) => t !== mine);
				if (!who || !mine || !other || cur.includes(mine.name)) {
					continue;
				}
				const opensOnOther =
					prev.startsWith(`The ${other.name}`) ||
					other.players.some((p) => prev.startsWith(p.name));
				const mentionsMine =
					prev.includes(mine.name) ||
					mine.players.some((p) => prev.includes(p.name));
				assert.ok(
					!opensOnOther ||
						mentionsMine ||
						/\bthey\b|\bnever (?:led|trailed|got back)/i.test(prev),
					`gid ${gid}: ${prev} || ${cur}`,
				);
			}
		}
	});

	// "Jalen Ingram totaled 23 points and 12 rebounds... Jalen Ingram's 23
	// points were more than twice his 8.8 average", and "Walker Carter
	// exploded for 37 points and 8 assists. Walker Carter won it on a layup
	// ... and finished with 37 points and 8 assists".
	test("a man's points are given once in the body", () => {
		for (const { gid, recap, game } of pairs) {
			const sentences = recap
				.split("\n\n")
				.slice(1)
				.join(" ")
				.split(/(?<=[!.?])\s+/);
			for (const t of game.teams) {
				for (const p of t.players) {
					if (p.pts < 10) {
						continue;
					}
					const told = sentences.filter(
						(s) => s.includes(p.name) && s.includes(`${p.pts} points`),
					);
					assert.ok(told.length <= 1, `gid ${gid}: ${told.join(" | ")}`);
				}
			}
		}
	});

	// "The Magic were ahead 63-39 at the half" and nothing after it, over a
	// 106-97 final; "...but it was 57-57 at halftime" over a 54-41 second
	// half. A second half that moved the game is told.
	test("a second half that moved the game gets told", () => {
		const LATER =
			/second half|after the break|after halftime|third|fourth|going into the|entering the|for good|for the last time|with \d+:\d\d|seconds|two minutes|the rest of the way|from there|pushed on|never (?:led|trailed|got back)|as many as \d+\.|lead reached|the final|overtime|stretched|got within|cut it|pulled/i;
		for (const { gid, recap, game } of pairs) {
			const { w, l } = sides(game);
			const wq = w.ptsQtrs ?? [];
			const lq = l.ptsQtrs ?? [];
			if (game.overtimes > 0 || wq.length !== 4 || lq.length !== 4) {
				continue;
			}
			const w2 = wq[2]! + wq[3]!;
			const l2 = lq[2]! + lq[3]!;
			if (l2 - w2 < 8 && w2 - l2 < 12) {
				continue;
			}
			const body = recap.split("\n\n").slice(1, 3).join(" ");
			assert.match(body, LATER, `gid ${gid}\n${recap}`);
		}
	});

	// "Kings make it 8 straight" in the deck over "the Kings downed the
	// Hawks 123-110" and not a word of the eight.
	test("a streak the deck headlines is told in the body", () => {
		for (const wrap of dayRecaps) {
			const paras = wrap.split("\n\n");
			const deck = paras.find((p) => /^\*[^*]/.test(p)) ?? "";
			const body = paras.filter((p) => !p.startsWith("*")).join(" ");
			for (const m of deck.matchAll(
				/([A-Z][\w ]*?) (?:make it \d+ straight|end an? \d+-game skid)/g,
			)) {
				assert.match(
					body,
					new RegExp(`${m[1]}[^.]*(?:straight|in a row|losing streak|skid)`),
					wrap,
				);
			}
		}
	});

	// "Three of the 15 games were decided by five points or fewer" is any
	// night; "Around the league, the Bulls blew out the Grizzlies 101-80" was
	// the whole league on a two-game night; "Four of the ten favorites lost.
	// Four of the ten games were..." ran one frame twice.
	test("a wrap's league notes are news, and its openers fit the list", () => {
		const WORDS = [
			"zero",
			"one",
			"two",
			"three",
			"four",
			"five",
			"six",
			"seven",
			"eight",
			"nine",
			"ten",
			"eleven",
			"twelve",
		];
		const num = (s: string) =>
			WORDS.includes(s.toLowerCase())
				? WORDS.indexOf(s.toLowerCase())
				: Number(s);
		for (const wrap of dayRecaps) {
			const sentences = wrap.split(/(?<=[!.?])\s+/);
			for (let i = 0; i < sentences.length; i++) {
				const s = sentences[i]!;
				assert.notMatch(s, /^Around the league, [^,]*\.$/, wrap);
				const m = /^(\w+) of the (\w+) games were decided by five/.exec(s);
				if (m) {
					assert.ok(num(m[1]!) >= 0.4 * num(m[2]!), wrap);
				}
				if (i > 0) {
					assert.ok(
						!(/^\w+ of the /.test(s) && /^\w+ of the /.test(sentences[i - 1]!)),
						wrap,
					);
				}
			}
		}
	});

	// "That is three straight losses for the 76ers" closed one recap in
	// seven; every team drops three now and then.
	test("a losing streak is news from four", () => {
		for (const { gid, recap } of pairs) {
			assert.notMatch(
				recap,
				/three straight losses|lost three in a row|third straight loss|losses? was the \w+'s? third in a row/,
				`gid ${gid}\n${recap}`,
			);
		}
	});

	// "Bulls cannot keep up with the Pacers, 110-107" over a game won on a
	// jumper with 19.9 seconds left.
	test("a headline's verb fits the margin", () => {
		for (const { gid, recap, game } of pairs) {
			const { w, l } = sides(game);
			const margin = w.pts - l.pts;
			const head = recap.split("\n\n")[0]!;
			assert.notMatch(head, /\bsee off\b/, `gid ${gid}: ${head}`);
			if (margin < 12) {
				assert.notMatch(head, /cannot keep up/, `gid ${gid}: ${head}`);
			}
			if (margin < 8) {
				assert.notMatch(
					head,
					/are too much for|take care of/,
					`gid ${gid}: ${head}`,
				);
			}
			// Mild words are not a rout: "Pistons turn back the Nets 116-73".
			if (margin >= 20) {
				assert.notMatch(head, /turn back/, `gid ${gid}: ${head}`);
			}
			if (margin >= 25) {
				assert.notMatch(head, /take care of/, `gid ${gid}: ${head}`);
			}
			// "had led by 28, and very nearly let it go" of a seven-point win.
			if (margin > 4) {
				assert.notMatch(
					recap,
					/very nearly let it go|led by as many as \d+ and had to hold on/,
					`gid ${gid}\n${recap}`,
				);
			}
		}
	});

	// "Evan Hayes and Darius Hayes post double-doubles..." over a body that
	// never said Darius Hayes.
	test("every player the headline names is in the body", () => {
		for (const { gid, recap, game } of pairs) {
			const [head, ...rest] = recap.split("\n\n");
			const body = rest.join(" ");
			for (const t of game.teams) {
				for (const p of t.players) {
					if (head!.includes(p.name)) {
						assert.include(body, p.name, `gid ${gid}\n${recap}`);
					}
				}
			}
		}
	});

	// "Ace Hawk turns in 33 points as the Hawks handle the Bulls" over "Ace
	// Hawk finished 12-of-22 from the floor", and nothing else about him:
	// the body never said what he scored.
	test("a headline's points for a man are in the body too", () => {
		for (const { gid, recap, game } of pairs) {
			const [head, ...rest] = recap.split("\n\n");
			const body = rest.join(" ");
			for (const t of game.teams) {
				for (const p of t.players) {
					const said = new RegExp(
						String.raw`${p.name}(?:'s?)? (?:[a-z]+ ){0,2}${p.pts}\b`,
					).test(head!);
					if (said) {
						assert.match(
							body,
							new RegExp(
								String.raw`\b${p.pts} points|his (?:season-high )?${p.pts}\b`,
							),
							`gid ${gid}\n${recap}`,
						);
					}
				}
			}
		}
	});

	// "It was 95-95 going into the fourth. At one point the Magic led by
	// 14." sent the reader back to a third quarter the story had left.
	test("the losers' peak is not told after the quarter that followed it", () => {
		for (const { gid, recap } of pairs) {
			for (const para of recap.split("\n\n")) {
				const sentences = para.split(/(?<=\.)\s+/);
				const into = sentences.findIndex((t) =>
					/going into the fourth|entering the fourth|after three\b/.test(t),
				);
				if (into < 0) {
					continue;
				}
				for (const t of sentences.slice(into + 1)) {
					assert.notMatch(
						t,
						/^At one point the|had been up by \d+ at one stage\.$|^The [A-Z][\w ]+ led by as many as \d+\.$/,
						`gid ${gid}\n${recap}`,
					);
				}
			}
		}
	});

	// "Also worth the watch: ..." and "There was more besides: ..." - list
	// openers nobody writes.
	test("a wrap's list openers are plain", () => {
		for (const wrap of dayRecaps) {
			assert.notMatch(
				wrap,
				/Also worth the watch|There was more besides/,
				wrap,
			);
		}
	});
});
