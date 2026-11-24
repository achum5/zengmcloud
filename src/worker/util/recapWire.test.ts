import { assert, describe, test } from "vitest";
import { getAutoRecap } from "./getAutoRecap.ts";
import type {
	RecapGame,
	RecapPlayer,
	RecapTeam,
} from "./getDayGamesForRecap.ts";
import { verifyRecap } from "./recapAccuracy.ts";
import { datelineOf, storyOf } from "./recapWire.ts";

const player = (p: Partial<RecapPlayer> & { name: string }): RecapPlayer => ({
	pid: 0,
	min: 32,
	pts: 0,
	reb: 0,
	ast: 0,
	stl: 0,
	blk: 0,
	tov: 0,
	fg: 0,
	fga: 0,
	tp: 0,
	tpa: 0,
	ft: 0,
	fta: 0,
	pf: 0,
	...p,
});

const nuggets = (over: Partial<RecapTeam> = {}): RecapTeam => ({
	tid: 1,
	region: "Denver",
	name: "Nuggets",
	abbrev: "DEN",
	pts: 118,
	ptsQtrs: [26, 28, 36, 28],
	record: { won: 8, lost: 2 },
	players: [
		player({
			pid: 10,
			name: "Devin Nash",
			pts: 34,
			reb: 9,
			ast: 11,
			fg: 12,
			fga: 22,
			ft: 8,
			fta: 9,
			tp: 2,
			tpa: 5,
		}),
		player({
			pid: 11,
			name: "Franz Owens",
			pts: 21,
			reb: 4,
			ast: 6,
			fg: 8,
			fga: 15,
			ft: 3,
			fta: 4,
			tp: 2,
			tpa: 6,
		}),
		player({
			pid: 12,
			name: "Bennedict Iverson",
			pts: 18,
			reb: 7,
			ast: 2,
			fg: 7,
			fga: 13,
			ft: 2,
			fta: 2,
			tp: 2,
			tpa: 4,
		}),
		player({
			pid: 13,
			name: "Trey Rivers",
			pts: 15,
			reb: 5,
			ast: 3,
			fg: 6,
			fga: 11,
			ft: 1,
			fta: 2,
			tp: 2,
			tpa: 5,
		}),
		player({
			pid: 14,
			name: "Amen Simmons",
			pts: 16,
			reb: 10,
			ast: 1,
			fg: 7,
			fga: 12,
			ft: 2,
			fta: 3,
		}),
		player({
			pid: 15,
			name: "Cade Thompson",
			pts: 14,
			reb: 3,
			ast: 2,
			fg: 5,
			fga: 10,
			ft: 2,
			fta: 2,
			tp: 2,
			tpa: 4,
		}),
	],
	...over,
});

const knicks = (over: Partial<RecapTeam> = {}): RecapTeam => ({
	tid: 2,
	region: "New York",
	name: "Knicks",
	abbrev: "NYK",
	pts: 104,
	ptsQtrs: [27, 25, 24, 28],
	record: { won: 6, lost: 4 },
	players: [
		player({
			pid: 20,
			name: "Quentin King",
			pts: 27,
			reb: 4,
			ast: 9,
			fg: 10,
			fga: 22,
			ft: 5,
			fta: 6,
			tp: 2,
			tpa: 7,
		}),
		player({
			pid: 21,
			name: "Xavier Lowry",
			pts: 22,
			reb: 8,
			ast: 2,
			fg: 9,
			fga: 18,
			ft: 2,
			fta: 3,
			tp: 2,
			tpa: 5,
		}),
		player({
			pid: 22,
			name: "Devin Mathis",
			pts: 19,
			reb: 6,
			ast: 3,
			fg: 7,
			fga: 15,
			ft: 3,
			fta: 4,
			tp: 2,
			tpa: 6,
		}),
		player({
			pid: 23,
			name: "Franz Nowell",
			pts: 14,
			reb: 3,
			ast: 1,
			fg: 5,
			fga: 12,
			ft: 2,
			fta: 2,
			tp: 2,
			tpa: 5,
		}),
		player({
			pid: 24,
			name: "Cade Reed",
			pts: 22,
			reb: 5,
			ast: 2,
			fg: 9,
			fga: 17,
			ft: 2,
			fta: 2,
			tp: 2,
			tpa: 6,
		}),
	],
	...over,
});

const gameOf = (
	home: RecapTeam,
	away: RecapTeam,
	over: Partial<RecapGame> = {},
): RecapGame => ({
	gid: 77,
	day: 12,
	overtimes: 0,
	winnerTid: home.pts > away.pts ? home.tid : away.tid,
	playoffs: false,
	teams: [home, away],
	clutchPlays: [],
	...over,
});

const partsOf = (recap: string) =>
	[...recap.matchAll(/^### (.+)$/gm)].map((m) => m[1]);

describe("datelines", () => {
	test("a city stands alone, a state's club plays in its city", () => {
		assert.strictEqual(datelineOf("Denver"), "DENVER");
		assert.strictEqual(datelineOf("Golden State"), "SAN FRANCISCO");
		assert.strictEqual(datelineOf("Utah"), "SALT LAKE CITY");
		assert.strictEqual(datelineOf("Brooklyn"), "NEW YORK");
		assert.strictEqual(datelineOf("Memphis"), "MEMPHIS, Tenn.");
		assert.strictEqual(datelineOf("Portland"), "PORTLAND, Ore.");
		assert.strictEqual(datelineOf("the Bay"), undefined);
		assert.strictEqual(datelineOf(""), undefined);
	});
});

describe("the wire format", () => {
	test("dateline, the story, then the four parts in order", () => {
		const g = gameOf(nuggets(), knicks());
		const recap = getAutoRecap(g, { wire: true });
		const [headline, lede] = recap.split("\n\n");
		assert.match(headline!, /^\*\*/);
		assert.match(lede!, /^DENVER — /);
		assert.deepEqual(partsOf(recap), ["Takeaways", "Key moment", "Key stat"]);
		const takeaways = recap
			.split("### Takeaways\n\n")[1]!
			.split("\n\n### ")[0]!
			.split("\n\n");
		assert.strictEqual(takeaways.length, 2);
		assert.match(takeaways[0]!, /^Nuggets: /);
		assert.match(takeaways[1]!, /^Knicks: /);
		assert.deepEqual(verifyRecap(recap, g), []);
	});

	test("without it, the story alone, as before", () => {
		const g = gameOf(nuggets(), knicks());
		const recap = getAutoRecap(g);
		assert.notInclude(recap, "###");
		assert.notInclude(recap, "DENVER");
		assert.strictEqual(
			storyOf(getAutoRecap(g, { wire: true })).includes("###"),
			false,
		);
	});

	test("a neutral site has no dateline", () => {
		const g = gameOf(nuggets(), knicks(), { neutral: true });
		const recap = getAutoRecap(g, { wire: true });
		assert.notMatch(recap.split("\n\n")[1]!, /—/);
	});

	test("up next says the day of the week only when the schedule has dates", () => {
		const next = (
			oppTid: number,
			oppName: string,
			oppRegion: string,
			home: boolean,
			date?: string,
		) => ({
			day: 14,
			daysAway: 2,
			home,
			oppTid,
			oppName,
			oppAbbrev: "???",
			oppRegion,
			...(date ? { date } : {}),
		});
		const dated = gameOf(
			nuggets({
				nextGame: next(9, "Hornets", "Charlotte", true, "2026-11-29"),
			}),
			knicks({
				nextGame: next(8, "Thunder", "Oklahoma City", false, "2026-11-28"),
			}),
		);
		const recap = getAutoRecap(dated, { wire: true });
		assert.include(
			recap,
			"### Up next\n\nNuggets: Host the Charlotte Hornets on Sunday.\n\nKnicks: Visit the Oklahoma City Thunder on Saturday.",
		);
		// The story leaves what is next to Up next.
		assert.notMatch(storyOf(recap), /\bnext\b/);

		const classic = gameOf(
			nuggets({ nextGame: next(9, "Hornets", "Charlotte", true) }),
			knicks({ nextGame: next(8, "Thunder", "Oklahoma City", false) }),
		);
		const plain = getAutoRecap(classic, { wire: true });
		assert.include(plain, "Nuggets: Host the Charlotte Hornets.");
		assert.notMatch(plain, /day\b/);
	});

	test("the same two teams next is one line", () => {
		const g = gameOf(
			nuggets({
				nextGame: {
					day: 14,
					daysAway: 2,
					home: false,
					oppTid: 2,
					oppName: "Knicks",
					oppAbbrev: "NYK",
					date: "2026-11-27",
				},
			}),
			knicks({
				nextGame: {
					day: 14,
					daysAway: 2,
					home: true,
					oppTid: 1,
					oppName: "Nuggets",
					oppAbbrev: "DEN",
					date: "2026-11-27",
				},
			}),
		);
		const recap = getAutoRecap(g, { wire: true });
		assert.include(
			recap,
			"### Up next\n\nThe teams meet again on Friday in New York.",
		);
	});

	test("a losing streak is news from four, a winning one from three", () => {
		const three = getAutoRecap(
			gameOf(
				nuggets({ streak: { won: true, count: 3 } }),
				knicks({ streak: { won: false, count: 3 } }),
			),
			{ wire: true },
		);
		assert.match(
			three,
			/Nuggets: Denver has (?:won three|won three in a row|won three straight)/,
		);
		assert.notMatch(three, /lost three|dropped three/);
		const four = getAutoRecap(
			gameOf(nuggets(), knicks({ streak: { won: false, count: 4 } })),
			{ wire: true },
		);
		assert.match(
			four,
			/Knicks: New York has (?:lost|dropped) four|Knicks' fourth straight loss|Knicks,? (?:who )?have lost four/,
			four,
		);
	});

	test("a series line says where the series stands", () => {
		const g = gameOf(nuggets(), knicks(), {
			playoffs: true,
			series: {
				round: 2,
				numRounds: 4,
				bestOf: 7,
				homeAbbrev: "DEN",
				awayAbbrev: "NYK",
				homeWon: 2,
				awayWon: 2,
			},
		});
		const recap = getAutoRecap(g, { wire: true });
		assert.include(
			recap,
			"Nuggets: Denver is one win from the Conference Finals.",
		);
		assert.include(recap, "Knicks: New York faces elimination in Game 6.");
	});
});
