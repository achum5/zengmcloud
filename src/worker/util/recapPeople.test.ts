import { assert, describe, test } from "vitest";
import { getAutoRecap } from "./getAutoRecap.ts";
import type {
	RecapGame,
	RecapPlayer,
	RecapTeam,
} from "./getDayGamesForRecap.ts";
import { verifyRecap } from "./recapAccuracy.ts";
import type { PlayerPast } from "./recapCareer.ts";
import { storyOf } from "./recapWire.ts";

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

const past = (over: Partial<PlayerPast> = {}): PlayerPast => ({
	gp: 120,
	high: { pts: 30, reb: 12, ast: 11, tp: 6, stl: 4, blk: 3 },
	dd: 10,
	td: 2,
	...over,
});

const nuggets = (
	nash: Partial<RecapPlayer> = {},
	over: Partial<RecapTeam> = {},
): RecapTeam => ({
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
			...nash,
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

const gameOf = (home: RecapTeam, away: RecapTeam): RecapGame => ({
	gid: 77,
	day: 12,
	overtimes: 0,
	winnerTid: home.pts > away.pts ? home.tid : away.tid,
	playoffs: false,
	teams: [home, away],
	clutchPlays: [],
});

const count = (text: string, re: RegExp) => [...text.matchAll(re)].length;

describe("careers", () => {
	test("a career high is called one once, in his line", () => {
		const g = gameOf(nuggets({ past: past() }), knicks());
		const recap = getAutoRecap(g, { wire: true });
		assert.include(recap, "career-high 34 points");
		assert.strictEqual(count(recap, /career[ -]high/g), 1, recap);
		assert.deepEqual(verifyRecap(recap, g), []);
	});

	test("no career on file, no career high", () => {
		const g = gameOf(nuggets(), knicks());
		assert.notMatch(getAutoRecap(g, { wire: true }), /career/);
	});

	test("a first triple-double", () => {
		const g = gameOf(
			nuggets({
				reb: 10,
				past: past({
					td: 0,
					high: { pts: 40, reb: 14, ast: 12, tp: 6, stl: 4, blk: 3 },
				}),
			}),
			knicks(),
		);
		const recap = getAutoRecap(g, { wire: true });
		assert.match(
			recap,
			/first (?:career )?triple-double|first triple-double of his career/,
		);
		assert.deepEqual(verifyRecap(recap, g), []);
	});

	test("a top pick's debut", () => {
		const g = gameOf(
			nuggets(),
			knicks({
				players: knicks().players.map((p) =>
					p.name === "Xavier Lowry"
						? {
								...p,
								past: past({ gp: 0, dd: 0, td: 0 }),
								draft: { year: 2025, round: 1, pick: 2 },
							}
						: p,
				),
			}),
		);
		const recap = getAutoRecap(g, { wire: true });
		assert.match(recap, /Lowry, the second pick in the 2025 draft,/);
		assert.match(recap, /debut/);
		assert.deepEqual(verifyRecap(recap, g), []);
	});
});

describe("the race and the trip", () => {
	test("the first team to clinch, said once", () => {
		const g = gameOf(
			nuggets({}, { race: { clinched: "playoffs", firstToClinch: true } }),
			knicks(),
		);
		const recap = getAutoRecap(g, { wire: true });
		assert.strictEqual(
			count(recap, /first team to clinch a playoff spot/g),
			1,
			recap,
		);
		assert.deepEqual(verifyRecap(recap, g), []);
	});

	test("an elimination is the losers' news", () => {
		const g = gameOf(nuggets(), knicks({ race: { eliminated: true } }));
		const recap = getAutoRecap(g, { wire: true });
		assert.match(recap, /eliminated from playoff contention/);
		assert.deepEqual(verifyRecap(recap, g), []);
	});

	test("the end of a trip, with its record", () => {
		const g = gameOf(
			nuggets(),
			knicks({ stretch: { home: false, game: 5, games: 5, won: 3, lost: 2 } }),
		);
		const recap = getAutoRecap(g, { wire: true });
		assert.match(recap, /finished (?:its|their) five-game trip 3-2/);
		assert.deepEqual(verifyRecap(recap, g), []);
	});

	test("the checker catches a trip that was not", () => {
		const g = gameOf(nuggets(), knicks());
		const recap = `${getAutoRecap(g)}\n\nThe Knicks finished their five-game trip 3-2.`;
		assert.isNotEmpty(verifyRecap(recap, g).filter((v) => v.kind === "trip"));
		assert.notInclude(storyOf(recap), "###");
	});
});
