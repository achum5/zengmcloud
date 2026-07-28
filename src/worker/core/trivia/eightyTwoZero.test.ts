import { assert, describe, test } from "vitest";
import {
	buildEras,
	buildMatchups,
	getOptions,
	inEra,
	type EightyTwoZeroEra,
} from "./eightyTwoZero.ts";
import type { TriviaPlayer, TriviaSeasonRow } from "./pool.ts";

const row = (
	season: number,
	tid: number,
	pos: string,
	over: Partial<TriviaSeasonRow> = {},
): TriviaSeasonRow => ({
	season,
	tid,
	pos,
	gp: 70,
	min: 2400,
	pts: 1400,
	trb: 500,
	ast: 300,
	stl: 90,
	blk: 40,
	tp: 80,
	tpa: 200,
	fg: 500,
	fga: 1100,
	ft: 300,
	fta: 380,
	jerseyNumber: "1",
	...over,
});

const player = (pid: number, rows: TriviaSeasonRow[]): TriviaPlayer =>
	({
		pid,
		name: `Player ${pid}`,
		rows,
	}) as TriviaPlayer;

describe("buildEras", () => {
	test("a long league is cut into decades", () => {
		const eras = buildEras(1995, 2024);
		assert.deepStrictEqual(
			eras.map((era) => era.label),
			["1990s", "2000s", "2010s", "2020s"],
		);
		assert.strictEqual(eras[0]!.start, 1990);
		assert.strictEqual(eras[0]!.end, 1999);
	});

	// A five-season league has one decade in it, which would roll the same
	// matchup every round and stop being a constraint at all.
	test("a young league is cut into single seasons instead", () => {
		const eras = buildEras(2020, 2024);
		assert.strictEqual(eras.length, 5);
		assert.deepStrictEqual(
			eras.map((era) => era.label),
			["2020", "2021", "2022", "2023", "2024"],
		);
	});

	test("eras cover every season with no gaps", () => {
		for (const [min, max] of [
			[1995, 2024],
			[2020, 2024],
			[2001, 2013],
		]) {
			const eras = buildEras(min!, max!);
			for (let season = min!; season <= max!; season++) {
				assert.ok(
					eras.some((era) => inEra(season, era)),
					`${season} not in any era of ${min}-${max}`,
				);
			}
		}
	});
});

const ERA_2000S: EightyTwoZeroEra = { start: 2000, end: 2009, label: "2000s" };

describe("position eligibility", () => {
	const pool = {
		players: [
			player(1, [row(2005, 0, "PG")]),
			player(2, [row(2005, 0, "G")]),
			player(3, [row(2005, 0, "C")]),
			player(4, [row(2005, 0, "FC")]),
			player(5, [row(2005, 0, "SF")]),
		],
	};

	// Hybrids play both sides of the hybrid. Without that, a league whose big men
	// are mostly listed FC would roll center rounds with nobody in them.
	test("a hybrid qualifies at both of its positions", () => {
		const atC = getOptions(pool, 0, ERA_2000S, "C", new Set()).map(
			(x) => x.pid,
		);
		assert.deepStrictEqual(atC.sort(), [3, 4]);

		const atPF = getOptions(pool, 0, ERA_2000S, "PF", new Set()).map(
			(x) => x.pid,
		);
		assert.deepStrictEqual(atPF, [4]);

		const atPG = getOptions(pool, 0, ERA_2000S, "PG", new Set()).map(
			(x) => x.pid,
		);
		assert.deepStrictEqual(atPG.sort(), [1, 2]);
	});

	test("a player already drafted is off the board", () => {
		const left = getOptions(pool, 0, ERA_2000S, "C", new Set([3])).map(
			(x) => x.pid,
		);
		assert.deepStrictEqual(left, [4]);
	});

	test("wrong team or wrong era means not eligible", () => {
		assert.strictEqual(
			getOptions(pool, 1, ERA_2000S, "PG", new Set()).length,
			0,
		);
		assert.strictEqual(
			getOptions(
				pool,
				0,
				{ start: 2010, end: 2019, label: "2010s" },
				"PG",
				new Set(),
			).length,
			0,
		);
	});
});

describe("which season a player is drafted from", () => {
	// A player who spent a decade somewhere is offered as his best year there,
	// which is what "the 2000s Lakers" means to anyone picking.
	test("the best season inside the era wins", () => {
		const pool = {
			players: [
				player(1, [
					row(2001, 0, "SF", { pts: 700 }),
					row(2004, 0, "SF", { pts: 2100 }),
					row(2007, 0, "SF", { pts: 1200 }),
					// Outside the era - must never be the one offered.
					row(2012, 0, "SF", { pts: 3000 }),
				]),
			],
		};
		const [option] = getOptions(pool, 0, ERA_2000S, "SF", new Set());
		assert.strictEqual(option!.season, 2004);
	});

	test("a season nobody played in doesn't count as time with the team", () => {
		const pool = {
			players: [player(1, [row(2004, 0, "SF", { gp: 0 })])],
		};
		assert.strictEqual(
			getOptions(pool, 0, ERA_2000S, "SF", new Set()).length,
			0,
		);
	});

	test("the better season is listed first", () => {
		const pool = {
			players: [
				player(1, [row(2004, 0, "SF", { pts: 400 })]),
				player(2, [row(2004, 0, "SF", { pts: 2200 })]),
			],
		};
		const options = getOptions(pool, 0, ERA_2000S, "SF", new Set());
		assert.deepStrictEqual(
			options.map((x) => x.pid),
			[2, 1],
		);
	});
});

describe("buildMatchups", () => {
	const eras = buildEras(2000, 2019);
	const pool = {
		players: [
			player(1, [row(2004, 0, "PG"), row(2015, 1, "C")]),
			player(2, [row(2006, 0, "C")]),
		],
	};

	// The slot machine only rolls from this list, so anything in it has to have
	// an answer - being handed an unanswerable round is a dead end, not a hard
	// choice.
	test("every listed matchup has somebody eligible", () => {
		const matchups = buildMatchups(pool, [0, 1], eras);
		for (const position of ["PG", "SG", "SF", "PF", "C"] as const) {
			for (const m of matchups[position]) {
				const era = eras.find((row2) => row2.start === m.eraStart)!;
				assert.ok(
					getOptions(pool, m.tid, era, position, new Set()).length > 0,
					`${position} ${m.tid} ${m.eraStart} has nobody`,
				);
			}
		}
	});

	test("combinations nobody played are not offered", () => {
		const matchups = buildMatchups(pool, [0, 1], eras);
		// Team 1 only ever had a center, and only in the 2010s.
		assert.deepStrictEqual(
			matchups.PG.map((m) => `${m.tid}|${m.eraStart}`),
			["0|2000"],
		);
		assert.deepStrictEqual(
			matchups.C.map((m) => `${m.tid}|${m.eraStart}`).sort(),
			["0|2000", "1|2010"],
		);
	});

	test("teams outside the league are ignored", () => {
		const matchups = buildMatchups(pool, [0], eras);
		assert.ok(matchups.C.every((m) => m.tid === 0));
	});

	test("the order is stable, so a seeded roll always lands the same", () => {
		const a = buildMatchups(pool, [0, 1], eras);
		const b = buildMatchups(pool, [0, 1], eras);
		assert.deepStrictEqual(a.C, b.C);
	});
});

describe("82-0 shows no ratings", () => {
	// The game is scored on box scores, never on ratings, so a league that hides
	// player ratings has nothing to hide here. That is a property of the payload
	// rather than of the display, and this is what keeps it one: if a rating ever
	// gets added to what a round offers, it fails here rather than quietly
	// leaking onto the screen of a no-ratings league.
	const RATING_KEYS = [
		"ovr",
		"pot",
		"hgt",
		"stre",
		"spd",
		"jmp",
		"endu",
		"ins",
		"dnk",
		"ft",
		"fg",
		"tp",
		"oiq",
		"diq",
		"drb",
		"pss",
		"reb",
		"ratings",
		"skills",
		"value",
		"valueNoPot",
	];

	test("nothing a round offers is a rating", () => {
		const pool = {
			players: [player(1, [row(2004, 0, "SF")])],
		};
		const [option] = getOptions(pool, 0, ERA_2000S, "SF", new Set());
		const keys = Object.keys(option!);
		for (const key of RATING_KEYS) {
			// "fg", "tp" and "ft" are box score counts here, not the shooting
			// ratings of the same name - everything else must be absent outright.
			if (["fg", "tp", "ft", "reb", "drb"].includes(key)) {
				continue;
			}
			assert.ok(!keys.includes(key), `${key} is in the payload`);
		}
	});

	test("the matchup board is teams and years, nothing else", () => {
		const eras = buildEras(2000, 2019);
		const pool = { players: [player(1, [row(2004, 0, "PG")])] };
		const matchups = buildMatchups(pool, [0], eras);
		for (const m of matchups.PG) {
			assert.deepStrictEqual(Object.keys(m).sort(), ["eraStart", "tid"]);
		}
	});
});
