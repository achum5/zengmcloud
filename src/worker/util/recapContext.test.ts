import { assert, describe, test } from "vitest";
import {
	careerMilestone,
	crossedMark,
	homeAwayRecords,
	nextGameFor,
	pastSeasonTotals,
	playerEntering,
	restEntering,
	returnFromAbsence,
	seasonMilestone,
	seasonSeries,
	standingOf,
	teamGamesBefore,
	teamSeasonHighs,
	type ContextGameRow,
} from "./recapContext.ts";

// A season in a few lines: gid, day, home, away, score. The winner is whoever
// scored more.
const game = (
	gid: number,
	day: number,
	home: number,
	away: number,
	homePts: number,
	awayPts: number,
	extra: Partial<ContextGameRow> & {
		players?: [any[], any[]];
	} = {},
): ContextGameRow => {
	const homeWon = homePts > awayPts;
	const { players, ...rest } = extra;
	return {
		gid,
		day,
		won: homeWon ? { tid: home, pts: homePts } : { tid: away, pts: awayPts },
		lost: homeWon ? { tid: away, pts: awayPts } : { tid: home, pts: homePts },
		teams: [
			{ tid: home, pts: homePts, players: players?.[0] },
			{ tid: away, pts: awayPts, players: players?.[1] },
		],
		...rest,
	};
};

describe("the season series", () => {
	const season = [
		game(1, 1, 0, 1, 100, 90),
		game(2, 5, 1, 0, 105, 99),
		game(3, 9, 0, 2, 110, 100),
		game(4, 12, 1, 0, 88, 101),
		game(5, 20, 0, 1, 95, 97),
	];

	test("counts only the meetings before this one, and remembers the last", () => {
		const series = seasonSeries(0, 1, 4, 12, season);
		assert.deepStrictEqual(series, {
			won: 1,
			lost: 1,
			last: { won: false, pts: 99, oppPts: 105, day: 5, home: false },
			left: undefined,
		});
	});

	test("counts the meetings still to come from the log and the schedule", () => {
		const series = seasonSeries(0, 1, 4, 12, season, [
			{ gid: 40, day: 30, homeTid: 1, awayTid: 0 },
			{ gid: 41, day: 31, homeTid: 2, awayTid: 0 },
		]);
		// Game 5 on day 20 is in the log; game 40 is scheduled; game 41 is
		// somebody else.
		assert.strictEqual(series?.left, 2);
	});

	test("a first meeting has no series to speak of", () => {
		assert.strictEqual(seasonSeries(0, 1, 1, 1, season), undefined);
	});

	test("playoff games are not part of the season series", () => {
		const withPlayoffs = [
			...season,
			game(9, 90, 0, 1, 100, 80, { playoffs: true }),
		];
		const series = seasonSeries(0, 1, 10, 91, withPlayoffs);
		assert.strictEqual(series?.won, 2);
		assert.strictEqual(series?.lost, 2);
	});
});

describe("rest and the next game", () => {
	const log = [
		game(1, 3, 0, 1, 100, 90),
		game(2, 4, 2, 0, 100, 90),
		game(3, 7, 0, 3, 100, 90),
	];

	test("a game the day after the last one is a back-to-back", () => {
		assert.deepStrictEqual(restEntering(0, 2, 4, log), {
			daysSince: 1,
			prevDay: 3,
		});
		assert.deepStrictEqual(restEntering(0, 3, 7, log), {
			daysSince: 3,
			prevDay: 4,
		});
		assert.strictEqual(restEntering(0, 1, 3, log), undefined);
	});

	test("the next game comes from the log when it has been played", () => {
		assert.deepStrictEqual(nextGameFor(0, 1, 3, log), {
			day: 4,
			home: false,
			oppTid: 2,
		});
	});

	test("...and from the schedule when it has not, whichever is sooner", () => {
		const schedule = [
			{ gid: 50, day: 9, homeTid: 0, awayTid: 4 },
			{ gid: 51, day: 8, homeTid: 5, awayTid: 0 },
		];
		assert.deepStrictEqual(nextGameFor(0, 3, 7, log, schedule), {
			day: 8,
			home: false,
			oppTid: 5,
		});
		assert.strictEqual(nextGameFor(0, 3, 7, log), undefined);
	});

	test("the team's games before a given one come back oldest first", () => {
		assert.deepStrictEqual(
			teamGamesBefore(0, 3, 7, log).map((g) => g.gid),
			[1, 2],
		);
	});
});

describe("home and road records", () => {
	test("split the regular season by venue through the day", () => {
		const log = [
			game(1, 1, 0, 1, 100, 90),
			game(2, 2, 1, 0, 100, 90),
			game(3, 3, 0, 2, 80, 90),
			game(4, 4, 2, 0, 80, 90),
			game(5, 5, 0, 1, 100, 90),
			game(6, 40, 0, 1, 100, 90, { playoffs: true }),
		];
		assert.deepStrictEqual(homeAwayRecords(0, 4, log), {
			home: { won: 1, lost: 1 },
			away: { won: 1, lost: 1 },
		});
		assert.deepStrictEqual(homeAwayRecords(0, 99, log), {
			home: { won: 2, lost: 1 },
			away: { won: 1, lost: 1 },
		});
	});
});

describe("season highs", () => {
	const log = [
		game(1, 1, 0, 1, 100, 90),
		game(2, 2, 0, 2, 118, 90),
		game(3, 3, 3, 4, 125, 80),
		game(4, 4, 0, 3, 95, 110),
	];

	test("a team high and a league high are told apart", () => {
		const tonight = game(5, 5, 0, 1, 120, 90);
		const highs = teamSeasonHighs(0, tonight, [...log, tonight]);
		assert.deepStrictEqual(highs, {
			priorGames: 3,
			pts: true,
			margin: true,
			leaguePts: false,
		});
		const bigger = game(6, 6, 0, 1, 130, 90);
		assert.strictEqual(
			teamSeasonHighs(0, bigger, [...log, bigger]).leaguePts,
			true,
		);
	});

	test("a loser cannot set a margin high, and an opener sets nothing", () => {
		const loss = game(5, 5, 0, 1, 90, 100);
		assert.strictEqual(teamSeasonHighs(0, loss, [...log, loss]).margin, false);
		const opener = game(1, 1, 7, 8, 140, 100);
		const highs = teamSeasonHighs(7, opener, [opener]);
		assert.strictEqual(highs.priorGames, 0);
		assert.strictEqual(highs.pts, false);
	});
});

describe("what a player brought into the game", () => {
	const line = (
		day: number,
		gid: number,
		pts: number,
		reb = 5,
		ast = 4,
		tp = 2,
	) => ({
		day,
		gid,
		playoffs: false,
		row: { pts, orb: 1, drb: reb - 1, ast, tp, stl: 1, blk: 0 },
	});

	test("highs, totals and the streak he was on", () => {
		const lines = [
			line(1, 1, 18, 12, 10),
			line(2, 2, 31, 7, 3),
			line(3, 3, 22, 11, 11),
			line(4, 4, 24, 10, 4),
			// This game - never counted.
			line(5, 5, 40, 3, 3),
		];
		const entering = playerEntering(lines, 5, 5, false);
		assert.strictEqual(entering.gp, 4);
		assert.deepStrictEqual(entering.high, {
			pts: 31,
			reb: 12,
			ast: 11,
			tp: 2,
			stl: 1,
			blk: 0,
		});
		assert.strictEqual(entering.totals.pts, 95);
		assert.deepStrictEqual(entering.streaks, {
			twenty: 3,
			thirty: 0,
			doubleDouble: 2,
		});
	});

	test("a streak is broken by the most recent miss, not an old one", () => {
		const lines = [
			line(1, 1, 35),
			line(2, 2, 12),
			line(3, 3, 33),
			line(4, 4, 30),
		];
		const entering = playerEntering(lines, 5, 5, false);
		assert.deepStrictEqual(entering.streaks, {
			twenty: 2,
			thirty: 2,
			doubleDouble: 0,
		});
	});

	test("playoff lines and regular-season lines never mix", () => {
		const lines = [line(1, 1, 30), { ...line(2, 2, 30), playoffs: true }];
		assert.strictEqual(playerEntering(lines, 3, 3, false).gp, 1);
		assert.strictEqual(playerEntering(lines, 3, 3, true).gp, 1);
	});
});

describe("milestones", () => {
	test("the mark passed in this game, and only in this game", () => {
		assert.strictEqual(crossedMark(980, 1010, 1000, 1000), 1000);
		assert.strictEqual(crossedMark(1010, 1040, 1000, 1000), undefined);
		assert.strictEqual(crossedMark(480, 510, 500, 1000), undefined);
		assert.strictEqual(crossedMark(1990, 2040, 1000, 1000), 2000);
	});

	test("a season mark and a career mark, points before everything", () => {
		const before = { pts: 490, reb: 495, ast: 100, tp: 99, stl: 0, blk: 0 };
		const after = { pts: 515, reb: 505, ast: 104, tp: 102, stl: 0, blk: 0 };
		assert.deepStrictEqual(seasonMilestone(before, after), {
			scope: "season",
			stat: "pts",
			mark: 500,
			total: 515,
		});
		assert.deepStrictEqual(
			seasonMilestone({ ...before, pts: 600 }, { ...after, pts: 620 }),
			{ scope: "season", stat: "reb", mark: 500, total: 505 },
		);
		assert.deepStrictEqual(
			careerMilestone(
				{ pts: 9990, reb: 0, ast: 0, tp: 0, stl: 0, blk: 0 },
				{ pts: 10012, reb: 0, ast: 0, tp: 0, stl: 0, blk: 0 },
			),
			{ scope: "career", stat: "pts", mark: 10000, total: 10012 },
		);
	});

	test("past-season totals leave the live season out", () => {
		const totals = pastSeasonTotals(
			{
				stats: [
					{
						season: 2014,
						playoffs: false,
						gp: 80,
						pts: 1600,
						orb: 100,
						drb: 300,
						ast: 200,
						tp: 90,
					},
					{
						season: 2015,
						playoffs: false,
						gp: 70,
						pts: 1400,
						orb: 80,
						drb: 220,
						ast: 150,
						tp: 110,
					},
					{
						season: 2015,
						playoffs: true,
						gp: 10,
						pts: 300,
						orb: 10,
						drb: 30,
						ast: 20,
						tp: 15,
					},
					{
						season: 2016,
						playoffs: false,
						gp: 30,
						pts: 700,
						orb: 40,
						drb: 100,
						ast: 60,
						tp: 40,
					},
				],
			},
			2016,
		);
		assert.deepStrictEqual(totals, {
			gp: 150,
			pts: 3000,
			reb: 700,
			ast: 350,
			tp: 200,
			stl: 0,
			blk: 0,
		});
	});
});

describe("the first game back", () => {
	const out = (pid: number, type = "sprained ankle") => ({
		pid,
		min: 0,
		injury: { type, gamesRemaining: 4 },
	});
	const played = (pid: number) => ({
		pid,
		min: 30,
		injury: { type: "Healthy", gamesRemaining: 0 },
	});

	test("counts the games he sat out injured, back to the last one he played", () => {
		const log = [
			game(1, 1, 0, 1, 100, 90, { players: [[played(7)], []] }),
			game(2, 2, 0, 2, 100, 90, { players: [[out(7)], []] }),
			game(3, 3, 3, 0, 100, 90, { players: [[], [out(7)]] }),
			game(4, 4, 0, 4, 100, 90, { players: [[out(7)], []] }),
		];
		assert.deepStrictEqual(returnFromAbsence(7, 0, 5, 5, log), {
			games: 3,
			type: "sprained ankle",
		});
	});

	test("two games out, a healthy scratch, or a new arrival is not a return", () => {
		const twoOut = [
			game(1, 1, 0, 1, 100, 90, { players: [[played(7)], []] }),
			game(2, 2, 0, 2, 100, 90, { players: [[out(7)], []] }),
			game(3, 3, 0, 3, 100, 90, { players: [[out(7)], []] }),
		];
		assert.strictEqual(returnFromAbsence(7, 0, 4, 4, twoOut), undefined);

		const scratch = [
			game(1, 1, 0, 1, 100, 90, {
				players: [
					[{ pid: 7, min: 0, injury: { type: "Healthy", gamesRemaining: 0 } }],
					[],
				],
			}),
			game(2, 2, 0, 2, 100, 90, { players: [[out(7)], []] }),
			game(3, 3, 0, 3, 100, 90, { players: [[out(7)], []] }),
			game(4, 4, 0, 4, 100, 90, { players: [[out(7)], []] }),
		];
		// Three injured absences, then a healthy DNP before them - still a
		// return, because the walk stops at the first game he was not hurt.
		assert.strictEqual(returnFromAbsence(7, 0, 5, 5, scratch)?.games, 3);

		const arrival = [
			game(1, 1, 0, 1, 100, 90, { players: [[], []] }),
			game(2, 2, 0, 2, 100, 90, { players: [[out(7)], []] }),
			game(3, 3, 0, 3, 100, 90, { players: [[out(7)], []] }),
		];
		assert.strictEqual(returnFromAbsence(7, 0, 4, 4, arrival), undefined);
	});
});

describe("a team's place in the standings", () => {
	test("is found by tid, with the conference and its size", () => {
		const standings = {
			confs: [
				{
					name: "East",
					teams: [
						{ tid: 3, abbrev: "A", rank: 1, won: 10, lost: 2, gb: 0 },
						{ tid: 0, abbrev: "B", rank: 2, won: 8, lost: 4, gb: 2 },
					],
				},
				{
					name: "West",
					teams: [{ tid: 5, abbrev: "C", rank: 1, won: 9, lost: 3, gb: 0 }],
				},
			],
		};
		assert.deepStrictEqual(standingOf(standings, 0), {
			conf: "East",
			rank: 2,
			gb: 2,
			teams: 2,
			won: 8,
			lost: 4,
		});
		assert.strictEqual(standingOf(standings, 9), undefined);
		// First place knows its cushion; everyone else knows who is first.
		const named = {
			confs: [
				{
					name: "East",
					teams: [
						{
							tid: 3,
							abbrev: "A",
							name: "Hawks",
							rank: 1,
							won: 10,
							lost: 2,
							gb: 0,
						},
						{
							tid: 0,
							abbrev: "B",
							name: "Bulls",
							rank: 2,
							won: 8,
							lost: 4,
							gb: 2,
						},
					],
				},
			],
		};
		assert.strictEqual(standingOf(named, 3)?.lead, 2);
		assert.strictEqual(standingOf(named, 3)?.leader, undefined);
		assert.strictEqual(standingOf(named, 0)?.leader, "Hawks");
		assert.strictEqual(standingOf(undefined, 0), undefined);
	});
});
