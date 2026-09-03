import { assert, beforeEach, describe, test } from "vitest";
import {
	benchBeat,
	dayColdStreak,
	dayMilestones,
	dayRaceSentence,
	dayStandingsMovers,
	dayTomorrow,
	homeRoadBeat,
	milestoneBeat,
	nextGameBeat,
	playerHighBeat,
	playerStreakBeat,
	restBeat,
	returnBeat,
	seriesBeat,
	standingsBeat,
	teamHighBeat,
	type BeatContext,
	type DayBeatContext,
} from "./recapBeats.ts";
import type {
	RecapGame,
	RecapPlayer,
	RecapTeam,
} from "./getDayGamesForRecap.ts";
import { beginRecapBatch, endRecapBatch, rngFromSeed } from "./recapText.ts";

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

const team = (t: Partial<RecapTeam> & { name: string }): RecapTeam => ({
	tid: 0,
	region: "",
	abbrev: "???",
	pts: 100,
	players: [],
	...t,
});

const game = (
	winner: RecapTeam,
	loser: RecapTeam,
	extra: Partial<RecapGame> = {},
): { ctx: BeatContext; game: RecapGame } => {
	const g: RecapGame = {
		gid: 1,
		day: 30,
		overtimes: 0,
		winnerTid: winner.tid,
		playoffs: false,
		clutchPlays: [],
		teams: [winner, loser],
		...extra,
	};
	return {
		game: g,
		ctx: {
			game: g,
			winner,
			loser,
			margin: winner.pts - loser.pts,
			said: new Set(),
			written: "",
		},
	};
};

const entering = (
	over: Partial<NonNullable<RecapPlayer["entering"]>> = {},
): NonNullable<RecapPlayer["entering"]> => ({
	gp: 20,
	high: { pts: 31, reb: 11, ast: 9, tp: 5, stl: 3, blk: 2 },
	totals: { pts: 400, reb: 150, ast: 100, tp: 40, stl: 20, blk: 10 },
	streaks: { twenty: 0, thirty: 0, doubleDouble: 0 },
	...over,
});

// Every phrasing in a pool, by running the beat across seeds inside one batch
// (so pick rotates) and collecting the distinct shapes.
const shapes = (
	beat: (rng: () => number) => string | undefined,
): Set<string> => {
	const out = new Set<string>();
	beginRecapBatch();
	try {
		for (let seed = 1; seed <= 12; seed++) {
			const text = beat(rngFromSeed(seed));
			if (text) {
				out.add(text.replaceAll(/\d+(\.\d+)?/g, "#"));
			}
		}
	} finally {
		endRecapBatch();
	}
	return out;
};

const noNumeralOpener = (texts: Iterable<string>) => {
	for (const t of texts) {
		assert.notMatch(t, /^\d/, `opens on a numeral: ${t}`);
		assert.match(t, /\.$/, `no full stop: ${t}`);
	}
};

describe("standings", () => {
	beforeEach(() => endRecapBatch());

	test("a climb, a lead at the top, and a drop each get a sentence", () => {
		const up = team({
			tid: 1,
			name: "Celtics",
			standing: {
				conf: "Eastern Conference",
				rank: 3,
				rankBefore: 5,
				gb: 2,
				teams: 15,
				won: 20,
				lost: 10,
			},
		});
		const down = team({
			tid: 2,
			name: "Knicks",
			pts: 90,
			standing: {
				conf: "Eastern Conference",
				rank: 7,
				rankBefore: 6,
				gb: 6,
				teams: 15,
				won: 15,
				lost: 15,
			},
		});
		const all = shapes((rng) => standingsBeat(game(up, down).ctx, rng));
		assert.ok(
			[...all].some((s) => /third/.test(s)),
			[...all].join("\n"),
		);
		assert.ok(
			[...all].some((s) => /seventh/.test(s)),
			[...all].join("\n"),
		);
		noNumeralOpener(all);

		const top = team({
			tid: 1,
			name: "Celtics",
			standing: {
				conf: "East",
				rank: 1,
				rankBefore: 1,
				gb: 0,
				teams: 15,
				won: 20,
				lost: 10,
				lead: 2.5,
			},
		});
		const topShapes = shapes((rng) =>
			standingsBeat(
				game(top, team({ tid: 2, name: "Knicks", pts: 90 })).ctx,
				rng,
			),
		);
		assert.strictEqual(topShapes.size, 3);
		assert.ok([...topShapes].every((s) => s.includes("# game")));
	});

	test("says nothing early in the season, in the playoffs, or with nothing to say", () => {
		const early = team({
			tid: 1,
			name: "Celtics",
			standing: {
				conf: "East",
				rank: 1,
				rankBefore: 3,
				gb: 0,
				teams: 15,
				won: 4,
				lost: 1,
			},
		});
		const other = team({ tid: 2, name: "Knicks", pts: 90 });
		assert.strictEqual(
			standingsBeat(game(early, other).ctx, rngFromSeed(1)),
			undefined,
		);
		const settled = team({
			tid: 1,
			name: "Celtics",
			standing: {
				conf: "East",
				rank: 8,
				rankBefore: 8,
				gb: 12,
				teams: 15,
				won: 20,
				lost: 20,
			},
		});
		assert.strictEqual(
			standingsBeat(game(settled, other).ctx, rngFromSeed(1)),
			undefined,
		);
		const playoffs = game(early, other, { playoffs: true });
		assert.strictEqual(standingsBeat(playoffs.ctx, rngFromSeed(1)), undefined);
		// A climb to thirteenth of fifteen, or a slide from twelfth, is noise.
		const deep = team({
			tid: 1,
			name: "Celtics",
			standing: {
				conf: "East",
				rank: 13,
				rankBefore: 14,
				gb: 20,
				teams: 15,
				won: 10,
				lost: 25,
			},
		});
		assert.strictEqual(
			standingsBeat(game(deep, other).ctx, rngFromSeed(1)),
			undefined,
		);
		const slid = team({
			tid: 2,
			name: "Knicks",
			pts: 90,
			standing: {
				conf: "East",
				rank: 13,
				rankBefore: 12,
				gb: 20,
				teams: 15,
				won: 10,
				lost: 25,
			},
		});
		assert.strictEqual(
			standingsBeat(
				game(team({ tid: 1, name: "Celtics" }), slid).ctx,
				rngFromSeed(1),
			),
			undefined,
		);
	});
});

describe("the season series", () => {
	beforeEach(() => endRecapBatch());

	test("a sweep so far, a level series, a lead, a first win", () => {
		const loser = team({ tid: 2, name: "Knicks", pts: 90 });
		const cases: [Partial<NonNullable<RecapTeam["seasonSeries"]>>, RegExp][] = [
			[{ won: 2, lost: 0 }, /all three meetings|three for three|yet to lose/],
			[{ won: 1, lost: 2 }, /2-2/],
			[{ won: 2, lost: 1 }, /3-1|third win in four/],
			[{ won: 0, lost: 2 }, /first win|first two meetings|on the board/],
		];
		for (const [series, pattern] of cases) {
			const winner = team({
				tid: 1,
				name: "Celtics",
				seasonSeries: { won: 0, lost: 0, ...series },
			});
			const all = shapes((rng) => seriesBeat(game(winner, loser).ctx, rng));
			assert.ok(
				all.size >= 3,
				`${JSON.stringify(series)}: ${[...all].join(" | ")}`,
			);
			const text = seriesBeat(game(winner, loser).ctx, rngFromSeed(3))!;
			assert.match(text, pattern);
			noNumeralOpener(all);
		}
	});

	test("a lopsided previous loss becomes a revenge angle, and the last meeting is flagged", () => {
		const winner = team({
			tid: 1,
			name: "Celtics",
			seasonSeries: {
				won: 0,
				lost: 1,
				last: { won: false, pts: 95, oppPts: 115, day: 10, home: true },
				left: 1,
			},
		});
		const loser = team({ tid: 2, name: "Knicks", pts: 90 });
		const all = shapes((rng) => seriesBeat(game(winner, loser).ctx, rng));
		assert.ok(
			[...all].some((s) =>
				/avenged|previous meeting|lost the last meeting/.test(s),
			),
			[...all].join("\n"),
		);
		assert.ok(
			[...all].some((s) => /one meeting to go/.test(s)),
			[...all].join("\n"),
		);
	});

	test("a first meeting has no series to talk about", () => {
		const winner = team({ tid: 1, name: "Celtics" });
		assert.strictEqual(
			seriesBeat(
				game(winner, team({ tid: 2, name: "Knicks", pts: 90 })).ctx,
				rngFromSeed(1),
			),
			undefined,
		);
	});
});

describe("home and road, rest, and what comes next", () => {
	beforeEach(() => endRecapBatch());

	test("a strong road record for a road winner, a bad home record for a home loser", () => {
		const winner = team({
			tid: 1,
			name: "Celtics",
			awayRecord: { won: 9, lost: 3 },
			homeRecord: { won: 8, lost: 4 },
		});
		const loser = team({
			tid: 2,
			name: "Knicks",
			pts: 90,
			homeRecord: { won: 3, lost: 9 },
		});
		// The loser is listed first: home team.
		const all = shapes((rng) =>
			homeRoadBeat(game(winner, loser, { teams: [loser, winner] }).ctx, rng),
		);
		assert.ok(
			[...all].some((s) => /road|away from home/.test(s)),
			[...all].join("\n"),
		);
		assert.ok(
			[...all].some((s) => /at home|own building|home losses/.test(s)),
			[...all].join("\n"),
		);
		noNumeralOpener(all);
	});

	test("a back-to-back and a long layoff", () => {
		const winner = team({
			tid: 1,
			name: "Celtics",
			rest: { daysSince: 5, prevDay: 25 },
		});
		const loser = team({
			tid: 2,
			name: "Knicks",
			pts: 90,
			rest: { daysSince: 1, prevDay: 29 },
		});
		const all = shapes((rng) => restBeat(game(winner, loser).ctx, rng));
		assert.ok([...all].some((s) => /back-to-back/.test(s)));
		assert.ok([...all].some((s) => /five days/.test(s)));
		assert.strictEqual(
			restBeat(
				game(
					team({ tid: 1, name: "A", rest: { daysSince: 2, prevDay: 1 } }),
					team({ tid: 2, name: "B", pts: 90 }),
				).ctx,
				rngFromSeed(1),
			),
			undefined,
		);
	});

	test("the next game, tomorrow or in a few days, home or away", () => {
		const winner = team({
			tid: 1,
			name: "Celtics",
			nextGame: {
				day: 31,
				daysAway: 1,
				home: true,
				oppTid: 5,
				oppName: "Bulls",
				oppAbbrev: "CHI",
			},
		});
		const loser = team({ tid: 2, name: "Knicks", pts: 90 });
		const all = shapes((rng) => nextGameBeat(game(winner, loser).ctx, rng));
		assert.ok(all.size >= 3, [...all].join("\n"));
		assert.ok([...all].every((s) => /tomorrow/.test(s) && /Bulls/.test(s)));
		const later = {
			...winner,
			nextGame: { ...winner.nextGame!, daysAway: 3, home: false },
		};
		const text = nextGameBeat(game(later, loser).ctx, rngFromSeed(2))!;
		assert.match(text, /three days/);
		assert.match(text, /road|away|visit/);
	});
});

describe("season highs, streaks, milestones and returns", () => {
	beforeEach(() => endRecapBatch());

	test("a team high and a league high", () => {
		const winner = team({
			tid: 1,
			name: "Celtics",
			pts: 138,
			seasonHighs: {
				priorGames: 20,
				pts: true,
				margin: true,
				leaguePts: false,
			},
		});
		const loser = team({ tid: 2, name: "Knicks", pts: 100 });
		const all = shapes((rng) => teamHighBeat(game(winner, loser).ctx, rng));
		assert.ok(
			[...all].every((s) => /season/.test(s)),
			[...all].join("\n"),
		);
		const league = {
			...winner,
			seasonHighs: { ...winner.seasonHighs!, leaguePts: true },
		};
		assert.match(
			teamHighBeat(game(league, loser).ctx, rngFromSeed(1))!,
			/any team|No team|highest-scoring/,
		);
		const early = {
			...winner,
			seasonHighs: { ...winner.seasonHighs!, priorGames: 3 },
		};
		assert.strictEqual(
			teamHighBeat(game(early, loser).ctx, rngFromSeed(1)),
			undefined,
		);
	});

	test("a player's season high is quoted bare, never as tonight's line", () => {
		const p = player({ name: "Jayson Tatum", pts: 41, entering: entering() });
		const all = shapes((rng) => playerHighBeat(p, rng));
		assert.ok(all.size >= 3, [...all].join("\n"));
		for (const s of all) {
			assert.notMatch(s, /# points/);
			assert.match(s, /#|season high|best/);
		}
		const quiet = player({
			name: "Jayson Tatum",
			pts: 25,
			entering: entering(),
		});
		assert.strictEqual(playerHighBeat(quiet, rngFromSeed(1)), undefined);
		const fresh = player({
			name: "Jayson Tatum",
			pts: 41,
			entering: entering({ gp: 3 }),
		});
		assert.strictEqual(playerHighBeat(fresh, rngFromSeed(1)), undefined);
	});

	test("scoring streaks count tonight, and a double-double run", () => {
		const hot = player({
			name: "Luka Doncic",
			pts: 33,
			entering: entering({
				streaks: { twenty: 6, thirty: 2, doubleDouble: 0 },
			}),
		});
		assert.match(playerStreakBeat(hot, rngFromSeed(1))!, /three|third/);
		const twenty = player({
			name: "Luka Doncic",
			pts: 22,
			entering: entering({
				streaks: { twenty: 6, thirty: 0, doubleDouble: 0 },
			}),
		});
		assert.match(playerStreakBeat(twenty, rngFromSeed(1))!, /seven|seventh/);
		const dd = player({
			name: "Nikola Jokic",
			pts: 18,
			reb: 14,
			entering: entering({
				streaks: { twenty: 0, thirty: 0, doubleDouble: 4 },
			}),
		});
		assert.match(playerStreakBeat(dd, rngFromSeed(1))!, /five|fifth/);
		const cold = player({
			name: "Luka Doncic",
			pts: 12,
			entering: entering({
				streaks: { twenty: 6, thirty: 0, doubleDouble: 0 },
			}),
		});
		assert.strictEqual(playerStreakBeat(cold, rngFromSeed(1)), undefined);
	});

	test("the biggest milestone on the floor, with career over season", () => {
		const winner = team({
			tid: 1,
			name: "Celtics",
			players: [
				player({
					name: "Role Guy",
					pts: 12,
					milestone: { scope: "season", stat: "pts", mark: 500, total: 508 },
				}),
				player({
					name: "Old Head",
					pts: 9,
					milestone: { scope: "career", stat: "reb", mark: 5000, total: 5004 },
				}),
			],
		});
		const loser = team({ tid: 2, name: "Knicks", pts: 90 });
		const { ctx } = game(winner, loser);
		const all = shapes((rng) => milestoneBeat(ctx, rng));
		assert.ok(all.size >= 3, [...all].join("\n"));
		assert.ok(
			[...all].every((s) => /Old Head/.test(s) && /career/.test(s)),
			[...all].join("\n"),
		);
		const raw = milestoneBeat(ctx, rngFromSeed(1))!;
		assert.match(raw, /5,000|5,004/);
		assert.ok(ctx.said.has("Old Head"));
	});

	test("the man back from injury gets his line and his absence", () => {
		const winner = team({
			tid: 1,
			name: "Celtics",
			players: [
				player({
					name: "Kristaps Porzingis",
					pts: 21,
					reb: 9,
					returnFrom: { games: 8, type: "Sprained Ankle" },
				}),
			],
		});
		const loser = team({ tid: 2, name: "Knicks", pts: 90 });
		const all = shapes((rng) => returnBeat(game(winner, loser).ctx, rng));
		assert.ok(all.size >= 3, [...all].join("\n"));
		for (const s of all) {
			assert.match(s, /eight/);
			assert.match(s, /sprained ankle/);
			assert.notMatch(s, /\ba eight/);
		}
		assert.match(
			returnBeat(game(winner, loser).ctx, rngFromSeed(1))!,
			/21 points/,
		);
		// "torn meniscus" is singular for all its final s.
		const knee = team({
			tid: 1,
			name: "Celtics",
			players: [
				player({
					name: "Al Horford",
					pts: 14,
					returnFrom: { games: 5, type: "Torn Meniscus" },
				}),
			],
		});
		const kneeShapes = shapes((rng) => returnBeat(game(knee, loser).ctx, rng));
		assert.ok(
			[...kneeShapes].some((x) => x.includes("a torn meniscus")),
			[...kneeShapes].join("\n"),
		);
		for (const x of kneeShapes) {
			assert.notMatch(x, /with torn meniscus|since torn meniscus/);
		}
	});

	test("the bench: an edge, a losing bench, a spark", () => {
		const starters = (prefix: string, pts: number[]) =>
			pts.map((p, i) =>
				player({ name: `${prefix} Starter ${i}`, pts: p, starter: true }),
			);
		const reserves = (prefix: string, pts: number[]) =>
			pts.map((p, i) =>
				player({ name: `${prefix} Sub ${i}`, pts: p, starter: false }),
			);
		const winner = team({
			tid: 1,
			name: "Celtics",
			pts: 110,
			players: [
				...starters("C", [20, 15, 12, 10, 8]),
				...reserves("C", [18, 14, 13]),
			],
		});
		const loser = team({
			tid: 2,
			name: "Knicks",
			pts: 95,
			players: [
				...starters("K", [25, 20, 18, 12, 10]),
				...reserves("K", [6, 4]),
			],
		});
		const all = shapes((rng) => benchBeat(game(winner, loser).ctx, rng));
		assert.ok(
			[...all].every((s) => /bench|reserves/.test(s)),
			[...all].join("\n"),
		);
		assert.ok([...all].some((s) => /45/.test(s) || /#-#/.test(s)));

		const spark = team({
			tid: 1,
			name: "Celtics",
			pts: 110,
			players: [
				...starters("C", [20, 15, 12, 10, 8]),
				...reserves("C", [24, 4, 2]),
			],
		});
		const text = benchBeat(game(spark, loser).ctx, rngFromSeed(1))!;
		assert.match(text, /C Sub 0/);
		assert.match(text, /bench|reserve/);

		const unknown = team({
			tid: 1,
			name: "Celtics",
			pts: 110,
			players: [player({ name: "X", pts: 30 })],
		});
		assert.strictEqual(
			benchBeat(game(unknown, loser).ctx, rngFromSeed(1)),
			undefined,
		);
	});
});

// THE NIGHT'S OWN BEATS.
//
// The day wrap's context: who moved in the table, the race at the cut line,
// the team that cannot win, the round numbers, the season bests, tomorrow.
describe("the day wrap's context", () => {
	beforeEach(() => endRecapBatch());

	const dayTeam = (
		tid: number,
		name: string,
		over: Partial<RecapTeam> = {},
	): RecapTeam => team({ tid, name, abbrev: name.slice(0, 3), ...over });

	const dayGame = (winner: RecapTeam, loser: RecapTeam): RecapGame => ({
		gid: winner.tid * 100,
		day: 40,
		overtimes: 0,
		winnerTid: winner.tid,
		playoffs: false,
		clutchPlays: [],
		teams: [winner, loser],
	});

	const ctxOf = (
		games: RecapGame[],
		standings?: DayBeatContext["standings"],
	): DayBeatContext => ({
		games,
		standings,
		saidTids: new Set(),
		saidPlayers: new Set(),
	});

	const standingRow = (
		conf: string,
		rank: number,
		rankBefore: number,
		teams = 15,
	) => ({
		conf,
		rank,
		rankBefore,
		gb: rank,
		teams,
		won: 25,
		lost: 15,
	});

	test("a team into the places, a team out of them, a climb inside them", () => {
		const games = [
			dayGame(
				dayTeam(1, "Celtics", { standing: standingRow("East", 8, 10) }),
				dayTeam(2, "Knicks", { standing: standingRow("East", 9, 8) }),
			),
			dayGame(
				dayTeam(3, "Bucks", { standing: standingRow("East", 2, 4) }),
				dayTeam(4, "Heat", { standing: standingRow("East", 12, 12) }),
			),
		];
		const text = dayStandingsMovers(
			ctxOf(games, { playoffSpots: 8, confs: [] }),
			rngFromSeed(1),
		)!;
		assert.match(text, /Celtics moved into the East places at eighth/);
		assert.match(text, /Knicks slid out of the East places to ninth/);
		assert.match(text, /Bucks climbed to second/);
		// No clause carries its own comma: they are joined into one list.
		assert.notMatch(text, /places, down to/);

		const all = shapes((rng) =>
			dayStandingsMovers(ctxOf(games, { playoffSpots: 8, confs: [] }), rng),
		);
		assert.ok(all.size >= 3, [...all].join("\n"));
	});

	test("a move outside the picture, or with no sample behind it, is not news", () => {
		const deep = [
			dayGame(
				dayTeam(1, "Celtics", { standing: standingRow("East", 12, 13) }),
				dayTeam(2, "Knicks", { standing: standingRow("East", 13, 12) }),
			),
		];
		assert.strictEqual(
			dayStandingsMovers(
				ctxOf(deep, { playoffSpots: 8, confs: [] }),
				rngFromSeed(1),
			),
			undefined,
		);
		const early = [
			dayGame(
				dayTeam(1, "Celtics", {
					standing: { ...standingRow("East", 3, 5), won: 4, lost: 2 },
				}),
				dayTeam(2, "Knicks"),
			),
		];
		assert.strictEqual(
			dayStandingsMovers(
				ctxOf(early, { playoffSpots: 8, confs: [] }),
				rngFromSeed(1),
			),
			undefined,
		);
	});

	test("the cut line names who holds the last place, and by how much", () => {
		const standings = {
			playoffSpots: 8,
			confs: [
				{
					name: "East",
					teams: [
						{
							tid: 1,
							name: "Celtics",
							abbrev: "BOS",
							rank: 8,
							won: 20,
							lost: 20,
							gb: 6,
						},
						{
							tid: 2,
							name: "Knicks",
							abbrev: "NYK",
							rank: 9,
							won: 19,
							lost: 21,
							gb: 7,
						},
					],
				},
				{
					name: "West",
					teams: [
						{
							tid: 3,
							name: "Kings",
							abbrev: "SAC",
							rank: 8,
							won: 20,
							lost: 20,
							gb: 5,
						},
						{
							tid: 4,
							name: "Suns",
							abbrev: "PHX",
							rank: 9,
							won: 20,
							lost: 20,
							gb: 5,
						},
					],
				},
			],
		};
		const text = dayRaceSentence(ctxOf([], standings), rngFromSeed(1))!;
		assert.match(
			text,
			/Celtics hold the last East place by 1 game over the Knicks/,
		);
		assert.match(text, /Kings and the Suns are level for the last West place/);
		// "cut line" never twice in one sentence.
		assert.ok((text.match(/cut line/g) ?? []).length <= 1, text);
		assert.ok(
			shapes((rng) => dayRaceSentence(ctxOf([], standings), rng)).size >= 3,
		);
	});

	test("a runaway race, no spots known, or too few games says nothing", () => {
		const runaway = {
			playoffSpots: 8,
			confs: [
				{
					name: "East",
					teams: [
						{
							tid: 1,
							name: "Celtics",
							abbrev: "BOS",
							rank: 8,
							won: 25,
							lost: 15,
							gb: 2,
						},
						{
							tid: 2,
							name: "Knicks",
							abbrev: "NYK",
							rank: 9,
							won: 15,
							lost: 25,
							gb: 12,
						},
					],
				},
			],
		};
		assert.strictEqual(
			dayRaceSentence(ctxOf([], runaway), rngFromSeed(1)),
			undefined,
		);
		assert.strictEqual(
			dayRaceSentence(ctxOf([], { confs: runaway.confs }), rngFromSeed(1)),
			undefined,
		);
	});

	test("the longest losing run in the league gets a line", () => {
		const games = [
			dayGame(
				dayTeam(1, "Celtics"),
				dayTeam(2, "Knicks", { streak: { won: false, count: 11 } }),
			),
			dayGame(
				dayTeam(3, "Bucks"),
				dayTeam(4, "Heat", { streak: { won: false, count: 7 } }),
			),
		];
		const text = dayColdStreak(ctxOf(games), rngFromSeed(1))!;
		assert.match(text, /Knicks/);
		assert.match(text, /11|eleven/);
		assert.ok(shapes((rng) => dayColdStreak(ctxOf(games), rng)).size >= 3);

		const short = [
			dayGame(
				dayTeam(1, "Celtics"),
				dayTeam(2, "Knicks", { streak: { won: false, count: 3 } }),
			),
		];
		assert.strictEqual(dayColdStreak(ctxOf(short), rngFromSeed(1)), undefined);
	});

	test("milestones say whether they are career or season, biggest first", () => {
		const games = [
			dayGame(
				dayTeam(1, "Celtics", {
					players: [
						player({
							name: "Old Head",
							pts: 20,
							milestone: {
								scope: "career",
								stat: "pts",
								mark: 20000,
								total: 20004,
							},
						}),
						player({
							name: "Young Gun",
							pts: 18,
							milestone: {
								scope: "season",
								stat: "reb",
								mark: 500,
								total: 502,
							},
						}),
					],
				}),
				dayTeam(2, "Knicks"),
			),
		];
		const text = dayMilestones(ctxOf(games), rngFromSeed(1))!;
		assert.match(text, /Old Head went past 20,000 career points/);
		assert.match(text, /Young Gun went past 500 rebounds for the season/);
		assert.ok(text.indexOf("Old Head") < text.indexOf("Young Gun"));
		assert.strictEqual(
			dayMilestones(
				ctxOf([dayGame(dayTeam(1, "A"), dayTeam(2, "B"))]),
				rngFromSeed(1),
			),
			undefined,
		);
	});

	test("a player already in the deck is not given a milestone line as well", () => {
		const games = [
			dayGame(
				dayTeam(1, "Celtics", {
					players: [
						player({
							name: "Old Head",
							pts: 20,
							milestone: {
								scope: "career",
								stat: "pts",
								mark: 20000,
								total: 20004,
							},
						}),
					],
				}),
				dayTeam(2, "Knicks"),
			),
		];
		const ctx = ctxOf(games);
		ctx.saidPlayers.add("Old Head");
		assert.strictEqual(dayMilestones(ctx, rngFromSeed(1)), undefined);
	});

	test("tomorrow names the biggest matchup and counts the rest in words", () => {
		const next = (oppName: string, daysAway = 1, home = true) => ({
			day: 41,
			daysAway,
			home,
			oppTid: 9,
			oppName,
			oppAbbrev: "OPP",
		});
		const games = [
			dayGame(
				dayTeam(1, "Celtics", {
					record: { won: 30, lost: 10 },
					nextGame: next("Bucks"),
				}),
				dayTeam(2, "Knicks", {
					record: { won: 10, lost: 30 },
					nextGame: next("Heat"),
				}),
			),
			dayGame(
				dayTeam(3, "Kings", {
					record: { won: 20, lost: 20 },
					nextGame: next("Suns"),
				}),
				dayTeam(4, "Jazz", {
					record: { won: 15, lost: 25 },
					nextGame: next("Nets", 2),
				}),
			),
		];
		const text = dayTomorrow(ctxOf(games), rngFromSeed(1))!;
		assert.match(text, /the Bucks at the Celtics/);
		assert.match(text, /two others?|two other games/);
		assert.notMatch(text, /\b\d+ other/);
		assert.ok(shapes((rng) => dayTomorrow(ctxOf(games), rng)).size >= 3);

		const noneTomorrow = [
			dayGame(
				dayTeam(1, "Celtics", { nextGame: next("Bucks", 3) }),
				dayTeam(2, "Knicks"),
			),
		];
		assert.strictEqual(
			dayTomorrow(ctxOf(noneTomorrow), rngFromSeed(1)),
			undefined,
		);
	});
});
