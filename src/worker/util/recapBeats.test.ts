import { assert, beforeEach, describe, test } from "vitest";
import {
	benchBeat,
	dayBracketWatch,
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
	scoringNormBeat,
	seriesBeat,
	seriesShapeBeat,
	standingsBeat,
	teamHighBeat,
	vsOpponentBeat,
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

	test("a scoring high is dropped once the average sentence has made the point", () => {
		const p = player({ name: "Jayson Tatum", pts: 41, entering: entering() });
		// The name can sit either side of the number in the average clause.
		for (const said of [
			"Jayson Tatum came into the night averaging 22.0 points a game.",
			"It was a long way past the 22.0 a night Jayson Tatum had been putting up.",
		]) {
			assert.strictEqual(
				playerHighBeat(p, rngFromSeed(1), said),
				undefined,
				said,
			);
		}
		// A different man's average says nothing about his night.
		assert.ok(
			playerHighBeat(
				p,
				rngFromSeed(1),
				"Jaylen Brown came into the night averaging 20.0 points a game.",
			),
		);
	});

	test("a rebounding high survives a sentence about his scoring", () => {
		const p = player({
			name: "Domantas Sabonis",
			pts: 24,
			reb: 19,
			entering: entering({
				high: { pts: 31, reb: 14, ast: 9, tp: 5, stl: 3, blk: 2 },
			}),
		});
		const text = playerHighBeat(
			p,
			rngFromSeed(1),
			"That is 11 more than the 13.0 a game Domantas Sabonis had been averaging.",
		);
		assert.ok(text, "the rebounding high was dropped");
		assert.match(text!, /rebound|glass|grabbed/);
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
		assert.match(
			text,
			/Celtics moved into a playoff spot in the East at eighth/,
		);
		assert.match(
			text,
			/Knicks slid out of the playoff spots in the East to ninth/,
		);
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
			/Celtics hold the last playoff spot in the East by 1 game over the Knicks/,
		);
		assert.match(
			text,
			/Kings and the Suns are level for the last spot in the West/,
		);
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

describe("the night against the season's norms", () => {
	beforeEach(() => endRecapBatch());

	const withNorm = (
		name: string,
		tid: number,
		pts: number,
		norm: { gp: number; pts: number; oppPts: number } | undefined,
	) => team({ tid, name, pts, norm });

	test("a night well above what the winner had been scoring", () => {
		const w = withNorm("Celtics", 1, 128, { gp: 30, pts: 110.4, oppPts: 108 });
		const l = withNorm("Knicks", 2, 100, { gp: 30, pts: 104.2, oppPts: 106 });
		const all = shapes((rng) => scoringNormBeat(game(w, l).ctx, rng));
		assert.ok(all.size >= 3, [...all].join("\n"));
		assert.ok(
			[...all].some((s) => /averaging|season average|managing/.test(s)),
			[...all].join("\n"),
		);
		noNumeralOpener(all);
	});

	test("a losing side well below its own, and a defensive night named as one", () => {
		const w = withNorm("Celtics", 1, 100, { gp: 30, pts: 101, oppPts: 100 });
		const l = withNorm("Knicks", 2, 84, { gp: 30, pts: 112.5, oppPts: 106 });
		const all = shapes((rng) => scoringNormBeat(game(w, l).ctx, rng));
		assert.ok(all.size >= 3, [...all].join("\n"));
		assert.ok(
			[...all].some((s) =>
				/short of|never got near|managed|held|under/.test(s),
			),
			[...all].join("\n"),
		);
	});

	test("the season-high sentence and the average sentence never both fire", () => {
		const w = withNorm("Celtics", 1, 128, { gp: 30, pts: 110.4, oppPts: 108 });
		const l = withNorm("Knicks", 2, 100, { gp: 30, pts: 104.2, oppPts: 106 });
		const { ctx } = game(w, l);
		ctx.written = "The 128 points were a season high for the Celtics.";
		// Only the loser's side of it is left to say, and here there is none.
		assert.strictEqual(scoringNormBeat(ctx, rngFromSeed(1)), undefined);
	});

	test("every shape names its team, so nothing dangles mid-paragraph", () => {
		const w = withNorm("Celtics", 1, 128, { gp: 30, pts: 110.4, oppPts: 108 });
		const l = withNorm("Knicks", 2, 100, { gp: 30, pts: 104.2, oppPts: 106 });
		const all = shapes((rng) => scoringNormBeat(game(w, l).ctx, rng));
		for (const s of all) {
			assert.ok(/Celtics|Knicks/.test(s), s);
		}
	});

	test("an ordinary night, a short sample, or the playoffs says nothing", () => {
		const w = withNorm("Celtics", 1, 108, { gp: 30, pts: 106, oppPts: 104 });
		const l = withNorm("Knicks", 2, 102, { gp: 30, pts: 105, oppPts: 107 });
		assert.strictEqual(
			scoringNormBeat(game(w, l).ctx, rngFromSeed(1)),
			undefined,
		);

		const early = withNorm("Celtics", 1, 128, { gp: 4, pts: 100, oppPts: 100 });
		assert.strictEqual(
			scoringNormBeat(game(early, l).ctx, rngFromSeed(1)),
			undefined,
		);

		const big = withNorm("Celtics", 1, 128, { gp: 30, pts: 105, oppPts: 100 });
		assert.strictEqual(
			scoringNormBeat(game(big, l, { playoffs: true }).ctx, rngFromSeed(1)),
			undefined,
		);
	});
});

describe("a player against this opponent", () => {
	beforeEach(() => endRecapBatch());

	test("a man who keeps doing it to them", () => {
		const p = player({
			name: "Jayson Tatum",
			pts: 34,
			vsOpponent: { games: 2, bestPts: 36, avgPts: 33.5 },
		});
		const all = shapes((rng) => vsOpponentBeat(p, "Knicks", rng));
		assert.ok(all.size >= 3, [...all].join("\n"));
		assert.ok(
			[...all].every((s) => /Knicks/.test(s)),
			[...all].join("\n"),
		);
		noNumeralOpener(all);
	});

	test("a man who had never done it to them before", () => {
		const p = player({
			name: "Jayson Tatum",
			pts: 31,
			vsOpponent: { games: 3, bestPts: 15, avgPts: 12.5 },
		});
		const all = shapes((rng) => vsOpponentBeat(p, "Knicks", rng));
		assert.ok(all.size >= 3, [...all].join("\n"));
		assert.ok(
			[...all].every((s) =>
				/not managed more than|previous best|had held/.test(s),
			),
			[...all].join("\n"),
		);
		assert.match(vsOpponentBeat(p, "Knicks", rngFromSeed(1))!, /\b15\b/);
	});

	test("one earlier meeting, a quiet night, or nothing at all says nothing", () => {
		const once = player({
			name: "Jayson Tatum",
			pts: 34,
			vsOpponent: { games: 1, bestPts: 30, avgPts: 30 },
		});
		assert.strictEqual(
			vsOpponentBeat(once, "Knicks", rngFromSeed(1)),
			undefined,
		);
		const quiet = player({
			name: "Jayson Tatum",
			pts: 11,
			vsOpponent: { games: 3, bestPts: 30, avgPts: 28 },
		});
		assert.strictEqual(
			vsOpponentBeat(quiet, "Knicks", rngFromSeed(1)),
			undefined,
		);
		assert.strictEqual(
			vsOpponentBeat(
				player({ name: "Nobody", pts: 30 }),
				"Knicks",
				rngFromSeed(1),
			),
			undefined,
		);
	});
});

describe("the postseason day wrap", () => {
	beforeEach(() => endRecapBatch());

	const seriesGame = (
		gid: number,
		winnerName: string,
		loserName: string,
		homeWon: number,
		awayWon: number,
		tids: [number, number],
	): RecapGame => {
		const winner = team({
			tid: tids[0],
			name: winnerName,
			abbrev: "WIN",
			pts: 100,
		});
		const loser = team({
			tid: tids[1],
			name: loserName,
			abbrev: "LOS",
			pts: 90,
		});
		return {
			gid,
			day: 100,
			overtimes: 0,
			winnerTid: tids[0],
			playoffs: true,
			clutchPlays: [],
			teams: [winner, loser],
			series: {
				round: 1,
				numRounds: 4,
				bestOf: 7,
				homeAbbrev: "WIN",
				awayAbbrev: "LOS",
				homeSeed: 1,
				awaySeed: 8,
				homeWon,
				awayWon,
			},
		};
	};

	const ctx = (games: RecapGame[]): DayBeatContext => ({
		games,
		saidTids: new Set(),
		saidPlayers: new Set(),
	});

	// Four wins takes a best-of-seven, and tonight's winner had one fewer than
	// the score below says: (1, 2) is a series pulled level at 2-2, (2, 2) is a
	// 3-2 lead, (2, 3) is 3-3 and everything on Game 7.
	test("elimination outranks a series pulled level", () => {
		const games = [
			seriesGame(1, "Bucks", "Heat", 1, 2, [3, 4]),
			seriesGame(2, "Celtics", "Knicks", 2, 2, [1, 2]),
		];
		const text = dayBracketWatch(ctx(games), rngFromSeed(1))!;
		assert.match(text, /Knicks face elimination in Game 6/);
		assert.match(text, /Bucks and the Heat are level at 2-2/);
		assert.ok(text.indexOf("elimination") < text.indexOf("level"), text);
		assert.ok(shapes((rng) => dayBracketWatch(ctx(games), rng)).size >= 3);
	});

	test("a decider outranks everything", () => {
		const games = [
			seriesGame(1, "Celtics", "Knicks", 2, 2, [1, 2]),
			seriesGame(2, "Bucks", "Heat", 2, 3, [3, 4]),
		];
		const text = dayBracketWatch(ctx(games), rngFromSeed(1))!;
		assert.match(text, /Bucks-Heat go to a decider in Game 7/);
		assert.ok(text.indexOf("decider") < text.indexOf("elimination"), text);
	});

	test("three series on the brink share one tail, not three", () => {
		const games = [
			seriesGame(1, "Celtics", "Knicks", 2, 2, [1, 2]),
			seriesGame(2, "Bucks", "Heat", 2, 2, [3, 4]),
			seriesGame(3, "Kings", "Suns", 2, 2, [5, 6]),
		];
		const text = dayBracketWatch(ctx(games), rngFromSeed(1))!;
		assert.strictEqual((text.match(/face elimination/g) ?? []).length, 1);
		assert.match(text, /all face elimination in Game 6/);
		for (const name of ["Knicks", "Heat", "Suns"]) {
			assert.ok(text.includes(name), text);
		}
	});

	test("a series that just ended is not in the watch", () => {
		const clinch = [seriesGame(1, "Celtics", "Knicks", 3, 1, [1, 2])];
		assert.strictEqual(dayBracketWatch(ctx(clinch), rngFromSeed(1)), undefined);
		assert.strictEqual(dayBracketWatch(ctx([]), rngFromSeed(1)), undefined);
	});
});

describe("what kind of series it has been", () => {
	beforeEach(() => endRecapBatch());

	const sg = (
		won: boolean,
		home: boolean,
		pts: number,
		oppPts: number,
		day: number,
	) => ({ won, home, pts, oppPts, day });

	const playoffGame = (
		winner: RecapTeam,
		loser: RecapTeam,
		winnerHome = true,
	) => {
		const g: RecapGame = {
			gid: 10,
			day: 100,
			overtimes: 0,
			winnerTid: winner.tid,
			playoffs: true,
			clutchPlays: [],
			teams: winnerHome ? [winner, loser] : [loser, winner],
		};
		return {
			game: g,
			ctx: {
				game: g,
				winner,
				loser,
				margin: winner.pts - loser.pts,
				said: new Set<string>(),
				written: "",
			},
		};
	};

	test("every game going to the home side is worth saying", () => {
		const w = team({
			tid: 1,
			name: "Celtics",
			pts: 105,
			seriesSoFar: [
				sg(true, true, 110, 100, 96),
				sg(false, false, 95, 105, 98),
				sg(false, false, 90, 101, 99),
			],
		});
		const l = team({ tid: 2, name: "Knicks", pts: 98 });
		const all = shapes((rng) => seriesShapeBeat(playoffGame(w, l).ctx, rng));
		assert.ok(all.size >= 3, [...all].join("\n"));
		assert.ok(
			[...all].some((s) => /home|road/i.test(s)),
			[...all].join("\n"),
		);
		noNumeralOpener(all);
	});

	test("a road win in a series breaks the pattern and says so", () => {
		const w = team({
			tid: 1,
			name: "Celtics",
			pts: 105,
			seriesSoFar: [
				sg(false, false, 95, 105, 96),
				sg(true, true, 110, 100, 98),
			],
		});
		const l = team({ tid: 2, name: "Knicks", pts: 98 });
		const text = seriesShapeBeat(playoffGame(w, l, false).ctx, rngFromSeed(1))!;
		assert.match(text, /road|away from home|floor/);
	});

	test("a series of one-possession games, and the night that ended it", () => {
		const tight = team({
			tid: 1,
			name: "Celtics",
			pts: 101,
			seriesSoFar: [
				sg(true, true, 100, 97, 96),
				sg(false, false, 99, 102, 98),
				sg(true, true, 104, 101, 99),
			],
		});
		const l = team({ tid: 2, name: "Knicks", pts: 98 });
		const all = shapes((rng) =>
			seriesShapeBeat(playoffGame(tight, l).ctx, rng),
		);
		assert.ok(
			[...all].some((s) => /five|last minute/.test(s)),
			[...all].join("\n"),
		);

		const blowout = team({
			tid: 1,
			name: "Celtics",
			pts: 120,
			seriesSoFar: [
				sg(true, true, 100, 97, 96),
				sg(false, false, 99, 102, 98),
				sg(true, true, 104, 101, 99),
			],
		});
		const wide = team({ tid: 2, name: "Knicks", pts: 96 });
		const shapes2 = shapes((rng) =>
			seriesShapeBeat(playoffGame(blowout, wide).ctx, rng),
		);
		assert.ok(
			[...shapes2].some((s) => /not close|eight|one-possession/.test(s)),
			[...shapes2].join("\n"),
		);
	});

	test("a regular-season game, or a series one game old, says nothing", () => {
		const w = team({
			tid: 1,
			name: "Celtics",
			pts: 105,
			seriesSoFar: [sg(true, true, 110, 100, 96)],
		});
		const l = team({ tid: 2, name: "Knicks", pts: 98 });
		assert.strictEqual(
			seriesShapeBeat(playoffGame(w, l).ctx, rngFromSeed(1)),
			undefined,
		);
		const regular = playoffGame(w, l);
		regular.ctx.game.playoffs = false;
		assert.strictEqual(seriesShapeBeat(regular.ctx, rngFromSeed(1)), undefined);
	});
});
