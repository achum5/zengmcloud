import { assert, describe, test } from "vitest";
import { getAutoDayRecap } from "./getAutoRecap.ts";
import type {
	RecapDayStandings,
	RecapGame,
	RecapPlayer,
	RecapTeam,
} from "./getDayGamesForRecap.ts";
import { verifyNight } from "./recapAccuracy.ts";
import {
	byTheNumbers,
	milestonesSection,
	movesSection,
	raceSection,
	streaksSection,
	worthWatching,
	type NightStory,
} from "./recapNight.ts";
import { rngFromSeed } from "./recapText.ts";

const player = (p: Partial<RecapPlayer> & { name: string }): RecapPlayer => ({
	pid: 0,
	min: 32,
	pts: 10,
	reb: 3,
	ast: 2,
	stl: 1,
	blk: 0,
	tov: 2,
	fg: 4,
	fga: 9,
	tp: 1,
	tpa: 3,
	ft: 1,
	fta: 2,
	pf: 2,
	...p,
});

const team = (
	tid: number,
	region: string,
	name: string,
	pts: number,
	ptsQtrs: number[],
	players: RecapPlayer[],
	over: Partial<RecapTeam> = {},
): RecapTeam => ({
	tid,
	region,
	name,
	abbrev: name.slice(0, 3).toUpperCase(),
	pts,
	ptsQtrs,
	players,
	...over,
});

const game = (
	gid: number,
	home: RecapTeam,
	away: RecapTeam,
	over: Partial<RecapGame> = {},
): RecapGame => ({
	gid,
	day: 40,
	overtimes: 0,
	winnerTid: home.pts > away.pts ? home.tid : away.tid,
	playoffs: false,
	teams: [home, away],
	clutchPlays: [],
	...over,
});

const story = (g: RecapGame, over: Partial<NightStory> = {}): NightStory => {
	const [home, away] = g.teams;
	const W = g.winnerTid === home.tid ? home : away;
	return {
		game: g,
		W,
		L: W === home ? away : home,
		shape: { margin: Math.abs(home.pts - away.pts), ot: 0, regPeriods: 4 },
		comeback: 0,
		...over,
	};
};

const row = (
	tid: number,
	name: string,
	won: number,
	lost: number,
	rank: number,
	over: Partial<RecapDayStandings["confs"][number]["teams"][number]> = {},
) => ({
	tid,
	rank,
	abbrev: name.slice(0, 3).toUpperCase(),
	region: name,
	name,
	won,
	lost,
	gb: 0,
	...over,
});

// Games back of the leader, as the builder figures them.
const withGb = (teams: ReturnType<typeof row>[]) =>
	teams.map((t) => ({
		...t,
		gb: (teams[0]!.won - t.won + (t.lost - teams[0]!.lost)) / 2,
	}));

describe("by the numbers", () => {
	const denver = team(
		1,
		"Denver",
		"Nuggets",
		120,
		[24, 41, 30, 25],
		[player({ name: "Devin Nash", pts: 31, fg: 12, fga: 13 })],
	);
	const boston = team(
		2,
		"Boston",
		"Celtics",
		98,
		[26, 15, 29, 28],
		[player({ name: "Trey Rivers", pts: 22, reb: 21 })],
	);
	const g1 = game(1, boston, denver, { flow: { leadChanges: 21 } as any });
	const miami = team(
		3,
		"Miami",
		"Heat",
		110,
		[40, 20, 25, 25],
		[player({ name: "Kai Holt", pts: 20, tp: 9, tpa: 15 })],
	);
	const utah = team(
		4,
		"Utah",
		"Jazz",
		101,
		[18, 30, 27, 26],
		[player({ name: "Obi Lane", pts: 19 })],
	);
	const g2 = game(2, miami, utah);

	test("the night's figures, each with what it was", () => {
		const text = byTheNumbers([story(g1), story(g2)], "", undefined)!;
		assert.match(text, /^### By the numbers\n/);
		assert.include(
			text,
			"- **41-15:** The Nuggets' second quarter at the Celtics",
		);
		assert.include(
			text,
			"- **21:** Rebounds for Trey Rivers against the Nuggets",
		);
		assert.include(
			text,
			"- **9:** 3-pointers by Kai Holt for the Heat against the Jazz",
		);
		// Two of the Denver game's four are left for its own recap: Nash's 12
		// of 13 and the 21 lead changes.
		assert.notInclude(text, "12 of 13");
		// At most two from a game.
		const fromDenver = text
			.split("\n")
			.filter((l) => /Nuggets|Celtics|Nash|Rivers/.test(l));
		assert.isAtMost(fromDenver.length, 2, text);
	});

	test("cities stand in for clubs that have one of their own", () => {
		const standings: RecapDayStandings = {
			day: 40,
			confs: [
				{
					name: "Eastern Conference",
					teams: [
						row(1, "Nuggets", 20, 10, 1, { region: "Denver" }),
						row(2, "Celtics", 18, 12, 2, { region: "Boston" }),
						row(3, "Heat", 15, 15, 3, { region: "Miami" }),
						row(4, "Jazz", 12, 18, 4, { region: "Utah" }),
					],
				},
			],
		};
		const text = byTheNumbers([story(g1), story(g2)], "", standings)!;
		assert.include(text, "Denver's second quarter at Boston");
	});

	test("nothing a paragraph above gave, and no section of fewer than three", () => {
		assert.isUndefined(
			byTheNumbers(
				[story(g1)],
				"Devin Nash went 12 of 13. A 41-15 quarter.",
				undefined,
			),
		);
	});
});

describe("streaks", () => {
	test("winning and losing, longest first, on a night with a run of four", () => {
		const standings: RecapDayStandings = {
			day: 40,
			confs: [
				{
					name: "East",
					teams: [
						row(1, "Pelicans", 30, 10, 1, { streak: { won: true, count: 6 } }),
						row(2, "Mavericks", 28, 12, 2, { streak: { won: true, count: 3 } }),
						row(3, "Hornets", 10, 30, 3, { streak: { won: false, count: 7 } }),
						row(4, "Spurs", 12, 28, 4, { streak: { won: false, count: 2 } }),
					],
				},
			],
		};
		assert.strictEqual(
			streaksSection(standings),
			"### Streaks\n- **Winning:** Pelicans 6, Mavericks 3\n- **Losing:** Hornets 7",
		);
		const quiet = structuredClone(standings);
		quiet.confs[0]!.teams[0]!.streak = { won: true, count: 3 };
		quiet.confs[0]!.teams[2]!.streak = { won: false, count: 3 };
		assert.isUndefined(streaksSection(quiet));
	});
});

describe("the race", () => {
	const night = (
		teams: ReturnType<typeof row>[],
		games: RecapGame[],
		over: Partial<RecapDayStandings> = {},
	) =>
		raceSection(
			games,
			{
				day: 100,
				numGames: 82,
				race: { byConf: true, direct: 6, reach: 10 },
				confs: [{ name: "Western Conference", teams: withGb(teams) }],
				...over,
			},
			"",
			rngFromSeed(1),
		);
	const twelve = (rows: ReturnType<typeof row>[]) => {
		const out = [...rows];
		for (let i = out.length; i < 12; i++) {
			out.push(row(100 + i, `Club${i}`, 30 - i, 40 + i, i + 1));
		}
		return out;
	};

	test("a clinch the night settled", () => {
		const den = team(
			1,
			"Denver",
			"Nuggets",
			110,
			[],
			[player({ name: "A B" })],
			{
				race: { clinched: "playoffs", firstToClinch: true },
			},
		);
		const opp = team(2, "Utah", "Jazz", 100, [], [player({ name: "C D" })]);
		const text = night(
			twelve([row(1, "Nuggets", 55, 20, 1), row(2, "Jazz", 50, 25, 2)]),
			[game(1, den, opp)],
		)!;
		assert.include(
			text,
			"- The Nuggets became the first team to clinch a playoff spot.",
		);
	});

	test("the top on a night that changed it, with a magic number late on", () => {
		const den = team(
			1,
			"Denver",
			"Nuggets",
			100,
			[],
			[player({ name: "A B" })],
		);
		const opp = team(2, "Utah", "Jazz", 110, [], [player({ name: "C D" })]);
		// A new leader, who won: the top changed and the magic number fell.
		const text = night(
			twelve([
				row(1, "Nuggets", 60, 15, 1, { rankBefore: 2 }),
				row(2, "Jazz", 55, 20, 2),
			]),
			[game(1, { ...den, pts: 120 }, opp)],
		)!;
		assert.match(
			text,
			/The Nuggets \(60-15\) (?:lead the West by 5 games over|are 5 games ahead of) the Jazz/,
		);
		// 82 + 1 - 60 - 20.
		assert.match(
			text,
			/The Nuggets' magic number for first in the West is 3\.|Any combination of three Nuggets wins or Jazz losses clinches first in the West for the Nuggets\./,
		);
		// Beaten by the runner-up: the leader line, but the magic number
		// did not move.
		const beaten = night(
			twelve([row(1, "Nuggets", 60, 15, 1), row(2, "Jazz", 55, 20, 2)]),
			[game(1, den, opp)],
		)!;
		assert.include(beaten, "Nuggets (60-15)");
		assert.notMatch(beaten, /magic number|Any combination/);
		// The leader won and was first before: no line.
		const quiet = night(
			twelve([
				row(1, "Nuggets", 40, 15, 1, { rankBefore: 1 }),
				row(2, "Jazz", 30, 25, 2),
			]),
			[game(1, { ...den, pts: 120 }, opp)],
		);
		assert.notMatch(quiet ?? "", /Nuggets/);
	});

	test("who crossed a line, and how close the last play-in spot is", () => {
		const kings = team(
			9,
			"Sacramento",
			"Kings",
			110,
			[],
			[player({ name: "A B" })],
		);
		const opp = team(50, "Utah", "Jazz", 100, [], [player({ name: "C D" })]);
		const rows = twelve([]).map((r) => ({ ...r }));
		rows[7] = row(9, "Kings", 30, 30, 8, { rankBefore: 11 });
		rows[9] = row(10, "Suns", 29, 31, 10);
		rows[10] = row(11, "Spurs", 28, 31, 11);
		const text = night(rows, [game(1, kings, opp)])!;
		assert.include(
			text,
			"The Kings moved from 11th to eighth in the West, into a play-in spot.",
		);
		assert.match(
			text,
			/The Suns hold the last play-in spot in the West by half a game over the Spurs\.|The Suns are half a game ahead of the Spurs for the last play-in spot in the West\.|The Spurs are half a game behind the Suns for the last play-in spot in the West\./,
		);
		// A night nobody near the line played: no cut line.
		const still = rows.map((r) => ({ ...r, rankBefore: r.rank }));
		const quiet = night(still, [
			game(
				1,
				kings,
				team(51, "Utah", "Jazz", 100, [], [player({ name: "C D" })]),
			),
		]);
		assert.notMatch(quiet ?? "", /last play-in spot/);
	});

	test("past six lines, a magic number gives way before a crossing", () => {
		const kings = team(
			9,
			"Sacramento",
			"Kings",
			110,
			[],
			[player({ name: "A B" })],
		);
		const den = team(
			1,
			"Denver",
			"Nuggets",
			120,
			[],
			[player({ name: "E F" })],
		);
		const opp = team(50, "Utah", "Jazz", 100, [], [player({ name: "C D" })]);
		const opp2 = team(51, "Utah", "Jazz", 100, [], [player({ name: "G H" })]);
		const west = twelve([
			row(1, "Nuggets", 60, 15, 1, { rankBefore: 2 }),
			row(2, "Lakers", 55, 20, 2),
		]).map((r) => ({ ...r }));
		west[7] = row(9, "Kings", 30, 30, 8, { rankBefore: 11 });
		west[9] = row(10, "Suns", 29, 31, 10);
		west[10] = row(11, "Spurs", 28, 31, 11);
		const east = twelve([
			row(201, "Celtics", 60, 15, 1, { rankBefore: 2 }),
			row(202, "Knicks", 55, 20, 2),
		]).map((r, i) => ({ ...r, tid: (r.tid ?? 0) + (i > 1 ? 300 : 0) }));
		east[7] = row(250, "Hawks", 30, 30, 8, { rankBefore: 11 });
		const hawks = team(
			250,
			"Atlanta",
			"Hawks",
			105,
			[],
			[player({ name: "M N" })],
		);
		const celtics = team(
			201,
			"Boston",
			"Celtics",
			120,
			[],
			[player({ name: "I J" })],
		);
		const text = night(
			west,
			[
				game(1, kings, opp),
				game(2, { ...den }, opp2),
				game(
					3,
					celtics,
					team(52, "Utah", "Jazz", 90, [], [player({ name: "K L" })]),
				),
				game(
					4,
					hawks,
					team(53, "Utah", "Jazz", 95, [], [player({ name: "O P" })]),
				),
			],
			{
				confs: [
					{ name: "Western Conference", teams: withGb(west) },
					{ name: "Eastern Conference", teams: withGb(east) },
				],
			},
		)!;
		const lines = text.split("\n").filter((l) => l.startsWith("- "));
		assert.strictEqual(lines.length, 6);
		assert.include(text, "The Kings moved from 11th to eighth in the West");
		assert.include(text, "The Hawks moved from 11th to eighth in the East");
		// Eight lines: both magic numbers give way.
		assert.notMatch(text, /magic number|Any combination/);
		assert.include(text, "The Celtics (60-15)");
	});

	test("no race in the playoffs, or without standings", () => {
		assert.isUndefined(raceSection([], undefined, "", rngFromSeed(1)));
	});
});

describe("milestones and firsts", () => {
	test("a first triple-double, a career mark and a top pick's debut", () => {
		const nuggets = team(
			1,
			"Denver",
			"Nuggets",
			110,
			[],
			[
				player({
					name: "Devin Nash",
					pts: 14,
					reb: 11,
					ast: 10,
					past: {
						gp: 90,
						high: { pts: 30, reb: 14, ast: 12, tp: 4, stl: 4, blk: 2 },
						dd: 8,
						td: 0,
					},
				}),
				player({
					name: "Old Hand",
					pts: 20,
					milestone: {
						scope: "career",
						stat: "pts",
						mark: 10000,
						total: 10012,
					},
				}),
			],
		);
		const knicks = team(
			2,
			"New York",
			"Knicks",
			100,
			[],
			[
				player({
					name: "Rook Ie",
					pts: 15,
					past: {
						gp: 0,
						high: { pts: 0, reb: 0, ast: 0, tp: 0, stl: 0, blk: 0 },
						dd: 0,
						td: 0,
					},
					draft: { year: 2025, round: 1, pick: 2 },
				}),
				player({
					name: "Bench Guy",
					pts: 8,
					milestone: { scope: "career", stat: "pts", mark: 3000, total: 3004 },
				}),
			],
		);
		const text = milestonesSection([game(1, nuggets, knicks)], "")!;
		assert.include(
			text,
			"- The Nuggets' Devin Nash had 14 points, 11 rebounds and 10 assists for his first career triple-double.",
		);
		assert.include(
			text,
			"- The Nuggets' Old Hand went past 10,000 career points.",
		);
		assert.include(
			text,
			"- The Knicks' Rook Ie, the second pick in the 2025 draft, had 15 points in his debut.",
		);
		// 3,000 career points is the game recap's, not the night's.
		assert.notInclude(text, "Bench Guy");
		// Told above: not again.
		const told = milestonesSection(
			[game(1, nuggets, knicks)],
			"Devin Nash had his first career triple-double.",
		)!;
		assert.notInclude(told, "Devin Nash");
	});
});

describe("moves", () => {
	test("a traded man's first game for his new club, and against his old one", () => {
		const nuggets = team(
			1,
			"Denver",
			"Nuggets",
			110,
			[],
			[
				player({
					name: "Franz Owens",
					pts: 14,
					reb: 6,
					past: {
						gp: 200,
						high: { pts: 30, reb: 14, ast: 12, tp: 4, stl: 4, blk: 2 },
						dd: 8,
						td: 0,
					},
					newTeam: {
						fromTid: 5,
						via: "trade",
						midSeason: true,
						region: "Boston",
						name: "Celtics",
					},
				}),
			],
		);
		const knicks = team(
			2,
			"New York",
			"Knicks",
			100,
			[],
			[
				player({
					name: "Kai Holt",
					pts: 18,
					past: {
						gp: 200,
						high: { pts: 30, reb: 14, ast: 12, tp: 4, stl: 4, blk: 2 },
						dd: 8,
						td: 0,
					},
					formerTeam: {
						fromTid: 1,
						via: "trade",
						midSeason: false,
						region: "Denver",
						name: "Nuggets",
					},
				}),
			],
		);
		const text = movesSection([game(1, nuggets, knicks)], "", rngFromSeed(1))!;
		assert.match(text, /^### Moves\n/);
		assert.include(
			text,
			"Franz Owens made his Nuggets debut after coming over from the Celtics in a trade, finishing with 14 points.",
		);
		assert.include(
			text,
			"Kai Holt was facing the Nuggets for the first time since they traded him, and had 18 points.",
		);
	});
});

describe("worth watching", () => {
	const standings: RecapDayStandings = {
		day: 50,
		race: { byConf: true, direct: 6, reach: 10 },
		confs: [
			{
				name: "Western Conference",
				teams: withGb([
					row(1, "Nuggets", 30, 10, 1, { streak: { won: true, count: 6 } }),
					row(2, "Lakers", 29, 11, 2),
					row(3, "Spurs", 22, 18, 3),
					row(4, "Kings", 10, 30, 4),
				]),
			},
		],
	};

	test("first place on the line beats a good record", () => {
		const text = worthWatching(
			{
				day: 51,
				games: [
					{ homeTid: 3, awayTid: 4 },
					{ homeTid: 1, awayTid: 2 },
				],
			},
			standings,
			rngFromSeed(1),
		)!;
		assert.strictEqual(
			text,
			"### Worth watching\n- **Lakers (29-11) at Nuggets (30-10):** First place in the West is on the line: the Nuggets lead by 1 game. The Nuggets have won six straight.",
		);
	});

	test("a leader, with how far clear", () => {
		const text = worthWatching(
			{ day: 51, games: [{ homeTid: 1, awayTid: 4 }] },
			standings,
			rngFromSeed(2),
		)!;
		assert.match(
			text,
			/The Nuggets (?:lead the West by 1 game|are 1 game clear in the West|sit 1 game up in the West) and have won six straight\./,
		);
	});

	test("nothing without a next day", () => {
		assert.isUndefined(worthWatching(undefined, standings, rngFromSeed(1)));
	});
});

describe("the night", () => {
	const slate = (): RecapGame[] => {
		const out: RecapGame[] = [];
		const names = [
			"Nuggets",
			"Celtics",
			"Heat",
			"Jazz",
			"Kings",
			"Suns",
			"Bulls",
			"Nets",
		];
		for (let i = 0; i < 4; i++) {
			const home = team(
				i * 2 + 1,
				`City${i * 2}`,
				names[i * 2]!,
				110 + i * 3,
				[28, 27, 30, 25 + i * 3],
				[
					player({
						name: `Ace ${names[i * 2]}`,
						pts: 24 + i,
						reb: 6,
						ast: 5,
						fg: 9,
						fga: 18,
					}),
				],
				{ record: { won: 20 + i, lost: 10 } },
			);
			const away = team(
				i * 2 + 2,
				`City${i * 2 + 1}`,
				names[i * 2 + 1]!,
				100 + i,
				[25, 25, 25, 25 + i],
				[
					player({
						name: `Bo ${names[i * 2 + 1]}`,
						pts: 18,
						reb: 4,
						ast: 3,
						fg: 7,
						fga: 15,
					}),
				],
				{ record: { won: 15, lost: 15 } },
			);
			out.push(game(10 + i, home, away));
		}
		return out;
	};

	test("a deck under the headline, its sections under its games, and nothing untrue", () => {
		const games = slate();
		const standings: RecapDayStandings = {
			day: 40,
			confs: [
				{
					name: "Eastern Conference",
					teams: withGb(
						games
							.flatMap((g) => g.teams)
							.map((t, i) =>
								row(t.tid, t.name, 20 - i, 10 + i, i + 1, {
									region: t.region,
									streak: { won: i < 4, count: i === 0 ? 5 : 2 },
								}),
							),
					),
				},
			],
		};
		const recap = getAutoDayRecap({
			season: 2026,
			day: 40,
			playoffs: false,
			games,
			standings,
			next: { day: 41, games: [{ homeTid: 1, awayTid: 3 }] },
		});
		const blocks = recap.split("\n\n");
		assert.match(blocks[0]!, /^\*\*.+\*\*$/);
		assert.include(recap, "### Around the league");
		assert.include(recap, "### Streaks\n- **Winning:** Nuggets 5");
		assert.include(
			recap,
			"### Worth watching\n- **Heat (18-12) at Nuggets (20-10)",
		);
		assert.deepEqual(verifyNight(recap, games, standings), []);
		// A streak that is not the standings' is caught.
		assert.isNotEmpty(
			verifyNight(recap.replace("Nuggets 5", "Nuggets 7"), games, standings),
		);
	});
});
