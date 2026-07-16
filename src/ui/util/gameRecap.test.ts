import { assert, describe, test } from "vitest";
import { buildRecapPrompt, parseRecaps } from "./gameRecap.ts";
import type {
	RecapGame,
	RecapPlayer,
} from "../../worker/util/getDayGamesForRecap.ts";

let nextPid = 1;
const player = (
	name: string,
	pts: number,
	extra: Record<string, number> = {},
) => ({
	name,
	pid: nextPid++,
	min: 34,
	pts,
	reb: 8,
	ast: 5,
	stl: 1,
	blk: 0,
	tov: 2,
	fg: 10,
	fga: 18,
	tp: 2,
	tpa: 5,
	ft: 4,
	fta: 4,
	pf: 2,
	...extra,
});

const game = (gid: number): RecapGame => ({
	gid,
	day: 7,
	overtimes: 1,
	winnerTid: 0,
	playoffs: false,
	teams: [
		{
			tid: 0,
			region: "Brooklyn",
			name: "Bagels",
			abbrev: "BKN",
			pts: 105,
			players: [player("Star Guy", 30), player("Role Guy", 12)],
		},
		{
			tid: 1,
			region: "Boston",
			name: "Massacre",
			abbrev: "BOS",
			pts: 103,
			players: [player("Their Ace", 28)],
		},
	],
	clutchPlays: ['<a href="/l/1/player/5">Star Guy</a> hit the game-winner.'],
});

describe("buildRecapPrompt", () => {
	test("bakes in ids, scores, stat lines, and cleaned highlights", () => {
		const prompt = buildRecapPrompt([game(42)], "Day 7");
		// Marker id the AI must echo back.
		assert.ok(prompt.includes("GAME 42"), prompt);
		// Both teams + scores.
		assert.ok(prompt.includes("BKN 105") || prompt.includes("105"), prompt);
		assert.ok(prompt.includes("Brooklyn Bagels"), prompt);
		assert.ok(prompt.includes("Boston Massacre"), prompt);
		// A stat line.
		assert.ok(prompt.includes("Star Guy: 30 PTS"), prompt);
		// Highlight, with the HTML link stripped to plain text.
		assert.ok(prompt.includes("Star Guy hit the game-winner."), prompt);
		assert.ok(!prompt.includes("<a href"), prompt);
	});
});

describe("buildRecapPrompt — rich context", () => {
	test("bakes in records, quarter scoring, last-10, averages, and playoff series", () => {
		const rich: RecapGame = {
			gid: 1,
			day: 90,
			overtimes: 0,
			winnerTid: 0,
			playoffs: true,
			series: {
				round: 2,
				numRounds: 4,
				homeAbbrev: "BOS",
				awayAbbrev: "BKN",
				homeSeed: 1,
				awaySeed: 8,
				homeWon: 2,
				awayWon: 1,
			},
			teams: [
				{
					tid: 0,
					region: "Brooklyn",
					name: "Bagels",
					abbrev: "BKN",
					pts: 100,
					record: { won: 50, lost: 32 },
					ptsQtrs: [25, 20, 30, 25],
					streak: { won: true, count: 3 },
					injuries: [
						{ name: "Hurt Guy", type: "Sprained Ankle", gamesRemaining: 4 },
					],
					seed: 8,
					last10: [
						{ opp: "BOS", home: false, won: true, pts: 100, oppPts: 98 },
					],
					players: [
						{
							name: "Star",
							pid: 1,
							min: 38,
							pts: 35,
							reb: 10,
							ast: 8,
							stl: 2,
							blk: 1,
							tov: 3,
							fg: 12,
							fga: 22,
							tp: 4,
							tpa: 9,
							ft: 7,
							fta: 8,
							pf: 2,
							seasonAvg: {
								gp: 80,
								min: 36,
								pts: 28,
								reb: 8,
								ast: 6,
								stl: 1.5,
								blk: 0.5,
								tov: 2.5,
								fgp: 47,
								tpp: 38,
								ftp: 85,
							},
							career: [
								{
									season: 2025,
									age: 27,
									teams: ["BKN"],
									gp: 70,
									min: 34,
									pts: 24,
									reb: 7,
									ast: 5,
									stl: 1.4,
									blk: 0.4,
									tov: 2.3,
									fgp: 45,
									tpp: 36,
									ftp: 83,
								},
							],
						},
					],
				},
				{
					tid: 1,
					region: "Boston",
					name: "Massacre",
					abbrev: "BOS",
					pts: 98,
					players: [],
				},
			],
			clutchPlays: [],
		};
		const prompt = buildRecapPrompt([rich], "Day 90");
		assert.ok(prompt.includes("50-32, W3"), prompt); // record + streak
		assert.ok(prompt.includes("By quarter: 25 | 20 | 30 | 25"), prompt);
		assert.ok(prompt.includes("Last 10 (1-0)"), prompt);
		assert.ok(prompt.includes("Season avg entering this game:"), prompt);
		assert.ok(prompt.includes("Career by season (past seasons):"), prompt);
		assert.ok(prompt.includes("2025 (BKN, age 27)"), prompt); // team + age
		assert.ok(prompt.includes("Round 2 of 4"), prompt); // playoff series
		assert.ok(prompt.includes("#8 seed"), prompt);
		assert.ok(prompt.includes("Out (injury): Hurt Guy"), prompt); // injuries
		// 35 PTS, 10 REB → a double-double, tagged so the AI can't misread it.
		assert.ok(prompt.includes("[double-double: PTS, REB]"), prompt);
		// The accuracy guardrail must be present.
		assert.ok(prompt.includes("ACCURACY IS THE TOP PRIORITY"), prompt);
	});
});

describe("double-double milestone tags", () => {
	const base = {
		gid: 1,
		day: 1,
		overtimes: 0,
		winnerTid: 0,
		playoffs: false,
		clutchPlays: [] as string[],
	};
	const mkPlayer = (stats: Partial<RecapPlayer> & { name: string }) => ({
		pid: 1,
		min: 34,
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
		...stats,
	});
	// The player's own line in the prompt. Negative assertions must scope to this
	// (not the whole prompt), since the accuracy instructions legitimately mention
	// "triple-double", "quintuple-double", etc.
	const lineFor = (p: { name: string }): string => {
		const line = buildRecapPrompt(
			[
				{
					...base,
					teams: [
						{
							tid: 0,
							region: "A",
							name: "A",
							abbrev: "AAA",
							pts: 100,
							players: [p],
						},
						{
							tid: 1,
							region: "B",
							name: "B",
							abbrev: "BBB",
							pts: 90,
							players: [],
						},
					],
				} as any,
			],
			"Day 1",
		)
			.split("\n")
			.find((l) => l.includes(`${p.name}:`));
		assert.ok(line, "player line should exist");
		return line!;
	};

	test("a true triple-double is tagged as such (Pryor case)", () => {
		// 21/13/10 with 3 STL, 5 BLK — a triple-double, NOT near a quintuple-double.
		const line = lineFor(
			mkPlayer({ name: "Pryor", pts: 21, reb: 13, ast: 10, stl: 3, blk: 5 }),
		);
		assert.ok(line.includes("[triple-double: PTS, REB, AST]"), line);
		assert.ok(!line.includes("quintuple"), line);
	});

	test("a double-double is not upgraded (Allen case)", () => {
		// 19/3/14 — a double-double (PTS, AST), not triple-double-caliber.
		const line = lineFor(mkPlayer({ name: "Allen", pts: 19, reb: 3, ast: 14 }));
		assert.ok(line.includes("[double-double: PTS, AST]"), line);
		assert.ok(!line.includes("triple"), line);
	});

	test("no tag when fewer than two categories reach 10", () => {
		const line = lineFor(
			mkPlayer({ name: "Role", pts: 9, reb: 9, ast: 9, stl: 9, blk: 9 }),
		);
		assert.ok(!line.includes("double"), line);
	});

	test("a genuine quintuple-double is labeled correctly", () => {
		const line = lineFor(
			mkPlayer({ name: "Unicorn", pts: 10, reb: 10, ast: 10, stl: 10, blk: 10 }),
		);
		assert.ok(line.includes("[quintuple-double: PTS, REB, AST, STL, BLK]"), line);
	});
});

describe("buildRecapPrompt — play-in games", () => {
	const playInGame = (playIn: RecapGame["playIn"]): RecapGame => ({
		gid: 5,
		day: 88,
		overtimes: 0,
		winnerTid: 0,
		playoffs: true,
		playIn,
		teams: [
			{
				tid: 0,
				region: "Brooklyn",
				name: "Bagels",
				abbrev: "BKN",
				pts: 110,
				players: [],
			},
			{
				tid: 1,
				region: "Boston",
				name: "Massacre",
				abbrev: "BOS",
				pts: 104,
				players: [],
			},
		],
		clutchPlays: [],
	});

	test("7-vs-8 game is framed as a play-in with the seed on the line, not a series", () => {
		const prompt = buildRecapPrompt(
			[
				playInGame({
					kind: "seed7v8",
					homeAbbrev: "BKN",
					awayAbbrev: "BOS",
					homeSeed: 7,
					awaySeed: 8,
					prizeSeed: 7,
				}),
			],
			"Day 88",
		);
		assert.ok(prompt.includes("Play-In Tournament"), prompt);
		assert.ok(prompt.includes("#7 seed"), prompt);
		assert.ok(prompt.includes("final play-in game"), prompt);
		// Must NOT masquerade as a normal playoff series.
		assert.ok(!prompt.includes("Playoffs — Round"), prompt);
	});

	test("9-vs-10 game frames elimination stakes", () => {
		const prompt = buildRecapPrompt(
			[
				playInGame({
					kind: "seed9v10",
					homeAbbrev: "BKN",
					awayAbbrev: "BOS",
					homeSeed: 9,
					awaySeed: 10,
				}),
			],
			"Day 88",
		);
		assert.ok(prompt.includes("Play-In Tournament"), prompt);
		assert.ok(prompt.includes("eliminated"), prompt);
	});

	test("final play-in game frames the last playoff spot", () => {
		const prompt = buildRecapPrompt(
			[
				playInGame({
					kind: "final",
					homeAbbrev: "BKN",
					awayAbbrev: "BOS",
					homeSeed: 8,
					awaySeed: 9,
					prizeSeed: 8,
				}),
			],
			"Day 88",
		);
		assert.ok(prompt.includes("Play-In Tournament"), prompt);
		assert.ok(prompt.includes("last playoff spot"), prompt);
		assert.ok(prompt.includes("#8 seed"), prompt);
	});
});

describe("buildRecapPrompt — pregame spread", () => {
	const withSpread = (spread: RecapGame["spread"]): RecapGame => ({
		...game(9),
		spread,
	});

	test("names the favorite and the number", () => {
		const prompt = buildRecapPrompt(
			[withSpread({ favTid: 1, points: 6.5 })],
			"Day 7",
		);
		assert.ok(prompt.includes("Pregame line:"), prompt);
		// tid 1 in game() is the Boston Massacre.
		assert.ok(prompt.includes("Boston Massacre favored by 6.5"), prompt);
	});

	test("a pick'em is framed as evenly matched", () => {
		const prompt = buildRecapPrompt(
			[withSpread({ favTid: 0, points: 0 })],
			"Day 7",
		);
		assert.ok(prompt.includes("pick'em"), prompt);
	});

	test("no spread line when the game has no spread", () => {
		const prompt = buildRecapPrompt([game(9)], "Day 7");
		assert.ok(!prompt.includes("Pregame line:"), prompt);
	});
});

describe("parseRecaps", () => {
	test("files each recap to its game id, ignoring preamble", () => {
		const text = `Here are your recaps!

<!--game:42-->
**Bagels edge Massacre in OT**

Brooklyn survived a thriller.

<!--game:43-->
**Blowout in Boston**

Not close.`;
		const { games: map } = parseRecaps(text);
		assert.strictEqual(map.size, 2);
		assert.ok(
			map.get(42)!.startsWith("**Bagels edge Massacre in OT**"),
			map.get(42),
		);
		assert.ok(map.get(42)!.includes("Brooklyn survived a thriller."));
		assert.ok(map.get(43)!.includes("Not close."));
		// Preamble before the first marker is dropped.
		assert.ok(!map.get(42)!.includes("Here are your recaps"));
	});

	test("tolerates spacing variations in the marker", () => {
		const { games: map } = parseRecaps("<!-- game: 7 -->\nRecap seven.");
		assert.strictEqual(map.get(7), "Recap seven.");
	});

	test("no markers → empty result (so the UI can warn)", () => {
		const { dayRecaps, games } = parseRecaps(
			"The AI forgot the markers entirely.",
		);
		assert.strictEqual(games.size, 0);
		assert.strictEqual(dayRecaps.size, 0);
	});

	test("peels an outer ```markdown fence off a selected-all paste", () => {
		const text = [
			"```markdown",
			"<!--game:42-->",
			"**Bagels edge Massacre in OT**",
			"",
			"Brooklyn survived a thriller.",
			"```",
		].join("\n");
		const { games: map } = parseRecaps(text);
		assert.strictEqual(map.size, 1);
		// The stray closing ``` must NOT end up in the stored recap.
		assert.ok(!map.get(42)!.includes("`"), map.get(42));
		assert.ok(map.get(42)!.startsWith("**Bagels edge Massacre in OT**"));
		assert.ok(map.get(42)!.endsWith("Brooklyn survived a thriller."));
	});

	test("extracts a <!--day:DAY--> recap, keyed by its league day", () => {
		const text = [
			"```markdown",
			"<!--day:7-->",
			"**A wild night across the league**",
			"",
			"Upsets everywhere as the standings shuffled.",
			"",
			"<!--game:42-->",
			"**Bagels edge Massacre in OT**",
			"",
			"Brooklyn survived a thriller.",
			"```",
		].join("\n");
		const { dayRecaps, games } = parseRecaps(text);
		assert.strictEqual(dayRecaps.size, 1);
		assert.ok(dayRecaps.get(7)!.startsWith("**A wild night across the league**"));
		assert.ok(dayRecaps.get(7)!.includes("standings shuffled."));
		// The day recap must NOT bleed into the game recap, or vice versa.
		assert.ok(!dayRecaps.get(7)!.includes("Brooklyn"));
		assert.strictEqual(games.size, 1);
		assert.ok(games.get(42)!.startsWith("**Bagels edge Massacre in OT**"));
		assert.ok(!games.get(42)!.includes("wild night"));
	});

	test("backfills MULTIPLE days in one paste, each filed to its own day", () => {
		const text = [
			"<!--day:5-->",
			"**Day five**",
			"",
			"Slow start to the week.",
			"",
			"<!--day:6-->",
			"**Day six**",
			"",
			"Things heated up.",
		].join("\n");
		const { dayRecaps } = parseRecaps(text);
		assert.strictEqual(dayRecaps.size, 2);
		assert.ok(dayRecaps.get(5)!.includes("Slow start"));
		assert.ok(dayRecaps.get(6)!.includes("Things heated up"));
		// No cross-contamination between the two days.
		assert.ok(!dayRecaps.get(5)!.includes("heated up"));
	});

	test("a day recap with no game recaps still parses", () => {
		const { dayRecaps, games } = parseRecaps(
			"<!--day:3-->\n**Quiet day**\n\nNot much happened.",
		);
		assert.strictEqual(games.size, 0);
		assert.ok(dayRecaps.get(3)!.includes("Not much happened."));
	});
});

describe("buildRecapPrompt — day recaps", () => {
	const game = {
		gid: 1,
		day: 4,
		overtimes: 0,
		winnerTid: 0,
		playoffs: false,
		clutchPlays: [],
		teams: [
			{ tid: 0, abbrev: "A", region: "A", name: "A", pts: 100, players: [] },
			{ tid: 1, abbrev: "B", region: "B", name: "B", pts: 90, players: [] },
		],
	} as any;

	test("lists the days needing a recap so the AI files each to the right day", () => {
		const prompt = buildRecapPrompt([game], "Days 4–6", [4, 5, 6]);
		assert.ok(prompt.includes("Day recaps needed (oldest first): 4, 5, 6"));
	});

	test("says 'none' when no day recaps are needed", () => {
		const prompt = buildRecapPrompt([game], "Day 4", []);
		assert.ok(prompt.includes("Day recaps needed: none"));
	});
});

describe("buildRecapPrompt — fenced output", () => {
	test("asks the AI to wrap the whole reply in one markdown fence", () => {
		const prompt = buildRecapPrompt(
			[
				{
					gid: 1,
					teams: [
						{
							tid: 0,
							abbrev: "A",
							region: "A",
							name: "A",
							pts: 1,
							players: [],
						},
						{
							tid: 1,
							abbrev: "B",
							region: "B",
							name: "B",
							pts: 2,
							players: [],
						},
					],
					winnerTid: 1,
					overtimes: 0,
					clutchPlays: [],
				},
			] as any,
			"Day 1",
		);
		assert.ok(prompt.includes("```markdown"), prompt);
		assert.ok(prompt.includes("ONE fenced code block"), prompt);
	});
});
