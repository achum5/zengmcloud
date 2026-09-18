import { assert, describe, test } from "vitest";
import type { FinishEvent, GameFlow } from "../../common/gameFlow.ts";
import { getAutoDayRecap, getAutoRecap } from "./getAutoRecap.ts";
import type {
	RecapGame,
	RecapPlayer,
	RecapTeam,
} from "./getDayGamesForRecap.ts";
import { verifyRecap } from "./recapAccuracy.ts";

// THE GAMES A LEAGUE THROWS THAT A CORPUS RARELY DOES. Each one is built by
// hand, run through the engine, read back by the accuracy reader, and
// checked for the tells of a builder that did not expect it: "undefined",
// "NaN", a doubled space or full stop, a score claimed twice, a clock in a
// game that had none.

const player = (
	name: string,
	pid: number,
	p: Partial<RecapPlayer> = {},
): RecapPlayer => ({
	name,
	pid,
	min: 30,
	pts: 10,
	reb: 4,
	ast: 2,
	stl: 0,
	blk: 0,
	tov: 1,
	fg: 4,
	fga: 9,
	tp: 0,
	tpa: 2,
	ft: 2,
	fta: 2,
	pf: 2,
	...p,
});

// Seven men whose points sum to `pts`, the top line given.
const squad = (
	base: Partial<RecapTeam> & { tid: number; name: string },
	top: Partial<RecapPlayer> & { name: string; pid: number },
	pts: number,
): RecapTeam => {
	const star = player(top.name, top.pid, top);
	const rest = pts - star.pts;
	const shares = [0.24, 0.2, 0.17, 0.15, 0.13, 0.11];
	const others = shares.map((sh, i) =>
		player(`${base.name} Man${i + 2}`, base.tid * 100 + i + 2, {
			pts: Math.round(rest * sh),
			fg: Math.round((rest * sh) / 2.3),
			fga: Math.round((rest * sh) / 1.05),
		}),
	);
	const sum = others.reduce((a, p) => a + p.pts, 0);
	others[0]!.pts += rest - sum;
	return {
		region: "",
		abbrev: base.name.slice(0, 3).toUpperCase(),
		players: [star, ...others],
		pts,
		ptsQtrs: [
			Math.round(pts * 0.26),
			Math.round(pts * 0.24),
			Math.round(pts * 0.25),
			pts -
				Math.round(pts * 0.26) -
				Math.round(pts * 0.24) -
				Math.round(pts * 0.25),
		],
		...base,
	};
};

const game = (
	teams: [RecapTeam, RecapTeam],
	over: Partial<RecapGame> = {},
): RecapGame => ({
	gid: 77,
	day: 12,
	overtimes: 0,
	winnerTid: teams[0].pts >= teams[1].pts ? teams[0].tid : teams[1].tid,
	playoffs: false,
	clutchPlays: [],
	teams,
	...over,
});

const ev = (
	side: 0 | 1,
	pid: number,
	pts: number,
	kind: FinishEvent["kind"],
	period: number,
	clock: number,
	score: [number, number],
): FinishEvent => ({ side, pid, pts, kind, period, clock, score });

// The tells of a builder that met a game it did not expect.
const clean = (recap: string) => {
	assert.notMatch(recap, /undefined|NaN|Infinity|null/, recap);
	assert.notMatch(recap, / {2}|\.\.|\s,|\ba a\b|\ban an\b/, recap);
	assert.notMatch(recap, /\b0 points\b/, recap);
	assert.notMatch(recap, /'s's|s's\b/, recap);
};

describe("edge cases", () => {
	test("a double-overtime game", () => {
		const home = squad(
			{ tid: 1, name: "Hawks", ptsQtrs: [26, 24, 25, 25, 10, 12] },
			{ name: "Ace Hawk", pid: 11, pts: 34, reb: 8, fg: 13, fga: 24 },
			122,
		);
		const away = squad(
			{ tid: 2, name: "Bulls", ptsQtrs: [24, 26, 25, 25, 10, 8] },
			{ name: "Bo Bull", pid: 21, pts: 31, reb: 6, fg: 12, fga: 22 },
			118,
		);
		const flow: GameFlow = {
			leadChanges: 14,
			ties: 12,
			maxLead: [8, 7],
			lastLead: {
				side: 0,
				pid: 11,
				period: 6,
				clock: 41,
				pts: [120, 118],
				by: 2,
			},
			lastTie: { period: 6, clock: 70, pts: 118 },
			late: [
				{ clock: 300, pts: [92, 94] },
				{ clock: 120, pts: [98, 98] },
			],
			finish: [
				ev(0, 11, 2, "rim", 4, 30, [100, 98]),
				ev(1, 21, 2, "mid", 4, 2.1, [100, 100]),
				ev(0, 12, 3, "tp", 5, 200, [103, 100]),
				ev(1, 22, 3, "tp", 5, 100, [103, 103]),
				ev(0, 11, 2, "mid", 5, 40, [105, 103]),
				ev(1, 21, 2, "rim", 5, 3, [110, 110]),
				ev(1, 21, 2, "rim", 6, 200, [110, 112]),
				ev(0, 11, 3, "tp", 6, 120, [113, 112]),
				ev(1, 22, 3, "tp", 6, 90, [118, 118]),
				ev(0, 11, 2, "mid", 6, 41, [120, 118]),
				ev(0, 12, 2, "ft", 6, 5, [122, 118]),
			],
		};
		const g = game([home, away], {
			overtimes: 2,
			flow,
			clutchPlays: [
				'<a href="#">Bo Bull</a> made a basket with 2.1 seconds remaining to force overtime.',
				'<a href="#">Bo Bull</a> made a basket with 3.0 seconds remaining to force a second overtime.',
			],
		});
		const recap = getAutoRecap(g);
		clean(recap);
		assert.match(
			recap,
			/2OT|double overtime|2 extra periods|two extra periods/,
		);
		assert.match(recap, /Ace Hawk's jumper .*41 seconds/);
		assert.deepEqual(verifyRecap(recap, g), []);
	});

	test("an Elam ending has no clock, and the recap never invents one", () => {
		const home = squad(
			{ tid: 1, name: "Hawks", ptsQtrs: [26, 24, 25, 30] },
			{ name: "Ace Hawk", pid: 11, pts: 28 },
			105,
		);
		const away = squad(
			{ tid: 2, name: "Bulls", ptsQtrs: [24, 26, 25, 26] },
			{ name: "Bo Bull", pid: 21, pts: 25 },
			101,
		);
		const flow: GameFlow = {
			leadChanges: 9,
			ties: 6,
			maxLead: [7, 5],
			lastLead: {
				side: 0,
				pid: 11,
				period: 4,
				clock: Infinity,
				pts: [102, 101],
				by: 2,
			},
			lastTie: { period: 4, clock: Infinity, pts: 99 },
		};
		const recap = getAutoRecap(game([home, away], { flow }));
		clean(recap);
		assert.notMatch(recap, /seconds|:\d\d (?:left|to go|to play)|two minutes/);
		assert.deepEqual(verifyRecap(recap, game([home, away], { flow })), []);
	});

	test("a two-period game still has a halftime, a three-period one does not", () => {
		const two = game([
			squad(
				{ tid: 1, name: "Hawks", ptsQtrs: [50, 52] },
				{ name: "Ace Hawk", pid: 11, pts: 24 },
				102,
			),
			squad(
				{ tid: 2, name: "Bulls", ptsQtrs: [48, 47] },
				{ name: "Bo Bull", pid: 21, pts: 22 },
				95,
			),
		]);
		const recap2 = getAutoRecap(two);
		clean(recap2);
		assert.match(recap2, /50-48/);
		assert.deepEqual(verifyRecap(recap2, two), []);
		const three = game([
			squad(
				{ tid: 1, name: "Hawks", ptsQtrs: [34, 33, 35] },
				{ name: "Ace Hawk", pid: 11, pts: 24 },
				102,
			),
			squad(
				{ tid: 2, name: "Bulls", ptsQtrs: [30, 32, 33] },
				{ name: "Bo Bull", pid: 21, pts: 22 },
				95,
			),
		]);
		const recap3 = getAutoRecap(three);
		clean(recap3);
		assert.notMatch(recap3, /halftime|at the break|at the half/);
		assert.deepEqual(verifyRecap(recap3, three), []);
	});

	test("no quarters, no flow, no records: the first game a league ever stored", () => {
		const home = squad(
			{ tid: 1, name: "Hawks", ptsQtrs: undefined },
			{ name: "Ace Hawk", pid: 11, pts: 24 },
			102,
		);
		const away = squad(
			{ tid: 2, name: "Bulls", ptsQtrs: undefined },
			{ name: "Bo Bull", pid: 21, pts: 22 },
			95,
		);
		const g = game([home, away]);
		const recap = getAutoRecap(g);
		clean(recap);
		assert.notMatch(recap, /\(\d+-\d+\)|halftime|quarter/);
		assert.match(recap, /Hawks .*Bulls|Bulls .*Hawks/);
		assert.deepEqual(verifyRecap(recap, g), []);
	});

	test("a 52-point night on the losing side is the story", () => {
		const home = squad(
			{ tid: 1, name: "Hawks" },
			{ name: "Ace Hawk", pid: 11, pts: 24, fg: 9, fga: 18 },
			110,
		);
		const away = squad(
			{ tid: 2, name: "Bulls" },
			{
				name: "Bo Bull",
				pid: 21,
				pts: 52,
				reb: 9,
				fg: 19,
				fga: 33,
				tp: 6,
				tpa: 12,
			},
			104,
		);
		const g = game([home, away]);
		const recap = getAutoRecap(g);
		clean(recap);
		assert.match(
			recap,
			/despite .*52|52 .*not enough|Bo Bull poured in 52|52 points from Bo Bull/,
		);
		assert.deepEqual(verifyRecap(recap, g), []);
	});

	test("a mononym and an all-free-throw scorer", () => {
		const home = squad(
			{ tid: 1, name: "Hawks" },
			{
				name: "Nene",
				pid: 11,
				pts: 16,
				fg: 0,
				fga: 0,
				ft: 16,
				fta: 18,
				reb: 11,
			},
			98,
		);
		const away = squad(
			{ tid: 2, name: "Bulls" },
			{ name: "Bo Bull", pid: 21, pts: 22 },
			90,
		);
		const g = game([home, away]);
		const recap = getAutoRecap(g);
		clean(recap);
		assert.notMatch(recap, /0-of-0|Nene's's/);
		assert.deepEqual(verifyRecap(recap, g), []);
	});

	test("two men at 16 apiece: the lede shares it, the body does not introduce the second twice", () => {
		const home = squad(
			{ tid: 1, name: "Kings" },
			{ name: "Ace King", pid: 11, pts: 16, reb: 5, ast: 3 },
			100,
		);
		home.players[1] = player("Bo King", 12, {
			pts: 16,
			reb: 4,
			ast: 4,
			fg: 6,
			fga: 12,
		});
		home.players[2]!.pts = 14;
		const total = home.players.reduce((a, p) => a + p.pts, 0);
		home.players[6]!.pts += 100 - total;
		const away = squad(
			{ tid: 2, name: "Bulls" },
			{ name: "Cy Bull", pid: 21, pts: 20 },
			90,
		);
		const g = game([home, away]);
		let shared = 0;
		for (let gid = 1; gid <= 8; gid++) {
			const recap = getAutoRecap({ ...g, gid });
			clean(recap);
			if (/16 points apiece/.test(recap)) {
				shared += 1;
				const body = recap.split("\n\n").slice(1).join(" ");
				assert.strictEqual((body.match(/Bo King/g) ?? []).length, 1, recap);
			}
			assert.deepEqual(verifyRecap(recap, { ...g, gid }), []);
		}
		assert.ok(shared >= 4, `${shared}`);
	});

	test("seven in double figures behind a quiet leader is a team night, said once", () => {
		const home = squad(
			{ tid: 1, name: "Pacers" },
			{ name: "Ace Pacer", pid: 11, pts: 14, reb: 12, fg: 5, fga: 16 },
			108,
		);
		for (const [i, pts] of [16, 15, 14, 13, 12, 11].entries()) {
			home.players[i + 1]!.pts = pts;
		}
		home.players[6]!.pts =
			108 - home.players.slice(0, 6).reduce((a, p) => a + p.pts, 0);
		const away = squad(
			{ tid: 2, name: "Bulls" },
			{ name: "Cy Bull", pid: 21, pts: 22 },
			93,
		);
		const g = game([home, away]);
		const recap = getAutoRecap(g);
		clean(recap);
		assert.match(recap, /double figures/);
		assert.strictEqual((recap.match(/double figures/g) ?? []).length, 1, recap);
		assert.deepEqual(verifyRecap(recap, g), []);
	});

	test("a tie is a tie, a shootout is a shootout", () => {
		const home = squad(
			{ tid: 1, name: "Hawks" },
			{ name: "Ace Hawk", pid: 11, pts: 24 },
			100,
		);
		const away = squad(
			{ tid: 2, name: "Bulls" },
			{ name: "Bo Bull", pid: 21, pts: 22 },
			100,
		);
		const tie = game([home, away], { overtimes: 1, winnerTid: 1 });
		const recap = getAutoRecap(tie);
		clean(recap);
		assert.match(recap, /tie/);
		assert.notMatch(recap, /beat|topped|held off|edged/);
		assert.strictEqual((recap.match(/100-100/g) ?? []).length, 1, recap);
		assert.deepEqual(verifyRecap(recap, tie), []);

		const so = game([home, away], {
			overtimes: 1,
			winnerTid: 2,
			shootout: { won: 3, lost: 2 },
		});
		const soRecap = getAutoRecap(so);
		clean(soRecap);
		assert.match(soRecap, /shootout/);
		assert.match(soRecap, /Bulls beat the Hawks|the Bulls took the shootout/);
		assert.deepEqual(verifyRecap(soRecap, so), []);

		// And the wrap tells it plainly.
		const wrap = getAutoDayRecap({
			season: 2025,
			day: 12,
			playoffs: false,
			games: [tie, { ...so, gid: 78 }],
		});
		clean(wrap);
		assert.match(wrap, /tied 100-100|100-100 tie/);
		assert.match(wrap, /shootout/);
	});

	test("a game-winner in overtime after a tying shot in regulation", () => {
		const home = squad(
			{ tid: 1, name: "Hawks", ptsQtrs: [26, 24, 25, 25, 12] },
			{ name: "Ace Hawk", pid: 11, pts: 30, fg: 12, fga: 22 },
			112,
		);
		const away = squad(
			{ tid: 2, name: "Bulls", ptsQtrs: [24, 26, 25, 25, 10] },
			{ name: "Bo Bull", pid: 21, pts: 28, fg: 11, fga: 20 },
			110,
		);
		const flow: GameFlow = {
			leadChanges: 11,
			ties: 9,
			maxLead: [8, 7],
			lastLead: {
				side: 0,
				pid: 11,
				period: 5,
				clock: 1.4,
				pts: [112, 110],
				by: 2,
			},
			lastTie: { period: 5, clock: 20, pts: 110 },
			late: [
				{ clock: 300, pts: [92, 94] },
				{ clock: 120, pts: [98, 98] },
			],
			finish: [
				ev(1, 21, 2, "mid", 4, 4.0, [100, 100]),
				ev(0, 12, 2, "rim", 5, 200, [102, 100]),
				ev(1, 22, 2, "rim", 5, 150, [102, 102]),
				ev(0, 11, 3, "tp", 5, 100, [105, 102]),
				ev(1, 21, 3, "tp", 5, 60, [105, 105]),
				ev(0, 11, 2, "ft", 5, 40, [107, 105]),
				ev(1, 22, 3, "tp", 5, 30, [107, 108]),
				ev(0, 12, 3, "tp", 5, 24, [110, 108]),
				ev(1, 21, 2, "rim", 5, 20, [110, 110]),
				ev(0, 11, 2, "mid", 5, 1.4, [112, 110]),
			],
		};
		const g = game([home, away], {
			overtimes: 1,
			flow,
			clutchPlays: [
				'<a href="#">Bo Bull</a> made a basket with 4.0 seconds remaining to force overtime.',
				'<a href="#">Ace Hawk</a> made a game-winning basket with 1.4 seconds remaining.',
			],
		});
		const recap = getAutoRecap(g);
		clean(recap);
		assert.match(recap, /Ace Hawk .*jumper with 1\.4 seconds/);
		assert.match(recap, /Bo Bull.*(overtime|extra period)/);
		assert.deepEqual(verifyRecap(recap, g), []);
	});

	test("a blowout says how big it got, and a skid says how long", () => {
		const home = squad(
			{ tid: 1, name: "Spurs", ptsQtrs: [36, 30, 33, 28] },
			{ name: "Ace Spur", pid: 11, pts: 26, fg: 9, fga: 14 },
			127,
		);
		const away = squad(
			{
				tid: 2,
				name: "Blazers",
				ptsQtrs: [22, 24, 20, 30],
				streak: { won: false, count: 5 },
			},
			{ name: "Bo Blazer", pid: 21, pts: 19, reb: 12 },
			96,
		);
		const flow: GameFlow = { leadChanges: 0, ties: 0, maxLead: [38, 0] };
		const g = game([home, away], { flow });
		let bigLead = 0;
		let skid = 0;
		for (let gid = 1; gid <= 10; gid++) {
			const recap = getAutoRecap({ ...g, gid });
			clean(recap);
			if (
				/as many as 38|lead reached 38|up by 38 at their biggest/.test(recap)
			) {
				bigLead += 1;
			}
			if (
				/fifth straight loss|lost five in a row|fifth in a row|5 straight losses/.test(
					recap,
				)
			) {
				skid += 1;
			}
			assert.deepEqual(verifyRecap(recap, { ...g, gid }), []);
		}
		assert.ok(bigLead >= 8, `bigLead ${bigLead}`);
		assert.ok(skid >= 8, `skid ${skid}`);
	});
	test("a rookie is called one, and a veteran's age rides on his name", () => {
		const rookie = squad(
			{ tid: 1, name: "Hawks" },
			{
				name: "Ace Hawk",
				pid: 11,
				pts: 26,
				reb: 6,
				fg: 10,
				fga: 18,
				rookie: true,
			},
			104,
		);
		const away = squad(
			{ tid: 2, name: "Bulls" },
			{ name: "Bo Bull", pid: 21, pts: 22 },
			96,
		);
		let called = 0;
		for (let gid = 1; gid <= 20; gid++) {
			const recap = getAutoRecap(game([rookie, away], { gid }));
			clean(recap);
			if (/Rookie Ace Hawk/.test(recap)) {
				called += 1;
			}
			assert.deepEqual(verifyRecap(recap, game([rookie, away], { gid })), []);
		}
		assert.ok(called >= 3 && called <= 17, `${called}`);

		const vet = squad(
			{ tid: 1, name: "Hawks" },
			{ name: "Ace Hawk", pid: 11, pts: 26, reb: 6, fg: 10, fga: 18, age: 36 },
			104,
		);
		let aged = 0;
		for (let gid = 1; gid <= 20; gid++) {
			const recap = getAutoRecap(game([vet, away], { gid }));
			clean(recap);
			if (/Ace Hawk, 36,/.test(recap)) {
				aged += 1;
			}
		}
		assert.ok(aged >= 2 && aged <= 14, `${aged}`);
	});

	test("the Nth 30-point game of the season, once the count means something", () => {
		const home = squad(
			{ tid: 1, name: "Hawks" },
			{
				name: "Ace Hawk",
				pid: 11,
				pts: 33,
				reb: 6,
				fg: 12,
				fga: 22,
				entering: {
					gp: 20,
					high: { pts: 38, reb: 12, ast: 8, tp: 6, stl: 3, blk: 2 },
					totals: { pts: 500, reb: 120, ast: 80, tp: 40, stl: 20, blk: 10 },
					streaks: { twenty: 2, thirty: 0, doubleDouble: 0 },
					counts: { twenty: 12, thirty: 5, forty: 0 },
				},
			},
			110,
		);
		const away = squad(
			{ tid: 2, name: "Bulls" },
			{ name: "Bo Bull", pid: 21, pts: 22 },
			98,
		);
		let counted = 0;
		for (let gid = 1; gid <= 10; gid++) {
			const recap = getAutoRecap(game([home, away], { gid }));
			clean(recap);
			if (
				/sixth 30-point game|six 30-point games|six games of 30 or more/.test(
					recap,
				)
			) {
				counted += 1;
			}
			assert.deepEqual(verifyRecap(recap, game([home, away], { gid })), []);
		}
		assert.ok(counted >= 5, `${counted}`);
	});
	test("the reader holds a streak, a count and a high against what he carried in", () => {
		const home = squad(
			{ tid: 1, name: "Hawks" },
			{
				name: "Ace Hawk",
				pid: 11,
				pts: 33,
				entering: {
					gp: 20,
					high: { pts: 31, reb: 12, ast: 8, tp: 6, stl: 3, blk: 2 },
					totals: { pts: 500, reb: 120, ast: 80, tp: 40, stl: 20, blk: 10 },
					streaks: { twenty: 4, thirty: 1, doubleDouble: 0 },
					counts: { twenty: 12, thirty: 5, forty: 0 },
				},
			},
			110,
		);
		const away = squad(
			{ tid: 2, name: "Bulls" },
			{ name: "Bo Bull", pid: 21, pts: 22 },
			98,
		);
		const g = game([home, away]);
		const wrong = [
			"Ace Hawk has not been held under 20 in seven games.",
			"It was Ace Hawk's fifth 30-point game of the season.",
			"Ace Hawk had not scored more than 30 in a game this season.",
		].join(" ");
		const found = verifyRecap(`**x**\n\n${wrong}`, g).map((v) => v.kind);
		assert.deepEqual(found, ["streak of 20", "count of 30", "season high"]);
		const right = [
			"Ace Hawk has not been held under 20 in five games.",
			"It was his sixth 30-point game of the season.",
			"He had not scored more than 31 in a game this season.",
		].join(" ");
		assert.deepEqual(verifyRecap(`**x**\n\n${right}`, g), []);
	});

	test("the reader holds series claims against the bracket", () => {
		const home = squad(
			{ tid: 1, name: "Hawks", ptsQtrs: [28, 26, 27, 29] },
			{ name: "Ace Hawk", pid: 11, pts: 30, fg: 12, fga: 22 },
			110,
		);
		const away = squad(
			{ tid: 2, name: "Kings", ptsQtrs: [24, 25, 26, 27] },
			{ name: "Rex King", pid: 21, pts: 28, fg: 11, fga: 21 },
			102,
		);
		// Hawks win Game 4 to lead the series 3-1: entering 2-1, best of 7.
		const g = game([home, away], {
			playoffs: true,
			series: {
				round: 1,
				numRounds: 4,
				bestOf: 7,
				homeAbbrev: home.abbrev,
				awayAbbrev: away.abbrev,
				homeSeed: 2,
				awaySeed: 7,
				homeWon: 2,
				awayWon: 1,
			},
		});

		const right = [
			"The Hawks beat the Kings 110-102 in Game 4 of the First Round.",
			"They take a 3-1 series lead.",
			"The #2 seed is one win from putting out the #7 seed.",
			"Game 5 is tomorrow in Atlanta.",
		].join(" ");
		assert.deepEqual(verifyRecap(`**x**\n\n${right}`, g), []);

		const wrong = [
			"The Hawks beat the Kings 110-102 in Game 5 of the First Round.",
			"They take a 3-2 series lead.",
			"The #3 seed is two wins from putting out the #7 seed.",
			"Game 6 is tomorrow in Atlanta.",
		].join(" ");
		const kinds = verifyRecap(`**x**\n\n${wrong}`, g).map((v) => v.kind);
		assert.includeMembers(kinds, [
			"series game number",
			"series score",
			"series seed",
			"series wins remaining",
			"series next game",
		]);
	});
});
