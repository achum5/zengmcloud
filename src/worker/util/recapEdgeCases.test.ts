import { assert, describe, test } from "vitest";
import type { FinishEvent, GameFlow } from "../../common/gameFlow.ts";
import {
	beginRecapBatch,
	endRecapBatch,
	getAutoDayRecap,
	getAutoRecap,
} from "./getAutoRecap.ts";
import {
	aFigure,
	aNum,
	aOrdinal,
	fixArticles,
	injuryPhrase,
	ordinal,
	pickByDay,
} from "./recapText.ts";
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
		assert.match(
			recap,
			/Ace Hawk(?:'s jumper| answered with a jumper) .*41 seconds/,
		);
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

	test("the desk rejects any claim about the calendar", () => {
		const home = squad(
			{ tid: 1, name: "Hawks" },
			{ name: "Ace Hawk", pid: 11, pts: 24 },
			102,
		);
		const away = squad(
			{ tid: 2, name: "Bulls" },
			{ name: "Bo Bull", pid: 21, pts: 22 },
			95,
		);
		const g = game([home, away]);
		for (const sentence of [
			"The Hawks host the Heat tomorrow.",
			"The Bulls were on the second night of a back-to-back.",
			"The Hawks had come in off four days' rest.",
			"The Bulls get the Nets in three days.",
		]) {
			const kinds = verifyRecap(
				`**x**\n\nThe Hawks beat the Bulls 102-95. ${sentence}`,
				g,
			).map((v) => v.kind);
			assert.include(kinds, "calendar", sentence);
		}
		// Seasons are not days: a title repeat is still a back-to-back.
		assert.notInclude(
			verifyRecap(
				"**x**\n\nThe Hawks beat the Bulls 102-95. They won back-to-back championships once.",
				g,
			).map((v) => v.kind),
			"calendar",
		);
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

	// "Walker Ellis walks off the Pacers 108-106" of a free throw with 12.1
	// seconds left, and the Pacers still to shoot.
	test("a day wrap walks off only on the game's last shot", () => {
		const slate = (day: number, play: string) => {
			const home = squad(
				{ tid: 1, name: "Hawks" },
				{ name: "Ace Hawk", pid: 11, pts: 29, fg: 11, fga: 21 },
				101,
			);
			const away = squad(
				{ tid: 2, name: "Bulls" },
				{ name: "Bo Bull", pid: 21, pts: 24, fg: 9, fga: 20 },
				100,
			);
			const other = [
				squad({ tid: 3, name: "Kings" }, { name: "Cy King", pid: 31 }, 112),
				squad({ tid: 4, name: "Suns" }, { name: "Di Sun", pid: 41 }, 96),
			] as [RecapTeam, RecapTeam];
			return getAutoDayRecap({
				season: 2025,
				day,
				playoffs: false,
				games: [
					game([home, away], {
						day,
						clutchPlays: [`<a href="#">Ace Hawk</a> made a ${play}.`],
					}),
					game(other, { gid: 78, day }),
				],
			});
		};
		let walked = 0;
		for (let day = 1; day <= 40; day++) {
			const early = slate(
				day,
				"game-winning free throw with 12.1 seconds remaining",
			);
			clean(early);
			assert.notMatch(early, /walks off/, early);
			if (
				/walks off/.test(slate(day, "game-winning three-pointer at the buzzer"))
			) {
				walked += 1;
			}
		}
		assert.isAbove(walked, 0);
	});

	// "It took an extra period, but the Suns pulled away... The Suns won the
	// second half 51-49, and it went to overtime. Regulation settled
	// nothing, and it took an extra period." Overtime, three times over.
	test("overtime is said once, and the second half ends on the score", () => {
		let told = 0;
		for (let gid = 1; gid <= 20; gid++) {
			const home = squad(
				{ tid: 1, name: "Hawks", ptsQtrs: [22, 24, 28, 26, 12] },
				{ name: "Ace Hawk", pid: 11, pts: 30, fg: 12, fga: 22 },
				112,
			);
			const away = squad(
				{ tid: 2, name: "Bulls", ptsQtrs: [28, 26, 24, 22, 10] },
				{ name: "Bo Bull", pid: 21, pts: 28, fg: 11, fga: 20 },
				110,
			);
			const g = game([home, away], { gid, overtimes: 1, winnerTid: 1 });
			const recap = getAutoRecap(g);
			clean(recap);
			const body = recap.split("\n\n").slice(1).join(" ");
			assert.isAtMost(
				(body.match(/overtime|extra period|Regulation settled/g) ?? []).length,
				2,
				recap,
			);
			if (/regulation ended tied at 100\b/.test(body)) {
				told += 1;
			}
			assert.deepEqual(verifyRecap(recap, g), []);
		}
		assert.isAbove(told, 0);
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
				/sixth 30-point game|six 30-point games|six games of 30 or more|30 or more six times/.test(
					recap,
				)
			) {
				counted += 1;
			}
			assert.deepEqual(verifyRecap(recap, game([home, away], { gid })), []);
		}
		assert.ok(counted >= 5, `${counted}`);
	});
	// "Walker Green racked up 32 points..." and a paragraph on "Walker
	// Green's previous season high was 31.": the wire writes "a season-high
	// 32 points".
	test("a season high is in his line, not a footnote", () => {
		const entering = (high: { pts: number; reb: number }) => ({
			gp: 20,
			high: { ...high, ast: 8, tp: 6, stl: 3, blk: 2 },
			totals: { pts: 500, reb: 120, ast: 80, tp: 40, stl: 20, blk: 10 },
			streaks: { twenty: 0, thirty: 0, doubleDouble: 0 },
			counts: { twenty: 12, thirty: 0, forty: 0 },
		});
		for (let gid = 1; gid <= 12; gid++) {
			const home = squad(
				{ tid: 1, name: "Hawks" },
				{
					name: "Ace Hawk",
					pid: 11,
					pts: 33,
					fg: 12,
					fga: 22,
					entering: entering({ pts: 27, reb: 12 }),
				},
				110,
			);
			const away = squad(
				{ tid: 2, name: "Bulls" },
				{
					name: "Bo Bull",
					pid: 21,
					pts: 24,
					reb: 15,
					entering: entering({ pts: 30, reb: 13 }),
				},
				98,
			);
			const g = game([home, away], { gid });
			const recap = getAutoRecap(g);
			clean(recap);
			assert.match(
				recap,
				/(?:\ba|his|Ace Hawk's) season-high 33 points\b/,
				recap,
			);
			assert.match(recap, /season-high 15 rebounds/, recap);
			assert.notMatch(
				recap,
				/previous season high|season high of|a season high for|best night on the glass/,
				recap,
			);
			assert.deepEqual(verifyRecap(recap, g), [], recap);
		}
		// The reader holds the figure to his night and his old best.
		const home = squad(
			{ tid: 1, name: "Hawks" },
			{
				name: "Ace Hawk",
				pid: 11,
				pts: 33,
				entering: entering({ pts: 35, reb: 12 }),
			},
			110,
		);
		const away = squad(
			{ tid: 2, name: "Bulls" },
			{ name: "Bo Bull", pid: 21 },
			98,
		);
		const g = game([home, away]);
		for (const said of [
			"Ace Hawk had a season-high 33 points.",
			"Ace Hawk had a season-high 34 points.",
		]) {
			assert.include(
				verifyRecap(`**x**\n\n${said}`, g).map((v) => v.kind),
				"season high",
				said,
			);
		}
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
		assert.deepEqual(
			verifyRecap(
				"**x**\n\nAce Hawk has scored 30 or more five times this season.",
				g,
			).map((v) => v.kind),
			["count of 30"],
		);
		// His best against tonight's opponent, however it is put.
		home.players[0]!.vsOpponent = { games: 3, bestPts: 15, avgPts: 12 };
		for (const said of [
			"Ace Hawk had not managed more than 17 points against the Bulls this season.",
			"The Bulls had held Ace Hawk to 12 or fewer in every earlier meeting.",
		]) {
			assert.deepEqual(
				verifyRecap(`**x**\n\n${said}`, g).map((v) => v.kind),
				["against this opponent"],
				said,
			);
		}
		assert.deepEqual(
			verifyRecap(
				"**x**\n\nAce Hawk had not managed more than 15 points against the Bulls this season.",
				g,
			),
			[],
		);
		const right = [
			"Ace Hawk has not been held under 20 in five games.",
			"It was his sixth 30-point game of the season.",
			"He had not scored more than 31 in a game this season.",
		].join(" ");
		assert.deepEqual(verifyRecap(`**x**\n\n${right}`, g), []);
		// A previous high, of whatever column it names.
		const kinds = (s: string) =>
			verifyRecap(`**x**\n\n${s}`, g).map((v) => v.kind);
		assert.deepEqual(kinds("Ace Hawk's previous season high was 31."), []);
		assert.deepEqual(kinds("Ace Hawk's previous season high was 29."), [
			"season high",
		]);
		assert.deepEqual(
			kinds("Ace Hawk's previous season high was 12 rebounds."),
			[],
		);
		assert.deepEqual(kinds("Ace Hawk's previous season high was 9 assists."), [
			"season high",
		]);
	});

	// "They were 12 up going into the fourth" was never held to the
	// quarters; only "it was 80-68 going into the fourth" was.
	test("the reader holds a margin going into a quarter, and a lead stretched", () => {
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
		const g = game([home, away], {
			flow: { leadChanges: 2, ties: 1, maxLead: [14, 3], maxLeadAt: [4, 1] },
		});
		// 81-75 after three: a 6-point game.
		for (const wrong of [
			"The Hawks were 9 up going into the fourth.",
			"It was a 4-point game entering the fourth.",
			"Going into the fourth, the Hawks were up 8.",
			"The Hawks took a 7-point lead into the fourth and stretched it to 14.",
		]) {
			assert.deepEqual(
				verifyRecap(`**x**\n\n${wrong}`, g).map((v) => v.kind),
				["margin entering a quarter"],
				wrong,
			);
		}
		assert.deepEqual(
			verifyRecap(
				"**x**\n\nThe Hawks took a 6-point lead into the fourth and stretched it to 15.",
				g,
			).map((v) => v.kind),
			["biggest lead"],
		);
		assert.deepEqual(
			verifyRecap(
				"**x**\n\nIt was a 6-point game entering the fourth. The Hawks took a 6-point lead into the fourth and stretched it to 14.",
				g,
			),
			[],
		);
	});

	test("the reader holds a shooting percentage to a shooting line", () => {
		const home = squad(
			{ tid: 1, name: "Hawks", ptsQtrs: [28, 26, 27, 29] },
			{
				name: "Ace Hawk",
				pid: 11,
				pts: 30,
				fg: 12,
				fga: 20,
				seasonAvg: {
					gp: 20,
					min: 34,
					pts: 22,
					reb: 5,
					ast: 4,
					stl: 1,
					blk: 0,
					tov: 2,
					fgp: 44.8,
					tpp: 35,
					ftp: 80,
				},
			},
			110,
		);
		const away = squad(
			{ tid: 2, name: "Kings", ptsQtrs: [24, 25, 26, 27] },
			{ name: "Rex King", pid: 21, pts: 28, fg: 11, fga: 21 },
			102,
		);
		const g = game([home, away]);
		const fg = (t: RecapTeam) =>
			(100 * t.players.reduce((a, p) => a + p.fg, 0)) /
			t.players.reduce((a, p) => a + p.fga, 0);
		const right = [
			`The Hawks shot ${fg(home).toFixed(1)}% from the floor.`,
			"Ace Hawk was 12-of-20 from the floor, far better than the 44.8% he had managed on the season.",
			"He shot 60% on the night.",
		].join(" ");
		assert.deepEqual(verifyRecap(`**x**\n\n${right}`, g), []);
		assert.deepEqual(
			verifyRecap(
				`**x**\n\nThe Kings shot just ${(fg(away) - 3).toFixed(1)}% as a team.`,
				g,
			).map((v) => v.kind),
			["percentage"],
		);
	});

	// "hitting 4 of 7 threes" is a split like "4-of-7", and held to one.
	test("the reader holds a written-out split to a shooting line", () => {
		const home = squad(
			{ tid: 1, name: "Hawks", ptsQtrs: [28, 26, 27, 29] },
			{ name: "Ace Hawk", pid: 11, pts: 29, fg: 10, fga: 20, tp: 4, tpa: 7 },
			110,
		);
		const away = squad(
			{ tid: 2, name: "Kings", ptsQtrs: [24, 25, 26, 27] },
			{ name: "Rex King", pid: 21, pts: 28, fg: 11, fga: 21 },
			102,
		);
		const g = game([home, away]);
		assert.deepEqual(
			verifyRecap(
				"**x**\n\nAce Hawk scored 29 points, hitting 4 of 7 threes.",
				g,
			),
			[],
		);
		assert.deepEqual(
			verifyRecap(
				"**x**\n\nAce Hawk scored 29 points, hitting 5 of 7 threes.",
				g,
			).map((v) => v.kind),
			["shooting split"],
		);
		assert.deepEqual(
			verifyRecap(
				"**x**\n\nAce Hawk scored 29 points, hitting all 7 of his threes.",
				g,
			).map((v) => v.kind),
			["shooting split"],
		);
	});

	test("the reader holds a team high to the box score", () => {
		const home = squad(
			{ tid: 1, name: "Hawks", ptsQtrs: [28, 26, 27, 29] },
			{ name: "Ace Hawk", pid: 11, pts: 29, fg: 10, fga: 20 },
			110,
		);
		const away = squad(
			{ tid: 2, name: "Kings", ptsQtrs: [24, 25, 26, 27] },
			{ name: "Rex King", pid: 21, pts: 28, fg: 11, fga: 21 },
			102,
		);
		const g = game([home, away]);
		const kinds = (s: string) =>
			verifyRecap(`**x**\n\n${s}`, g).map((v) => v.kind);
		assert.deepEqual(kinds("Ace Hawk had a team-high 29 points."), []);
		assert.deepEqual(kinds("Rex King had a team-high 28 points."), []);
		assert.include(kinds("Ace Hawk had a team-high 27 points."), "team high");
		// The second-best man on the side has no team high.
		home.players.push({
			...home.players[0]!,
			name: "Bo Hawk",
			pid: 12,
			pts: 20,
		});
		assert.deepEqual(kinds("Bo Hawk had a team-high 20 points."), [
			"team high",
		]);
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
			"Game 5 is in Atlanta.",
		].join(" ");
		assert.deepEqual(verifyRecap(`**x**\n\n${right}`, g), []);

		const wrong = [
			"The Hawks beat the Kings 110-102 in Game 5 of the First Round.",
			"They take a 3-2 series lead.",
			"The #3 seed is two wins from putting out the #7 seed.",
			"Game 6 is in Atlanta.",
		].join(" ");
		const kinds = verifyRecap(`**x**\n\n${wrong}`, g).map((v) => v.kind);
		assert.includeMembers(kinds, [
			"series game number",
			"series score",
			"series seed",
			"series wins remaining",
			"series next game",
		]);

		// The next-game check is on the NUMBER, not the phrasing: a new way of
		// pointing at the next game cannot smuggle a wrong one past the desk.
		for (const phrasing of [
			"The series resumes in Atlanta with Game 7.",
			"They meet again in Atlanta for Game 7.",
			"Back at it in Atlanta for Game 7.",
			"Game 7 is in Atlanta.",
		]) {
			const kinds2 = verifyRecap(
				`**x**\n\nThe Hawks beat the Kings 110-102. ${phrasing}`,
				g,
			).map((v) => v.kind);
			assert.include(kinds2, "series next game", phrasing);
		}
		// ...and the right number passes in every one of those shapes.
		for (const phrasing of [
			"Game 5 is in Atlanta.",
			"The series resumes in Atlanta with Game 5.",
			"They meet again in Atlanta for Game 5.",
			"Next is Game 5, in Atlanta.",
			"Game 5 is in Atlanta.",
			"Back at it in Atlanta for Game 5.",
		]) {
			const kinds3 = verifyRecap(
				`**x**\n\nThe Hawks beat the Kings 110-102 in Game 4 of the First Round. ${phrasing}`,
				g,
			).map((v) => v.kind);
			assert.notInclude(kinds3, "series next game", phrasing);
		}
	});

	test("the title clinch knows the franchise's history", () => {
		const mk = (titleHistory?: RecapTeam["titleHistory"]) => {
			const home = squad(
				{ tid: 1, name: "Hawks", titleHistory },
				{ name: "Ace Hawk", pid: 11, pts: 32, fg: 12, fga: 22 },
				112,
			);
			const away = squad(
				{ tid: 2, name: "Kings" },
				{ name: "Rex King", pid: 21, pts: 28, fg: 11, fga: 21 },
				101,
			);
			return game([home, away], {
				playoffs: true,
				series: {
					round: 4,
					numRounds: 4,
					bestOf: 7,
					homeAbbrev: home.abbrev,
					awayAbbrev: away.abbrev,
					homeWon: 3,
					awayWon: 1,
				},
			});
		};

		// A franchise with a past: third title, first since 2015.
		const g1 = mk({ titles: [2008, 2015], seasons: 30, season: 2025 });
		const r1 = getAutoRecap(g1);
		clean(r1);
		assert.match(
			r1,
			/third championship in franchise history, and the first since 2015/,
		);
		assert.deepEqual(verifyRecap(r1, g1), []);

		// Back-to-back.
		const g2 = mk({ titles: [2024], seasons: 10, season: 2025 });
		const r2 = getAutoRecap(g2);
		assert.match(r2, /back-to-back championships/);
		assert.deepEqual(verifyRecap(r2, g2), []);

		// A first for an established franchise.
		const g3 = mk({ titles: [], seasons: 30, season: 2025 });
		const r3 = getAutoRecap(g3);
		assert.match(r3, /first championship in franchise history/);
		assert.deepEqual(verifyRecap(r3, g3), []);

		// A brand-new expansion team keeps quiet about "franchise history".
		const g4 = mk({ titles: [], seasons: 1, season: 2025 });
		assert.notMatch(getAutoRecap(g4), /franchise history/);

		// And the reader catches lies about the record.
		const lies = [
			"The Hawks beat the Kings 112-101 to win the title.",
			"It is the first championship in franchise history.",
			"That makes it back-to-back championships.",
			"It is the fifth championship in franchise history, and the first since 2008.",
		].join(" ");
		const kinds = verifyRecap(`**x**\n\n${lies}`, g1).map((v) => v.kind);
		assert.includeMembers(kinds, [
			"franchise first title",
			"back-to-back claim",
			"franchise title count",
			"franchise last title year",
		]);
	});

	// "Heat put the Nuggets out in five" over "Evan Hayes went for 23 points
	// as the Heat came back to top the Nuggets 124-111": the wrap's lead
	// left the series to its closing paragraph.
	test("a wrap's lead tells the series its marquee game ended", () => {
		const series = (home: RecapTeam, away: RecapTeam, won: number) => ({
			round: 2,
			numRounds: 4,
			bestOf: 7,
			homeAbbrev: home.abbrev,
			awayAbbrev: away.abbrev,
			homeWon: won,
			awayWon: 2,
		});
		for (let day = 1; day <= 12; day++) {
			const home = squad(
				{ tid: 1, name: "Hawks" },
				{ name: "Ace Hawk", pid: 11, pts: 33, fg: 12, fga: 22 },
				118,
			);
			const away = squad(
				{ tid: 2, name: "Kings" },
				{ name: "Rex King", pid: 21, pts: 24, fg: 9, fga: 20 },
				104,
			);
			const h2 = squad(
				{ tid: 3, name: "Suns" },
				{ name: "Cy Sun", pid: 31 },
				99,
			);
			const a2 = squad(
				{ tid: 4, name: "Jazz" },
				{ name: "Di Jazz", pid: 41 },
				95,
			);
			const wrap = getAutoDayRecap({
				season: 2025,
				day,
				playoffs: true,
				games: [
					game([home, away], {
						day,
						playoffs: true,
						series: series(home, away, 3),
					}),
					game([h2, a2], {
						gid: 78,
						day,
						playoffs: true,
						series: series(h2, a2, 1),
					}),
				],
			});
			clean(wrap);
			const lead = wrap.split("\n\n").find((p) => !p.startsWith("*"))!;
			const first = lead.split(/(?<=\.) /)[0]!;
			assert.match(first, /Hawks/, wrap);
			assert.match(
				first,
				/close out the series in six|win the series in six|in Game 6/,
				wrap,
			);
		}
	});

	// "Grant Green put up 29 points as the Lakers took down the Pistons
	// 124-114" was the whole lede of the night that won them the title; not
	// one of 45 series-ending ledes said the series was over.
	test("a series-ending game says so in its lede, and once", () => {
		const cases: [number, number, number, RegExp][] = [
			// round, winner's wins before, loser's wins before, lede
			[1, 3, 1, /to close out the series 4-1|to win the series in five games/],
			[1, 3, 0, /to finish a four-game sweep/],
			[2, 3, 3, /in Game 7 to reach the Conference Finals/],
			[4, 3, 2, /to win the Finals 4-2/],
			[4, 3, 3, /in Game 7 to win the title/],
		];
		for (const [round, won, lost, lede] of cases) {
			for (let gid = 1; gid <= 12; gid++) {
				const home = squad(
					{ tid: 1, name: "Hawks" },
					{ name: "Ace Hawk", pid: 11, pts: 30, fg: 11, fga: 21 },
					112,
				);
				const away = squad(
					{ tid: 2, name: "Kings" },
					{ name: "Rex King", pid: 21, pts: 24, fg: 9, fga: 20 },
					101,
				);
				const g = game([home, away], {
					gid,
					playoffs: true,
					series: {
						round,
						numRounds: 4,
						bestOf: 7,
						homeAbbrev: home.abbrev,
						awayAbbrev: away.abbrev,
						homeWon: won,
						awayWon: lost,
					},
				});
				const recap = getAutoRecap(g);
				clean(recap);
				const first = recap.split("\n\n")[1]!.split(/(?<=\.) /)[0]!;
				assert.match(first, lede, recap);
				assert.notMatch(
					recap.split("\n\n").slice(1).join(" ").replace(first, ""),
					/took the series|closed out the series|won the series|series goes to|are champions|finished a \d-0 sweep/,
					recap,
				);
				assert.deepEqual(verifyRecap(recap, g), [], recap);
			}
		}
	});
});

describe("pickByDay", () => {
	test("consecutive days rotate instead of gambling", () => {
		const pool = ["A", "B", "C"];
		const rng = () => 0.5;
		const byDay: string[] = [];
		for (let day = 1; day <= 6; day++) {
			beginRecapBatch();
			byDay.push(pickByDay(day, rng, pool, "t"));
			endRecapBatch();
		}
		// Never the same variant two days running, and the whole pool gets used.
		for (let i = 1; i < byDay.length; i++) {
			assert.notEqual(byDay[i], byDay[i - 1]);
		}
		assert.deepEqual([...new Set(byDay)].sort(), pool);
	});

	test("two uses in one night differ, and the same day reproduces itself", () => {
		const pool = ["east", "west"];
		const rng = () => 0.5;
		beginRecapBatch();
		const first = pickByDay(9, rng, pool, "conf");
		const second = pickByDay(9, rng, pool, "conf");
		endRecapBatch();
		assert.notEqual(first, second);
		beginRecapBatch();
		assert.equal(pickByDay(9, rng, pool, "conf"), first);
		endRecapBatch();
	});
});

describe("facts told once, in English", () => {
	const l10 = (results: boolean[]) =>
		results.map((won, i) => ({
			opp: "BUL",
			home: i % 2 === 0,
			won,
			pts: won ? 105 : 95,
			oppPts: won ? 95 : 105,
		}));

	test("a skid is not restated as recent form", () => {
		// "It was the Suns' eighth straight loss." followed by "The Suns had
		// lost every one of their last seven coming in." is one fact twice,
		// with two numbers that look like they disagree.
		const home = squad(
			{ tid: 1, name: "Hawks" },
			{ name: "Ace Hawk", pid: 11, pts: 27, fg: 10, fga: 18 },
			105,
		);
		const away = squad(
			{
				tid: 2,
				name: "Suns",
				streak: { won: false, count: 8 },
				streakBefore: { won: false, count: 7 },
				last10: l10([false, false, false, false, false, false, false, false]),
			},
			{ name: "Bo Sun", pid: 21, pts: 22, fg: 8, fga: 20 },
			98,
		);
		const g = game([home, away]);
		const recap = getAutoRecap(g);
		clean(recap);
		// The streak is worth saying once; the form note must then stay quiet.
		const streakSaid =
			/straight loss|lost eight in a row|eighth in a row|straight losses/.test(
				recap,
			);
		const formSaid =
			/had lost (?:every one of|\w+ of) their last|arrived having dropped|rough stretch for/.test(
				recap,
			);
		assert.ok(streakSaid, recap);
		assert.ok(!formSaid, `skid restated as form:\n${recap}`);
		assert.deepEqual(verifyRecap(recap, g), []);
	});

	// "...ending the Knicks' 6-game winning streak" in the lede, then "It
	// snapped the Knicks' six-game winning streak" in the notes: one fact
	// twice, in two styles.
	test("a streak the winner ended is told once, spelled out", () => {
		for (let gid = 1; gid < 30; gid++) {
			const home = squad(
				{ tid: 1, name: "Kings" },
				{ name: "Ace King", pid: 11, pts: 27, fg: 10, fga: 18 },
				113,
			);
			const away = squad(
				{
					tid: 2,
					name: "Knicks",
					streak: { won: false, count: 1 },
					streakBefore: { won: true, count: 6 },
					last10: l10([false, true, true, true, true, true, true]),
				},
				{ name: "Bo Knick", pid: 21, pts: 22, fg: 8, fga: 20 },
				108,
			);
			const recap = getAutoRecap(game([home, away], { gid }));
			const told = recap.match(
				/six-game winning streak|won six in a row until|six-game run/g,
			);
			assert.strictEqual(told?.length, 1, recap);
			assert.notMatch(recap, /6-game winning streak[^*]*$/, recap);
		}
	});

	// "Isaiah Carter swats 5 as the Hornets slip past the Timberwolves" sat
	// over the night an eleven-game winning streak ended, and "Hawks end the
	// Kings' 9-game winning streak" over a body that never said so.
	test("a long streak ended is the headline, and the body tells it too", () => {
		const make = (gid: number, lost: Partial<RecapTeam>, won = {}) => {
			const home = squad(
				{ tid: 1, name: "Hornets", ...won },
				{ name: "Ace Hornet", pid: 11, pts: 21, fg: 8, fga: 17 },
				110,
			);
			const away = squad(
				{ tid: 2, name: "Wolves", ...lost },
				{ name: "Bo Wolf", pid: 21, pts: 20, fg: 8, fga: 19 },
				104,
			);
			return getAutoRecap(game([home, away], { gid }));
		};
		for (let gid = 1; gid < 30; gid++) {
			const run = make(gid, {
				streak: { won: false, count: 1 },
				streakBefore: { won: true, count: 11 },
				last10: l10([false, true, true, true, true, true, true, true]),
			});
			const [head, ...body] = run.split("\n\n");
			assert.match(head!, /11-game winning streak/, run);
			assert.match(body[0]!, /eleven-game winning streak/, run);
			clean(run);

			const skid = make(
				gid,
				{},
				{
					streak: { won: true, count: 1 },
					streakBefore: { won: false, count: 8 },
					last10: l10([true, false, false, false, false, false, false]),
				},
			);
			const [skidHead, ...skidBody] = skid.split("\n\n");
			assert.match(skidHead!, /8-game (?:skid|losing streak)/, skid);
			assert.match(skidBody[0]!, /eight-game losing streak/, skid);
			clean(skid);
		}
	});

	test("a skid the headline names is in the body", () => {
		let named = 0;
		for (let gid = 1; gid < 60; gid++) {
			const home = squad(
				{ tid: 1, name: "Nets" },
				{ name: "Ace Net", pid: 11, pts: 18, fg: 7, fga: 15 },
				99,
			);
			const away = squad(
				{
					tid: 2,
					name: "Nuggets",
					streak: { won: false, count: 7 },
					streakBefore: { won: false, count: 6 },
					last10: l10([false, false, false, false, false, false, false]),
				},
				{ name: "Bo Nugget", pid: 21, pts: 17, fg: 7, fga: 18 },
				92,
			);
			// Underdogs, so the upset headlines can name the skid.
			const recap = getAutoRecap(
				game([home, away], { gid, spread: { favTid: 2, points: 7 } }),
			);
			const [head, ...body] = recap.split("\n\n");
			if (/straight|in a row|losing streak/.test(head!)) {
				named += 1;
				assert.match(
					body.join(" "),
					/Nuggets[^.]*(?:straight|in a row|losing streak)|(?:straight|in a row|losing streak)[^.]*Nuggets/,
					recap,
				);
			}
		}
		assert.isAbove(named, 3);
	});

	test("two meetings are 'both', never 'all two'", () => {
		const home = squad(
			{
				tid: 1,
				name: "Hawks",
				seasonSeries: { won: 1, lost: 0, left: 0 },
			},
			{ name: "Ace Hawk", pid: 11, pts: 27, fg: 10, fga: 18 },
			110,
		);
		const away = squad(
			{ tid: 2, name: "Bulls", seasonSeries: { won: 0, lost: 1, left: 0 } },
			{ name: "Bo Bull", pid: 21, pts: 24, fg: 9, fga: 19 },
			101,
		);
		// Every phrasing in the pool, across many seeds.
		for (let gid = 1; gid < 40; gid++) {
			const recap = getAutoRecap(game([home, away], { gid }));
			assert.notMatch(recap, /all two\b/, recap);
		}
	});
});

// "Magic knock off the Knicks 118-113" of a game the 7-point underdogs
// trailed by 20 in the first quarter (10 at the break), and "Down 13" of a
// game trailed by 17: the comeback is the score log's biggest deficit, and
// at 15 or more it outranks the upset.
describe("the deficit a comeback came from", () => {
	const make = (gid: number) => {
		const magic = squad(
			{ tid: 1, name: "Magic", ptsQtrs: [20, 27, 28, 43] },
			{ name: "Obi Jackson", pid: 11, pts: 26, fg: 8, fga: 12 },
			118,
		);
		const knicks = squad(
			{ tid: 2, name: "Knicks", ptsQtrs: [35, 22, 24, 32] },
			{ name: "Walker Ingram", pid: 21, pts: 27, ast: 14, fg: 10, fga: 22 },
			113,
		);
		return game([magic, knicks], {
			gid,
			spread: { favTid: 2, points: 7 },
			flow: {
				leadChanges: 4,
				ties: 3,
				maxLead: [6, 20],
				maxLeadAt: [4, 1],
				lastLead: { side: 0, period: 4, clock: 67, pts: [111, 108] },
			},
		});
	};

	test("is the biggest the log saw, and leads over the upset", () => {
		for (let gid = 900; gid < 915; gid++) {
			const g = make(gid);
			const recap = getAutoRecap(g);
			const top = recap.split("\n\n").slice(0, 2).join(" ");
			assert.match(
				top,
				/20-point|from 20 down|Down 20|rall|come back|came back|erase/,
				recap,
			);
			assert.notMatch(recap, /\b10-point deficit|from 10 down|Down 10,/, recap);
			assert.deepStrictEqual(verifyRecap(recap, g), [], recap);
		}
	});

	test("the accuracy reader holds a deficit to the log", () => {
		const g = make(900);
		assert.deepStrictEqual(
			verifyRecap(
				"**x**\n\nThe Magic erased a 25-point deficit to beat the Knicks 118-113.",
				g,
			).map((v) => v.kind),
			["deficit"],
		);
	});

	// 47-57 at the half. "Tied at N at the break" is a halftime score, not the
	// game's last tie.
	test("the accuracy reader holds a halftime score to the quarters", () => {
		const g = make(900);
		const kinds = (s: string) =>
			verifyRecap(`**x**\n\n${s}`, g).map((v) => v.kind);
		assert.deepStrictEqual(kinds("The Magic trailed 47-57 at halftime."), []);
		assert.deepStrictEqual(kinds("It was 52-52 at halftime."), [
			"halftime score",
		]);
		assert.deepStrictEqual(kinds("The teams were tied at 47 at the break."), [
			"halftime score",
		]);
	});
});

// Up 13 going into the fourth, won by 3: "It was a 13-point game entering
// the fourth" went straight to a three that cut it to one, and the quarter
// that made it close was never told - in all 16 such games of a corpus.
describe("the lead that nearly went", () => {
	test("the losers' last quarter is told, with an article that fits", () => {
		let told = 0;
		for (let gid = 300; gid < 330; gid++) {
			const lakers = squad(
				{ tid: 1, name: "Lakers", ptsQtrs: [30, 28, 30, 19] },
				{ name: "Grant Green", pid: 11, pts: 22, fg: 8, fga: 17 },
				107,
			);
			const bulls = squad(
				{ tid: 2, name: "Bulls", ptsQtrs: [20, 22, 33, 29] },
				{ name: "Xavier Carter", pid: 21, pts: 24, fg: 9, fga: 20 },
				104,
			);
			const g = game([lakers, bulls], {
				gid,
				flow: {
					leadChanges: 2,
					ties: 1,
					maxLead: [21, 2],
					maxLeadAt: [3, 1],
					lastLead: { side: 0, period: 1, clock: 400, pts: [8, 6] },
				},
			});
			const recap = getAutoRecap(g);
			clean(recap);
			if (/going into the fourth|entering the fourth/.test(recap)) {
				assert.match(recap, /fourth quarter 29-19/, recap);
				told += 1;
			}
			assert.notMatch(
				recap,
				/\ba (?:8|11|18)-|\ba (?:eight|eleven|eighteen)-/,
				recap,
			);
			assert.deepStrictEqual(verifyRecap(recap, g), [], recap);
		}
		assert.isAbove(told, 0);
	});
});

// "It was a 22th straight game out for Amen Green", and "send the Pistons to
// a eighth straight defeat".
describe("ordinals", () => {
	test("past the words, the right suffix", () => {
		assert.strictEqual(ordinal(15), "fifteenth");
		assert.strictEqual(ordinal(16), "16th");
		assert.strictEqual(ordinal(21), "21st");
		assert.strictEqual(ordinal(22), "22nd");
		assert.strictEqual(ordinal(23), "23rd");
		assert.strictEqual(ordinal(111), "111th");
		assert.strictEqual(ordinal(112), "112th");
	});

	test("a number takes the article it sounds like", () => {
		assert.strictEqual(aNum(8), "an 8");
		assert.strictEqual(aNum(81), "an 81");
		assert.strictEqual(aNum(11), "an 11");
		assert.strictEqual(aNum(18), "an 18");
		assert.strictEqual(aNum(115), "a 115");
		assert.strictEqual(aNum(28), "a 28");
	});

	test("a figure, and a finished text, take the article they sound like", () => {
		assert.strictEqual(aFigure("85-80"), "an 85-80");
		assert.strictEqual(aFigure("115-93"), "a 115-93");
		assert.strictEqual(aFigure("11-9"), "an 11-9");
		assert.strictEqual(
			fixArticles(
				"A 18-2 run gave them a 81-point edge in a 115-93 win and a 11-of-15 night.",
			),
			"An 18-2 run gave them an 81-point edge in a 115-93 win and an 11-of-15 night.",
		);
	});

	test("and the article that sounds right", () => {
		assert.strictEqual(aOrdinal(8), "an eighth");
		assert.strictEqual(aOrdinal(11), "an eleventh");
		assert.strictEqual(aOrdinal(18), "an 18th");
		assert.strictEqual(aOrdinal(22), "a 22nd");
		assert.strictEqual(aOrdinal(3), "a third");
	});
});

// "Sore achilles kept Anthony Lowry out": a singular tendon ending in s was
// read as a plural, and a man's name lost its capital.
// "The Heat assisted on 31 of their 41 baskets": one team's assists and
// field goals, exactly.
describe("assisted baskets", () => {
	test("are held to one team's box score", () => {
		const a = squad(
			{ tid: 1, name: "Magic", ptsQtrs: [25, 25, 25, 25] },
			{ name: "Obi Jackson", pid: 11, pts: 26, ast: 9, fg: 10, fga: 18 },
			100,
		);
		const b = squad(
			{ tid: 2, name: "Knicks", ptsQtrs: [24, 24, 24, 24] },
			{ name: "Walker Ingram", pid: 21, pts: 27, ast: 4, fg: 11, fga: 22 },
			96,
		);
		const g = game([a, b], { gid: 990 });
		const ast = a.players.reduce((s, p) => s + p.ast, 0);
		const fg = a.players.reduce((s, p) => s + p.fg, 0);
		const kinds = (s: string) =>
			verifyRecap(`**x**\n\n${s}`, g).map((v) => v.kind);
		assert.deepStrictEqual(
			kinds(`The Magic assisted on ${ast} of their ${fg} baskets.`),
			[],
		);
		assert.deepStrictEqual(
			kinds(`The Magic assisted on ${ast + 1} of their ${fg} baskets.`),
			["assists"],
		);
	});
});

describe("injury names in prose", () => {
	test("singular names ending in s keep their article", () => {
		assert.strictEqual(injuryPhrase("Sore Achilles"), "a sore Achilles");
		assert.strictEqual(
			injuryPhrase("Bruised Quadriceps"),
			"a bruised quadriceps",
		);
		assert.strictEqual(
			injuryPhrase("Achilles Tendinitis"),
			"Achilles tendinitis",
		);
		assert.strictEqual(injuryPhrase("Back Spasms"), "back spasms");
		assert.strictEqual(injuryPhrase("Torn ACL"), "a torn ACL");
	});
});

// The reader never counted threes: "They knocked down 18 threes" went
// unchecked against a box score that has every make and attempt.
describe("the accuracy reader counts threes", () => {
	const home = squad(
		{ tid: 1, name: "Hawks" },
		{ name: "Ace Hawk", pid: 11, pts: 27, tp: 5, tpa: 9 },
		110,
	);
	const away = squad(
		{ tid: 2, name: "Bulls" },
		{ name: "Bo Bull", pid: 21, pts: 24, tp: 2, tpa: 6 },
		101,
	);
	const g = game([home, away]);
	// Hawks 5-of-21 (the star 5-of-9, six others 0-of-2), Bulls 2-of-18.
	const kinds = (text: string) =>
		verifyRecap(`**x**\n\n${text}`, g).map((v) => v.kind);

	test("a team's makes, a man's makes and the gap pass", () => {
		assert.deepEqual(kinds("The Hawks knocked down 5 threes."), []);
		assert.deepEqual(kinds("The Hawks made 3 more threes than the Bulls."), []);
		assert.deepEqual(kinds("The Bulls missed 16 of their 18 threes."), []);
	});

	test("a count the box score does not have is caught", () => {
		assert.deepEqual(kinds("The Hawks knocked down 17 threes."), ["threes"]);
		assert.deepEqual(kinds("The Bulls missed 15 of their 18 threes."), [
			"threes",
		]);
	});
});
