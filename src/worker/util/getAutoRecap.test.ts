import assert from "node:assert/strict";
import { describe, test } from "vitest";
import { getAutoRecap } from "./getAutoRecap.ts";
import type {
	RecapGame,
	RecapPlayer,
	RecapTeam,
} from "./getDayGamesForRecap.ts";

// A box-score line with only the fields a test cares about; the rest default to
// zero so fixtures stay short.
const player = (p: Partial<RecapPlayer> & { name: string }): RecapPlayer => ({
	pid: 0,
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
	...p,
});

const team = (t: Partial<RecapTeam> & { name: string }): RecapTeam => ({
	tid: 0,
	region: "",
	name: t.name,
	abbrev: t.abbrev ?? "???",
	pts: 0,
	players: [],
	...t,
});

const game = (
	g: Partial<RecapGame> & { teams: [RecapTeam, RecapTeam] },
): RecapGame => ({
	gid: 1,
	day: 1,
	overtimes: 0,
	winnerTid: g.teams[0].tid,
	playoffs: false,
	clutchPlays: [],
	...g,
});

describe("getAutoRecap", () => {
	test("game-winner headlines the shot and names the shooter", () => {
		const kings = team({
			tid: 5,
			region: "Sacramento",
			name: "Kings",
			abbrev: "SAC",
			pts: 112,
			ptsQtrs: [28, 24, 30, 30],
			players: [
				player({ name: "Peja Stojakovic", pts: 31, reb: 6, ast: 4, tp: 5 }),
				player({ name: "Chris Webber", pts: 22, reb: 11, ast: 7 }),
			],
		});
		const spurs = team({
			tid: 9,
			region: "San Antonio",
			name: "Spurs",
			abbrev: "SAS",
			pts: 109,
			ptsQtrs: [30, 25, 27, 27],
			players: [player({ name: "Tim Duncan", pts: 28, reb: 14, blk: 3 })],
		});
		const g = game({
			gid: 3603,
			teams: [kings, spurs],
			winnerTid: 5,
			clutchPlays: [
				'<a href="#">Peja Stojakovic</a> made a game-winning three-pointer with 2 seconds remaining.',
			],
		});
		const recap = getAutoRecap(g);
		assert.ok(recap.includes("Peja Stojakovic"), recap);
		assert.ok(/three-pointer/.test(recap), recap);
		// Never invents a bigger margin or an untrue milestone.
		assert.ok(!recap.includes("triple-double"), recap);
	});

	test("triple-double is stated, never inflated", () => {
		const mavs = team({
			tid: 3,
			region: "Dallas",
			name: "Mavericks",
			abbrev: "DAL",
			pts: 104,
			ptsQtrs: [24, 28, 26, 26],
			players: [
				player({
					name: "Michael Finley",
					pts: 21,
					reb: 11,
					ast: 10,
					stl: 2,
				}),
			],
		});
		const suns = team({
			tid: 7,
			region: "Phoenix",
			name: "Suns",
			abbrev: "PHO",
			pts: 98,
			ptsQtrs: [26, 22, 25, 25],
			players: [player({ name: "Jason Kidd", pts: 18, reb: 7, ast: 12 })],
		});
		const g = game({ gid: 3618, teams: [mavs, suns], winnerTid: 3 });
		const recap = getAutoRecap(g);
		assert.ok(recap.includes("triple-double"), recap);
		assert.ok(!recap.includes("quadruple"), recap);
	});

	test("blowout reads as a rout and uses the real score", () => {
		const pistons = team({
			tid: 1,
			region: "Detroit",
			name: "Pistons",
			abbrev: "DET",
			pts: 126,
			ptsQtrs: [34, 30, 32, 30],
			record: { won: 40, lost: 15 },
			players: [
				player({ name: "Richard Hamilton", pts: 27, reb: 4, ast: 5 }),
				player({ name: "Ben Wallace", pts: 12, reb: 18, blk: 4 }),
			],
		});
		const bulls = team({
			tid: 2,
			region: "Chicago",
			name: "Bulls",
			abbrev: "CHI",
			pts: 82,
			ptsQtrs: [20, 22, 20, 20],
			players: [player({ name: "Jamal Crawford", pts: 19, reb: 3, ast: 4 })],
		});
		const g = game({
			gid: 3617,
			teams: [pistons, bulls],
			winnerTid: 1,
			spread: { favTid: 1, points: 9 },
		});
		const recap = getAutoRecap(g);
		assert.ok(recap.includes("126"), recap);
		assert.ok(recap.includes("82"), recap);
	});

	test("upset is framed as one when the underdog wins", () => {
		const clippers = team({
			tid: 4,
			region: "Los Angeles",
			name: "Clippers",
			abbrev: "LAC",
			pts: 99,
			ptsQtrs: [24, 25, 25, 25],
			players: [player({ name: "Elton Brand", pts: 24, reb: 12 })],
		});
		const lakers = team({
			tid: 6,
			region: "Los Angeles",
			name: "Lakers",
			abbrev: "LAL",
			pts: 91,
			ptsQtrs: [22, 23, 23, 23],
			players: [player({ name: "Kobe Bryant", pts: 33, reb: 5, ast: 4 })],
		});
		// Lakers favored by 8, Clippers win.
		const g = game({
			gid: 3620,
			teams: [clippers, lakers],
			winnerTid: 4,
			spread: { favTid: 6, points: 8 },
		});
		const recap = getAutoRecap(g);
		assert.ok(/stun|upset|shock|knock/.test(recap), recap);
	});

	test("overtime is tagged in the score", () => {
		const jazz = team({
			tid: 8,
			region: "Utah",
			name: "Jazz",
			abbrev: "UTA",
			pts: 118,
			ptsQtrs: [26, 24, 26, 22, 20],
			players: [player({ name: "Jahidi White", pts: 33, reb: 15 })],
		});
		const kings = team({
			tid: 5,
			region: "Sacramento",
			name: "Kings",
			abbrev: "SAC",
			pts: 114,
			ptsQtrs: [24, 26, 26, 22, 16],
			players: [player({ name: "Mike Bibby", pts: 26, reb: 4, ast: 9 })],
		});
		const g = game({
			gid: 3623,
			teams: [jazz, kings],
			winnerTid: 8,
			overtimes: 1,
		});
		const recap = getAutoRecap(g);
		assert.ok(recap.includes("(OT)"), recap);
		assert.ok(recap.includes("Jahidi White"), recap);
	});

	test("deterministic: same game always produces the same recap", () => {
		const a = team({
			tid: 1,
			name: "Pistons",
			abbrev: "DET",
			pts: 100,
			ptsQtrs: [25, 25, 25, 25],
			players: [player({ name: "Chauncey Billups", pts: 24, ast: 8 })],
		});
		const b = team({
			tid: 2,
			name: "Pacers",
			abbrev: "IND",
			pts: 95,
			ptsQtrs: [24, 24, 24, 23],
			players: [player({ name: "Reggie Miller", pts: 22 })],
		});
		const g = game({ gid: 4242, teams: [a, b], winnerTid: 1 });
		assert.equal(getAutoRecap(g), getAutoRecap(g));
	});

	test("playoff series state is reported after the game", () => {
		const celtics = team({
			tid: 1,
			region: "Boston",
			name: "Celtics",
			abbrev: "BOS",
			pts: 101,
			ptsQtrs: [24, 26, 25, 26],
			seed: 2,
			players: [player({ name: "Paul Pierce", pts: 30, reb: 8, ast: 6 })],
		});
		const pistons = team({
			tid: 2,
			region: "Detroit",
			name: "Pistons",
			abbrev: "DET",
			pts: 96,
			ptsQtrs: [24, 24, 24, 24],
			seed: 3,
			players: [player({ name: "Chauncey Billups", pts: 25, ast: 7 })],
		});
		const g = game({
			gid: 5001,
			teams: [celtics, pistons],
			winnerTid: 1,
			playoffs: true,
			series: {
				round: 2,
				numRounds: 4,
				bestOf: 7,
				homeAbbrev: "BOS",
				awayAbbrev: "DET",
				homeSeed: 2,
				awaySeed: 3,
				homeWon: 2,
				awayWon: 1,
			},
		});
		const recap = getAutoRecap(g);
		// Boston led 2-1 entering; now 3-1.
		assert.ok(recap.includes("3-1"), recap);
	});
});

// A visual sample of a slate, printed once so the output can be eyeballed.
test("print a sample slate", () => {
	const mk = (
		gid: number,
		homeName: string,
		awayName: string,
		homePts: number,
		awayPts: number,
		winnerHome: boolean,
		homePlayers: RecapPlayer[],
		awayPlayers: RecapPlayer[],
		extra: Partial<RecapGame> = {},
	): RecapGame => {
		const home = team({
			tid: gid * 2,
			name: homeName,
			abbrev: homeName.slice(0, 3).toUpperCase(),
			pts: homePts,
			ptsQtrs: [
				Math.round(homePts / 4),
				Math.round(homePts / 4),
				Math.round(homePts / 4),
				homePts - 3 * Math.round(homePts / 4),
			],
			players: homePlayers,
		});
		const away = team({
			tid: gid * 2 + 1,
			name: awayName,
			abbrev: awayName.slice(0, 3).toUpperCase(),
			pts: awayPts,
			ptsQtrs: [
				Math.round(awayPts / 4),
				Math.round(awayPts / 4),
				Math.round(awayPts / 4),
				awayPts - 3 * Math.round(awayPts / 4),
			],
			players: awayPlayers,
		});
		return game({
			gid,
			teams: [home, away],
			winnerTid: winnerHome ? home.tid : away.tid,
			...extra,
		});
	};

	const slate: RecapGame[] = [
		mk(
			3603,
			"Kings",
			"Spurs",
			112,
			109,
			true,
			[
				player({ name: "Peja Stojakovic", pts: 31, reb: 6, ast: 4, tp: 5 }),
				player({ name: "Chris Webber", pts: 22, reb: 11, ast: 7 }),
			],
			[player({ name: "Tim Duncan", pts: 28, reb: 14, blk: 3 })],
			{
				clutchPlays: [
					'<a href="#">Peja Stojakovic</a> made a game-winning three-pointer with 2 seconds remaining.',
				],
			},
		),
		mk(
			3617,
			"Pistons",
			"Bulls",
			126,
			82,
			true,
			[
				player({ name: "Richard Hamilton", pts: 27, reb: 4, ast: 5 }),
				player({ name: "Ben Wallace", pts: 12, reb: 18, blk: 4 }),
			],
			[player({ name: "Jamal Crawford", pts: 19 })],
			{ spread: { favTid: 3617 * 2, points: 9 } },
		),
		mk(
			3618,
			"Mavericks",
			"Suns",
			104,
			98,
			true,
			[player({ name: "Michael Finley", pts: 21, reb: 11, ast: 10, stl: 2 })],
			[player({ name: "Jason Kidd", pts: 18, reb: 7, ast: 12 })],
		),
		mk(
			3623,
			"Jazz",
			"Kings",
			118,
			114,
			true,
			[player({ name: "Jahidi White", pts: 33, reb: 15 })],
			[player({ name: "Mike Bibby", pts: 26, reb: 4, ast: 9 })],
			{ overtimes: 1 },
		),
		mk(
			3630,
			"Clippers",
			"Lakers",
			99,
			91,
			true,
			[player({ name: "Elton Brand", pts: 24, reb: 12 })],
			[player({ name: "Kobe Bryant", pts: 33, reb: 5, ast: 4 })],
			{ spread: { favTid: 3630 * 2 + 1, points: 8 } },
		),
	];

	console.log(
		"\n===== SAMPLE AUTO-RECAPS =====\n" +
			slate.map((g) => getAutoRecap(g)).join("\n\n---\n\n") +
			"\n==============================\n",
	);
	assert.ok(true);
});
