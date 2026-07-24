import assert from "node:assert/strict";
import { describe, test } from "vitest";
import { getAutoRecap, getAutoDayRecap } from "./getAutoRecap.ts";
import type {
	RecapAverages,
	RecapGame,
	RecapPlayer,
	RecapTeam,
} from "./getDayGamesForRecap.ts";

// A box-score line with only the fields a test cares about; the rest default to
// zero so fixtures stay short.
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

const avg = (a: Partial<RecapAverages>): RecapAverages => ({
	gp: 40,
	min: 32,
	pts: 0,
	reb: 0,
	ast: 0,
	stl: 0,
	blk: 0,
	tov: 0,
	fgp: 45,
	tpp: 35,
	ftp: 80,
	...a,
});

// A realistic team: a star, three more starters, and a couple bench pieces, so
// team totals (rebounds, assists, shooting) land in a believable range.
const realisticTeam = (
	base: Partial<RecapTeam> & { name: string },
	star: RecapPlayer,
): RecapTeam => ({
	tid: 0,
	region: "",
	abbrev: base.abbrev ?? "???",
	pts: 100,
	players: [
		star,
		player({ name: "Role One", pts: 16, reb: 7, ast: 3, fg: 6, fga: 12 }),
		player({ name: "Role Two", pts: 13, reb: 9, ast: 2, fg: 5, fga: 11 }),
		player({ name: "Role Three", pts: 11, reb: 5, ast: 6, fg: 4, fga: 9 }),
		player({ name: "Bench One", pts: 8, reb: 4, ast: 2, fg: 3, fga: 7 }),
		player({ name: "Bench Two", pts: 6, reb: 6, ast: 1, fg: 2, fga: 6 }),
		player({ name: "Bench Three", pts: 4, reb: 3, ast: 2, fg: 1, fga: 4 }),
	],
	...base,
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
				player({ name: "Michael Finley", pts: 21, reb: 11, ast: 10, stl: 2 }),
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
		// Sparse fixture must never yield an absurd rebound line.
		assert.ok(!/won the glass \d+-0\b/.test(recap), recap);
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

	test("entering average is used when a scorer erupts", () => {
		const star = player({
			name: "Allen Iverson",
			pts: 44,
			reb: 4,
			ast: 7,
			fg: 16,
			fga: 30,
			seasonAvg: avg({ pts: 26, ast: 6, gp: 45 }),
		});
		const sixers = realisticTeam(
			{
				tid: 11,
				region: "Philadelphia",
				name: "76ers",
				abbrev: "PHI",
				pts: 108,
				ptsQtrs: [27, 25, 28, 28],
			},
			star,
		);
		const knicks = realisticTeam(
			{
				tid: 12,
				region: "New York",
				name: "Knicks",
				abbrev: "NYK",
				pts: 101,
				ptsQtrs: [26, 24, 25, 26],
			},
			player({ name: "Latrell Sprewell", pts: 25, reb: 5, ast: 4 }),
		);
		const g = game({ gid: 3650, teams: [sixers, knicks], winnerTid: 11 });
		const recap = getAutoRecap(g);
		assert.ok(recap.includes("averaging 26"), recap);
		assert.ok(recap.includes("44"), recap);
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
});

describe("recap quality (from real Day 1 output)", () => {
	test("18-point comeback never reads 'a 18-point'", () => {
		// Cumulative after Q2: winner 40, loser 58 - an 18-point hole.
		const cavs = realisticTeam(
			{
				tid: 30,
				region: "Cleveland",
				name: "Cavaliers",
				abbrev: "CLE",
				pts: 105,
				ptsQtrs: [20, 20, 35, 30],
			},
			player({ name: "Paul Pierce", pts: 23, blk: 4, fg: 9, fga: 17 }),
		);
		const pistons = realisticTeam(
			{
				tid: 31,
				region: "Detroit",
				name: "Pistons",
				abbrev: "DET",
				pts: 103,
				ptsQtrs: [30, 28, 25, 20],
			},
			player({ name: "Jason Richardson", pts: 28, ast: 7 }),
		);
		// Sweep several seeds so both headline/flow variants get exercised.
		for (const gid of [9001, 9002, 9003, 9004, 9005]) {
			const recap = getAutoRecap(
				game({ gid, teams: [cavs, pistons], winnerTid: 30 }),
			);
			assert.ok(!/\ba 18-point/.test(recap), recap);
			assert.ok(!/\ba 18 point/.test(recap), recap);
		}
	});

	test("the lead sentence never undersells the star with a support verb", () => {
		const raptors = realisticTeam(
			{
				tid: 32,
				region: "Toronto",
				name: "Raptors",
				abbrev: "TOR",
				pts: 94,
				ptsQtrs: [24, 24, 23, 23],
			},
			player({ name: "Pau Gasol", pts: 22, reb: 9, fg: 9, fga: 16 }),
		);
		const warriors = realisticTeam(
			{
				tid: 33,
				region: "Golden State",
				name: "Warriors",
				abbrev: "GSW",
				pts: 65,
				ptsQtrs: [16, 16, 17, 16],
			},
			player({ name: "Jason Richardson", pts: 14 }),
		);
		for (const gid of [9010, 9011, 9012, 9013, 9014]) {
			const recap = getAutoRecap(
				game({ gid, teams: [raptors, warriors], winnerTid: 32 }),
			);
			const body = recap.split("\n\n")[1]!;
			assert.ok(!/^Pau Gasol (added|chipped in|contributed)/.test(body), body);
		}
	});

	test("an efficient 94-65 blowout is never called a defensive grind", () => {
		const raptors = realisticTeam(
			{
				tid: 32,
				region: "Toronto",
				name: "Raptors",
				abbrev: "TOR",
				pts: 94,
				ptsQtrs: [24, 24, 23, 23],
			},
			// High-efficiency star pushes team fgp well above the grind bar.
			player({ name: "Pau Gasol", pts: 22, reb: 9, fg: 10, fga: 14 }),
		);
		const warriors = realisticTeam(
			{
				tid: 33,
				region: "Golden State",
				name: "Warriors",
				abbrev: "GSW",
				pts: 65,
				ptsQtrs: [16, 16, 17, 16],
			},
			player({ name: "Jason Richardson", pts: 14 }),
		);
		for (const gid of [9020, 9021, 9022, 9023, 9024]) {
			const recap = getAutoRecap(
				game({ gid, teams: [raptors, warriors], winnerTid: 32 }),
			);
			assert.ok(!/Neither offense got going/.test(recap), recap);
		}
	});

	test("a game-winner always gets its own beat in the body, with timing", () => {
		const kings = realisticTeam(
			{
				tid: 34,
				region: "Sacramento",
				name: "Kings",
				abbrev: "SAC",
				pts: 102,
				ptsQtrs: [20, 25, 27, 30],
			},
			player({ name: "Shareef Abdur-Rahim", pts: 22, reb: 13, ast: 8 }),
		);
		const grizzlies = realisticTeam(
			{
				tid: 35,
				region: "Memphis",
				name: "Grizzlies",
				abbrev: "MEM",
				pts: 100,
				ptsQtrs: [32, 25, 23, 20],
			},
			player({ name: "Troy Hudson", pts: 20, ast: 6 }),
		);
		const g = game({
			gid: 9030,
			teams: [kings, grizzlies],
			winnerTid: 34,
			clutchPlays: [
				'<a href="#">Lindsey Hunter</a> made a game-winning basket with 2 seconds remaining.',
			],
		});
		const recap = getAutoRecap(g);
		// The generic "basket" reads as a real term, and the body describes the
		// moment even though the lead is about a different player.
		assert.ok(/game-winner/.test(recap), recap);
		assert.ok(
			recap.includes(
				"Lindsey Hunter won it with a game-winner with 2 seconds left",
			),
			recap,
		);
	});

	test("a true buzzer-beater is labeled one", () => {
		const kings = realisticTeam(
			{
				tid: 34,
				region: "Sacramento",
				name: "Kings",
				abbrev: "SAC",
				pts: 102,
				ptsQtrs: [25, 25, 26, 26],
			},
			player({ name: "Mike Bibby", pts: 24, ast: 7 }),
		);
		const grizzlies = realisticTeam(
			{
				tid: 35,
				region: "Memphis",
				name: "Grizzlies",
				abbrev: "MEM",
				pts: 100,
				ptsQtrs: [26, 25, 25, 24],
			},
			player({ name: "Troy Hudson", pts: 20 }),
		);
		const g = game({
			gid: 9031,
			teams: [kings, grizzlies],
			winnerTid: 34,
			clutchPlays: [
				'<a href="#">Mike Bibby</a> made a game-winning basket at the buzzer.',
			],
		});
		const recap = getAutoRecap(g);
		assert.ok(/buzzer-beater/.test(recap), recap);
		// Never the redundant "buzzer-beater at the buzzer".
		assert.ok(!/buzzer-beater at the buzzer/.test(recap), recap);
	});

	test("a 9-point stat-stuffer never carries the lead over a real scoring line", () => {
		const lakers = team({
			tid: 36,
			region: "Los Angeles",
			name: "Lakers",
			abbrev: "LAL",
			pts: 87,
			ptsQtrs: [22, 22, 22, 21],
			players: [
				// Impact loves this line (steals, boards, low usage)...
				player({ name: "Nene", pts: 9, reb: 11, stl: 4, fg: 4, fga: 6 }),
				// ...but the lead should belong to a genuine scoring night.
				player({
					name: "Andre Miller",
					pts: 15,
					reb: 12,
					ast: 6,
					fg: 6,
					fga: 12,
				}),
				player({ name: "Maurice Taylor", pts: 13, reb: 10, fg: 5, fga: 11 }),
				player({ name: "Role Four", pts: 11, reb: 4, fg: 4, fga: 9 }),
				player({ name: "Role Five", pts: 8, reb: 5, fg: 3, fga: 7 }),
			],
		});
		const sixers = realisticTeam(
			{
				tid: 37,
				region: "Philadelphia",
				name: "76ers",
				abbrev: "PHI",
				pts: 76,
				ptsQtrs: [19, 19, 19, 19],
			},
			player({ name: "Keith Van Horn", pts: 18, reb: 7 }),
		);
		for (const gid of [9040, 9041, 9042]) {
			const recap = getAutoRecap(
				game({ gid, teams: [lakers, sixers], winnerTid: 36 }),
			);
			const lead = recap.split("\n\n")[1]!.split(". ")[0]!;
			assert.ok(!lead.startsWith("Nene"), recap);
		}
	});
});

describe("getAutoRecap playoffs", () => {
	const series = (
		over: Partial<RecapGame["series"]> = {},
	): RecapGame["series"] => ({
		round: 1,
		numRounds: 4,
		bestOf: 7,
		homeAbbrev: "BOS",
		awayAbbrev: "DET",
		homeSeed: 2,
		awaySeed: 3,
		homeWon: 0,
		awayWon: 0,
		...over,
	});

	const playoffGame = (
		homeWon: number,
		awayWon: number,
		winnerHome: boolean,
		seriesOver: Partial<RecapGame["series"]> = {},
		gid = 6000,
	): RecapGame => {
		const boston = realisticTeam(
			{
				tid: 1,
				region: "Boston",
				name: "Celtics",
				abbrev: "BOS",
				pts: 101,
				ptsQtrs: [26, 24, 25, 26],
				seed: 2,
			},
			player({ name: "Paul Pierce", pts: 30, reb: 8, ast: 6 }),
		);
		const detroit = realisticTeam(
			{
				tid: 2,
				region: "Detroit",
				name: "Pistons",
				abbrev: "DET",
				pts: 96,
				ptsQtrs: [24, 24, 24, 24],
				seed: 3,
			},
			player({ name: "Chauncey Billups", pts: 25, ast: 7 }),
		);
		return game({
			gid,
			teams: [boston, detroit],
			winnerTid: winnerHome ? 1 : 2,
			playoffs: true,
			series: series({ homeWon, awayWon, ...seriesOver }),
		});
	};

	test("series lead is reported after the game", () => {
		// Boston up 2-1 entering, wins Game 4 -> 3-1.
		const recap = getAutoRecap(
			playoffGame(2, 1, true, { round: 2, numRounds: 4 }),
		);
		assert.ok(recap.includes("3-1"), recap);
		assert.ok(/Conference Semifinals/.test(recap), recap);
	});

	test("closeout is called a clinch and advance", () => {
		// Boston up 3-1, wins Game 5 -> 4-1, series over.
		const recap = getAutoRecap(playoffGame(3, 1, true, { round: 1 }));
		assert.ok(/closed out|advanced/.test(recap), recap);
	});

	test("Finals clincher says champions", () => {
		const recap = getAutoRecap(
			playoffGame(3, 2, true, { round: 4, numRounds: 4 }),
		);
		assert.ok(/champions|title/.test(recap), recap);
	});

	test("Game 7 and elimination are flagged", () => {
		// Series tied 3-3, Game 7, Boston wins.
		const recap = getAutoRecap(playoffGame(3, 3, true, { round: 1 }));
		assert.ok(/Game 7/.test(recap), recap);
	});

	test("staving off elimination is described", () => {
		// Boston down 1-3, wins Game 5 to stay alive -> still trails 3-2.
		const recap = getAutoRecap(playoffGame(1, 3, true, { round: 1 }));
		assert.ok(/elimination/.test(recap), recap);
		assert.ok(recap.includes("3-2"), recap);
	});

	test("best-of-5 decider is Game 5, never Game 7", () => {
		const boston = realisticTeam(
			{
				tid: 1,
				region: "Boston",
				name: "Celtics",
				abbrev: "BOS",
				pts: 94,
				ptsQtrs: [20, 29, 23, 22],
				seed: 2,
			},
			player({ name: "Paul Pierce", pts: 24, reb: 8 }),
		);
		const detroit = realisticTeam(
			{
				tid: 2,
				region: "Detroit",
				name: "Pistons",
				abbrev: "DET",
				pts: 73,
				ptsQtrs: [18, 14, 20, 21],
				seed: 3,
			},
			player({ name: "Antoine Walker", pts: 20, reb: 6 }),
		);
		// Series tied 2-2 in a best-of-5; Boston wins Game 5 to clinch.
		const g = game({
			gid: 6200,
			teams: [boston, detroit],
			winnerTid: 1,
			playoffs: true,
			series: {
				round: 1,
				numRounds: 4,
				bestOf: 5,
				homeAbbrev: "BOS",
				awayAbbrev: "DET",
				homeSeed: 2,
				awaySeed: 3,
				homeWon: 2,
				awayWon: 2,
			},
		});
		const recap = getAutoRecap(g);
		assert.ok(!/Game 7/.test(recap), recap);
		assert.ok(/Game 5/.test(recap), recap);
		assert.ok(/closed out|advanced/.test(recap), recap);
	});
});

describe("getAutoRecap play-in", () => {
	const playInGame = (
		kind: "seed7v8" | "seed9v10" | "final",
		prizeSeed: number | undefined,
	): RecapGame => {
		const a = realisticTeam(
			{
				tid: 1,
				region: "Miami",
				name: "Heat",
				abbrev: "MIA",
				pts: 104,
				ptsQtrs: [26, 26, 26, 26],
			},
			player({ name: "Dwyane Wade", pts: 28, reb: 6, ast: 7 }),
		);
		const b = realisticTeam(
			{
				tid: 2,
				region: "Atlanta",
				name: "Hawks",
				abbrev: "ATL",
				pts: 99,
				ptsQtrs: [24, 25, 25, 25],
			},
			player({ name: "Joe Johnson", pts: 26, reb: 4, ast: 5 }),
		);
		return game({
			gid: 7000,
			teams: [a, b],
			winnerTid: 1,
			playoffs: true,
			playIn: {
				kind,
				homeAbbrev: "MIA",
				awayAbbrev: "ATL",
				homeSeed: kind === "seed7v8" ? 7 : kind === "seed9v10" ? 9 : 8,
				awaySeed: kind === "seed7v8" ? 8 : kind === "seed9v10" ? 10 : 9,
				prizeSeed,
			},
		});
	};

	test("7-vs-8 winner claims the higher seed", () => {
		const recap = getAutoRecap(playInGame("seed7v8", 7));
		assert.ok(/#7 seed/.test(recap), recap);
		assert.ok(/win-or-go-home/.test(recap), recap);
	});

	test("9-vs-10 elimination ends a season", () => {
		const recap = getAutoRecap(playInGame("seed9v10", undefined));
		assert.ok(/season is over/.test(recap), recap);
	});

	test("final play-in grabs the last playoff spot", () => {
		const recap = getAutoRecap(playInGame("final", 8));
		assert.ok(/last playoff berth/.test(recap), recap);
		assert.ok(/#8 seed/.test(recap), recap);
	});
});

// A slate builder for the day-recap tests and the printed sample.
const mkGame = (
	gid: number,
	homeName: string,
	awayName: string,
	homePts: number,
	awayPts: number,
	winnerHome: boolean,
	homeStar: RecapPlayer,
	awayStar: RecapPlayer,
	extra: Partial<RecapGame> = {},
): RecapGame => {
	const q = (pts: number): number[] => [
		Math.round(pts / 4),
		Math.round(pts / 4),
		Math.round(pts / 4),
		pts - 3 * Math.round(pts / 4),
	];
	const home = realisticTeam(
		{
			tid: gid * 2,
			name: homeName,
			abbrev: homeName.slice(0, 3).toUpperCase(),
			pts: homePts,
			ptsQtrs: q(homePts),
		},
		homeStar,
	);
	const away = realisticTeam(
		{
			tid: gid * 2 + 1,
			name: awayName,
			abbrev: awayName.slice(0, 3).toUpperCase(),
			pts: awayPts,
			ptsQtrs: q(awayPts),
		},
		awayStar,
	);
	return game({
		gid,
		teams: [home, away],
		winnerTid: winnerHome ? home.tid : away.tid,
		...extra,
	});
};

describe("getAutoDayRecap", () => {
	const slate: RecapGame[] = [
		mkGame(
			3603,
			"Kings",
			"Spurs",
			112,
			109,
			true,
			player({ name: "Peja Stojakovic", pts: 31, reb: 6, ast: 4, tp: 5 }),
			player({ name: "Tim Duncan", pts: 28, reb: 14, blk: 3 }),
			{
				clutchPlays: [
					'<a href="#">Peja Stojakovic</a> made a game-winning three-pointer with 2 seconds remaining.',
				],
			},
		),
		mkGame(
			3617,
			"Pistons",
			"Bulls",
			126,
			82,
			true,
			player({ name: "Richard Hamilton", pts: 27, reb: 4, ast: 5 }),
			player({ name: "Jamal Crawford", pts: 19 }),
			{ spread: { favTid: 3617 * 2, points: 9 } },
		),
		mkGame(
			3623,
			"Jazz",
			"Kings",
			118,
			114,
			true,
			player({ name: "Jahidi White", pts: 33, reb: 15 }),
			player({ name: "Mike Bibby", pts: 26, reb: 4, ast: 9 }),
			{ overtimes: 1 },
		),
		mkGame(
			3630,
			"Clippers",
			"Lakers",
			99,
			91,
			true,
			player({ name: "Elton Brand", pts: 24, reb: 12 }),
			player({ name: "Kobe Bryant", pts: 41, reb: 5, ast: 4 }),
			{ spread: { favTid: 3630 * 2 + 1, points: 8 } },
		),
	];

	test("covers the day with a headline and multiple sentences", () => {
		const recap = getAutoDayRecap({
			season: 2005,
			day: 88,
			playoffs: false,
			games: slate,
			standings: {
				day: 88,
				confs: [
					{
						name: "Eastern Conference",
						teams: [
							{
								rank: 1,
								abbrev: "DET",
								region: "Detroit",
								name: "Pistons",
								won: 44,
								lost: 15,
								gb: 0,
							},
						],
					},
					{
						name: "Western Conference",
						teams: [
							{
								rank: 1,
								abbrev: "SAS",
								region: "San Antonio",
								name: "Spurs",
								won: 46,
								lost: 13,
								gb: 0,
							},
						],
					},
				],
			},
		});
		assert.ok(recap.startsWith("**"), recap);
		// Names the day's top scorer (Kobe, 41).
		assert.ok(recap.includes("Kobe Bryant"), recap);
		// Weaves in the standings picture.
		assert.ok(/Detroit Pistons|San Antonio Spurs/.test(recap), recap);
		// A couple of paragraphs of coverage.
		assert.ok(recap.length > 200, recap);
	});

	test("headline reflects the day's biggest story, not a generic slate", () => {
		// A day whose marquee is a walk-off buzzer-beater.
		const buzzerDay = getAutoDayRecap({
			season: 2005,
			day: 90,
			playoffs: false,
			games: slate,
		});
		const buzzerHead = buzzerDay.split("\n")[0]!;
		assert.ok(!/-game slate/.test(buzzerHead), buzzerHead);
		assert.ok(buzzerHead.includes("Peja Stojakovic"), buzzerHead);

		// A day whose only stories are lopsided results reads as a rout headline.
		const routDay = getAutoDayRecap({
			season: 2005,
			day: 91,
			playoffs: false,
			games: [
				mkGame(
					4001,
					"Spurs",
					"Hawks",
					120,
					88,
					true,
					player({ name: "Tim Duncan", pts: 24, reb: 14 }),
					player({ name: "Al Harrington", pts: 18 }),
				),
				mkGame(
					4002,
					"Suns",
					"Bobcats",
					118,
					90,
					true,
					player({ name: "Steve Nash", pts: 19, ast: 15 }),
					player({ name: "Gerald Wallace", pts: 20 }),
				),
			],
		});
		const routHead = routDay.split("\n")[0]!;
		assert.ok(/rout|blow out/.test(routHead), routHead);
	});

	test("a buzzer-beater on an upset-filled night gets a league-scope headline", () => {
		// Marquee: an underdog wins at the buzzer. Two more upsets elsewhere.
		const games = [
			mkGame(
				5001,
				"Kings",
				"Grizzlies",
				102,
				100,
				true,
				player({ name: "Shareef Abdur-Rahim", pts: 22, reb: 13, ast: 8 }),
				player({ name: "Troy Hudson", pts: 20 }),
				{
					spread: { favTid: 5001 * 2 + 1, points: 5.5 },
					clutchPlays: [
						'<a href="#">Lindsey Hunter</a> made a game-winning basket at the buzzer.',
					],
				},
			),
			mkGame(
				5002,
				"Nuggets",
				"Knicks",
				85,
				82,
				true,
				player({ name: "Rodney White", pts: 22, reb: 9 }),
				player({ name: "Dion Glover", pts: 22 }),
				{ spread: { favTid: 5002 * 2 + 1, points: 6 } },
			),
			mkGame(
				5003,
				"SuperSonics",
				"Trail Blazers",
				91,
				84,
				true,
				player({ name: "Glenn Robinson", pts: 24, reb: 10 }),
				player({ name: "Rasheed Wallace", pts: 21 }),
				{ spread: { favTid: 5003 * 2 + 1, points: 7 } },
			),
		];
		const recap = getAutoDayRecap({
			season: 2003,
			day: 1,
			playoffs: false,
			games,
		});
		const head = recap.split("\n")[0]!;
		// The headline is about the league's night, not one game's box score.
		assert.ok(/night of upsets|favorites fall/.test(head), head);
		// The upsets roundup varies its verbs and carries the biggest spread.
		assert.ok(!/upset .* upset/.test(recap), recap);
	});

	test("meaningless 1-0 standings are left out of the day recap", () => {
		const games = [
			mkGame(
				5010,
				"Hawks",
				"Suns",
				102,
				86,
				true,
				player({ name: "Gary Payton", pts: 23, ast: 14, stl: 5 }),
				player({ name: "Richard Jefferson", pts: 18 }),
			),
		];
		const recap = getAutoDayRecap({
			season: 2003,
			day: 1,
			playoffs: false,
			games,
			standings: {
				day: 1,
				confs: [
					{
						name: "Eastern Conference",
						teams: [
							{
								rank: 1,
								abbrev: "ATL",
								region: "Atlanta",
								name: "Hawks",
								won: 1,
								lost: 0,
								gb: 0,
							},
						],
					},
				],
			},
		});
		assert.ok(!/standings|narrow lead|atop the/.test(recap), recap);
	});

	test("the day recap covers the night's injuries", () => {
		const hurtStar = player({
			name: "Vince Carter",
			pts: 12,
			injury: { type: "Sprained Ankle", gamesRemaining: 5, newThisGame: true },
		});
		const games = [
			mkGame(
				5020,
				"Raptors",
				"Nets",
				100,
				90,
				true,
				player({ name: "Morris Peterson", pts: 24, reb: 6 }),
				hurtStar,
			),
		];
		const recap = getAutoDayRecap({
			season: 2003,
			day: 2,
			playoffs: false,
			games,
		});
		assert.ok(/On the injury front/.test(recap), recap);
		assert.ok(
			/Vince Carter \(sprained ankle, out ~5 games\)/.test(recap),
			recap,
		);
	});
});

describe("regressions from real games", () => {
	const finley = player({
		name: "Michael Finley",
		pts: 18,
		reb: 12,
		ast: 12,
		fg: 7,
		fga: 14,
	});
	const heat = realisticTeam(
		{
			tid: 50,
			region: "Miami",
			name: "Heat",
			abbrev: "MIA",
			pts: 105,
			ptsQtrs: [26, 26, 27, 26],
			seed: 2,
			streak: { won: true, count: 5 },
		},
		finley,
	);
	const bulls = realisticTeam(
		{
			tid: 51,
			region: "Chicago",
			name: "Bulls",
			abbrev: "CHI",
			pts: 78,
			ptsQtrs: [22, 26, 15, 15],
			seed: 3,
		},
		player({ name: "Voshon Lenard", pts: 14, reb: 3, stl: 3 }),
	);
	const heatGame = game({
		gid: 8001,
		teams: [heat, bulls],
		winnerTid: 50,
		playoffs: true,
		series: {
			round: 3,
			numRounds: 4,
			bestOf: 7,
			homeAbbrev: "MIA",
			awayAbbrev: "CHI",
			homeSeed: 2,
			awaySeed: 3,
			homeWon: 1,
			awayWon: 0,
		},
	});

	const shaq = player({
		name: "Shaquille O'Neal",
		pts: 27,
		reb: 15,
		ast: 7,
		blk: 6,
		fg: 12,
		fga: 19,
	});
	const spurs = realisticTeam(
		{
			tid: 52,
			region: "San Antonio",
			name: "Spurs",
			abbrev: "SAS",
			pts: 93,
			ptsQtrs: [28, 20, 23, 22],
			seed: 2,
		},
		shaq,
	);
	const kings = realisticTeam(
		{
			tid: 53,
			region: "Sacramento",
			name: "Kings",
			abbrev: "SAC",
			pts: 79,
			ptsQtrs: [16, 21, 21, 21],
		},
		player({ name: "Shareef Abdur-Rahim", pts: 24, reb: 20, blk: 3 }),
	);
	const spursGame = game({
		gid: 8002,
		teams: [spurs, kings],
		winnerTid: 52,
		playoffs: true,
		series: {
			round: 3,
			numRounds: 4,
			bestOf: 7,
			homeAbbrev: "SAS",
			awayAbbrev: "SAC",
			homeSeed: 2,
			awaySeed: 4,
			homeWon: 1,
			awayWon: 0,
		},
	});

	test("team possessive reads right for a name not ending in s", () => {
		const recap = getAutoRecap(heatGame);
		assert.ok(recap.includes("the Heat's fifth in a row"), recap);
		assert.ok(!/Heat' /.test(recap), recap);
	});

	test("a shot-blocker's signature stat survives into the body", () => {
		const recap = getAutoRecap(spursGame);
		// Headline is about blocks; the body line must include them too.
		assert.ok(/blocks anchor/.test(recap), recap);
		assert.ok(/6 blocks/.test(recap), recap);
	});

	test("day recap never names the marquee star twice or repeats a series line", () => {
		const recap = getAutoDayRecap({
			season: 2005,
			day: 100,
			playoffs: true,
			games: [heatGame, spursGame],
		});
		// A full Spurs/Grizzlies fixture to eyeball blocks-in-body, no double
		// "Conference Semifinals", subject dedupe, and suppressed halftime echo.
		const spursFull = realisticTeam(
			{
				tid: 60,
				region: "San Antonio",
				name: "Spurs",
				abbrev: "SAS",
				pts: 113,
				ptsQtrs: [30, 25, 29, 29],
				seed: 2,
			},
			player({
				name: "Shaquille O'Neal",
				pts: 26,
				reb: 15,
				ast: 7,
				blk: 6,
				fg: 9,
				fga: 12,
			}),
		);
		const grizFull = realisticTeam(
			{
				tid: 61,
				region: "Memphis",
				name: "Grizzlies",
				abbrev: "MEM",
				pts: 93,
				ptsQtrs: [20, 17, 28, 28],
			},
			player({ name: "David Robinson", pts: 19, reb: 7, blk: 4 }),
		);
		const spursFullGame = game({
			gid: 8100,
			teams: [spursFull, grizFull],
			winnerTid: 60,
			playoffs: true,
			series: {
				round: 2,
				numRounds: 4,
				bestOf: 7,
				homeAbbrev: "SAS",
				awayAbbrev: "MEM",
				homeSeed: 2,
				awaySeed: 6,
				homeWon: 2,
				awayWon: 1,
			},
		});
		// Blocks (the headline stat) must reach the body; no doubled round name.
		const spursRecap = getAutoRecap(spursFullGame);
		assert.ok(/6 blocks/.test(spursRecap), spursRecap);
		assert.ok(!/Conference Semifinals win/.test(spursRecap), spursRecap);
		// Subject dedupe turns a repeated "The Spurs" into "They".
		assert.ok(
			!/The Spurs [a-z].*\. The Spurs [a-z]/.test(spursRecap),
			spursRecap,
		);
		const finleyCount = recap.split("Michael Finley").length - 1;
		assert.ok(finleyCount <= 1, recap);
		// The two 2-0 series must not read as the identical sentence twice.
		assert.ok(
			!/took a 2-0 lead in the Conference Finals\. .*took a 2-0 lead in the Conference Finals\./.test(
				recap,
			),
			recap,
		);
		// Headline uses the article ("the Heat", not "Heat past").
		assert.ok(!/powers Heat /.test(recap), recap);
	});
});

// Printed samples so the output can be eyeballed.
test("print sample recaps", () => {
	const out: string[] = [];

	const rich = getAutoRecap(
		(() => {
			const star = player({
				name: "Allen Iverson",
				pts: 44,
				reb: 4,
				ast: 8,
				stl: 3,
				fg: 16,
				fga: 31,
				tp: 4,
				seasonAvg: avg({ pts: 26, ast: 6 }),
			});
			const sixers = realisticTeam(
				{
					tid: 11,
					region: "Philadelphia",
					name: "76ers",
					abbrev: "PHI",
					pts: 110,
					ptsQtrs: [22, 30, 28, 30],
					record: { won: 33, lost: 26 },
					streak: { won: true, count: 4 },
				},
				star,
			);
			const knicks = realisticTeam(
				{
					tid: 12,
					region: "New York",
					name: "Knicks",
					abbrev: "NYK",
					pts: 103,
					ptsQtrs: [28, 26, 24, 25],
				},
				player({ name: "Stephon Marbury", pts: 27, reb: 3, ast: 9, tov: 6 }),
			);
			return game({
				gid: 3701,
				teams: [sixers, knicks],
				winnerTid: 11,
				spread: { favTid: 12, points: 5 },
			});
		})(),
	);
	out.push(rich);

	const p = getAutoRecap(
		(() => {
			const boston = realisticTeam(
				{
					tid: 1,
					region: "Boston",
					name: "Celtics",
					abbrev: "BOS",
					pts: 94,
					ptsQtrs: [20, 26, 22, 26],
					seed: 2,
				},
				player({
					name: "Paul Pierce",
					pts: 34,
					reb: 9,
					ast: 5,
					playoffAvg: avg({ pts: 25, reb: 7, gp: 5 }),
				}),
			);
			const detroit = realisticTeam(
				{
					tid: 2,
					region: "Detroit",
					name: "Pistons",
					abbrev: "DET",
					pts: 91,
					ptsQtrs: [24, 22, 23, 22],
					seed: 3,
				},
				player({ name: "Chauncey Billups", pts: 28, ast: 8 }),
			);
			return game({
				gid: 6100,
				teams: [boston, detroit],
				winnerTid: 1,
				playoffs: true,
				series: {
					round: 3,
					numRounds: 4,
					bestOf: 7,
					homeAbbrev: "BOS",
					awayAbbrev: "DET",
					homeSeed: 2,
					awaySeed: 3,
					homeWon: 3,
					awayWon: 3,
				},
			});
		})(),
	);
	out.push(p);

	const dayRecap = getAutoDayRecap({
		season: 2005,
		day: 88,
		playoffs: false,
		games: [
			mkGame(
				3603,
				"Kings",
				"Spurs",
				112,
				109,
				true,
				player({ name: "Peja Stojakovic", pts: 31, reb: 6, ast: 4, tp: 5 }),
				player({ name: "Tim Duncan", pts: 28, reb: 14, blk: 3 }),
				{
					clutchPlays: [
						'<a href="#">Peja Stojakovic</a> made a game-winning three-pointer with 2 seconds remaining.',
					],
				},
			),
			mkGame(
				3617,
				"Pistons",
				"Bulls",
				126,
				82,
				true,
				player({ name: "Richard Hamilton", pts: 27, reb: 4, ast: 5 }),
				player({ name: "Jamal Crawford", pts: 19 }),
				{ spread: { favTid: 3617 * 2, points: 9 } },
			),
			mkGame(
				3630,
				"Clippers",
				"Lakers",
				99,
				91,
				true,
				player({ name: "Elton Brand", pts: 24, reb: 12 }),
				player({ name: "Kobe Bryant", pts: 41, reb: 5, ast: 4 }),
				{ spread: { favTid: 3630 * 2 + 1, points: 8 } },
			),
		],
		standings: {
			day: 88,
			confs: [
				{
					name: "Eastern Conference",
					teams: [
						{
							rank: 1,
							abbrev: "DET",
							region: "Detroit",
							name: "Pistons",
							won: 44,
							lost: 15,
							gb: 0,
						},
					],
				},
				{
					name: "Western Conference",
					teams: [
						{
							rank: 1,
							abbrev: "SAS",
							region: "San Antonio",
							name: "Spurs",
							won: 46,
							lost: 13,
							gb: 0,
						},
					],
				},
			],
		},
	});

	console.log(
		"\n===== SAMPLE DAY RECAP =====\n" +
			dayRecap +
			"\n============================\n",
	);
	console.log(
		"\n===== SAMPLE GAME RECAPS (rich + playoff) =====\n" +
			out.join("\n\n---\n\n") +
			"\n===============================================\n",
	);
	assert.ok(true);
});
