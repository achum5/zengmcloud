import assert from "node:assert/strict";
import { describe, test } from "vitest";
import {
	beginRecapBatch,
	endRecapBatch,
	getAutoDayRecap,
	getAutoRecap,
	pick,
} from "./getAutoRecap.ts";
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
		// The generic "basket" reads as a real term in the headline, and the body
		// describes the moment concretely - never the tautological "won it with a
		// game-winner".
		assert.ok(/game-winner/.test(recap), recap);
		assert.ok(
			recap.includes(
				"Lindsey Hunter won it with a go-ahead basket with 2 seconds left",
			),
			recap,
		);
		assert.ok(!recap.includes("won it with a game-winner"), recap);
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

	test("a heavy favorite that barely escapes reads as a scare, not a weak stat line", () => {
		// Cavs -11.5 win by 3 with a 16-point "star": the story is the scare.
		const cavs = realisticTeam(
			{
				tid: 40,
				region: "Cleveland",
				name: "Cavaliers",
				abbrev: "CLE",
				pts: 98,
				ptsQtrs: [25, 24, 25, 24],
			},
			player({ name: "Paul Pierce", pts: 16, reb: 8, ast: 6 }),
		);
		const grizzlies = realisticTeam(
			{
				tid: 41,
				region: "Memphis",
				name: "Grizzlies",
				abbrev: "MEM",
				pts: 95,
				ptsQtrs: [24, 24, 24, 23],
			},
			player({ name: "Keith Van Horn", pts: 21, reb: 11, stl: 4 }),
		);
		for (const gid of [9050, 9051, 9052]) {
			const recap = getAutoRecap(
				game({
					gid,
					teams: [cavs, grizzlies],
					winnerTid: 40,
					spread: { favTid: 40, points: 11.5 },
				}),
			);
			const head = recap.split("\n")[0]!;
			assert.ok(/survive a scare|escape|hold off a feisty/.test(head), head);
		}
	});

	test("a 20-20 game is never flattened to 'scores 24'", () => {
		const bulls = realisticTeam(
			{
				tid: 42,
				region: "Chicago",
				name: "Bulls",
				abbrev: "CHI",
				pts: 98,
				ptsQtrs: [25, 25, 24, 24],
			},
			player({ name: "Tyson Chandler", pts: 24, reb: 22, stl: 4 }),
		);
		const nuggets = realisticTeam(
			{
				tid: 43,
				region: "Denver",
				name: "Nuggets",
				abbrev: "DEN",
				pts: 84,
				ptsQtrs: [21, 21, 21, 21],
			},
			player({ name: "Tracy McGrady", pts: 13, reb: 8 }),
		);
		for (const gid of [9060, 9061, 9062]) {
			const recap = getAutoRecap(
				game({ gid, teams: [bulls, nuggets], winnerTid: 42 }),
			);
			const head = recap.split("\n")[0]!;
			assert.ok(head.includes("22"), head);
			assert.ok(!/scores 24 as/.test(head), head);
		}
	});

	test("when the clutch shooter is the lead star, the name isn't repeated back-to-back", () => {
		const magic = realisticTeam(
			{
				tid: 44,
				region: "Orlando",
				name: "Magic",
				abbrev: "ORL",
				pts: 100,
				ptsQtrs: [25, 25, 24, 26],
			},
			player({ name: "Richard Hamilton", pts: 20, reb: 6, stl: 3 }),
		);
		const nets = realisticTeam(
			{
				tid: 45,
				region: "New Jersey",
				name: "Nets",
				abbrev: "NJN",
				pts: 99,
				ptsQtrs: [25, 25, 25, 24],
			},
			player({ name: "Tim Duncan", pts: 28, reb: 11 }),
		);
		const recap = getAutoRecap(
			game({
				gid: 9070,
				teams: [magic, nets],
				winnerTid: 44,
				clutchPlays: [
					'<a href="#">Richard Hamilton</a> made a game-winning free throw with 0.5 seconds remaining.',
				],
			}),
		);
		// The winning shot merges into the lead sentence...
		assert.ok(/, winning it with a free throw/.test(recap), recap);
		// ...instead of a second sentence restarting with the same name.
		assert.ok(!/\. Richard Hamilton won it/.test(recap), recap);
	});

	test("a clutch hero with a big night gets his line folded into the shot", () => {
		const hornets = realisticTeam(
			{
				tid: 60,
				region: "New Orleans",
				name: "Hornets",
				abbrev: "NOL",
				pts: 120,
				ptsQtrs: [30, 30, 30, 30],
			},
			player({ name: "Derrick Rose", pts: 22, ast: 13, fg: 9, fga: 16 }),
		);
		// The hero is NOT the lead star, and scored plenty himself.
		hornets.players.push(
			player({ name: "Viktor Khryapa", pts: 23, reb: 5, fg: 9, fga: 14 }),
		);
		const magic = realisticTeam(
			{
				tid: 61,
				region: "Orlando",
				name: "Magic",
				abbrev: "ORL",
				pts: 117,
				ptsQtrs: [30, 29, 29, 29],
			},
			player({ name: "O.J. Mayo", pts: 30, fg: 11, fga: 22 }),
		);
		const recap = getAutoRecap(
			game({
				gid: 9090,
				teams: [hornets, magic],
				winnerTid: 60,
				clutchPlays: [
					'<a href="#">Viktor Khryapa</a> made a game-winning three-point play with 0.7 seconds remaining.',
				],
			}),
		);
		// His total rides with the winning shot...
		assert.ok(
			/Viktor Khryapa won it with .* and finished with 23 points/.test(recap),
			recap,
		);
		// ...so the supporting cast must not introduce him a second time. His
		// name appears in the headline and the clutch sentence, nowhere else.
		const mentions = recap.match(/Viktor Khryapa/g) ?? [];
		assert.ok(mentions.length <= 2, recap);
	});

	test("a clutch hero with a modest total keeps the shot sentence clean", () => {
		const pacers = realisticTeam(
			{
				tid: 62,
				region: "Indiana",
				name: "Pacers",
				abbrev: "IND",
				pts: 97,
				ptsQtrs: [24, 24, 24, 25],
			},
			player({ name: "Zoran Planinic", pts: 27, ast: 6, fg: 12, fga: 15 }),
		);
		pacers.players.push(player({ name: "Matt Bonner", pts: 6, reb: 3 }));
		const clippers = realisticTeam(
			{
				tid: 63,
				region: "Los Angeles",
				name: "Clippers",
				abbrev: "LAC",
				pts: 95,
				ptsQtrs: [24, 24, 24, 23],
			},
			player({ name: "Jarvis Hayes", pts: 20, fg: 8, fga: 16 }),
		);
		const recap = getAutoRecap(
			game({
				gid: 9091,
				teams: [pacers, clippers],
				winnerTid: 62,
				clutchPlays: [
					'<a href="#">Matt Bonner</a> made a game-winning free throw with 0.5 seconds remaining.',
				],
			}),
		);
		// Six points is an anticlimax stapled to the biggest moment of the game.
		assert.ok(!/won it with .* and finished with/.test(recap), recap);
	});

	test("injury text is prose-cased with acronyms kept, and says 'games'", () => {
		const spurs = realisticTeam(
			{
				tid: 46,
				region: "San Antonio",
				name: "Spurs",
				abbrev: "SAS",
				pts: 110,
				ptsQtrs: [28, 28, 27, 27],
			},
			player({ name: "Shaquille O'Neal", pts: 28, reb: 14 }),
		);
		const hurtRockets = realisticTeam(
			{
				tid: 47,
				region: "Houston",
				name: "Rockets",
				abbrev: "HOU",
				pts: 90,
				ptsQtrs: [23, 23, 22, 22],
			},
			player({
				name: "Manu Ginobili",
				pts: 18,
				injury: { type: "Torn ACL", gamesRemaining: 40, newThisGame: true },
			}),
		);
		const recap = getAutoRecap(
			game({ gid: 9080, teams: [spurs, hurtRockets], winnerTid: 46 }),
		);
		assert.ok(recap.includes("a torn ACL"), recap);
		// The duration is always stated, but the phrasing rotates so a slate full
		// of injuries doesn't read from one template.
		assert.ok(
			/out ~40 games|out around 40 games|about 40 games/.test(recap),
			recap,
		);
		assert.ok(!recap.includes("Torn ACL"), recap);
	});

	test("a weak-star blowout leads with the team and headlines the result", () => {
		// Balanced 31-point blowout where 13/9 genuinely is the top line.
		const clippers = team({
			tid: 48,
			region: "Los Angeles",
			name: "Clippers",
			abbrev: "LAC",
			pts: 105,
			ptsQtrs: [27, 26, 26, 26],
			players: [
				player({ name: "Brad Miller", pts: 13, reb: 9, fg: 5, fga: 9 }),
				player({ name: "Michael Jordan", pts: 12, reb: 4, fg: 5, fga: 11 }),
				player({ name: "Eddie Jones", pts: 11, reb: 3, fg: 4, fga: 9 }),
				player({ name: "Matt Maloney", pts: 10, ast: 5, fg: 4, fga: 8 }),
				player({ name: "Bench Clip", pts: 8, reb: 4, fg: 3, fga: 6 }),
			],
		});
		const bucks = realisticTeam(
			{
				tid: 49,
				region: "Milwaukee",
				name: "Bucks",
				abbrev: "MIL",
				pts: 74,
				ptsQtrs: [19, 19, 18, 18],
			},
			player({ name: "Peja Stojakovic", pts: 16, ast: 5 }),
		);
		for (const gid of [9090, 9091, 9092]) {
			const recap = getAutoRecap(
				game({ gid, teams: [clippers, bucks], winnerTid: 48 }),
			);
			const head = recap.split("\n")[0]!;
			const body = recap.split("\n\n")[1]!;
			// Headline is the result, not "Brad Miller's 13 leads..."
			assert.ok(!/Brad Miller'?s? 13/.test(head), head);
			// The lead is a team sentence with the top line attached.
			assert.ok(/^The Clippers /.test(body), body);
		}
	});

	test("a losing record is never framed as an improvement", () => {
		const warriors = realisticTeam(
			{
				tid: 50,
				region: "Golden State",
				name: "Warriors",
				abbrev: "GSW",
				pts: 93,
				ptsQtrs: [24, 23, 23, 23],
				record: { won: 2, lost: 8 },
			},
			player({ name: "Corliss Williamson", pts: 20, reb: 10 }),
		);
		const pacers = realisticTeam(
			{
				tid: 51,
				region: "Indiana",
				name: "Pacers",
				abbrev: "IND",
				pts: 85,
				ptsQtrs: [22, 21, 21, 21],
			},
			player({ name: "Jason Terry", pts: 21, ast: 9 }),
		);
		for (const gid of [9100, 9101, 9102, 9103, 9104, 9105, 9106, 9107]) {
			const recap = getAutoRecap(
				game({ gid, teams: [warriors, pacers], winnerTid: 50 }),
			);
			assert.ok(!/improved to 2-8|moved to 2-8|to 2-8\./.test(recap), recap);
		}
	});

	test("a sub-15-point star never headlines a close game", () => {
		const blazers = realisticTeam(
			{
				tid: 52,
				region: "Portland",
				name: "Trail Blazers",
				abbrev: "POR",
				pts: 97,
				ptsQtrs: [24, 24, 25, 24],
			},
			player({ name: "Chris Webber", pts: 13, reb: 9, blk: 2, fg: 6, fga: 13 }),
		);
		const wizards = realisticTeam(
			{
				tid: 53,
				region: "Washington",
				name: "Wizards",
				abbrev: "WAS",
				pts: 94,
				ptsQtrs: [25, 24, 23, 22],
			},
			player({ name: "Dirk Nowitzki", pts: 26, reb: 15, blk: 3 }),
		);
		for (const gid of [9110, 9111, 9112]) {
			const recap = getAutoRecap(
				game({ gid, teams: [blazers, wizards], winnerTid: 52 }),
			);
			const head = recap.split("\n")[0]!;
			assert.ok(!/Webber'?s? 13/.test(head), head);
		}
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
		// Weaves in the standings picture. Nicknames, matching every other team
		// reference in the piece.
		assert.ok(/\bPistons\b|\bSpurs\b/.test(recap), recap);
		// A couple of paragraphs of coverage.
		assert.ok(recap.length > 200, recap);
	});

	test("headline carries a deck of MULTIPLE secondary storylines", () => {
		const deckSlate: RecapGame[] = [
			// Marquee: a walk-off that's also an upset.
			mkGame(
				6001,
				"Bobcats",
				"Lakers",
				101,
				100,
				true,
				player({ name: "Gerald Wallace", pts: 24, reb: 10 }),
				player({ name: "Kobe Bryant", pts: 38 }),
				{
					clutchPlays: [
						'<a href="#">Gerald Wallace</a> made a game-winning layup with 1 seconds remaining.',
					],
					spread: { favTid: 6001 * 2 + 1, points: 9 },
				},
			),
			// A blowout.
			mkGame(
				6002,
				"Spurs",
				"Grizzlies",
				120,
				84,
				true,
				player({ name: "Tim Duncan", pts: 22, reb: 13 }),
				player({ name: "Pau Gasol", pts: 18 }),
				{ spread: { favTid: 6002 * 2, points: 10 } },
			),
			// A 46-point eruption.
			mkGame(
				6003,
				"Wizards",
				"Hawks",
				118,
				110,
				true,
				player({ name: "Gilbert Arenas", pts: 46, ast: 8 }),
				player({ name: "Joe Johnson", pts: 25 }),
			),
		];
		const recap = getAutoDayRecap({
			season: 2007,
			day: 5,
			playoffs: false,
			games: deckSlate,
		});
		const deckLine = recap.split("\n\n")[1] ?? "";
		// The deck is an italic line with multiple storylines separated by " · ".
		assert.ok(deckLine.startsWith("*") && deckLine.includes(" · "), recap);
		// The secondary stories (the rout and the 46-point night) are surfaced.
		assert.ok(/Spurs|Gilbert Arenas/.test(deckLine), recap);
	});

	test("an 'Around the league' sweep covers every remaining game", () => {
		const sweepSlate: RecapGame[] = [
			// Marquee upset.
			mkGame(
				7001,
				"Nets",
				"Raptors",
				99,
				80,
				true,
				player({ name: "Vince Carter", pts: 30, reb: 8 }),
				player({ name: "Chris Bosh", pts: 20, reb: 10 }),
				{ spread: { favTid: 7001 * 2 + 1, points: 8 } },
			),
			// Four ordinary wins that should still each get a mention.
			mkGame(
				7002,
				"Heat",
				"Bucks",
				101,
				95,
				true,
				player({ name: "Dwyane Wade", pts: 28 }),
				player({ name: "Michael Redd", pts: 26 }),
			),
			mkGame(
				7003,
				"Suns",
				"Kings",
				110,
				104,
				true,
				player({ name: "Steve Nash", pts: 19, ast: 15 }),
				player({ name: "Mike Bibby", pts: 22 }),
			),
			mkGame(
				7004,
				"Jazz",
				"Rockets",
				97,
				90,
				true,
				player({ name: "Deron Williams", pts: 21, ast: 10 }),
				player({ name: "Tracy McGrady", pts: 25 }),
			),
			mkGame(
				7005,
				"Magic",
				"Pacers",
				100,
				93,
				true,
				player({ name: "Dwight Howard", pts: 20, reb: 16 }),
				player({ name: "Danny Granger", pts: 24 }),
			),
		];
		const recap = getAutoDayRecap({
			season: 2008,
			day: 6,
			playoffs: false,
			games: sweepSlate,
		});
		// The opener rotates now, so pin the behaviour and not one phrasing.
		assert.ok(
			["Around the league", "Also on the night", "In the other games"].some(
				(o) => recap.includes(o),
			),
			recap,
		);
		// Every winner is named somewhere in the recap - you can feel caught up.
		for (const w of ["Nets", "Heat", "Suns", "Jazz", "Magic"]) {
			assert.ok(recap.includes(w), `${w} missing:\n${recap}`);
		}
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

	test("an unbeaten leader reads 'still perfect', and a monster line in a loss is covered", () => {
		const kg = player({
			name: "Kevin Garnett",
			pts: 23,
			reb: 18,
			ast: 8,
			fg: 9,
			fga: 18,
		});
		// The Heat game is the clear marquee (35-point night), so KG's line in a
		// loss comes from the OTHER game and must still make the wrap.
		const games = [
			mkGame(
				5030,
				"Heat",
				"Pacers",
				94,
				83,
				true,
				player({ name: "Michael Finley", pts: 35, reb: 8, fg: 13, fga: 22 }),
				player({ name: "Role Pacer", pts: 12 }),
			),
			mkGame(
				5031,
				"Bucks",
				"Hornets",
				101,
				90,
				true,
				player({ name: "Peja Stojakovic", pts: 26, tp: 5 }),
				kg,
			),
		];
		const recap = getAutoDayRecap({
			season: 2003,
			day: 6,
			playoffs: false,
			games,
			standings: {
				day: 6,
				confs: [
					{
						name: "Eastern Conference",
						teams: [
							{
								rank: 1,
								abbrev: "MIA",
								region: "Miami",
								name: "Heat",
								won: 6,
								lost: 0,
								gb: 0,
							},
							{
								rank: 2,
								abbrev: "ORL",
								region: "Orlando",
								name: "Magic",
								won: 5,
								lost: 1,
								gb: 1,
							},
						],
					},
				],
			},
		});
		assert.ok(/still perfect at 6-0/.test(recap), recap);
		assert.ok(!/hold a narrow lead/.test(recap), recap);
		// KG's 23-18-8 in a loss makes the wrap.
		assert.ok(/Kevin Garnett'?s.*losing effort/.test(recap), recap);
	});

	test("a big scorer on a losing team is never phrased as a win contribution", () => {
		const games = [
			mkGame(
				5040,
				"Knicks",
				"Mavericks",
				98,
				97,
				true,
				player({ name: "Quentin Richardson", pts: 20, reb: 7 }),
				player({ name: "Ray Allen", pts: 22 }),
				{
					clutchPlays: [
						'<a href="#">Stephon Marbury</a> made a game-winning basket with 3.6 seconds remaining.',
					],
				},
			),
			// The only 25+ line outside the marquee game comes in a LOSS.
			mkGame(
				5041,
				"Hawks",
				"Timberwolves",
				98,
				90,
				true,
				player({ name: "David Wesley", pts: 16 }),
				player({ name: "Kenny Satterfield", pts: 31, ast: 7, reb: 10 }),
			),
		];
		const recap = getAutoDayRecap({
			season: 2003,
			day: 11,
			playoffs: false,
			games,
		});
		assert.ok(
			!/Kenny Satterfield added .* for the Timberwolves/.test(recap),
			recap,
		);
		// The line still makes the wrap, framed honestly ("despite the
		// Timberwolves' loss" / "in a losing effort").
		assert.ok(/Kenny Satterfield.*(losing effort|loss)/.test(recap), recap);
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
		// The frame rotates day to day; any of them counts as covering it.
		assert.ok(
			/On the injury front|took its toll|casualty list|hurt along the way/.test(
				recap,
			),
			recap,
		);
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
		// "Heat" doesn't end in s, so the possessive is "Heat's" - the bug this
		// guards produced "the Heat' fifth in a row". Which streak phrasing comes
		// out rotates (the pool is shared across every recap in a run), so assert
		// the streak is reported at all, and that no phrasing anywhere in the
		// recap renders the possessive as a bare apostrophe.
		const recap = getAutoRecap(heatGame);
		assert.ok(/in a row|straight|last \d/.test(recap), recap);
		assert.ok(!/Heat'(?!s)/.test(recap), recap);
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

describe("phrasing variety across a night", () => {
	// Fourteen independent games reliably produced "the Bucks routed the Suns,
	// the Hornets routed the Pacers, the Nets routed the Heat" and five straight
	// "got past"es, because every game picked its phrasing independently.
	test("a batch rotates through a pool instead of repeating", () => {
		const pool = ["routed", "blew out", "ran away from", "cruised past"];
		const rng = () => 0; // Always picks the first available option.

		beginRecapBatch();
		const picks = [pick(rng, pool), pick(rng, pool), pick(rng, pool)];
		endRecapBatch();

		assert.strictEqual(new Set(picks).size, 3, picks.join(", "));
	});

	test("an exhausted pool starts over rather than running dry", () => {
		const pool = ["edged", "held off"];
		const rng = () => 0;

		beginRecapBatch();
		const picks = [pick(rng, pool), pick(rng, pool), pick(rng, pool)];
		endRecapBatch();

		assert.strictEqual(picks[2], picks[0]);
	});

	test("the memory doesn't leak between batches", () => {
		// Each night starts fresh, so day 2 isn't shaped by what day 1 happened to
		// use. (A single recap generated on its own is covered by the determinism
		// test above, which resets on entry to getAutoRecap.)
		const pool = ["routed", "blew out", "ran away from"];
		const rng = () => 0;

		beginRecapBatch();
		const first = pick(rng, pool);
		endRecapBatch();

		beginRecapBatch();
		const second = pick(rng, pool);
		endRecapBatch();

		assert.strictEqual(first, second);
	});
});

// Everything below is a defect found by reading a real day's page of recaps
// stacked on top of each other, which is how they're actually consumed. Each
// one read fine in isolation and badly in a column.
describe("a page of recaps doesn't repeat itself", () => {
	const twoSided = (
		winnerStar: RecapPlayer,
		loserStar: RecapPlayer,
		opts: {
			wPts?: number;
			lPts?: number;
			wq?: number[];
			lq?: number[];
			wExtra?: RecapPlayer[];
			lExtra?: RecapPlayer[];
		} = {},
	) => {
		const wPts = opts.wPts ?? 115;
		const lPts = opts.lPts ?? 105;
		const home = realisticTeam(
			{
				tid: 1,
				region: "Boston",
				name: "Celtics",
				abbrev: "BOS",
				pts: wPts,
				ptsQtrs: opts.wq ?? [30, 28, 29, 28],
			},
			winnerStar,
		);
		if (opts.wExtra) {
			home.players.splice(1, 0, ...opts.wExtra);
		}
		const away = realisticTeam(
			{
				tid: 2,
				region: "Memphis",
				name: "Grizzlies",
				abbrev: "MEM",
				pts: lPts,
				ptsQtrs: opts.lq ?? [25, 26, 27, 27],
			},
			loserStar,
		);
		if (opts.lExtra) {
			away.players.splice(1, 0, ...opts.lExtra);
		}
		return game({ teams: [home, away], winnerTid: 1 });
	};

	const parts = (recap: string) => {
		const [headline, ...rest] = recap.split("\n\n");
		return { headline: headline!, body: rest.join("\n\n") };
	};

	// The defect: "Bagaric goes for 26 points as the Celtics beat the Grizzlies"
	// followed immediately by "Bagaric scored 26 points as the Celtics topped the
	// Grizzlies 115-105." The same sentence with the verbs swapped.
	test("when the headline spends the star, the body opens on the result", () => {
		const recap = getAutoRecap(
			twoSided(
				player({ name: "Dalibor Bagaric", pts: 26, reb: 7, fg: 11, fga: 17 }),
				player({ name: "Ruben Patterson", pts: 22, reb: 7, fg: 9, fga: 19 }),
			),
		);
		const { headline, body } = parts(recap);
		assert.ok(headline.includes("Bagaric"), recap);
		const firstSentence = body.split(". ")[0]!;
		assert.ok(
			!firstSentence.includes("Bagaric"),
			`body restates the headline: ${recap}`,
		);
		// But he still gets his line, somewhere.
		assert.ok(body.includes("Bagaric"), recap);
	});

	test("a result headline still gets a star-led body", () => {
		// Nobody scored enough to headline, so the headline is the result - and
		// then the body SHOULD lead with a player.
		const recap = getAutoRecap(
			twoSided(
				player({ name: "Quiet Star", pts: 13, reb: 5, fg: 5, fga: 12 }),
				player({ name: "Other Guy", pts: 12, reb: 4, fg: 5, fga: 13 }),
				{ wPts: 120, lPts: 92, wq: [32, 30, 29, 29], lq: [22, 24, 23, 23] },
			),
		);
		const { headline, body } = parts(recap);
		// No individual line big enough to headline, so the headline is the result
		// and the body is free to open on a player.
		assert.ok(!/\d+ points/.test(headline), recap);
		assert.ok(!body.startsWith("The Celtics"), recap);
	});

	// "Zach Randolph's 24 points and 11 rebounds LEADS the Cavaliers past..."
	test("a plural stat phrase takes a plural verb", () => {
		for (const pts of [20, 24, 26, 31]) {
			const recap = getAutoRecap(
				twoSided(
					player({ name: "Zach Randolph", pts, reb: 11, fg: 10, fga: 18 }),
					player({ name: "Chris Bosh", pts: 18, reb: 5, fg: 7, fga: 20 }),
				),
			);
			assert.ok(
				!/rebounds (leads|powers)\b/.test(recap),
				`subject-verb disagreement: ${recap}`,
			);
		}
	});

	// "Elton Brand and Mike Miller lead the Raptors past the Suns" - and then the
	// recap never mentioned Mike Miller again.
	test("every player named in the headline appears in the body", () => {
		const recap = getAutoRecap(
			twoSided(
				player({
					name: "Marko Jaric",
					pts: 23,
					reb: 4,
					ast: 8,
					fg: 9,
					fga: 18,
				}),
				player({
					name: "LeBron James",
					pts: 22,
					reb: 5,
					ast: 10,
					fg: 8,
					fga: 22,
				}),
				{
					wPts: 89,
					lPts: 80,
					wq: [22, 21, 24, 22],
					lq: [20, 19, 20, 21],
					wExtra: [
						player({ name: "Elton Brand", pts: 14, reb: 11, fg: 6, fga: 12 }),
						player({ name: "Mike Miller", pts: 12, reb: 10, fg: 5, fga: 12 }),
					],
				},
			),
		);
		const { headline, body } = parts(recap);
		for (const name of ["Marko Jaric", "Elton Brand", "Mike Miller"]) {
			if (headline.includes(name)) {
				assert.ok(body.includes(name), `${name} headlined but never appears`);
			}
		}
	});

	// "The Nets turned 19 Magic turnovers into offense. ... The Magic were undone
	// by 19 turnovers."
	test("a turnover count is spent once, not twice", () => {
		const recap = getAutoRecap(
			twoSided(
				player({
					name: "Damon Jones",
					pts: 17,
					reb: 3,
					ast: 8,
					stl: 4,
					fg: 6,
					fga: 13,
				}),
				player({ name: "Leon Smith", pts: 9, reb: 5, tov: 5, fg: 4, fga: 11 }),
				{ wPts: 101, lPts: 78, wq: [26, 25, 30, 20], lq: [22, 20, 20, 16] },
			),
		);
		const turnoverMentions = recap.match(/turnovers|coughed it up/g) ?? [];
		assert.ok(
			turnoverMentions.length <= 1,
			`turnovers mentioned ${turnoverMentions.length} times: ${recap}`,
		);
	});

	// "Hornets get past the Clippers, 123-108" - a 15-point win.
	test("the verb matches the scoreboard", () => {
		const recap = getAutoRecap(
			twoSided(
				player({
					name: "Khalid El-Amin",
					pts: 18,
					reb: 3,
					ast: 8,
					fg: 7,
					fga: 14,
				}),
				player({
					name: "Brad Miller",
					pts: 21,
					reb: 4,
					ast: 3,
					fg: 9,
					fga: 18,
				}),
				{ wPts: 123, lPts: 108, wq: [30, 26, 34, 33], lq: [28, 16, 32, 32] },
			),
		);
		for (const weak of [
			"get past",
			"got past",
			"slip past",
			"slipped past",
			"survive",
			"escape",
		]) {
			assert.ok(
				!recap.includes(weak),
				`"${weak}" for a 15-point win: ${recap}`,
			);
		}
	});

	test("a three-point win never reads as a rout", () => {
		const recap = getAutoRecap(
			twoSided(
				player({ name: "Close Winner", pts: 24, reb: 6, fg: 9, fga: 18 }),
				player({ name: "Close Loser", pts: 22, reb: 5, fg: 9, fga: 19 }),
				{ wPts: 98, lPts: 95, wq: [24, 25, 24, 25], lq: [25, 23, 24, 23] },
			),
		);
		for (const strong of [
			"rout",
			"blew out",
			"ran away",
			"cruise",
			"rolled past",
		]) {
			assert.ok(
				!recap.includes(strong),
				`"${strong}" for a 3-point win: ${recap}`,
			);
		}
	});

	// The best player on the floor lost, and the recap crowned the winner's
	// 15-point leading scorer instead.
	test("a losing star who outplayed everyone gets the headline, once", () => {
		const recap = getAutoRecap(
			twoSided(
				player({
					name: "Tracy McGrady",
					pts: 15,
					reb: 5,
					ast: 4,
					fg: 6,
					fga: 17,
				}),
				player({
					name: "Antoine Walker",
					pts: 27,
					reb: 10,
					blk: 3,
					fg: 11,
					fga: 26,
				}),
				{ wPts: 81, lPts: 72, wq: [18, 17, 24, 22], lq: [20, 18, 17, 17] },
			),
		);
		const { headline, body } = parts(recap);
		assert.ok(headline.includes("Walker"), recap);
		// He IS in the body - a headline about a man the story never mentions is
		// the giveaway this whole engine exists to avoid. What must not happen is
		// his LINE being printed twice, so the body covers him with something the
		// headline didn't say (how he shot).
		assert.ok(
			body.includes("Walker"),
			`Walker headlined but dropped: ${recap}`,
		);
		assert.ok(
			!body.includes("27 points"),
			`Walker's line stated twice: ${recap}`,
		);
	});

	// A 19-rebound night was being flattened into "(19 points and 19 rebounds)".
	test("a monster rebounding night is called out, not listed", () => {
		const recap = getAutoRecap(
			twoSided(
				player({ name: "Rashard Lewis", pts: 21, reb: 12, fg: 8, fga: 17 }),
				player({ name: "Robert Traylor", pts: 19, reb: 19, fg: 8, fga: 15 }),
				{ wPts: 108, lPts: 102, wq: [24, 26, 25, 22], lq: [26, 24, 25, 22] },
			),
		);
		assert.ok(recap.includes("19 rebounds"), recap);
		assert.ok(
			/glass|everywhere/.test(recap),
			`a 19-rebound night read as routine: ${recap}`,
		);
	});
});

describe("a day wrap reads as one night, not one game", () => {
	const oneGame = (
		tids: [number, number],
		names: [string, string],
		pts: [number, number],
		star: RecapPlayer,
		gid: number,
	) =>
		game({
			gid,
			teams: [
				realisticTeam(
					{
						tid: tids[0],
						name: names[0],
						abbrev: names[0].slice(0, 3).toUpperCase(),
						pts: pts[0],
						ptsQtrs: [pts[0] / 4, pts[0] / 4, pts[0] / 4, pts[0] / 4],
					},
					star,
				),
				realisticTeam(
					{
						tid: tids[1],
						name: names[1],
						abbrev: names[1].slice(0, 3).toUpperCase(),
						pts: pts[1],
						ptsQtrs: [pts[1] / 4, pts[1] / 4, pts[1] / 4, pts[1] / 4],
					},
					player({ name: `${names[1]} Guy`, pts: 14, reb: 5, fg: 5, fga: 12 }),
				),
			],
			winnerTid: tids[0],
		});

	// "Dirk Nowitzki ADDED 29 points and 10 rebounds for the Wizards" - tacked
	// onto a sentence about a completely different game, which read as though
	// he'd been on the floor for it.
	test("a performance from another game names its opponent", () => {
		const games = [
			oneGame(
				[1, 2],
				["Hawks", "Mavericks"],
				[108, 102],
				player({ name: "Rashard Lewis", pts: 21, reb: 12, fg: 8, fga: 17 }),
				1,
			),
			oneGame(
				[3, 4],
				["Wizards", "Warriors"],
				[105, 84],
				player({ name: "Dirk Nowitzki", pts: 29, reb: 10, fg: 11, fga: 20 }),
				2,
			),
		];
		const recap = getAutoDayRecap({
			season: 2004,
			day: 70,
			playoffs: false,
			games,
		});
		if (recap.includes("Nowitzki")) {
			const sentence = recap
				.split(". ")
				.find((line) => line.includes("Nowitzki"))!;
			assert.ok(
				!/^Dirk Nowitzki added/.test(sentence),
				`reads as the same game: ${sentence}`,
			);
			assert.ok(
				sentence.includes("Warriors"),
				`no opponent, so which game was it? ${sentence}`,
			);
		}
	});
});

// The third paragraph: the detail beyond the result. These angles exist because
// the box score carries far more than a lead scorer and a final score, and none
// of it was being read.
describe("the extra colour paragraph", () => {
	const bigNight = (extra: Partial<RecapPlayer> = {}) =>
		player({
			name: "Breakout Guy",
			pts: 34,
			reb: 6,
			ast: 4,
			fg: 13,
			fga: 22,
			tp: 4,
			tpa: 9,
			ft: 4,
			fta: 5,
			min: 39,
			seasonAvg: avg({ gp: 30, pts: 12.4, fgp: 43 }),
			...extra,
		});

	const twoTeamGame = (
		home: RecapTeam,
		away: RecapTeam,
		over: Partial<RecapGame> = {},
	) =>
		game({
			gid: 4242,
			teams: [home, away],
			winnerTid: home.tid,
			...over,
		});

	test("a night far above a player's average says so, and names him", () => {
		const w = realisticTeam(
			{
				tid: 1,
				name: "Suns",
				abbrev: "PHO",
				pts: 112,
				ptsQtrs: [28, 28, 28, 28],
			},
			bigNight(),
		);
		const l = realisticTeam(
			{
				tid: 2,
				name: "Kings",
				abbrev: "SAC",
				pts: 98,
				ptsQtrs: [25, 25, 24, 24],
			},
			player({ name: "Other Guy", pts: 20, reb: 5, fg: 8, fga: 18 }),
		);
		const recap = getAutoRecap(twoTeamGame(w, l));
		assert.ok(/12\.4/.test(recap), `no average context: ${recap}`);
		// The paragraph is detached from where he was introduced, so a bare "He"
		// would have no antecedent.
		const last = recap.split("\n\n").at(-1)!;
		assert.ok(
			!/^(He|That is \d+ clear of his)\b/.test(last) ||
				last.includes("Breakout Guy"),
			`orphan pronoun: ${last}`,
		);
	});

	test("a career-best scoring season is called out", () => {
		const w = realisticTeam(
			{
				tid: 3,
				name: "Bucks",
				abbrev: "MIL",
				pts: 106,
				ptsQtrs: [26, 27, 26, 27],
			},
			bigNight({
				seasonAvg: avg({ gp: 40, pts: 24.5 }),
				career: [
					{ ...avg({ pts: 11.0 }), season: 2001 },
					{ ...avg({ pts: 15.2 }), season: 2002 },
					{ ...avg({ pts: 18.9 }), season: 2003 },
				],
			}),
		);
		const l = realisticTeam(
			{
				tid: 4,
				name: "Hawks",
				abbrev: "ATL",
				pts: 95,
				ptsQtrs: [24, 24, 23, 24],
			},
			player({ name: "Loser Star", pts: 21, reb: 6, fg: 8, fga: 19 }),
		);
		const recap = getAutoRecap(twoTeamGame(w, l));
		assert.ok(/career/i.test(recap), `no career context: ${recap}`);
	});

	test("nobody is named twice across the whole recap", () => {
		const w = realisticTeam(
			{
				tid: 5,
				name: "Jazz",
				abbrev: "UTA",
				pts: 104,
				ptsQtrs: [26, 26, 26, 26],
			},
			bigNight(),
		);
		const l = realisticTeam(
			{
				tid: 6,
				name: "Magic",
				abbrev: "ORL",
				pts: 99,
				ptsQtrs: [25, 25, 25, 24],
			},
			player({
				name: "Foul Trouble",
				pts: 22,
				reb: 11,
				pf: 6,
				min: 33,
				fg: 9,
				fga: 20,
			}),
		);
		const recap = getAutoRecap(twoTeamGame(w, l));
		// The headline names the game's best player and so does the lead sentence;
		// that is normal. What must never happen is the same man's LINE being
		// printed twice, or a role player being introduced twice.
		const body = recap.split("\n\n").slice(1).join(" ");
		assert.ok(
			body.split("Breakout Guy").length - 1 <= 2,
			`star's name overused: ${recap}`,
		);
		assert.ok(
			body.split("Foul Trouble").length - 1 <= 1,
			`role player named twice: ${recap}`,
		);
	});

	// From a real recap: the Celtics lost 106-92, and the last line of the story
	// read "The Celtics are 9-0 over their last 9." The form window deliberately
	// excludes the game being recapped, so every sentence built from it has to be
	// past tense - present tense turns "how they had been playing" into a claim
	// about right now, directly contradicted by the box score above it.
	test("a hot team that just lost is never described as currently unbeaten", () => {
		const hot = Array.from({ length: 9 }, (_, i) => ({
			opp: "ORL",
			home: i % 2 === 0,
			won: true,
			pts: 100,
			oppPts: 90,
		}));
		// Index 0 is this game - the loss being recapped.
		const loserL10 = [
			{ opp: "CHA", home: true, won: false, pts: 92, oppPts: 106 },
			...hot,
		];
		const w = realisticTeam(
			{
				tid: 30,
				name: "Bobcats",
				abbrev: "CHA",
				pts: 106,
				ptsQtrs: [18, 27, 36, 25],
			},
			player({
				name: "Antonis Fotsis",
				pts: 20,
				reb: 12,
				ast: 7,
				fg: 8,
				fga: 15,
			}),
		);
		const l = realisticTeam(
			{
				tid: 1,
				name: "Celtics",
				abbrev: "BOS",
				pts: 92,
				ptsQtrs: [30, 18, 22, 22],
				last10: loserL10,
			},
			player({ name: "Chris Paul", pts: 13, reb: 3, ast: 13, fg: 5, fga: 14 }),
		);
		// The seed picks which of the three phrasings runs, so sweep enough gids
		// to exercise all of them - one seed would test one sentence.
		let toldTheRun = 0;
		for (let gid = 1; gid <= 60; gid++) {
			const recap = getAutoRecap(twoTeamGame(w, l, { gid }));

			assert.ok(
				!/Celtics are \d+-\d+ over/.test(recap),
				`gid ${gid}: a team that just lost is given a present-tense record: ${recap}`,
			);
			assert.ok(
				!/Celtics have now won/.test(recap),
				`gid ${gid}: "have now won" excludes the loss it sits under: ${recap}`,
			);
			assert.ok(
				!/that is \d+ wins in \d+ games for the Celtics(?! coming in)/.test(
					recap,
				),
				`gid ${gid}: present-tense form claim under a loss: ${recap}`,
			);

			if (
				/came in having won|coming in|entered the night|arrived having/.test(
					recap,
				)
			) {
				toldTheRun += 1;
			}
		}
		// Guard against a vacuous pass: if the form note stopped being reached at
		// all, the assertions above would hold for the wrong reason.
		assert.ok(
			toldTheRun > 0,
			"the form note never ran, so this test proved nothing",
		);
	});

	test("the streak sentence and the form note don't contradict each other", () => {
		const l10 = [
			{ opp: "ORL", home: true, won: true, pts: 104, oppPts: 99 },
			{ opp: "MIA", home: false, won: true, pts: 98, oppPts: 90 },
			{ opp: "CHI", home: true, won: true, pts: 101, oppPts: 95 },
			{ opp: "DET", home: true, won: true, pts: 97, oppPts: 88 },
			{ opp: "NYK", home: false, won: true, pts: 105, oppPts: 100 },
			{ opp: "BOS", home: true, won: true, pts: 99, oppPts: 91 },
			{ opp: "TOR", home: false, won: true, pts: 96, oppPts: 92 },
			{ opp: "PHI", home: true, won: true, pts: 110, oppPts: 101 },
		];
		const w = realisticTeam(
			{
				tid: 7,
				name: "Pistons",
				abbrev: "DET",
				pts: 104,
				ptsQtrs: [26, 26, 26, 26],
				streak: { won: true, count: 8 },
				last10: l10,
			},
			bigNight(),
		);
		const wl = realisticTeam(
			{
				tid: 8,
				name: "Wizards",
				abbrev: "WAS",
				pts: 95,
				ptsQtrs: [24, 24, 24, 23],
			},
			player({ name: "Wiz Star", pts: 19, reb: 5, fg: 7, fga: 17 }),
		);
		const recap = getAutoRecap(twoTeamGame(w, wl));
		const streakTold = /in a row|straight game|ran their streak/.test(recap);
		if (streakTold) {
			assert.ok(
				!/Pistons have now won \d+ of their last/.test(recap),
				`streak stated two different ways: ${recap}`,
			);
		}
	});

	test("a quiet game still gets a short recap, not padding", () => {
		// No averages, no career, no injuries, no spread, balanced shooting - the
		// extra paragraph should simply not appear.
		const w = team({
			tid: 9,
			name: "Pacers",
			abbrev: "IND",
			pts: 92,
			ptsQtrs: [23, 23, 23, 23],
			players: [
				player({ name: "Plain One", pts: 18, reb: 5, ast: 3, fg: 7, fga: 15 }),
				player({ name: "Plain Two", pts: 14, reb: 6, ast: 2, fg: 6, fga: 13 }),
			],
		});
		const l = team({
			tid: 10,
			name: "Bobcats",
			abbrev: "CHA",
			pts: 88,
			ptsQtrs: [22, 22, 22, 22],
			players: [
				player({
					name: "Plain Three",
					pts: 17,
					reb: 4,
					ast: 3,
					fg: 7,
					fga: 16,
				}),
				player({ name: "Plain Four", pts: 12, reb: 5, ast: 2, fg: 5, fga: 12 }),
			],
		});
		const recap = getAutoRecap(twoTeamGame(w, l));
		assert.ok(
			recap.split("\n\n").length <= 3,
			`padded a nothing game: ${recap}`,
		);
	});
});

describe("the day wrap reads as prose, not a list", () => {
	const slate = (): RecapGame[] => {
		const games: RecapGame[] = [];
		for (let i = 0; i < 12; i++) {
			const w = realisticTeam(
				{
					tid: 100 + i * 2,
					name: `Alphas${i}`,
					abbrev: `A${i}`,
					pts: 100 + i,
					ptsQtrs: [25, 25, 25, 25 + i],
				},
				player({
					name: `Star A${i}`,
					pts: 20 + (i % 8),
					reb: 6,
					ast: 4,
					fg: 8,
					fga: 16,
				}),
			);
			const l = realisticTeam(
				{
					tid: 101 + i * 2,
					name: `Betas${i}`,
					abbrev: `B${i}`,
					pts: 90 + (i % 7),
					ptsQtrs: [22, 23, 22, 23],
				},
				player({
					name: `Star B${i}`,
					pts: 18 + (i % 6),
					reb: 5,
					ast: 3,
					fg: 7,
					fga: 17,
				}),
			);
			games.push(
				game({ gid: 7000 + i, teams: [w, l], winnerTid: w.tid, day: 30 }),
			);
		}
		return games;
	};

	test("the roundup is broken into sentences, not one giant comma list", () => {
		const recap = getAutoDayRecap({
			season: 2004,
			day: 30,
			playoffs: false,
			games: slate(),
		});
		const longest = recap
			.split("\n\n")
			.flatMap((para) => para.split(". "))
			.reduce((max, s) => Math.max(max, s.split(",").length), 0);
		assert.ok(
			longest <= 6,
			`a sentence with ${longest} comma clauses: ${recap}`,
		);
	});

	test("no roundup opener is used twice in one night", () => {
		const recap = getAutoDayRecap({
			season: 2004,
			day: 31,
			playoffs: false,
			games: slate(),
		});
		for (const opener of [
			"Around the league",
			"Also on the night",
			"In the rest of the schedule",
			"Rounding out the slate",
		]) {
			const hits = recap.split(opener).length - 1;
			assert.ok(hits <= 1, `"${opener}" used ${hits} times: ${recap}`);
		}
	});

	test("a sentence never opens with a bare numeral", () => {
		const recap = getAutoDayRecap({
			season: 2004,
			day: 32,
			playoffs: false,
			games: slate(),
		});
		for (const para of recap.split("\n\n")) {
			assert.ok(
				!/(^|\. )\d/.test(para.replaceAll("*", "")),
				`sentence starts with a digit: ${para}`,
			);
		}
	});
});

// Copy problems found by reading a full day of recaps side by side, which is
// the only way most of these show up: each sentence is defensible alone and
// wrong next to the one before it.
describe("copy that only reads wrong in context", () => {
	const withInjury = (
		name: string,
		type: string,
		extra: Partial<RecapTeam> = {},
	) =>
		realisticTeam(
			{
				tid: 1,
				region: "Indiana",
				name: "Pacers",
				abbrev: "IND",
				pts: 106,
				injuries: [{ name, type, gamesRemaining: 20 }],
				...extra,
			},
			player({
				name: "Kirk Hinrich",
				pts: 28,
				reb: 3,
				ast: 7,
				fg: 11,
				fga: 18,
			}),
		);

	test("an injury named mid-sentence is not capitalized", () => {
		// The injury types come out of the DB capitalized ("Torn achilles
		// tendon"). One phrasing put the injury first and pre-capped it, which was
		// right only when that bit happened to open the sentence - the bits get
		// joined with "; ", so it read "...; a Torn achilles tendon kept...".
		for (let seed = 0; seed < 40; seed++) {
			const recap = getAutoRecap(
				game({
					gid: seed,
					teams: [
						withInjury("Eddie House", "Torn achilles tendon"),
						realisticTeam(
							{
								tid: 2,
								region: "Atlanta",
								name: "Hawks",
								abbrev: "ATL",
								pts: 99,
								injuries: [
									{
										name: "Al Harrington",
										type: "Strained hamstring",
										gamesRemaining: 8,
									},
								],
							},
							player({ name: "Rashard Lewis", pts: 17, reb: 5, ast: 2 }),
						),
					],
					winnerTid: 1,
				}),
			);
			assert.ok(
				!/[,a-z] (?:A|An) [A-Z][a-z]+ (?:achilles|hamstring)/.test(recap),
				recap,
			);
			assert.ok(!/a Torn|a Strained/.test(recap), recap);
		}
	});

	test("a scoring average is never left as a bare number", () => {
		// "Kirk Hinrich came into the night averaging 16.4." - 16.4 what?
		for (let seed = 0; seed < 40; seed++) {
			const recap = getAutoRecap(
				game({
					gid: seed,
					teams: [
						realisticTeam(
							{ tid: 1, name: "Pacers", abbrev: "IND", pts: 112 },
							player({
								name: "Kirk Hinrich",
								pts: 34,
								reb: 4,
								ast: 6,
								fg: 13,
								fga: 20,
								seasonAvg: avg({ pts: 16.4, reb: 3, ast: 5, gp: 50 }),
							}),
						),
						realisticTeam(
							{ tid: 2, name: "Hawks", abbrev: "ATL", pts: 99 },
							player({ name: "Rashard Lewis", pts: 17, reb: 5 }),
						),
					],
					winnerTid: 1,
				}),
			);
			// The number must be followed by what it counts, not by a full stop.
			// (?!\d) so the decimal point in "16.4" isn't read as the full stop.
			assert.ok(!/averaging \d+(?:\.\d+)?\.(?!\d)/.test(recap), recap);
			assert.ok(!/averaging \d+(?:\.\d+)? a game/.test(recap), recap);
		}
	});

	test("a blowout win under the number is not a scare", () => {
		// A 22.5-point favorite winning by 10 covered nothing, but it also never
		// "had to sweat this one out" - it was a comfortable win and a bad beat.
		for (let seed = 0; seed < 40; seed++) {
			const recap = getAutoRecap(
				game({
					gid: seed,
					teams: [
						realisticTeam(
							{ tid: 1, name: "Cavaliers", abbrev: "CLE", pts: 114 },
							player({ name: "DerMarr Johnson", pts: 22, reb: 5, stl: 2 }),
						),
						realisticTeam(
							{ tid: 2, name: "Magic", abbrev: "ORL", pts: 104 },
							player({ name: "Richard Hamilton", pts: 18, reb: 5, ast: 5 }),
						),
					],
					winnerTid: 1,
					spread: { favTid: 1, points: 22.5 },
				}),
			);
			assert.ok(!/sweat this one out/.test(recap), recap);
		}
	});

	test("the loser's supporting man never outscores the man called their leader", () => {
		// The losing side's featured player is chosen on his whole line, so the
		// next name up can have more points. "Odom's 22 led the Grizzlies... Van
		// Horn added 23 in defeat" is a contradiction in consecutive sentences.
		for (let seed = 0; seed < 60; seed++) {
			const recap = getAutoRecap(
				game({
					gid: seed,
					teams: [
						realisticTeam(
							{ tid: 1, name: "Warriors", abbrev: "GSW", pts: 112 },
							player({ name: "Gary Payton", pts: 25, reb: 4, ast: 14 }),
						),
						team({
							tid: 2,
							name: "Grizzlies",
							abbrev: "MEM",
							pts: 99,
							players: [
								// Leads on the all-round line, but not on points.
								player({ name: "Lamar Odom", pts: 22, reb: 10, stl: 2 }),
								player({ name: "Keith Van Horn", pts: 23, reb: 3 }),
								player({ name: "Bench One", pts: 10, reb: 4 }),
								player({ name: "Bench Two", pts: 8, reb: 3 }),
							],
						}),
					],
					winnerTid: 1,
				}),
			);
			if (
				/led the Grizzlies|paced the Grizzlies|fronted the Grizzlies|headed the Grizzlies|topped the Grizzlies/.test(
					recap,
				)
			) {
				assert.ok(!/Van Horn/.test(recap), recap);
			}
		}
	});

	test("a team total is not given twice in the same recap", () => {
		// Paragraph 2 can say "piled up 29 assists" and paragraph 3 could then say
		// "assisted on far more of their baskets, 29 to 18" - the same 29, two
		// sentences apart. Same for the double-figures count.
		for (let seed = 0; seed < 60; seed++) {
			const recap = getAutoRecap(
				game({
					gid: seed,
					teams: [
						team({
							tid: 1,
							name: "Timberwolves",
							abbrev: "MIN",
							pts: 102,
							players: [
								player({ name: "Marquis Daniels", pts: 16, reb: 4, ast: 6 }),
								player({ name: "Chris Andersen", pts: 17, reb: 6, ast: 5 }),
								player({ name: "Andre Iguodala", pts: 18, reb: 10, ast: 6 }),
								player({ name: "Wally World", pts: 14, reb: 3, ast: 5 }),
								player({ name: "Fred Hoiberg", pts: 13, reb: 2, ast: 4 }),
								player({ name: "Trenton Hassell", pts: 12, reb: 5, ast: 3 }),
								player({ name: "Mark Madsen", pts: 12, reb: 4, ast: 2 }),
							],
						}),
						team({
							tid: 2,
							name: "Bucks",
							abbrev: "MIL",
							pts: 78,
							players: [
								player({ name: "Mike Wilks", pts: 14, reb: 2, ast: 3 }),
								player({ name: "Desmond Mason", pts: 12, reb: 4, ast: 2 }),
								player({ name: "Joe Smith", pts: 9, reb: 6, ast: 1 }),
								player({ name: "Dan Gadzuric", pts: 8, reb: 5, ast: 1 }),
							],
						}),
					],
					winnerTid: 1,
				}),
			);
			// Only TEAM-total assist sentences count - a player's own line
			// legitimately says "6 assists" alongside them. 31 is the winner's
			// total for this fixture.
			const assistSentences = recap
				.split(/(?<=\.)\s+/)
				.filter(
					(sentence) =>
						sentence.includes("31 assist") ||
						/assisted on far more|out-assisted|moved the ball far better|found the open man/.test(
							sentence,
						),
				);
			assert.ok(assistSentences.length <= 1, recap);
			const dblFig = recap
				.split(/(?<=\.)\s+/)
				.filter((sentence) => /double figures/.test(sentence));
			assert.ok(dblFig.length <= 1, recap);
		}
	});

	test("a game-winner headline is not followed by someone else's scoring line", () => {
		// The headline is always the winning shot when there is one, so the body
		// has to reach it immediately. Opening on the leading scorer made the
		// headline look like it belonged to a different game.
		for (let seed = 0; seed < 30; seed++) {
			const recap = getAutoRecap(
				game({
					gid: seed,
					teams: [
						realisticTeam(
							{ tid: 1, name: "Mavericks", abbrev: "DAL", pts: 100 },
							player({ name: "Ray Allen", pts: 18, reb: 3, ast: 4 }),
						),
						realisticTeam(
							{ tid: 2, name: "Knicks", abbrev: "NYK", pts: 99 },
							player({ name: "Dion Glover", pts: 27, reb: 3, ast: 8 }),
						),
					],
					winnerTid: 1,
					clutchPlays: [
						'<a href="#">Metta World Peace</a> made a game-winning three point play with 0.8 seconds remaining.',
					],
				}),
			);
			const body = recap.split("\n\n").slice(1).join(" ");
			const shooterAt = body.indexOf("Metta World Peace");
			const otherAt = body.indexOf("Ray Allen");
			assert.ok(shooterAt >= 0, recap);
			assert.ok(otherAt === -1 || shooterAt < otherAt, recap);
		}
	});
});

// A slate is read as a page, not as twelve separate recaps, so the thing that
// gives it away is not any one sentence but the same sentence twelve times with
// the names swapped. `pick` rotates a pool before repeating - but it keys on the
// rendered text, and almost every pool interpolates a name or a number, so for
// years the key was different in every game and the rotation never engaged.
describe("a slate does not repeat itself", () => {
	const slateHeadlines = () => {
		const shapes: [string, string, number, number, number, number][] = [
			["Bobcats", "SuperSonics", 87, 84, 27, 4],
			["76ers", "Bucks", 98, 82, 23, 9],
			["Hawks", "Warriors", 106, 93, 24, 10],
			["Bulls", "Nets", 100, 99, 34, 16],
			["Spurs", "Mavericks", 94, 88, 23, 7],
			["Raptors", "Suns", 110, 103, 21, 5],
			["Magic", "Knicks", 115, 89, 24, 5],
			["Timberwolves", "Rockets", 109, 86, 21, 4],
		];

		beginRecapBatch();
		const out = shapes.map(([w, l, wp, lp, pts, reb], i) => {
			const winner = realisticTeam(
				{ tid: i * 2, name: w, pts: wp },
				player({ name: `${w} Star`, pts, reb, ast: 3, fg: 9, fga: 18 }),
			);
			const loser = realisticTeam(
				{ tid: i * 2 + 1, name: l, pts: lp },
				player({ name: `${l} Star`, pts: 18, reb: 6, ast: 3, fg: 7, fga: 17 }),
			);
			return (
				getAutoRecap(
					game({ gid: i, teams: [winner, loser], winnerTid: winner.tid }),
				)
					.split("\n")[0]!
					// Blank the names and numbers, leaving the sentence's shape.
					.replaceAll(/\b[A-Z][\w'.’-]*(?:\s+[A-Z][\w'.’-]*)*/g, "~")
					.replaceAll(/\d+/g, "#")
			);
		});
		endRecapBatch();
		return out;
	};

	// "to" takes a bare infinitive. The comeback verb pool ("storm back to beat")
	// run through pastTense and dropped into that slot produced "erased an
	// 18-point deficit to stormed back to beat the Rivers 103-100".
	test("a comeback lead never puts a past-tense verb after 'to'", () => {
		const texts: string[] = [];
		for (let i = 0; i < 40; i++) {
			const winner = realisticTeam(
				{ tid: 0, name: "Apollos", pts: 103, ptsQtrs: [18, 22, 31, 32] },
				player({ name: "Nenad Canak", pts: 24, reb: 6, fg: 9, fga: 17 }),
			);
			const loser = realisticTeam(
				{ tid: 1, name: "Rivers", pts: 100, ptsQtrs: [36, 24, 22, 18] },
				player({ name: "Will Ruland", pts: 20, reb: 5, fg: 8, fga: 18 }),
			);
			texts.push(
				getAutoRecap(
					game({ gid: i, teams: [winner, loser], winnerTid: winner.tid }),
				),
			);
		}
		const broken = texts.filter((t) =>
			/deficit to (?:stormed|rallied|came back|beat back|routed|stunned|shocked|held|knocked|topped|handled|survived|outlasted|edged)\b/.test(
				t,
			),
		);
		assert.deepEqual(broken, []);
	});

	// Paragraph one's team-stat sentence and paragraph three's comparison were
	// picked independently, so a recap regularly said "They knocked down 16
	// threes." and then "From deep it was no contest - 16 threes to 6."
	test("the three-point line is not reported twice in one recap", () => {
		// The winner hits 18 threes to the loser's 4, which is both over
		// statNote's team-total bar and over threeNote's comparison gap - so
		// paragraph one and paragraph three both WANT to talk about it.
		const shooters = (
			name: string,
			tid: number,
			pts: number,
			threes: number[],
		) =>
			({
				tid,
				region: "",
				name,
				abbrev: "???",
				pts,
				ptsQtrs: [28, 30, 32, pts - 90],
				players: threes.map((tp, n) =>
					player({
						name: `${name} ${"ABCDE"[n]}`,
						pts: 26 - n * 3,
						reb: 5 + n,
						tp,
						tpa: tp + 4,
						fg: 9 - n,
						fga: 16 - n,
					}),
				),
			}) as RecapTeam;

		for (let i = 0; i < 40; i++) {
			const winner = shooters("Vultures", 0, 120, [6, 4, 4, 2, 2]);
			const loser = shooters("Spirits", 1, 100, [2, 1, 1, 0, 0]);
			const text = getAutoRecap(
				game({ gid: i, teams: [winner, loser], winnerTid: 0 }),
			);
			// The TEAM's three-point total (paragraph one) against the two sides'
			// three-point comparison (paragraph three). A player's own threes are
			// a different fact and may appear alongside either.
			const teamTotal =
				/knocked down \d+ threes|hit \d+ from deep|made \d+ three-pointers|\w+ threes fell for/.test(
					text,
				);
			const comparison =
				/difference was behind the arc|made \d+ more threes than|From deep it was no contest/.test(
					text,
				);
			assert.ok(
				!(teamTotal && comparison),
				`the team's three-point line told twice:\n${text}`,
			);
		}
	});

	test("eight games do not produce the same headline shape twice", () => {
		const shapes = slateHeadlines();
		const counts = new Map<string, number>();
		for (const s of shapes) {
			counts.set(s, (counts.get(s) ?? 0) + 1);
		}
		const repeated = [...counts.entries()].filter(([, n]) => n > 1);
		assert.deepEqual(
			repeated,
			[],
			`repeated headline shapes across one slate:\n${shapes.join("\n")}`,
		);
	});
});

// Defects found by reading a real day of recaps out of a live league, each one
// a line a person actually saw on the page.
describe("copy defects found in the field", () => {
	const teamWithInjury = (type: string) =>
		realisticTeam(
			{
				tid: 1,
				region: "Atlanta",
				name: "Hawks",
				abbrev: "ATL",
				pts: 102,
				injuries: [{ name: "Blake Griffin", type, gamesRemaining: 20 }],
			},
			player({ name: "Stephon Marbury", pts: 18, ast: 12, fg: 7, fga: 15 }),
		);
	const opponent = realisticTeam(
		{
			tid: 2,
			region: "Portland",
			name: "Trail Blazers",
			abbrev: "POR",
			pts: 96,
		},
		player({ name: "Keith Bogans", pts: 18, reb: 4, fg: 7, fga: 16 }),
	);

	// "Blake Griffin sat out for the Hawks with an injured;" - the generic type
	// some imported rosters carry is a bare adjective, and the article machinery
	// treated it as a noun.
	test("a bare-participle injury type does not become 'an injured'", () => {
		const recap = getAutoRecap(
			game({ gid: 7001, teams: [teamWithInjury("Injured"), opponent] }),
		);
		assert.ok(!/\ban injured\b/i.test(recap), recap);
		assert.ok(/\ban injury\b/i.test(recap), recap);
	});

	test("a real injury name still keeps its own words", () => {
		const recap = getAutoRecap(
			game({ gid: 7002, teams: [teamWithInjury("Sprained Ankle"), opponent] }),
		);
		assert.ok(/\ba sprained ankle\b/i.test(recap), recap);
	});

	// "Al Horford was good for 18 points... Keith Bogans was good for 18 points
	// in defeat." Three sentence builders drew from overlapping verb lists, and
	// `pick` rotates within a pool but cannot see across two of them.
	test("no scoring verb is used twice in one recap", () => {
		const verbs = [
			"added",
			"chipped in",
			"contributed",
			"kicked in",
			"pitched in with",
			"tacked on",
			"was good for",
			"supplied",
			"came up with",
			"put up",
			"posted",
			"went for",
		];
		// Sweep seeds: which verbs come up at all is seed-dependent, so one game
		// proves nothing.
		for (let gid = 7100; gid < 7160; gid += 1) {
			const recap = getAutoRecap(
				game({ gid, teams: [teamWithInjury("Sore Knee"), opponent] }),
			);
			for (const verb of verbs) {
				const hits = recap.split(verb).length - 1;
				assert.ok(hits <= 1, `"${verb}" twice in gid ${gid}:\n${recap}`);
			}
		}
	});

	// "Sasha Pavlovic pitched in with 27 points, and Jamal Sampson put up 15.
	// Sasha Pavlovic finished +34, the best mark on the floor." The plus-minus
	// note shares a paragraph with the supporting cast and knew nothing about it.
	test("the plus-minus note does not re-introduce a man already named", () => {
		// LeBron is the story; Pavlovic is the supporting-cast pick AND the
		// biggest swing on the floor, which is exactly when the two sentences
		// collided.
		const suns = realisticTeam(
			{
				tid: 3,
				region: "Phoenix",
				name: "Suns",
				abbrev: "PHO",
				pts: 122,
				ptsQtrs: [30, 30, 32, 30],
				players: [
					player({
						name: "Lebron James",
						pts: 34,
						reb: 5,
						ast: 9,
						fg: 13,
						fga: 20,
					}),
					player({
						name: "Sasha Pavlovic",
						pts: 27,
						reb: 4,
						fg: 10,
						fga: 16,
						pm: 34,
					}),
					player({ name: "Jamal Sampson", pts: 15, reb: 8, fg: 6, fga: 10 }),
					player({ name: "Bench Guy", pts: 6, reb: 3, fg: 2, fga: 5 }),
				],
			},
			player({
				name: "Lebron James",
				pts: 34,
				reb: 5,
				ast: 9,
				fg: 13,
				fga: 20,
			}),
		);
		const jazz = realisticTeam(
			{ tid: 4, region: "Utah", name: "Jazz", abbrev: "UTA", pts: 89 },
			player({ name: "Rodrigue Beaubois", pts: 20, fg: 8, fga: 19 }),
		);
		for (let gid = 7200; gid < 7240; gid += 1) {
			const recap = getAutoRecap(game({ gid, teams: [suns, jazz] }));
			const hits = recap.split("Sasha Pavlovic").length - 1;
			assert.ok(hits <= 1, `named twice in gid ${gid}:\n${recap}`);
		}
	});

	// The efficient-shooting note fires only when the star shot far BETTER than
	// his season mark, and one of its three phrasings said the opposite.
	test("a big shooting night is never called a long way FROM his average", () => {
		const bulls = realisticTeam(
			{
				tid: 5,
				region: "Chicago",
				name: "Bulls",
				abbrev: "CHI",
				pts: 115,
				ptsQtrs: [30, 28, 29, 28],
			},
			player({
				name: "Tyson Chandler",
				pts: 51,
				reb: 10,
				stl: 3,
				fg: 16,
				fga: 20,
				seasonAvg: avg({ pts: 18, fgp: 55.1 }),
			}),
		);
		const spurs = realisticTeam(
			{ tid: 6, region: "San Antonio", name: "Spurs", abbrev: "SAS", pts: 104 },
			player({ name: "Corey Maggette", pts: 23, stl: 5, fg: 9, fga: 20 }),
		);
		for (let gid = 7300; gid < 7340; gid += 1) {
			const recap = getAutoRecap(game({ gid, teams: [bulls, spurs] }));
			assert.ok(
				!/long way from/.test(recap),
				`gid ${gid} calls a 16-of-20 night a shortfall:\n${recap}`,
			);
		}
	});
});

// The postseason context wrote only the winner's half of a series result: the
// loser's nickname was computed at the top of the function and thrown away
// unused at the bottom, so a recap said who advanced and simply stopped.
describe("the losing side of a playoff series", () => {
	const seriesGame = (
		homeWon: number,
		awayWon: number,
		opts: {
			gid?: number;
			round?: number;
			numRounds?: number;
			bestOf?: number;
			wSeed?: number;
			lSeed?: number;
		} = {},
	) => {
		const bos = realisticTeam(
			{
				tid: 1,
				region: "Boston",
				name: "Celtics",
				abbrev: "BOS",
				pts: 104,
				ptsQtrs: [26, 24, 28, 26],
				seed: opts.wSeed ?? 2,
			},
			player({ name: "Paul Pierce", pts: 28, reb: 7, ast: 5, fg: 10, fga: 19 }),
		);
		const det = realisticTeam(
			{
				tid: 2,
				region: "Detroit",
				name: "Pistons",
				abbrev: "DET",
				pts: 96,
				ptsQtrs: [24, 24, 24, 24],
				seed: opts.lSeed ?? 3,
			},
			player({ name: "Chauncey Billups", pts: 25, ast: 7, fg: 9, fga: 18 }),
		);
		return game({
			gid: opts.gid ?? 8000,
			teams: [bos, det],
			winnerTid: 1,
			playoffs: true,
			series: {
				round: opts.round ?? 2,
				numRounds: opts.numRounds ?? 4,
				bestOf: opts.bestOf ?? 7,
				homeAbbrev: "BOS",
				awayAbbrev: "DET",
				homeSeed: opts.wSeed ?? 2,
				awaySeed: opts.lSeed ?? 3,
				homeWon,
				awayWon,
			},
		});
	};

	test("a 3-1 lead says the other side must win three straight", () => {
		const recap = getAutoRecap(seriesGame(2, 1, { gid: 8001 }));
		assert.ok(/must win three straight/.test(recap), recap);
		assert.ok(/Game 5/.test(recap), recap);
	});

	test("a 3-2 lead says the other side faces elimination next game", () => {
		const recap = getAutoRecap(seriesGame(2, 2, { gid: 8002 }));
		assert.ok(/face elimination in Game 6/.test(recap), recap);
	});

	test("surviving elimination still leaves the other side able to close it out", () => {
		const recap = getAutoRecap(seriesGame(1, 3, { gid: 8003 }));
		assert.ok(/elimination/.test(recap), recap);
		assert.ok(/can still close it out in Game 6/.test(recap), recap);
	});

	test("a series win ends somebody's season, and says so", () => {
		const recap = getAutoRecap(seriesGame(3, 1, { gid: 8004, round: 1 }));
		assert.ok(/season is over/.test(recap), recap);
	});

	test("a sweep says they never won a game", () => {
		const recap = getAutoRecap(seriesGame(3, 0, { gid: 8005, round: 1 }));
		assert.ok(/without winning a game/.test(recap), recap);
	});

	test("the Finals loser is named as runner-up", () => {
		const recap = getAutoRecap(
			seriesGame(3, 2, { gid: 8006, round: 4, numRounds: 4 }),
		);
		assert.ok(/runners-up/.test(recap), recap);
	});

	// Every sentence built here opens with a team, and they were bare nicknames
	// while the rest of the piece says "the Celtics".
	test("postseason sentences take the article the rest of the recap uses", () => {
		for (const [h, a] of [
			[2, 1],
			[0, 0],
			[1, 3],
			[2, 2],
		] as const) {
			const recap = getAutoRecap(seriesGame(h, a, { gid: 8100 + h * 10 + a }));
			assert.ok(
				!/(?:^|[!.] )(?:Celtics|Pistons) /.test(recap),
				`bare nickname opens a sentence:\n${recap}`,
			);
		}
	});

	// A title-clinching game used to get a headline indistinguishable from a
	// Tuesday in January.
	test("the headline carries the stakes of a clincher", () => {
		const title = getAutoRecap(
			seriesGame(3, 2, { gid: 8200, round: 4, numRounds: 4 }),
		);
		assert.ok(/clinching the title/.test(title.split("\n")[0]!), title);

		const advance = getAutoRecap(seriesGame(3, 1, { gid: 8201, round: 1 }));
		assert.ok(
			/advancing to the Conference Semifinals/.test(advance.split("\n")[0]!),
			advance,
		);
	});

	// A clincher that is also the decider keeps BOTH facts - dropping "Game 7"
	// out of a Game 7 is the one detail nobody would leave out.
	test("a Game 7 clincher stays a Game 7", () => {
		const recap = getAutoRecap(seriesGame(3, 3, { gid: 8300, round: 1 }));
		assert.ok(/Game 7/.test(recap), recap);
		assert.ok(/advancing to/.test(recap), recap);
	});

	// Seeds were in the payload and never read, and a low seed beating a high one
	// is the fact everyone repeats about a playoff series.
	test("a big seed gap is named on an upset", () => {
		const recap = getAutoRecap(
			seriesGame(3, 2, { gid: 8400, round: 1, wSeed: 7, lSeed: 2 }),
		);
		assert.ok(/#7 seed/.test(recap), recap);
		assert.ok(/#2 seed/.test(recap), recap);
	});

	test("a close seed gap is not remarked on", () => {
		const recap = getAutoRecap(
			seriesGame(3, 2, { gid: 8401, round: 1, wSeed: 2, lSeed: 3 }),
		);
		assert.ok(!/#\d seed/.test(recap), recap);
	});
});
