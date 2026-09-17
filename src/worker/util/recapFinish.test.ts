import assert from "node:assert/strict";
import { describe, test } from "vitest";
import type { FinishEvent } from "../../common/gameFlow.ts";
import type {
	RecapGame,
	RecapPlayer,
	RecapTeam,
} from "./getDayGamesForRecap.ts";
import { finishScores, finishStory, shotName } from "./recapFinish.ts";
import { rngFromSeed } from "./recapText.ts";
import { verifyRecap } from "./recapAccuracy.ts";

const player = (name: string, pid: number, pts = 10): RecapPlayer => ({
	name,
	pid,
	min: 30,
	pts,
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
});

const wizards: RecapTeam = {
	tid: 1,
	region: "Washington",
	name: "Wizards",
	abbrev: "WAS",
	pts: 104,
	ptsQtrs: [36, 22, 24, 22],
	players: [player("Marcus Nowell", 11, 20), player("Tyrese Nowell", 12, 12)],
};
const sixers: RecapTeam = {
	tid: 2,
	region: "Philadelphia",
	name: "76ers",
	abbrev: "PHI",
	pts: 101,
	ptsQtrs: [16, 30, 28, 27],
	players: [player("Cade King", 21, 24), player("Anthony King", 22, 18)],
};

const ev = (
	side: 0 | 1,
	pid: number,
	pts: number,
	kind: FinishEvent["kind"],
	clock: number,
	score: [number, number],
	extra: Partial<FinishEvent> = {},
): FinishEvent => ({ side, pid, pts, kind, period: 4, clock, score, ...extra });

const build = (
	finish: FinishEvent[],
	extra: Partial<RecapGame> = {},
): RecapGame => ({
	gid: 1,
	day: 1,
	overtimes: 0,
	winnerTid: 1,
	playoffs: false,
	clutchPlays: [],
	teams: [wizards, sixers],
	flow: {
		leadChanges: 6,
		ties: 5,
		maxLead: [20, 3],
		late: [
			{ clock: 300, pts: [92, 90] },
			{ clock: 120, pts: [97, 97] },
		],
		finish,
	},
	...extra,
});

const tell = (game: RecapGame, seed = 1, shotTold = false) =>
	finishStory(
		{
			game,
			winner: wizards,
			loser: sixers,
			regPeriods: 4,
			shotTold,
		},
		rngFromSeed(seed),
	);

describe("finishStory", () => {
	test("the tie, the go-ahead shot and the free throws that sealed it", () => {
		const game = build([
			ev(0, 11, 2, "mid", 95, [99, 97]),
			ev(1, 21, 2, "rim", 60, [99, 99]),
			ev(1, 22, 2, "mid", 40, [99, 101]),
			ev(0, 11, 3, "tp", 24.9, [102, 101]),
			ev(0, 12, 2, "ft", 4.1, [104, 101]),
		]);
		for (let seed = 0; seed < 12; seed++) {
			const out = tell(game, seed);
			const text = out.join(" ");
			// The losers' lead is the thing the go-ahead shot answered.
			assert.match(text, /Anthony King's jumper/);
			assert.match(text, /101-99/);
			assert.match(
				text,
				/Marcus Nowell's three-pointer .*24\.9 seconds|three-pointer with 24\.9 seconds/,
			);
			assert.match(text, /102-101/);
			assert.match(text, /Tyrese Nowell.*two free throws.*4\.1 seconds/);
			assert.ok(out.length <= 3, text);
			// The accuracy reader holds every score against the log.
			assert.deepEqual(verifyRecap(`**x**\n\n${text}`, game), []);
		}
	});

	test("when the lede already told the winning shot, the tie is told in the past perfect and the shot is not told twice", () => {
		const game = build([
			ev(1, 21, 2, "rim", 24.9, [101, 101]),
			ev(0, 11, 3, "tp", 1.8, [104, 101]),
		]);
		const text = tell(game, 3, true).join(" ");
		assert.match(
			text,
			/Cade King's layup .*had (tied it at 101|made it 101-101)|It was 101-101 after Cade King's layup/,
		);
		assert.doesNotMatch(text, /Marcus Nowell/);
	});

	test("a winner that led throughout the window: how close the losers came, and who shut the door", () => {
		const held: RecapTeam = { ...wizards, pts: 103 };
		const chased: RecapTeam = { ...sixers, pts: 101 };
		const game = build(
			[
				ev(1, 21, 3, "tp", 50, [100, 98]),
				ev(0, 11, 1, "ft", 30, [101, 98]),
				ev(1, 22, 2, "rim", 12, [101, 100]),
				ev(0, 12, 2, "ft", 5.5, [103, 100]),
				ev(1, 21, 1, "ft", 1, [103, 101]),
			],
			{ teams: [held, chased] },
		);
		(game.flow as any).late = [{ clock: 120, pts: [95, 90] }];
		const text = finishStory(
			{ game, winner: held, loser: chased, regPeriods: 4, shotTold: false },
			rngFromSeed(5),
		).join(" ");
		assert.match(text, /Anthony King's layup/);
		assert.match(text, /within 1|101-100/);
		assert.match(text, /Tyrese Nowell .*two free throws/);
		assert.deepEqual(verifyRecap(`**x**\n\n${text}`, game), []);
	});

	test("nothing to say when the losers never got close", () => {
		const game = build([
			ev(0, 11, 2, "mid", 95, [99, 91]),
			ev(1, 21, 2, "rim", 60, [99, 93]),
			ev(0, 12, 2, "ft", 4.1, [101, 93]),
		]);
		assert.deepEqual(tell(game), []);
	});

	test("overtime opens on the shot that forced it, then tells the extra period", () => {
		const game = build(
			[
				ev(1, 21, 2, "rim", 3.2, [96, 96]),
				ev(0, 11, 2, "rim", 250, [98, 96], { period: 5 }),
				ev(1, 22, 3, "tp", 100, [98, 99], { period: 5 }),
				ev(0, 12, 2, "mid", 40, [100, 99], { period: 5 }),
				ev(0, 11, 2, "ft", 8, [102, 99], { period: 5 }),
			],
			{
				overtimes: 1,
				clutchPlays: [
					'<a href="#">Cade King</a> made a basket with 3.2 seconds remaining to force overtime.',
				],
			},
		);
		(game.teams[0] as any).ptsQtrs = [36, 22, 24, 14, 6];
		(game.teams[1] as any).ptsQtrs = [16, 30, 28, 22, 3];
		const text = tell(game, 2).join(" ");
		assert.match(text, /Cade King.*(overtime|extra period)/);
		assert.match(text, /3\.2 seconds/);
		assert.match(text, /Anthony King's three-pointer/);
		assert.match(text, /99-98/);
		assert.match(text, /Tyrese Nowell.*jumper.*100-99/);
		assert.match(text, /Marcus Nowell.*two free throws/);
		(game.teams[0] as any).ptsQtrs = [36, 22, 24, 22];
		(game.teams[1] as any).ptsQtrs = [16, 30, 28, 27];
	});

	test("the same man twice in a row becomes a pronoun", () => {
		const game = build([
			ev(1, 21, 2, "rim", 60, [99, 99]),
			ev(0, 11, 2, "mid", 30, [101, 99]),
		]);
		const text = finishStory(
			{
				game,
				winner: wizards,
				loser: sixers,
				regPeriods: 4,
				shotTold: false,
				justNamed: "Cade King",
			},
			rngFromSeed(1),
		).join(" ");
		assert.match(text, /^(His layup|It was 99-99 after his layup)/);
	});
});

describe("shotName", () => {
	test("names the play, not the stat call", () => {
		assert.equal(shotName({ kind: "rim", pts: 2 }), "layup");
		assert.equal(
			shotName({ kind: "rim", pts: 3, andOne: true }),
			"three-point play",
		);
		assert.equal(
			shotName({ kind: "tp", pts: 4, andOne: true }),
			"four-point play",
		);
		assert.equal(shotName({ kind: "ft", pts: 2 }), "two free throws");
		assert.equal(shotName({ kind: "mid", pts: 2 }), "jumper");
	});
});

describe("finishScores", () => {
	test("every score the stretch passed through, both ways round", () => {
		const { pairs, ties } = finishScores(
			build([
				ev(1, 21, 2, "rim", 60, [99, 99]),
				ev(0, 11, 2, "mid", 30, [101, 99]),
			]),
		);
		assert.ok(pairs.has("101-99") && pairs.has("99-101"));
		assert.ok(ties.has(99) && ties.has(97));
	});
});
