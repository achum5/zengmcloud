import { assert, describe, test } from "vitest";
import type { FinishEvent } from "../../common/gameFlow.ts";
import type {
	RecapGame,
	RecapPlayer,
	RecapTeam,
} from "./getDayGamesForRecap.ts";
import {
	finishScores,
	finishStory,
	shotName,
	withSettledLead,
} from "./recapFinish.ts";
import { otPhrase, rngFromSeed } from "./recapText.ts";
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
		assert.notMatch(text, /Marcus Nowell/);
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

	// "Lonnie Ingram's three-pointer with 0.9 seconds left made it 97-97.
	// Payton Ingram's three-pointer with 4:39 to go then made it 100-97." -
	// the tie that forced a second overtime, told like any other, and the
	// reader could not tell there had been one.
	test("a tie that ends an overtime says it forced another", () => {
		const game = build(
			[
				ev(1, 21, 2, "rim", 3.2, [96, 96]),
				ev(0, 11, 2, "rim", 250, [98, 96], { period: 5 }),
				ev(1, 22, 3, "tp", 0.9, [98, 99], { period: 5 }),
				ev(0, 12, 1, "ft", 0.4, [99, 99], { period: 5 }),
				ev(0, 11, 2, "mid", 100, [101, 99], { period: 6 }),
				ev(0, 11, 2, "ft", 8, [103, 99], { period: 6 }),
			],
			{
				overtimes: 2,
				clutchPlays: [
					'<a href="#">Cade King</a> made a basket with 3.2 seconds remaining to force overtime.',
				],
			},
		);
		for (let seed = 1; seed <= 6; seed++) {
			const text = tell(game, seed).join(" ");
			assert.match(text, /tied it at 99 and forced a second overtime/, text);
		}
	});

	// "It took Anthony Lowry's three-pointer at the buzzer to force the extra
	// period. They led 108-106 on Lonnie Lowry's layup with 4:51 to play" -
	// that 4:51 was in the SECOND overtime, and the free throw that forced
	// it was never told.
	test("every overtime that was forced is told, in order", () => {
		const game = build(
			[
				ev(1, 21, 3, "tp", 0, [96, 96]),
				ev(0, 11, 2, "rim", 250, [98, 96], { period: 5 }),
				ev(1, 22, 1, "ft", 43.3, [98, 98], { period: 5 }),
				ev(1, 21, 2, "rim", 291, [98, 100], { period: 6 }),
				ev(0, 12, 2, "mid", 200, [100, 100], { period: 6 }),
				ev(1, 22, 2, "rim", 160, [100, 102], { period: 6 }),
				ev(0, 11, 3, "tp", 94, [103, 102], { period: 6 }),
				ev(0, 12, 2, "ft", 5, [105, 102], { period: 6 }),
			],
			{
				overtimes: 2,
				clutchPlays: [
					'<a href="#">Cade King</a> made a three-pointer at the buzzer to force  overtime.',
				],
			},
		);
		for (let seed = 1; seed <= 8; seed++) {
			const text = tell(game, seed).join(" ");
			assert.match(text, /Cade King.*buzzer.*(overtime|extra period)/, text);
			assert.match(
				text,
				/Anthony King's free throw with 43\.3 seconds (left|to go|to play) tied it at 98 and forced a second overtime/,
				text,
			);
			// In order: the first overtime's tie before the second's baskets.
			assert.ok(text.indexOf("second overtime") < text.indexOf("102"), text);
			assert.notMatch(text, /the extra period/, text);
		}
	});

	// The sim notes only a tie in the last seconds. One with 38.7 seconds
	// left that nobody broke forced overtime too, and the overtime baskets
	// after it read as regulation's.
	test("a tie with no clutch note is found in the log", () => {
		const game = build(
			[
				ev(1, 21, 2, "post", 38.7, [99, 99]),
				ev(0, 11, 2, "rim", 100, [101, 99], { period: 5 }),
				ev(0, 12, 2, "ft", 3, [103, 99], { period: 5 }),
			],
			{ overtimes: 1 },
		);
		for (let seed = 1; seed <= 6; seed++) {
			const text = tell(game, seed).join(" ");
			assert.match(text, /Cade King.*hook shot.*38\.7 seconds/, text);
			assert.match(text, /overtime|extra period/, text);
		}
	});

	// A double-overtime game whose regulation tie had no note but whose
	// second did: "to force double overtime" is the SECOND overtime's.
	test("the note that forced a second overtime is not taken for regulation's", () => {
		const game = build(
			[
				ev(1, 21, 2, "mid", 29.7, [96, 96]),
				ev(0, 11, 2, "rim", 250, [98, 96], { period: 5 }),
				ev(1, 22, 2, "rim", 17.8, [98, 98], { period: 5 }),
				ev(0, 11, 2, "rim", 100, [100, 98], { period: 6 }),
				ev(0, 12, 2, "ft", 3, [102, 98], { period: 6 }),
			],
			{
				overtimes: 2,
				clutchPlays: [
					'<a href="#">Anthony King</a> made a basket with 17.8 seconds remaining to force double overtime.',
				],
			},
		);
		for (let seed = 1; seed <= 6; seed++) {
			const text = tell(game, seed).join(" ");
			assert.match(text, /Cade King.*29\.7 seconds/, text);
			assert.match(
				text,
				/Anthony King's layup .*forced a second overtime/,
				text,
			);
			assert.notMatch(
				text,
				/Anthony King[^.]*(sent it to overtime|forced overtime|force overtime)/,
				text,
			);
			assert.deepEqual(verifyRecap(`**x**\n\n${text}`, game), []);
		}
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

// Up 112-109 on a three with 41.6 seconds left, tied at 119 with 5.9 left,
// ahead again on a layup with 2.5 left: the flow's lastLead stayed on the
// three, and the recap called it the go-ahead basket.
describe("withSettledLead", () => {
	test("a tie after the last lead change moves the lead to the basket that broke it", () => {
		const game = build([
			ev(0, 11, 3, "tp", 41.6, [112, 109]),
			ev(1, 21, 2, "rim", 5.9, [112, 114]),
			ev(1, 22, 3, "tp", 4, [117, 117]),
			ev(0, 12, 2, "rim", 2.5, [119, 117]),
			ev(0, 12, 1, "ft", 1.6, [120, 117]),
		]);
		game.flow = {
			...game.flow!,
			lastLead: {
				side: 0,
				pid: 11,
				period: 4,
				clock: 41.6,
				pts: [112, 109],
				by: 3,
			},
			lastTie: { period: 4, clock: 4, pts: 117 },
		};
		const settled = withSettledLead(game).flow!.lastLead!;
		assert.equal(settled.pid, 12);
		assert.equal(settled.clock, 2.5);
		assert.deepEqual(settled.pts, [119, 117]);
		// No tie after it: left alone.
		game.flow.lastTie = { period: 4, clock: 50, pts: 109 };
		assert.equal(withSettledLead(game).flow!.lastLead!.pid, 11);
	});
});

describe("how many overtimes", () => {
	test("a headline says double overtime, and the reader holds it to the game", () => {
		assert.equal(otPhrase(1), "overtime");
		assert.equal(otPhrase(2), "double overtime");
		assert.equal(otPhrase(3), "triple overtime");
		assert.equal(otPhrase(4), "four overtimes");
		const game = build([], { overtimes: 2 });
		const kinds = (s: string) =>
			verifyRecap(`**x**\n\n${s}`, game).map((v) => v.detail);
		assert.deepEqual(kinds("The Wizards won in double overtime."), []);
		assert.deepEqual(
			kinds("A layup tied it at 99 and forced a second overtime."),
			[],
		);
		assert.equal(
			kinds("A layup tied it at 99 and forced a third overtime.").length,
			1,
		);
		assert.equal(kinds("The Wizards won in triple overtime.").length, 1);
		assert.equal(kinds("The Wizards won 104-101 (3OT).").length, 1);
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
		assert.equal(shotName({ kind: "ft", pts: 3 }), "three free throws");
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

describe("the finish says only what the clock allows", () => {
	test("a cut at the buzzer is too late, not something the winners held", () => {
		const held: RecapTeam = { ...wizards, pts: 104 };
		const chased: RecapTeam = { ...sixers, pts: 102 };
		const game = build(
			[ev(0, 11, 2, "rim", 80, [104, 97]), ev(1, 21, 3, "tp", 0.2, [104, 102])],
			{ teams: [held, chased] },
		);
		(game.flow as any).late = [{ clock: 120, pts: [100, 95] }];
		for (let seed = 0; seed < 12; seed++) {
			const text = finishStory(
				{ game, winner: held, loser: chased, regPeriods: 4, shotTold: false },
				rngFromSeed(seed),
			).join(" ");
			assert.notMatch(text, /held from there|never got the stop/, text);
		}
	});

	// "The Nets got as close as 114-112 ..., but that was as close as it
	// got" said it twice; "The Mavericks got as close as 115-112 ..., and
	// the Mavericks never got the stop they needed" named them twice.
	test("how close they got is said once, and so are they", () => {
		const held: RecapTeam = { ...wizards, pts: 104 };
		const chased: RecapTeam = { ...sixers, pts: 101 };
		const game = build(
			[ev(0, 11, 2, "rim", 80, [104, 98]), ev(1, 21, 3, "tp", 20, [104, 101])],
			{ teams: [held, chased] },
		);
		(game.flow as any).late = [{ clock: 120, pts: [102, 98] }];
		for (let seed = 0; seed < 24; seed++) {
			const text = finishStory(
				{ game, winner: held, loser: chased, regPeriods: 4, shotTold: false },
				rngFromSeed(seed),
			).join(" ");
			assert.notMatch(text, /got as close as[^.]*as close as it got/, text);
			assert.notMatch(text, /The 76ers [^.]*the 76ers never/, text);
		}
	});

	// "It was an 11-point game going into the fourth, but the Cavaliers took
	// the fourth quarter 24-14. It was 117-117 with two minutes to play."
	test("the finish does not open on It was after a sentence that did", () => {
		const game = build([
			ev(1, 21, 2, "rim", 60, [99, 99]),
			ev(0, 11, 2, "mid", 30, [101, 99]),
		]);
		(game.flow as any).late = [{ clock: 120, pts: [97, 97] }];
		for (let seed = 0; seed < 12; seed++) {
			const text = finishStory(
				{
					game,
					winner: wizards,
					loser: sixers,
					regPeriods: 4,
					shotTold: false,
					afterItWas: true,
				},
				rngFromSeed(seed),
			);
			assert.notMatch(text[0] ?? "", /^It was\b/, text.join(" "));
		}
	});

	test("free throws that did not decide anything do not finish it", () => {
		// Up nine when they were taken: the go-ahead basket and the cut are
		// the story, and "his two free throws with 3.7 seconds left finished
		// it" is not.
		const game = build([
			ev(1, 21, 2, "rim", 100, [97, 99]),
			ev(0, 11, 3, "tp", 90, [100, 99]),
			ev(1, 22, 2, "rim", 75, [100, 101]),
			ev(0, 12, 2, "mid", 60, [102, 101]),
			ev(1, 21, 1, "ft", 50, [102, 102]),
			ev(0, 11, 2, "rim", 40, [104, 102]),
			ev(0, 11, 3, "tp", 30, [107, 102]),
			ev(0, 12, 2, "rim", 20, [109, 102]),
			ev(0, 12, 2, "ft", 3.7, [111, 102]),
		]);
		for (let seed = 0; seed < 12; seed++) {
			const text = tell(game, seed).join(" ");
			assert.notMatch(text, /3\.7 seconds/, text);
		}
	});
});

// "The Jazz were up 100-91 with two minutes left" and then the winning
// three: nothing of the 103-94 with 56.8 seconds to go, or the ten straight
// points after it.
describe("a late rally", () => {
	// The 76ers in front all the way through the window, nine up with under
	// a minute left; the Wizards score the last ten.
	const rally = () =>
		build([
			ev(1, 21, 2, "rim", 110, [91, 100]),
			ev(0, 11, 2, "rim", 95, [93, 100]),
			ev(1, 22, 3, "tp", 56.8, [93, 103]),
			ev(0, 12, 3, "tp", 40, [96, 103]),
			ev(0, 11, 2, "rim", 30, [98, 103]),
			ev(0, 12, 3, "tp", 15, [101, 103]),
			ev(0, 11, 3, "tp", 4.4, [104, 103]),
		]);

	test("the deficit and the run that erased it are told", () => {
		for (const shotTold of [false, true]) {
			for (let seed = 0; seed < 8; seed++) {
				const game = rally();
				const text = tell(game, seed, shotTold).join(" ");
				assert.match(text, /103-93/, text);
				assert.match(text, /56\.8 seconds/, text);
				assert.match(text, /the next 11 points/, text);
				assert.deepEqual(verifyRecap(`**x**\n\n${text}`, game), [], text);
			}
		}
	});

	test("an answered rally names the basket that got them close", () => {
		const game = build([
			ev(1, 21, 2, "rim", 80, [95, 100]),
			ev(0, 11, 3, "tp", 50, [98, 100]),
			ev(1, 22, 1, "ft", 30, [98, 101]),
			ev(0, 12, 2, "rim", 20, [100, 101]),
			ev(0, 11, 2, "mid", 2, [102, 101]),
		]);
		const text = tell(game, 2, true).join(" ");
		assert.match(text, /100-95/, text);
		assert.match(text, /Tyrese Nowell's layup .*had cut it to 101-100/, text);
		assert.deepEqual(verifyRecap(`**x**\n\n${text}`, game), [], text);
	});

	test("the accuracy reader holds the run to the log", () => {
		const game = rally();
		const bad = verifyRecap(
			"**x**\n\nThe 76ers led 103-93 with 56.8 seconds left before the Wizards scored the next 12 points.",
			game,
		);
		assert.deepEqual(
			bad.map((v) => v.kind),
			["finish run"],
		);
	});

	// "Cade Brooks' three-pointer with 28.8 seconds left put the Hawks
	// ahead 114-111. Anthony Brooks set it up." - the pass is the basket's.
	test("the pass that set up the go-ahead basket is in its sentence", () => {
		const game = build([
			ev(1, 21, 2, "rim", 50, [101, 101]),
			ev(0, 11, 3, "tp", 28.8, [104, 101], { ast: 12 }),
		]);
		for (let seed = 0; seed < 8; seed++) {
			const text = tell(game, seed).join(" ");
			assert.match(
				text,
				/three-pointer off a pass from Tyrese Nowell with 28\.8 seconds/,
				text,
			);
			assert.notMatch(text, /set it up|had the assist/, text);
		}
	});

	// "Quentin Ingram sealed it with two free throws with 3:44 left", and
	// the Pelicans scored eleven more.
	test("free throws minutes from the end did not seal it", () => {
		const game = build([
			ev(1, 21, 2, "rim", 110, [99, 99]),
			ev(0, 11, 2, "mid", 100, [101, 99]),
			ev(0, 12, 2, "ft", 90, [103, 99]),
		]);
		for (let seed = 0; seed < 8; seed++) {
			const text = tell(game, seed).join(" ");
			assert.notMatch(
				text,
				/sealed it|close it out|finished it|put it away/,
				text,
			);
		}
	});

	// "Tyrese Nowell set up the winner." on its own, after a sentence about
	// something else: the winner was the lede's, and so is its assist.
	test("an assist on a winner the lede told is not a sentence of its own", () => {
		const game = build([
			ev(1, 21, 2, "rim", 24.9, [101, 101]),
			ev(0, 11, 3, "tp", 1.8, [104, 101], { ast: 12 }),
		]);
		for (let seed = 0; seed < 8; seed++) {
			const text = tell(game, seed, true).join(" ");
			assert.notMatch(text, /Tyrese Nowell/, text);
		}
	});
});
