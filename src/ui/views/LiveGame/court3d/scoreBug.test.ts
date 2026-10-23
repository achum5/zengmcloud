import { assert, describe, test } from "vitest";
import { buildClocks, gameClockAt, shotClockAt } from "./clock.ts";
import {
	breakText,
	buildFouls,
	finalText,
	foulsAt,
	isBreak,
	isFinal,
	SHOT_LOW,
	shotClockText,
} from "./scoreBug.ts";
import { compile } from "./testGame.ts";

// Lines at one-second steps, in order: a beat for each.
const run = (events: any[], limits?: number[]) => {
	const beats = events.map((_, i) => ({
		i,
		type: events[i].type,
		preStart: i * 1000,
		actionStart: i * 1000,
		end: i * 1000 + 500,
	}));
	return buildFouls({ beats }, events, limits);
};

describe("team fouls on the score bug", () => {
	test("counted per team, starting over each period", () => {
		const marks = run([
			{ type: "period", period: 1 },
			{ type: "pfNonShooting", t: 0, clock: 600 },
			{ type: "pfFG", t: 1, clock: 500 },
			{ type: "pfTP", t: 0, clock: 400 },
			{ type: "period", period: 2 },
			{ type: "pfBonus", t: 1, clock: 700 },
		]);
		assert.deepStrictEqual(foulsAt(marks, 3500).fouls, [2, 1]);
		assert.deepStrictEqual(foulsAt(marks, 4000).fouls, [0, 0]);
		assert.deepStrictEqual(foulsAt(marks, 5000).fouls, [0, 1]);
		// Before anything at all.
		assert.deepStrictEqual(foulsAt(marks, -1).fouls, [0, 0]);
	});

	test("a team is in the bonus once the other team reaches the limit", () => {
		const events: any[] = [{ type: "period", period: 1 }];
		for (let k = 0; k < 5; k++) {
			events.push({ type: "pfNonShooting", t: 1, clock: 600 - k * 10 });
		}
		const marks = run(events);
		assert.deepStrictEqual(foulsAt(marks, 4000).bonus, [false, false]);
		// The visitors' fifth: the home team shoots.
		assert.deepStrictEqual(foulsAt(marks, 5000).bonus, [true, false]);
	});

	test("the lower limit in the last two minutes, and the overtime limit", () => {
		const late = run([
			{ type: "period", period: 1 },
			{ type: "pfNonShooting", t: 0, clock: 100 },
			{ type: "pfNonShooting", t: 0, clock: 50 },
		]);
		assert.deepStrictEqual(foulsAt(late, 2000).bonus, [false, true]);
		const ot = run([
			{ type: "overtime", period: 5 },
			...Array.from({ length: 4 }, (_, k) => ({
				type: "pfNonShooting",
				t: 0,
				clock: 290 - k * 10,
			})),
		]);
		assert.deepStrictEqual(foulsAt(ot, 4000).bonus, [false, true]);
	});

	test("a whole game adds up to the box score's fouls", () => {
		const { events, tl } = compile("a", 80);
		const marks = buildFouls(tl, events);
		// Every foul line lands in the running count of its period.
		const pf = events.filter(
			(e) => /^pf/.test(e.type) && (e.t === 0 || e.t === 1),
		).length;
		assert.isAbove(pf, 0);
		let counted = 0;
		let prev: [number, number] = [0, 0];
		for (const m of marks) {
			const d = m.fouls[0] + m.fouls[1] - (prev[0] + prev[1]);
			if (d > 0) {
				counted += d;
			}
			prev = m.fouls;
		}
		assert.strictEqual(counted, pf);
	});
});

describe("the final horn on the score bug", () => {
	// Through the play, the period and the clock; from the final horn on,
	// FINAL where the clock was - FINAL/OT after overtime.
	test("FINAL from the horn, not before", () => {
		const { tl } = compile("a", 150);
		const k = tl.beats.findIndex((b) => b.type === "gameOver");
		assert.isAbove(k, 0);
		// (The horn: the end of the last period, just before the line that
		// says the game is over.)
		const horn = tl.beats[k - 1]!;
		assert.strictEqual(horn.type, "endOfPeriod");
		assert.isFalse(isFinal(tl, horn.actionStart - 1));
		assert.isFalse(isFinal(tl, tl.beats[0]!.actionStart));
		assert.isTrue(isFinal(tl, horn.actionStart));
		assert.isTrue(isFinal(tl, tl.end));
		assert.strictEqual(finalText("Q4"), "FINAL");
		assert.strictEqual(finalText("H2"), "FINAL");
		assert.strictEqual(finalText("OT"), "FINAL/OT");
		assert.strictEqual(finalText("3OT"), "FINAL/3OT");
	}, 60_000);
});

describe("between periods on the score bug", () => {
	// From the horn that ends a period until the next one starts, END Q1 -
	// HALF at halftime - not the period just ended and 0.0; not after the
	// last one (FINAL), and never while one is being played.
	test("END Q1, HALF, END Q3 - from the horn to the next period", () => {
		const { tl } = compile("a", 150);
		const ends = tl.beats.filter((b) => b.type === "endOfPeriod");
		const starts = tl.beats.filter(
			(b) => b.type === "period" || b.type === "overtime",
		);
		assert.isAtLeast(ends.length, 4);
		for (const [k, end] of ends.entries()) {
			const next = starts.find((b) => b.actionStart > end.actionStart);
			assert.isFalse(isBreak(tl, end.actionStart - 1), `${k}`);
			if (!next) {
				// (The last: FINAL.)
				assert.isFalse(isBreak(tl, end.actionStart), `${k}`);
				assert.isTrue(isFinal(tl, end.actionStart), `${k}`);
				continue;
			}
			assert.isTrue(isBreak(tl, end.actionStart), `${k}`);
			assert.isTrue(isBreak(tl, next.actionStart - 1), `${k}`);
			assert.isFalse(isBreak(tl, next.actionStart), `${k}`);
		}
		assert.strictEqual(breakText("Q1", 4), "END Q1");
		assert.strictEqual(breakText("Q2", 4), "HALF");
		assert.strictEqual(breakText("Q3", 4), "END Q3");
		assert.strictEqual(breakText("Q4", 4), "END Q4");
		assert.strictEqual(breakText("H1", 2), "HALF");
		assert.strictEqual(breakText("OT", 4), "END OT");
	}, 60_000);
});

describe("the shot clock on the score bug", () => {
	// A new period starts with a full shot clock. The lines that open one
	// carry it as the last period left it (the sim restarts it after them):
	// the quarter came up with the last one's 14 on it until the inbound.
	test("a full 24 to start a period, not what the last one left", () => {
		const events: any[] = [
			{ type: "jumpBall", t: 0, clock: 720, shotClock: 24 },
			{ type: "fgaTp", t: 0, clock: 1.2, shotClock: 14.3 },
			{ type: "missTp", t: 0, clock: 0, shotClock: 14.3 },
			{ type: "endOfPeriod", t: 0, clock: 0, shotClock: 14.3 },
			{ type: "period", period: 2, clock: 720, shotClock: 14.3 },
			{ type: "sub", t: 1, clock: 720, shotClock: 14.3 },
			{ type: "fgaAtRim", t: 1, clock: 710.7, shotClock: 14.7 },
		];
		const tl = {
			beats: events.map((e, i) => ({
				i,
				type: e.type,
				preStart: i * 1000,
				actionStart: i * 1000 + 600,
				end: i * 1000 + 900,
			})),
			poss: [],
			fast: [],
			cuts: [],
			ball: [],
		};
		const clocks = buildClocks(tl as never, events);
		// (Off over the break.)
		assert.isUndefined(shotClockAt(clocks, 3600));
		for (const k of [4, 5]) {
			assert.strictEqual(gameClockAt(clocks, k * 1000 + 600), 720);
			assert.strictEqual(shotClockAt(clocks, k * 1000 + 600), 24, `${k}`);
		}
		// And on from there as the sim has it.
		assert.closeTo(shotClockAt(clocks, 6600)!, 14.7, 0.01);
	});

	// Whole seconds as it counts down, never 0 while there is time on it;
	// in its last five, tenths.
	test("tenths in its last five seconds", () => {
		assert.strictEqual(shotClockText(24), "24");
		assert.strictEqual(shotClockText(14.2), "15");
		assert.strictEqual(shotClockText(5), "5");
		assert.strictEqual(shotClockText(4.96), "5");
		assert.strictEqual(shotClockText(4.9), "4.9");
		assert.strictEqual(shotClockText(4.31), "4.4");
		assert.strictEqual(shotClockText(0.04), "0.1");
		assert.strictEqual(shotClockText(0), "0.0");
		assert.isBelow(Number(shotClockText(4.31)), SHOT_LOW);
	});
});
