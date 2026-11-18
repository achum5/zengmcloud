import { assert, test } from "vitest";
import {
	lineupsOf,
	onOffOf,
	poss,
	stintsOf,
	type Seg,
	type Stints,
	type Tot,
} from "./stints.ts";

const tot = (x: Partial<Tot>): Tot => ({
	pts: 0,
	fg: 0,
	fga: 0,
	tp: 0,
	tpa: 0,
	ft: 0,
	fta: 0,
	orb: 0,
	drb: 0,
	ast: 0,
	tov: 0,
	stl: 0,
	blk: 0,
	...x,
});
const seg = (
	period: number,
	start: number,
	end: number,
	on: number[],
	own: Partial<Tot>,
	opp: Partial<Tot>,
): Seg => ({
	period,
	start,
	end,
	on: [on, [90, 91, 92, 93, 94]],
	d: [tot(own), tot(opp)],
});

// One team's game: 1-5 start, 6 in for 5 at 300 s, 5 back at the start of
// the 2nd period.
const stints: Stints = {
	tids: [7, 8],
	segs: [
		seg(1, 0, 300, [1, 2, 3, 4, 5], { pts: 10, fga: 8 }, { pts: 6, fga: 8 }),
		seg(1, 300, 720, [1, 2, 3, 4, 6], { pts: 8, fga: 9 }, { pts: 12, fga: 10 }),
		seg(
			2,
			720,
			1440,
			[1, 2, 3, 4, 5],
			{ pts: 20, fga: 15 },
			{ pts: 18, fga: 16 },
		),
	],
	periods: [
		[0, 720],
		[720, 1440],
	],
	end: 1440,
};

test("possessions", () => {
	assert.closeTo(poss(tot({ fga: 10, fta: 5, orb: 2, tov: 3 })), 13.2, 1e-9);
});

test("a five's time and points add up over its stretches", () => {
	const fives = lineupsOf(stints, 0, 5);
	assert.strictEqual(fives.length, 2);
	assert.deepStrictEqual(fives[0]!.pids, [1, 2, 3, 4, 5]);
	assert.strictEqual(fives[0]!.secs, 300 + 720);
	assert.strictEqual(fives[0]!.ptsFor, 30);
	assert.strictEqual(fives[0]!.ptsAgainst, 24);
	// Four of them were on the floor the whole game.
	const fours = lineupsOf(stints, 0, 4);
	assert.deepStrictEqual(fours[0]!.pids, [1, 2, 3, 4]);
	assert.strictEqual(fours[0]!.secs, 1440);
});

test("on and off cover the whole game", () => {
	const { on, off } = onOffOf(stints, 0, 5);
	assert.strictEqual(on.secs + off.secs, 1440);
	assert.strictEqual(off.secs, 420);
	assert.strictEqual(on.own.pts, 30);
	assert.strictEqual(off.opp.pts, 12);
});

test("a man's stints join only within a period", () => {
	// Player 1 never sat, but the two periods stay two bars.
	const one = stintsOf(stints, 0, 1);
	assert.strictEqual(one.length, 2);
	assert.strictEqual(one[0]!.end, 720);
	assert.strictEqual(one[0]!.margin, 10 - 6 + 8 - 12);
	// Player 6 played once.
	assert.deepStrictEqual(
		stintsOf(stints, 0, 6).map((s) => [s.start, s.end]),
		[[300, 720]],
	);
});
