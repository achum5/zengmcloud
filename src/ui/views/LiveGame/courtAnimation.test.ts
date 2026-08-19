import { assert, describe, test } from "vitest";
import { nextSpinDeg, rimReaction, SPIN_DEG_PER_FT } from "./courtAnimation.ts";

describe("nextSpinDeg", () => {
	test("a ball that has not moved has not turned", () => {
		assert.strictEqual(nextSpinDeg(40, { x: 10, y: 10 }, { x: 10, y: 10 }), 40);
	});

	test("turns in proportion to the distance covered", () => {
		const one = nextSpinDeg(0, { x: 0, y: 0 }, { x: 1, y: 0 });
		assert.strictEqual(one, SPIN_DEG_PER_FT);
		// Two feet is twice the turn of one.
		assert.strictEqual(nextSpinDeg(0, { x: 0, y: 0 }, { x: 2, y: 0 }), one * 2);
	});

	test("a ball going the other way turns the other way", () => {
		assert.ok(nextSpinDeg(0, { x: 10, y: 0 }, { x: 8, y: 0 }) < 0);
	});

	test("distance is the real distance, not just the x part", () => {
		// A 3-4-5 triangle: five feet of travel, however it is split.
		assert.strictEqual(
			nextSpinDeg(0, { x: 0, y: 0 }, { x: 3, y: 4 }),
			5 * SPIN_DEG_PER_FT,
		);
	});

	// A shot travels ~35 ft/s; at 60fps that is ~0.58ft a frame. If the turn per
	// frame ever approached half a revolution the seams would alias and the ball
	// would read as vibrating rather than spinning - which is what a literal
	// roll rate did.
	test("does not spin fast enough to strobe at 60fps", () => {
		const perFrame = nextSpinDeg(0, { x: 0, y: 0 }, { x: 0.58, y: 0 });
		assert.ok(perFrame < 180, `${perFrame} deg per frame`);
	});
});

describe("rimReaction", () => {
	const frames = (made: boolean) =>
		Array.from({ length: 21 }, (_, i) => rimReaction(made, i / 20));

	test("nothing happens before or after the reaction", () => {
		for (const made of [true, false]) {
			assert.strictEqual(rimReaction(made, 1).opacity, 0);
			assert.strictEqual(rimReaction(made, 1.5).opacity, 0);
			assert.strictEqual(rimReaction(made, Number.NaN).opacity, 0);
		}
	});

	// The one thing this whole reaction exists for: you must be able to tell a
	// make from a miss at a glance, without reading the text.
	test("only a make ever swells the rim", () => {
		assert.ok(Math.max(...frames(true).map((f) => f.scale)) > 1.3);
		assert.ok(Math.max(...frames(false).map((f) => f.scale)) <= 1);
	});

	test("only a miss ever moves the rim off its spot", () => {
		assert.ok(frames(true).every((f) => f.dx === 0 && f.dy === 0));
		assert.ok(Math.max(...frames(false).map((f) => Math.abs(f.dx))) > 0.8);
	});

	test("a make is always the brighter of the two", () => {
		const made = frames(true);
		const missed = frames(false);
		for (const [i, m] of made.entries()) {
			assert.ok(
				m.opacity >= missed[i]!.opacity,
				`frame ${i}: make ${m.opacity} < miss ${missed[i]!.opacity}`,
			);
		}
	});

	test("both fade out rather than snapping off", () => {
		for (const made of [true, false]) {
			const f = frames(made);
			assert.ok(f[0]!.opacity > 0.3);
			assert.ok(f.at(-2)!.opacity < 0.1);
		}
	});

	// The clang is front-loaded - iron stops ringing quickly.
	test("the rattle is mostly over in the first third", () => {
		const f = frames(false);
		const early = Math.max(...f.slice(0, 7).map((x) => Math.abs(x.dx)));
		const late = Math.max(...f.slice(14).map((x) => Math.abs(x.dx)));
		assert.ok(late < early * 0.25, `early ${early} late ${late}`);
	});
});
