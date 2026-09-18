import { assert, describe, test } from "vitest";

import {
	bounceBackScore as bounceBack,
	mipScore as mip,
} from "./processAwards.ts";

describe("bounce back scoring", () => {
	test("rewards returning to an earlier peak after a down year", () => {
		// Was a 20, fell to an 8, back to a 19
		assert.isAbove(bounceBack(19, 8, 20), 0);
	});

	test("ignores a steady star who never fell off", () => {
		assert.strictEqual(bounceBack(20, 20, 20), 0);
	});

	test("does not reward a career year, which is MIP's job", () => {
		// Never better than a 10, suddenly a 20
		const breakout = { current: 20, prev: 10, max: 10 };

		assert.strictEqual(
			bounceBack(breakout.current, breakout.prev, breakout.max),
			0,
		);
		assert.isAbove(mip(breakout.current, breakout.prev, breakout.max), 0);
	});

	test("prefers the bigger fall when the comeback is the same", () => {
		assert.isAbove(bounceBack(19, 5, 20), bounceBack(19, 12, 20));
	});

	test("prefers the fuller recovery when the fall is the same", () => {
		assert.isAbove(bounceBack(19, 8, 20), bounceBack(13, 8, 20));
	});

	test("a partial recovery is capped by how good he is now, not his old peak", () => {
		// Same old peak and same down year, but one is only halfway back
		assert.strictEqual(bounceBack(14, 8, 25), 6);
		assert.strictEqual(bounceBack(14, 8, 20), 6);
	});

	test("ranks the opposite of MIP for a comeback vs a breakout", () => {
		const comeback = { current: 19, prev: 8, max: 20 };
		const breakout = { current: 19, prev: 8, max: 9 };

		assert.isAbove(
			bounceBack(comeback.current, comeback.prev, comeback.max),
			bounceBack(breakout.current, breakout.prev, breakout.max),
		);
		assert.isAbove(
			mip(breakout.current, breakout.prev, breakout.max),
			mip(comeback.current, comeback.prev, comeback.max),
		);
	});
});
