import { assert, describe, test } from "vitest";
import { wrapBend } from "./sculpt.ts";

// A finger laid round a ball, a bone at a time (see wrapBend): in the
// finger's plane, x along it from the knuckle, y toward the palm.
const lay = (
	bones: number[],
	cu: number,
	cv: number,
	rho: number,
	rest = [0.2, 0.35, 0.25],
) => {
	const lo = [-0.3, 0, -0.15];
	const hi = [1.6, 1.9, 1.3];
	let x = 0;
	let y = 0;
	let phi = 0;
	const ends: [number, number][] = [];
	bones.forEach((l, k) => {
		phi += wrapBend(x, y, phi, l, cu, cv, rho, lo[k]!, hi[k]!, rest[k]!);
		x += l * Math.cos(phi);
		y += l * Math.sin(phi);
		ends.push([x, y]);
	});
	return ends;
};

describe("fingers on the ball", () => {
	test("each bone of a finger is laid on the ball, not into it or off it", () => {
		// A ball 0.4 ft across (its cut through the finger's plane), its
		// middle under the knuckle, the knuckle just off it.
		const cu = 0.02;
		const cv = 0.45;
		const rho = 0.42;
		const ends = lay([0.08, 0.05, 0.04], cu, cv, rho);
		for (const [x, y] of ends) {
			assert.closeTo(Math.hypot(x - cu, y - cv), rho, 1e-6);
		}
	});

	test("a ball out of a finger's reach leaves it as it rests", () => {
		const ends = lay([0.08, 0.05, 0.04], 0, 3, 0.4);
		const rested = lay([0.08, 0.05, 0.04], 0, 0, 0);
		assert.deepEqual(ends, rested);
	});

	test("a finger already into the ball is bent back onto it, a little", () => {
		// Its knuckle on the ball, heading in: bent back till the bone lies
		// on it again.
		const a = wrapBend(0, 0, 0.6, 0.06, 0, 0.42, 0.42, 0, 1.9, 0.35);
		assert.isBelow(a, 0);
		assert.isAtLeast(a, -0.35);
		const x = 0.06 * Math.cos(0.6 + a);
		const y = 0.06 * Math.sin(0.6 + a);
		if (a > -0.35) {
			assert.closeTo(Math.hypot(x, y - 0.42), 0.42, 1e-6);
		}
	});
});
