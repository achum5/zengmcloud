import { assert, describe, test } from "vitest";
import { banked, bankFrom } from "./evaluate.ts";
import { bodyOf, poseAt, skeleton } from "./poses.ts";

// A MAN COMING ROUND A CORNER AT SPEED LEANS INTO IT (see bankFrom).

// Round a circle `r` feet across at `v` feet a second, its middle on his
// left (`side` 1) or his right (-1). (His left is (sin, -cos) of the way he
// goes.)
const round = (v: number, r: number, side: 1 | -1) => (t: number) => {
	const th = (-side * v * (t / 1000)) / r;
	return {
		x: 47 - side * r * Math.sin(th),
		y: 25 + side * r * Math.cos(th),
	};
};

describe("3D: leaning into a turn", () => {
	test("round to his left he leans left, to his right, right", () => {
		const left = bankFrom(round(15, 7, 1), 5000);
		const right = bankFrom(round(15, 7, -1), 5000);
		assert.isAbove(left, 0.8);
		assert.closeTo(right, -left, 1e-6);
	});

	test("the faster and the tighter, the further - to all a man can", () => {
		const wide = bankFrom(round(15, 30, 1), 5000);
		const tight = bankFrom(round(15, 12, 1), 5000);
		const slower = bankFrom(round(9, 12, 1), 5000);
		assert.isAbove(wide, 0);
		assert.isAbove(tight, wide);
		assert.isAbove(tight, slower);
		assert.isAtMost(bankFrom(round(18, 3, 1), 5000), 1);
	});

	test("not on a straight run, a long bend, or stepping round", () => {
		const straight = (t: number) => ({ x: 10 + 0.015 * t, y: 25 });
		assert.strictEqual(bankFrom(straight, 5000), 0);
		assert.strictEqual(bankFrom(round(12, 80, 1), 5000), 0);
		assert.strictEqual(bankFrom(round(3, 2, 1), 5000), 0);
		assert.strictEqual(
			bankFrom(() => ({ x: 47, y: 25 }), 5000),
			0,
		);
	});

	test("leaning left, his left shoulder is the lower", () => {
		const b = bodyOf();
		const up = skeleton(b, poseAt("run", 0.2));
		const over = skeleton(b, banked(poseAt("run", 0.2), 1));
		const drop = (sk: typeof up) => sk.armR.root.u - sk.armL.root.u;
		assert.isAbove(drop(over), drop(up) + 0.15);
		const back = skeleton(b, banked(poseAt("run", 0.2), -1));
		assert.isBelow(drop(back), drop(up) - 0.15);
		// (His feet are where they were: he leans over them.)
		assert.closeTo(over.legR.end.u, up.legR.end.u, 1e-9);
		assert.closeTo(over.legL.end.s, up.legL.end.s, 1e-9);
	});
});
