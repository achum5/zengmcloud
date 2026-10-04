import { assert, describe, test } from "vitest";
import { ANIMS, bodyOf, poseFor, skeleton, type AnimName } from "./poses.ts";

describe("retro poses", () => {
	// A sprite is anchored at its feet and jumps are added on top, so every
	// frame of every animation has to stand on the floor - otherwise a body
	// would sink through the hardwood or hover between steps.
	test("every frame stands on the floor, hands where a hand can be", () => {
		for (const [hgt, weight] of [
			[70, 170],
			[78, 215],
			[88, 290],
		] as const) {
			const body = bodyOf(hgt, weight);
			for (const anim of Object.keys(ANIMS) as AnimName[]) {
				for (let frame = 0; frame < ANIMS[anim].n; frame++) {
					const sk = skeleton(body, poseFor(anim, frame));
					const sole = Math.min(sk.legN.ankle.y, sk.legF.ankle.y) - 2;
					assert.closeTo(sole, 0, 1e-9, `${anim} ${frame}`);
					for (const hand of [sk.armN.hand, sk.armF.hand]) {
						assert.isTrue(Number.isFinite(hand.x) && Number.isFinite(hand.y));
						assert.isBelow(Math.abs(hand.x), body.H, `${anim} ${frame}`);
						assert.isBelow(hand.y, body.H * 1.35, `${anim} ${frame}`);
					}
				}
			}
		}
	});

	test("a seven-footer stands taller than a six-footer", () => {
		const tall = skeleton(bodyOf(84), poseFor("ready", 0));
		const short = skeleton(bodyOf(72), poseFor("ready", 0));
		assert.isAbove(tall.headC.y, short.headC.y);
	});
});
