import { assert, describe, test } from "vitest";
import { ANIMS, bodyOf, poseAt, skeleton, type AnimName } from "./poses.ts";

describe("2.5D poses", () => {
	// A body is anchored at its feet and jumps are added on top, so every
	// moment of every animation has to stand on the floor - otherwise a body
	// would sink through the hardwood or hover between steps.
	test("every pose stands on the floor, hands where a hand can be", () => {
		for (const [hgt, weight] of [
			[70, 170],
			[78, 215],
			[88, 290],
		] as const) {
			const body = bodyOf(hgt, weight);
			for (const anim of Object.keys(ANIMS) as AnimName[]) {
				for (let k = 0; k <= 12; k++) {
					const sk = skeleton(body, poseAt(anim, k / 12));
					const sole = Math.min(sk.legR.end.u, sk.legL.end.u);
					assert.closeTo(sole, body.ankleH, 1e-9, `${anim} ${k}`);
					for (const hand of [sk.armR.end, sk.armL.end]) {
						for (const v of [hand.f, hand.s, hand.u]) {
							assert.isTrue(Number.isFinite(v), `${anim} ${k}`);
						}
						assert.isBelow(Math.abs(hand.f), body.H, `${anim} ${k}`);
						assert.isBelow(Math.abs(hand.s), body.H * 0.6, `${anim} ${k}`);
						assert.isBelow(hand.u, body.H * 1.35, `${anim} ${k}`);
					}
					// The head is on top of the shoulders.
					assert.isAbove(sk.head.u, sk.chest.u, `${anim} ${k}`);
				}
			}
		}
	});

	test("a seven-footer stands taller than a six-footer", () => {
		const tall = skeleton(bodyOf(84), poseAt("ready", 0));
		const short = skeleton(bodyOf(72), poseAt("ready", 0));
		assert.isAbove(tall.head.u, short.head.u);
		// And about as tall as he is.
		const top = tall.head.u + bodyOf(84).headR;
		assert.closeTo(top, 7, 0.35);
	});

	test("poses blend smoothly - no joint jumps between nearby moments", () => {
		const body = bodyOf();
		for (const anim of Object.keys(ANIMS) as AnimName[]) {
			let prev = skeleton(body, poseAt(anim, 0));
			for (let k = 1; k <= 200; k++) {
				const sk = skeleton(body, poseAt(anim, k / 200));
				const d = Math.hypot(
					sk.armR.end.f - prev.armR.end.f,
					sk.armR.end.u - prev.armR.end.u,
				);
				assert.isBelow(d, 0.45, `${anim} ${k}`);
				prev = sk;
			}
		}
	});
});
