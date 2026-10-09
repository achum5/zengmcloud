import { assert, describe, test } from "vitest";
import { BALL_R, moveBallAt } from "./evaluate.ts";
import {
	ANIMS,
	bodyOf,
	gripOf,
	holdBall,
	MOVE_BALL,
	moveAnim,
	moveFloor,
	poseAt,
	skeleton,
	underPalm,
	type AnimName,
	type Hand,
	type V3,
} from "./poses.ts";

const mix = (a: V3, b: V3, u: number): V3 => ({
	f: a.f + (b.f - a.f) * u,
	s: a.s + (b.s - a.s) * u,
	u: a.u + (b.u - a.u) * u,
});
// How far a point is from the segment a-b.
const away = (p: V3, a: V3, b: V3): number => {
	const d = { f: b.f - a.f, s: b.s - a.s, u: b.u - a.u };
	const l2 = d.f * d.f + d.s * d.s + d.u * d.u;
	const t =
		l2 > 0
			? Math.max(
					0,
					Math.min(
						1,
						((p.f - a.f) * d.f + (p.s - a.s) * d.s + (p.u - a.u) * d.u) / l2,
					),
				)
			: 0;
	const q = mix(a, b, t);
	return Math.hypot(p.f - q.f, p.s - q.s, p.u - q.u);
};

describe("3D poses", () => {
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

	test("a held ball is in his hands, whatever the move", () => {
		const body = bodyOf(80, 230);
		for (const anim of Object.keys(ANIMS) as AnimName[]) {
			for (let k = 0; k <= 12; k++) {
				const { sk, ball } = holdBall(body, poseAt(anim, k / 12), anim);
				const d = (h: { f: number; s: number; u: number }) =>
					Math.hypot(h.f - ball.f, h.s - ball.s, h.u - ball.u);
				// The shooting hand under it, or palming it - and in a two-hand
				// hold the other hand on its side too. (Up on a jumper the guide
				// hand only gets as close as a cartoon's short arm reaches.)
				assert.isBelow(d(sk.armR.end), 0.75, `${anim} ${k}`);
				if (gripOf(anim) === "two") {
					assert.isBelow(d(sk.armL.end), 0.75, `${anim} ${k}`);
				}
				// Out in front of him or up over his head, not in his chest.
				assert.isTrue(
					ball.f > sk.chest.f + 0.2 || ball.u > sk.chest.u + 0.5,
					`${anim} ${k}`,
				);
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

	// Each move's hands are placed round where it lets the ball go and where
	// the ball bounces, so its path goes round him - across in front of his
	// knees, through the gap between his legs, behind his seat - for a small
	// guard and a big man alike, either hand.
	test("a dribble move's ball goes round his legs, never through them", () => {
		for (const [hgt, weight] of [
			[70, 175],
			[76, 200],
			[84, 255],
		] as const) {
			const body = bodyOf(hgt, weight);
			for (const move of ["front", "legs", "back"] as const) {
				for (const from of ["R", "L"] as const) {
					const to: Hand = from === "R" ? "L" : "R";
					const anim = moveAnim(move, from);
					const under = (at: number, hand: Hand) => {
						const sk = skeleton(body, poseAt(anim, at));
						return underPalm(hand === "R" ? sk.armR : sk.armL);
					};
					const floor = { ...moveFloor(body, move, to, true), u: BALL_R };
					for (let k = 0; k <= 50; k++) {
						const ph = k / 50;
						const ball = moveBallAt(
							ph,
							MOVE_BALL[move].letGo,
							floor,
							under,
							from,
							to,
						);
						const sk = skeleton(body, poseAt(anim, ph));
						const pel = sk.pelvis;
						// His shorts' legs, knees, shins and shoes, hips and seat - a
						// little thicker than sculpt.ts makes them.
						const parts: [V3, V3, number][] = [
							[
								{ ...pel, s: -body.hipW },
								{ ...pel, s: body.hipW },
								body.thighR * 1.32,
							],
							[
								{
									...pel,
									f: pel.f - 0.02,
									s: -body.hipW * 0.55,
									u: pel.u + 0.17,
								},
								{
									...pel,
									f: pel.f - 0.02,
									s: body.hipW * 0.55,
									u: pel.u + 0.17,
								},
								body.thighR * 1.2,
							],
						];
						for (const leg of [sk.legR, sk.legL]) {
							const tip = leg.tip ?? leg.end;
							parts.push(
								[
									mix(leg.root, leg.mid, 0.16),
									mix(leg.root, leg.mid, 0.84),
									body.thighR * 1.4,
								],
								[leg.mid, leg.mid, body.kneeR],
								[mix(leg.mid, leg.end, 0.1), leg.end, body.calfR * 0.95],
								[
									mix(leg.end, tip, -0.32),
									mix(leg.end, tip, 1.04),
									body.ankleR * 1.35,
								],
							);
						}
						for (const [a, b, r] of parts) {
							assert.isAbove(
								away(ball, a, b) - r - BALL_R,
								-0.08,
								`${hgt}in ${move} from ${from} at ${ph}`,
							);
						}
					}
				}
			}
		}
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
