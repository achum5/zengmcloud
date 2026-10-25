import { assert, describe, test } from "vitest";
import {
	BALL_R,
	holdOf,
	moveBallAt,
	poseOf,
	type PlayerState,
} from "./evaluate.ts";
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
	// Nobody runs quite like anybody else - and each man the same way every
	// time: his own lean, swing of his arms, bend at the elbows; and down in
	// his stance, sat right down in it or playing it taller. Not with the
	// ball.
	test("each man runs his own way, and always the same way", () => {
		const at = (pid: number, anim: AnimName) =>
			poseOf({
				pid,
				team: 0,
				shown: true,
				x: 0,
				y: 0,
				z: 0,
				yaw: 0,
				anim,
				phase: 0.3,
				moving: true,
			} as PlayerState);
		const leans = [];
		const swings = [];
		const knees = [];
		for (let pid = 1; pid <= 20; pid++) {
			const q = at(pid, "run");
			assert.deepEqual(at(pid, "run"), q);
			leans.push(q.lean);
			swings.push(q.shN - q.shF);
			knees.push(at(pid, "stance").kneeN);
			assert.deepEqual(at(pid, "dribble"), at(pid + 100, "dribble"));
		}
		assert.isAbove(Math.max(...leans) - Math.min(...leans), 5);
		assert.isAbove(Math.max(...knees) - Math.min(...knees), 8);
		assert.isAbove(
			Math.max(...swings) / Math.min(...swings.filter((x) => x > 0)),
			1.3,
		);
	});

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
				// Out in front of him, up over his head, out wide of him or
				// round behind his back - not in his chest.
				assert.isTrue(
					ball.f > sk.chest.f + 0.2 ||
						ball.u > sk.chest.u + 0.5 ||
						Math.abs(ball.s - sk.chest.s) > 1.1 ||
						ball.f < sk.chest.f - 0.9,
					`${anim} ${k}`,
				);
			}
		}
	});

	// Held in both hands and straight into a move that has it all in one
	// hand from the start - a windmill out of a rebound, a pass pushed out of
	// a hold - the ball goes over from the one hold to the other as he eases
	// out of the first: not across into the one hand in the frame the move
	// starts.
	test("the ball goes over from one hold to the next, not across in a frame", () => {
		const body = bodyOf();
		const base = {
			pid: 1,
			team: 0,
			shown: true,
			x: 0,
			y: 0,
			z: 0,
			yaw: 0,
			moving: false,
			holding: true,
		} as const;
		const gap = (a: V3, b: V3) => Math.hypot(a.f - b.f, a.s - b.s, a.u - b.u);
		let pairs = 0;
		for (const was of ["hold", "board", "triple"] as AnimName[]) {
			for (const anim of Object.keys(ANIMS) as AnimName[]) {
				const g = gripOf(anim);
				if (g !== "push" && g !== "pull" && g !== "cup") {
					continue;
				}
				pairs += 1;
				const at = (w: number) => {
					const st: PlayerState = {
						...base,
						anim,
						phase: 0,
						from: { anim: was, phase: 0.5, w, held: true },
					};
					return holdOf(st, body, poseOf(st)).ball;
				};
				const before: PlayerState = { ...base, anim: was, phase: 0.5 };
				const after: PlayerState = { ...base, anim, phase: 0 };
				// Where he had it as the move began - and, all the way into it,
				// where it holds it.
				assert.isBelow(
					gap(at(1), holdOf(before, body, poseOf(before)).ball),
					0.01,
					`${was} > ${anim}`,
				);
				assert.isBelow(
					gap(at(0.001), holdOf(after, body, poseOf(after)).ball),
					0.02,
					`${was} > ${anim}`,
				);
			}
		}
		assert.isAbove(pairs, 8);
	});

	// Hands on his hips, arms folded, bent over with his hands on his knees:
	// each hand where it goes - not held out wide in the air, as an arm that
	// only bends in the plane it swings out in would put it.
	test("hands on hips, arms folded, hands on knees", () => {
		const b = bodyOf();
		const gap = (a: V3, c: V3) => Math.hypot(a.f - c.f, a.s - c.s, a.u - c.u);
		const hips = skeleton(b, poseAt("hips", 0));
		for (const [arm, side] of [
			[hips.armR, -1],
			[hips.armL, 1],
		] as const) {
			// The hand at his waist, by his side - the elbow out wide of it.
			assert.isBelow(Math.abs(arm.end.u - hips.pelvis.u - 0.34), 0.2);
			assert.isBelow(Math.abs(side * arm.end.s - b.hipW - 0.26), 0.2);
			assert.isAbove(side * arm.mid.s, side * arm.end.s + 0.25);
		}
		const folded = skeleton(b, poseAt("crossed", 0));
		for (const [arm, side] of [
			[folded.armR, -1],
			[folded.armL, 1],
		] as const) {
			// Across in front of his chest, over at the other arm.
			assert.isBelow(side * arm.end.s, 0);
			assert.isAbove(arm.end.f, folded.chest.f + 0.2);
			assert.isAbove(arm.end.u, folded.pelvis.u + 0.6);
		}
		// A flex: fists up past his shoulders, elbows out wide of them.
		const flex = skeleton(b, poseAt("flex", 0.5));
		for (const [arm, side] of [
			[flex.armR, -1],
			[flex.armL, 1],
		] as const) {
			assert.isAbove(arm.end.u, arm.root.u + 0.3);
			assert.isAbove(side * arm.mid.s, side * arm.end.s + 0.2);
			assert.isBelow(Math.abs(arm.mid.u - arm.root.u), 0.45);
		}
		const bent = skeleton(b, poseAt("crouch", 0));
		assert.isBelow(gap(bent.armR.end, bent.legR.mid), 0.5);
		assert.isBelow(gap(bent.armL.end, bent.legL.mid), 0.5);
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
							MOVE_BALL[move].down,
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
