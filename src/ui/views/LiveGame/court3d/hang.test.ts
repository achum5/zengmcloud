import { assert, describe, test } from "vitest";
import { HANG_Z, poseOf, type PlayerState } from "./evaluate.ts";
import { type AnimName, bodyOf, poseAt, skeleton } from "./poses.ts";

// UP IN THE AIR HIS LEGS HANG LOOSE UNDER HIM (see Pose.curlN): his heels
// drawn up, his hips - and so his hands, and the ball - where they were.

const body = bodyOf();
const man = (anim: AnimName, phase: number, z: number) =>
	({
		pid: 1,
		team: 0,
		shown: true,
		x: 47,
		y: 25,
		z,
		yaw: 0,
		anim,
		phase,
		moving: false,
		holding: false,
	}) as PlayerState;

describe("3D: legs hanging in the air", () => {
	test("heels drawn up lift his feet and leave the rest of him be", () => {
		const q = poseAt("stance", 0);
		const down = skeleton(body, q);
		const up = skeleton(body, { ...q, curlN: 30, curlF: 30, raiseN: 10 });
		for (const part of ["pelvis", "chest", "head"] as const) {
			assert.deepEqual(up[part], down[part], part);
		}
		assert.deepEqual(up.armR.end, down.armR.end);
		assert.deepEqual(up.armL.end, down.armL.end);
		assert.deepEqual(up.legR.root, down.legR.root);
		assert.isAbove(up.legR.end.u, down.legR.end.u + 0.05);
		assert.isAbove(up.legL.end.u, down.legL.end.u + 0.05);
		// (Behind him: his heels, not his knees out in front.)
		assert.isBelow(up.legL.end.f, down.legL.end.f);
	});

	test("a jump shot's knees are soft at the top, straight off the floor and onto it", () => {
		const top = poseOf(man("shoot", 0.55, 2));
		assert.isAbove(top.curlN, 10);
		assert.isAbove(top.curlF, 10);
		for (const phase of [0, 0.2, 0.28, 0.87, 0.95, 1]) {
			const q = poseOf(man("shoot", phase, 2));
			assert.strictEqual(q.curlN, 0, `${phase}`);
			assert.strictEqual(q.curlF, 0, `${phase}`);
		}
	});

	test("and the ball goes up from just where it did", () => {
		for (const anim of ["shoot", "shootKick", "shootSplit", "dunk"] as const) {
			for (const phase of [0.4, 0.55, 0.7]) {
				const q = poseOf(man(anim, phase, 2));
				const hung = skeleton(body, q);
				const straight = skeleton(body, {
					...q,
					curlN: 0,
					curlF: 0,
					raiseN: 0,
					raiseF: 0,
				});
				assert.deepEqual(hung.armR.end, straight.armR.end, anim);
				assert.deepEqual(hung.pelvis, straight.pelvis, anim);
			}
		}
	});

	test("nobody's feet come up while he stands on the floor", () => {
		for (const anim of [
			"shoot",
			"dunk",
			"tomahawk",
			"rebound",
			"board",
			"block",
			"contest",
			"tip",
		] as const) {
			let most = 0;
			for (let phase = 0; phase <= 1; phase += 0.05) {
				const floor = poseOf(man(anim, phase, 0));
				assert.strictEqual(floor.curlN, 0, anim);
				assert.strictEqual(floor.curlF, 0, anim);
				assert.strictEqual(floor.raiseN, 0, anim);
				const up = poseOf(man(anim, phase, 2));
				most = Math.max(most, up.curlN, up.curlF);
				// Coming off the floor, a part of it.
				const low = poseOf(man(anim, phase, HANG_Z / 2));
				assert.closeTo(low.curlN, up.curlN / 2, 1e-9, anim);
			}
			assert.isAbove(most, 10, anim);
		}
	});

	test("his picture is told how far up he is, made standing on the floor", () => {
		const up = poseOf(man("dunk", 0.55, 2));
		assert.deepEqual(poseOf({ ...man("dunk", 0.55, 0), aloft: 1 }), up);
		assert.strictEqual(poseOf({ ...man("dunk", 0.55, 2), aloft: 0 }).curlN, 0);
	});
});
