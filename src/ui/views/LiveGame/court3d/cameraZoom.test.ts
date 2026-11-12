import { assert, describe, test } from "vitest";
import {
	courtFit,
	makeCamera,
	operate,
	project,
	zoomedOn,
	type Operator,
	type Shot,
} from "./camera.ts";
import { CAMERA_ZOOM, parseCamera } from "./cameraSetting.ts";
import { COURT_H, COURT_W } from "./geometry.ts";

// THE CAMERA IN CLOSER, as whoever is watching has asked (see zoomedOn and
// cameraSetting.ts).

const fit = courtFit(16 / 9);
// What the camera shows of a half-court set, the whole floor's depth in it.
const wide = (x: number): Shot => ({
	x,
	width: Math.max(72, fit.min),
	y: fit.y(Math.max(72, fit.min)),
});

describe("3D camera zoom", () => {
	test("as it always was, unless asked", () => {
		const shot = wide(70);
		assert.strictEqual(zoomedOn(shot, 1, fit, { x: 70, y: 25 }, 25), shot);
		assert.strictEqual(parseCamera(undefined).zoom, 1);
		assert.strictEqual(parseCamera({ zoom: "2" }).zoom, 1);
		assert.strictEqual(parseCamera({ zoom: 99 }).zoom, CAMERA_ZOOM.max);
		assert.strictEqual(parseCamera({ zoom: 0.2 }).zoom, CAMERA_ZOOM.min);
	});

	test("in closer: that many times less of the floor across the picture", () => {
		const shot = wide(70);
		const z = zoomedOn(shot, 1.8, fit, { x: 70, y: 25 }, 25);
		assert.closeTo(z.width, shot.width / 1.8, 1e-9);
		assert.isTrue(z.loose);
		// (Never so close that a man fills the picture.)
		assert.isAtLeast(zoomedOn(shot, 50, fit, { x: 70, y: 25 }, 25).width, 26);
	});

	test("the ball stays in the picture, along the floor and across it", () => {
		for (const zoom of [1.25, 1.5, 2, 2.5]) {
			for (const bx of [50, 62, 74, 86, 93]) {
				for (const by of [1, 12, 25, 38, 49]) {
					const z = zoomedOn(wide(70), zoom, fit, { x: bx, y: by }, 25);
					const cam = makeCamera(z, 1600, 900);
					const p = project(cam, { x: bx, y: by, z: 4 });
					const where = `zoom ${zoom} ball ${bx},${by}`;
					assert.isAbove(p.x, 0, where);
					assert.isBelow(p.x, 1600, where);
					assert.isAbove(p.y, 0, where);
					assert.isBelow(p.y, 900, where);
				}
			}
		}
	});

	test("it looks across the floor toward the play", () => {
		const far = zoomedOn(wide(70), 2, fit, { x: 70, y: 6 }, 14);
		const mid = zoomedOn(wide(70), 2, fit, { x: 70, y: 25 }, 25);
		const near = zoomedOn(wide(70), 2, fit, { x: 70, y: 44 }, 36);
		assert.isBelow(far.y, mid.y - 3);
		assert.isAbove(near.y, mid.y + 3);
		// (And never off the end of the floor.)
		for (const bx of [0, 2, COURT_W - 2, COURT_W]) {
			const z = zoomedOn(wide(bx), 2.5, fit, { x: bx, y: COURT_H / 2 }, 25);
			assert.isAtLeast(z.x - z.width / 2, -12 - 1e-9);
			assert.isAtMost(z.x + z.width / 2, COURT_W + 12 + 1e-9);
		}
	});

	test("a shot already in close is left as it is", () => {
		const ft: Shot = { x: 80, width: fit.min * 0.6, y: fit.y(fit.min * 0.6) };
		assert.strictEqual(zoomedOn(ft, 2, fit, { x: 80, y: 25 }, 25), ft);
	});

	test("the operator goes where a closer shot looks, across the floor too", () => {
		const op: Operator = {
			camX: 70,
			camW: fit.min,
			camY: fit.y(fit.min),
			camZ: 2,
			camVX: 0,
			camVW: 0,
			camVY: 0,
			camVZ: 0,
		};
		const aim = zoomedOn(wide(70), 2, fit, { x: 70, y: 6 }, 14);
		for (let i = 0; i < 240; i++) {
			operate(op, aim, fit, 1 / 60, false);
		}
		assert.closeTo(op.camW, aim.width, 0.3);
		assert.closeTo(op.camY, aim.y, 0.3);
		// Back out to the whole floor: across it where the whole floor has it.
		const back = wide(70);
		for (let i = 0; i < 240; i++) {
			operate(op, back, fit, 1 / 60, false);
		}
		assert.closeTo(op.camW, back.width, 0.3);
		assert.closeTo(op.camY, fit.y(op.camW), 1e-6);
	});
});
