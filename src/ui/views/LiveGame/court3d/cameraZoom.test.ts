import { assert, describe, test } from "vitest";
import {
	CAM_HOLD,
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

	// The play is at a basket: zoomed in on a half-court set, the basket it
	// attacks is in the picture with the ball - not off its edge.
	test("in the half court the basket stays in the picture with the ball", () => {
		for (const zoom of [1.25, 1.5, 1.8, 2]) {
			for (const [bx, rim] of [
				[66, COURT_W - 5.25],
				[74, COURT_W - 5.25],
				[84, COURT_W - 5.25],
				[28, 5.25],
				[18, 5.25],
			] as const) {
				for (const by of [8, 25, 42]) {
					const z = zoomedOn(wide(bx), zoom, fit, { x: bx, y: by }, 25, rim);
					const where = `zoom ${zoom} ball ${bx},${by}`;
					// (Wherever both fit across the picture.)
					if (Math.abs(bx - rim) + 6 > z.width - 6) {
						continue;
					}
					assert.isAbove(rim, z.x - z.width / 2 + 2, where);
					assert.isBelow(rim, z.x + z.width / 2 - 2, where);
					assert.isAbove(bx, z.x - z.width / 2 + 2, where);
					assert.isBelow(bx, z.x + z.width / 2 - 2, where);
				}
			}
		}
		// (Up the floor, a long way from either: on the ball, as before.)
		const mid = zoomedOn(
			wide(47),
			2,
			fit,
			{ x: 47, y: 25 },
			25,
			COURT_W - 5.25,
		);
		assert.closeTo(mid.x, 47, 1e-9);
	});

	// Framed the way a broadcast frames a set: the basket well inside the
	// picture - its board and the baseline behind it in it too - not hard up
	// against the edge with its stand cut off, nor its board off the top;
	// and the ball in it with it, the shot gone wider for the two of them if
	// it has to.
	test("a half-court set has the basket well inside the picture, not at its edge", () => {
		for (const [aspect, W, H] of [
			[4 / 3, 1336, 1000],
			[16 / 9, 1600, 900],
		] as const) {
			const f = courtFit(aspect);
			const shotAt = (x: number): Shot => ({
				x,
				width: Math.max(72, f.min),
				y: f.y(Math.max(72, f.min)),
			});
			for (const zoom of [1.25, 1.5, 2, 2.5]) {
				for (const right of [true, false]) {
					const rim = right ? COURT_W - 5.25 : 5.25;
					for (const out of [6, 14, 22, 26, 30]) {
						for (const by of [10, 25, 40]) {
							const bx = rim + (right ? -out : out);
							const z = zoomedOn(
								shotAt(bx),
								zoom,
								f,
								{ x: bx, y: by },
								25,
								rim,
							);
							const cam = makeCamera(z, W, H);
							const at = (x: number, y: number, h: number) => {
								const s = project(cam, { x, y, z: h }).x / W;
								return right ? s : 1 - s;
							};
							const where = `${aspect.toFixed(2)} zoom ${zoom} ball ${bx},${by}`;
							assert.isAtMost(at(rim, 25, 10), 0.85, where);
							assert.isAtLeast(at(rim, 25, 10), 0.55, where);
							assert.isBelow(at(right ? COURT_W : 0, 25, 0), 0.97, where);
							assert.isAbove(at(bx, by, 4), 0.04, where);
							// And up and down: the top of the board in it - not
							// tilted off it toward the near side - and the man
							// with the ball.
							const down = (x: number, y: number, h: number) =>
								project(cam, { x, y, z: h }).y / H;
							assert.isAbove(
								down(rim + (right ? 1.25 : -1.25), 25, 13),
								0.04,
								where,
							);
							assert.isBelow(down(bx, by, 0), 0.97, where);
						}
					}
				}
			}
		}
	});

	// Coming up the floor the man on the camera goes with the play, ahead of
	// it - not held on the basket behind the ball, which is the one nearest
	// it till he is over half court.
	test("coming up the floor, it goes with the play - not the basket behind it", () => {
		const attack = COURT_W - 5.25;
		for (const zoom of [1.5, 2, 2.5]) {
			for (const bx of [12, 20, 30, 40]) {
				// (Ahead of the ball, as the main camera has it - see aimFor.)
				const z = zoomedOn(
					wide(bx + 6),
					zoom,
					fit,
					{ x: bx, y: 25 },
					25,
					attack,
				);
				assert.isAtLeast(z.x, bx, `zoom ${zoom} ball ${bx}`);
			}
		}
	});

	// And into the set a little at a time: no swing from one end to the
	// other as the ball crosses half court, or anywhere on its way.
	test("into the set a little at a time - no swing as the ball crosses half court", () => {
		for (const [attack, from, to] of [
			[COURT_W - 5.25, 20, 84],
			[5.25, 74, 10],
		] as const) {
			for (const zoom of [1.5, 2, 2.5]) {
				let was: number | undefined;
				const step = from < to ? 0.25 : -0.25;
				for (let bx = from; (to - bx) * step > 0; bx += step) {
					const z = zoomedOn(wide(bx), zoom, fit, { x: bx, y: 25 }, 25, attack);
					if (was !== undefined) {
						assert.isBelow(Math.abs(z.x - was), 1.5, `zoom ${zoom} ball ${bx}`);
					}
					was = z.x;
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

	// Holding the shot: the play's little moves - a crossover, a jab, a man
	// shifting his feet - leave the camera be; it pans once the play goes on
	// somewhere.
	test("the operator holds his shot through the play's little moves", () => {
		const op: Operator = {
			camX: 70,
			camW: 40,
			camY: fit.y(40),
			camZ: 2,
			camVX: 0,
			camVW: 0,
			camVY: 0,
			camVZ: 0,
		};
		const hold = 40 * CAM_HOLD;
		let most = 0;
		for (let i = 0; i < 600; i++) {
			const x = 70 + 1.4 * Math.sin(i / 9);
			operate(
				op,
				{ x, width: 40, y: op.camY, loose: true },
				fit,
				1 / 60,
				false,
				hold,
			);
			most = Math.max(most, Math.abs(op.camX - 70));
		}
		assert.isBelow(most, 0.05);
		// Gone on along the floor: after it, to within the hold of it.
		for (let i = 0; i < 300; i++) {
			operate(
				op,
				{ x: 82, width: 40, y: op.camY, loose: true },
				fit,
				1 / 60,
				false,
				hold,
			);
		}
		assert.closeTo(op.camX, 82 - hold, 0.05);
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
