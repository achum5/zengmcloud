import { assert, describe, test } from "vitest";
import { RIM_R, RIM_Z, rimX, type Pt3, type Side } from "./geometry.ts";
import {
	findShot,
	kindOf,
	playAt,
	playShot,
	SAMPLE_MS,
	type ShotKind,
} from "./physics.ts";

// The same numbers every run.
const seeded = (seed: number) => () => {
	seed = (Math.imul(seed, 1103515245) + 12345) | 0;
	return ((seed >>> 8) & 0xffffff) / 0x1000000;
};

// Spots round the floor at the left rim (display team 0 attacks it): a
// three, jumpers and floaters, a hook, a layup off the glass.
const spots: { from: Pt3; flight: [number, number] }[] = [
	{ from: { x: 5.25 + 23.5, y: 25, z: 8.2 }, flight: [1000, 1150] },
	{ from: { x: 12, y: 3.5, z: 8.2 }, flight: [980, 1120] },
	{ from: { x: 18, y: 31, z: 7.9 }, flight: [740, 860] },
	{ from: { x: 10, y: 17, z: 7.9 }, flight: [640, 760] },
	{ from: { x: 9, y: 28, z: 7.2 }, flight: [520, 620] },
	{ from: { x: 6.2, y: 22.4, z: 8.5 }, flight: [380, 560] },
];

const through = (pts: number[], side: Side) => {
	for (let i = 0; i < pts.length; i += 3) {
		const rho = Math.hypot(pts[i]! - rimX(side), pts[i + 1]! - 25);
		if (pts[i + 2]! < RIM_Z - 0.5 && rho < RIM_R - 0.12) {
			return true;
		}
	}
	return false;
};

describe("3D ball at the rim", () => {
	test("dropped through the middle of the hoop, it goes in clean - slowed by the net, down to the floor", () => {
		const p = playShot(
			0,
			{ x: rimX(0), y: 25, z: RIM_Z + 1 },
			{ x: 0, y: 0, z: -8 },
		);
		assert.isTrue(p.made);
		assert.strictEqual(kindOf(p), "swish");
		assert.isBelow(p.end.p.z, 0.5);
		// Through the net it comes down slower than it would have fallen.
		const free = Math.sqrt(8 * 8 + 2 * 32.2 * (RIM_Z + 1 - 0.39));
		assert.isBelow(-p.end.v.z, free - 2);
		assert.isTrue(through(p.pts, 0));
	});

	test("short, it comes back off the front of the rim", () => {
		const p = playShot(
			0,
			{ x: rimX(0) + 2.5, y: 25, z: RIM_Z + 1.5 },
			{ x: -7, y: 0, z: -4 },
		);
		assert.isFalse(p.made);
		assert.isAbove(p.touches.length, 0);
		assert.isTrue(p.touches[0]!.rim);
		// Back the way it came.
		assert.isAbove(p.end.v.x, 0);
		assert.isFalse(through(p.pts, 0));
	});

	test("thrown at the glass, it comes off it", () => {
		const p = playShot(
			0,
			{ x: rimX(0) + 3, y: 25, z: RIM_Z + 2.5 },
			{ x: -14, y: 0, z: 0 },
		);
		const glass = p.touches.find((h) => !h.rim);
		assert.isDefined(glass);
		assert.closeTo(glass!.at.x, rimX(0) - 1.25, 0.01);
	});

	test("the same shot plays out the same way every time", () => {
		const a = findShot(
			0,
			spots[2]!.from,
			spots[2]!.flight,
			{ made: false },
			seeded(7),
		);
		const b = findShot(
			0,
			spots[2]!.from,
			spots[2]!.flight,
			{ made: false },
			seeded(7),
		);
		assert.isDefined(a);
		assert.deepEqual(a!.play.pts, b!.play.pts);
		assert.deepEqual(a!.aim, b!.aim);
	});

	test("a shot is found going the way the sim says it went - and looking it", () => {
		const wanted: { made: boolean; kinds: ShotKind[] }[] = [
			{ made: true, kinds: ["swish"] },
			{ made: true, kinds: ["rim"] },
			{ made: false, kinds: ["rimOut", "rollOut"] },
			{ made: false, kinds: ["brick"] },
			{ made: false, kinds: ["off", "rimOut", "brick"] },
		];
		let right = 0;
		let all = 0;
		spots.forEach(({ from, flight }, s) => {
			wanted.forEach((want, w) => {
				for (let k = 0; k < 4; k++) {
					const f = findShot(
						0,
						from,
						flight,
						want,
						seeded(s * 97 + w * 13 + k),
					);
					assert.isDefined(f, `spot ${s} ${JSON.stringify(want)}`);
					const p = f!.play;
					assert.strictEqual(p.made, want.made);
					assert.strictEqual(through(p.pts, 0), want.made);
					// A miss is off the iron or the glass, not an air ball.
					if (!want.made) {
						assert.isAbove(p.touches.length, 0);
					}
					// Written down on the beat, from where it was handed over.
					assert.strictEqual(p.pts.length % 3, 0);
					assert.closeTo((p.pts.length / 3 - 1) * SAMPLE_MS, p.end.t, 1e-6);
					assert.deepEqual(playAt(p.pts, 0), f!.p);
					right += want.kinds.includes(f!.kind) ? 1 : 0;
					all += 1;
				}
			});
		});
		assert.isAbove(right / all, 0.85);
	});

	test("off the glass from the side, a layup banks in", () => {
		let banks = 0;
		for (let k = 0; k < 20; k++) {
			const f = findShot(
				0,
				spots[5]!.from,
				spots[5]!.flight,
				{ made: true, kinds: ["bank"] },
				seeded(500 + k),
			);
			banks += f?.kind === "bank" ? 1 : 0;
		}
		assert.isAbove(banks, 12);
	});

	test("a miss comes off toward the man who gets the rebound", () => {
		let near = 0;
		let all = 0;
		for (const toward of [
			{ x: 11, y: 18 },
			{ x: 11, y: 32 },
			{ x: 4, y: 31 },
			{ x: 4, y: 19 },
		]) {
			for (let k = 0; k < 10; k++) {
				const f = findShot(
					0,
					spots[k % 4]!.from,
					spots[k % 4]!.flight,
					{ made: false, toward },
					seeded(900 + k),
				)!;
				// Where it comes down into a rebounder's reach.
				const e = f.play.end;
				const drop = Math.max(0, e.p.z - 7.4);
				const tau = (e.v.z + Math.sqrt(e.v.z ** 2 + 2 * 32.2 * drop)) / 32.2;
				const at = { x: e.p.x + e.v.x * tau, y: e.p.y + e.v.y * tau };
				// Nearer him than the spot across the rim from him.
				const mirror = { x: 2 * rimX(0) - toward.x, y: 50 - toward.y };
				near +=
					Math.hypot(at.x - toward.x, at.y - toward.y) <
					Math.hypot(at.x - mirror.x, at.y - mirror.y)
						? 1
						: 0;
				all += 1;
			}
		}
		assert.isAbove(near / all, 0.8);
	});
});
