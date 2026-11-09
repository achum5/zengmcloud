import { assert, describe, test } from "vitest";
import {
	callCoverage,
	dropTo,
	isZone,
	zoneSpots,
	type Coverage,
} from "./coverage.ts";

// The field across in the offense's frame: the ball on the middle of it.
const MID = 0;
const HALF = 80;
// A nickel defense as it lines up: the corners out wide, the nickel over
// the slot, the linebackers in the box, the safeties deep over the hashes.
const NICKEL = [
	{ m: "CB-L", role: "CB", at: { d: 18, a: -60 } },
	{ m: "CB-R", role: "CB", at: { d: 18, a: 60 } },
	{ m: "NB", role: "NB", at: { d: 15, a: 30 } },
	{ m: "MIKE", role: "MIKE", at: { d: 15, a: -5 } },
	{ m: "WILL", role: "WILL", at: { d: 15, a: -18 } },
	{ m: "FS", role: "FS", at: { d: 39, a: -20 } },
	{ m: "SS", role: "SS", at: { d: 39, a: 20 } },
];
const counter = () => {
	let k = 0;
	return () => (k += 0.37) % 1;
};
const dropsIn = (c: Coverage) =>
	dropTo(NICKEL, zoneSpots(c, MID, HALF, counter()), MID);

describe("coverage", () => {
	test("pressed, the corners are on their men: man", () => {
		for (let u = 0; u < 1; u += 0.05) {
			for (const twoHigh of [false, true]) {
				const c = callCoverage({
					twoHigh,
					pressed: true,
					down: 1,
					toGo: 10,
					toGoal: 60,
					u,
				});
				assert.isFalse(isZone(c), c);
			}
		}
	});

	test("third and long shows two deep and plays the deep zones, mostly", () => {
		let zone = 0;
		let n = 0;
		for (let u = 0; u < 1; u += 0.01) {
			n += 1;
			const c = callCoverage({
				twoHigh: true,
				pressed: false,
				down: 3,
				toGo: 12,
				toGoal: 60,
				u,
			});
			if (isZone(c)) {
				zone += 1;
			}
		}
		assert.isAbove(zone / n, 0.8);
	});

	test("Cover 3: the corners and the free safety a deep third each, four under", () => {
		const d = dropsIn("c3");
		assert.isAbove(d.get("CB-L")!.d, 30);
		assert.isBelow(d.get("CB-L")!.a, -30);
		assert.isAbove(d.get("CB-R")!.a, 30);
		// (One safety to the deep middle, the other down under.)
		const deepS = ["FS", "SS"].filter((s) => d.get(s)!.d >= 40);
		assert.lengthOf(deepS, 1);
		assert.closeTo(d.get(deepS[0]!)!.a, MID, 1);
		for (const m of ["NB", "MIKE", "WILL"]) {
			assert.isBelow(d.get(m)!.d, 35, m);
		}
	});

	test("Cover 2: the safeties a deep half each, the corners sat in the flats", () => {
		const d = dropsIn("c2");
		for (const s of ["FS", "SS"]) {
			assert.isAbove(d.get(s)!.d, 35, s);
		}
		assert.isBelow(d.get("FS")!.a * d.get("SS")!.a, 0);
		for (const c of ["CB-L", "CB-R"]) {
			assert.isBelow(d.get(c)!.d, 21, c);
		}
	});

	test("Cover 4: the corners and the safeties a deep quarter each", () => {
		const d = dropsIn("c4");
		for (const m of ["CB-L", "CB-R", "FS", "SS"]) {
			assert.isAbove(d.get(m)!.d, 30, m);
		}
		const across = ["CB-L", "FS", "SS", "CB-R"]
			.map((m) => d.get(m)!.a)
			.sort((a, b) => a - b);
		// (Four of them, across the field.)
		assert.isBelow(across[0]!, -40);
		assert.isAbove(across[3]!, 40);
	});
});
