import { assert, describe, test } from "vitest";
import { courtLineStrips } from "./courtLines.ts";
import { COURT_H, COURT_W } from "./geometry.ts";
import { RIM_INSET } from "../courtSpots.ts";

describe("3D court lines", () => {
	test("every line is on the floor", () => {
		for (const strip of courtLineStrips()) {
			for (const p of strip.pts) {
				assert.isAtLeast(p.x, -1e-9);
				assert.isAtMost(p.x, COURT_W + 1e-9);
				assert.isAtLeast(p.y, -1e-9);
				assert.isAtMost(p.y, COURT_H + 1e-9);
			}
		}
	});

	// The line the reports were about: whole, at both ends - straight down
	// each corner 22 feet from the basket, round the arc at 23.75.
	test("both three-point lines, corners and arc", () => {
		const threes = courtLineStrips().filter(
			(s) =>
				s.pts.some(
					(p) => Math.abs(p.y - (COURT_H / 2 - 22)) < 1e-9 && p.x < 1e-9,
				) ||
				s.pts.some(
					(p) =>
						Math.abs(p.y - (COURT_H / 2 - 22)) < 1e-9 && p.x > COURT_W - 1e-9,
				),
		);
		assert.strictEqual(threes.length, 2);
		for (const strip of threes) {
			const left = strip.pts[0]!.x < COURT_W / 2;
			const rim = { x: left ? RIM_INSET : COURT_W - RIM_INSET, y: COURT_H / 2 };
			const ends = [strip.pts[0]!, strip.pts.at(-1)!];
			for (const p of ends) {
				assert.closeTo(Math.abs(p.y - rim.y), 22, 1e-9);
			}
			const arc = strip.pts.slice(1, -1);
			assert.isAbove(arc.length, 20);
			for (const p of arc) {
				assert.closeTo(Math.hypot(p.x - rim.x, p.y - rim.y), 23.75, 1e-9);
			}
		}
	});
});
