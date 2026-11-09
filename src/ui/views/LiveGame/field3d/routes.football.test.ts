import { assert, describe, test } from "vitest";
import {
	conceptRoutes,
	SIDE_CONCEPTS,
	type RouteName,
	type Runner,
} from "./routes.ts";

// A play's random numbers, the same every run.
const seeded = (seed: number) => {
	let s = seed >>> 0;
	return () => {
		s = (Math.imul(s, 1664525) + 1013904223) >>> 0;
		return s / 4294967296;
	};
};

// Two out to the right - the Z wide, a slot inside him - an X alone on the
// left, and a back behind the quarterback.
const RUNNERS: Runner[] = [
	{ side: 1, out: 36, spot: "wide" },
	{ side: 1, out: 14, spot: "slot" },
	{ side: -1, out: 36, spot: "wide" },
	{ side: 0, out: 3, spot: "back" },
];
// The routes of a side's concepts, by how many run them.
const conceptsOf = (n: number) =>
	SIDE_CONCEPTS[n]!.map((c) => c.legs.map(([name]) => name).join(","));

describe("route concepts", () => {
	test("the ball's man's side runs a concept drawn up with his route in it", () => {
		const thrown: [number, RouteName][] = [
			[0, "hitch"],
			[1, "corner"],
			[0, "slant"],
			[1, "flat"],
			[0, "dig"],
			[1, "seam"],
			[0, "go"],
			[1, "out"],
			[0, "curl"],
			[0, "post"],
		];
		for (const [i, name] of thrown) {
			for (let seed = 1; seed <= 20; seed++) {
				const called = conceptRoutes(RUNNERS, i, name, seeded(seed));
				assert.isUndefined(called[i], `${name}: his own`);
				const both = [0, 1].map((k) => (k === i ? name : called[k]!.name));
				assert.include(
					conceptsOf(2),
					both.join(","),
					`${name} by the ${i === 0 ? "wide" : "slot"} man`,
				);
			}
		}
	});

	test("a side the ball does not go to runs one of its own, a back his own underneath", () => {
		const backs = new Set(["checkdown", "flat", "swing", "wheel"]);
		for (let seed = 1; seed <= 50; seed++) {
			const called = conceptRoutes(RUNNERS, 0, "hitch", seeded(seed));
			assert.include(conceptsOf(1), called[2]!.name);
			assert.isTrue(backs.has(called[3]!.name), called[3]!.name);
			// (As deep as the concept has it: feet, off the line.)
			for (const c of called) {
				if (c) {
					assert.isAtMost(c.depth, 30 * 3);
				}
			}
		}
	});

	test("trips: three to a side run the three of a concept", () => {
		const trips: Runner[] = [
			{ side: 1, out: 40, spot: "wide" },
			{ side: 1, out: 26, spot: "slot" },
			{ side: 1, out: 12, spot: "slot" },
			{ side: -1, out: 36, spot: "wide" },
		];
		for (let seed = 1; seed <= 50; seed++) {
			const called = conceptRoutes(trips, -1, undefined, seeded(seed));
			assert.include(
				conceptsOf(3),
				[0, 1, 2].map((k) => called[k]!.name).join(","),
			);
		}
	});
});
