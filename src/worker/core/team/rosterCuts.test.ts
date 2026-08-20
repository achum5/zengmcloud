import { assert, describe, test } from "vitest";
import {
	cutOrder,
	keepScore,
	positionCounts,
	SCARCE_AT_POSITION,
	type CutCandidate,
} from "./rosterCuts.ts";

const p = (o: Partial<CutCandidate> = {}): CutCandidate => ({
	pid: 1,
	value: 45,
	age: 26,
	pos: "G",
	...o,
});

// A roster deep enough that nobody is protected by scarcity, so the age lean
// is what the test is actually looking at.
const deepRoster = (extra: CutCandidate[]) => [
	...extra,
	...Array.from({ length: 4 }, (_, i) => p({ pid: 100 + i, pos: "G" })),
	...Array.from({ length: 4 }, (_, i) => p({ pid: 200 + i, pos: "F" })),
	...Array.from({ length: 4 }, (_, i) => p({ pid: 300 + i, pos: "C" })),
];

describe("who goes first", () => {
	// THE BUG THIS EXISTS FOR. A project's current value is low by definition -
	// that is what makes him a project - so the team whose whole plan is young
	// players released the youngest one it had.
	test("a rebuild keeps the young player over the veteran", () => {
		const kid = p({ pid: 1, age: 20, value: 42, pos: "F" });
		const vet = p({ pid: 2, age: 33, value: 44, pos: "F" });
		const order = cutOrder(deepRoster([kid, vet]), "teardown");
		assert.strictEqual(order[0]!.pid, 2, "the veteran should go first");
	});

	test("a contender keeps the veteran over the project", () => {
		const kid = p({ pid: 1, age: 20, value: 44, pos: "F" });
		const vet = p({ pid: 2, age: 31, value: 42, pos: "F" });
		const order = cutOrder(deepRoster([kid, vet]), "allIn");
		assert.strictEqual(order[0]!.pid, 1, "the project should go first");
	});

	// It decides between comparable players; it does not keep a bad one.
	test("a clearly worse player still goes first, whatever his age", () => {
		const scrub = p({ pid: 1, age: 21, value: 20, pos: "F" });
		const good = p({ pid: 2, age: 33, value: 60, pos: "F" });
		for (const tier of ["teardown", "allIn"] as const) {
			assert.strictEqual(
				cutOrder(deepRoster([scrub, good]), tier)[0]!.pid,
				1,
				tier,
			);
		}
	});
});

describe("the last player at a position", () => {
	test("counting is by bucket, not by exact position", () => {
		const counts = positionCounts([{ pos: "PG" }, { pos: "SG" }, { pos: "C" }]);
		assert.strictEqual(counts.get("G"), 2);
		assert.strictEqual(counts.get("C"), 1);
		assert.strictEqual(counts.get("F"), undefined);
	});

	// Team overall punishes a roster that cannot field a centre, and nothing in
	// the basketball path counted positions at all.
	test("the only centre survives a marginally better guard", () => {
		const roster = [
			p({ pid: 1, pos: "C", value: 40 }),
			...Array.from({ length: 8 }, (_, i) =>
				p({ pid: 100 + i, pos: "G", value: 41 }),
			),
		];
		const order = cutOrder(roster, "fringe");
		assert.notStrictEqual(order[0]!.pid, 1);
	});

	test("protection runs out once there are enough of them", () => {
		const counts = new Map([["C" as const, SCARCE_AT_POSITION + 1]]);
		const thin = new Map([["C" as const, SCARCE_AT_POSITION]]);
		const player = p({ pos: "C" });
		assert.isAbove(
			keepScore({ p: player, tier: "fringe", counts: thin }),
			keepScore({ p: player, tier: "fringe", counts }),
		);
	});

	test("it cannot save a replacement-level body", () => {
		const roster = [
			p({ pid: 1, pos: "C", value: 10 }),
			...Array.from({ length: 8 }, (_, i) =>
				p({ pid: 100 + i, pos: "G", value: 45 }),
			),
		];
		assert.strictEqual(cutOrder(roster, "fringe")[0]!.pid, 1);
	});
});

describe("leagues without the smart front office", () => {
	// Turning the setting off has to give back exactly the old behaviour:
	// lowest value first, nothing else consulted.
	test("no posture means the ordering is raw value", () => {
		const roster = [
			p({ pid: 1, value: 50, age: 20, pos: "C" }),
			p({ pid: 2, value: 30, age: 34, pos: "G" }),
			p({ pid: 3, value: 40, age: 27, pos: "F" }),
		];
		assert.deepStrictEqual(
			cutOrder(roster, undefined).map((x) => x.pid),
			[2, 3, 1],
		);
	});
});

describe("it never produces a nonsense order", () => {
	test("a bad value is not a way to survive a cut", () => {
		const roster = [
			p({ pid: 1, value: Number.NaN }),
			p({ pid: 2, value: 40 }),
			p({ pid: 3, value: 50 }),
		];
		assert.strictEqual(cutOrder(roster, "fringe")[0]!.pid, 1);
		assert.strictEqual(cutOrder(roster, undefined)[0]!.pid, 1);
	});

	// Two devices in a shared league that ordered an identical roster
	// differently would release different players and diverge.
	test("identical players break their tie the same way everywhere", () => {
		const roster = [p({ pid: 7 }), p({ pid: 3 }), p({ pid: 5 })];
		assert.deepStrictEqual(
			cutOrder(roster, "fringe").map((x) => x.pid),
			[3, 5, 7],
		);
	});

	test("everyone is accounted for", () => {
		const roster = Array.from({ length: 15 }, (_, i) =>
			p({ pid: i, value: 30 + i, age: 19 + (i % 15) }),
		);
		for (const tier of ["teardown", "fringe", "allIn", undefined] as const) {
			assert.strictEqual(cutOrder(roster, tier).length, 15);
			assert.strictEqual(
				new Set(cutOrder(roster, tier).map((x) => x.pid)).size,
				15,
			);
		}
	});
});
