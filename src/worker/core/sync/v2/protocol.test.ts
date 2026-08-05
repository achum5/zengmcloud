import { assert, describe, test } from "vitest";
import {
	catchUpPlan,
	decideApply,
	versionsToFetch,
	type RoomVersionState,
} from "./protocol.ts";

// The whole v2 protocol is these pure rules. Every incident the v1 engine
// produced maps to one of these cases deciding differently than v1 did.

describe("decideApply: the one admission rule", () => {
	test("the next version applies", () => {
		assert.deepStrictEqual(decideApply(41, 42), { type: "apply" });
	});

	test("version 1 applies onto a fresh device at 0", () => {
		assert.deepStrictEqual(decideApply(0, 1), { type: "apply" });
	});

	test("an echo or retry is a duplicate, never a re-apply", () => {
		assert.deepStrictEqual(decideApply(42, 42), { type: "duplicate" });
		assert.deepStrictEqual(decideApply(42, 17), { type: "duplicate" });
	});

	// THE RULE THAT MAKES THE MISSED-DAY FORK UNREPRESENTABLE. v1 applied
	// day 12 over a skipped day 11 because nothing tied "may I apply this?"
	// to "do I have everything before it?". Here that IS the rule.
	test("a version past the next one is a GAP, never an apply", () => {
		assert.deepStrictEqual(decideApply(41, 43), {
			type: "gap",
			missingFrom: 42,
			missingThrough: 42,
		});
		assert.deepStrictEqual(decideApply(10, 50), {
			type: "gap",
			missingFrom: 11,
			missingThrough: 49,
		});
	});
});

describe("versionsToFetch", () => {
	test("walks from just past applied to the head, inclusive", () => {
		assert.deepStrictEqual(versionsToFetch(41, 44), [42, 43, 44]);
	});

	test("caught up means nothing to fetch", () => {
		assert.deepStrictEqual(versionsToFetch(44, 44), []);
		assert.deepStrictEqual(versionsToFetch(45, 44), []);
	});
});

describe("catchUpPlan", () => {
	const room = (
		version: number,
		checkpointVersion?: number,
	): RoomVersionState => ({
		version,
		authorId: "a",
		byName: "Alex",
		at: 1,
		checkpointVersion,
	});

	test("at the head: caught up", () => {
		assert.deepStrictEqual(catchUpPlan(50, room(50, 30)), {
			type: "caught-up",
		});
	});

	test("slightly behind, above the checkpoint: deltas only", () => {
		assert.deepStrictEqual(catchUpPlan(48, room(50, 30)), {
			type: "deltas",
			versions: [49, 50],
		});
	});

	// The v1 equivalent of this case is the whole recovery ladder: windowed
	// replays, era sorting, batch rescue. Here it is one branch.
	test("behind the checkpoint: restore it, then walk the tail", () => {
		assert.deepStrictEqual(catchUpPlan(10, room(50, 30)), {
			type: "checkpoint-then-deltas",
			checkpointVersion: 30,
			versions: [
				31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48,
				49, 50,
			],
		});
	});

	test("a fresh device with no checkpoint yet walks the chain from the start", () => {
		assert.deepStrictEqual(catchUpPlan(0, room(3)), {
			type: "deltas",
			versions: [1, 2, 3],
		});
	});

	test("exactly at the checkpoint: deltas only, no restore", () => {
		assert.deepStrictEqual(catchUpPlan(30, room(50, 30)), {
			type: "deltas",
			versions: versionsToFetch(30, 50),
		});
	});
});
