import { assert, describe, test } from "vitest";
import { holdPublishForSim, resolveStaleAdvancePlan } from "./publishGuards.ts";

// The night a simmed day went missing, in test form. A follower published a
// trading card during a ten-second sim, the sim lost the compare-and-swap, the
// advance was discarded, and there was no checkpoint to snap back to - so the
// games stayed on one device and nowhere else. Each of these covers one of the
// three things that had to be true for that to happen.

describe("holdPublishForSim", () => {
	test("a follower waits out a sim", () => {
		assert.strictEqual(
			holdPublishForSim({ isAuthority: false, roomBusy: true }),
			true,
		);
	});

	test("a follower publishes freely when no sim is running", () => {
		assert.strictEqual(
			holdPublishForSim({ isAuthority: false, roomBusy: false }),
			false,
		);
	});

	// The device doing the simming is the one whose advance the lease exists to
	// protect. Holding it would deadlock the room against itself.
	test("the simmer never holds", () => {
		assert.strictEqual(
			holdPublishForSim({ isAuthority: true, roomBusy: true }),
			false,
		);
	});
});

describe("resolveStaleAdvancePlan", () => {
	// THE INCIDENT. A trading card took the version; nothing a played day reads
	// or writes moved, so the day is still good and belongs on top.
	test("rebases over a version that touched only inert stores", () => {
		const plan = resolveStaleAdvancePlan({
			applied: 621,
			roomVersion: 622,
			hasCheckpoint: true,
			interveningStores: ["tradingCards"],
		});
		assert.strictEqual(plan.plan, "rebase");
	});

	test("discards when the winning version touched the league itself", () => {
		const plan = resolveStaleAdvancePlan({
			applied: 621,
			roomVersion: 622,
			hasCheckpoint: true,
			interveningStores: ["players", "teamSeasons"],
		});
		assert.strictEqual(plan.plan, "discard");
	});

	// One inert store and one that is not is not inert.
	test("discards on a mixed changeset", () => {
		const plan = resolveStaleAdvancePlan({
			applied: 621,
			roomVersion: 622,
			hasCheckpoint: true,
			interveningStores: ["tradingCards", "players"],
		});
		assert.strictEqual(plan.plan, "discard");
	});

	test("discards when what intervened could not be determined", () => {
		const plan = resolveStaleAdvancePlan({
			applied: 621,
			roomVersion: 624,
			hasCheckpoint: true,
			interveningStores: undefined,
		});
		assert.strictEqual(plan.plan, "discard");
	});

	// THE SECOND HALF OF THE INCIDENT. A discard is only the first half of
	// "discard, then restore"; with nothing to restore from it just strands the
	// records here with every indicator green.
	test("never discards when the room has no checkpoint", () => {
		const plan = resolveStaleAdvancePlan({
			applied: 621,
			roomVersion: 622,
			hasCheckpoint: false,
			interveningStores: ["players"],
		});
		assert.strictEqual(plan.plan, "rebase");
	});

	test("never discards with no checkpoint even several versions behind", () => {
		const plan = resolveStaleAdvancePlan({
			applied: 400,
			roomVersion: 622,
			hasCheckpoint: false,
			interveningStores: undefined,
		});
		assert.strictEqual(plan.plan, "rebase");
	});

	test("rebases when nothing actually intervened", () => {
		const plan = resolveStaleAdvancePlan({
			applied: 622,
			roomVersion: 622,
			hasCheckpoint: true,
			interveningStores: undefined,
		});
		assert.strictEqual(plan.plan, "rebase");
	});

	// An empty list is not "touched only inert stores", it is "touched nothing
	// we can account for" - which is not a fact worth keeping a day on.
	test("discards on an empty store list", () => {
		const plan = resolveStaleAdvancePlan({
			applied: 621,
			roomVersion: 622,
			hasCheckpoint: true,
			interveningStores: [],
		});
		assert.strictEqual(plan.plan, "discard");
	});
});
