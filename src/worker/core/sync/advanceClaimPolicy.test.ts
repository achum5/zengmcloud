import { assert, describe, test } from "vitest";
import {
	decideAdvanceClaim,
	type AdvanceClaimDoc,
} from "./advanceClaimPolicy.ts";

const LEASE = 90_000;
const NOW = 1_784_169_868_841;

const doc = (over: Partial<AdvanceClaimDoc> = {}): AdvanceClaimDoc => ({
	holderId: "device-a",
	draftKey: "2084-8",
	pick: 40,
	at: NOW - 5_000,
	maxPick: 40,
	completed: false,
	...over,
});

const ask = (pick: number, over: Partial<{ draftKey: string; now: number }> = {}) => ({
	draftKey: over.draftKey ?? "2084-8",
	pick,
	now: over.now ?? NOW,
	leaseMs: LEASE,
});

describe("decideAdvanceClaim", () => {
	test("no existing claim: granted", () => {
		const d = decideAdvanceClaim(undefined, ask(12));
		assert.deepStrictEqual(d, { grant: true, maxPick: 12 });
	});

	test("a claim from a different stage: granted (fence resets per stage)", () => {
		const d = decideAdvanceClaim(
			doc({ draftKey: "2084-5", maxPick: 99, completed: true }),
			ask(1),
		);
		assert.deepStrictEqual(d, { grant: true, maxPick: 1 });
	});

	test("REGRESSION: a step below the high-water mark is never re-claimable, even long after the lease", () => {
		// The 2084 free-agency incident: a device rejoined ~20 days stale, its
		// ready-through entries still covered the old steps, the lease on them had
		// long expired, and it re-claimed + re-simmed finished days - publishing
		// regressed daysLeft/rosters as new history and dragging the whole room
		// back from 1 day left to 21.
		const existing = doc({ pick: 60, maxPick: 60, at: NOW - 10 * 60_000 });
		for (const stalePick of [40, 45, 59]) {
			const d = decideAdvanceClaim(existing, ask(stalePick));
			assert.deepStrictEqual(d, { grant: false, reason: "step-already-run" });
		}
	});

	test("legacy doc without maxPick: its pick is the fence", () => {
		const legacy = {
			holderId: "device-a",
			draftKey: "2084-8",
			pick: 50,
			at: NOW - 10 * 60_000,
		};
		assert.deepStrictEqual(decideAdvanceClaim(legacy, ask(49)), {
			grant: false,
			reason: "step-already-run",
		});
		assert.deepStrictEqual(decideAdvanceClaim(legacy, ask(51)), {
			grant: true,
			maxPick: 51,
		});
	});

	test("the newest step, lease alive: rejected (someone is running it)", () => {
		const d = decideAdvanceClaim(doc({ at: NOW - 1_000 }), ask(40));
		assert.deepStrictEqual(d, { grant: false, reason: "lease-held" });
	});

	test("the newest step, completed: rejected even after the lease lapses", () => {
		const d = decideAdvanceClaim(
			doc({ completed: true, at: NOW - 10 * 60_000 }),
			ask(40),
		);
		assert.deepStrictEqual(d, { grant: false, reason: "step-completed" });
	});

	test("the newest step, lease lapsed, not completed: granted (crash recovery)", () => {
		const d = decideAdvanceClaim(doc({ at: NOW - LEASE - 1 }), ask(40));
		assert.deepStrictEqual(d, { grant: true, maxPick: 40 });
	});

	test("a genuinely newer step: granted, fence advances", () => {
		// Even while the previous step's lease is technically alive - its advance
		// published the state this step was derived from.
		const d = decideAdvanceClaim(doc({ at: NOW - 1_000 }), ask(41));
		assert.deepStrictEqual(d, { grant: true, maxPick: 41 });
	});
});
