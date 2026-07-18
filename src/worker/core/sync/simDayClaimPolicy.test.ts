import { assert, describe, test } from "vitest";
import { decideSimDayClaim, type SimDayClaimDoc } from "./simDayClaimPolicy.ts";

const LEASE = 90_000;
const NOW = 1_784_169_868_841;

const doc = (over: Partial<SimDayClaimDoc> = {}): SimDayClaimDoc => ({
	holderId: "device-a",
	stageKey: "sim:2000",
	day: 32,
	gids: [407, 408, 409, 410],
	at: NOW - 5_000,
	maxDay: 32,
	completed: false,
	...over,
});

const ask = (
	day: number,
	gids: number[],
	over: Partial<{ stageKey: string; now: number }> = {},
) => ({
	stageKey: over.stageKey ?? "sim:2000",
	day,
	gids,
	now: over.now ?? NOW,
	leaseMs: LEASE,
});

describe("decideSimDayClaim", () => {
	test("no existing claim: granted", () => {
		const d = decideSimDayClaim(undefined, ask(1, [0, 1, 2]));
		assert.deepStrictEqual(d, {
			grant: true,
			day: 1,
			maxDay: 1,
			gids: [0, 1, 2],
		});
	});

	test("a claim from a different season: granted (fence resets per stage)", () => {
		const d = decideSimDayClaim(
			doc({ stageKey: "sim:1999", maxDay: 93, completed: true }),
			ask(1, [0, 1]),
		);
		assert.deepStrictEqual(d, { grant: true, day: 1, maxDay: 1, gids: [0, 1] });
	});

	test("a day below the high-water mark: rejected, even from a fresh holder", () => {
		const d = decideSimDayClaim(
			doc({ day: 33, maxDay: 33, completed: true }),
			ask(32, [500, 501]),
		);
		assert.deepStrictEqual(d, { grant: false, reason: "day-already-run" });
	});

	test("a newer day: granted without waiting for the completion mark", () => {
		const d = decideSimDayClaim(doc({ completed: false }), ask(33, [420, 421]));
		assert.deepStrictEqual(d, {
			grant: true,
			day: 33,
			maxDay: 33,
			gids: [420, 421],
		});
	});

	test("same day, disjoint games (live-simmed game then rest of day): granted, gids merged", () => {
		const d = decideSimDayClaim(
			doc({ gids: [410], completed: true }),
			ask(32, [407, 408, 409]),
		);
		assert.deepStrictEqual(d, {
			grant: true,
			day: 32,
			maxDay: 32,
			gids: [410, 407, 408, 409],
		});
	});

	test("same day, overlapping games, completed: rejected (the double-sim case)", () => {
		const d = decideSimDayClaim(
			doc({ completed: true }),
			ask(32, [407, 408, 409, 410]),
		);
		assert.deepStrictEqual(d, {
			grant: false,
			reason: "games-already-simmed",
		});
	});

	test("same day, overlapping games, lease still held: rejected (concurrent sim)", () => {
		const d = decideSimDayClaim(doc({ completed: false }), ask(32, [407]));
		assert.deepStrictEqual(d, { grant: false, reason: "lease-held" });
	});

	test("same day, overlapping games, lease lapsed without completion: granted (crash recovery)", () => {
		const d = decideSimDayClaim(
			doc({ completed: false, at: NOW - LEASE - 1 }),
			ask(32, [407, 408, 409, 410, 411]),
		);
		assert.deepStrictEqual(d, {
			grant: true,
			day: 32,
			maxDay: 32,
			gids: [407, 408, 409, 410, 411],
		});
	});

	test("doc without maxDay (pre-fence shape): day serves as the mark", () => {
		const d = decideSimDayClaim(
			doc({ maxDay: undefined, day: 40 }),
			ask(39, [600]),
		);
		assert.deepStrictEqual(d, { grant: false, reason: "day-already-run" });
	});
});
