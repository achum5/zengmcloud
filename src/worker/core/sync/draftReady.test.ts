import { assert, describe, test } from "vitest";
import { overallPickNumber, readyTeamTids } from "./draftReady.ts";

describe("draft ready-up", () => {
	test("overall pick number spans rounds", () => {
		assert.strictEqual(overallPickNumber({ round: 1, pick: 1 }, 30), 1);
		assert.strictEqual(overallPickNumber({ round: 1, pick: 16 }, 30), 16);
		assert.strictEqual(overallPickNumber({ round: 2, pick: 1 }, 30), 31);
	});

	test("readiness is counted per TEAM, not per device", () => {
		const userTids = [0, 5, 9];
		const key = "2026-5";

		// Two devices covering the same team (a user on phone + laptop) count
		// once; a third team readied by one device completes nothing yet.
		const ready = {
			uidA1: { untilPick: 3, draftKey: key, tid: 0 },
			uidA2: { untilPick: 9, draftKey: key, tid: 0 },
			uidB: { untilPick: 3, draftKey: key, tid: 5 },
		};
		assert.deepEqual(readyTeamTids(ready, userTids, key, 3), [0, 5]);

		// Team 9 readies → full coverage.
		assert.deepEqual(
			readyTeamTids(
				{ ...ready, uidC: { untilPick: 3, draftKey: key, tid: 9 } },
				userTids,
				key,
				3,
			),
			[0, 5, 9],
		);
	});

	test("ready-through expires as picks pass, and per pick target", () => {
		const userTids = [0];
		const key = "2026-5";
		const ready = { uid: { untilPick: 16, draftKey: key, tid: 0 } };

		// Ready through pick 16: covers picks up to 16, not 17.
		assert.deepEqual(readyTeamTids(ready, userTids, key, 16), [0]);
		assert.deepEqual(readyTeamTids(ready, userTids, key, 17), []);
	});

	test("stale entries from another draft or team never count", () => {
		const userTids = [0, 5];

		// Last season's ready entry (different draftKey) is ignored, as is an
		// entry for a team that is no longer user-controlled, and a cleared
		// (null) entry.
		const ready = {
			uidOld: { untilPick: 999, draftKey: "2025-5", tid: 0 },
			uidGone: { untilPick: 999, draftKey: "2026-5", tid: 7 },
			uidCleared: null,
		};
		assert.deepEqual(readyTeamTids(ready, userTids, "2026-5", 1), []);
	});
});
