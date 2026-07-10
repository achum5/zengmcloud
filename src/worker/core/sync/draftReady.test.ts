import { assert, describe, test } from "vitest";
import {
	lastHoldoutToNotify,
	overallPickNumber,
	readyTeamTids,
} from "./draftReady.ts";

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

	test("free-agency steps: ready-through-days-left math", () => {
		// daysLeft counts DOWN; steps count UP as days sim. With 30 days left the
		// next day's step is 971 (1000 - 30 + 1); "ready until 25 left" is step
		// 975, which covers exactly the next 5 day-sims.
		const FA_STEP_BASE = 1000;
		const stepFor = (daysLeft: number) => FA_STEP_BASE - daysLeft + 1;
		const untilDaysLeft = (target: number) => FA_STEP_BASE - target;

		const until25 = untilDaysLeft(25);
		assert.ok(stepFor(30) <= until25, "day 30→29 covered");
		assert.ok(stepFor(26) <= until25, "day 26→25 covered");
		assert.ok(stepFor(25) > until25, "day 25→24 NOT covered");
		// End of free agency covers every remaining day.
		const untilEnd = untilDaysLeft(0);
		assert.ok(stepFor(1) <= untilEnd);
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

	describe("last-holdout notification", () => {
		const key = "2026-5";
		const base = {
			latestReady: {
				uidA: { untilPick: 5, draftKey: key, tid: 0 },
				uidB: { untilPick: 5, draftKey: key, tid: 5 },
			} as Record<string, any>,
			userTids: [0, 5, 9],
			readyTids: [0, 5],
			stageKey: key,
			nextStep: 5,
			onClockUser: false,
		};

		test("names the sole holdout, published by the smallest ready client id", () => {
			// uidA < uidB, so only the device with client id "uidA" publishes.
			assert.strictEqual(lastHoldoutToNotify({ ...base, clientId: "uidA" }), 9);
		});

		test("a non-designated ready device stays silent (no duplicate pushes)", () => {
			assert.strictEqual(
				lastHoldoutToNotify({ ...base, clientId: "uidB" }),
				undefined,
			);
		});

		test("the holdout's own device never publishes", () => {
			// The holdout (team 9) has an open, connected device that just hasn't
			// readied - it must not send itself the nudge.
			assert.strictEqual(
				lastHoldoutToNotify({ ...base, clientId: "uidHoldout9" }),
				undefined,
			);
		});

		test("no nudge when two or more teams are still out", () => {
			assert.strictEqual(
				lastHoldoutToNotify({
					...base,
					readyTids: [0],
					clientId: "uidA",
				}),
				undefined,
			);
		});

		test("no nudge when everyone is ready", () => {
			assert.strictEqual(
				lastHoldoutToNotify({
					...base,
					readyTids: [0, 5, 9],
					clientId: "uidA",
				}),
				undefined,
			);
		});

		test("no nudge while a human is on the clock", () => {
			assert.strictEqual(
				lastHoldoutToNotify({ ...base, onClockUser: true, clientId: "uidA" }),
				undefined,
			);
		});

		test("no nudge in a single-team room", () => {
			assert.strictEqual(
				lastHoldoutToNotify({
					latestReady: {},
					userTids: [0],
					readyTids: [],
					stageKey: key,
					nextStep: 5,
					onClockUser: false,
					clientId: "uidA",
				}),
				undefined,
			);
		});
	});
});
