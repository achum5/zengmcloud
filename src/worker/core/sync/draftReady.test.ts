import { assert, describe, test } from "vitest";
import {
	lastHoldoutToNotify,
	overallPickNumber,
	readyTeamTids,
	stopStep,
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

// THE BUG: a league that pauses on day 15 and again at the trade deadline
// arrived at the deadline with everybody already showing ready. Every regular-
// season stop used step 1, and a stage key is only season-and-phase, so the
// ready entry published on day 15 went on satisfying every later stop in the
// season - the room was never asked a second time.
describe("regular-season sim stops are separate gates", () => {
	const key = "2026-4";
	const userTids = [0, 5];

	test("each stop has its own step, increasing through the season", () => {
		assert.strictEqual(stopStep({ kind: "day", day: 15 }), 15);
		assert.strictEqual(stopStep({ kind: "day", day: 41 }), 41);
		// The deadline sits on its own day, later than any day stop that can
		// still be pending in the same phase.
		assert.strictEqual(stopStep({ kind: "deadline", gid: 500, day: 60 }), 60);
		assert.isAbove(
			stopStep({ kind: "deadline", gid: 500, day: 60 }),
			stopStep({ kind: "day", day: 41 }),
		);
	});

	test("readying up for day 15 does NOT ready you for the deadline", () => {
		const dayStop = stopStep({ kind: "day", day: 15 });
		const deadline = stopStep({ kind: "deadline", gid: 500, day: 60 });

		// Both teams ready for the day-15 stop, exactly as the UI publishes it.
		const ready = {
			uidA: { untilPick: dayStop, draftKey: key, tid: 0 },
			uidB: { untilPick: dayStop, draftKey: key, tid: 5 },
		};
		assert.deepEqual(readyTeamTids(ready, userTids, key, dayStop), [0, 5]);

		// The room plays day 15 and reaches the deadline. Nobody has agreed to
		// anything since, so nobody is ready.
		assert.deepEqual(readyTeamTids(ready, userTids, key, deadline), []);
	});

	test("a legacy sentinel with no day still gets a step past every day stop", () => {
		const step = stopStep({ kind: "deadline", gid: 500, day: undefined });
		assert.isAbove(step, stopStep({ kind: "day", day: 82 }));
		assert.deepEqual(
			readyTeamTids(
				{ uidA: { untilPick: 82, draftKey: key, tid: 0 } },
				userTids,
				key,
				step,
			),
			[],
		);
	});

	test("readying up for the stop in front of you still counts", () => {
		const deadline = stopStep({ kind: "deadline", gid: 500, day: 60 });
		assert.deepEqual(
			readyTeamTids(
				{
					uidA: { untilPick: deadline, draftKey: key, tid: 0 },
					uidB: { untilPick: deadline, draftKey: key, tid: 5 },
				},
				userTids,
				key,
				deadline,
			),
			[0, 5],
		);
	});
});
