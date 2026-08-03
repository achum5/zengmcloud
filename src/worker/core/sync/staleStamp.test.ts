import { afterEach, assert, beforeEach, describe, test, vi } from "vitest";
import { resetCache, resetG } from "../../../test/helpers.ts";
import { g } from "../../util/index.ts";
import {
	checkBehindAuthority,
	getSimSafety,
	resetBehindAuthorityStateForTesting,
} from "./connect.ts";
import { setSyncEngine } from "./engineHolder.ts";

// ---------------------------------------------------------------------------
// The stale-stamp loop, replayed from a real league's logs.
//
// The room's announced position said "2005 phase 3 day 113" while the log
// itself - fully drained by every device - ended at "2005 phase 4 day 114"
// (the season's final playoff day plus the lottery advance). The authority had
// stamped before its live sim ran and never restamped after.
//
// Every follower then read as "ahead of the room": the checker replayed the
// entire 2000-entry log, landed exactly where it started (because the log
// AGREED with it), declared failure, and re-armed 30 seconds later - forever.
// To the people playing, the season appeared to re-sim itself in a loop, and
// the sim guard blocked every action on top of it.
//
// The rule under test: a conclusive full replay that leaves the device still
// "ahead" of the stamp proves the STAMP is stale, not the device - every
// state-changing byte travels through that same log. Stand down, unblock, and
// wait for a fresh stamp.
// ---------------------------------------------------------------------------

const STALE_STAMP = { season: 2005, phase: 3, day: 113 };

const makeEngine = ({
	isAuthority,
	resyncResult,
}: {
	isAuthority: boolean;
	resyncResult: {
		total: number;
		applied: number;
		incomplete: number;
		failed: boolean;
	};
}) => {
	let position = { ...STALE_STAMP };
	const resyncCalls: number[] = [];
	const engine = {
		isAuthority: () => isAuthority,
		isBusyApplying: () => false,
		getAuthority: () => ({ position }),
		getCatchUpDiagnostics: () => ({}),
		resyncAll: async () => {
			resyncCalls.push(Date.now());
			if (isAuthority) {
				// The real engine restamps after a conclusive pass on the authority
				// (SyncEngine.resyncAllInner) - mirror that, since it IS the fix.
				if (!resyncResult.failed && resyncResult.incomplete === 0) {
					position = {
						season: g.get("season"),
						phase: g.get("phase"),
						day: 114,
					};
				}
			}
			return resyncResult;
		},
		catchUp: async () => true,
	};
	return {
		engine,
		resyncCalls,
		restamp: (p: typeof position) => {
			position = p;
		},
	};
};

// The device's local truth: 2005, lottery phase, day 114 in the games store.
const setupLocal = async () => {
	resetG();
	g.setWithoutSavingToDB("season", 2005);
	g.setWithoutSavingToDB("phase", 4);
	await resetCache({
		teams: [
			{ tid: 0, region: "LA", name: "Lakers", abbrev: "LAL" },
			{ tid: 1, region: "Cleveland", name: "Cavaliers", abbrev: "CLE" },
		],
	});
	const { idb } = await import("../../db/index.ts");
	// getSimSafety consults the durable repair flag, which lives in idb.meta -
	// real IndexedDB, absent under node. A stub with no flag set is the honest
	// state here: these scenarios are about the stamp, not the flag.
	(idb as any).meta = {
		get: async () => undefined,
		put: async () => undefined,
	};
	await idb.cache.games.add({
		gid: 1,
		day: 114,
		season: 2005,
		playoffs: true,
		teams: [{ tid: 0 }, { tid: 1 }] as any,
		won: { tid: 0, pts: 100 },
		lost: { tid: 1, pts: 90 },
		overtimes: 0,
		att: 0,
		gameAttributes: undefined,
	} as any);
};

// Two ticks with the 30s grace between them: the first NOTICES, the second ACTS.
const tickThroughGrace = async () => {
	await checkBehindAuthority();
	vi.advanceTimersByTime(31_000);
	await checkBehindAuthority();
};

describe("a stale authority stamp must not loop the room", () => {
	beforeEach(() => {
		vi.useFakeTimers();
		resetBehindAuthorityStateForTesting();
	});

	afterEach(() => {
		vi.useRealTimers();
		setSyncEngine(undefined);
		resetBehindAuthorityStateForTesting();
	});

	test("a follower whose conclusive replay agrees with it stands down for good", async () => {
		await setupLocal();
		const { engine, resyncCalls } = makeEngine({
			isAuthority: false,
			// The real numbers from the incident: full window read, clean apply.
			resyncResult: { total: 2000, applied: 736, incomplete: 0, failed: false },
		});
		setSyncEngine(engine as any);

		await tickThroughGrace();
		assert.strictEqual(
			resyncCalls.length,
			1,
			"the first pass should replay the log once to find out who is wrong",
		);

		// The old behavior: re-arm and grind the same 2000 entries every 30s,
		// forever. The proof already happened; asking again cannot answer better.
		for (let i = 0; i < 5; i++) {
			vi.advanceTimersByTime(31_000);
			await checkBehindAuthority();
		}
		assert.strictEqual(
			resyncCalls.length,
			1,
			`the checker kept grinding the log against a stamp already proven stale (${resyncCalls.length} replays)`,
		);
	});

	test("while stood down, the sim guard stops blocking every action", async () => {
		await setupLocal();
		const { engine } = makeEngine({
			isAuthority: false,
			resyncResult: { total: 2000, applied: 736, incomplete: 0, failed: false },
		});
		setSyncEngine(engine as any);

		// Before the proof: ahead of the stamp reads as a health problem, and the
		// guard says no. This is the "everytime i do something" half of the bug.
		const before = await getSimSafety();
		assert.strictEqual(before.safe, false);

		await tickThroughGrace();

		const after = await getSimSafety();
		assert.strictEqual(
			after.safe,
			true,
			"the guard kept blocking actions over a stamp the full log disproved",
		);
	});

	test("a fresh stamp from the authority ends the stand-down", async () => {
		await setupLocal();
		const { engine, resyncCalls, restamp } = makeEngine({
			isAuthority: false,
			resyncResult: { total: 2000, applied: 736, incomplete: 0, failed: false },
		});
		setSyncEngine(engine as any);

		await tickThroughGrace();
		assert.strictEqual(resyncCalls.length, 1);

		// The authority restamps at the real position - everything agrees now, so
		// the checker must go quiet WITHOUT another replay.
		restamp({ season: 2005, phase: 4, day: 114 });
		vi.advanceTimersByTime(31_000);
		await checkBehindAuthority();
		assert.strictEqual(resyncCalls.length, 1);

		// And a LATER genuine divergence (new stale stamp value) is noticed again
		// - the stand-down is scoped to the one stamp it proved stale. Note the
		// divergence must be in (season, phase): day is deliberately excluded
		// from ahead/behind, since a follower routinely applies a day's games a
		// moment before the stamp catches up.
		restamp({ season: 2005, phase: 3, day: 999 });
		await tickThroughGrace();
		assert.strictEqual(
			resyncCalls.length,
			2,
			"the stand-down must not blind the checker to future divergence",
		);
	});

	test("the authority heals its own stale stamp instead of skipping the check", async () => {
		await setupLocal();
		const { engine, resyncCalls } = makeEngine({
			isAuthority: true,
			resyncResult: { total: 2000, applied: 736, incomplete: 0, failed: false },
		});
		setSyncEngine(engine as any);

		await tickThroughGrace();

		// One conclusive replay, which restamps (mirrored in the stub exactly as
		// SyncEngine does it) - this is what un-sticks the whole room, because
		// only the authority can write the stamp.
		assert.strictEqual(resyncCalls.length, 1);
		assert.deepStrictEqual(engine.getAuthority().position, {
			season: 2005,
			phase: 4,
			day: 114,
		});

		// Healed: later ticks see stamp == local and do nothing.
		vi.advanceTimersByTime(31_000);
		await checkBehindAuthority();
		assert.strictEqual(resyncCalls.length, 1);
	});

	test("an inconclusive replay does NOT count as proof", async () => {
		await setupLocal();
		const { engine, resyncCalls } = makeEngine({
			isAuthority: false,
			// A fetch died mid-window: nothing was proven either way.
			resyncResult: { total: 2000, applied: 500, incomplete: 3, failed: false },
		});
		setSyncEngine(engine as any);

		await tickThroughGrace();
		assert.strictEqual(resyncCalls.length, 1);

		// Still blocked - rightly, since the device might genuinely be corrupt.
		const safety = await getSimSafety();
		assert.strictEqual(safety.safe, false);

		// And the checker retries later rather than standing down on no evidence.
		vi.advanceTimersByTime(31_000);
		await checkBehindAuthority();
		assert.strictEqual(resyncCalls.length, 2);
	});
});
