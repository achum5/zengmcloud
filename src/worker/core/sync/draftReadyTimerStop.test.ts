import { afterEach, assert, beforeEach, describe, test, vi } from "vitest";
import { PHASE } from "../../../common/constants.ts";
import { resetCache, resetG } from "../../../test/helpers.ts";
import { idb } from "../../db/index.ts";
import { g, local } from "../../util/index.ts";
import { setSyncEngine } from "./engineHolder.ts";
import {
	setupDraftReady,
	shownPhaseReady,
	teardownDraftReady,
	timerCrossesStop,
} from "./draftReady.ts";
import { setRoomAutoPlayNextRunAt } from "./roomAutoPlayClock.ts";
import {
	getPendingSimStop,
	roomLeftStopToSim,
	shouldStopAtSimStop,
	singleGameWaitsAtSimStop,
} from "./tradeDeadlineGate.ts";
import type { DraftReadyEntry } from "./types.ts";

type Ready = Record<string, DraftReadyEntry | null> | undefined;

const STOP_DAY = 41;

// Ready entries for the stop in front of the league, one device a team.
const ready = (step: number, ...tids: number[]): Ready =>
	Object.fromEntries(
		tids.map((tid) => [
			`device${tid}`,
			{
				draftKey: `${g.get("season")}-${g.get("phase")}`,
				tid,
				untilPick: step,
				name: `Team ${tid}`,
			},
		]),
	);

const makeRoom = () => {
	const listeners: ((ready: Ready) => void)[] = [];
	let server: Ready;
	const claimDraftAdvance = vi.fn(async () => true);
	return {
		claimDraftAdvance,
		say(next: Ready) {
			server = next;
			for (const listener of listeners) {
				listener(next);
			}
		},
		transport: {
			subscribeDraftReady(onChange: (ready: Ready) => void) {
				listeners.push(onChange);
				return () => {};
			},
			fetchDraftReady: async () => server,
			publishDraftReady: async () => {},
			claimDraftAdvance,
		},
	};
};

describe("a regular-season stop the room has readied up for", () => {
	beforeEach(async () => {
		vi.useFakeTimers();
		resetG();
		await resetCache({ schedule: [] });
		g.setWithoutSavingToDB("phase", PHASE.REGULAR_SEASON);
		g.setWithoutSavingToDB("userTids", [0, 1, 2]);
		g.setWithoutSavingToDB("userTid", 0);
		g.setWithoutSavingToDB("simStopDays", `${STOP_DAY}, deadline`);
		local.leagueLoaded = true;
		// Never caught up, so nothing here tries to notify or advance.
		setSyncEngine({ isCaughtUp: () => false } as any);
	});

	afterEach(() => {
		teardownDraftReady();
		setSyncEngine(undefined as any);
		setRoomAutoPlayNextRunAt(undefined);
		local.leagueLoaded = false;
		vi.useRealTimers();
	});

	const gameDay = async () => {
		await idb.cache.schedule.add({
			gid: 600,
			day: STOP_DAY,
			homeTid: 0,
			awayTid: 1,
		} as any);
	};

	// Alex, 2026-11-25: 3/3 used to sim the day right there. The ready-up goes
	// away and the countdown comes back instead; the timer plays the day.
	test("with the timer running, the ready-up goes and the timer's sim crosses", async () => {
		await gameDay();
		setRoomAutoPlayNextRunAt(Date.now() + 15 * 60_000);
		const room = makeRoom();
		setupDraftReady(room.transport as any);

		room.say(ready(STOP_DAY, 0, 1));
		await vi.advanceTimersByTimeAsync(10);
		// Still being asked: the clock stays hidden and the sim stays put.
		assert.strictEqual(shownPhaseReady()?.readyTeams, 2);
		const stop = await getPendingSimStop();
		assert.strictEqual(shouldStopAtSimStop(true, stop), true);

		room.say(ready(STOP_DAY, 0, 1, 2));
		await vi.advanceTimersByTimeAsync(10);
		assert.strictEqual(shownPhaseReady(), undefined);
		assert.strictEqual(room.claimDraftAdvance.mock.calls.length, 0);
		// The auto play fire (or Play, in charge of simming) crosses, and an own
		// game is a game like any other day's.
		assert.strictEqual(roomLeftStopToSim(stop), true);
		assert.strictEqual(shouldStopAtSimStop(true, stop), false);
		assert.strictEqual(singleGameWaitsAtSimStop(stop), false);
	});

	test("with no timer, everyone ready plays the day at once, as before", async () => {
		await gameDay();
		const room = makeRoom();
		setupDraftReady(room.transport as any);

		room.say(ready(STOP_DAY, 0, 1, 2));
		await vi.advanceTimersByTimeAsync(10);
		assert.strictEqual(shownPhaseReady()?.readyTeams, 3);
		const stop = await getPendingSimStop();
		assert.strictEqual(roomLeftStopToSim(stop), false);
		assert.strictEqual(shouldStopAtSimStop(true, stop), true);
	});

	test("an empty day plays no games, so it is crossed at once too", async () => {
		await idb.cache.schedule.add({
			gid: 600,
			day: STOP_DAY,
			homeTid: -4,
			awayTid: -4,
		} as any);
		setRoomAutoPlayNextRunAt(Date.now() + 15 * 60_000);
		const room = makeRoom();
		setupDraftReady(room.transport as any);

		const stop = await getPendingSimStop();
		assert.deepStrictEqual(stop, {
			kind: "day",
			day: STOP_DAY,
			noGames: true,
		});
		room.say(ready(STOP_DAY, 0, 1, 2));
		await vi.advanceTimersByTimeAsync(10);
		assert.strictEqual(shownPhaseReady()?.readyTeams, 3);
		assert.strictEqual(roomLeftStopToSim(stop), false);
	});

	test("the trade deadline is crossed at once, timer or no timer", async () => {
		await idb.cache.schedule.add({
			gid: 600,
			day: 40,
			homeTid: -3,
			awayTid: -3,
		} as any);
		setRoomAutoPlayNextRunAt(Date.now() + 15 * 60_000);
		const room = makeRoom();
		setupDraftReady(room.transport as any);

		room.say(ready(40, 0, 1, 2));
		await vi.advanceTimersByTimeAsync(10);
		assert.strictEqual(shownPhaseReady()?.readyTeams, 3);
		assert.strictEqual(roomLeftStopToSim(await getPendingSimStop()), false);
	});

	// The ready entry for an earlier stop says nothing about this one.
	test("ready for another stop is not ready for this one", async () => {
		await gameDay();
		setRoomAutoPlayNextRunAt(Date.now() + 15 * 60_000);
		const room = makeRoom();
		setupDraftReady(room.transport as any);

		room.say(ready(15, 0, 1, 2));
		await vi.advanceTimersByTimeAsync(10);
		assert.strictEqual(shownPhaseReady()?.readyTeams, 0);
		assert.strictEqual(roomLeftStopToSim(await getPendingSimStop()), false);
	});
});

describe("timerCrossesStop", () => {
	const now = 1_000_000;
	const day = { kind: "day", day: STOP_DAY } as const;

	test("a day with games and a timer counting down", () => {
		assert.strictEqual(timerCrossesStop(day, now + 60_000, now), true);
	});

	test("not without a timer, or one long overdue", () => {
		assert.strictEqual(timerCrossesStop(day, undefined, now), false);
		assert.strictEqual(timerCrossesStop(day, now - 30_000, now), true);
		assert.strictEqual(timerCrossesStop(day, now - 5 * 60_000, now), false);
	});

	test("never a day with no games, or the deadline", () => {
		assert.strictEqual(
			timerCrossesStop({ ...day, noGames: true }, now + 60_000, now),
			false,
		);
		assert.strictEqual(
			timerCrossesStop(
				{ kind: "deadline", gid: 1, day: 40 },
				now + 60_000,
				now,
			),
			false,
		);
	});
});
