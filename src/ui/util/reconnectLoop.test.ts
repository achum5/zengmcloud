import { assert, describe, test } from "vitest";
import { ERROR_MESSAGE_SYNC_ROOM_MISMATCH } from "../../common/constants.ts";
import type { StoredSync } from "./autoReconnectSync.ts";
import {
	RECONNECT_RETRY_MS,
	runReconnect,
	type ReconnectDeps,
} from "./reconnectLoop.ts";

// A device with league `lid` on screen and a room stored for it, whose
// connects answer from `answers` in turn (an Error is thrown).
const device = (
	answers: ({ connected: boolean } | undefined | Error)[],
	{
		lid = 7,
		session = { code: "room", isHost: false } as StoredSync,
		connected = false,
	} = {},
) => {
	const state = {
		lid: lid as number | undefined,
		stored: new Map<number, StoredSync>([[lid, session]]),
		connects: 0,
		marked: 0,
		refreshed: 0,
		waits: [] as number[],
		connected,
		// Run between tries, as if the user did something while it waited.
		onWait: undefined as (() => void) | undefined,
	};
	const deps: ReconnectDeps = {
		markSyncRequired: async () => {
			state.marked += 1;
		},
		getSyncStatus: async () => ({ connected: state.connected }),
		refreshSyncUIState: async () => {
			state.refreshed += 1;
		},
		connect: async () => {
			const answer = answers[state.connects];
			state.connects += 1;
			if (answer instanceof Error) {
				throw answer;
			}
			return answer;
		},
		currentLid: () => state.lid,
		stored: (l) => state.stored.get(l),
		setStored: (l, s) => {
			state.stored.set(l, s);
		},
		clearStored: (l) => {
			state.stored.delete(l);
		},
		wait: async (ms) => {
			state.waits.push(ms);
			state.onWait?.();
		},
	};
	return { state, deps, lid };
};

describe("runReconnect", () => {
	test("connects, gating sims first", async () => {
		const { state, deps, lid } = device([{ connected: true }]);
		assert.strictEqual(await runReconnect(lid, deps), "connected");
		assert.strictEqual(state.marked, 1);
		assert.strictEqual(state.connects, 1);
		assert.deepStrictEqual(state.waits, []);
	});

	// It used to give up after five, which meant "until the page is reloaded":
	// the league showing as one that had never been in a room.
	test("never gives up while the league is in a room", async () => {
		const fail = new Error("Timed out connecting (auth)");
		const { state, deps, lid } = device([
			...Array.from({ length: 9 }, () => fail),
			{ connected: true },
		]);
		assert.strictEqual(await runReconnect(lid, deps), "connected");
		assert.strictEqual(state.connects, 10);
		assert.deepStrictEqual(
			state.waits,
			[0, 1, 2, 3, 4, 5, 6, 7, 8].map(
				(i) => RECONNECT_RETRY_MS[Math.min(i, RECONNECT_RETRY_MS.length - 1)],
			),
		);
	});

	// "Not connected" with nothing thrown - the league still loading - was
	// taken for success, and nothing ever tried again.
	test("a connect that comes back not connected is tried again", async () => {
		const { state, deps, lid } = device([
			{ connected: false },
			undefined,
			{ connected: true },
		]);
		assert.strictEqual(await runReconnect(lid, deps), "connected");
		assert.strictEqual(state.connects, 3);
	});

	test("already connected: re-asserts the screen, no connect", async () => {
		const { state, deps, lid } = device([], { connected: true });
		assert.strictEqual(await runReconnect(lid, deps), "connected");
		assert.strictEqual(state.connects, 0);
		assert.strictEqual(state.refreshed, 1);
	});

	test("stops when the room is disconnected between tries", async () => {
		const { state, deps, lid } = device([new Error("offline"), new Error("x")]);
		state.onWait = () => {
			state.stored.delete(lid);
		};
		assert.strictEqual(await runReconnect(lid, deps), "disconnected");
		assert.strictEqual(state.connects, 1);
	});

	test("stops when another league is opened", async () => {
		const { state, deps, lid } = device([new Error("offline")]);
		state.onWait = () => {
			state.lid = 8;
		};
		assert.strictEqual(await runReconnect(lid, deps), "left");
		assert.strictEqual(state.connects, 1);
	});

	test("a room bound to another league drops this league's stored room", async () => {
		const { state, deps, lid } = device([
			new Error(ERROR_MESSAGE_SYNC_ROOM_MISMATCH),
		]);
		assert.strictEqual(await runReconnect(lid, deps), "mismatch");
		assert.strictEqual(state.stored.get(lid), undefined);
	});

	// A connect for this league still finishing after a switch to another one
	// says nothing about this league's room.
	test("...but not once another league is on screen", async () => {
		const { state, deps, lid } = device([]);
		deps.connect = async () => {
			state.lid = 8;
			throw new Error(ERROR_MESSAGE_SYNC_ROOM_MISMATCH);
		};
		assert.strictEqual(await runReconnect(lid, deps), "left");
		assert.ok(state.stored.get(lid));
	});

	test("a join chosen at league creation stays explicit until it connects", async () => {
		const { state, deps, lid } = device(
			[new Error("offline"), { connected: true }],
			{
				session: { code: "room", isHost: true, pendingJoin: true },
			},
		);
		let pendingAtSecondTry: boolean | undefined;
		state.onWait = () => {
			pendingAtSecondTry = state.stored.get(lid)?.pendingJoin;
		};
		assert.strictEqual(await runReconnect(lid, deps), "connected");
		assert.strictEqual(pendingAtSecondTry, true);
		assert.deepStrictEqual(state.stored.get(lid), {
			code: "room",
			isHost: true,
			firebaseConfig: undefined,
		});
	});
});
