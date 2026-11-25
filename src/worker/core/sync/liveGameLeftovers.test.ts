import { afterEach, assert, beforeEach, describe, test, vi } from "vitest";
import {
	LEFTOVER_OWN_MS,
	LEFTOVER_ROOM_MS,
	QUIET_WATCH_MS,
} from "./liveBroadcastFollow.ts";
import {
	createLatestOnlyQueue,
	createLiveGameSweep,
	QUIET_GAME_RECHECK_MS,
	type SweepTransport,
} from "./liveGameLeftovers.ts";
import type { LiveBroadcastMeta } from "./types.ts";

const meta = (over: Partial<LiveBroadcastMeta> = {}): LiveBroadcastMeta => ({
	holderId: "alex",
	active: true,
	gid: 7,
	byName: "Alex",
	cursor: 900,
	paused: false,
	speed: 7,
	gameOver: false,
	startedAt: 1_000,
	chunkCount: 2,
	expiresAt: 100_000,
	...over,
});

// A room on the server: the docs as it has them, and when it took each.
const fakeRoom = (clientId: string) => {
	const docs = new Map<
		number,
		{ meta: LiveBroadcastMeta; updatedAt: number }
	>();
	const cleared: number[] = [];
	let reads = 0;
	const transport: SweepTransport = {
		clientId,
		fetchLiveGameFromServer: async (gid) => {
			reads += 1;
			const doc = docs.get(gid);
			return { meta: doc?.meta, updatedAt: doc?.updatedAt };
		},
		clearLiveGame: async (gid) => {
			docs.delete(gid);
			cleared.push(gid);
		},
	};
	return {
		docs,
		cleared,
		transport,
		reads: () => reads,
	};
};

const setup = ({
	me,
	heard,
	playing,
}: {
	me: string;
	heard: LiveBroadcastMeta[];
	playing?: number;
}) => {
	const room = fakeRoom(me);
	for (const m of heard) {
		room.docs.set(m.gid, { meta: m, updatedAt: m.expiresAt - 12_000 });
	}
	let now = 0;
	const over: number[] = [];
	let repaints = 0;
	const sweep = createLiveGameSweep({
		transport: () => room.transport,
		metas: () => heard,
		playingHere: (gid) => gid === playing,
		giveUp: (m) => {
			if (over.includes(m.startedAt)) {
				return false;
			}
			over.push(m.startedAt);
			return true;
		},
		onCalledOver: () => {
			repaints += 1;
		},
		now: () => now,
	});
	return {
		room,
		over,
		sweep,
		repaints: () => repaints,
		at: (t: number) => {
			now = t;
		},
	};
};

describe("a live game left behind by a dropped connection", () => {
	test("the simmer's own device clears it as soon as it is back", async () => {
		// Alex's game, frozen at OT 1:46 when his connection went; his page
		// refreshed since, so nothing on his device is playing it.
		const s = setup({ me: "alex", heard: [meta()] });
		s.at(100_000 + LEFTOVER_OWN_MS - 1);
		await s.sweep();
		assert.deepEqual(s.room.cleared, []);
		s.at(100_000 + LEFTOVER_OWN_MS);
		await s.sweep();
		assert.deepEqual(s.room.cleared, [7]);
	});

	test("a league mate's device calls it over after the wait, and clears it later", async () => {
		const s = setup({ me: "dominic", heard: [meta()] });
		s.at(100_000 + QUIET_WATCH_MS - 1);
		await s.sweep();
		assert.strictEqual(s.room.reads(), 0);
		assert.deepEqual(s.over, []);

		s.at(100_000 + QUIET_WATCH_MS);
		await s.sweep();
		assert.deepEqual(s.over, [1_000]);
		assert.strictEqual(s.repaints(), 1);
		assert.deepEqual(s.room.cleared, []);

		// (Looked at again no sooner than a minute on.)
		s.at(100_000 + QUIET_WATCH_MS + 1_000);
		await s.sweep();
		assert.strictEqual(s.room.reads(), 1);

		s.at(100_000 + LEFTOVER_ROOM_MS);
		await s.sweep();
		assert.deepEqual(s.room.cleared, [7]);
		// Called over once, repainted once.
		assert.strictEqual(s.repaints(), 1);
	});

	test("hours later, a device just opened clears it at once", async () => {
		const s = setup({ me: "dominic", heard: [meta()] });
		s.at(100_000 + 3 * 60 * 60 * 1000);
		await s.sweep();
		assert.deepEqual(s.over, [1_000]);
		assert.deepEqual(s.room.cleared, [7]);
	});

	test("a game still going on the server is left alone (this device's copy was behind)", async () => {
		const heard = meta();
		const s = setup({ me: "dominic", heard: [heard] });
		const now = 100_000 + LEFTOVER_ROOM_MS;
		s.room.docs.set(7, {
			meta: { ...heard, expiresAt: now + 12_000 },
			updatedAt: now,
		});
		s.at(now);
		await s.sweep();
		assert.deepEqual(s.over, []);
		assert.deepEqual(s.room.cleared, []);
		assert.strictEqual(s.room.docs.size, 1);
	});

	test("never the game this device is playing", async () => {
		const s = setup({ me: "alex", heard: [meta()], playing: 7 });
		s.at(100_000 + LEFTOVER_ROOM_MS * 3);
		await s.sweep();
		assert.strictEqual(s.room.reads(), 0);
		assert.deepEqual(s.room.cleared, []);
	});

	test("a failed read is tried again next time round", async () => {
		const s = setup({ me: "dominic", heard: [meta()] });
		const fetch = s.room.transport.fetchLiveGameFromServer!;
		let fail = true;
		s.room.transport.fetchLiveGameFromServer = async (gid) => {
			if (fail) {
				throw new Error("offline");
			}
			return fetch(gid);
		};
		s.at(100_000 + LEFTOVER_ROOM_MS);
		await s.sweep();
		assert.deepEqual(s.room.cleared, []);
		fail = false;
		s.at(100_000 + LEFTOVER_ROOM_MS + QUIET_GAME_RECHECK_MS);
		await s.sweep();
		assert.deepEqual(s.room.cleared, [7]);
	});
});

describe("the simmer's heartbeats", () => {
	beforeEach(() => {
		vi.useFakeTimers();
	});
	afterEach(() => {
		vi.useRealTimers();
	});

	test("one on its way at a time, and only the latest waits", async () => {
		const queue = createLatestOnlyQueue<number>(10_000);
		const sent: number[] = [];
		const answers: (() => void)[] = [];
		const send = (n: number) => {
			sent.push(n);
			return new Promise<void>((resolve) => {
				answers.push(resolve);
			});
		};
		void queue.push(1, send);
		void queue.push(2, send);
		void queue.push(3, send);
		void queue.push(4, send);
		assert.deepEqual(sent, [1]);
		answers.shift()!();
		await vi.advanceTimersByTimeAsync(0);
		assert.deepEqual(sent, [1, 4]);
		answers.shift()!();
		await vi.advanceTimersByTimeAsync(0);
		assert.deepEqual(sent, [1, 4]);
		// Free again: the next goes straight out.
		void queue.push(5, send);
		assert.deepEqual(sent, [1, 4, 5]);
	});

	test("one the server never answers stops holding the next up", async () => {
		const queue = createLatestOnlyQueue<number>(10_000);
		const sent: number[] = [];
		const send = (n: number) => {
			sent.push(n);
			return new Promise<void>(() => {});
		};
		void queue.push(1, send);
		void queue.push(2, send);
		await vi.advanceTimersByTimeAsync(9_999);
		assert.deepEqual(sent, [1]);
		await vi.advanceTimersByTimeAsync(1);
		assert.deepEqual(sent, [1, 2]);
	});

	test("a failed one doesn't stop the next", async () => {
		const queue = createLatestOnlyQueue<number>(10_000);
		const sent: number[] = [];
		let reject: ((error: Error) => void) | undefined;
		const send = (n: number) => {
			sent.push(n);
			return new Promise<void>((_resolve, rej) => {
				reject = rej;
			});
		};
		void queue.push(1, send);
		void queue.push(2, send);
		reject!(new Error("offline"));
		await vi.advanceTimersByTimeAsync(0);
		assert.deepEqual(sent, [1, 2]);
	});

	test("nothing waiting goes out once the broadcast is over", async () => {
		const queue = createLatestOnlyQueue<number>(10_000);
		const sent: number[] = [];
		let answer: (() => void) | undefined;
		const send = (n: number) => {
			sent.push(n);
			return new Promise<void>((resolve) => {
				answer = resolve;
			});
		};
		void queue.push(1, send);
		void queue.push(2, send);
		queue.clear();
		answer!();
		await vi.advanceTimersByTimeAsync(0);
		assert.deepEqual(sent, [1]);
	});
});
