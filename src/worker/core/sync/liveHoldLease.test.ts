import { assert, describe, test } from "vitest";
import { createLiveHoldLease, LIVE_HOLD_REFRESH_MS } from "./liveHoldLease.ts";

const flush = () => new Promise((resolve) => setTimeout(resolve, 0));

const setup = () => {
	let clock = 1_000_000;
	const writes: boolean[] = [];
	const fresh: boolean[] = [];
	let fail = false;
	const pending: (() => void)[] = [];
	let deferred = false;
	const lease = createLiveHoldLease({
		write: (held, isFresh) => {
			writes.push(held);
			fresh.push(isFresh);
			if (fail) {
				return Promise.reject(new Error("offline"));
			}
			if (deferred) {
				return new Promise<void>((resolve) => {
					pending.push(resolve);
				});
			}
			return Promise.resolve();
		},
		now: () => clock,
	});
	return {
		lease,
		writes,
		fresh,
		advance: (ms: number) => {
			clock += ms;
		},
		failing: (on: boolean) => {
			fail = on;
		},
		deferring: (on: boolean) => {
			deferred = on;
		},
		settle: () => {
			for (const resolve of pending.splice(0)) {
				resolve();
			}
		},
	};
};

describe("the push hold, this device's half", () => {
	test("says where it stands once on a connection, then only when that changes", async () => {
		const { lease, writes } = setup();
		lease.sync(false);
		await flush();
		lease.sync(false);
		lease.sync(false);
		await flush();
		assert.deepEqual(writes, [false]);
		lease.sync(true);
		await flush();
		assert.isTrue(lease.held());
		lease.sync(false);
		await flush();
		assert.deepEqual(writes, [false, true, false]);
		assert.isFalse(lease.held());
	});

	test("a hold that lasts is written again every so often - not every tick", async () => {
		const { lease, writes, advance } = setup();
		lease.sync(true);
		await flush();
		// (The health tick: every 5 seconds.)
		for (let i = 0; i < LIVE_HOLD_REFRESH_MS / 5000 - 1; i++) {
			advance(5000);
			lease.sync(true);
			await flush();
		}
		assert.deepEqual(writes, [true]);
		advance(5000);
		lease.sync(true);
		await flush();
		assert.deepEqual(writes, [true, true]);
		// (No hold needs no keeping up.)
		lease.sync(false);
		await flush();
		advance(LIVE_HOLD_REFRESH_MS * 5);
		lease.sync(false);
		await flush();
		assert.deepEqual(writes, [true, true, false]);
	});

	test("a hold just begun says so - kept up, it doesn't", async () => {
		const { lease, writes, fresh, advance } = setup();
		lease.sync(true);
		await flush();
		advance(LIVE_HOLD_REFRESH_MS);
		lease.sync(true);
		await flush();
		lease.sync(false);
		await flush();
		lease.sync(true);
		await flush();
		assert.deepEqual(writes, [true, true, false, true]);
		assert.deepEqual(fresh, [true, false, false, true]);
	});

	test("a write that fails is tried again on the next sync, not straight away", async () => {
		const { lease, writes, failing } = setup();
		failing(true);
		lease.sync(true);
		await flush();
		await flush();
		assert.deepEqual(writes, [true]);
		assert.isFalse(lease.held());
		failing(false);
		lease.sync(true);
		await flush();
		assert.deepEqual(writes, [true, true]);
		assert.isTrue(lease.held());
	});

	test("the game ending while the hold is still being written: lifted right after", async () => {
		const { lease, writes, deferring, settle } = setup();
		deferring(true);
		lease.sync(true);
		lease.sync(false);
		lease.sync(true);
		lease.sync(false);
		assert.deepEqual(writes, [true]);
		deferring(false);
		settle();
		await flush();
		await flush();
		assert.deepEqual(writes, [true, false]);
		assert.isFalse(lease.held());
	});
});
