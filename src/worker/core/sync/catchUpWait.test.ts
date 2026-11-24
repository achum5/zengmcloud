import { afterEach, assert, beforeEach, describe, test, vi } from "vitest";
import { waitForCatchUp, type CatchUpEngine } from "./catchUpWait.ts";

// A device `behind` versions behind the room, whose catch-up lands one
// version per pass after `passMs` - or fails while `failing` is set.
const makeEngine = ({
	behind,
	passMs = 500,
}: {
	behind: number;
	passMs?: number;
}) => {
	const state = {
		behind,
		failing: false,
		passes: 0,
		kicks: 0,
		running: false,
	};
	const pass = async () => {
		state.passes += 1;
		state.running = true;
		await new Promise((resolve) => setTimeout(resolve, passMs));
		state.running = false;
		if (state.failing) {
			return false;
		}
		state.behind = Math.max(0, state.behind - 1);
		return state.behind === 0;
	};
	const engine: CatchUpEngine = {
		isCaughtUp: () => state.behind === 0,
		catchUp: pass,
		kickCatchUp: () => {
			state.kicks += 1;
			if (!state.running && state.behind > 0) {
				void pass();
			}
		},
	};
	return { engine, state };
};

describe("waitForCatchUp", () => {
	beforeEach(() => {
		vi.useFakeTimers();
	});
	afterEach(() => {
		vi.useRealTimers();
	});

	test("already caught up: true at once, nothing started", async () => {
		const { engine, state } = makeEngine({ behind: 0 });
		assert.isTrue(await waitForCatchUp(engine));
		assert.strictEqual(state.kicks, 0);
		assert.strictEqual(state.passes, 0);
	});

	test("a league mate's change arriving: waits for it, then goes ahead", async () => {
		const { engine, state } = makeEngine({ behind: 1, passMs: 1_500 });
		let result: boolean | undefined;
		void waitForCatchUp(engine).then((value) => {
			result = value;
		});
		await vi.advanceTimersByTimeAsync(1_000);
		assert.isUndefined(result);
		await vi.advanceTimersByTimeAsync(700);
		assert.isTrue(result);
		assert.strictEqual(state.passes, 1);
	});

	test("several versions behind: keeps the walk going until caught up", async () => {
		const { engine } = makeEngine({ behind: 4, passMs: 1_000 });
		let result: boolean | undefined;
		void waitForCatchUp(engine).then((value) => {
			result = value;
		});
		await vi.advanceTimersByTimeAsync(15_000);
		assert.isTrue(result);
	});

	test("never catching up: false at the deadline", async () => {
		const { engine, state } = makeEngine({ behind: 1 });
		state.failing = true;
		let result: boolean | undefined;
		void waitForCatchUp(engine, { timeoutMs: 10_000 }).then((value) => {
			result = value;
		});
		await vi.advanceTimersByTimeAsync(9_000);
		assert.isUndefined(result);
		await vi.advanceTimersByTimeAsync(1_500);
		assert.isFalse(result);
	});

	test("the engine replaced mid-wait (a reconnect): false", async () => {
		const { engine } = makeEngine({ behind: 1, passMs: 5_000 });
		let current = true;
		let result: boolean | undefined;
		void waitForCatchUp(engine, { current: () => current }).then((value) => {
			result = value;
		});
		await vi.advanceTimersByTimeAsync(1_000);
		current = false;
		await vi.advanceTimersByTimeAsync(200);
		assert.isFalse(result);
	});

	describe("drain (a sim)", () => {
		test("caught up by its own count still runs a pass to the head", async () => {
			const { engine, state } = makeEngine({ behind: 0, passMs: 300 });
			let result: boolean | undefined;
			void waitForCatchUp(engine, { drain: true }).then((value) => {
				result = value;
			});
			await vi.advanceTimersByTimeAsync(400);
			assert.isTrue(result);
			assert.strictEqual(state.passes, 1);
		});

		test("a failed pass is tried again, not refused", async () => {
			const { engine, state } = makeEngine({ behind: 1, passMs: 300 });
			state.failing = true;
			let result: boolean | undefined;
			void waitForCatchUp(engine, { drain: true }).then((value) => {
				result = value;
			});
			await vi.advanceTimersByTimeAsync(2_000);
			assert.isUndefined(result);
			state.failing = false;
			await vi.advanceTimersByTimeAsync(1_000);
			assert.isTrue(result);
			assert.isAbove(state.passes, 2);
		});

		test("a pass still out at the deadline: false, not a hang", async () => {
			const { engine } = makeEngine({ behind: 1, passMs: 60_000 });
			let result: boolean | undefined;
			void waitForCatchUp(engine, { drain: true, timeoutMs: 5_000 }).then(
				(value) => {
					result = value;
				},
			);
			await vi.advanceTimersByTimeAsync(5_100);
			assert.isFalse(result);
		});
	});
});

// The play buttons on a signing or a trade fetch their 3D scene with a pure
// read. Not skipping capture, it went through the guard like an edit, and a
// tap as a league mate's change arrived said "Still catching up to the cloud".
test("scene reads behind play buttons skip capture, so the guard never stops them", async () => {
	const source: string = await import(("node" + ":fs") as any).then((fs: any) =>
		fs.readFileSync(
			new URL("../../index.ts", import.meta.url).pathname,
			"utf8",
		),
	);
	const skipBlock = source.slice(
		source.indexOf("SKIP_CHANGESET_CAPTURE"),
		source.indexOf("const isChangesetSuppressedCall"),
	);
	for (const name of [
		"getPressConference",
		"getHeadlineScene",
		"getDraftClock",
		"getReplayFeed",
	]) {
		assert.include(skipBlock, `"${name}"`);
	}
});
