import { afterEach, assert, beforeEach, describe, test, vi } from "vitest";
import { PHASE } from "../../../common/constants.ts";
import { resetCache, resetG } from "../../../test/helpers.ts";
import { g, local } from "../../util/index.ts";
import { setSyncEngine } from "./engineHolder.ts";
import {
	setupDraftReady,
	shownPhaseReady,
	teardownDraftReady,
} from "./draftReady.ts";
import type { DraftReadyEntry } from "./types.ts";

type Ready = Record<string, DraftReadyEntry | null> | undefined;

const ready = (...tids: number[]): Ready =>
	Object.fromEntries(
		tids.map((tid) => [
			`device${tid}`,
			{
				draftKey: `${g.get("season")}-${PHASE.PRESEASON}`,
				tid,
				untilPick: 1,
				name: `Team ${tid}`,
			},
		]),
	);

// A room's ready doc: a listener that can go quiet or fail, and the server's
// copy, which may have moved on without it.
const makeTransport = () => {
	const listeners: {
		onChange: (ready: Ready) => void;
		onError?: (error: unknown) => void;
		live: boolean;
	}[] = [];
	let server: Promise<Ready> = Promise.resolve(undefined);
	return {
		listeners,
		setServer(next: Ready | Promise<Ready>) {
			server = Promise.resolve(next);
		},
		transport: {
			subscribeDraftReady(
				onChange: (ready: Ready) => void,
				onError?: (error: unknown) => void,
			) {
				const listener = { onChange, onError, live: true };
				listeners.push(listener);
				return () => {
					listener.live = false;
				};
			},
			fetchDraftReady: () => server,
		},
	};
};

describe("the ready count the header shows", () => {
	beforeEach(async () => {
		vi.useFakeTimers();
		resetG();
		await resetCache();
		g.setWithoutSavingToDB("phase", PHASE.PRESEASON);
		g.setWithoutSavingToDB("userTids", [0, 1, 2]);
		g.setWithoutSavingToDB("userTid", 0);
		local.leagueLoaded = true;
		// Never caught up, so nothing here tries to notify or advance.
		setSyncEngine({ isCaughtUp: () => false } as any);
	});

	afterEach(() => {
		teardownDraftReady();
		setSyncEngine(undefined as any);
		local.leagueLoaded = false;
		vi.useRealTimers();
	});

	// The field report: loaded into the league, "0/3" - with two teams ready,
	// this one among them - until a refresh. The listener had what it had and
	// never heard another word.
	test("a listener that has gone quiet is overruled by the server", async () => {
		const room = makeTransport();
		room.setServer(ready(0, 1));
		setupDraftReady(room.transport as any);
		room.listeners[0]!.onChange(ready());
		await vi.advanceTimersByTimeAsync(10);

		assert.strictEqual(shownPhaseReady()?.readyTeams, 2);
		assert.strictEqual(shownPhaseReady()?.totalTeams, 3);
		assert.strictEqual(shownPhaseReady()?.ready, true);
		// And a new listener in place of the one that missed it.
		assert.strictEqual(room.listeners.length, 2);
		assert.strictEqual(room.listeners[0]!.live, false);
	});

	test("and is asked again while the stage lasts, not only on connecting", async () => {
		const room = makeTransport();
		room.setServer(ready(0));
		setupDraftReady(room.transport as any);
		room.listeners[0]!.onChange(ready(0));
		await vi.advanceTimersByTimeAsync(10);
		assert.strictEqual(shownPhaseReady()?.readyTeams, 1);

		// Someone readies up and the listener never hears of it.
		room.setServer(ready(0, 1));
		await vi.advanceTimersByTimeAsync(20_000);
		assert.strictEqual(shownPhaseReady()?.readyTeams, 2);
	});

	test("nothing heard yet is no count at all, not 0/3", async () => {
		const room = makeTransport();
		room.setServer(new Promise<Ready>(() => {}));
		setupDraftReady(room.transport as any);
		await vi.advanceTimersByTimeAsync(4000);
		assert.strictEqual(shownPhaseReady(), undefined);

		room.listeners[0]!.onChange(ready(0, 1));
		await vi.advanceTimersByTimeAsync(10);
		assert.strictEqual(shownPhaseReady()?.readyTeams, 2);
	});

	test("a listener that fails is built again", async () => {
		const room = makeTransport();
		setupDraftReady(room.transport as any);
		room.listeners[0]!.onError?.(new Error("permission-denied"));
		await vi.advanceTimersByTimeAsync(5000);
		assert.strictEqual(room.listeners.length, 2);
		assert.strictEqual(room.listeners[1]!.live, true);
	});

	test("an answer that crossed a newer delivery is thrown away", async () => {
		const room = makeTransport();
		let answer: (ready: Ready) => void = () => {};
		room.setServer(
			new Promise<Ready>((resolve) => {
				answer = resolve;
			}),
		);
		setupDraftReady(room.transport as any);
		room.listeners[0]!.onChange(ready(0));
		await vi.advanceTimersByTimeAsync(10);

		// The listener hears the second team while the server read is out; the
		// read then comes back with the room as it was before that.
		room.listeners[0]!.onChange(ready(0, 1));
		answer(ready(0));
		await vi.advanceTimersByTimeAsync(10);
		assert.strictEqual(shownPhaseReady()?.readyTeams, 2);
		assert.strictEqual(room.listeners.length, 1);
	});
});
