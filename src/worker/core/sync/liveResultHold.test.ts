import { afterEach, assert, beforeEach, describe, test } from "vitest";
import { resetCache, resetG } from "../../../test/helpers.ts";
import { idb } from "../../db/index.ts";
import { g, local } from "../../util/index.ts";
import { PHASE } from "../../../common/constants.ts";
import {
	flushDeferredRefreshAfterLive,
	noticeLiveResults,
	refreshAfterApply,
	type Changeset,
} from "./changeset.ts";
import { createLiveResultWait } from "./liveBroadcastFollow.ts";
import { setLiveResultNotice, setLiveWatchGate } from "./liveWatchGate.ts";

// A league-mate live-sims a game. Its result syncs to this device on its own,
// usually AHEAD of the broadcast that pulls this device in to watch it - and
// nothing may show that result until the game has been watched here (or
// walked out of).

const replayRow = (gid: number, liveAt?: number) => ({
	store: "liveGamePlayByPlay" as const,
	id: gid,
	type: "put" as const,
	value: {
		gid,
		season: 2026,
		playByPlay: [],
		...(liveAt === undefined ? {} : { liveAt }),
	},
});
const gameRow = (gid: number) => ({
	store: "games" as const,
	id: gid,
	type: "put" as const,
	value: { gid, season: 2026, teams: [] },
});

describe("noticeLiveResults", () => {
	let noticed: [number, number][];
	beforeEach(async () => {
		resetG();
		await resetCache();
		noticed = [];
		setLiveResultNotice((gid, liveAt) => {
			noticed.push([gid, liveAt]);
		});
	});
	afterEach(() => {
		setLiveResultNotice(undefined);
	});

	test("a live game's result arriving is held", async () => {
		const at = Date.now();
		await noticeLiveResults({
			changes: [gameRow(7), replayRow(7, at)],
		} as Changeset);
		assert.deepStrictEqual(noticed, [[7, at]]);
	});

	test("the live marker landing ahead of the result is held too", async () => {
		const at = Date.now();
		await noticeLiveResults({ changes: [replayRow(7, at)] } as Changeset);
		assert.deepStrictEqual(noticed, [[7, at]]);
	});

	test("the same replay touched again once the game is in (the chat saved at the buzzer) is not", async () => {
		await idb.cache.games.put({ gid: 7, season: 2026, teams: [] } as any);
		await noticeLiveResults({
			changes: [replayRow(7, Date.now())],
		} as Changeset);
		assert.deepStrictEqual(noticed, []);
	});

	test("a game that was not live, or a long-stale one, is not", async () => {
		await noticeLiveResults({
			changes: [gameRow(7), replayRow(7)],
		} as Changeset);
		await noticeLiveResults({
			changes: [gameRow(8), replayRow(8, Date.now() - 2 * 60 * 60 * 1000)],
		} as Changeset);
		assert.deepStrictEqual(noticed, []);
	});
});

describe("createLiveResultWait", () => {
	const make = (following?: number) => {
		const timers: (() => void)[] = [];
		const calls: string[] = [];
		let watching = false;
		const wait = createLiveResultWait({
			take: () => calls.push("take"),
			release: () => calls.push("release"),
			isFollowing: (gid) => gid === following,
			isWatching: () => watching,
			schedule: (fn) => {
				timers.push(fn);
				return timers.length - 1;
			},
			cancel: (i) => {
				timers[i as number] = () => {};
			},
		});
		return {
			wait,
			calls,
			timers,
			setWatching: (w: boolean) => {
				watching = w;
			},
		};
	};

	test("holds the paint the moment the result lands, before any broadcast", () => {
		const { wait, calls } = make();
		wait.notice(7, 100);
		assert.deepStrictEqual(calls, ["take"]);
		assert.isTrue(wait.pending());
	});

	test("watched to the end (or walked out of): settled, and nothing left waiting", () => {
		const { wait, calls, timers } = make();
		wait.notice(7, 100);
		wait.settle(7);
		assert.isFalse(wait.pending());
		// Its timer, gone off later, does nothing.
		timers[0]!();
		assert.deepStrictEqual(calls, ["take"]);
	});

	test("while its broadcast is being watched, waiting running out releases nothing", () => {
		const { wait, calls, timers, setWatching } = make();
		wait.notice(7, 100);
		setWatching(true);
		timers[0]!();
		assert.deepStrictEqual(calls, ["take"]);
		assert.isFalse(wait.pending());
	});

	test("no broadcast ever got this device watching it: let go when waiting runs out", () => {
		const { wait, calls, timers } = make();
		wait.notice(7, 100);
		timers[0]!();
		assert.deepStrictEqual(calls, ["take", "release"]);
	});

	test("another game's result is not released by this one's waiting running out", () => {
		const { wait, calls, timers } = make();
		wait.notice(7, 100);
		wait.notice(8, 200);
		timers[0]!();
		assert.deepStrictEqual(calls, ["take", "take"]);
		assert.isTrue(wait.pending());
		timers[1]!();
		assert.deepStrictEqual(calls, ["take", "take", "release"]);
	});

	test("already inside (or walked out of) that game's broadcast: nothing more to hold", () => {
		const { wait, calls } = make(7);
		wait.notice(7, 100);
		assert.deepStrictEqual(calls, []);
		assert.isFalse(wait.pending());
	});

	test("the same result delivered twice is held once", () => {
		const { wait, calls } = make();
		wait.notice(7, 100);
		wait.settle(7);
		wait.notice(7, 100);
		assert.deepStrictEqual(calls, ["take"]);
	});
});

describe("a league-mate's live result, landing ahead of its broadcast", () => {
	afterEach(() => {
		setLiveResultNotice(undefined);
		setLiveWatchGate(undefined);
	});

	// The fire-and-forget flush needs a turn to land.
	const settle = () => new Promise((resolve) => setTimeout(resolve, 50));

	test("repaints nothing until the game has been watched here", async () => {
		resetG();
		await resetCache();
		local.liveSimGid = undefined;
		g.setWithoutSavingToDB("phase", PHASE.PLAYOFFS);
		local.phaseText = "stale";

		// Wired the way connect.ts wires it.
		let held = false;
		const wait = createLiveResultWait({
			take: () => {
				held = true;
			},
			release: () => {
				held = false;
				flushDeferredRefreshAfterLive();
			},
			isFollowing: () => false,
			isWatching: () => false,
			schedule: () => 0,
			cancel: () => {},
		});
		setLiveResultNotice(wait.notice);
		setLiveWatchGate(() => held || wait.pending());

		// The same order the sync engines take: notice, then the refresh.
		await noticeLiveResults({
			changes: [gameRow(7), replayRow(7, Date.now())],
		} as Changeset);
		await refreshAfterApply({
			touchedSeason: false,
			touchedGameAttributes: false,
			touchedGames: true,
			touchedPhase: true,
			touchedStatus: false,
			touchedStores: new Set<any>(["games"]),
			refreshUI: true,
			sweepGames: false,
			redirect: false,
		});
		await settle();
		assert.strictEqual(local.phaseText, "stale", "held while not yet watched");

		// Watched to the final buzzer here: now it paints.
		wait.settle(7);
		held = false;
		flushDeferredRefreshAfterLive();
		await settle();
		assert.strictEqual(local.phaseText, `${g.get("season")} playoffs`);
	});
});
