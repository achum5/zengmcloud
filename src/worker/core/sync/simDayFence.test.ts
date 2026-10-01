import { afterEach, assert, beforeEach, describe, test } from "vitest";
import { resetG } from "../../../test/helpers.ts";
import { g } from "../../util/index.ts";
import { setSyncEngine } from "./engineHolder.ts";
import {
	claimSimDayFence,
	completeClaimedSimDayFence,
	completeDeferredSimDayFence,
	fencedGamesIn,
	lastSimDayRejection,
	revalidateQueuedSingleGame,
	setupSimDayFence,
	teardownSimDayFence,
} from "./simDayFence.ts";
import type { SimDayClaimDoc } from "./simDayClaimPolicy.ts";

// A transport that is only the fence: what was claimed, what was completed,
// and whatever document the test says the room holds.
const makeTransport = (clientId = "me") => {
	const claims: { day: number; gids: number[] }[] = [];
	const completions: { day: number; gids: number[] }[] = [];
	const state: { doc: SimDayClaimDoc | undefined } = { doc: undefined };
	const transport = {
		clientId,
		claimSimDay: async (_stage: string, day: number, gids: number[]) => {
			claims.push({ day, gids });
			return true;
		},
		completeSimDay: async (_stage: string, day: number, gids: number[]) => {
			completions.push({ day, gids });
		},
		readSimDayClaim: async () => state.doc,
	};
	return { transport, claims, completions, state };
};

const gameEntry = (action: string, games: { gid: number; day: number }[]) => ({
	action,
	changeset: {
		changes: games.map((gm) => ({
			store: "games" as any,
			id: gm.gid,
			type: "put" as const,
			value: { gid: gm.gid, day: gm.day },
		})),
	},
});

describe("simDayFence completion", () => {
	beforeEach(() => {
		resetG();
		g.setWithoutSavingToDB("season", 2026);
		setSyncEngine({ catchUp: async () => true } as any);
	});
	afterEach(() => {
		teardownSimDayFence();
		setSyncEngine(undefined);
	});

	test("a confirmed publish completes the slice at once", async () => {
		const { transport, completions } = makeTransport();
		setupSimDayFence(transport as any);
		assert.isTrue(await claimSimDayFence(12, [77]));
		completeClaimedSimDayFence({ synced: true, singleGame: true });
		await Promise.resolve();
		assert.deepEqual(completions, [{ day: 12, gids: [77] }]);
	});

	// THE PHONE PUT AWAY. "Sim my game", pocket the phone before the upload
	// lands: the result is durably queued and WILL go up when the app comes
	// back, so the slice must not be left to a 90-second lease that hands the
	// game to the room's next scheduled sim as crash recovery.
	test("a queued single game completes when its upload lands, not before", async () => {
		const { transport, completions } = makeTransport();
		setupSimDayFence(transport as any);
		assert.isTrue(await claimSimDayFence(12, [77]));
		completeClaimedSimDayFence({ synced: false, singleGame: true });
		await Promise.resolve();
		assert.deepEqual(completions, [], "nothing confirmed yet");

		completeDeferredSimDayFence();
		await Promise.resolve();
		assert.deepEqual(completions, [{ day: 12, gids: [77] }]);
		// Once only.
		completeDeferredSimDayFence();
		await Promise.resolve();
		assert.strictEqual(completions.length, 1);
	});

	test("a queued whole DAY is left to its lease, as before", async () => {
		// A day advance can still be discarded by the engine if it lost a race;
		// completing it on a later unrelated upload would fence games the room
		// never received.
		const { transport, completions } = makeTransport();
		setupSimDayFence(transport as any);
		assert.isTrue(await claimSimDayFence(12, [70, 71, 72]));
		completeClaimedSimDayFence({ synced: false, singleGame: false });
		completeDeferredSimDayFence();
		await Promise.resolve();
		assert.deepEqual(completions, []);
	});
});

describe("claim rejection reason", () => {
	beforeEach(() => {
		resetG();
		g.setWithoutSavingToDB("season", 2026);
		setSyncEngine({ catchUp: async () => true } as any);
	});
	afterEach(() => {
		teardownSimDayFence();
		setSyncEngine(undefined);
	});

	test("the transport's reason is kept for the message, and cleared by a grant", async () => {
		const t = makeTransport();
		let grant = false;
		const transport = {
			...t.transport,
			lastSimDayClaimRejection: undefined as string | undefined,
			claimSimDay: async () => {
				transport.lastSimDayClaimRejection = grant
					? undefined
					: "day-already-run";
				return grant;
			},
		};
		setupSimDayFence(transport as any);
		assert.isFalse(await claimSimDayFence(79, [21828]));
		assert.strictEqual(lastSimDayRejection(), "day-already-run");
		grant = true;
		assert.isTrue(await claimSimDayFence(90, [30000]));
		assert.isUndefined(lastSimDayRejection());
	});
});

describe("revalidateQueuedSingleGame", () => {
	beforeEach(() => {
		resetG();
		g.setWithoutSavingToDB("season", 2026);
		setSyncEngine({ catchUp: async () => true } as any);
	});
	afterEach(() => {
		teardownSimDayFence();
		setSyncEngine(undefined);
	});

	const stage = "sim:2026";
	const entry = gameEntry("playMenu.simGame", [{ gid: 77, day: 12 }]);

	// Claim, publish attempt fails, completion deferred - the state every
	// re-validation starts from.
	const queueOne = async (t: ReturnType<typeof makeTransport>) => {
		setupSimDayFence(t.transport as any);
		assert.isTrue(await claimSimDayFence(12, [77]));
		completeClaimedSimDayFence({ synced: false, singleGame: true });
	};

	test("reads the fenced games off the changeset", () => {
		assert.deepEqual(fencedGamesIn(entry), { day: 12, gids: [77] });
		assert.isUndefined(
			fencedGamesIn(gameEntry("main.updatePlayingTime", [{ gid: 1, day: 2 }])),
			"not a single-game label",
		);
		assert.isUndefined(
			fencedGamesIn({ action: "playMenu.simGame", changeset: { changes: [] } }),
		);
	});

	test("a first attempt is never re-validated", async () => {
		const t = makeTransport();
		setupSimDayFence(t.transport as any);
		assert.isTrue(await claimSimDayFence(12, [77]));
		// Another device took the holder field with a disjoint slice; our own
		// in-flight publish must still go up without asking.
		t.state.doc = {
			holderId: "someone",
			stageKey: stage,
			day: 12,
			gids: [77, 78],
			at: Date.now(),
			maxDay: 12,
			completedGids: [],
		};
		assert.strictEqual(await revalidateQueuedSingleGame(entry), "publish");
	});

	// THE RESTARTED PHONE: the result was queued in an earlier session, so this
	// session has no record of it - and the room has since played on.
	test("a result queued before a restart is still re-validated", async () => {
		const t = makeTransport("me");
		setupSimDayFence(t.transport as any);
		t.state.doc = {
			holderId: "simmer",
			stageKey: stage,
			day: 30,
			gids: [200],
			at: Date.now(),
			maxDay: 30,
			completedGids: [200],
		};
		assert.strictEqual(await revalidateQueuedSingleGame(entry), "drop");
	});

	test("a result queued before a restart publishes if still ours to give", async () => {
		const t = makeTransport("me");
		setupSimDayFence(t.transport as any);
		const claimsBefore = t.claims.length;
		t.state.doc = {
			holderId: "old-session",
			stageKey: stage,
			day: 12,
			gids: [77],
			at: Date.now() - 10 * 60_000,
			maxDay: 12,
			completedGids: [],
		};
		assert.strictEqual(await revalidateQueuedSingleGame(entry), "publish");
		assert.strictEqual(t.claims.length, claimsBefore + 1, "re-claimed");
	});

	test("our own live claim publishes", async () => {
		const t = makeTransport("me");
		await queueOne(t);
		t.state.doc = {
			holderId: "me",
			stageKey: stage,
			day: 12,
			gids: [77],
			at: Date.now() - 10_000,
			maxDay: 12,
			completedGids: [],
		};
		assert.strictEqual(await revalidateQueuedSingleGame(entry), "publish");
	});

	// THE REPLAYED GAME, from the other side: the room simmed it while we were
	// away. Our result is now a second sim of a game the room already has.
	test("a game the room completed elsewhere is dropped", async () => {
		const t = makeTransport("me");
		await queueOne(t);
		t.state.doc = {
			holderId: "simmer",
			stageKey: stage,
			day: 12,
			gids: [77, 78, 79],
			at: Date.now() - 5_000,
			maxDay: 12,
			completedGids: [77, 78, 79],
		};
		assert.strictEqual(await revalidateQueuedSingleGame(entry), "drop");
		// And nothing left to complete later - completing it would re-fence a
		// game that is not ours.
		completeDeferredSimDayFence();
		await Promise.resolve();
		assert.deepEqual(t.completions, []);
	});

	test("a day the room has moved past is dropped", async () => {
		const t = makeTransport("me");
		await queueOne(t);
		t.state.doc = {
			holderId: "simmer",
			stageKey: stage,
			day: 14,
			gids: [90],
			at: Date.now(),
			maxDay: 14,
			completedGids: [],
		};
		assert.strictEqual(await revalidateQueuedSingleGame(entry), "drop");
	});

	test("somebody mid-sim on these games means wait", async () => {
		const t = makeTransport("me");
		await queueOne(t);
		t.state.doc = {
			holderId: "simmer",
			stageKey: stage,
			day: 12,
			gids: [77],
			at: Date.now() - 1_000,
			maxDay: 12,
			completedGids: [],
		};
		assert.strictEqual(await revalidateQueuedSingleGame(entry), "wait");
	});

	test("a lapsed lease is re-taken and the result publishes", async () => {
		const t = makeTransport("me");
		await queueOne(t);
		const claimsBefore = t.claims.length;
		t.state.doc = {
			holderId: "simmer",
			stageKey: stage,
			day: 12,
			gids: [77, 78],
			at: Date.now() - 10 * 60_000,
			maxDay: 12,
			completedGids: [78],
		};
		assert.strictEqual(await revalidateQueuedSingleGame(entry), "publish");
		assert.strictEqual(t.claims.length, claimsBefore + 1, "re-claimed");
	});

	test("an entry that is not a single game is never questioned", async () => {
		const t = makeTransport("me");
		await queueOne(t);
		t.state.doc = {
			holderId: "simmer",
			stageKey: stage,
			day: 12,
			gids: [77],
			at: Date.now(),
			maxDay: 12,
			completedGids: [77],
		};
		assert.strictEqual(
			await revalidateQueuedSingleGame(
				gameEntry("main.updatePlayingTime", [{ gid: 77, day: 12 }]),
			),
			"publish",
		);
	});
});

// THE FIELD CAPTURE: playoff games simmed one after another on the device in
// charge of simming. The gameSim lock frees before a game finishes uploading, so
// the second "Sim game" claimed while the first was still publishing, and the
// first call's drain then swept the second game's records into its own entry.
// The first game's completion used to close the SECOND game's claim (one shared
// slot), that entry was then refused as "games-already-simmed" on the very
// device that simmed it, and the game never reached anyone else.
describe("overlapping single-game sims", () => {
	beforeEach(() => {
		resetG();
		g.setWithoutSavingToDB("season", 2016);
		setSyncEngine({ catchUp: async () => true } as any);
	});
	afterEach(() => {
		teardownSimDayFence();
		setSyncEngine(undefined);
	});

	const stage = "sim:2016";

	test("each game closes its own claim, and the swept game still publishes", async () => {
		const t = makeTransport("me");
		setupSimDayFence(t.transport as any);
		assert.isTrue(await claimSimDayFence(92, [21995], { singleGame: true }));
		assert.isTrue(await claimSimDayFence(92, [21998], { singleGame: true }));

		// Game 1 lands and closes - only game 1.
		completeClaimedSimDayFence({ synced: true, singleGame: true, gids: [21995] });
		await Promise.resolve();
		assert.deepEqual(t.completions, [{ day: 92, gids: [21995] }]);

		// Game 2's records went out under the generic label. Room doc as the
		// transport would have it after game 1's completion.
		t.state.doc = {
			holderId: "me",
			stageKey: stage,
			day: 92,
			gids: [21995, 21998],
			at: Date.now(),
			maxDay: 92,
			completedGids: [21995],
		};
		const swept = gameEntry("actions.simGame", [{ gid: 21998, day: 92 }]);
		assert.strictEqual(await revalidateQueuedSingleGame(swept), "publish");

		// Game 2's own drain found nothing (already swept) while that entry was
		// still on its way up: deferred, not closed.
		completeClaimedSimDayFence({
			synced: false,
			singleGame: true,
			gids: [21998],
		});
		await Promise.resolve();
		assert.strictEqual(t.completions.length, 1);
		// The deferred game is still recognised as ours by the room doc.
		assert.strictEqual(await revalidateQueuedSingleGame(swept), "publish");

		completeDeferredSimDayFence();
		await Promise.resolve();
		assert.deepEqual(t.completions[1], { day: 92, gids: [21998] });
	});

	test("a game this session closed itself is never dropped as someone else's", async () => {
		const t = makeTransport("me");
		setupSimDayFence(t.transport as any);
		assert.isTrue(await claimSimDayFence(92, [21998], { singleGame: true }));
		completeClaimedSimDayFence({ synced: true, singleGame: true, gids: [21998] });
		t.state.doc = {
			holderId: "someone-else",
			stageKey: stage,
			day: 92,
			gids: [21998, 21999],
			at: Date.now(),
			maxDay: 92,
			completedGids: [21998],
		};
		assert.strictEqual(
			await revalidateQueuedSingleGame(
				gameEntry("actions.simGame", [{ gid: 21998, day: 92 }]),
			),
			"publish",
		);
	});

	test("a day sim's completion is not taken by a single game claimed after it", async () => {
		const t = makeTransport("me");
		setupSimDayFence(t.transport as any);
		assert.isTrue(await claimSimDayFence(40, [1, 2, 3]));
		assert.isTrue(await claimSimDayFence(40, [9], { singleGame: true }));
		completeClaimedSimDayFence({ synced: true, singleGame: false });
		await Promise.resolve();
		assert.deepEqual(t.completions, [{ day: 40, gids: [1, 2, 3] }]);
	});
});
