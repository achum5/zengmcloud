import { afterEach, assert, beforeEach, describe, test } from "vitest";
import { resetCache, resetG } from "../../../test/helpers.ts";
import { g } from "../../util/index.ts";
import { idb } from "../../db/index.ts";
import { setSyncEngine } from "./engineHolder.ts";
import { setupDraftReady, teardownDraftReady } from "./draftReady.ts";
import getSchedule from "../season/getSchedule.ts";
import {
	getTradeDeadlineGame,
	isTradeDeadlineGame,
	isTradeDeadlineGateActive,
	notifySimStopArrived,
	setTradeDeadlineGateActive,
	shouldStopAtSimStop,
	singleGameWaitsAtSimStop,
	getPendingSimStop,
	allowCrossingNextSimStop,
	clearCrossingNextSimStop,
} from "./tradeDeadlineGate.ts";

const makeEngine = () => {
	const notifications: any[] = [];
	return {
		engine: {
			localName: "Alex",
			publishNotification: async (n: any) => {
				notifications.push(n);
			},
		},
		notifications,
	};
};

// Schedule rows are keyed by gid and read back in gid order, which is the order
// they are played - the same assumption play.ts makes when it treats
// schedule[0] as "next". So a row's gid, not the order it is added here, is
// what puts it first.
const deadline = (gid: number) => ({
	gid,
	day: 40,
	homeTid: -3,
	awayTid: -3,
});
const allStarGame = (gid: number) => ({
	gid,
	day: 30,
	homeTid: -1,
	awayTid: -2,
});
const realGame = (gid: number) => ({ gid, day: 41, homeTid: 0, awayTid: 1 });

describe("recognising the deadline", () => {
	// The deadline lives in the schedule as a sentinel with both tids at -3. The
	// All-Star game is also a negative-tid sentinel, and it must never be
	// mistaken for the deadline.
	test("only the -3/-3 sentinel counts", () => {
		assert.strictEqual(isTradeDeadlineGame(deadline(1) as any), true);
		assert.strictEqual(isTradeDeadlineGame(allStarGame(1) as any), false);
		assert.strictEqual(isTradeDeadlineGame(realGame(1) as any), false);
		assert.strictEqual(isTradeDeadlineGame(undefined), false);
	});
});

describe("getTradeDeadlineGame", () => {
	beforeEach(() => {
		resetG();
	});

	// resetCache doesn't seed the schedule store, so these add rows directly.
	const seed = async (games: any[]) => {
		await resetCache();
		for (const game of games) {
			await idb.cache.schedule.add(game);
		}
	};

	test("finds it when it is the next thing on the schedule", async () => {
		await seed([deadline(500), realGame(501)]);
		assert.strictEqual((await getTradeDeadlineGame())?.gid, 500);
	});

	// The gate must not trip early. A deadline four days out is not a reason to
	// stop tonight's games, and treating it as one would freeze the league for
	// the rest of the season.
	test("ignores it while there are games to play first", async () => {
		await seed([realGame(500), deadline(501)]);
		assert.strictEqual(await getTradeDeadlineGame(), undefined);
	});

	test("no schedule, no deadline", async () => {
		await seed([]);
		assert.strictEqual(await getTradeDeadlineGame(), undefined);
	});

	test("the All-Star game is not it", async () => {
		await seed([allStarGame(500), deadline(501)]);
		assert.strictEqual(await getTradeDeadlineGame(), undefined);
	});

	// Once it has been crossed it is gone from the schedule, and the gate has to
	// stop reporting it or the league never sims again.
	test("crossing it clears the gate", async () => {
		await seed([deadline(500), realGame(501)]);
		assert.ok(await getTradeDeadlineGame());
		await idb.cache.schedule.delete(500);
		assert.strictEqual(await getTradeDeadlineGame(), undefined);
	});
});

describe("which stop the sim is standing in front of", () => {
	beforeEach(async () => {
		resetG();
		await resetCache({ schedule: [] });
	});

	// The deadline used to stop the league whether it wanted to or not. It is an
	// entry in a list now, and an empty list means nothing stops.
	test("nothing stops when no stop days are configured", async () => {
		g.setWithoutSavingToDB("simStopDays", "");
		await idb.cache.schedule.add(deadline(500) as any);
		assert.strictEqual(await getPendingSimStop(), undefined);
	});

	test("the deadline stops only when it is asked for", async () => {
		await idb.cache.schedule.add(deadline(500) as any);

		g.setWithoutSavingToDB("simStopDays", "15");
		assert.strictEqual(await getPendingSimStop(), undefined);

		g.setWithoutSavingToDB("simStopDays", "15, deadline");
		// The sentinel's day rides along: the ready-up gate turns it into a step
		// number, so readying up for an earlier stop cannot count for this one.
		assert.deepStrictEqual(await getPendingSimStop(), {
			kind: "deadline",
			gid: 500,
			day: 40,
		});
	});

	// A day stop fires when that day is NEXT, before it is played, which is what
	// makes the pause useful: you deal, then the day goes.
	test("a configured day stops before it is played", async () => {
		g.setWithoutSavingToDB("simStopDays", "41");
		await idb.cache.schedule.add(realGame(600) as any);
		assert.deepStrictEqual(await getPendingSimStop(), { kind: "day", day: 41 });
	});

	test("a day nobody asked about does not stop", async () => {
		g.setWithoutSavingToDB("simStopDays", "15, deadline");
		await idb.cache.schedule.add(realGame(600) as any);
		assert.strictEqual(await getPendingSimStop(), undefined);
	});

	// Crossing a day stop means PLAYING it, after which the next day on the
	// schedule is a different number - which is the whole reason there is no
	// cleared-list to keep in sync.
	test("playing the day is what clears it", async () => {
		g.setWithoutSavingToDB("simStopDays", "41");
		await idb.cache.schedule.add(realGame(600) as any);
		assert.ok(await getPendingSimStop());
		await idb.cache.schedule.delete(600);
		assert.strictEqual(await getPendingSimStop(), undefined);
	});
});

describe("shouldStopAtSimStop", () => {
	afterEach(() => {
		setTradeDeadlineGateActive(false);
		clearCrossingNextSimStop();
	});

	// The escape hatch. Gating means the ordinary sim path refuses to cross, so
	// one person who never readies up could strand the league - there was no
	// button anywhere that got past it. Confirming "Advance anyway" grants this,
	// and the ready-up evaluator uses the same thing to play a stop day.
	test("a granted crossing gets through a gate, once", () => {
		setTradeDeadlineGateActive(true);
		allowCrossingNextSimStop();
		assert.strictEqual(shouldStopAtSimStop(true), false);
		// Consumed - the next sim stops again.
		assert.strictEqual(shouldStopAtSimStop(true), true);
	});

	test("a granted crossing can be taken back", () => {
		setTradeDeadlineGateActive(true);
		allowCrossingNextSimStop();
		clearCrossingNextSimStop();
		assert.strictEqual(shouldStopAtSimStop(true), true);
	});

	// Alone: stop on arrival, cross on the next press. `start` is true only when
	// a play-menu action began the sim, so this is exactly "the sim ran into the
	// deadline" versus "the sim was started on top of it".
	test("alone, it stops on arrival and crosses on the next press", () => {
		setTradeDeadlineGateActive(false);
		assert.strictEqual(shouldStopAtSimStop(false), true);
		assert.strictEqual(shouldStopAtSimStop(true), false);
	});

	// Gated, pressing play harder must not be a way around the room - the
	// evaluator is the only thing that crosses.
	test("gated, it never crosses however the sim was started", () => {
		setTradeDeadlineGateActive(true);
		assert.strictEqual(shouldStopAtSimStop(false), true);
		assert.strictEqual(shouldStopAtSimStop(true), true);
	});

	// REGRESSION: a single-game sim used to route through shouldStopAtSimStop,
	// which CONSUMES the one-shot. A "Sim my game" press racing the ready-up
	// advance ate the advance's crossing permission, so the advance stopped at
	// the very gate it was sent to cross - and then falsely completed its
	// claim, sealing the step while the whole room showed 3/3 ready.
	test("a single-game sim waits at an armed gate and leaves the one-shot intact", () => {
		setTradeDeadlineGateActive(true);
		allowCrossingNextSimStop();
		// The interleaved single-game sim: waits, consumes nothing.
		assert.strictEqual(singleGameWaitsAtSimStop(), true);
		// The advance's own full-day sim still holds its permission.
		assert.strictEqual(shouldStopAtSimStop(true), false);
	});

	test("a single-game sim plays through a stop in a solo league, same as always", () => {
		setTradeDeadlineGateActive(false);
		assert.strictEqual(singleGameWaitsAtSimStop(), false);
	});

	// Gating means the normal sim path REFUSES to cross, so an armed gate with
	// nothing able to open it would wedge the league. It is armed and disarmed
	// by the evaluator's own lifecycle, and starts off.
	test("the gate is off until something arms it", () => {
		assert.strictEqual(isTradeDeadlineGateActive(), false);
		setTradeDeadlineGateActive(true);
		assert.strictEqual(isTradeDeadlineGateActive(), true);
		setTradeDeadlineGateActive(false);
		assert.strictEqual(isTradeDeadlineGateActive(), false);
	});
});

describe("notifyTradeDeadlineArrived", () => {
	beforeEach(() => {
		resetG();
		setTradeDeadlineGateActive(false);
	});

	afterEach(() => {
		setSyncEngine(undefined as any);
		setTradeDeadlineGateActive(false);
	});

	test("tells the whole room, and points at the trade page", async () => {
		const { engine, notifications } = makeEngine();
		setSyncEngine(engine as any);

		await notifySimStopArrived("Trade deadline");
		assert.strictEqual(notifications.length, 1);
		assert.strictEqual(notifications[0].targetTids, null);
		assert.strictEqual(notifications[0].path, "trade");
		assert.ok(notifications[0].title.includes("Trade deadline"));
		assert.ok(notifications[0].body.includes("ready up"));
	});

	// The sim can be retried any number of times while the room is stuck at the
	// deadline. Each retry reaches this again, and none of them is news.
	test("it announces once, not once per attempt", async () => {
		const { engine, notifications } = makeEngine();
		setSyncEngine(engine as any);

		await notifySimStopArrived("Trade deadline");
		await notifySimStopArrived("Trade deadline");
		await notifySimStopArrived("Trade deadline");
		assert.strictEqual(notifications.length, 1);
	});

	test("next season's deadline is news again", async () => {
		const { engine, notifications } = makeEngine();
		setSyncEngine(engine as any);

		await notifySimStopArrived("Trade deadline");
		g.setWithoutSavingToDB("season", g.get("season") + 1);
		await notifySimStopArrived("Trade deadline");
		assert.strictEqual(notifications.length, 2);
	});

	test("nothing is sent outside a shared league", async () => {
		setSyncEngine(undefined as any);
		// Must not throw, and must not need an engine.
		await notifySimStopArrived("Trade deadline");
	});

	// A dropped push must not silence the announcement forever.
	test("a failed push can be retried", async () => {
		let fail = true;
		const sent: any[] = [];
		setSyncEngine({
			localName: "Alex",
			publishNotification: async (n: any) => {
				if (fail) {
					throw new Error("offline");
				}
				sent.push(n);
			},
		} as any);

		await notifySimStopArrived("Trade deadline");
		assert.strictEqual(sent.length, 0);
		fail = false;
		await notifySimStopArrived("Trade deadline");
		assert.strictEqual(sent.length, 1);
	});

	test("disarming the gate lets a later room announce again", async () => {
		const { engine, notifications } = makeEngine();
		setSyncEngine(engine as any);

		await notifySimStopArrived("Trade deadline");
		assert.strictEqual(notifications.length, 1);
		// Leaving and rejoining a room is the same season, but a new room needs
		// to hear it.
		setTradeDeadlineGateActive(true);
		setTradeDeadlineGateActive(false);
		await notifySimStopArrived("Trade deadline");
		assert.strictEqual(notifications.length, 2);
	});
});

describe("the gate can always be opened", () => {
	afterEach(() => {
		teardownDraftReady();
	});

	// This is the dangerous direction. An armed gate makes the ordinary sim path
	// REFUSE to cross the deadline, so if it were ever armed without an evaluator
	// able to run the crossing, the league would be stuck there forever. It is
	// armed only for a transport that can do both halves of a ready-up.
	test("a transport that can't advance never arms it", () => {
		setupDraftReady({} as any);
		assert.strictEqual(isTradeDeadlineGateActive(), false);

		teardownDraftReady();
		setupDraftReady({ publishDraftReady: async () => {} } as any);
		assert.strictEqual(isTradeDeadlineGateActive(), false);

		teardownDraftReady();
		setupDraftReady({ claimDraftAdvance: async () => true } as any);
		assert.strictEqual(isTradeDeadlineGateActive(), false);
	});

	test("a transport that can advance arms it, and leaving disarms it", () => {
		setupDraftReady({
			claimDraftAdvance: async () => true,
			publishDraftReady: async () => {},
		} as any);
		assert.strictEqual(isTradeDeadlineGateActive(), true);

		teardownDraftReady();
		assert.strictEqual(isTradeDeadlineGateActive(), false);
	});
});

describe("the sim and the evaluator agree on what is next", () => {
	beforeEach(() => {
		resetG();
	});

	// play.ts decides from season.getSchedule(true)[0]; the ready-up stage decides
	// from getTradeDeadlineGame(). If those two ever disagreed, the sim would
	// refuse to cross a deadline the evaluator couldn't see, and the league would
	// wedge. They must be reading the same row.
	test("both read the same head of the schedule", async () => {
		await resetCache();
		for (const game of [deadline(500), realGame(501), realGame(502)]) {
			await idb.cache.schedule.add(game);
		}

		const simSees = (await getSchedule(true))[0];
		const gateSees = await getTradeDeadlineGame();
		assert.strictEqual(isTradeDeadlineGame(simSees), true);
		assert.strictEqual(gateSees?.gid, simSees?.gid);
	});

	test("and they agree when it is NOT next, too", async () => {
		await resetCache();
		for (const game of [realGame(500), deadline(501)]) {
			await idb.cache.schedule.add(game);
		}

		const simSees = (await getSchedule(true))[0];
		assert.strictEqual(isTradeDeadlineGame(simSees), false);
		assert.strictEqual(await getTradeDeadlineGame(), undefined);
	});
});
