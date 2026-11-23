import { afterEach, assert, beforeEach, describe, test, vi } from "vitest";
import { PHASE } from "../../../common/constants.ts";
import { resetCache, resetG } from "../../../test/helpers.ts";
import { g, local } from "../../util/index.ts";
import { setSyncEngine } from "./engineHolder.ts";
import {
	capDraftReadyForOnePick,
	setDraftReady,
	setupDraftReady,
	teardownDraftReady,
} from "./draftReady.ts";
import type { DraftReadyEntry } from "./types.ts";

type Ready = Record<string, DraftReadyEntry | null> | undefined;

// A draft of five teams, a pick each in round 1: R1P1..R1P5, all CPU teams
// but team 0's at pick 5.
const NUM_TEAMS = 5;
const draftPicks = [1, 2, 3, 4, 0].map((tid, i) => ({
	dpid: 100 + i,
	tid,
	originalTid: tid,
	round: 1,
	pick: i + 1,
	season: 2016,
}));

const stageKey = () => `${g.get("season")}-${PHASE.DRAFT}`;

const entry = (tid: number, untilPick: number): DraftReadyEntry => ({
	draftKey: stageKey(),
	tid,
	untilPick,
	name: `Team ${tid}`,
});

const makeRoom = () => {
	const published: { entry: DraftReadyEntry | null; clearUids?: string[] }[] =
		[];
	let onChange: (ready: Ready) => void = () => {};
	return {
		published,
		deliver: (ready: Ready) => onChange(ready),
		transport: {
			subscribeDraftReady(cb: (ready: Ready) => void) {
				onChange = cb;
				return () => {};
			},
			async publishDraftReady(e: DraftReadyEntry | null, clearUids?: string[]) {
				published.push({ entry: e, clearUids });
			},
		},
	};
};

describe("pulling back from a ready-up", () => {
	beforeEach(async () => {
		vi.useFakeTimers();
		resetG();
		await resetCache({ draftPicks });
		g.setWithoutSavingToDB("phase", PHASE.DRAFT);
		g.setWithoutSavingToDB("numActiveTeams", NUM_TEAMS);
		g.setWithoutSavingToDB("userTids", [0, 3]);
		g.setWithoutSavingToDB("userTid", 0);
		local.leagueLoaded = true;
		// The room mid-stretch: a league mate's device running a pick, this
		// one behind on it.
		setSyncEngine({
			clientId: "me",
			localName: "Me",
			isCaughtUp: () => false,
			isRoomBusy: () => true,
		} as any);
	});

	afterEach(() => {
		teardownDraftReady();
		setSyncEngine(undefined as any);
		local.leagueLoaded = false;
		vi.useRealTimers();
	});

	test("Ready for the next pick, mid-stretch, goes through", async () => {
		const room = makeRoom();
		setupDraftReady(room.transport as any);
		room.deliver({ me: entry(0, 5), mate: entry(3, 5) });

		await setDraftReady(1);
		assert.deepEqual(room.published.at(-1)?.entry?.untilPick, 1);
	});

	test("Not ready, mid-stretch, goes through", async () => {
		const room = makeRoom();
		setupDraftReady(room.transport as any);
		room.deliver({ me: entry(0, 5), mate: entry(3, 5) });

		await setDraftReady(null);
		assert.strictEqual(room.published.at(-1)?.entry, null);
	});

	test("Sim one pick cuts the team's ready-up back to the pick up", async () => {
		const room = makeRoom();
		setupDraftReady(room.transport as any);
		// This team readied up until its pick (5) from another device.
		room.deliver({ phone: entry(0, 5), mate: entry(3, 5) });

		await capDraftReadyForOnePick(undefined);
		const last = room.published.at(-1);
		assert.strictEqual(last?.entry?.untilPick, 1);
		assert.strictEqual(last?.entry?.tid, 0);
		// And the other device's further ready-up is taken back with it.
		assert.deepEqual(last?.clearUids, ["phone"]);
	});

	test("asked for the pick the page showed, already made: ready for nothing more", async () => {
		const room = makeRoom();
		setupDraftReady(room.transport as any);
		room.deliver({ me: entry(0, 5), mate: entry(3, 5) });

		// The page showed R1P0's pick (dpid 99), which the room has since made.
		await capDraftReadyForOnePick(99);
		assert.strictEqual(room.published.at(-1)?.entry?.untilPick, 0);
	});

	test("never readies a team up", async () => {
		const room = makeRoom();
		setupDraftReady(room.transport as any);

		// Not ready at all.
		room.deliver({ mate: entry(3, 5) });
		await capDraftReadyForOnePick(undefined);
		// Ready for just this pick already.
		room.deliver({ me: entry(0, 1), mate: entry(3, 5) });
		await capDraftReadyForOnePick(100);
		// Another team's ready-up is theirs.
		room.deliver({ me: entry(0, 1), mate: entry(3, 5) });
		await capDraftReadyForOnePick(undefined);

		assert.strictEqual(room.published.length, 0);
	});
});
