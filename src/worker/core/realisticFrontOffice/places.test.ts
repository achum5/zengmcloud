import { afterAll, assert, beforeAll, test } from "vitest";
import { PHASE } from "../../../common/constants.ts";
import { makeSeededRandom } from "../../../common/random.ts";
import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import { createArenaLeague } from "../../../test/arena/runLeague.ts";
import { currentOutlook, loadTeam, priceFor } from "./market.ts";
import { placesAfter } from "./outlook.ts";

const importNode = (name: string) => import(/* @vite-ignore */ name);

const realRandom = Math.random;

beforeAll(async () => {
	await importNode("fake-indexeddb/auto");
	Math.random = makeSeededRandom(5);
	// The test setup shares modules between files, and another may have left a
	// stand-in database here; creating a league would try to close it.
	(idb as any).league = undefined;
	await createArenaLeague(9010);
	g.setWithoutSavingToDB("realisticFrontOffices", true);
	g.setWithoutSavingToDB("phase", PHASE.REGULAR_SEASON);
}, 120_000);

afterAll(() => {
	Math.random = realRandom;
});

test(
	"a team that gives up its best player expects its own pick to land higher",
	{ timeout: 120_000 },
	async () => {
		const outlook = await currentOutlook();
		let moved = 0;
		let checked = 0;
		for (let tid = 0; tid < g.get("numTeams"); tid++) {
			const t = await loadTeam(outlook, tid);
			const own = t.picks.find(
				(dp) =>
					dp.originalTid === tid &&
					dp.round === 1 &&
					dp.season === g.get("season"),
			);
			if (!own) {
				continue;
			}
			const best = [...t.players].sort(
				(a, b) => b.valueNoPot - a.valueNoPot,
			)[0]!;
			const after = placesAfter(outlook, [
				{
					tid,
					before: t.players,
					after: t.players.filter((p) => p !== best),
				},
			]);
			const was = outlook.draftPlace.get(tid)![0]!;
			const now = after.draftPlace.get(tid)![0]!;
			checked += 1;
			// Never later; usually earlier.
			assert.isAtMost(now, was);
			if (now < was) {
				moved += 1;
			}
			// Nobody else's moves.
			for (let other = 0; other < g.get("numTeams"); other++) {
				if (other !== tid) {
					assert.strictEqual(
						after.draftPlace.get(other),
						outlook.draftPlace.get(other),
					);
				}
			}
		}
		assert.isAbove(checked, 10);
		assert.isAbove(moved, checked / 2);
	},
);

test(
	"giving up its star and its own pick costs a team more than the two apart",
	{ timeout: 120_000 },
	async () => {
		const outlook = await currentOutlook();
		let more = 0;
		let checked = 0;
		for (let tid = 0; tid < g.get("numTeams"); tid++) {
			const t = await loadTeam(outlook, tid);
			const own = t.picks.find(
				(dp) =>
					dp.originalTid === tid &&
					dp.round === 1 &&
					dp.season === g.get("season"),
			);
			if (!own || outlook.draftPlace.get(tid)![0]! < 5) {
				continue;
			}
			const best = [...t.players].sort(
				(a, b) => b.valueNoPot - a.valueNoPot,
			)[0]!;
			const star = { kind: "player" as const, p: best };
			const pick = { kind: "pick" as const, dp: own };
			const both = -priceFor(outlook, t, [star, pick], []);
			const apart =
				-priceFor(outlook, t, [star], []) - priceFor(outlook, t, [pick], []);
			checked += 1;
			if (both > apart) {
				more += 1;
			}
		}
		assert.isAbove(checked, 5);
		assert.isAbove(more, checked / 2);
	},
);
