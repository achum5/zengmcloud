import { afterAll, assert, beforeAll, test } from "vitest";
import { PHASE } from "../../../common/constants.ts";
import { makeSeededRandom } from "../../../common/random.ts";
import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import { createArenaLeague } from "../../../test/arena/runLeague.ts";
import { currentOutlook, loadTeam, priceFor } from "./market.ts";
import { signingsWith } from "./payroll.ts";
import {
	OUTLOOK_YEARS,
	outlookSettings,
	projectPlayer,
	teamOutlookSettings,
} from "./outlook.ts";

const importNode = (name: string) => import(/* @vite-ignore */ name);

const realRandom = Math.random;

beforeAll(async () => {
	await importNode("fake-indexeddb/auto");
	Math.random = makeSeededRandom(19);
	// The test setup shares modules between files, and another may have left a
	// stand-in database here; creating a league would try to close it.
	(idb as any).league = undefined;
	await createArenaLeague(9015);
	g.setWithoutSavingToDB("realisticFrontOffices", true);
	g.setWithoutSavingToDB("phase", PHASE.REGULAR_SEASON);
}, 120_000);

afterAll(() => {
	Math.random = realRandom;
	teamOutlookSettings.clear();
});

const youngest = async () =>
	(await idb.cache.players.indexGetAll("playersByTid", [0, Infinity]))
		.filter((p) => g.get("season") - p.born.year <= 21)
		.sort((a, b) => a.pid - b.pid);

test(
	"a player's halves straddle his average",
	{ timeout: 120_000 },
	async () => {
		for (const p of (await youngest()).slice(0, 20)) {
			const projection = projectPlayer(p, g.get("season"));
			for (let y = 1; y <= OUTLOOK_YEARS; y++) {
				const half = projection.halves[y]!;
				assert.isAtMost(half.ovr[0], projection.ovr[y]! + 1e-9);
				assert.isAtLeast(half.ovr[1], projection.ovr[y]! - 1e-9);
				assert.closeTo(
					(half.ovr[0] + half.ovr[1]) / 2,
					projection.ovr[y]!,
					1e-9,
				);
			}
		}
	},
);

test(
	"young players cost their teams more to give up when their upside counts",
	{ timeout: 120_000 },
	async () => {
		const outlook = await currentOutlook();
		let more = 0;
		let checked = 0;
		for (const p of (await youngest()).slice(0, 30)) {
			// The team's own worth as it stands is priced the same way.
			const cost = async (upside: boolean) => {
				teamOutlookSettings.set(p.tid, { upside });
				try {
					const t = await loadTeam(outlook, p.tid);
					return -priceFor(outlook, t, [{ kind: "player", p }], []);
				} finally {
					teamOutlookSettings.delete(p.tid);
				}
			};
			checked += 1;
			if ((await cost(true)) > (await cost(false))) {
				more += 1;
			}
		}
		assert.isTrue(outlookSettings.upside);
		assert.isAbove(checked, 10);
		assert.isAbove(more, (2 * checked) / 3);
	},
);

test("the same cap room buys a bad team less than a good one", () => {
	const room = 0.3 * g.get("salaryCap");
	const best = (winp: number) => signingsWith(room, false, winp)[0]!.valueNoPot;
	assert.isBelow(best(0.25), best(0.5));
	assert.isBelow(best(0.5), best(0.7));
	// Unknown: the league's average.
	assert.isAbove(best(0.7), signingsWith(room)[0]!.valueNoPot);
});
