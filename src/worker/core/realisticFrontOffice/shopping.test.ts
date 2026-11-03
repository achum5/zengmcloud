import { afterAll, assert, beforeAll, test } from "vitest";
import { PHASE } from "../../../common/constants.ts";
import { makeSeededRandom } from "../../../common/random.ts";
import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import { createArenaLeague } from "../../../test/arena/runLeague.ts";
import {
	bestDealWith,
	currentOutlook,
	loadTeam,
	MIN_GAIN,
	teamMarketSettings,
} from "./market.ts";

// How a team out shopping picks among the deals both teams would make.

const importNode = (name: string) => import(/* @vite-ignore */ name);

const realRandom = Math.random;

beforeAll(async () => {
	await importNode("fake-indexeddb/auto");
	Math.random = makeSeededRandom(37);
	(idb as any).league = undefined;
	await createArenaLeague(9019);
	g.setWithoutSavingToDB("realisticFrontOffices", true);
	g.setWithoutSavingToDB("phase", PHASE.REGULAR_SEASON);
}, 120_000);

afterAll(() => {
	Math.random = realRandom;
});

test(
	"a team shopping for itself takes the deal that does most for it that the other team would still make",
	{ timeout: 300_000 },
	async () => {
		const outlook = await currentOutlook();
		const own = (deal: { gains: number[]; margins: number[] }) =>
			deal.gains[0]! - deal.margins[0]!;
		// In this league team 13 has a deal with team 21 that both would make.
		const shopper = await loadTeam(outlook, 13);
		const partner = await loadTeam(outlook, 21);
		try {
			const usual = bestDealWith(outlook, shopper, partner, new Map());
			teamMarketSettings.set(shopper.tid, { selfishShopping: true });
			const selfish = bestDealWith(outlook, shopper, partner, new Map());
			assert.isDefined(usual);
			assert.isDefined(selfish);
			// Usually the deal that does most for the side it does least for:
			// shopping for itself, team 13 does far better, and team 21 still
			// clears its margin.
			assert.isAbove(own(selfish!), 2 * own(usual!));
			assert.isAtLeast(selfish!.gains[1] - selfish!.margins[1], MIN_GAIN);
		} finally {
			teamMarketSettings.clear();
		}
	},
);
