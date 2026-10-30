import { afterAll, assert, beforeAll, test } from "vitest";
import { PHASE } from "../../../common/constants.ts";
import { makeSeededRandom } from "../../../common/random.ts";
import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import { createArenaLeague } from "../../../test/arena/runLeague.ts";
import { draft } from "../index.ts";
import { humanTradeSettings } from "./humanTrades.ts";
import {
	currentOutlook,
	loadTeam,
	marginOf,
	MIN_GAIN,
	priceFor,
} from "./market.ts";
import {
	buildOutlook,
	expectedPlace,
	OUTLOOK_YEARS,
	pickLineAt,
	type LeagueOutlook,
} from "./outlook.ts";
import { valueOverReplacement } from "./strength.ts";

const importNode = (name: string) => import(/* @vite-ignore */ name);

const realRandom = Math.random;

beforeAll(async () => {
	await importNode("fake-indexeddb/auto");
	Math.random = makeSeededRandom(17);
	// The test setup shares modules between files, and another may have left a
	// stand-in database here; creating a league would try to close it.
	(idb as any).league = undefined;
	await createArenaLeague(9014);
	g.setWithoutSavingToDB("realisticFrontOffices", true);
	g.setWithoutSavingToDB("phase", PHASE.REGULAR_SEASON);
}, 120_000);

afterAll(() => {
	Math.random = realRandom;
});

// What a pick is worth over the outlook, held by `holder`: its player's value
// over a replacement each season, as far as he is expected to be there.
const worth = (
	outlook: LeagueOutlook,
	originalTid: number,
	{ round = 1, holder = originalTid } = {},
) => {
	const dp = { originalTid, round, pick: 0, season: g.get("season") };
	let sum = 0;
	for (let y = 0; y <= OUTLOOK_YEARS; y++) {
		const at = pickLineAt(outlook, dp, y, 0, holder);
		if (at) {
			sum += at.line.availability * valueOverReplacement(at.line);
		}
	}
	return sum;
};

const withoutLottery = (outlook: LeagueOutlook): LeagueOutlook => ({
	...outlook,
	slotPicks: undefined,
	cola: undefined,
	pickLines: new Map(),
	pickYears: new WeakMap(),
});

const byPlace = (outlook: LeagueOutlook) =>
	[...outlook.draftPlace.entries()]
		.sort((a, b) => a[1][0]! - b[1][0]!)
		.map(([tid]) => tid);

test(
	"under COLA the worst team's pick is worth less, and less again traded",
	{ timeout: 120_000 },
	async () => {
		assert.strictEqual(g.get("draftType"), "cola");
		const outlook = await currentOutlook();
		assert.isDefined(outlook.cola);
		const worst = byPlace(outlook)[0]!;
		const other = byPlace(outlook)[10]!;
		const plain = withoutLottery(outlook);
		const own = worth(outlook, worst);
		assert.isBelow(own, worth(plain, worst));
		// Held by another team, it is out of the draw.
		assert.isBelow(worth(outlook, worst, { holder: other }), own);
		// The second round has no draw.
		assert.closeTo(
			worth(outlook, worst, { round: 2 }),
			worth(plain, worst, { round: 2 }),
			1e-9,
		);
	},
);

test(
	"under the NBA's lottery the worst team's pick is worth less, the best's about the same",
	{ timeout: 120_000 },
	async () => {
		g.setWithoutSavingToDB("draftType", "nba2027");
		try {
			const outlook = await buildOutlook({ iterations: 100 });
			assert.isUndefined(outlook.cola);
			const order = byPlace(outlook);
			const plain = withoutLottery(outlook);
			assert.isBelow(worth(outlook, order[0]!), worth(plain, order[0]!));
			// Hardly ever in the lottery.
			const best = worth(plain, order.at(-1)!);
			assert.closeTo(worth(outlook, order.at(-1)!), best, 0.01 * best);
		} finally {
			g.setWithoutSavingToDB("draftType", "cola");
		}
	},
);

test(
	"before the lottery, this season's picks go by how teams finished",
	{ timeout: 120_000 },
	async () => {
		const season = g.get("season");
		// The season played out: the lower the tid the worse, the top half in
		// the playoffs.
		for (const ts of await idb.cache.teamSeasons.indexGetAll(
			"teamSeasonsBySeasonTid",
			[[season], [season, "Z"]],
		)) {
			ts.won = 20 + ts.tid;
			ts.lost = 82 - ts.won;
			ts.playoffRoundsWon = ts.tid >= g.get("numTeams") / 2 ? 0 : -1;
			await idb.cache.teamSeasons.put(ts);
		}
		g.setWithoutSavingToDB("phase", PHASE.DRAFT_LOTTERY);
		const outlook = await currentOutlook();
		for (const tid of [0, 5, g.get("numTeams") - 1]) {
			assert.strictEqual(outlook.finalPlace?.get(tid), tid);
			assert.strictEqual(
				expectedPlace(outlook, {
					originalTid: tid,
					round: 1,
					pick: 0,
					season,
				}),
				tid,
			);
		}
		g.setWithoutSavingToDB("phase", PHASE.REGULAR_SEASON);
	},
);

test(
	"once the draft order is set, a pick carries the doubt of the player it is priced as",
	{ timeout: 300_000 },
	async () => {
		const season = g.get("season");
		g.setWithoutSavingToDB("phase", PHASE.DRAFT);
		try {
			await draft.genOrder(false);
			const picks = await idb.cache.draftPicks.indexGetAll(
				"draftPicksBySeason",
				season,
			);
			const at = (overall: number) =>
				picks.find(
					(dp) => (dp.round - 1) * g.get("numTeams") + dp.pick === overall,
				)!;
			const outlook = await currentOutlook();
			assert.isDefined(outlook.classBoard);
			const first = at(1);
			const t = await loadTeam(outlook, first.tid);
			assert.isAbove(
				marginOf(outlook, t, [{ kind: "pick", dp: first }], []),
				0,
			);

			// Picks 30 and 58 for pick 15 (owner's exploit list).
			const want = at(15);
			const other = (want.tid + 1) % g.get("numTeams");
			const give = [at(30), at(58)];
			for (const dp of give) {
				dp.tid = other;
				await idb.cache.draftPicks.put(dp);
			}
			const seller = await loadTeam(outlook, want.tid);
			const buyer = await loadTeam(outlook, other);
			const outgoing = [{ kind: "pick" as const, dp: want }];
			const incoming = give.map((dp) => ({ kind: "pick" as const, dp }));
			assert.isBelow(
				priceFor(outlook, seller, outgoing, incoming, buyer) -
					humanTradeSettings.marginFactor *
						marginOf(outlook, seller, outgoing, incoming),
				MIN_GAIN,
			);
		} finally {
			g.setWithoutSavingToDB("phase", PHASE.REGULAR_SEASON);
		}
	},
);
