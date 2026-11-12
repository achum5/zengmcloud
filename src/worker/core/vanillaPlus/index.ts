import { ValueChangeCalculator } from "../team/ValueChangeCalculator.ts";
import { vanillaPlusOn } from "../realisticFrontOffice/isOn.ts";
import { ensureLanes, planFor } from "./lanes.ts";
import { tweaksFor } from "./tweaks.ts";
import { vanillaPlusSettings } from "./settings.ts";
import { pickValues, playerWorth, worthFor, zOf } from "./worth.ts";
import { calendar } from "./calendar.ts";
import { bestOfferElsewhere, keenestBidders } from "./market.ts";
import type { Player } from "../../../common/types.ts";
import { g } from "../../util/index.ts";

// What prices a trade for a CPU team: with vanilla+ on, vanilla's machinery
// with players and picks priced by their worth season by season, weighed by
// each team's lane (worth.ts), and small tweaks for fit and production
// (docs/vanilla-plus.md); otherwise stock's.
export const tradeValueCalculator = async (
	opts: { quick?: boolean } = {},
): Promise<ValueChangeCalculator> => {
	if (!vanillaPlusOn()) {
		return new ValueChangeCalculator(opts);
	}
	const lanes = await ensureLanes();
	const tweaks = await tweaksFor(lanes);
	const c = vanillaPlusSettings.calendar ? await calendar() : undefined;
	const playoffsOf = (tid: number) => c?.playoffs.get(tid) ?? 1;
	const humans = new Set(g.get("spectator") ? [] : g.get("userTids"));
	const bestOffers = new Map<string, number>();
	const biddersFor = (tid: number, p: Player) =>
		keenestBidders({ lanes, tweaks, calendar: c, humans, tid, p });
	const calc: ValueChangeCalculator = new ValueChangeCalculator({
		...opts,
		planOf: (tid) => planFor(lanes.get(tid)),
		adjust: tweaks,
		cpuMarkup: vanillaPlusSettings.cpuMarkup,
		playerWorth: async (tid, p, vsHuman) => {
			const worth = {
				...playerWorth(p, lanes.get(tid), c, playoffsOf(tid), tid),
				injuryPriced: c !== undefined,
			};
			if (!vsHuman) {
				return worth;
			}
			if (c) {
				// A human pays at least what the best real offer elsewhere is
				// worth to this team (market.ts bestOfferElsewhere). Not the
				// keenest team's own worth for him, as without the calendar: a
				// contender's worth for a player this season is no price a
				// contender could pay.
				const key = `${tid}:${p.pid}`;
				let best = bestOffers.get(key);
				if (best === undefined) {
					best = await bestOfferElsewhere(calc, tid, p.pid, biddersFor(tid, p));
					bestOffers.set(key, best);
				}
				// Only a real offer sets a floor: nobody bidding for a bad
				// contract doesn't make it worth nothing instead of less.
				return { ...worth, floor: best > 0 ? best : undefined };
			}
			// A human pays at least what the keenest other team would, its fit
			// included: no buying cheap here to sell dear there.
			const asset = {
				type: "player",
				pid: p.pid,
				value: 0,
				contractValue: 0,
				injury: p.injury,
				age: g.get("season") - p.born.year,
				justDrafted: false,
			} as const;
			let keenest = 0;
			for (const [other, lane] of lanes) {
				if (other !== tid && !humans.has(other)) {
					keenest = Math.max(
						keenest,
						worthFor(p, lane, c, playoffsOf(other), other) *
							tweaks(other, asset, "add"),
					);
				}
			}
			return { ...worth, market: Math.max(worth.market, zOf(keenest)) };
		},
		pickValues,
		// A team that sells its players is a worse team, and its own pick a
		// better one: a tank counts for what it's worth, and a contender
		// buying gives some of its pick's worth up.
		ownPicks: vanillaPlusSettings.calendar,
		margin:
			c && vanillaPlusSettings.patience
				? (tid) => c.patienceOf.get(tid) ?? c.patience
				: 0,
	});
	return calc;
};
