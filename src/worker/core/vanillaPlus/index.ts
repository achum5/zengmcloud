import { ValueChangeCalculator } from "../team/ValueChangeCalculator.ts";
import { vanillaPlusOn } from "../realisticFrontOffice/isOn.ts";
import { ensureLanes, planFor } from "./lanes.ts";
import { tweaksFor } from "./tweaks.ts";
import { vanillaPlusSettings } from "./settings.ts";
import { pickValues, playerWorth, worthFor, zOf } from "./worth.ts";
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
	const humans = new Set(g.get("spectator") ? [] : g.get("userTids"));
	return new ValueChangeCalculator({
		...opts,
		planOf: (tid) => planFor(lanes.get(tid)),
		adjust: tweaks,
		cpuMarkup: vanillaPlusSettings.cpuMarkup,
		playerWorth: (tid, p, vsHuman) => {
			const worth = playerWorth(p, lanes.get(tid));
			if (!vsHuman) {
				return worth;
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
			let best = 0;
			for (const [other, lane] of lanes) {
				if (other !== tid && !humans.has(other)) {
					best = Math.max(
						best,
						worthFor(p, lane) * tweaks(other, asset, "add"),
					);
				}
			}
			return { own: worth.own, market: Math.max(worth.market, zOf(best)) };
		},
		pickValues,
	});
};
