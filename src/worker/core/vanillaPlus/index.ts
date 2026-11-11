import { ValueChangeCalculator } from "../team/ValueChangeCalculator.ts";
import { vanillaPlusOn } from "../realisticFrontOffice/isOn.ts";
import { ensureLanes, planFor } from "./lanes.ts";

// What prices a trade for a CPU team: with vanilla+ on, vanilla's values
// weighed by each team's lane (docs/vanilla-plus.md); otherwise stock's.
export const tradeValueCalculator = async (
	opts: { quick?: boolean } = {},
): Promise<ValueChangeCalculator> => {
	if (!vanillaPlusOn()) {
		return new ValueChangeCalculator(opts);
	}
	const lanes = await ensureLanes();
	return new ValueChangeCalculator({
		...opts,
		planOf: (tid) => planFor(lanes.get(tid)),
	});
};
