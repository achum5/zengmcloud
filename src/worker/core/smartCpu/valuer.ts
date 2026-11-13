import { ValueChangeCalculator } from "../team/ValueChangeCalculator.ts";
import { currentOutlook } from "../realisticFrontOffice/market.ts";
import { viewFor } from "./signing.ts";

// THE GM'S WORTH FOR THE TRADE SCREENS' SEARCHES (docs/smart-cpu.md, layer
// 2): what would make this work, trade proposals and the trading block ask
// `evaluate` of each deal they try; with the smart CPU it answers by the
// GM's worth, in hundredths of a title (about vanilla's scale: "close" is
// within half a hundredth). Without the market rule, which would cost a best
// real offer every step of a search - the offer a human then proposes is
// judged with it (trade/propose.ts).
export const SMART_DV_SCALE = 100;

export class SmartValuer extends ValueChangeCalculator {
	override async evaluate({
		tid,
		pidsAdd,
		pidsRemove,
		dpidsAdd,
		dpidsRemove,
		tradingPartnerTid,
	}: Parameters<ValueChangeCalculator["evaluate"]>[0]) {
		const view = viewFor(await currentOutlook());
		const v = await view.verdict(
			tid,
			tradingPartnerTid ?? -1,
			{ pids: pidsRemove, dpids: dpidsRemove },
			{ pids: pidsAdd, dpids: dpidsAdd },
			{ market: false },
		);
		return v.gain * SMART_DV_SCALE;
	}
}
