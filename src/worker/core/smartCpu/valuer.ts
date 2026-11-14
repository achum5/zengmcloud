import { ValueChangeCalculator } from "../team/ValueChangeCalculator.ts";
import { currentOutlook } from "../realisticFrontOffice/market.ts";
import { viewFor } from "./signing.ts";
import type { SmartView } from "./worth.ts";

// THE GM'S WORTH FOR THE TRADE SCREENS' SEARCHES (docs/smart-cpu.md, layer
// 2): what would make this work, trade proposals and the trading block ask
// `evaluate` of each deal they try; with the smart CPU it answers by the
// GM's worth, in hundredths of a title (about vanilla's scale: "close" is
// within half a hundredth), without the market rule - the offer a human
// proposes is judged with it (trade/propose.ts). The trade screens
// themselves run on the GM's own offers (smartCpu/screens.ts); this is for
// whatever else asks.
export const SMART_DV_SCALE = 100;

export class SmartValuer extends ValueChangeCalculator {
	private view: SmartView | undefined;

	override async evaluate({
		tid,
		pidsAdd,
		pidsRemove,
		dpidsAdd,
		dpidsRemove,
		tradingPartnerTid,
	}: Parameters<ValueChangeCalculator["evaluate"]>[0]) {
		if (!this.view) {
			this.view = viewFor(await currentOutlook());
			await this.view.prime();
		}
		const v = await this.view.verdict(
			tid,
			tradingPartnerTid ?? -1,
			{ pids: pidsRemove, dpids: dpidsRemove },
			{ pids: pidsAdd, dpids: dpidsAdd },
			{ market: false },
		);
		return v.gain * SMART_DV_SCALE;
	}
}
