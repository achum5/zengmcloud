import type { DraftPick, Player } from "../../../common/types.ts";
import { canTradeInPhase } from "../../../common/transactionPhases.ts";
import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import processTrade from "../trade/processTrade.ts";
import summary from "../trade/summary.ts";
import { contractOf } from "../realisticFrontOffice/contractOf.ts";
import { timed } from "./clock.ts";
import { clearRoomSettings, takerGains } from "./clearRoom.ts";
import { marketSettings, standing, teamsOf } from "./market.ts";
import { smartTradable } from "./tradeRules.ts";
import type { Holdings, SmartView } from "./worth.ts";

// A TRADE BEFORE A CUT (owner, 2026-11-27: "Try a trade. If they have to
// attach something then that's fine"). A CPU team over the roster limit that
// would rather lose a player still owed money after this season than one
// owed less - Dallas, one over in the 2025 preseason, Tyreke Evans (36,
// three seasons left at $24M) - first offers him to the teams with the room
// for his salary and a place on their rosters, for nothing back, its
// cheapest picks thrown in if that is what it takes, fewest first. A deal
// is made where it leaves the team better off than the cut it would make
// otherwise (smartCpu/worth.ts cheapestToCut: whoever it owes least), and
// the taker gains past its margin of error as in any trade. Released, his
// salary is owed for nothing for good; traded, it is off the books.

export type DumpPlan = {
	partner: number;
	give: Holdings;
	// What it gains past the cut it would make otherwise, and what the taker
	// gains.
	gain: number;
	partnerGain: number;
};

const none: Holdings = { players: [], picks: [] };

// The deal that sends `p` away, if any is better for the team than its cut
// (`cutValue`: its franchise value after it).
export const dumpBeforeCut = (
	view: SmartView,
	tid: number,
	p: Player,
	cutValue: number,
) =>
	timed("dump before cut", async (): Promise<DumpPlan | undefined> => {
		if (
			!clearRoomSettings.on ||
			!canTradeInPhase(g.get("phase"), g.get("tradesAfterDeadline")) ||
			!smartTradable(p)
		) {
			return undefined;
		}
		const salary = contractOf(p).amount;
		const cap = g.get("salaryCap");
		const h = await view.holdingsOf(tid);
		const partners: number[] = [];
		for (const t of await idb.cache.teams.getAll()) {
			if (t.disabled || t.tid === tid || view.humans.has(t.tid)) {
				continue;
			}
			const held = await view.holdingsOf(t.tid);
			if (
				cap - view.payrollNow(t.tid) >= salary &&
				held.players.length < g.get("maxRosterSize")
			) {
				partners.push(t.tid);
			}
		}
		if (partners.length === 0) {
			return undefined;
		}
		// Most room first.
		partners.sort((a, b) => view.payrollNow(a) - view.payrollNow(b) || a - b);
		await view.prime();
		// What the cut it would make otherwise does for it, from what it holds
		// now (its value there already counts the cut it would make).
		const cutGain = cutValue - view.value(tid, h);
		// Its cheapest picks, as sweeteners if a taker wants them.
		const picks: DraftPick[] = [...h.picks]
			.sort(
				(a, b) =>
					view.pickFloorNow(a) - view.pickFloorNow(b) ||
					b.round - a.round ||
					a.dpid - b.dpid,
			)
			.slice(0, clearRoomSettings.sweeteners);
		for (let k = 0; k <= picks.length; k++) {
			const give = { players: [p], picks: picks.slice(0, k) };
			let best: DumpPlan | undefined;
			let worthIt = false;
			for (const partner of partners.slice(0, clearRoomSettings.partners)) {
				await view.price(give, [tid, partner]);
				// What sending him gains it past the cut, its picks counted at
				// what they fetch (as a trade counts them).
				const gain = view.sureWorthNow(tid, partner, give, none) - cutGain;
				if (!(gain > 0)) {
					continue;
				}
				worthIt = true;
				const partnerGain = await view.dealWorth(partner, tid, none, give);
				const partnerMargin = marketSettings.margins
					? view.marginNow(partner, none, give)
					: 0;
				const partnerSure = marketSettings.margins
					? view.sureWorthNow(partner, tid, none, give)
					: partnerGain;
				if (
					takerGains(partnerGain, partnerSure, partnerMargin, gain) &&
					(!best || gain > best.gain)
				) {
					best = { partner, give, gain, partnerGain };
				}
			}
			if (best || !worthIt) {
				// Found at the fewest sweeteners - or more only cost it more.
				return best;
			}
		}
		return undefined;
	});

// Make the trade the plan needs. Whether it went through: checked as it
// stands against the trade rules first.
export const dumpNow = async (view: SmartView, tid: number, plan: DumpPlan) => {
	const teams = teamsOf(tid, plan.partner, plan.give, none);
	if ((await summary(teams)).warning) {
		return false;
	}
	await processTrade(teams, undefined, {
		initiatorTid: tid,
		tiers: [standing(view, tid), standing(view, plan.partner)],
		dv: Math.round(Math.min(plan.gain, plan.partnerGain) * 10_000) / 10_000,
		motivation: "making a roster place",
	});
	return true;
};
