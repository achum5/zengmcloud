import type { DraftPick, Player } from "../../../common/types.ts";
import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import { getContractDemand } from "../contract/index.ts";
import isUntradable from "../trade/isUntradable.ts";
import processTrade from "../trade/processTrade.ts";
import summary from "../trade/summary.ts";
import { contractOf } from "../realisticFrontOffice/contractOf.ts";
import { ensureRetention } from "../realisticFrontOffice/outlook.ts";
import { timed } from "./clock.ts";
import { fairToSeller, marketSettings, standing, teamsOf } from "./market.ts";
import { signedOn, smartSigningPrice } from "./signing.ts";
import { tradedAwayCount, type Holdings, type SmartView } from "./worth.ts";

// CLEARING ROOM FOR A FREE AGENT, on vanilla's rules. A team under vanilla's
// cap rules may sign a free agent only with the room for his ask; one it
// would gain far more from than anyone it can pay, it can make room for, as
// a human GM does: send salary it can spare to a team with the room to take
// it (a pick or two thrown in if that is what it takes), then sign him. Both
// the trade and the signing are judged together by the GM's worth, against
// the best signing it can make without trading. League 5, before this: in
// summer 2031 Wembanyama (78) signed for $54.9M while five teams were $5-17M
// short of his ask with expiring or spare salary on their books.
//
// The room is the taker's to sell, and it gets the seller's share of what
// the deal makes (market.ts fairToSeller): sweeteners, cheapest first, until
// it does. Taking salary for nothing, teams handed the free agent the room
// was for to the team they took it from - League 7: LAL took a $16.3M dump
// and UTA signed Tripp with the room, where LAL could have paid him itself.

export const clearRoomSettings = {
	on: true,
	// Free agents it can't pay that it weighs a day: the best by value,
	// among those who would be one of its best players.
	targets: 3,
	// Players it would send, at most, and picks it may add (the taker's
	// share can take more than a late second).
	players: 3,
	sweeteners: 4,
	// Teams with room it asks to take them.
	partners: 8,
};

type Plan = {
	p: Player;
	partner: number;
	give: Holdings;
	// What the trade and the signing do for it together, and for the team
	// taking the salary.
	gain: number;
	partnerGain: number;
};

const none: Holdings = { players: [], picks: [] };
const salaryOf = (players: readonly Player[]) =>
	players.reduce((sum, p) => sum + contractOf(p).amount, 0);

// Sets of its players whose salary covers `need`, least missed per dollar
// first: the cheapest single player that covers it, then two or three of
// its least useful contracts.
const salarySets = (
	players: readonly Player[],
	uses: ReadonlyMap<number, number>,
	need: number,
) => {
	const sets: Player[][] = [];
	const byUse = [...players].sort(
		(a, b) => uses.get(a.pid)! - uses.get(b.pid)! || a.pid - b.pid,
	);
	const single = byUse.find((p) => contractOf(p).amount >= need);
	if (single) {
		sets.push([single]);
	}
	const perDollar = [...players].sort(
		(a, b) =>
			uses.get(a.pid)! / Math.max(1, contractOf(a).amount) -
				uses.get(b.pid)! / Math.max(1, contractOf(b).amount) || a.pid - b.pid,
	);
	const greedy: Player[] = [];
	for (const p of perDollar) {
		if (greedy.length >= clearRoomSettings.players) {
			break;
		}
		greedy.push(p);
		if (salaryOf(greedy) >= need) {
			sets.push([...greedy]);
			break;
		}
	}
	return sets;
};

// The trade and signing that make room for the free agent it would gain most
// from, if any beats `bestNet` (its best signing without trading).
export const roomToSign = (
	view: SmartView,
	tid: number,
	targets: readonly Player[],
	bestNet: number,
) =>
	timed("clear room", async (): Promise<Plan | undefined> => {
		if (!clearRoomSettings.on || targets.length === 0) {
			return undefined;
		}
		const cap = g.get("salaryCap");
		const h = await view.holdingsOf(tid);
		const payroll = view.payrollNow(tid, h);
		const uses = view.usesOf(tid, h);
		const tradable = h.players.filter(
			(p) => !isUntradable(p).untradable && contractOf(p).amount > 0,
		);
		const partners: number[] = [];
		for (const t of await idb.cache.teams.getAll()) {
			if (t.disabled || t.tid === tid || view.humans.has(t.tid)) {
				continue;
			}
			await view.holdingsOf(t.tid);
			if (cap - view.payrollNow(t.tid) > 0) {
				partners.push(t.tid);
			}
		}
		if (partners.length === 0) {
			return undefined;
		}
		// Most room first.
		partners.sort((a, b) => view.payrollNow(a) - view.payrollNow(b) || a - b);

		let best: Plan | undefined;
		for (const p of targets) {
			const c = getContractDemand(p);
			const need = payroll + c.amount - cap;
			if (need <= 0) {
				continue;
			}
			// What he alone is worth at his ask (as if it had the room): only a
			// free agent worth more than its best signing is worth clearing for.
			await ensureRetention(view.outlook, [p], tid);
			const alone = smartSigningPrice(
				view.outlook,
				{ tid, players: h.players, picks: h.picks },
				p,
				{ amount: c.amount, exp: c.exp },
			).net;
			if (!(alone > Math.max(bestNet, 0) + marketSettings.minGain)) {
				continue;
			}
			const signed = signedOn(p, tid, { amount: c.amount, exp: c.exp });
			// Its picks ranked by what they fetch (pickFloorNow), as a trade's
			// are: the market's tables first.
			await view.prime();
			for (const players of salarySets(tradable, uses, need)) {
				const sent = salaryOf(players);
				// Cheapest picks first, as sweeteners if the taker wants them.
				const picks: DraftPick[] = [...h.picks]
					.sort(
						(a, b) =>
							view.pickFloorNow(a) - view.pickFloorNow(b) ||
							b.round - a.round ||
							a.dpid - b.dpid,
					)
					.slice(0, clearRoomSettings.sweeteners);
				const takers = partners
					.slice(0, clearRoomSettings.partners)
					// The taker must have the room for all of it.
					.filter((partner) => cap - view.payrollNow(partner) >= sent);
				// Fewest sweeteners first: what it gains doesn't depend on who
				// takes the salary, only on what it gives.
				dealing: for (let k = 0; k <= picks.length; k++) {
					const give = { players, picks: picks.slice(0, k) };
					const after = view.swappedFrom(h, give, {
						players: [signed],
						picks: [],
					});
					const gain =
						view.value(tid, after, [], tradedAwayCount(players)) -
						view.value(tid, h);
					if (
						!(gain > Math.max(bestNet, 0) + marketSettings.minGain) ||
						(best && gain <= best.gain)
					) {
						// More sweeteners only cost it more.
						break;
					}
					for (const partner of takers) {
						const partnerGain = await view.dealWorth(partner, tid, none, give);
						// Its share of what the deal makes past the clearer's best
						// signing without it.
						if (
							partnerGain > marketSettings.minGain &&
							fairToSeller(partnerGain, gain - Math.max(bestNet, 0))
						) {
							best = { p, partner, give, gain, partnerGain };
							break dealing;
						}
					}
				}
			}
		}
		return best;
	});

// Make the trade the plan needs. Whether it went through: checked as it
// stands against the trade rules first.
export const clearRoomNow = async (
	view: SmartView,
	tid: number,
	plan: Plan,
) => {
	const teams = teamsOf(tid, plan.partner, plan.give, none);
	if ((await summary(teams)).warning) {
		return false;
	}
	await processTrade(teams, undefined, {
		initiatorTid: tid,
		tiers: [standing(view, tid), standing(view, plan.partner)],
		dv: Math.round(Math.min(plan.gain, plan.partnerGain) * 10_000) / 10_000,
		motivation: "clearing room",
	});
	return true;
};
