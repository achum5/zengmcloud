import type { DraftPick, Player } from "../../../common/types.ts";
import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import { getContractDemand } from "../contract/index.ts";
import processTrade from "../trade/processTrade.ts";
import summary from "../trade/summary.ts";
import { contractOf } from "../realisticFrontOffice/contractOf.ts";
import { ensureRetention } from "../realisticFrontOffice/outlook.ts";
import { timed } from "./clock.ts";
import { smartTradable } from "./tradeRules.ts";
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
//
// Each side must also clear its margin of error, as a trade's must
// (marketSettings.margins): the clearer's gain past its best signing
// without trading by more than its read of everyone who moves - the players
// it sends and the free agent - could be off; the taker's by more than its
// read of what it takes. In League 12 (2010-18), of 28 rooms cleared 19
// signed clearly better than they sent, but six swapped like for like and
// paid a second to do it - Minnesota Al Jefferson (11.3 WS the next three
// seasons) for Nenê (9.8), Detroit Gerald Wallace (13.0) for Peja
// Stojaković at 36 (11.9) - and Philadelphia paid a first and three seconds
// for Corey Brewer (2026-11-19).
//
// A player it sends counts at least at what the market would pay for him,
// as he would in a trade (SmartView.sureWorthNow): a GM sells a player
// other teams want, or gets something back, before he gives him away for
// room. The taker may send picks back. Counted at his use alone, a team
// with no title in sight - to which nearly any player is worth little -
// sent players for nothing whom contenders would have paid firsts for: in
// League 13 New Orleans gave Chris Paul (25, 60/63; 40.9 win shares for
// Oklahoma City) to clear room for Andray Blatche, Memphis Zach Randolph
// for David Lee (2026-11-20).

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
	// The players it sends counted at least at their market price, and the
	// taker's cheapest picks it may take back, one of them.
	atPrice: true,
	picksBack: 3,
	// Picks worth more it asks the taker for, at most, once the taker would
	// take the salary with a cheap one or none back.
	asks: 4,
};

// A team's cheapest picks by what they fetch (pickFloorNow), as many as it
// may send back for nothing more (clearRoomSettings.picksBack).
const cheapest = (view: SmartView, picks: readonly DraftPick[]) =>
	[...picks]
		.sort(
			(a, b) =>
				view.pickFloorNow(a) - view.pickFloorNow(b) ||
				b.round - a.round ||
				a.dpid - b.dpid,
		)
		.slice(0, clearRoomSettings.picksBack);

// Whether clearing room is worth it to the team: what the trade and the
// signing gain it past its best signing without trading, by more than the
// smallest gain worth a deal and its margin of error on everyone who moves.
export const clearerGains = (gain: number, bestNet: number, margin: number) =>
	gain > Math.max(bestNet, 0) + Math.max(marketSettings.minGain, margin);

// Whether the team taking the salary gains enough: past its own margin of
// error on what it takes - counting no more of the price of the picks thrown
// in than it can be sure of getting (`sure`: SmartView.sureWorthNow) - and
// its share of what the deal makes past the clearer's best signing without
// it (fairToSeller).
export const takerGains = (
	partnerGain: number,
	sure: number,
	margin: number,
	clearerGain: number,
) =>
	partnerGain > marketSettings.minGain &&
	sure > Math.max(marketSettings.minGain, margin) &&
	fairToSeller(partnerGain, clearerGain);

export type ClearRoomPlan = {
	p: Player;
	partner: number;
	give: Holdings;
	// What the taker sends back: none, or one of its picks.
	back: Holdings;
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
// its least useful contracts. `uses`: what each would cost the team to
// send - his use to it, or the market's price for him if more
// (clearRoomSettings.atPrice).
export const salarySets = (
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
	timed("clear room", async (): Promise<ClearRoomPlan | undefined> => {
		if (!clearRoomSettings.on || targets.length === 0) {
			return undefined;
		}
		const cap = g.get("salaryCap");
		const h = await view.holdingsOf(tid);
		const payroll = view.payrollNow(tid, h);
		const tradable = h.players.filter(
			(p) => smartTradable(p) && contractOf(p).amount > 0,
		);
		// What each would cost it to send, priced only once a free agent is
		// worth clearing for: priced for every team every free agency day, it
		// tripled clearing room's time (League 12, 14 s to 50 s a summer).
		let costs: Map<number, number> | undefined;
		const costsOf = async () => {
			if (!costs) {
				costs = new Map(view.usesOf(tid, h));
				if (clearRoomSettings.atPrice) {
					for (const p of tradable) {
						const one = { players: [p], picks: [] };
						const price = await view.price(one, [tid]);
						if (price > costs.get(p.pid)!) {
							costs.set(p.pid, price);
						}
					}
				}
			}
			return costs;
		};
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

		let best: ClearRoomPlan | undefined;
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
			for (const players of salarySets(tradable, await costsOf(), need)) {
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
				// Fewest sweeteners first. With none, the taker may send one of
				// its cheapest picks back, if that is what makes it worth it - and
				// where what it takes is worth far more to it than the room, any
				// pick it would still take the deal with.
				for (let k = 0; k <= picks.length; k++) {
					const give = { players, picks: picks.slice(0, k) };
					let ownBar = false;
					let made = false;
					for (const partner of takers) {
						// Whether it gains first, its prices read: the taker's whole
						// worth only for what it would make (four times over for
						// every partner, it tripled clearing room's time).
						await view.price(give, [tid, partner]);
						const rowOf = async (back: Holdings) => {
							if (back.picks.length > 0) {
								await view.price(back, [tid, partner]);
							}
							const inn = { players: [signed], picks: back.picks };
							const gain = clearRoomSettings.atPrice
								? view.sureWorthNow(tid, partner, give, inn)
								: view.value(
										tid,
										view.swappedFrom(h, give, inn),
										[],
										tradedAwayCount(players),
									) - view.value(tid, h);
							const margin = marketSettings.margins
								? view.marginNow(tid, give, inn)
								: 0;
							return {
								back,
								gain,
								clears: clearerGains(gain, bestNet, margin),
							};
						};
						// What the taker would gain, and whether that is enough.
						const asTaker = async (back: Holdings, gain: number) => {
							const partnerGain = await view.dealWorth(
								partner,
								tid,
								back,
								give,
							);
							const partnerMargin = marketSettings.margins
								? view.marginNow(partner, back, give)
								: 0;
							const partnerSure = marketSettings.margins
								? view.sureWorthNow(partner, tid, back, give)
								: partnerGain;
							return {
								partnerGain,
								// What it gains past its margin: the most it could pay.
								spare:
									partnerSure - Math.max(marketSettings.minGain, partnerMargin),
								takes: takerGains(
									partnerGain,
									partnerSure,
									partnerMargin,
									gain - Math.max(bestNet, 0),
								),
							};
						};
						const asks = k === 0 && clearRoomSettings.atPrice;
						const cheap = asks
							? cheapest(view, view.heldNow(partner).picks)
							: [];
						// The least it would take back first - nothing, or one of the
						// taker's cheapest picks: whether the taker would do it at all,
						// as with more back the taker only gains less.
						const rows = [];
						for (const back of [
							none,
							...cheap.map((dp) => ({ players: [], picks: [dp] })),
						]) {
							const row = await rowOf(back);
							if (row.clears) {
								rows.push(row);
							}
						}
						const least = rows.sort((a, b) => a.gain - b.gain)[0];
						if (!least) {
							continue;
						}
						ownBar = true;
						const base = await asTaker(least.back, least.gain);
						if (!base.takes) {
							continue;
						}
						made = true;
						if (!best || least.gain > best.gain) {
							best = {
								p,
								partner,
								give,
								back: least.back,
								gain: least.gain,
								partnerGain: base.partnerGain,
							};
						}
						if (!asks || !(base.spare > marketSettings.minGain)) {
							continue;
						}
						// Then as much back as the taker would still give, where what
						// it takes is worth more to it than the room: a GM doesn't
						// hand a player other teams want to a team with room for
						// nothing (a random-players league, 2026-11-22: Pittsburgh its
						// 29-year-old 70 to Los Angeles, for room for a 24-year-old
						// 65). Its best for it first, among the taker's picks it could
						// pay from what it gains past its margin.
						for (const dp of view.heldNow(partner).picks) {
							if (!cheap.includes(dp)) {
								const row = await rowOf({ players: [], picks: [dp] });
								if (row.clears) {
									rows.push(row);
								}
							}
						}
						const more = rows
							.filter(
								(r) =>
									r.gain > best!.gain &&
									r.back.picks.every(
										(dp) =>
											view.piecePriceNow({ players: [], picks: [dp] }, [
												tid,
												partner,
											]) < base.spare,
									),
							)
							.sort((a, b) => b.gain - a.gain)
							.slice(0, clearRoomSettings.asks);
						for (const r of more) {
							const asked = await asTaker(r.back, r.gain);
							if (asked.takes) {
								best = {
									p,
									partner,
									give,
									back: r.back,
									gain: r.gain,
									partnerGain: asked.partnerGain,
								};
								break;
							}
						}
					}
					if (made || !ownBar) {
						// Found at the fewest sweeteners - or more sweeteners only
						// cost it more.
						break;
					}
				}
			}
		}
		return best;
	});

// Watchers of the rooms cleared (tests, the arena): called just before each
// trade is made, with the view it was judged on.
export const clearRoomWatchers: ((
	tid: number,
	plan: ClearRoomPlan,
	view: SmartView,
) => void)[] = [];

// Make the trade the plan needs. Whether it went through: checked as it
// stands against the trade rules first.
export const clearRoomNow = async (
	view: SmartView,
	tid: number,
	plan: ClearRoomPlan,
) => {
	const teams = teamsOf(tid, plan.partner, plan.give, plan.back);
	if ((await summary(teams)).warning) {
		return false;
	}
	for (const watch of clearRoomWatchers) {
		watch(tid, plan, view);
	}
	await processTrade(teams, undefined, {
		initiatorTid: tid,
		tiers: [standing(view, tid), standing(view, plan.partner)],
		dv: Math.round(Math.min(plan.gain, plan.partnerGain) * 10_000) / 10_000,
		motivation: "clearing room",
	});
	return true;
};
