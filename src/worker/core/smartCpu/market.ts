import { smartDecidesHere } from "./decidesHere.ts";
import { holdRoomForAdvance } from "../sync/advanceLease.ts";
import { PHASE } from "../../../common/constants.ts";
import type { DraftPick, Player, TradeTeams } from "../../../common/types.ts";
import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import getDaysLeftSchedule from "../season/getDaysLeftSchedule.ts";
import isUntradable from "../trade/isUntradable.ts";
import processTrade from "../trade/processTrade.ts";
import setGameAttributes from "../league/setGameAttributes.ts";
import summary from "../trade/summary.ts";
import { contractOf } from "../realisticFrontOffice/contractOf.ts";
import { currentOutlook, gamesPlayed } from "../realisticFrontOffice/market.ts";
import { ensureRetention } from "../realisticFrontOffice/outlook.ts";
import { timed, timedSync } from "./clock.ts";
import { salaryRulesAllow } from "./tradeRules.ts";
import { inStages } from "../realisticFrontOffice/stages.ts";
import { smartVanillaOn } from "../realisticFrontOffice/isOn.ts";
import { viewFor } from "./signing.ts";
import { noteVanillaPool } from "./vanilla.ts";
import {
	offerStats,
	type Holdings,
	type OfferClears,
	type SmartView,
} from "./worth.ts";
import { marginShare, mustMove, plansOf, type Plan } from "./plans.ts";

// THE GM'S MARKET (docs/smart-cpu.md, layer 2). At each market moment every
// team reads where it stands (plans.ts), and every CPU team reads every
// tradable player on every other (the scan). A player is a candidate sale
// when some team would trade for him - its use for him past his own team's -
// each team shopping its few, those it must move before the deadline first.
// Each is auctioned: put to the teams that want him most once they have sent
// back his salary, asked in turn for a real offer (layer 1's offerFrom: its
// best for the seller that still clears its own margin, legal under the
// trade rules) until two the seller would take are in, and the seller takes
// the best if it gains - the market rule kept by construction. The deals are
// made best for each buyer first, each team dealing at most once a moment,
// each checked again as it stands before it is made.

export const marketSettings = {
	// In season, a player whose deal ends this season shopped every moment,
	// never rested: every moment nearer the deadline he is worth less to
	// sell, and after it nothing to a team he won't re-sign with. Rested
	// every other moment, Brooklyn's Michael Porter Jr. (63, $40.8M, under
	// 1% to re-sign) sat out the deadline's last day in the owner's League
	// 27 (2026-11-21).
	restExpiring: false,
	// Candidates ranked by a rough read (rosters not cut to the limit).
	roughRanking: true,
	// THE SCAN (owner, 2026-11-21: "they're scouting the entire league
	// constantly"): every CPU team reads every tradable player on every other
	// team at each moment, and every player some team would trade for is on
	// the block - those that must move first, then the most a deal could
	// make. Each is put to every team whose want for him, once it has sent
	// back his salary (SmartView.matchedNow), is more than his team needs for
	// him, the keenest first, for as long as one could still beat the offers
	// in hand (SmartView.bestRealOffer). Put to the two keenest by their use
	// alone, Brooklyn's Michael Porter Jr. (63, $40.8M, his deal ending, under
	// 1% to re-sign) went to the Lakers and Oklahoma City, neither of whom
	// could send back his salary and still clear its margin, and never to New
	// York, whose offer did (the owner's League 27).
	// The work a moment may take, in offers asked for - the time a phone has
	// for it, not a rule of the market: what is left waits for the next
	// moment, a few days on.
	offerBudget: 400,
	// In season, a moment about every this many games a team, and every day
	// in the last few before the deadline - where three in four of the NBA's
	// in-season trades are made (arena benchmarks).
	gamesBetween: 17,
	// On vanilla's rules (smartVanillaOn), every this many: CPU teams go after
	// players all season, not mostly at the deadline - so a human team can't
	// take the preseason to buy up every upgrade before the other contenders
	// move (owner, 2026-11-17).
	vanillaGamesBetween: 5,
	deadlineDays: 5,
	// In free agency, a moment on these days left - or, in stages, at each
	// stage (daysLeft counts the stages down).
	freeAgencyDays: [30, 20, 10],
	// What a deal between CPU teams must gain each side (titles), past
	// nothing - what it can be sure of (smartSettings.premiumRealized), past
	// its margin of error too: a real GM leaves a swap that hardly moves his
	// franchise alone. In the owner's league a quarter of the deals gained
	// one side under 0.001 - near-equal role players swapped, a 53 for a 53
	// and two picks; at 0.001, none did, with trades a season 31 against 33.
	// Teams out of the race still swapped bench players for each other: to a
	// team with no title in sight every player moves its title chances
	// little, and its margin of error as little - in League 12 runs New
	// Orleans gave Nate Wolters (48) to Oklahoma City for four players of 41
	// to 48 and a second (OKC +0.0031 against a margin of 0.0005), Phoenix
	// Robin Lopez for Isaiah Whitehead (+0.0048), Houston Devonte' Graham for
	// Brandon Davies and a second (+0.0040). Half a percent of a title each
	// way leaves those (2026-11-19).
	minGain: 0.005,
	// THE SELLER'S SHARE. A seller doesn't hand a team a real upgrade -
	// more than shareAbove titles - while keeping less than this share of
	// what the deal makes for both: it holds out, or the buyer pays more
	// (the winning offer is never trimmed below it). With one team wanting
	// a player far more than any other, the next best offer is nothing,
	// and trimmed to that the seller kept a sliver: in League 7 the
	// league's best teams bought Durant for Beasley, Harden for Horford
	// and a late first, Thornton for a 58 and filler, and four of them
	// started 94 of the 140 deals and won five of six titles. In the
	// deals the market found there, the seller kept a median fifth.
	sellerShare: 1 / 3,
	shareAbove: 0.01,
	// THE BUYER'S CHOICE. A team makes one deal a moment, and with several
	// open to it - each its seller's best offer for the player - it makes the
	// one best for itself, as it would pay once trimmed. Made in order of
	// what each seller gained, a buyer took whichever its seller wanted most:
	// League 34's Lakers were the winning bid in 16 of a moment's 26 deals
	// and made the one worth 0.025 of a title to them with others worth 0.05
	// open, and Minnesota gave two firsts for a 52 while a 56 and 57 its
	// package would buy were on offer to it (2026-11-19).
	buyersChoose: true,
	// THE BUYER'S NEXT BEST. The seller's share is of what the deal makes
	// past what the buyer would make anyway - its best other deal open that
	// moment, that seller taking no more than it must (buyerAlternatives) -
	// not of all the buyer gains. A seller holds out for its share of an
	// upgrade only it can sell; when the buyer has others as good on offer,
	// it sells for what beats keeping him. At one League 36 moment the
	// Lakers had 23 deals open, a role player in each, and nobody bidding
	// against them for any (the next best offer worth 0.001 to the seller);
	// with a third of the Lakers' gain the seller's, each cost two firsts
	// and a player, and they paid that for Kevin Huerter (55) and then
	// Duncan Robinson (58) (2026-11-19).
	buyerAlternatives: true,
	// THE MARGIN OF ERROR (owner's call, 2026-11-19): each side must gain by
	// more than its own read of the deal could be off (SmartView.marginNow) -
	// a GM doesn't give firsts for a gain he can't be sure of - counting no
	// more of the market's prices than it can be sure of getting
	// (smartSettings.premiumRealized). With only
	// minGain, a third to a half of the deals paying firsts gained the buyer
	// under 0.01 of a title (Oklahoma City three firsts for Cedric Coward,
	// 0.002), where its read of a one-player swap is off by about 0.04.
	margins: true,
};

// Whether a deal leaves the seller its share (marketSettings.sellerShare) of
// what it makes past what the buyer would gain anyway (buyerAlt, its next
// best: marketSettings.buyerAlternatives).
export const fairToSeller = (
	sellerGain: number,
	buyerGain: number,
	buyerAlt = 0,
) => {
	const over = buyerGain - Math.max(0, buyerAlt);
	return (
		!(over > marketSettings.shareAbove) ||
		sellerGain >= marketSettings.sellerShare * (sellerGain + over)
	);
};

// Where candidate sales fall away (for tests and the arena).
export const marketStats = {
	candidates: 0,
	noOffer: 0,
	sellerSaysNo: 0,
	// Deals no longer good for both by the time their turn came.
	changed: 0,
	illegal: 0,
	// Deals the seller turned down for its share (sellerShare).
	unfair: 0,
	// Deals a side's gain didn't clear its margin of error (margins).
	withinMargin: 0,
};

export type SmartDeal = {
	seller: number;
	buyer: number;
	out: Holdings;
	offer: Holdings;
	sellerGain: number;
	buyerGain: number;
	// The next best real offer's worth to the seller: what the winner's
	// offer is trimmed to beat, once it is to be made.
	bar: number;
	// What the buyer would gain from its best other deal open this moment
	// (withAlternatives), its seller taking no more than it must.
	buyerAlt?: number;
};

const none: Holdings = { players: [], picks: [] };
const one = (x: Player | DraftPick): Holdings =>
	"pid" in x ? { players: [x], picks: [] } : { players: [], picks: [x] };
export const teamsOf = (
	a: number,
	b: number,
	giveA: Holdings,
	giveB: Holdings,
) =>
	[
		{
			tid: a,
			pids: giveA.players.map((p) => p.pid),
			dpids: giveA.picks.map((dp) => dp.dpid),
			pidsExcluded: [],
			dpidsExcluded: [],
		},
		{
			tid: b,
			pids: giveB.players.map((p) => p.pid),
			dpids: giveB.picks.map((dp) => dp.dpid),
			pidsExcluded: [],
			dpidsExcluded: [],
		},
	] as TradeTeams;

// The candidate sales at this moment: every CPU team's read of every
// tradable player on another (the scan), and each player some team would
// trade for - its use for him past his own team's - those their teams must
// move before the deadline first (plans.ts), those shopped last moment
// without a taker after the rest, then the most a deal could make.
export type Candidate = {
	seller: number;
	p: Player;
	// The keenest team's use for him past his own team's.
	surplus: number;
	// His team must move him before the deadline (plans.ts).
	mustMove: boolean;
	// The keenest team by its use alone.
	buyer: number;
	// Every other team's rough want for him, keenest first.
	bids: { k: number; bid: number }[];
};

export const candidates = async (
	view: SmartView,
	cpu: ReadonlySet<number>,
	dealt: ReadonlySet<number>,
	// Players each team shopped last moment without a taker: behind the rest.
	rested: ReadonlyMap<number, ReadonlySet<number>> = new Map(),
	plans: ReadonlyMap<number, Plan> = new Map(),
): Promise<Candidate[]> => {
	const buyers = [...cpu].filter((k) => !dealt.has(k));
	const found: Candidate[] = [];
	for (const seller of cpu) {
		if (dealt.has(seller)) {
			continue;
		}
		await holdRoomForAdvance();
		const held = view.heldNow(seller);
		await ensureRetention(view.outlook, held.players, seller);
		for (const p of held.players) {
			if (isUntradable(p).untradable) {
				continue;
			}
			// Read roughly - rosters not cut to the limit, a third of the work
			// (2026-11-14) - the salary rules left to the read net of salary
			// and the real offers.
			const read = (tid: number) =>
				marketSettings.roughRanking
					? view.roughUseOfNow(tid, one(p))
					: view.useOfNow(tid, one(p));
			const own = read(seller);
			const bids: { k: number; bid: number }[] = [];
			for (const k of buyers) {
				if (k !== seller) {
					await ensureRetention(view.outlook, [p], k);
					bids.push({ k, bid: read(k) });
				}
			}
			bids.sort((a, b) => b.bid - a.bid || a.k - b.k);
			const top = bids[0];
			if (top && top.bid - own > marketSettings.minGain) {
				found.push({
					seller,
					p,
					surplus: top.bid - own,
					mustMove: mustMove(view, plans, p, seller),
					buyer: top.k,
					bids,
				});
			}
		}
	}
	const season = g.get("season");
	const restsNow = (p: Player) =>
		marketSettings.restExpiring ||
		g.get("phase") !== PHASE.REGULAR_SEASON ||
		contractOf(p).exp > season;
	const wasRested = (c: Candidate) =>
		rested.get(c.seller)?.has(c.p.pid) && restsNow(c.p) ? 1 : 0;
	return found.sort(
		(a, b) =>
			Number(b.mustMove) - Number(a.mustMove) ||
			wasRested(a) - wasRested(b) ||
			b.surplus - a.surplus ||
			a.p.pid - b.p.pid,
	);
};

// The teams a candidate is put to: every one whose rough want for him is
// more than his team needs for him (his use to it, and his price past that
// as far as it can still get it), read again net of the salary it must send
// back for him - a contender's want for a big contract is what is left past
// the rotation players it sends (the Lakers', for Michael Porter Jr., 0.385
// of a title to 0.11) - the keenest net first.
const biddersFor = async (view: SmartView, c: Candidate) => {
	await view.price(one(c.p), [c.seller]);
	const needs = -view.dealWorthNow(c.seller, -1, one(c.p), none);
	const wants: { tid: number; want: number }[] = [];
	for (const { k, bid } of c.bids) {
		if (bid <= needs) {
			break;
		}
		const m = view.matchedNow(k, one(c.p));
		if (m && m.net > needs) {
			wants.push({ tid: k, want: m.net });
		}
	}
	return wants.sort((a, b) => b.want - a.want || a.tid - b.tid);
};

// The plans read at this moment (dealsNow): a deal's margins by them.
let plansNow: ReadonlyMap<number, Plan> = new Map();

// A team's margin of error for a deal, by its plan (plans.ts marginShare).
const marginFor = (
	view: SmartView,
	tid: number,
	give: Holdings,
	get: Holdings,
) =>
	view.marginNow(tid, give, get) * marginShare(view, plansNow, tid, give, get);

// The market's test of a deal for its buyer, as each deal is made: its
// gain, and its sure gain past its margin of error by its plan (smartSettings.
// offerClears builds offers to it).
const buyerClearsFor =
	(view: SmartView, out: Holdings): OfferClears =>
	(buyer, worth, sure, margin) =>
		worth > marketSettings.minGain &&
		(!marketSettings.margins ||
			sure >
				Math.max(
					marketSettings.minGain,
					margin * marginShare(view, plansNow, buyer, none, out),
				));

// The winning offer, trimmed to what it must be: the buyer takes back what it
// would miss most, one piece at a time, while the seller still gains by more
// than `bar` (the next best real offer, or nothing) and keeps its share of
// what the deal makes for both (marketSettings.sellerShare). A real offer is
// its buyer's best for the seller while it still gains - all it would pay;
// as at an auction the winner pays about what beats the next bidder
// (untrimmed, a contender gave two prospects and two firsts for a
// 31-year-old 60 and kept 0.0009 of a title for itself). Salaries must
// still work.
const trimmed = (
	view: SmartView,
	seller: number,
	buyer: number,
	out: Holdings,
	offer: Holdings,
	bar: number,
	// Their payrolls as the trade rules count them.
	payrolls: { seller: number; buyer: number },
	buyerAlt = 0,
) => {
	const salary = (h: Holdings) =>
		h.players.reduce((sum, p) => sum + contractOf(p).amount, 0);
	// Its own cost of each piece, dearest first - the same however much of
	// the offer is left, so worked out once.
	const costs = [...offer.players, ...offer.picks]
		.map((x) => ({
			x,
			cost: -view.dealWorthNow(buyer, seller, one(x), none),
		}))
		.sort((a, b) => b.cost - a.cost);
	let now = offer;
	for (;;) {
		if (now.players.length + now.picks.length <= 1) {
			return now;
		}
		let next: Holdings | undefined;
		for (const { x } of costs) {
			if (
				!now.players.includes(x as Player) &&
				!now.picks.includes(x as DraftPick)
			) {
				continue;
			}
			const without = {
				players: now.players.filter((p) => p !== x),
				picks: now.picks.filter((dp) => dp !== x),
			};
			// The salary rules first, then each side's worth: the cheapest
			// checks before the dearest, the deal the same either way.
			if (
				!salaryRulesAllow(
					buyer,
					payrolls.buyer,
					salary(out),
					salary(without),
				) ||
				!salaryRulesAllow(seller, payrolls.seller, salary(without), salary(out))
			) {
				continue;
			}
			const sellerGain = view.dealWorthNow(seller, buyer, out, without);
			if (
				!(sellerGain > bar) ||
				(marketSettings.margins &&
					!(
						view.sureWorthNow(seller, buyer, out, without) >
						Math.max(
							marketSettings.minGain,
							marginFor(view, seller, out, without),
						)
					))
			) {
				continue;
			}
			const buyerGain = view.dealWorthNow(buyer, seller, without, out);
			if (fairToSeller(sellerGain, buyerGain, buyerAlt)) {
				next = without;
				break;
			}
		}
		if (!next) {
			return now;
		}
		now = next;
	}
};

// Every deal the market has at this moment, best for the seller first (by
// its best real offer, before it is trimmed to beat the next): each
// candidate put to the best real offer from the keen CPU teams (layer 1:
// each its best for the seller while it still gains, legal under the trade
// rules - the market rule kept by construction), kept if the seller gains.
// What makes a deal can't be told from the candidates cheaply (2026-11-13:
// 28 deals among 374 candidates, spread from the 41st to the 313th), so all
// are asked, once a moment.
export const dealsNow = async (
	view: SmartView,
	cpu: ReadonlySet<number>,
	rested: ReadonlyMap<number, ReadonlySet<number>> = new Map(),
	// Told which players were shopped.
	onShopped?: (shopped: readonly { seller: number; p: Player }[]) => void,
) => {
	await view.prime();
	const deals: SmartDeal[] = [];
	plansNow = plansOf(view, view.outlook.teams.keys());
	const all = await timed("market: candidates", () =>
		candidates(view, cpu, new Set(), rested, plansNow),
	);
	const shopped: Candidate[] = [];
	let asked = 0;
	for (const c of all) {
		// A moment can run for minutes on a phone: the room's busy mark
		// renewed as it goes, not only as it starts (a no-op while the mark
		// has more than half its time left).
		await holdRoomForAdvance();
		const out = one(c.p);
		if (asked >= marketSettings.offerBudget) {
			break;
		}
		shopped.push(c);
		marketStats.candidates += 1;
		const bidders = await timed("market: bidders", () => biddersFor(view, c));
		const buyers = bidders.map((b) => b.tid);
		trace({ stage: "shopped", seller: c.seller, p: c.p, buyers }, view);
		const tried = offerStats.tried;
		const real = await timed("market: offers", () =>
			view.bestRealOffer(
				c.seller,
				out,
				-1,
				new Set(),
				bidders,
				buyerClearsFor(view, out),
			),
		);
		asked += offerStats.tried - tried;
		if (!real) {
			marketStats.noOffer += 1;
			trace({ stage: "no offer", seller: c.seller, p: c.p, buyers }, view);
			continue;
		}
		if (!(real.worth > marketSettings.minGain)) {
			marketStats.sellerSaysNo += 1;
			trace(
				{
					stage: "seller says no",
					seller: c.seller,
					buyer: real.buyer,
					p: c.p,
					sellerGain: real.worth,
					offer: real.offer,
				},
				view,
			);
			continue;
		}
		deals.push({
			seller: c.seller,
			buyer: real.buyer,
			out,
			offer: real.offer,
			sellerGain: real.worth,
			buyerGain: Number.NaN,
			bar: Math.max(marketSettings.minGain, real.second),
		});
	}
	onShopped?.(shopped);
	return deals.sort((a, b) => b.sellerGain - a.sellerGain);
};

// A deal about to be made, its offer trimmed to what beats the next best
// (trimmed): only the few a moment makes are trimmed, not every deal found
// (82 one moment, 4 of them made).
export const trimDeal = async (view: SmartView, deal: SmartDeal) => {
	// Each side's chance of keeping what it gets, for its margin.
	await ensureRetention(view.outlook, deal.out.players, deal.buyer);
	await ensureRetention(view.outlook, deal.offer.players, deal.seller);
	const payrolls = {
		seller: await view.tradePayroll(deal.seller),
		buyer: await view.tradePayroll(deal.buyer),
	};
	return timedSync("market: trim", () =>
		trimmed(
			view,
			deal.seller,
			deal.buyer,
			deal.out,
			deal.offer,
			deal.bar,
			payrolls,
			deal.buyerAlt ?? 0,
		),
	);
};

// Each deal's buyerAlt (marketSettings.buyerAlternatives): what its buyer
// would gain from its best deal of these with another seller, that seller
// taking what it must - as much as beats its next best offer, or keeping
// the player - and the buyer the rest of what that deal makes for both
// (sellerGain + buyerGain, about the same whatever the offer, as each side
// counts a piece at its price or more). Untouched without the setting.
export const withAlternatives = async (
	view: SmartView,
	deals: readonly SmartDeal[],
): Promise<SmartDeal[]> => {
	if (!marketSettings.buyerAlternatives) {
		return [...deals];
	}
	// One at a time: what a franchise is worth is worked out in order.
	const free: number[] = [];
	for (const d of deals) {
		free.push(
			d.sellerGain +
				(await view.dealWorth(d.buyer, d.seller, d.offer, d.out)) -
				Math.max(d.bar, marketSettings.minGain),
		);
	}
	return deals.map((d, i) => {
		let alt = 0;
		for (const [j, other] of deals.entries()) {
			// Another seller's: a seller doesn't bid against itself.
			if (j !== i && other.buyer === d.buyer && other.seller !== d.seller) {
				alt = Math.max(alt, free[j]!);
			}
		}
		return { ...d, buyerAlt: alt };
	});
};

// Watchers of the deals the market makes (tests, the arena).
// Called just before each is made, with the view it was judged on (so a
// watcher can read what each side reckoned every piece worth).
export const smartDealWatchers: ((deal: SmartDeal, view: SmartView) => void)[] =
	[];

// Where each player shopped got to in a moment and what stopped him (tests:
// a no-op with none watching).
export type MarketTrace = {
	stage:
		| "shopped"
		| "no offer"
		| "seller says no"
		| "open"
		| "dealt already"
		| "changed"
		| "within margin"
		| "unfair"
		| "illegal"
		| "made";
	seller: number;
	buyer?: number;
	p: Player;
	buyers?: readonly number[];
	why?: string;
	sellerGain?: number;
	buyerGain?: number;
	offer?: Holdings;
};
export const smartTraceWatchers: ((t: MarketTrace, view: SmartView) => void)[] =
	[];
const trace = (t: MarketTrace, view: SmartView) => {
	for (const watch of smartTraceWatchers) {
		watch(t, view);
	}
};

// Watchers of every deal a moment finds, before any is made (tests).
export const smartMomentWatchers: ((
	deals: readonly SmartDeal[],
	view: SmartView,
) => Promise<void>)[] = [];

export const standing = (view: SmartView, tid: number) => {
	const o = view.outlook.teams.get(tid);
	return o
		? `${o.strength[0]! >= 0 ? "+" : ""}${o.strength[0]!.toFixed(1)} now, ${(100 * o.title[0]!).toFixed(1)}% title`
		: "";
};

// The players each team shopped at the last moment: without a taker, they
// rest the next. Kept with the league (gameAttributes.smartMarketRested),
// so a reload or the sim moving to a league mate's device rests the same.
const shoppedBefore = () =>
	new Map(
		(g.get("smartMarketRested") ?? []).map(
			([tid, pids]) => [tid, new Set(pids)] as const,
		),
	);

// A market moment: its deals, best for the seller first, each team dealing
// at most once - each checked again as it stands just before it is made
// (the deals before it moved prices a little).
export const marketMoment = () => timed("market", marketMomentNow);

const marketMomentNow = async () => {
	// A moment can hold the worker for minutes: the room's busy mark renewed
	// first (sync/advanceLease.ts).
	await holdRoomForAdvance();
	// In free agency, what each team's room still buys, as of today.
	await noteVanillaPool();
	const outlook = await currentOutlook();
	const view = viewFor(outlook);
	const cpu = new Set(
		(await idb.cache.teams.getAll())
			.filter((t) => !t.disabled && !view.humans.has(t.tid))
			.map((t) => t.tid),
	);
	const dealt = new Set<number>();
	const made: SmartDeal[] = [];
	const shoppedNow = new Map<number, Set<number>>();
	const deals = await dealsNow(view, cpu, shoppedBefore(), (shopped) => {
		for (const c of shopped) {
			const set = shoppedNow.get(c.seller) ?? new Set();
			set.add(c.p.pid);
			shoppedNow.set(c.seller, set);
		}
	});
	const open = marketSettings.buyersChoose
		? await timed("market: alternatives", () => withAlternatives(view, deals))
		: deals;
	for (const watch of smartMomentWatchers) {
		await watch(open, view);
	}
	// A player traded is gone from his team; the rest shopped rest a moment.
	await setGameAttributes({
		smartMarketRested: [...shoppedNow].map(([tid, pids]) => [tid, [...pids]]),
	});
	// Each deal as it would be made: trimmed, with what each side gains - by
	// what the buyer gains first (buyersChoose).
	const priced: SmartDeal[] = [];
	if (marketSettings.buyersChoose) {
		for (const found of open) {
			await holdRoomForAdvance();
			const offer = await trimDeal(view, found);
			priced.push({
				...found,
				offer,
				sellerGain: await view.dealWorth(
					found.seller,
					found.buyer,
					found.out,
					offer,
				),
				buyerGain: await view.dealWorth(
					found.buyer,
					found.seller,
					offer,
					found.out,
				),
			});
		}
		priced.sort(
			(a, b) =>
				b.buyerGain - a.buyerGain ||
				b.sellerGain - a.sellerGain ||
				a.seller - b.seller,
		);
	}
	for (const found of marketSettings.buyersChoose ? priced : deals) {
		const at = {
			seller: found.seller,
			buyer: found.buyer,
			p: found.out.players[0]!,
			offer: found.offer,
		};
		if (dealt.has(found.seller) || dealt.has(found.buyer)) {
			trace({ stage: "dealt already", ...at }, view);
			continue;
		}
		if (!marketSettings.buyersChoose) {
			found.offer = await trimDeal(view, found);
		}
		const sellerGain = marketSettings.buyersChoose
			? found.sellerGain
			: await view.dealWorth(found.seller, found.buyer, found.out, found.offer);
		const buyerGain = marketSettings.buyersChoose
			? found.buyerGain
			: await view.dealWorth(found.buyer, found.seller, found.offer, found.out);
		if (
			!(sellerGain > marketSettings.minGain) ||
			!(buyerGain > marketSettings.minGain)
		) {
			marketStats.changed += 1;
			trace({ stage: "changed", ...at, sellerGain, buyerGain }, view);
			continue;
		}
		// Each side's gain past its margin of error counting no more of the
		// market's prices than it can be sure of getting (smartSettings.
		// premiumRealized).
		if (
			marketSettings.margins &&
			(!(
				view.sureWorthNow(found.seller, found.buyer, found.out, found.offer) >
				Math.max(
					marketSettings.minGain,
					marginFor(view, found.seller, found.out, found.offer),
				)
			) ||
				!(
					view.sureWorthNow(found.buyer, found.seller, found.offer, found.out) >
					Math.max(
						marketSettings.minGain,
						marginFor(view, found.buyer, found.offer, found.out),
					)
				))
		) {
			marketStats.withinMargin += 1;
			trace(
				{
					stage: "within margin",
					...at,
					sellerGain,
					buyerGain,
					why: `seller sure ${view.sureWorthNow(found.seller, found.buyer, found.out, found.offer).toFixed(3)} margin ${marginFor(view, found.seller, found.out, found.offer).toFixed(3)}; buyer sure ${view.sureWorthNow(found.buyer, found.seller, found.offer, found.out).toFixed(3)} margin ${marginFor(view, found.buyer, found.offer, found.out).toFixed(3)}`,
				},
				view,
			);
			continue;
		}
		if (!fairToSeller(sellerGain, buyerGain, found.buyerAlt)) {
			marketStats.unfair += 1;
			trace(
				{
					stage: "unfair",
					...at,
					sellerGain,
					buyerGain,
					why: `buyer's next best ${(found.buyerAlt ?? 0).toFixed(3)}`,
				},
				view,
			);
			continue;
		}
		const teams = teamsOf(found.seller, found.buyer, found.out, found.offer);
		if ((await summary(teams)).warning) {
			marketStats.illegal += 1;
			trace({ stage: "illegal", ...at, sellerGain, buyerGain }, view);
			continue;
		}
		trace({ stage: "made", ...at, sellerGain, buyerGain }, view);
		const deal = { ...found, sellerGain, buyerGain };
		for (const watch of smartDealWatchers) {
			watch(deal, view);
		}
		await processTrade(
			teamsOf(deal.seller, deal.buyer, deal.out, deal.offer),
			undefined,
			{
				initiatorTid: deal.buyer,
				tiers: [standing(view, deal.seller), standing(view, deal.buyer)],
				dv:
					Math.round(Math.min(deal.sellerGain, deal.buyerGain) * 10_000) /
					10_000,
				motivation: "smart market",
			},
		);
		dealt.add(deal.seller);
		dealt.add(deal.buyer);
		made.push(deal);
	}
	return made;
};

// When the market meets: in season every few weeks (not as it opens) and
// each of the last days before the deadline, in free agency at each stage
// (or on a few of its days), and as each offseason window opens
// (smartTradeWindow). Each moment once, by the league's own record of the
// last (gameAttributes.smartMarketMoment).
export const smartTradeDay = async () => {
	if (!smartDecidesHere()) {
		return [];
	}
	const phase = g.get("phase");
	let key: string | undefined;
	if (phase === PHASE.REGULAR_SEASON) {
		let daysLeft = Infinity;
		try {
			daysLeft = await getDaysLeftSchedule("tradeDeadline");
		} catch {
			// No deadline left this season.
		}
		const games = await gamesPlayed();
		const every = Math.max(
			1,
			g.get("numActiveTeams") *
				(smartVanillaOn()
					? marketSettings.vanillaGamesBetween
					: marketSettings.gamesBetween),
		);
		const period = Math.floor(games / every);
		// Not as the season opens: the preseason's window has just met.
		key =
			daysLeft < marketSettings.deadlineDays
				? `deadline:${daysLeft}`
				: period > 0
					? `games:${period}`
					: undefined;
	} else if (
		phase === PHASE.FREE_AGENCY &&
		(inStages() || marketSettings.freeAgencyDays.includes(g.get("daysLeft")))
	) {
		key = `fa:${g.get("daysLeft")}`;
	}
	if (key === undefined) {
		return [];
	}
	key = `${g.get("season")}:${phase}:${key}`;
	if (key === g.get("smartMarketMoment")) {
		return [];
	}
	await setGameAttributes({ smartMarketMoment: key });
	return marketMoment();
};

// The offseason phases the market meets as they open. Not the draft or
// right after it: in League 12 played on six seasons twice (2026-11-20) each
// made one or two deals in all, while each moment held the phase change for a
// minute - more on a phone - with the deals open then made a phase later.
const WINDOW_PHASES: readonly number[] = [
	PHASE.RESIGN_PLAYERS,
	PHASE.PRESEASON,
];

export const smartTradeWindow = async () => {
	if (!WINDOW_PHASES.includes(g.get("phase")) || !smartDecidesHere()) {
		return [];
	}
	return marketMoment();
};
