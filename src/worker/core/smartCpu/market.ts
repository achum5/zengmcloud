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
import { viewFor } from "./signing.ts";
import { smartSettings, type Holdings, type SmartView } from "./worth.ts";

// THE GM'S MARKET (docs/smart-cpu.md, layer 2). At each market moment, every
// CPU team's tradable players are priced by their owner's use and the bids
// of the keenest teams. A player is a candidate sale when a keen CPU team
// bids above both what he does for his own team and what the rest of the
// market would pay, and could take him under the salary rules in an offer
// of a few players. Each candidate is put to the best real offer from the
// keen CPU teams (layer 1's bestRealOffer: each its best for the seller
// while it still gains, legal under the trade rules), and the seller takes
// it if it gains - the market rule kept by construction. The deals are made
// best for the seller first, each team dealing at most once a moment, each
// checked again as it stands before it is made.

export const marketSettings = {
	// Most deals a moment.
	dealsPerMoment: 8,
	// The keen teams each candidate is put to, keenest first (2026-11-14:
	// four found 82 deals a moment, two 74, and the deals made were the
	// same but one - for a third less work).
	buyers: 2,
	// The players each team shops a moment, those the market wants most
	// past what they do for it first (2026-11-14: the deals made were the
	// same shopping five as shopping all, for a third less work).
	shopped: 5,
	// Candidates ranked by a rough read (rosters not cut to the limit).
	roughRanking: true,
	// Each shopped player put to the teams that want him most of all teams,
	// not only of the keenest.
	anyBuyer: true,
	// A buyer's best players by its own use, kept out of salary matching.
	core: 3,
	// In season, a moment about every this many games a team, and every day
	// in the last few before the deadline - where three in four of the NBA's
	// in-season trades are made (arena benchmarks).
	gamesBetween: 17,
	deadlineDays: 5,
	// In free agency, a moment on these days left - or, in stages, at each
	// stage (daysLeft counts the stages down).
	freeAgencyDays: [30, 20, 10],
};

// Where candidate sales fall away (for tests and the arena).
export const marketStats = {
	candidates: 0,
	noOffer: 0,
	sellerSaysNo: 0,
	// Deals no longer good for both by the time their turn came.
	changed: 0,
	illegal: 0,
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
};

const none: Holdings = { players: [], picks: [] };
const one = (x: Player | DraftPick): Holdings =>
	"pid" in x ? { players: [x], picks: [] } : { players: [], picks: [x] };
const teamsOf = (a: number, b: number, giveA: Holdings, giveB: Holdings) =>
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

// The candidate sales at this moment, best surplus first: players some keen
// CPU team would pay more for than their owner and the rest of the market.
export const candidates = async (
	view: SmartView,
	cpu: ReadonlySet<number>,
	dealt: ReadonlySet<number>,
	// Players each team shopped last moment without a taker: they sit this
	// one out, behind its others.
	rested: ReadonlyMap<number, ReadonlySet<number>> = new Map(),
) => {
	const keen = await view.keenest();
	const found: {
		seller: number;
		buyer: number;
		p: Player;
		surplus: number;
		// The teams that want him most, of all of them (shopped players).
		buyers?: number[];
	}[] = [];
	for (const seller of cpu) {
		if (dealt.has(seller)) {
			continue;
		}
		const held = view.heldNow(seller);
		await ensureRetention(view.outlook, held.players, seller);
		for (const p of held.players) {
			if (isUntradable(p).untradable) {
				continue;
			}
			// Ranked by the keen teams' uses for him (what each would give),
			// read roughly - rosters not cut to the limit, a third of the work
			// (2026-11-14) - the salary rules left to `matchable` and the real
			// offer: a legal bid costs a franchise value more for every keen
			// team, every player (31 s of a moment).
			const read = (tid: number) =>
				marketSettings.roughRanking
					? view.roughUseOfNow(tid, one(p))
					: view.useOfNow(tid, one(p));
			const own = read(seller);
			const bids = keen
				.filter((k) => k !== seller)
				.map((k) => ({ k, bid: read(k) }))
				.sort((a, b) => b.bid - a.bid);
			const top = bids.find((b) => cpu.has(b.k) && !dealt.has(b.k));
			if (!top) {
				continue;
			}
			const rest = bids.filter((b) => b.k !== top.k);
			const price = rest[1]?.bid ?? rest[0]?.bid ?? -Infinity;
			const surplus = top.bid - Math.max(own, price);
			if (surplus > 0 && matchable(view, top.k, p)) {
				found.push({ seller, buyer: top.k, p, surplus });
			}
		}
	}
	// Each team shops the few the market wants most past what they do for
	// it, as a GM puts players on the block - those it shopped last time
	// without a taker after its others.
	const shopped = new Map<number, number>();
	const wasRested = (c: (typeof found)[number]) =>
		rested.get(c.seller)?.has(c.p.pid) ? 1 : 0;
	const chosen = found
		.sort(
			(a, b) =>
				wasRested(a) - wasRested(b) ||
				b.surplus - a.surplus ||
				a.p.pid - b.p.pid,
		)
		.filter((c) => {
			const n = (shopped.get(c.seller) ?? 0) + 1;
			shopped.set(c.seller, n);
			return n <= marketSettings.shopped;
		});
	// Each put to the teams that want him most, of all of them: the keenest
	// franchises set prices, but any team may buy (2026-11-14: with only the
	// six keenest buying, five teams made 85% of the purchases).
	if (marketSettings.anyBuyer) {
		for (const c of chosen) {
			const wants: { k: number; want: number }[] = [];
			for (const k of cpu) {
				if (k !== c.seller && !dealt.has(k)) {
					await ensureRetention(view.outlook, [c.p], k);
					wants.push({ k, want: view.roughUseOfNow(k, one(c.p)) });
				}
			}
			c.buyers = wants
				.sort((a, b) => b.want - a.want || a.k - b.k)
				.slice(0, marketSettings.buyers)
				.map((w) => w.k);
		}
	}
	return chosen;
};

// Whether a buyer could take him under the salary rules in an offer of a few
// players: with room for him, or with its dearest few players outside its
// core matching his salary. A price counts any trade the buyer would still
// gain from; a real offer is a few players (bestRealOffer), and expensive
// players a contender can only match with its core were all of the first
// two dozen candidates and none of the 26 deals (2026-11-13 scan).
const matchable = (view: SmartView, buyer: number, p: Player) => {
	const none = { players: [], picks: [] };
	if (view.salariesWork(buyer, none, one(p))) {
		return true;
	}
	const held = view.heldNow(buyer);
	const uses = view.usesOf(buyer, held);
	const byUse = [...held.players].sort(
		(a, b) => uses.get(b.pid)! - uses.get(a.pid)! || a.pid - b.pid,
	);
	const others = byUse
		.slice(marketSettings.core)
		.filter((x) => !isUntradable(x).untradable)
		.sort((a, b) => contractOf(b).amount - contractOf(a).amount)
		.slice(0, smartSettings.offerSize);
	return view.salariesWork(buyer, { players: others, picks: [] }, one(p));
};

// The winning offer, trimmed to what it must be: the buyer takes back what it
// would miss most, one piece at a time, while the seller still gains by more
// than `bar` (the next best real offer, or nothing). A real offer is its
// buyer's best for the seller while it still gains - all it would pay; as
// at an auction the winner pays about what beats the next bidder (untrimmed,
// a contender gave two prospects and two firsts for a 31-year-old 60 and
// kept 0.0009 of a title for itself). Salaries must still work.
const trimmed = (
	view: SmartView,
	seller: number,
	buyer: number,
	out: Holdings,
	offer: Holdings,
	bar: number,
	// Their payrolls as the trade rules count them.
	payrolls: { seller: number; buyer: number },
) => {
	const salary = (h: Holdings) =>
		h.players.reduce((sum, p) => sum + contractOf(p).amount, 0);
	let now = offer;
	for (;;) {
		const pieces: (Player | DraftPick)[] = [...now.players, ...now.picks];
		if (pieces.length <= 1) {
			return now;
		}
		// Its own cost of each piece, dearest first.
		const costs = pieces
			.map((x) => ({
				x,
				cost: -view.dealWorthNow(buyer, seller, one(x), none),
			}))
			.sort((a, b) => b.cost - a.cost);
		let next: Holdings | undefined;
		for (const { x } of costs) {
			const without = {
				players: now.players.filter((p) => p !== x),
				picks: now.picks.filter((dp) => dp !== x),
			};
			if (
				view.dealWorthNow(seller, buyer, out, without) > bar &&
				salaryRulesAllow(buyer, payrolls.buyer, salary(out), salary(without)) &&
				salaryRulesAllow(seller, payrolls.seller, salary(without), salary(out))
			) {
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
	// Every player's chance of staying with each team that sets prices, so
	// what anything is worth doesn't hang on what was priced before.
	const keen = await view.keenest();
	await timed("market: retention", async () => {
		for (const tid of cpu) {
			for (const k of [tid, ...keen]) {
				await ensureRetention(view.outlook, view.heldNow(tid).players, k);
			}
		}
	});
	const deals: SmartDeal[] = [];
	const all = await timed("market: candidates", () =>
		candidates(view, cpu, new Set(), rested),
	);
	onShopped?.(all);
	marketStats.candidates += all.length;
	for (const c of all) {
		const out = one(c.p);
		const real = await timed("market: offers", () =>
			view.bestRealOffer(
				c.seller,
				out,
				-1,
				new Set(),
				marketSettings.buyers,
				c.buyers,
			),
		);
		if (!real) {
			marketStats.noOffer += 1;
			continue;
		}
		if (!(real.worth > 0)) {
			marketStats.sellerSaysNo += 1;
			continue;
		}
		deals.push({
			seller: c.seller,
			buyer: real.buyer,
			out,
			offer: real.offer,
			sellerGain: real.worth,
			buyerGain: Number.NaN,
			bar: Math.max(0, real.second),
		});
	}
	return deals.sort((a, b) => b.sellerGain - a.sellerGain);
};

// A deal about to be made, its offer trimmed to what beats the next best
// (trimmed): only the few a moment makes are trimmed, not every deal found
// (82 one moment, 4 of them made).
export const trimDeal = async (view: SmartView, deal: SmartDeal) => {
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
		),
	);
};

// Watchers of the deals the market makes (tests, the arena).
export const smartDealWatchers: ((deal: SmartDeal) => void)[] = [];

const standing = (view: SmartView, tid: number) => {
	const o = view.outlook.teams.get(tid);
	return o
		? `${o.strength[0]! >= 0 ? "+" : ""}${o.strength[0]!.toFixed(1)} now, ${(100 * o.title[0]!).toFixed(1)}% title`
		: "";
};

// The players each team shopped at the last moment (smartCpu's market,
// for this league): without a taker, they rest the next.
let shoppedBefore: {
	lid: number | undefined;
	rested: Map<number, Set<number>>;
} = { lid: undefined, rested: new Map() };

// A market moment: its deals, best for the seller first, each team dealing
// at most once - each checked again as it stands just before it is made
// (the deals before it moved prices a little).
export const marketMoment = () => timed("market", marketMomentNow);

const marketMomentNow = async () => {
	const outlook = await currentOutlook();
	const view = viewFor(outlook);
	const cpu = new Set(
		(await idb.cache.teams.getAll())
			.filter((t) => !t.disabled && !view.humans.has(t.tid))
			.map((t) => t.tid),
	);
	const dealt = new Set<number>();
	const made: SmartDeal[] = [];
	if (shoppedBefore.lid !== g.get("lid")) {
		shoppedBefore = { lid: g.get("lid"), rested: new Map() };
	}
	const shoppedNow = new Map<number, Set<number>>();
	const deals = await dealsNow(view, cpu, shoppedBefore.rested, (shopped) => {
		for (const c of shopped) {
			const set = shoppedNow.get(c.seller) ?? new Set();
			set.add(c.p.pid);
			shoppedNow.set(c.seller, set);
		}
	});
	// A player traded is gone from his team; the rest shopped rest a moment.
	shoppedBefore = { lid: g.get("lid"), rested: shoppedNow };
	for (const found of deals) {
		if (made.length >= marketSettings.dealsPerMoment) {
			break;
		}
		if (dealt.has(found.seller) || dealt.has(found.buyer)) {
			continue;
		}
		found.offer = await trimDeal(view, found);
		const sellerGain = await view.dealWorth(
			found.seller,
			found.buyer,
			found.out,
			found.offer,
		);
		const buyerGain = await view.dealWorth(
			found.buyer,
			found.seller,
			found.offer,
			found.out,
		);
		if (!(sellerGain > 0) || !(buyerGain > 0)) {
			marketStats.changed += 1;
			continue;
		}
		const teams = teamsOf(found.seller, found.buyer, found.out, found.offer);
		if ((await summary(teams)).warning) {
			marketStats.illegal += 1;
			continue;
		}
		const deal = { ...found, sellerGain, buyerGain };
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
		for (const watch of smartDealWatchers) {
			watch(deal);
		}
	}
	return made;
};

// When the market meets: in season every few weeks (not as it opens) and
// each of the last days before the deadline, in free agency at each stage
// (or on a few of its days), and as each offseason window opens
// (smartTradeWindow). Each moment once, by the league's own record of the
// last (gameAttributes.smartMarketMoment).
export const smartTradeDay = async () => {
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
			g.get("numActiveTeams") * marketSettings.gamesBetween,
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

const WINDOW_PHASES: readonly number[] = [
	PHASE.DRAFT,
	PHASE.AFTER_DRAFT,
	PHASE.RESIGN_PLAYERS,
	PHASE.PRESEASON,
];

export const smartTradeWindow = async () => {
	if (!WINDOW_PHASES.includes(g.get("phase"))) {
		return [];
	}
	return marketMoment();
};
