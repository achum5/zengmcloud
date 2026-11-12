import { PHASE } from "../../../common/constants.ts";
import type { Player, TradeTeams } from "../../../common/types.ts";
import { idb } from "../../db/index.ts";
import { g, helpers, local } from "../../util/index.ts";
import { contractOf } from "../realisticFrontOffice/contractOf.ts";
import getDaysLeftSchedule from "../season/getDaysLeftSchedule.ts";
import isUntradable from "../trade/isUntradable.ts";
import processTrade from "../trade/processTrade.ts";
import summary from "../trade/summary.ts";
import type { ValueChangeCalculator } from "../team/ValueChangeCalculator.ts";
import { tradeValueCalculator } from "./index.ts";
import { ensureLanes, type Lane } from "./lanes.ts";
import { tweaksFor, type Tweaks } from "./tweaks.ts";
import { marketWatchers, type MarketEvent } from "./events.ts";
import { flushNews } from "./news.ts";
import type { Calendar } from "./calendar.ts";
import { worthFor } from "./worth.ts";
import { vanillaPlusSettings } from "./settings.ts";

// THE MARKET (docs/vanilla-plus.md): CPU-to-CPU trades by direction, not at
// random as stock's. Each day a few teams go shopping, more before the
// deadline, all-in and rebuilding teams most:
//
// - a buyer (all-in or playoff) goes after the players on other teams who
//   would fit it best, sellers' first, and takes the deal it likes best that
//   the other team would make;
// - a seller (rebuilding or retooling) shops a veteran to the buyers he
//   would fit best; each makes its best offer, and the seller takes the
//   best of them, if it beats keeping him.
//
// Prices are vanilla+ values, settled the vanilla way (makeItWork); a deal
// goes through only if both teams come out ahead and it fits the cap rules.
export const vplusMarketSettings = {
	shoppersPerDay: 2,
	// The last days before the deadline: this many shop each day.
	deadlineDays: 7,
	deadlineShoppers: 8,
	// Each day of free agency.
	freeAgencyShoppers: 3,
	// How busy each lane is: a team's chance to be the day's shopper, relative.
	activity: { allIn: 3, playoff: 1, retool: 2, rebuild: 3 } as Record<
		Lane,
		number
	>,
	// A buyer tries this many targets; a seller hears this many buyers.
	targets: 3,
	buyers: 4,
	// A player worth shopping for: this value or more (stock's player value -
	// about a rotation player).
	minValue: 52,
	// Sellers shop players this old or older.
	sellAge: 27,
	// A team trades at most once in this many days.
	coolDownDays: 10,
	// Contracts a team may add as salary filler to make a deal legal.
	fillers: 3,
	// Most players and picks a team pays with in one deal.
	payWith: 4,
	// A buyer's targets: sellers' players this old or older, or anyone who
	// would fit it this well (tweaks' factor) or better.
	targetAge: 25,
	targetFit: 1.1,
};

const tell = (event: MarketEvent) => {
	for (const watch of marketWatchers) {
		watch(event);
	}
};

// What happened to the market's tries, for the arena.
export const marketStats = {
	tries: 0,
	noDeal: 0,
	oneSided: 0,
	capRules: 0,
	initiatorSays: 0,
	otherSays: 0,
	trades: 0,
};

// When each team last traded, by day of this season's market.
const lastTraded = new Map<number, number>();
let marketDay = 0;
let marketSeason = -1;

const cpuTids = async () => {
	const userTids = g.get("userTids");
	const spectator = g.get("spectator") || !!local.autoPlayUntil;
	return (await idb.cache.teams.getAll())
		.filter((t) => !t.disabled && (spectator || !userTids.includes(t.tid)))
		.map((t) => t.tid);
};

const empty = (tid: number) => ({
	tid,
	pids: [] as number[],
	dpids: [] as number[],
	pidsExcluded: [] as number[],
	dpidsExcluded: [] as number[],
});

// What a deal (the trade screen's teams) does for each side by its values.
const dvFor = (calc: ValueChangeCalculator, teams: TradeTeams, k: 0 | 1) =>
	calc.evaluate({
		tid: teams[k].tid,
		pidsAdd: teams[1 - k]!.pids,
		pidsRemove: teams[k].pids,
		dpidsAdd: teams[1 - k]!.dpids,
		dpidsRemove: teams[k].dpids,
		tradingPartnerTid: teams[1 - k]!.tid,
	});

const weighted = <T>(items: T[], weight: (x: T) => number) => {
	const total = items.reduce((s, x) => s + weight(x), 0);
	let r = Math.random() * total;
	for (const x of items) {
		r -= weight(x);
		if (r <= 0) {
			return x;
		}
	}
	return items.at(-1);
};

const playerAsset = (p: Player) =>
	({
		type: "player",
		pid: p.pid,
		value: 0,
		contractValue: 0,
		injury: p.injury,
		age: g.get("season") - p.born.year,
		justDrafted: false,
	}) as const;

// A deal the cap rules allow: as real teams do, the team taking on too much
// salary sends back its least valuable contracts as filler (a few at most).
const withFiller = async (teams: TradeTeams) => {
	const out = helpers.deepCopy(teams);
	for (let i = 0; i <= vplusMarketSettings.fillers; i++) {
		const s = await summary(out);
		if (!s.warning) {
			return out;
		}
		if (i === vplusMarketSettings.fillers) {
			return;
		}
		const net = ([0, 1] as const).map(
			(j) => s.teams[j].payrollAfterTrade - s.teams[j].payrollBeforeTrade,
		);
		const j = net[0]! >= net[1]! ? 0 : 1;
		if (net[j]! <= 0) {
			return;
		}
		const need = net[j]! * 1000;
		const candidates = (
			await idb.cache.players.indexGetAll("playersByTid", out[j].tid)
		)
			.filter(
				(p) =>
					!out[j].pids.includes(p.pid) &&
					!isUntradable(p).untradable &&
					contractOf(p).amount > g.get("minContract"),
			)
			.sort((a, b) => a.value - b.value);
		const filler =
			candidates.find((p) => contractOf(p).amount >= 0.5 * need) ??
			candidates
				.slice(0, Math.ceil(candidates.length / 2))
				.sort((a, b) => contractOf(b).amount - contractOf(a).amount)[0];
		if (!filler) {
			return;
		}
		out[j].pids.push(filler.pid);
	}
	return;
};

// A deal that works for both teams and the cap rules, or undefined.
const works = async (calc: ValueChangeCalculator, made: TradeTeams) => {
	marketStats.tries += 1;
	if (
		made[0].pids.length + made[0].dpids.length === 0 ||
		made[1].pids.length + made[1].dpids.length === 0
	) {
		marketStats.oneSided += 1;
		return;
	}
	const teams = await withFiller(made);
	if (!teams) {
		marketStats.capRules += 1;
		return;
	}
	const dv0 = await dvFor(calc, teams, 0);
	const dv1 = await dvFor(calc, teams, 1);
	if (dv0 <= 0) {
		marketStats.initiatorSays += 1;
		return;
	}
	if (dv1 <= 0) {
		marketStats.otherSays += 1;
		return;
	}
	return { teams, dv0, dv1 };
};

// What a team could pay with: its players and picks, each with what it is
// worth to the team getting it and what it costs the team paying.
const paymentOptions = async (
	calc: ValueChangeCalculator,
	payer: number,
	payee: number,
	exclude: number[],
) => {
	const options: {
		pid?: number;
		dpid?: number;
		gets: number;
		gives: number;
	}[] = [];
	const value = async (pid?: number, dpid?: number) => {
		const gets = await calc.evaluate({
			tid: payee,
			pidsAdd: pid === undefined ? [] : [pid],
			pidsRemove: [],
			dpidsAdd: dpid === undefined ? [] : [dpid],
			dpidsRemove: [],
			tradingPartnerTid: payer,
		});
		const gives = -(await calc.evaluate({
			tid: payer,
			pidsAdd: [],
			pidsRemove: pid === undefined ? [] : [pid],
			dpidsAdd: [],
			dpidsRemove: dpid === undefined ? [] : [dpid],
			tradingPartnerTid: payee,
		}));
		if (gets > 0) {
			options.push({ pid, dpid, gets, gives: Math.max(0, gives) });
		}
	};
	for (const p of await idb.cache.players.indexGetAll("playersByTid", payer)) {
		if (!exclude.includes(p.pid) && !isUntradable(p).untradable) {
			await value(p.pid);
		}
	}
	for (const dp of await idb.cache.draftPicks.indexGetAll(
		"draftPicksByTid",
		payer,
	)) {
		await value(undefined, dp.dpid);
	}
	// Best value for money first: what the other team gets, per what it costs.
	return options.sort(
		(a, b) => b.gets / (b.gives + 0.05) - a.gets / (a.gives + 0.05),
	);
};

// paymentOptions for one calculator, by payer and payee: what a team could
// pay another with doesn't change while the prices don't.
const optionsCache = new WeakMap<
	ValueChangeCalculator,
	Map<string, Awaited<ReturnType<typeof paymentOptions>>>
>();
const cachedOptions = async (
	calc: ValueChangeCalculator,
	payer: number,
	payee: number,
) => {
	let byPair = optionsCache.get(calc);
	if (!byPair) {
		byPair = new Map();
		optionsCache.set(calc, byPair);
	}
	const key = `${payer}>${payee}`;
	let options = byPair.get(key);
	if (!options) {
		options = await paymentOptions(calc, payer, payee, []);
		byPair.set(key, options);
	}
	return options;
};

// The teams that would want a player most, other than his own and the
// humans': by what he's worth to each, fit included.
export const keenestBidders = ({
	lanes,
	tweaks,
	calendar,
	humans,
	tid,
	p,
}: {
	lanes: Map<number, Lane>;
	tweaks: Tweaks;
	calendar: Calendar | undefined;
	humans: Set<number>;
	tid: number;
	p: Player;
}) =>
	[...lanes]
		.filter(([other]) => other !== tid && !humans.has(other))
		.map(([other, lane]) => ({
			other,
			want:
				worthFor(p, lane, calendar, calendar?.playoffs.get(other) ?? 1) *
				tweaks(other, playerAsset(p), "add"),
		}))
		.sort((a, b) => b.want - a.want)
		.slice(0, vanillaPlusSettings.bidders)
		.map((x) => x.other);

// THE BEST REAL OFFER ELSEWHERE (docs/vanilla-plus.md, "A market that never
// stops", 4): what the teams that want a player most would actually give
// his team for him - each its best offer, as much as still works for it -
// valued in his team's own terms. What a team won't take less than from a
// human: "I'm not just going to take this smaller amount if that team would
// pay more" (owner).
export const bestOfferElsewhere = async (
	calc: ValueChangeCalculator,
	seller: number,
	pid: number,
	bidders: number[],
) => {
	const s = vplusMarketSettings;
	let best = 0;
	for (const buyer of bidders) {
		const options = await cachedOptions(calc, buyer, seller);
		const teams: TradeTeams = [
			{ ...empty(buyer) },
			{ ...empty(seller), pids: [pid] },
		];
		let used = 0;
		for (const o of options) {
			if (used >= s.payWith) {
				break;
			}
			const next = helpers.deepCopy(teams);
			if (o.pid !== undefined) {
				next[0].pids.push(o.pid);
			} else {
				next[0].dpids.push(o.dpid!);
			}
			if ((await dvFor(calc, next, 0)) <= 0) {
				continue;
			}
			teams[0] = next[0];
			used += 1;
		}
		if (used === 0) {
			continue;
		}
		const gets = await calc.evaluate({
			tid: seller,
			pidsAdd: teams[0].pids,
			pidsRemove: [],
			dpidsAdd: teams[0].dpids,
			dpidsRemove: [],
			tradingPartnerTid: buyer,
		});
		best = Math.max(best, gets);
	}
	return best;
};

const roomFor = async (tid: number, giving: number, getting: number) => {
	const now = (await idb.cache.players.indexGetAll("playersByTid", tid)).length;
	return Math.max(g.get("maxRosterSize"), now) - (now - giving + getting);
};

// The payer pays for the payee's players with its own, picking what's worth
// most to the payee for what it costs the payer - a contender's picks and
// prospects, a rebuilder's veterans - not, as stock's makeItWork would, the
// payee's best read of the payer's roster regardless of what it costs. With
// `bid`, the payer keeps adding while the deal still works for it: its best
// offer. Otherwise the least that gets the payee to yes.
const pay = async (
	calc: ValueChangeCalculator,
	payer: number,
	payee: number,
	pids: number[],
	bid: boolean,
): Promise<TradeTeams | undefined> => {
	const s = vplusMarketSettings;
	const options = await cachedOptions(calc, payer, payee);
	const teams: TradeTeams = [
		{ ...empty(payer) },
		{ ...empty(payee), pids: [...pids] },
	];
	const with_ = (o: (typeof options)[number]) => {
		const next = helpers.deepCopy(teams);
		if (o.pid !== undefined) {
			next[0].pids.push(o.pid);
		} else {
			next[0].dpids.push(o.dpid!);
		}
		return next;
	};
	let payeeDv = await dvFor(calc, teams, 1);
	let used = 0;
	for (const o of options) {
		if (payeeDv > 0 && !bid) {
			break;
		}
		if (used >= s.payWith) {
			break;
		}
		if (
			o.pid !== undefined &&
			(await roomFor(payee, pids.length, teams[0].pids.length + 1)) < 0
		) {
			continue;
		}
		const next = with_(o);
		if ((await dvFor(calc, next, 0)) <= 0) {
			// Past what the payer would give: something smaller may still do.
			continue;
		}
		teams[0] = next[0];
		used += 1;
		payeeDv = await dvFor(calc, teams, 1);
	}
	if (payeeDv <= 0) {
		return;
	}
	// Anything the payee would still say yes without, the payer keeps.
	if (!bid) {
		for (const pid of teams[0].pids) {
			const next = helpers.deepCopy(teams);
			next[0].pids = next[0].pids.filter((x) => x !== pid);
			if (
				next[0].pids.length + next[0].dpids.length > 0 &&
				(await dvFor(calc, next, 1)) > 0
			) {
				teams[0] = next[0];
			}
		}
		for (const dpid of teams[0].dpids) {
			const next = helpers.deepCopy(teams);
			next[0].dpids = next[0].dpids.filter((x) => x !== dpid);
			if (
				next[0].pids.length + next[0].dpids.length > 0 &&
				(await dvFor(calc, next, 1)) > 0
			) {
				teams[0] = next[0];
			}
		}
	}
	return teams;
};

const buyerShops = async (
	calc: ValueChangeCalculator,
	tweaks: Tweaks,
	lanes: Map<number, Lane>,
	buyer: number,
	others: number[],
	players: Player[],
) => {
	const s = vplusMarketSettings;
	const roster = players
		.filter((p) => p.tid === buyer)
		.sort((a, b) => b.value - a.value);
	// Upgrades: at least as good as its fifth best.
	const bar = Math.max(s.minValue, roster[4]?.value ?? 0);
	const scored = players
		.filter(
			(p) =>
				others.includes(p.tid) &&
				p.value >= bar &&
				!isUntradable(p).untradable &&
				canTrade(p.tid),
		)
		.map((p) => {
			const sellerLane = lanes.get(p.tid);
			const seller = sellerLane === "rebuild" || sellerLane === "retool";
			const fit = tweaks(buyer, playerAsset(p), "add");
			return {
				p,
				fit,
				score:
					(seller && g.get("season") - p.born.year >= s.targetAge) ||
					fit >= s.targetFit
						? fit * (seller ? 1.2 : 1)
						: 0,
			};
		})
		.filter((x) => x.score > 0)
		.sort((a, b) => b.score - a.score)
		.slice(0, s.targets);
	if (scored.length === 0) {
		return false;
	}
	tell({ kind: "hunt", buyer, pids: scored.map((x) => x.p.pid) });
	let best:
		| { teams: TradeTeams; dv0: number; pid: number; fit: number }
		| undefined;
	for (const { p, fit } of scored) {
		// The least of what the buyer has that gets the seller to yes.
		const made = await pay(calc, buyer, p.tid, [p.pid], false);
		if (!made) {
			marketStats.noDeal += 1;
			continue;
		}
		const deal = await works(calc, made);
		if (deal && (!best || deal.dv0 > best.dv0)) {
			best = { teams: deal.teams, dv0: deal.dv0, pid: p.pid, fit };
		}
	}
	if (!best) {
		return false;
	}
	await execute(best.teams, buyer, "buyer", best.pid, 1, best.fit);
	return true;
};

const sellerShops = async (
	calc: ValueChangeCalculator,
	tweaks: Tweaks,
	lanes: Map<number, Lane>,
	seller: number,
	others: number[],
	players: Player[],
) => {
	const s = vplusMarketSettings;
	const season = g.get("season");
	const veterans = players.filter(
		(p) =>
			p.tid === seller &&
			season - p.born.year >= s.sellAge &&
			p.value >= s.minValue &&
			!isUntradable(p).untradable,
	);
	const vet = weighted(veterans, (p) => p.value);
	if (!vet) {
		return false;
	}
	const buyers = others
		.filter((tid) => {
			const lane = lanes.get(tid);
			return (lane === "allIn" || lane === "playoff") && canTrade(tid);
		})
		.map((tid) => ({ tid, fit: tweaks(tid, playerAsset(vet), "add") }))
		.sort((a, b) => b.fit - a.fit)
		.slice(0, s.buyers);
	let offers = 0;
	let best: { teams: TradeTeams; dv0: number; fit: number } | undefined;
	for (const { tid, fit } of buyers) {
		// Each buyer's best offer for him: as much as it would pay.
		const offer = await pay(calc, tid, seller, [vet.pid], true);
		if (!offer) {
			marketStats.noDeal += 1;
			continue;
		}
		// The seller's side first, as the initiator.
		const made: TradeTeams = [offer[1], offer[0]];
		const deal = await works(calc, made);
		if (!deal) {
			continue;
		}
		offers += 1;
		if (!best || deal.dv0 > best.dv0) {
			best = { teams: deal.teams, dv0: deal.dv0, fit };
		}
	}
	tell({ kind: "shopped", seller, pid: vet.pid, offers });
	if (!best) {
		return false;
	}
	await execute(best.teams, seller, "seller", vet.pid, offers, best.fit);
	return true;
};

const canTrade = (tid: number) => {
	const last = lastTraded.get(tid);
	return (
		last === undefined || marketDay - last >= vplusMarketSettings.coolDownDays
	);
};

const execute = async (
	teams: TradeTeams,
	initiator: number,
	role: "buyer" | "seller",
	pid: number,
	offers: number,
	fit: number,
) => {
	await processTrade(teams, undefined);
	marketStats.trades += 1;
	lastTraded.set(teams[0].tid, marketDay);
	lastTraded.set(teams[1].tid, marketDay);
	tell({ kind: "trade", teams, initiator, role, pid, offers, fit });
};

const inDeadlineDays = async () => {
	try {
		return (
			(await getDaysLeftSchedule("tradeDeadline")) <
			vplusMarketSettings.deadlineDays
		);
	} catch {
		return false;
	}
};

// One day of the market.
export const tradeDay = async () => {
	const s = vplusMarketSettings;
	const phase = g.get("phase");
	if (marketSeason !== g.get("season")) {
		marketSeason = g.get("season");
		lastTraded.clear();
		marketDay = 0;
	}
	marketDay += 1;
	let shoppers: number;
	if (phase === PHASE.REGULAR_SEASON) {
		shoppers = (await inDeadlineDays()) ? s.deadlineShoppers : s.shoppersPerDay;
	} else if (phase === PHASE.FREE_AGENCY) {
		shoppers = s.freeAgencyShoppers;
	} else {
		return;
	}
	const tids = await cpuTids();
	if (tids.length < 2) {
		return;
	}
	const lanes = await ensureLanes();
	let calc = await tradeValueCalculator();
	let tweaks = await tweaksFor(lanes);
	let players = await idb.cache.players.indexGetAll("playersByTid", [
		0,
		Infinity,
	]);
	const pool = tids.filter(canTrade);
	for (let i = 0; i < shoppers && pool.length > 0; i++) {
		const tid = weighted(pool, (x) => s.activity[lanes.get(x) ?? "playoff"])!;
		pool.splice(pool.indexOf(tid), 1);
		if (!canTrade(tid)) {
			continue;
		}
		const lane = lanes.get(tid) ?? "playoff";
		const others = tids.filter((x) => x !== tid);
		const traded =
			lane === "allIn" || lane === "playoff"
				? await buyerShops(calc, tweaks, lanes, tid, others, players)
				: await sellerShops(calc, tweaks, lanes, tid, others, players);
		if (traded) {
			// Rosters moved: price afresh.
			calc = await tradeValueCalculator();
			tweaks = await tweaksFor(lanes);
			players = await idb.cache.players.indexGetAll("playersByTid", [
				0,
				Infinity,
			]);
		}
	}
	await flushNews();
};
