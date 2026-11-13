import { PHASE } from "../../../common/constants.ts";
import type { Player, TradeTeams } from "../../../common/types.ts";
import { idb } from "../../db/index.ts";
import { g, helpers, local } from "../../util/index.ts";
import { contractOf } from "../realisticFrontOffice/contractOf.ts";
import getDaysLeftSchedule from "../season/getDaysLeftSchedule.ts";
import isUntradable from "../trade/isUntradable.ts";
import processTrade from "../trade/processTrade.ts";
import setGameAttributes from "../league/setGameAttributes.ts";
import summary from "../trade/summary.ts";
import type { ValueChangeCalculator } from "../team/ValueChangeCalculator.ts";
import { tradeValueCalculator } from "./index.ts";
import { ensureLanes, type Lane } from "./lanes.ts";
import { tweaksFor, type Tweaks } from "./tweaks.ts";
import { marketWatchers, type MarketEvent } from "./events.ts";
import { flushNews } from "./news.ts";
import { calendar, type Calendar } from "./calendar.ts";
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
	buyers: 8,
	// A player worth shopping for: this value or more (stock's player value -
	// about a rotation player).
	minValue: 52,
	// Sellers shop players this old or older.
	sellAge: 27,
	// A team trades at most once in this many days.
	coolDownDays: 10,
	// With patience on (settings.ts), in the regular season every CPU team
	// shops on a day with this chance - about every ten days early, rising to
	// every other day at the deadline - times its lane's activity against the
	// league's average; and waits less after a trade.
	shopEarly: 0.1,
	shopDeadline: 0.5,
	shopCurve: 2,
	patientCoolDownDays: 5,
	// ...and in the last `deadlineDays` before the deadline, this many: the
	// deadline's rush, deal after deal.
	deadlineCoolDownDays: 5,
	// Contracts a team may add as salary filler to make a deal legal.
	fillers: 3,
	// Most players and picks a team pays with in one deal.
	payWith: 4,
	// A best offer (calendar on: bestPackage) is made from the payer's this
	// many assets worth most to the payee, and the best this many packages by
	// the sums are checked against the real values.
	packageFrom: 16,
	packageChecks: 6,
	// A real offer elsewhere is looked for only from this share of what the
	// team asks another CPU team for the player.
	offerNear: 0.9,
	// With auctions on (settings.ts), the other teams a seller hears from
	// (every one that can trade), and with nobody bidding against the buyer,
	// how far from what the seller would take toward the most the buyer
	// would give the price lands.
	auctionBidders: Infinity,
	bargainShare: 0.5,
	// A buyer's targets: sellers' players this old or older, or anyone who
	// would fit it this well (tweaks' factor) or better.
	targetAge: 25,
	targetFit: 1.1,
	// The offseason's trading windows (tradeWindow).
	windows: true,
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
	// Auctions where another team beat the buyer's first offer, and where
	// nobody did and the two met halfway.
	outbid: 0,
	bargained: 0,
	// Teams that shopped, and the time the market took (ms).
	looks: 0,
	ms: 0,
	// Each trade's day of the market, and each season's last day before the
	// deadline: how deals spread through the season.
	tradeDays: [] as {
		season: number;
		phase: number;
		day: number;
		// The smaller side's gain, as a share of what it gave (gainOf).
		gain: number;
	}[],
	deadlineDay: {} as Record<number, number>,
};

// The market's day this season, and when each team last traded, by that day
// - kept with the league (gameAttributes.vplusMarket), not on this device.
const clock = () => {
	const m = g.get("vplusMarket");
	return m?.season === g.get("season")
		? m
		: { season: g.get("season"), day: 0, traded: {} as Record<number, number> };
};

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

// What each asset is worth to each team getting it, and costs the team
// giving it, in a deal between CPU teams - the pieces every search below adds
// up - once per calculator.
const pieces = new WeakMap<ValueChangeCalculator, Map<string, number>>();
const piece = async (
	calc: ValueChangeCalculator,
	key: string,
	value: () => Promise<number>,
) => {
	let known = pieces.get(calc);
	if (!known) {
		known = new Map();
		pieces.set(calc, known);
	}
	let x = known.get(key);
	if (x === undefined) {
		x = await value();
		known.set(key, x);
	}
	return x;
};
const assetKey = (pid: number | undefined, dpid: number | undefined) =>
	pid !== undefined ? `p${pid}` : `d${dpid}`;
// Worth to `tid`, getting it from `from` (its worth alone: what a deal does
// to a team's own pick is left to the check of the whole deal).
const getsFrom = (
	calc: ValueChangeCalculator,
	tid: number,
	from: number,
	pid?: number,
	dpid?: number,
) =>
	piece(
		calc,
		`+${tid}:${from === g.get("userTid")}:${assetKey(pid, dpid)}`,
		() =>
			calc.evaluate({
				tid,
				pidsAdd: pid === undefined ? [] : [pid],
				pidsRemove: [],
				dpidsAdd: dpid === undefined ? [] : [dpid],
				dpidsRemove: [],
				tradingPartnerTid: from,
				quick: true,
			}),
	);
// What giving it to `to` costs `tid`.
const givesTo = (
	calc: ValueChangeCalculator,
	tid: number,
	to: number,
	pid?: number,
	dpid?: number,
) =>
	piece(
		calc,
		`-${tid}:${to === g.get("userTid")}:${assetKey(pid, dpid)}`,
		async () =>
			-(await calc.evaluate({
				tid,
				pidsAdd: [],
				pidsRemove: pid === undefined ? [] : [pid],
				dpidsAdd: [],
				dpidsRemove: dpid === undefined ? [] : [dpid],
				tradingPartnerTid: to,
				raw: true,
			})),
	);

// What a team could pay with: its players and picks, each with what it is
// worth to the team getting it and what it costs the team paying.
const paymentOptions = async (
	calc: ValueChangeCalculator,
	payer: number,
	payee: number,
) => {
	const options: {
		pid?: number;
		dpid?: number;
		gets: number;
		gives: number;
	}[] = [];
	const value = async (pid?: number, dpid?: number) => {
		const gets = await getsFrom(calc, payee, payer, pid, dpid);
		const gives = await givesTo(calc, payer, payee, pid, dpid);
		if (gets > 0) {
			options.push({ pid, dpid, gets, gives: Math.max(0, gives) });
		}
	};
	for (const p of await idb.cache.players.indexGetAll("playersByTid", payer)) {
		if (!isUntradable(p).untradable) {
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
		options = await paymentOptions(calc, payer, payee);
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
				worthFor(p, lane, calendar, calendar?.playoffs.get(other) ?? 1, other) *
				tweaks(other, playerAsset(p), "add"),
		}))
		.sort((a, b) => b.want - a.want)
		.slice(0, vanillaPlusSettings.bidders)
		.map((x) => x.other);

// THE BEST REAL OFFER ELSEWHERE (docs/vanilla-plus.md, "A market that never
// stops", 4): what other teams would actually give his team for a player -
// each its best offer, as much as still works for it - valued in his team's
// own terms, as it values anything it gets. What a team won't take less than
// from a human: "I'm not just going to take this smaller amount if that team
// would pay more" (owner). Only an offer near or above what the team asks
// another CPU team for him counts (0 otherwise): below that it changes
// nothing.
export const bestOfferElsewhere = async (
	calc: ValueChangeCalculator,
	seller: number,
	pid: number,
	bidders: number[],
) => {
	const asks =
		bidders.length > 0
			? vplusMarketSettings.offerNear *
				(await givesTo(calc, seller, bidders[0]!, pid))
			: 0;
	return (await bestOffer(calc, seller, pid, bidders, asks))?.gets ?? 0;
};

// The best real offer for a player from these teams that's worth more than
// `over` to his team: who makes it, the deal, and its worth to his team.
const bestOffer = async (
	calc: ValueChangeCalculator,
	seller: number,
	pid: number,
	bidders: number[],
	over: number,
) => {
	// Each team's packages by the sums - nearly what they're really worth -
	// and the teams with the best first: a team that couldn't beat the best
	// real offer so far isn't asked.
	const searches = [];
	for (const buyer of bidders) {
		const search = await packagesFor(calc, buyer, seller, [pid]);
		if (search && search.found.length > 0) {
			searches.push({ buyer, search });
		}
	}
	searches.sort((a, b) => b.search.found[0]!.gets - a.search.found[0]!.gets);
	let best: { buyer: number; teams: TradeTeams; gets: number } | undefined;
	for (const { buyer, search } of searches) {
		const bar = Math.max(over, best?.gets ?? 0);
		if (search.found[0]!.gets <= bar) {
			break;
		}
		const deal = await firstThatWorks(calc, search, bar);
		if (!deal) {
			continue;
		}
		const gets = await worthTo(calc, seller, deal.teams[0]);
		if (gets > bar) {
			best = { buyer, teams: deal.teams, gets };
		}
	}
	return best;
};

// What a payer's side of a deal is worth to the team getting it.
const worthTo = (
	calc: ValueChangeCalculator,
	tid: number,
	side: TradeTeams[number],
) =>
	calc.evaluate({
		tid,
		pidsAdd: side.pids,
		pidsRemove: [],
		dpidsAdd: side.dpids,
		dpidsRemove: [],
		tradingPartnerTid: side.tid,
		raw: true,
	});

// THE AUCTION (docs/vanilla-plus.md, "A market that never stops", 4;
// setting `auctions`): before a seller says yes to a buyer, every other team
// that would want the player bids - its best real offer. The team that would
// pay the most gets him, paying just enough to beat the next best, as a real
// market settles: "good teams should be having to bid on that player"
// (owner). With no other bid that beats the buyer's, the two meet halfway
// between what the seller would take and the most the buyer would give -
// not the least the seller would take. Returns the deal that's made, its
// buyer and how many offers the seller heard.
const auction = async (
	calc: ValueChangeCalculator,
	tweaks: Tweaks,
	lanes: Map<number, Lane>,
	deal: { teams: TradeTeams; pid: number },
	// The CPU teams.
	cpu: number[],
) => {
	const s = vplusMarketSettings;
	const [{ tid: buyer }, { tid: seller }] = deal.teams;
	const p = await idb.cache.players.get(deal.pid);
	const offered = await worthTo(calc, seller, deal.teams[0]);
	if (!p) {
		return { teams: deal.teams, buyer, offers: 1 };
	}
	const c = await calendar();
	const rivals = keenestBidders({
		lanes,
		tweaks,
		calendar: c,
		humans: new Set([buyer]),
		tid: seller,
		p,
	})
		.filter((tid) => cpu.includes(tid) && canTrade(tid))
		.slice(0, s.auctionBidders);
	const rival = await bestOffer(calc, seller, p.pid, rivals, offered);
	// The buyer's most.
	const most = await bestPackage(calc, buyer, seller, [p.pid]);
	const buyerMost = most ? await worthTo(calc, seller, most[0]) : offered;
	let winner: number;
	let toBeat: number;
	let fallback: TradeTeams | undefined;
	if (rival) {
		marketStats.outbid += 1;
		[winner, toBeat, fallback] =
			buyerMost >= rival.gets
				? [buyer, rival.gets, most]
				: [rival.buyer, Math.max(offered, buyerMost), rival.teams];
	} else if (buyerMost > offered) {
		marketStats.bargained += 1;
		[winner, toBeat, fallback] = [
			buyer,
			offered + s.bargainShare * (buyerMost - offered),
			deal.teams,
		];
	} else {
		return { teams: deal.teams, buyer, offers: 1 };
	}
	// What the player alone is worth to his team: the price to beat is on
	// top of it.
	const price = -(await calc.evaluate({
		tid: seller,
		pidsAdd: [],
		pidsRemove: [p.pid],
		dpidsAdd: [],
		dpidsRemove: [],
		tradingPartnerTid: winner,
		raw: true,
	}));
	const made = await pay(calc, winner, seller, [p.pid], false, toBeat - price);
	const done =
		(made && (await works(calc, made))) ||
		(fallback && (await works(calc, fallback)));
	if (!done) {
		return { teams: deal.teams, buyer, offers: rival ? 2 : 1 };
	}
	return { teams: done.teams, buyer: winner, offers: rival ? 2 : 1 };
};

const roomFor = async (tid: number, giving: number, getting: number) => {
	const now = (await idb.cache.players.indexGetAll("playersByTid", tid)).length;
	return Math.max(g.get("maxRosterSize"), now) - (now - giving + getting);
};

// THE PAYER'S BEST OFFER: of its players and picks, the package (payWith at
// most, no more players than the payee has room for) worth most to the
// payee that still leaves the payer ahead - not the first few by value for
// money, which spends the slots on scraps and leaves a team that would pay
// a lot offering little. What each asset is worth to each side adds up very
// nearly (vanilla sums its assets), so the search runs on those and checks
// its choices against the real values, best first.
const packagesFor = async (
	calc: ValueChangeCalculator,
	payer: number,
	payee: number,
	pids: number[],
) => {
	const s = vplusMarketSettings;
	const teams: TradeTeams = [
		{ ...empty(payer) },
		{ ...empty(payee), pids: [...pids] },
	];
	// What getting them for nothing does for the payer: its budget.
	const budget =
		pids.length === 1
			? await getsFrom(calc, payer, payee, pids[0])
			: await dvFor(calc, teams, 0);
	if (budget <= 0) {
		return;
	}
	const maxPlayers = await roomFor(payee, pids.length, 0);
	const options = (await cachedOptions(calc, payer, payee))
		.filter((o) => o.gives < budget && !pids.includes(o.pid ?? -1))
		.sort((a, b) => b.gets - a.gets)
		.slice(0, s.packageFrom);
	const found: { at: number[]; gets: number }[] = [];
	const search = (
		from: number,
		at: number[],
		gets: number,
		gives: number,
		players: number,
	) => {
		if (at.length > 0) {
			found.push({ at: [...at], gets });
		}
		if (at.length >= s.payWith) {
			return;
		}
		for (let i = from; i < options.length; i++) {
			const o = options[i]!;
			const isPlayer = o.pid !== undefined ? 1 : 0;
			if (gives + o.gives >= budget || players + isPlayer > maxPlayers) {
				continue;
			}
			at.push(i);
			search(i + 1, at, gets + o.gets, gives + o.gives, players + isPlayer);
			at.pop();
		}
	};
	search(0, [], 0, 0, 0);
	found.sort((a, b) => b.gets - a.gets);
	return { teams, options, found };
};

// The best few packages by the sums, each checked against the real values;
// one that's past what the payer would give drops its smallest pieces until
// it isn't (the sums miss a little, mostly on the small pieces). Only those
// worth more than `over` by the sums.
const firstThatWorks = async (
	calc: ValueChangeCalculator,
	{
		teams,
		options,
		found,
	}: NonNullable<Awaited<ReturnType<typeof packagesFor>>>,
	over = -Infinity,
) => {
	const s = vplusMarketSettings;
	let best: { teams: TradeTeams; gets: number } | undefined;
	const checked = new Set<string>();
	for (const candidate of found.slice(0, s.packageChecks)) {
		const at = [...candidate.at].sort(
			(a, b) => options[b]!.gets - options[a]!.gets,
		);
		while (at.length > 0) {
			const gets = at.reduce((sum, i) => sum + options[i]!.gets, 0);
			const key = [...at].sort((a, b) => a - b).join(",");
			if (gets <= over || checked.has(key) || (best && gets <= best.gets)) {
				break;
			}
			checked.add(key);
			const next = helpers.deepCopy(teams);
			for (const i of at) {
				const o = options[i]!;
				if (o.pid !== undefined) {
					next[0].pids.push(o.pid);
				} else {
					next[0].dpids.push(o.dpid!);
				}
			}
			if ((await dvFor(calc, next, 0)) > 0) {
				best = { teams: next, gets };
				break;
			}
			at.pop();
		}
	}
	return best;
};

const bestPackage = async (
	calc: ValueChangeCalculator,
	payer: number,
	payee: number,
	pids: number[],
) => {
	const search = await packagesFor(calc, payer, payee, pids);
	return search && (await firstThatWorks(calc, search))?.teams;
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
	// What the payee must come out ahead by (an auction's price to beat).
	need = 0,
): Promise<TradeTeams | undefined> => {
	const s = vplusMarketSettings;
	if (bid && vanillaPlusSettings.calendar) {
		const best = await bestPackage(calc, payer, payee, pids);
		return best && (await dvFor(calc, best, 1)) > 0 ? best : undefined;
	}
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
		if (payeeDv > need && !bid) {
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
	if (payeeDv <= need) {
		return;
	}
	// Anything the payee would still say yes without, the payer keeps.
	if (!bid) {
		for (const pid of teams[0].pids) {
			const next = helpers.deepCopy(teams);
			next[0].pids = next[0].pids.filter((x) => x !== pid);
			if (
				next[0].pids.length + next[0].dpids.length > 0 &&
				(await dvFor(calc, next, 1)) > need
			) {
				teams[0] = next[0];
			}
		}
		for (const dpid of teams[0].dpids) {
			const next = helpers.deepCopy(teams);
			next[0].dpids = next[0].dpids.filter((x) => x !== dpid);
			if (
				next[0].pids.length + next[0].dpids.length > 0 &&
				(await dvFor(calc, next, 1)) > need
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
	if (vanillaPlusSettings.auctions && vanillaPlusSettings.calendar) {
		const sold = await auction(calc, tweaks, lanes, best, others);
		await execute(
			calc,
			sold.teams,
			sold.buyer,
			"buyer",
			best.pid,
			sold.offers,
			tweaks(
				sold.buyer,
				playerAsset(players.find((p) => p.pid === best.pid)!),
				"add",
			),
		);
		return true;
	}
	await execute(calc, best.teams, buyer, "buyer", best.pid, 1, best.fit);
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
	await execute(calc, best.teams, seller, "seller", vet.pid, offers, best.fit);
	return true;
};

// Set each day: the last days before the deadline.
let deadlineRush = false;

const canTrade = (tid: number) => {
	const { day, traded } = clock();
	const last = traded[tid];
	const s = vplusMarketSettings;
	const days = !vanillaPlusSettings.patience
		? s.coolDownDays
		: deadlineRush
			? s.deadlineCoolDownDays
			: s.patientCoolDownDays;
	return last === undefined || day - last >= days;
};

// What a deal does for one side, as a share of what it gives: how far past
// the day's patience it was.
const gainOf = async (
	calc: ValueChangeCalculator,
	teams: TradeTeams,
	k: 0 | 1,
) => {
	const dv = await calc.evaluate({
		tid: teams[k].tid,
		pidsAdd: teams[1 - k]!.pids,
		pidsRemove: teams[k].pids,
		dpidsAdd: teams[1 - k]!.dpids,
		dpidsRemove: teams[k].dpids,
		tradingPartnerTid: teams[1 - k]!.tid,
		raw: true,
	});
	const gives = -(await calc.evaluate({
		tid: teams[k].tid,
		pidsAdd: [],
		pidsRemove: teams[k].pids,
		dpidsAdd: [],
		dpidsRemove: teams[k].dpids,
		tradingPartnerTid: teams[1 - k]!.tid,
		raw: true,
	}));
	return gives > 0 ? dv / gives : 99;
};

const execute = async (
	calc: ValueChangeCalculator,
	teams: TradeTeams,
	initiator: number,
	role: "buyer" | "seller",
	pid: number,
	offers: number,
	fit: number,
) => {
	const gain = Math.min(
		await gainOf(calc, teams, 0),
		await gainOf(calc, teams, 1),
	);
	await processTrade(teams, undefined);
	const m = clock();
	marketStats.trades += 1;
	marketStats.tradeDays.push({
		season: g.get("season"),
		phase: g.get("phase"),
		day: m.day,
		gain,
	});
	await setGameAttributes({
		vplusMarket: {
			...m,
			traded: {
				...m.traded,
				[teams[0].tid]: m.day,
				[teams[1].tid]: m.day,
			},
		},
	});
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

// Who shops today, in order. Stock vanilla+: a few teams a day, more in the
// last week before the deadline. With patience: every team, by a chance that
// rises to the deadline.
const shoppersToday = async (
	tids: number[],
	lanes: Map<number, Lane>,
	freeAgency: boolean,
) => {
	const s = vplusMarketSettings;
	const activity = (tid: number) => s.activity[lanes.get(tid) ?? "playoff"];
	const pool = tids.filter(canTrade);
	const out: number[] = [];
	if (vanillaPlusSettings.patience && !freeAgency) {
		const { toDeadline } = await calendar();
		const rate =
			s.shopEarly + (s.shopDeadline - s.shopEarly) * toDeadline ** s.shopCurve;
		const total = tids.reduce((sum, tid) => sum + activity(tid), 0);
		for (const tid of pool) {
			if (Math.random() < (rate * tids.length * activity(tid)) / total) {
				out.push(tid);
			}
		}
		for (let i = out.length - 1; i > 0; i--) {
			const j = Math.floor(Math.random() * (i + 1));
			[out[i], out[j]] = [out[j]!, out[i]!];
		}
		return out;
	}
	const shoppers = freeAgency
		? s.freeAgencyShoppers
		: (await inDeadlineDays())
			? s.deadlineShoppers
			: s.shoppersPerDay;
	for (let i = 0; i < shoppers && pool.length > 0; i++) {
		const tid = weighted(pool, activity)!;
		pool.splice(pool.indexOf(tid), 1);
		out.push(tid);
	}
	return out;
};

// A new day for the market: its count, and the cooldowns, by season.
const nextDay = async () => {
	const m = clock();
	await setGameAttributes({ vplusMarket: { ...m, day: m.day + 1 } });
};

// These teams shop, in turn.
const shop = async (
	shoppers: (tids: number[], lanes: Map<number, Lane>) => Promise<number[]>,
) => {
	const started = performance.now();
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
	for (const tid of await shoppers(tids, lanes)) {
		// A trade earlier today may have used it up.
		if (!canTrade(tid)) {
			continue;
		}
		marketStats.looks += 1;
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
	marketStats.ms += performance.now() - started;
};

// One day of the market.
export const tradeDay = async () => {
	const phase = g.get("phase");
	await nextDay();
	if (phase !== PHASE.REGULAR_SEASON && phase !== PHASE.FREE_AGENCY) {
		return;
	}
	if (phase === PHASE.REGULAR_SEASON) {
		marketStats.deadlineDay[g.get("season")] = clock().day;
	}
	deadlineRush = phase === PHASE.REGULAR_SEASON && (await inDeadlineDays());
	await shop((tids, lanes) =>
		shoppersToday(tids, lanes, phase === PHASE.FREE_AGENCY),
	);
};

// The offseason's trading windows, as these phases open: draft night, after
// the draft, the re-signing period and the preseason, once ratings have
// moved. Every CPU team gets a look, in a random order (CPU teams made 0-3
// trades an offseason, all in free agency, against the NBA's 28-39).
const WINDOW_PHASES: readonly number[] = [
	PHASE.DRAFT,
	PHASE.AFTER_DRAFT,
	PHASE.RESIGN_PLAYERS,
	PHASE.PRESEASON,
];

export const tradeWindow = async () => {
	if (!vplusMarketSettings.windows || !WINDOW_PHASES.includes(g.get("phase"))) {
		return;
	}
	await nextDay();
	deadlineRush = false;
	await shop(async (tids) => {
		const out = tids.filter(canTrade);
		for (let i = out.length - 1; i > 0; i--) {
			const j = Math.floor(Math.random() * (i + 1));
			[out[i], out[j]] = [out[j]!, out[i]!];
		}
		return out;
	});
};
