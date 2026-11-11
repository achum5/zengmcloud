import { PHASE } from "../../../common/constants.ts";
import type { Player, TradeTeams } from "../../../common/types.ts";
import { idb } from "../../db/index.ts";
import { g, helpers, local } from "../../util/index.ts";
import { contractOf } from "../realisticFrontOffice/contractOf.ts";
import getDaysLeftSchedule from "../season/getDaysLeftSchedule.ts";
import isUntradable from "../trade/isUntradable.ts";
import makeItWork from "../trade/makeItWork.ts";
import processTrade from "../trade/processTrade.ts";
import summary from "../trade/summary.ts";
import type { ValueChangeCalculator } from "../team/ValueChangeCalculator.ts";
import { tradeValueCalculator } from "./index.ts";
import { ensureLanes, type Lane } from "./lanes.ts";
import { tweaksFor, type Tweaks } from "./tweaks.ts";
import { marketWatchers, type MarketEvent } from "./events.ts";
import { flushNews } from "./news.ts";

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
		// The seller names its price for him (it is the screen's other team).
		const made = await makeItWork(
			[empty(buyer), { ...empty(p.tid), pids: [p.pid] }],
			{
				holdUserConstant: false,
				maxAssetsToAdd: 5,
				valueChangeCalculator: calc,
			},
		);
		if (!made || !made[1].pids.includes(p.pid)) {
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
		// Each buyer's best offer for him (it is the screen's other team).
		const made = await makeItWork(
			[{ ...empty(seller), pids: [vet.pid] }, empty(tid)],
			{
				holdUserConstant: true,
				maxAssetsToAdd: 5,
				valueChangeCalculator: calc,
			},
		);
		if (!made) {
			marketStats.noDeal += 1;
			continue;
		}
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
