import type { Team, TradeTeams } from "../../../common/types.ts";
import { idb } from "../../db/index.ts";
import { g, helpers, logEvent } from "../../util/index.ts";
import { team } from "../index.ts";
import processTrade from "../trade/processTrade.ts";
import {
	assetsFor,
	bestElsewhere,
	humanTradeSettings,
	playersIn,
	type Side,
} from "./humanTrades.ts";
import { ensureRetention, type LeagueOutlook } from "./outlook.ts";
import {
	aiTids,
	currentOutlook,
	gamesPlayed,
	legalDeal,
	loadTeam,
	makeDeal,
	marginOf,
	marketClock,
	MIN_GAIN,
	priceFor,
	settingFor,
	type Asset,
	type Deal,
	type TeamState,
} from "./market.ts";

// MATCHING WINDOWS (owner's calls). A notable deal two CPU teams agree - one
// in which a team gives one of its five best players or a first-round pick -
// is held until the next trading day, and any team, human or CPU, can offer
// either side more for what it gives. When the window closes each side takes
// the best offer for its assets by its own read - a human's binding, judged as
// any human's offer - which may break the deal. Its assets are off the table
// meanwhile.

type Pending = NonNullable<Team["pendingTrade"]>;
type Give = Pending["give"][number];

// A trading day: the regular season's by games played, free agency's by days.
const dayKey = async () =>
	`${g.get("season")}:${g.get("phase")}:${await gamesPlayed()}:${g.get("daysLeft")}`;

const TOP_PLAYERS = 5;

export const isNotable = (
	deal: Pick<Deal, "assets">,
	sides: readonly [TeamState, TeamState],
	initiatorTid: number,
) => {
	const setting =
		settingFor(initiatorTid, "windows") ??
		g.get("realisticFrontOfficesWindows") ??
		"notable";
	if (setting === "off") {
		return false;
	}
	if (setting === "all") {
		return true;
	}
	for (const k of [0, 1] as const) {
		const top = new Set(
			[...sides[k].players]
				.sort((a, b) => b.valueNoPot - a.valueNoPot)
				.slice(0, TOP_PLAYERS)
				.map((p) => p.pid),
		);
		for (const x of deal.assets[k]) {
			if (
				(x.kind === "player" && top.has(x.p.pid)) ||
				(x.kind === "pick" && x.dp.round === 1)
			) {
				return true;
			}
		}
	}
	return false;
};

// The players and picks in held deals, off the table till their windows close.
const reserved = { pids: new Set<number>(), dpids: new Set<number>() };

const reserve = (give: readonly Give[]) => {
	for (const side of give) {
		for (const pid of side.pids) {
			reserved.pids.add(pid);
		}
		for (const dpid of side.dpids) {
			reserved.dpids.add(dpid);
		}
	}
};

export const loadReserved = async () => {
	reserved.pids.clear();
	reserved.dpids.clear();
	for (const t of await idb.cache.teams.getAll()) {
		if (t.pendingTrade) {
			reserve(t.pendingTrade.give);
		}
	}
};

export const isReserved = (x: Asset) =>
	x.kind === "player"
		? reserved.pids.has(x.p.pid)
		: reserved.dpids.has(x.dp.dpid);

const humanTids = async () => {
	const ai = await aiTids();
	return g.get("userTids").filter((tid) => !ai.includes(tid));
};

const teamLink = (tid: number) => {
	const t = g.get("teamInfoCache")[tid];
	return t
		? `<a href="${helpers.leagueUrl(["roster", t.abbrev, g.get("season")])}">${t.region} ${t.name}</a>`
		: "";
};

export const holdDeal = async (
	initiatorTid: number,
	deal: Deal,
	// Why each team makes it and what it wants most in a bid (dealNotes.ts).
	notes?: Pending["notes"],
) => {
	const t = await idb.cache.teams.get(initiatorTid);
	if (!t) {
		return;
	}
	t.pendingTrade = {
		tids: deal.tids,
		give: deal.give,
		day: await dayKey(),
		bids: [],
		notes,
	};
	await idb.cache.teams.put(t);
	reserve(deal.give);
	marketClock.held += 1;
	if ((await humanTids()).length > 0) {
		await logEvent({
			type: "info",
			text: `The ${teamLink(deal.tids[0])} and the ${teamLink(deal.tids[1])} have a <a href="${helpers.leagueUrl(["trade_proposals"])}">pending deal</a>.`,
			saveToDb: false,
		});
	}
};

// The held deal in which this team gives all of these assets but those not
// held anywhere, its side, and those more it is asked for.
const pendingSide = async (
	tid: number,
	pids: readonly number[],
	dpids: readonly number[],
) => {
	await loadReserved();
	for (const t of await idb.cache.teams.getAll()) {
		const pending = t.pendingTrade;
		for (const k of [0, 1] as const) {
			const give = pending?.give[k];
			if (
				pending?.tids[k] === tid &&
				give &&
				give.pids.length + give.dpids.length > 0 &&
				give.pids.every((pid) => pids.includes(pid)) &&
				give.dpids.every((dpid) => dpids.includes(dpid))
			) {
				const take = {
					pids: pids.filter((pid) => !give.pids.includes(pid)),
					dpids: dpids.filter((dpid) => !give.dpids.includes(dpid)),
				};
				if (
					take.pids.some((pid) => reserved.pids.has(pid)) ||
					take.dpids.some((dpid) => reserved.dpids.has(dpid))
				) {
					continue;
				}
				return { t, k, take };
			}
		}
	}
	return undefined;
};

// What a human's offer for a CPU team's assets in a held deal is: a bid, if
// it asks for all that team gives in it (and maybe more of its assets, not
// held - salaries may need it); refused if it asks for some of them, or for
// another held deal's too; neither if it asks for none of them.
export const offerOnHeld = async (cpu: Side) => {
	await loadReserved();
	const asked =
		cpu.pids.some((pid) => reserved.pids.has(pid)) ||
		cpu.dpids.some((dpid) => reserved.dpids.has(dpid));
	if (!asked) {
		return undefined;
	}
	return (await pendingSide(cpu.tid, cpu.pids, cpu.dpids)) ? "bid" : "held";
};

// A human's bid, binding until the window closes.
export const placeBid = async (cpu: Side, human: Side) => {
	const found = await pendingSide(cpu.tid, cpu.pids, cpu.dpids);
	if (!found) {
		return false;
	}
	found.t.pendingTrade!.bids.push({
		tid: human.tid,
		side: found.k,
		give: { pids: [...human.pids], dpids: [...human.dpids] },
		...(found.take.pids.length + found.take.dpids.length > 0
			? { take: found.take }
			: {}),
	});
	await idb.cache.teams.put(found.t);
	return true;
};

const giveOf = (assets: readonly Asset[]): Give => ({
	pids: assets.flatMap((x) => (x.kind === "player" ? [x.p.pid] : [])),
	dpids: assets.flatMap((x) => (x.kind === "pick" ? [x.dp.dpid] : [])),
});

// A side's offers for what it gives: the deal it has, or another team's - a
// human's maybe for more of its assets (`take`).
type Offer = {
	tid: number;
	give: Give;
	net: number;
	human: boolean;
	take?: Give;
	// The deal's other team, giving more than it agreed.
	raised?: boolean;
};

// What a side gives with a human's bid: the held deal's assets and any more
// the bid asks for - undefined if any has gone.
const givingFor = (a: TeamState, held: Give, take: Give | undefined) => {
	const all = {
		pids: [...held.pids, ...(take?.pids ?? [])],
		dpids: [...held.dpids, ...(take?.dpids ?? [])],
	};
	const assets = assetsFor(a, all.pids, all.dpids);
	return assets && { give: all, assets };
};

// What one side, giving `giving`, nets from `getting` from team `b`: its gain
// less its margin of error, which counts for more with a human.
const netFor = (
	outlook: LeagueOutlook,
	a: TeamState,
	giving: readonly Asset[],
	getting: readonly Asset[],
	b: TeamState,
	human: boolean,
) =>
	priceFor(outlook, a, giving, getting, b) -
	(human ? humanTradeSettings.marginFactor : 1) *
		marginOf(outlook, a, giving, getting);

const bestOffer = async (
	outlook: LeagueOutlook,
	pending: Pending,
	sides: readonly [TeamState, TeamState],
	given: readonly [Asset[], Asset[]],
	k: 0 | 1,
) => {
	const j = k === 0 ? 1 : 0;
	const a = sides[k];
	let best: Offer = {
		tid: sides[j].tid,
		give: pending.give[j],
		net: netFor(outlook, a, given[k], given[j], sides[j], false),
		human: false,
	};
	if (given[k].length === 0) {
		return best;
	}
	const better = (offer: Offer) =>
		offer.net >= MIN_GAIN && offer.net > best.net;

	const elsewhere = await bestElsewhere(outlook, a, given[k], pending.tids);
	if (elsewhere) {
		const offer = {
			tid: elsewhere.tid,
			give: giveOf(elsewhere.assets),
			net: elsewhere.net,
			human: false,
		};
		if (better(offer)) {
			best = offer;
		}
	}

	for (const bid of pending.bids) {
		if (bid.side !== k) {
			continue;
		}
		const h = await loadTeam(outlook, bid.tid);
		const getting = assetsFor(h, bid.give.pids, bid.give.dpids);
		const giving = givingFor(a, pending.give[k], bid.take);
		if (
			!getting ||
			!giving ||
			!(await legalDeal({
				tids: [a.tid, h.tid],
				give: [giving.give, bid.give],
			}))
		) {
			continue;
		}
		await ensureRetention(outlook, playersIn(getting), a.tid);
		const offer = {
			tid: h.tid,
			give: bid.give,
			net: netFor(outlook, a, giving.assets, getting, h, true),
			human: true,
			take: bid.take,
		};
		if (better(offer)) {
			best = offer;
		}
	}

	// About to lose the deal, the other team answers with the most it would
	// really give for the same - so the deal only goes to a better offer than
	// any CPU team would make, not to one nobody else had the chance to match.
	if (best.tid !== sides[j].tid) {
		const others = (await aiTids()).filter((tid) => tid !== sides[j].tid);
		const raise = await bestElsewhere(outlook, a, given[k], others, best.net);
		if (raise && raise.net > best.net) {
			best = {
				tid: sides[j].tid,
				give: giveOf(raise.assets),
				net: raise.net,
				human: false,
				raised: true,
			};
		}
	}
	return best;
};

// Makes a deal with a human's bid.
const takeBid = async (cpuTid: number, cpuGive: Give, offer: Offer) => {
	const teams: TradeTeams = [
		{
			tid: offer.tid,
			pids: offer.give.pids,
			dpids: offer.give.dpids,
			pidsExcluded: [],
			dpidsExcluded: [],
		},
		{
			tid: cpuTid,
			pids: cpuGive.pids,
			dpids: cpuGive.dpids,
			pidsExcluded: [],
			dpidsExcluded: [],
		},
	];
	await processTrade(teams, undefined);
	for (const tid of [offer.tid, cpuTid]) {
		const t = await idb.cache.teams.get(tid);
		await team.rosterAutoSort(
			tid,
			g.get("userTids").includes(tid) && !t?.keepRosterSorted,
		);
	}
};

const closeWindow = async (outlook: LeagueOutlook, pending: Pending) => {
	let sides = [
		await loadTeam(outlook, pending.tids[0]),
		await loadTeam(outlook, pending.tids[1]),
	] as const;
	const assets = [
		assetsFor(sides[0], pending.give[0].pids, pending.give[0].dpids),
		assetsFor(sides[1], pending.give[1].pids, pending.give[1].dpids),
	];
	const winners = new Set<number>();
	if (!assets[0] || !assets[1]) {
		// Something in it has gone since.
		await tellBidders(pending, winners);
		return;
	}
	const given = assets as [Asset[], Asset[]];
	for (const k of [0, 1] as const) {
		await ensureRetention(
			outlook,
			playersIn(given[k === 0 ? 1 : 0]),
			sides[k].tid,
		);
	}

	const offers = [
		await bestOffer(outlook, pending, sides, given, 0),
		await bestOffer(outlook, pending, sides, given, 1),
	] as const;
	const kept = offers.every(
		(offer, k) => offer.tid === pending.tids[k === 0 ? 1 : 0] && !offer.raised,
	);
	if (kept) {
		if (
			offers[0].net >= MIN_GAIN &&
			offers[1].net >= MIN_GAIN &&
			(await makeDeal(
				outlook,
				priced(outlook, sides, given),
				sides[0].tid,
				sides,
			))
		) {
			marketClock.heldKept += 1;
		}
		await tellBidders(pending, winners);
		return;
	}

	// A side with a better offer takes it; the deal is off.
	for (const k of [0, 1] as const) {
		const offer = offers[k];
		if (offer.tid === pending.tids[k === 0 ? 1 : 0] && !offer.raised) {
			continue;
		}
		const a = await loadTeam(outlook, pending.tids[k]);
		const b = await loadTeam(outlook, offer.tid);
		const held = givingFor(a, pending.give[k], offer.take);
		const giving = held?.assets;
		const getting = assetsFor(b, offer.give.pids, offer.give.dpids);
		if (
			!held ||
			!giving ||
			!getting ||
			!(await legalDeal({
				tids: [a.tid, b.tid],
				give: [held.give, offer.give],
			}))
		) {
			continue;
		}
		if (offer.human) {
			// Still worth it to the CPU side after anything the other side did.
			if (netFor(outlook, a, giving, getting, b, true) >= MIN_GAIN) {
				await takeBid(a.tid, held.give, offer);
				winners.add(b.tid);
				marketClock.heldTaken += 1;
			}
			continue;
		}
		sides = [a, b];
		const deal = priced(outlook, sides, [giving, getting]);
		if (
			deal.gains[0] - deal.margins[0] >= MIN_GAIN &&
			deal.gains[1] - deal.margins[1] >= MIN_GAIN
		) {
			if (await makeDeal(outlook, deal, a.tid, sides)) {
				marketClock.heldTaken += 1;
			}
		}
	}
	await tellBidders(pending, winners);
};

// A deal between two CPU teams as it stands, priced for both.
const priced = (
	outlook: LeagueOutlook,
	sides: readonly [TeamState, TeamState],
	assets: [Asset[], Asset[]],
): Deal => ({
	tids: [sides[0].tid, sides[1].tid],
	give: [giveOf(assets[0]), giveOf(assets[1])],
	assets,
	gains: [
		priceFor(outlook, sides[0], assets[0], assets[1], sides[1]),
		priceFor(outlook, sides[1], assets[1], assets[0], sides[0]),
	],
	margins: [
		marginOf(outlook, sides[0], assets[0], assets[1]),
		marginOf(outlook, sides[1], assets[1], assets[0]),
	],
});

// Humans whose bids lost hear so; a winning bid's trade tells its own story.
const tellBidders = async (pending: Pending, winners: Set<number>) => {
	const told = new Set<string>();
	for (const bid of pending.bids) {
		const key = `${bid.tid}:${bid.side}`;
		if (winners.has(bid.tid) || told.has(key)) {
			continue;
		}
		told.add(key);
		await logEvent({
			type: "info",
			text: `The ${teamLink(pending.tids[bid.side])} turned down your bid.`,
			saveToDb: false,
		});
	}
};

// Closes the windows of deals held on an earlier trading day.
export const closeWindows = async () => {
	const today = await dayKey();
	const due = (await idb.cache.teams.getAll()).filter(
		(t) => t.pendingTrade && t.pendingTrade.day !== today,
	);
	if (due.length === 0) {
		return;
	}
	const outlook = await currentOutlook();
	for (const t of due) {
		const pending = t.pendingTrade!;
		t.pendingTrade = undefined;
		await idb.cache.teams.put(t);
		await loadReserved();
		await closeWindow(outlook, pending);
	}
	await loadReserved();
};

// The held deals, for the Trade Proposals page.
export const heldDeals = async () =>
	(await idb.cache.teams.getAll()).flatMap((t) =>
		t.pendingTrade ? [t.pendingTrade] : [],
	);
