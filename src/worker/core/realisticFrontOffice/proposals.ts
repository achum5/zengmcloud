import { PHASE } from "../../../common/constants.ts";
import type {
	MinimalPlayerRatings,
	Player,
	TradeTeams,
} from "../../../common/types.ts";
import { idb } from "../../db/index.ts";
import { g, helpers, logEvent } from "../../util/index.ts";
import { getFuzz, applyFuzz } from "../player/fuzzRating.ts";
import summary from "../trade/summary.ts";
import {
	assetsFor,
	humanTradeSettings,
	judgeHumanOffer,
	load,
	type Loaded,
	type Side,
} from "./humanTrades.ts";
import {
	aiTids,
	assetKey,
	count,
	currentOutlook,
	loadTeam,
	marginOf,
	MIN_GAIN,
	packages,
	priceFor,
	salariesWork,
	tableAssets,
	type Asset,
	type TeamState,
} from "./market.ts";
import {
	ensureRetention,
	franchiseValue,
	teamStrengths,
	type LeagueOutlook,
	type SeasonLines,
} from "./outlook.ts";
import { IMPACT_RATINGS } from "./playerImpact.ts";
import { proposalKey, wasDeclined } from "./proposalStore.ts";
import { ValueChangeCalculator } from "../team/ValueChangeCalculator.ts";

// CPU TEAMS' OFFERS TO HUMANS (owner's calls): "What would make this work"
// and Trade Proposals (the trading block is below). A CPU team offers the deal
// best for it by its own read - its gain over the bar a human's deal must clear
// (humanTrades.ts), so it takes the deal if it is offered back - that still
// looks fair on what the human can see: his side gains by the ratings he
// sees, scouting fuzz and all, through the same projections and title odds.
// Its offers never give away its private read of a player.

// Deals priced in full for each pair of teams, after a screen of many.
const CANDIDATES = 12;
// Asked for a price, the deals nearest what the CPU team needs priced in full.
const PRICE_CANDIDATES = 30;

// THE TRADING BLOCK (owner's call, replacing one that its offers had to look
// fair to the human): where a human sees what the market would pay for what
// he puts up. Each team bids up to three of its assets, one at least of the
// kinds he asks for, salaries left out (he makes the money work after).
// - How big: a pick barely moves a title contender's odds, so by its own read
//   nearly every bid in picks it would make at all was its three best, for
//   anyone, and the bids all came out alike. So the market sets the size: the
//   biggest bid the stock trade AI's values call fair for it (they price
//   players against picks the way the game always has), made only if the
//   team would still take the deal by its own read (this many priced in
//   full at most) and, before the deadline, would not rather wait.
// - Not shopped: whether its assets would fetch more elsewhere is weighed
//   when the human offers the deal (shopping every bid took the block from a
//   second or two to most of a minute).
// - Made of the assets it would give for his alone by those values, the most
//   valuable first, so many of each kind (players of the kinds asked for,
//   other players, picks) - a player only if the human would keep him, not
//   cut him - and worth this share of the price at least (a lowball is no
//   guide to the market).
const BID_WANTED = 3;
const BID_OTHERS = 1;
const BID_PICKS = 4;
const BID_CANDIDATES = 8;
const MIN_SHARE = 1 / 3;

// A player as the human sees him: every rating with his scouting fuzz on it.
const publicRow = <R extends MinimalPlayerRatings>(r: R): R => {
	const fuzz = getFuzz(r.fuzz);
	if (fuzz === 0) {
		return r;
	}
	const out: Record<string, unknown> = { ...r, fuzz: 0 };
	for (const key of IMPACT_RATINGS) {
		out[key] = applyFuzz((r as any)[key], fuzz);
	}
	out.ovr = applyFuzz(r.ovr, fuzz);
	out.pot = applyFuzz(r.pot, fuzz);
	return out as R;
};

export const publicPlayer = (p: Player): Player => {
	if (getFuzz(p.ratings.at(-1)!.fuzz) === 0) {
		return p;
	}
	return {
		...p,
		ratings: p.ratings.map(publicRow) as Player["ratings"],
		value: p.valueFuzz,
		valueNoPot: p.valueNoPotFuzz,
	};
};

const publicAssets = (assets: readonly Asset[]): Asset[] =>
	assets.map((x) =>
		x.kind === "player" ? { kind: "player", p: publicPlayer(x.p) } : x,
	);

// The league as it stands, with players' lines read off what the human sees,
// kept apart from the true ones.
const publicOutlooks = new WeakMap<LeagueOutlook, LeagueOutlook>();
const publicOutlookOf = (outlook: LeagueOutlook) => {
	let out = publicOutlooks.get(outlook);
	if (!out) {
		out = {
			...outlook,
			lines: new Map() as SeasonLines,
			placeBase: new Map(),
		};
		publicOutlooks.set(outlook, out);
	}
	return out;
};

// A human team as it sees itself.
export const publicTeam = (outlook: LeagueOutlook, t: TeamState): TeamState => {
	const pub = publicOutlookOf(outlook);
	const players = t.players.map(publicPlayer);
	const path = teamStrengths(pub, t.tid, players, t.picks);
	return {
		...t,
		players,
		tradablePlayers: t.tradablePlayers.map(publicPlayer),
		path,
		value: franchiseValue(pub, path),
	};
};

// What a deal does for a human team by its own read: `h` from publicTeam.
export const publicGain = (
	outlook: LeagueOutlook,
	h: TeamState,
	outgoing: readonly Asset[],
	incoming: readonly Asset[],
	partner?: TeamState,
) =>
	priceFor(
		publicOutlookOf(outlook),
		h,
		publicAssets(outgoing),
		publicAssets(incoming),
		partner,
	);

export type Proposal = {
	// What the CPU team gives and what it gets.
	give: Asset[];
	get: Asset[];
	// Its gain over the bar a human's deal must clear, in titles.
	net: number;
};

const sideOf = (
	tid: number,
	assets: readonly Asset[],
): { tid: number; pids: number[]; dpids: number[] } => ({
	tid,
	pids: assets.flatMap((x) => (x.kind === "player" ? [x.p.pid] : [])),
	dpids: assets.flatMap((x) => (x.kind === "pick" ? [x.dp.dpid] : [])),
});

const keyOf = (assets: readonly Asset[]) => assets.map(assetKey).sort().join();

// The deal best for `cpu` with `human` that the human would see as fair: it
// gives `keep.give` and gets `keep.get` plus up to two more of each side's
// assets.
export const bestProposal = async (
	outlook: LeagueOutlook,
	cpu: TeamState,
	human: TeamState,
	{
		keep = { give: [], get: [] },
		price = false,
	}: {
		keep?: { give: Asset[]; get: Asset[] };
		// The CPU team's price for what the human asked for: of the deals it
		// would take, the one it gains least from - no more than it needs, good
		// for the human or not, as he decides. Otherwise the deal best for the
		// CPU team that is fair on what the human sees.
		price?: boolean;
	} = {},
): Promise<Proposal | undefined> => {
	const factor = humanTradeSettings.marginFactor;
	// How each side's players would feel about re-signing with the other.
	await ensureRetention(outlook, human.tradablePlayers, cpu.tid);
	await ensureRetention(outlook, cpu.tradablePlayers, human.tid);
	const pub = publicTeam(outlook, human);
	const kept = new Set([...keep.give, ...keep.get].map(assetKey));
	const cpuAssets = tableAssets(cpu).filter((x) => !kept.has(assetKey(x)));
	const humanAssets = tableAssets(human).filter((x) => !kept.has(assetKey(x)));

	// One asset at a time: what it is worth to the CPU team by its read, and
	// to the human by his. The screen adds these up.
	const cpuSingle = new Map<string, number>();
	const humanSingle = new Map<string, number>();
	for (const x of [...cpuAssets, ...keep.give]) {
		cpuSingle.set(assetKey(x), priceFor(outlook, cpu, [x], []));
		humanSingle.set(assetKey(x), publicGain(outlook, pub, [], [x]));
	}
	for (const y of [...humanAssets, ...keep.get]) {
		cpuSingle.set(assetKey(y), priceFor(outlook, cpu, [], [y]));
		humanSingle.set(assetKey(y), publicGain(outlook, pub, [y], []));
	}
	const sum = (map: Map<string, number>, assets: readonly Asset[]) =>
		assets.reduce((s, x) => s + map.get(assetKey(x))!, 0);

	const candidates: { give: Asset[]; get: Asset[]; screen: number }[] = [];
	// Asked his price, the CPU team adds at most one of its own, as salaries
	// may need.
	const extraGive = price
		? [[], ...cpuAssets.map((x) => [x])]
		: packages(cpuAssets, (x) => -cpuSingle.get(assetKey(x))!);
	// And a deal has to clear its margin of error on the screen too.
	const bar = price
		? factor * marginOf(outlook, cpu, keep.give, keep.get) + MIN_GAIN
		: 0;
	// Pairs and threes of the human's assets: for a price, those worth most to
	// the CPU team over what they cost the human.
	const extraGet = packages(humanAssets, (x) =>
		price
			? cpuSingle.get(assetKey(x))! + humanSingle.get(assetKey(x))!
			: humanSingle.get(assetKey(x))!,
	);
	for (const pg of extraGive) {
		for (const ph of extraGet) {
			const give = [...keep.give, ...pg];
			const get = [...keep.get, ...ph];
			if (give.length === 0 || get.length === 0) {
				// A human is never asked to give something for nothing, or
				// offered something for nothing.
				continue;
			}
			const forCpu = sum(cpuSingle, give) + sum(cpuSingle, get);
			const forHuman = sum(humanSingle, give) + sum(humanSingle, get);
			if (forCpu > bar && (price || forHuman >= 0)) {
				candidates.push({ give, get, screen: price ? -forCpu : forCpu });
			}
		}
	}
	candidates.sort((x, y) => y.screen - x.screen);

	const seen = new Set<string>();
	let best: Proposal | undefined;
	let checked = 0;
	const maxRosterSize = g.get("maxRosterSize");
	const inSeason = g.get("phase") === PHASE.REGULAR_SEASON;
	for (const { give, get } of candidates) {
		if (checked >= (price ? PRICE_CANDIDATES : CANDIDATES)) {
			break;
		}
		const key = `${keyOf(give)}|${keyOf(get)}`;
		if (seen.has(key)) {
			continue;
		}
		seen.add(key);
		if (
			!salariesWork(cpu, give, get) ||
			!salariesWork(human, get, give) ||
			(inSeason &&
				(cpu.players.length + count(get) - count(give) > maxRosterSize ||
					human.players.length + count(give) - count(get) > maxRosterSize))
		) {
			continue;
		}
		checked += 1;
		const net =
			priceFor(outlook, cpu, give, get, human) -
			factor * marginOf(outlook, cpu, give, get);
		if (net < MIN_GAIN) {
			continue;
		}
		if (price) {
			if (!best || net < best.net) {
				best = { give, get, net };
			}
			continue;
		}
		if (best && net <= best.net) {
			continue;
		}
		if (publicGain(outlook, pub, get, give, cpu) < 0) {
			continue;
		}
		best = { give, get, net };
	}
	return best;
};

// The proposal, if the CPU team would take it as offered and the trade screen
// would process it: the same judge as a human's offer, and no warning - a
// bid's salaries and shopping aside (see BID_WANTED).
const confirmed = async (
	cpuTid: number,
	humanTid: number,
	proposal: Proposal,
	{ bid = false, loaded }: { bid?: boolean; loaded?: Loaded } = {},
) => {
	const cpuSide = sideOf(cpuTid, proposal.give);
	const humanSide = sideOf(humanTid, proposal.get);
	const verdict = await judgeHumanOffer(cpuSide, humanSide, {
		loaded,
		shop: !bid,
	});
	if (verdict.kind !== "yes") {
		return false;
	}
	if (bid) {
		return true;
	}
	const s = await summary([
		{
			tid: humanTid,
			pids: humanSide.pids,
			dpids: humanSide.dpids,
			pidsExcluded: [],
			dpidsExcluded: [],
		},
		{
			tid: cpuTid,
			pids: cpuSide.pids,
			dpids: cpuSide.dpids,
			pidsExcluded: [],
			dpidsExcluded: [],
		},
	]);
	return !s.warning;
};

// "What would make this work": the deal best for the CPU team that keeps
// everything in the human's offer - the offer itself, if it would take that.
export const counterOffer = async (
	outlook: LeagueOutlook,
	cpu: Side,
	human: Side,
): Promise<Proposal | undefined> => {
	const a = await loadTeam(outlook, cpu.tid);
	const h = await loadTeam(outlook, human.tid);
	const give = assetsFor(a, cpu.pids, cpu.dpids);
	const get = assetsFor(h, human.pids, human.dpids);
	if (!give || !get) {
		return undefined;
	}
	if ((await judgeHumanOffer(cpu, human)).kind === "yes") {
		return { give, get, net: 0 };
	}
	const proposal = await bestProposal(outlook, a, h, {
		keep: { give, get },
		price: true,
	});
	if (proposal && (await confirmed(cpu.tid, human.tid, proposal))) {
		return proposal;
	}
	return undefined;
};

// What a bid is worth to the team making it by the stock trade values: more
// than nothing, or it does not make it.
const marketGain = (
	classic: ValueChangeCalculator,
	cpuTid: number,
	humanTid: number,
	give: readonly Asset[],
	get: readonly Asset[],
) => {
	const gives = sideOf(cpuTid, give);
	const gets = sideOf(humanTid, get);
	return classic.evaluate({
		tid: cpuTid,
		pidsAdd: gets.pids,
		pidsRemove: gives.pids,
		dpidsAdd: gets.dpids,
		dpidsRemove: gives.dpids,
		tradingPartnerTid: humanTid,
	});
};

// A team's bid for what a human put on the block (see BID_WANTED), of its
// assets that pass `allow`, one at least that passes `wanted`: `pub` is the
// human team as it sees itself, `base` what giving `get` up for nothing does
// for it by that read.
const bestBid = async (
	outlook: LeagueOutlook,
	cpu: TeamState,
	human: TeamState,
	{
		get,
		pub,
		base,
		allow,
		wanted,
		classic,
	}: {
		get: Asset[];
		pub: TeamState;
		base: number;
		allow: (x: Asset) => boolean;
		wanted: (x: Asset) => boolean;
		classic: ValueChangeCalculator;
	},
): Promise<Proposal | undefined> => {
	// What it gets is worth this to it by the market.
	const price = await marketGain(classic, cpu.tid, human.tid, [], get);
	if (price <= 0) {
		return undefined;
	}
	// How each side's players would feel about re-signing with the other.
	await ensureRetention(outlook, human.tradablePlayers, cpu.tid);
	await ensureRetention(outlook, cpu.tradablePlayers, human.tid);
	// (The less it would gain giving one, the more that one is worth.)
	const assets: { x: Asset; dv: number }[] = [];
	for (const x of tableAssets(cpu)) {
		if (allow(x)) {
			const dv = await marketGain(classic, cpu.tid, human.tid, [x], get);
			if (dv > 0) {
				assets.push({ x, dv });
			}
		}
	}
	assets.sort((x, y) => x.dv - y.dv);
	const first = (n: number, pass: (x: Asset) => boolean) => {
		const out: Asset[] = [];
		for (const { x } of assets) {
			if (out.length >= n) {
				break;
			}
			if (
				pass(x) &&
				(x.kind === "pick" || publicGain(outlook, pub, get, [x]) > base)
			) {
				out.push(x);
			}
		}
		return out;
	};
	const top = [
		...first(BID_WANTED, (x) => x.kind === "player" && wanted(x)),
		...first(BID_OTHERS, (x) => x.kind === "player" && !wanted(x)),
		...first(BID_PICKS, (x) => x.kind === "pick"),
	];
	// Every one, two or three of those it would give, the most generous by
	// the market first.
	const combos: Asset[][] = [];
	for (let i = 0; i < top.length; i++) {
		combos.push([top[i]!]);
		for (let j = i + 1; j < top.length; j++) {
			combos.push([top[i]!, top[j]!]);
			for (let k = j + 1; k < top.length; k++) {
				combos.push([top[i]!, top[j]!, top[k]!]);
			}
		}
	}
	const sized: { give: Asset[]; dv: number }[] = [];
	for (const give of combos) {
		if (!give.some(wanted)) {
			continue;
		}
		const dv = await marketGain(classic, cpu.tid, human.tid, give, get);
		if (dv > 0 && price - dv >= MIN_SHARE * price) {
			sized.push({ give, dv });
		}
	}
	sized.sort((x, y) => x.dv - y.dv);
	const factor = humanTradeSettings.marginFactor;
	for (const { give } of sized.slice(0, BID_CANDIDATES)) {
		const net =
			priceFor(outlook, cpu, give, get, human) -
			factor * marginOf(outlook, cpu, give, get);
		if (net >= MIN_GAIN) {
			return { give, get, net };
		}
	}
	return undefined;
};

// The trading block: each CPU team's bid for these assets of the human's (see
// bestBid), if it would make it today.
export const blockOffers = async (
	outlook: LeagueOutlook,
	cpuTids: readonly number[],
	human: Side,
	{
		allow,
		wanted,
	}: {
		allow: (x: Asset) => boolean;
		wanted: (x: Asset) => boolean;
	},
) => {
	// (Nothing changes while it asks around.)
	const loaded: Loaded = { outlook, teams: new Map() };
	const h = await load(outlook, human.tid, loaded);
	const get = assetsFor(h, human.pids, human.dpids);
	if (!get || get.length === 0) {
		return [];
	}
	const pub = publicTeam(outlook, h);
	const base = publicGain(outlook, pub, get, []);
	const classic = new ValueChangeCalculator();
	const offers: { tid: number; proposal: Proposal }[] = [];
	for (const tid of cpuTids) {
		const a = await load(outlook, tid, loaded);
		const proposal = await bestBid(outlook, a, h, {
			get,
			pub,
			base,
			allow,
			wanted,
			classic,
		});
		if (
			proposal &&
			(await confirmed(tid, human.tid, proposal, { bid: true, loaded }))
		) {
			offers.push({ tid, proposal });
		}
	}
	return offers;
};

// TRADE PROPOSALS: the deal each CPU team stands ready to make with each
// human team, made when the team shops and kept while it still wants it -
// gone once it no longer does, or once it finds something better.
export const refreshProposals = async (
	outlook: LeagueOutlook,
	cpuTid: number,
	humanTids: readonly number[],
) => {
	const t = await idb.cache.teams.get(cpuTid);
	if (!t) {
		return [];
	}
	const a = await loadTeam(outlook, cpuTid);
	const before = t.tradeProposals ?? [];
	const after: NonNullable<typeof t.tradeProposals> = [];
	const fresh: number[] = [];
	for (const humanTid of humanTids) {
		const h = await loadTeam(outlook, humanTid);
		let proposal = await bestProposal(outlook, a, h);
		if (
			proposal &&
			(wasDeclined(
				t,
				humanTid,
				proposalKey({
					give: sideOf(cpuTid, proposal.give),
					get: sideOf(humanTid, proposal.get),
				}),
			) ||
				!(await confirmed(cpuTid, humanTid, proposal)))
		) {
			proposal = undefined;
		}
		const old = before.find((x) => x.tid === humanTid);
		const sameAsOld =
			old &&
			proposal &&
			keyOf(proposal.give) ===
				keyOf(assetsFor(a, old.give.pids, old.give.dpids) ?? []) &&
			keyOf(proposal.get) ===
				keyOf(assetsFor(h, old.get.pids, old.get.dpids) ?? []);
		if (proposal) {
			const give = sideOf(cpuTid, proposal.give);
			const get = sideOf(humanTid, proposal.get);
			after.push({
				tid: humanTid,
				give: { pids: give.pids, dpids: give.dpids },
				get: { pids: get.pids, dpids: get.dpids },
			});
			if (!sameAsOld) {
				fresh.push(humanTid);
			}
		}
	}
	t.tradeProposals = after.length > 0 ? after : undefined;
	await idb.cache.teams.put(t);
	return fresh;
};

// A new proposal: the human hears of it.
export const notifyProposal = async (cpuTid: number, humanTid: number) => {
	const t = g.get("teamInfoCache")[cpuTid];
	const h = g.get("teamInfoCache")[humanTid];
	if (!t || !h) {
		return;
	}
	const to = g.get("userTids").length > 1 ? `the ${h.region} ${h.name}` : "you";
	await logEvent({
		type: "info",
		text: `The <a href="${helpers.leagueUrl(["roster", t.abbrev, g.get("season")])}">${t.region} ${t.name}</a> have a <a href="${helpers.leagueUrl(["trade_proposals"])}">trade proposal</a> for ${to}.`,
		saveToDb: false,
	});
};

// "What would make this work" on the trade screen: the deal as a trade
// screen holds it, user's side first - undefined if there is none.
export const counterTeams = async (
	teams: TradeTeams,
): Promise<TradeTeams | undefined> => {
	const outlook = await currentOutlook();
	const proposal = await counterOffer(
		outlook,
		{ tid: teams[1].tid, pids: teams[1].pids, dpids: teams[1].dpids },
		{ tid: teams[0].tid, pids: teams[0].pids, dpids: teams[0].dpids },
	);
	if (!proposal) {
		return undefined;
	}
	const user = sideOf(teams[0].tid, proposal.get);
	const cpu = sideOf(teams[1].tid, proposal.give);
	return [
		{ ...teams[0], pids: user.pids, dpids: user.dpids },
		{ ...teams[1], pids: cpu.pids, dpids: cpu.dpids },
	];
};

// The trading block: each CPU team's bid for these assets of the user's, as
// trade screens hold them, user's side first. Each holds one at least of its
// players at the positions and with the skills asked for or, asked for, its
// picks - picks alone when only those are.
export const tradingBlockTeams = async (
	userTid: number,
	pids: number[],
	dpids: number[],
	lookingFor: {
		positions: Set<string>;
		skills: Set<string>;
		draftPicks: boolean;
	},
): Promise<TradeTeams[]> => {
	const outlook = await currentOutlook();
	const narrowed = lookingFor.positions.size > 0 || lookingFor.skills.size > 0;
	const allow = (x: Asset) =>
		x.kind === "pick" || narrowed || !lookingFor.draftPicks;
	const wanted = (x: Asset) => {
		if (x.kind === "pick") {
			return lookingFor.draftPicks || !narrowed;
		}
		if (!narrowed) {
			return !lookingFor.draftPicks;
		}
		const r = x.p.ratings.at(-1)!;
		return (
			(lookingFor.positions.size === 0 ||
				[...lookingFor.positions].some((pos) => r.pos.includes(pos))) &&
			(lookingFor.skills.size === 0 ||
				r.skills.some((skill) => lookingFor.skills.has(skill)))
		);
	};
	const teamInfo = g.get("teamInfoCache");
	const cpuTids = (await aiTids()).sort((a, b) =>
		`${teamInfo[a]?.region} ${teamInfo[a]?.name}`.localeCompare(
			`${teamInfo[b]?.region} ${teamInfo[b]?.name}`,
		),
	);
	const offers = await blockOffers(
		outlook,
		cpuTids,
		{ tid: userTid, pids, dpids },
		{ allow, wanted },
	);
	return offers.map(({ tid, proposal }) => {
		const cpu = sideOf(tid, proposal.give);
		return [
			{ tid: userTid, pids, dpids, pidsExcluded: [], dpidsExcluded: [] },
			{
				tid,
				pids: cpu.pids,
				dpids: cpu.dpids,
				pidsExcluded: [],
				dpidsExcluded: [],
			},
		];
	});
};
