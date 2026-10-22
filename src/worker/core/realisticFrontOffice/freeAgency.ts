import { PHASE, PLAYER } from "../../../common/constants.ts";
import type {
	FaDayResultItem,
	Player,
	PlayerContract,
	Team,
} from "../../../common/types.ts";
import { idb } from "../../db/index.ts";
import { g, helpers } from "../../util/index.ts";
import { uniformSeed } from "../../../common/random.ts";
import { revertBefore } from "../player/revertSnapshot.ts";
import { contractNegotiation, player, team } from "../index.ts";
import { mayPay } from "./contracts.ts";
import { marketSalary, setFreeAgentPoolLeft } from "./payroll.ts";
import { REPLACEMENT_OVR } from "./strength.ts";
import {
	aiTids,
	currentOutlook,
	loadTeam,
	marginOf,
	marketSettings,
	type TeamState,
} from "./market.ts";
import {
	appealOf,
	askFor,
	choiceOdds,
	marketUtility,
	offerSettings,
	offerUtility,
	offerYears,
	wantedYears,
	waitUtility,
	type Offer,
} from "./offers.ts";
import {
	ensureRetention,
	franchiseValue,
	placesAfter,
	teamStrengths,
	type LeagueOutlook,
} from "./outlook.ts";

// SIGNING PLAYERS, in an offer market (owner's call): every team, human or
// CPU, offers what money and years it likes, blind to the others, ranking its
// offers; each day a player with offers chooses among them, or waits, with
// odds (offers.ts). Each day his agent names a price - about the
// second-highest amount any team would pay him, as an auction clears - so it
// falls as teams spend their room. A CPU team bids by the same valuation it
// trades with: what signing him on those terms adds to its title chances,
// less the margin of error, times its chance of landing him.

// Free agents every team prices each day, best first, and how many of those a
// team prices at every length rather than only the one he wants.
const CANDIDATES = 20;
const SHORTLIST = 4;

// With nobody after him, his price heads for this share of itself (halfway
// there each day: about 7% a day).
const NOBODY_AFTER = 0.85;

// For the arena, to measure what a behaviour is worth by turning it off.
export const freeAgencySettings = {
	// Each day his price moves this share of the way to the market's.
	priceSpeed: 0.5,
};

// Called each day with what happened - for the arena.
export type FreeAgencyDayLog = {
	pool: number[];
	// His price before and after the day's move toward the market.
	priced: { pid: number; from: number; to: number; teams: number }[];
	offers: { pid: number; tid: number; amount: number; exp: number }[];
	decisions: {
		pid: number;
		offers: number;
		// Chance he signed today, and with whom he did (undefined: waited).
		signToday: number;
		tid?: number;
		amount?: number;
		exp?: number;
		// Whether the offer he took had the most money a year.
		topMoney?: boolean;
	}[];
};

export const freeAgencyWatchers: ((day: FreeAgencyDayLog) => void)[] = [];

const without = (
	outlook: LeagueOutlook,
	t: TeamState,
	pid: number,
): TeamState => {
	const players = t.players.filter((p) => p.pid !== pid);
	const path = teamStrengths(outlook, t.tid, players, t.picks);
	return {
		...t,
		players,
		tradablePlayers: t.tradablePlayers.filter((p) => p.pid !== pid),
		path,
		value: franchiseValue(outlook, path),
	};
};

// What signing him on these terms is worth to a team, in titles, and the
// margin of error on that: the team with him against the team without.
const priceSigning = (
	outlook: LeagueOutlook,
	t: TeamState,
	p: Player,
	contract: PlayerContract,
) => {
	const signed: Player = {
		...p,
		tid: t.tid,
		contract: { ...p.contract, amount: contract.amount, exp: contract.exp },
	};
	const players = [...t.players, signed];
	// His team's own picks land where he leaves it, as in a trade.
	const view = marketSettings.placesAfter
		? placesAfter(outlook, [{ tid: t.tid, before: t.players, after: players }])
		: outlook;
	const value = franchiseValue(
		outlook,
		teamStrengths(view, t.tid, players, t.picks),
	);
	const gain = value - t.value;
	const margin = marginOf(outlook, t, [], [{ kind: "player", p: signed }]);
	return { gain, margin, net: gain - margin };
};

const firstSeason = () =>
	g.get("season") + (g.get("phase") <= PHASE.AFTER_TRADE_DEADLINE ? 0 : 1);

const expFor = (years: number) => firstSeason() + years - 1;

const lengths = () => {
	const out: number[] = [];
	for (
		let years = g.get("minContractLength");
		years <= g.get("maxContractLength");
		years++
	) {
		out.push(years);
	}
	return out;
};

// What signing him for this many years is worth to a team, by the money a
// year: worth `atMin` at the minimum, less `slope` for each dollar more. Two
// prices are enough - money moves the value of a signing in a straight line,
// through the cap room it uses.
type Line = { years: number; atMin: number; slope: number; margin: number };

const lineFor = (
	outlook: LeagueOutlook,
	t: TeamState,
	p: Player,
	years: number,
): Line => {
	const minContract = g.get("minContract");
	const exp = expFor(years);
	const high = helpers.bound(
		marketSalary(p.value),
		2 * minContract,
		g.get("maxContract"),
	);
	const low = priceSigning(outlook, t, p, { amount: minContract, exp });
	const atHigh = priceSigning(outlook, t, p, { amount: high, exp }).net;
	return {
		years,
		atMin: low.net,
		slope: Math.max(1e-9, (low.net - atHigh) / (high - minContract)),
		margin: low.margin,
	};
};

// The lengths every free agent is first looked at for: the one he wants, and
// a single season - a deal a declining veteran is worth while a longer one is
// not.
const firstLengths = (p: Player) => {
	const wanted = wantedYears(p);
	const one = Math.max(1, g.get("minContractLength"));
	return wanted === one ? [wanted] : [wanted, one];
};

const netAt = (line: Line, amount: number) =>
	line.atMin - line.slope * (amount - g.get("minContract"));

// The most a team would pay a year and still gain by more than the margin of
// error, if anything.
const mostFrom = (line: Line) =>
	line.atMin > 0 ? g.get("minContract") + line.atMin / line.slope : undefined;

// The most a team may pay him under the cap rules, if anything.
const mostAllowed = (t: TeamState, resigning: boolean) => {
	const maxContract = g.get("maxContract");
	const rosterSize = t.players.length;
	const may = (amount: number) =>
		mayPay({ tid: t.tid, payroll: t.payroll, amount, resigning, rosterSize });
	if (may(maxContract)) {
		return maxContract;
	}
	if (!may(g.get("minContract"))) {
		return undefined;
	}
	let lo = g.get("minContract");
	let hi = maxContract;
	for (let i = 0; i < 12; i++) {
		const mid = (lo + hi) / 2;
		if (may(mid)) {
			lo = mid;
		} else {
			hi = mid;
		}
	}
	return lo;
};

// The best offer for a team to make him at this length: the money that
// makes the most of its chance of landing him times what he is worth to it
// at that price, against `rivals` (the weight of every other choice he has,
// waiting included, as offers.ts weighs them).
const bestBid = (
	p: Player,
	line: Line,
	appeal: number,
	rivals: number,
	cap: number,
) => {
	const minContract = g.get("minContract");
	const most = Math.min(cap, mostFrom(line) ?? 0);
	if (most < minContract) {
		return undefined;
	}
	const ask = askFor(p, line.years);
	let best: { amount: number; chance: number; ev: number } | undefined;
	const STEPS = 24;
	for (let i = 0; i <= STEPS; i++) {
		const amount = helpers.roundContract(
			minContract + ((most - minContract) * i) / STEPS,
		);
		const net = netAt(line, amount);
		if (net <= 0 || amount > most) {
			continue;
		}
		const weight = Math.exp(
			offerUtility({ amount, ask, appeal }) / offerSettings.temperature,
		);
		const chance = weight / (weight + rivals);
		// The margin of error is a bar to clear, not a cost: of the offers that
		// clear it, the one expected to help most, given his odds of taking it.
		const ev = chance * (net + line.margin);
		if (!best || ev > best.ev) {
			best = { amount, chance, ev };
		}
	}
	return best;
};

const sign = async (p: Player, tid: number, contract: PlayerContract) => {
	// Him as he was before signing, so reverting the signing hands back the
	// free agent he was.
	const before = revertBefore(p);
	await player.sign(p, tid, contract, g.get("phase"), before);
	await idb.cache.players.put(p);
	await team.rosterAutoSort(tid);
};

// Where a player of his value would usually play: his place in an average
// team's rotation, from his rank among every player on a roster.
const leagueRanks = async () => {
	const values = (
		await idb.cache.players.indexGetAll("playersByTid", [0, Infinity])
	)
		.map((p) => p.valueNoPot)
		.sort((a, b) => b - a);
	const numTeams = Math.max(1, g.get("numActiveTeams"));
	return (p: Player) => {
		let lo = 0;
		let hi = values.length;
		while (lo < hi) {
			const mid = (lo + hi) >> 1;
			if (values[mid]! > p.valueNoPot) {
				lo = mid + 1;
			} else {
				hi = mid;
			}
		}
		return lo / numTeams;
	};
};

// Every team's offers, best first: humans' as they left them, CPU teams' as
// they made them yesterday.
const boardOf = (t: Team) => t.offerBoard ?? [];

const setBoard = async (t: Team, offers: Offer[]) => {
	t.offerBoard = offers.map(({ pid, amount, exp }) => ({ pid, amount, exp }));
	await idb.cache.teams.put(t);
};

// Whether a team is re-signing him: his contract ran out with it, and the
// re-signing phase is on.
export const reSigningWith = async (tid: number, pid: number) =>
	g.get("phase") === PHASE.RESIGN_PLAYERS &&
	(await idb.cache.negotiations.getAll()).some(
		(n) => n.tid === tid && n.pid === pid && n.resigning,
	);

// Whether free agents can be offered contracts now: not from the playoffs to
// the end of the re-signing phase, as in stock.
export const offersOpen = () => {
	const phase = g.get("phase");
	return (
		phase >= 0 &&
		(phase <= PHASE.AFTER_TRADE_DEADLINE || phase >= PHASE.FREE_AGENCY)
	);
};

// A human team's offer, checked as it is made: a free agent (or its own
// player, while re-signing), a contract the league allows, and within the cap
// rules today. Ranked last on its board unless it replaces one already there.
export const makeOffer = async (
	tid: number,
	offer: { pid: number; amount: number; exp: number },
) => {
	const t = await idb.cache.teams.get(tid);
	const p = await idb.cache.players.get(offer.pid);
	if (!t || !p || p.tid !== PLAYER.FREE_AGENT) {
		return "He is not a free agent.";
	}
	const reSigning = await reSigningWith(tid, p.pid);
	if (!reSigning && !offersOpen()) {
		return "Free agents can't be offered contracts now.";
	}
	const years = offerYears(offer.exp);
	if (
		years < g.get("minContractLength") ||
		years > g.get("maxContractLength")
	) {
		return "That is not a contract length the league allows.";
	}
	const payroll = await team.getPayroll(tid);
	const rosterSize = (await idb.cache.players.indexGetAll("playersByTid", tid))
		.length;
	const resigning = reSigning;
	if (
		offer.amount < g.get("minContract") ||
		offer.amount > g.get("maxContract")
	) {
		return "That is not a contract the league allows.";
	}
	if (!mayPay({ tid, payroll, amount: offer.amount, resigning, rosterSize })) {
		return "Your cap room does not allow that offer.";
	}
	const board = [...(t.offerBoard ?? [])];
	const i = board.findIndex((x) => x.pid === offer.pid);
	if (i >= 0) {
		board[i] = offer;
	} else {
		board.push(offer);
	}
	t.offerBoard = board;
	await idb.cache.teams.put(t);
};

export const withdrawOffer = async (tid: number, pid: number) => {
	const t = await idb.cache.teams.get(tid);
	if (t?.offerBoard) {
		t.offerBoard = t.offerBoard.filter((x) => x.pid !== pid);
		await idb.cache.teams.put(t);
	}
};

export const rankOffers = async (tid: number, pids: number[]) => {
	const t = await idb.cache.teams.get(tid);
	if (t?.offerBoard) {
		const rank = new Map(pids.map((pid, i) => [pid, i]));
		t.offerBoard = [...t.offerBoard].sort(
			(a, b) => (rank.get(a.pid) ?? Infinity) - (rank.get(b.pid) ?? Infinity),
		);
		await idb.cache.teams.put(t);
	}
};

// What an offer is worth to him, and the weight of every other choice he has
// today - the other teams' offers standing and waiting - as offers.ts weighs
// them.
const appealCache = new Map<string, number>();
const appealFor = async (
	p: Player,
	tid: number,
	rank: number,
	rosters: Map<number, Player[]>,
) => {
	const key = `${g.get("season")}:${g.get("phase")}:${g.get("daysLeft")}:${p.pid}:${tid}:${rosters.get(tid)?.length ?? 0}`;
	let appeal = appealCache.get(key);
	if (appeal === undefined) {
		if (appealCache.size > 50_000) {
			appealCache.clear();
		}
		appeal = await appealOf(p, tid, rosters.get(tid) ?? [], rank);
		appealCache.set(key, appeal);
	}
	return appeal;
};

const utilityOf = async (
	p: Player,
	offer: Offer,
	rank: number,
	rosters: Map<number, Player[]>,
) =>
	offerUtility({
		amount: offer.amount,
		ask: askFor(p, offer.exp - firstSeason() + 1),
		appeal: await appealFor(p, offer.tid, rank, rosters),
	});

// The chance an offer would be chosen if he decided today, against the
// offers he has and waiting - what the offer screen shows.
export const offerChance = async (
	tid: number,
	offer: { pid: number; amount: number; exp: number },
) => {
	const p = await idb.cache.players.get(offer.pid);
	if (!p || p.tid !== PLAYER.FREE_AGENT) {
		return 0;
	}
	const rankOf = await leagueRanks();
	const rosters = await rostersByTid();
	const others: number[] = [];
	for (const t of await idb.cache.teams.getAll()) {
		if (t.tid === tid || t.disabled) {
			continue;
		}
		const theirs = boardOf(t).find((x) => x.pid === p.pid);
		if (theirs) {
			others.push(
				await utilityOf(p, { ...theirs, tid: t.tid }, rankOf(p), rosters),
			);
		}
	}
	const mine = await utilityOf(p, { ...offer, tid }, rankOf(p), rosters);
	return choiceOdds([mine, ...others], waitUtility())[0]!;
};

const rostersByTid = async () => {
	const rosters = new Map<number, Player[]>();
	for (const p of await idb.cache.players.indexGetAll("playersByTid", [
		0,
		Infinity,
	])) {
		const list = rosters.get(p.tid) ?? [];
		list.push(p);
		rosters.set(p.tid, list);
	}
	return rosters;
};

const lineCache: { outlook?: LeagueOutlook; lines: Map<string, Line> } = {
	lines: new Map(),
};

let poolAtOpen: { season: number; quality: number } | undefined;

// A day of free agency, or of the season.
export const freeAgencyDay = async () => {
	const phase = g.get("phase");
	const inSeason =
		phase === PHASE.REGULAR_SEASON || phase === PHASE.AFTER_TRADE_DEADLINE;
	const minContract = g.get("minContract");
	const maxContract = g.get("maxContract");
	const pool = (
		await idb.cache.players.indexGetAll("playersByTid", PLAYER.FREE_AGENT)
	).sort((a, b) => b.valueNoPot - a.valueNoPot || a.pid - b.pid);
	const teams = (await idb.cache.teams.getAll()).filter((t) => !t.disabled);
	const poolPids = new Set(pool.map((p) => p.pid));
	// Offers for players no longer free agents are gone.
	for (const t of teams) {
		if (t.offerBoard?.some((x) => !poolPids.has(x.pid))) {
			t.offerBoard = t.offerBoard.filter((x) => poolPids.has(x.pid));
			await idb.cache.teams.put(t);
		}
	}
	if (pool.length === 0) {
		return;
	}

	if (phase === PHASE.FREE_AGENCY) {
		// The quality left in the pool, against the quality as free agency
		// opened: players above the minimum's level, by how far.
		const quality = pool.reduce(
			(a, p) => a + Math.max(0, p.valueNoPot - REPLACEMENT_OVR),
			0,
		);
		if (poolAtOpen?.season !== g.get("season")) {
			poolAtOpen = { season: g.get("season"), quality };
		}
		setFreeAgentPoolLeft(
			poolAtOpen.quality > 0 ? Math.min(1, quality / poolAtOpen.quality) : 0,
		);
	}
	const outlook = await currentOutlook();
	const cpu = new Set(await aiTids());
	const rankOf = await leagueRanks();
	let rosters = await rostersByTid();
	const states = new Map<number, TeamState>();
	for (const t of teams) {
		states.set(t.tid, await loadTeam(outlook, t.tid));
	}
	const candidates = pool.slice(0, CANDIDATES);
	for (const t of teams) {
		await ensureRetention(outlook, candidates, t.tid);
	}

	// Teams that may sign anyone today: once games are played, only with a
	// place on the roster. And the most each may pay under the cap rules.
	const signing = teams.filter(
		(t) =>
			!inSeason || states.get(t.tid)!.players.length < g.get("maxRosterSize"),
	);
	const allowed = new Map(
		signing.map((t) => [t.tid, mostAllowed(states.get(t.tid)!, false)]),
	);

	// What each team would pay each of the best free agents, by length -
	// remembered while its roster and payroll stand.
	if (lineCache.outlook !== outlook) {
		lineCache.outlook = outlook;
		lineCache.lines.clear();
	}
	const rosterKey = new Map(
		teams.map((t) => {
			const state = states.get(t.tid)!;
			return [
				t.tid,
				`${state.payroll}:${state.players
					.map((p) => p.pid)
					.sort((a, b) => a - b)
					.join(",")}`,
			];
		}),
	);
	// During free agency, what room buys shrinks as the pool empties
	// (payroll.ts), so lines last a day.
	const day = phase === PHASE.FREE_AGENCY ? g.get("daysLeft") : -1;
	const lineOf = (tid: number, p: Player, years: number) => {
		const key = `${day}:${tid}:${p.pid}:${years}:${p.value}:${rosterKey.get(tid)}`;
		let line = lineCache.lines.get(key);
		if (!line) {
			if (lineCache.lines.size > 100_000) {
				lineCache.lines.clear();
			}
			line = lineFor(outlook, states.get(tid)!, p, years);
			lineCache.lines.set(key, line);
		}
		return line;
	};

	// His price moves toward the market's, as an auction clears: the
	// second-highest amount a team after him would pay, as an offer from a team
	// he feels nothing about. A team is after the few players it would gain
	// most from at today's prices, as many as it has places to fill, within
	// what the cap rules let it pay. With one team after him, halfway between
	// the minimum and its most; with none, down - toward the most any team
	// would pay, or the minimum - by enough that someone will be.
	const worth = new Map<number, Map<number, number>>();
	for (const p of candidates) {
		const byTeam = new Map<number, number>();
		for (const t of signing) {
			// At the length he wants, or for a season - priced as his ask for
			// the length he wants.
			let line: number | undefined;
			for (const years of firstLengths(p)) {
				const most = mostFrom(lineOf(t.tid, p, years));
				if (most !== undefined) {
					const asWanted =
						(most * askFor(p, wantedYears(p))) / askFor(p, years);
					line = Math.max(line ?? -Infinity, asWanted);
				}
			}
			const most =
				line === undefined
					? undefined
					: Math.min(line, maxContract, allowed.get(t.tid) ?? -Infinity);
			if (most !== undefined && most >= minContract) {
				const appeal = await appealFor(p, t.tid, rankOf(p), rosters);
				byTeam.set(t.tid, most * Math.exp(offerSettings.moodWeight * appeal));
			}
		}
		worth.set(p.pid, byTeam);
	}
	const after = new Map<number, number[]>();
	for (const t of signing) {
		const places = Math.max(
			1,
			Math.min(3, g.get("maxRosterSize") - states.get(t.tid)!.players.length),
		);
		const targets = candidates
			.filter((p) => worth.get(p.pid)!.has(t.tid))
			.map((p) => ({
				pid: p.pid,
				surplus: worth.get(p.pid)!.get(t.tid)! - p.contract.amount,
			}))
			.sort((a, b) => b.surplus - a.surplus)
			.slice(0, places);
		for (const { pid } of targets) {
			after.set(pid, [...(after.get(pid) ?? []), worth.get(pid)!.get(t.tid)!]);
		}
	}
	const priced: FreeAgencyDayLog["priced"] = [];
	for (const p of candidates) {
		const bids = (after.get(p.pid) ?? []).sort((a, b) => b - a);
		const anyone = Math.max(minContract, ...worth.get(p.pid)!.values());
		const market =
			bids.length >= 2
				? bids[1]!
				: bids.length === 1
					? (minContract + bids[0]!) / 2
					: Math.min(p.contract.amount * NOBODY_AFTER, anyone);
		const from = p.contract.amount;
		const to = helpers.bound(
			helpers.roundContract(
				from + freeAgencySettings.priceSpeed * (market - from),
			),
			minContract,
			maxContract,
		);
		if (to !== from) {
			p.contract.amount = to;
			await idb.cache.players.put(p);
		}
		priced.push({ pid: p.pid, from, to, teams: bids.length });
	}

	// Every offer standing, by player: humans' and yesterday's CPU teams'.
	const standing = new Map<number, Offer[]>();
	const addStanding = (offer: Offer) => {
		const list = standing.get(offer.pid) ?? [];
		list.push(offer);
		standing.set(offer.pid, list);
	};
	for (const t of teams) {
		if (!cpu.has(t.tid)) {
			for (const offer of boardOf(t)) {
				addStanding({ ...offer, tid: t.tid });
			}
		}
	}

	// CPU teams make their offers, in an order drawn for the day, each knowing
	// its odds against the offers made before it.
	const order = teams.filter((t) => cpu.has(t.tid));
	for (let i = order.length - 1; i > 0; i--) {
		const j = Math.floor(Math.random() * (i + 1));
		[order[i], order[j]] = [order[j]!, order[i]!];
	}
	const waitWeight = Math.exp(waitUtility() / offerSettings.temperature);
	for (const t of order) {
		const state = states.get(t.tid)!;
		const spots = g.get("maxRosterSize") - state.players.length;
		const cap = allowed.get(t.tid);
		if (cap === undefined) {
			await setBoard(t, []);
			continue;
		}
		const rivalsOf = async (p: Player) => {
			let weight = waitWeight;
			for (const offer of standing.get(p.pid) ?? []) {
				if (offer.tid !== t.tid) {
					weight += Math.exp(
						(await utilityOf(p, offer, rankOf(p), rosters)) /
							offerSettings.temperature,
					);
				}
			}
			return weight;
		};
		const bids: (Offer & { ev: number })[] = [];
		const first: { p: Player; ev: number }[] = [];
		for (const p of candidates) {
			const appeal = await appealFor(p, t.tid, rankOf(p), rosters);
			const rivals = await rivalsOf(p);
			let ev: number | undefined;
			for (const years of firstLengths(p)) {
				const bid = bestBid(p, lineOf(t.tid, p, years), appeal, rivals, cap);
				if (bid && (ev === undefined || bid.ev > ev)) {
					ev = bid.ev;
				}
			}
			if (ev !== undefined) {
				first.push({ p, ev });
			}
		}
		for (const { p } of first.sort((a, b) => b.ev - a.ev).slice(0, SHORTLIST)) {
			const appeal = await appealFor(p, t.tid, rankOf(p), rosters);
			const rivals = await rivalsOf(p);
			let best: (Offer & { ev: number }) | undefined;
			for (const years of lengths()) {
				const bid = bestBid(p, lineOf(t.tid, p, years), appeal, rivals, cap);
				if (bid && (!best || bid.ev > best.ev)) {
					best = {
						tid: t.tid,
						pid: p.pid,
						amount: bid.amount,
						exp: expFor(years),
						ev: bid.ev,
					};
				}
			}
			if (best) {
				bids.push(best);
			}
		}
		const board = bids
			.sort((a, b) => b.ev - a.ev)
			// One more than it has places, so the next best are in play if a
			// first choice holds out.
			.slice(0, Math.max(2, spots + 1))
			.map(({ ev: _ev, ...offer }) => offer);
		for (const offer of board) {
			addStanding(offer);
		}
		await setBoard(t, board);
	}

	// Each player with offers chooses one, or waits. A team two of whose
	// targets say yes takes them in the order it ranked them; an offer that no
	// longer fits falls through, and he chooses again.
	const decisions: FreeAgencyDayLog["decisions"] = [];
	const results: FaDayResultItem[] = [];
	const remaining = new Map<number, Offer[]>(
		[...standing].map(([pid, offers]) => [pid, [...offers]]),
	);
	const priority = (offer: Offer) =>
		boardOf(teams.find((t) => t.tid === offer.tid)!).findIndex(
			(x) => x.pid === offer.pid,
		);
	let queue = pool.filter((p) => remaining.has(p.pid));
	const chosen = new Map<number, Offer>();
	const choose = async (p: Player) => {
		const offers = remaining.get(p.pid) ?? [];
		if (offers.length === 0) {
			return;
		}
		const utilities: number[] = [];
		for (const offer of offers) {
			utilities.push(await utilityOf(p, offer, rankOf(p), rosters));
		}
		const odds = choiceOdds(utilities, waitUtility());
		const roll = Math.random();
		let k = 0;
		let acc = odds[0]!;
		while (roll > acc && k < odds.length - 1) {
			k += 1;
			acc += odds[k]!;
		}
		const decision: FreeAgencyDayLog["decisions"][number] = {
			pid: p.pid,
			offers: offers.length,
			signToday: 1 - odds.at(-1)!,
		};
		if (k < offers.length) {
			const offer = offers[k]!;
			chosen.set(p.pid, offer);
			decision.tid = offer.tid;
			decision.amount = offer.amount;
			decision.exp = offer.exp;
			decision.topMoney = offers.every((x) => x.amount <= offer.amount);
			results.push({
				type: "choice",
				pid: p.pid,
				name: `${p.firstName} ${p.lastName}`,
				offers: offers.map((x, i) => ({
					tid: x.tid,
					abbrev: g.get("teamInfoCache")[x.tid]?.abbrev ?? `${x.tid}`,
					amount: x.amount,
					exp: x.exp,
					oddsPct: Math.round(odds[i]! * 1000) / 10,
				})),
				waitPct: Math.round(odds.at(-1)! * 1000) / 10,
				roll: Math.round(roll * 1000) / 10,
				winnerTid: offer.tid,
				amount: offer.amount,
				exp: offer.exp,
			});
		}
		decisions.push(decision);
	};
	for (const p of queue) {
		await choose(p);
	}
	while (chosen.size > 0) {
		queue = [];
		const byTeam = new Map<number, Offer[]>();
		for (const offer of chosen.values()) {
			const list = byTeam.get(offer.tid) ?? [];
			list.push(offer);
			byTeam.set(offer.tid, list);
		}
		chosen.clear();
		for (const [tid, offers] of byTeam) {
			for (const offer of offers.sort((a, b) => priority(a) - priority(b))) {
				const p = pool.find((x) => x.pid === offer.pid)!;
				const roster = await idb.cache.players.indexGetAll("playersByTid", tid);
				const fits =
					mayPay({
						tid,
						payroll: await team.getPayroll(tid),
						amount: offer.amount,
						resigning: false,
						rosterSize: roster.length,
					}) &&
					(!inSeason || roster.length < g.get("maxRosterSize"));
				if (fits) {
					await sign(p, tid, { amount: offer.amount, exp: offer.exp });
					remaining.delete(p.pid);
				} else {
					remaining.set(
						p.pid,
						(remaining.get(p.pid) ?? []).filter((x) => x.tid !== tid),
					);
					const i = results.findLastIndex(
						(x) => x.type === "choice" && x.pid === p.pid,
					);
					if (i >= 0) {
						results.splice(i, 1);
					}
					queue.push(p);
				}
			}
		}
		if (queue.length > 0) {
			rosters = await rostersByTid();
		}
		for (const p of queue) {
			await choose(p);
		}
	}

	// Signed players' offers are gone from every board.
	for (const t of teams) {
		if (t.offerBoard?.some((x) => !remaining.has(x.pid))) {
			t.offerBoard = t.offerBoard.filter((x) => remaining.has(x.pid));
			await idb.cache.teams.put(t);
		}
	}
	if (results.length > 0) {
		const season = g.get("season");
		const daysLeft = g.get("daysLeft");
		await idb.cache.faDayResults.put({
			key: `${season}-${phase}-${daysLeft}`,
			season,
			daysLeft,
			items: results,
			boards: [],
			at: Date.now(),
		});
	}

	for (const watch of freeAgencyWatchers) {
		watch({
			pool: pool.map((p) => p.pid),
			priced,
			offers: [...standing.values()].flat(),
			decisions,
		});
	}
};

// Whether a CPU team re-signs its own player whose contract is up, and on
// what terms - undefined to let him go. The team makes the offer worth the
// most to it, given his chance of taking it; he takes it or tests the market,
// with odds (owner's call: re-signing stays its own phase, on the same odds).
// `payroll` is the team's without him.
export const reSignDecision = async (p: Player, payroll: number) => {
	const outlook = await currentOutlook();
	await ensureRetention(outlook, [p], p.tid);
	const t = {
		...without(outlook, await loadTeam(outlook, p.tid), p.pid),
		payroll,
	};
	const rankOf = await leagueRanks();
	const rosters = await rostersByTid();
	const market = await marketUtility(p, p.tid);
	const appeal = await appealFor(p, p.tid, rankOf(p), rosters);
	const cap = mostAllowed(t, true);
	if (cap === undefined) {
		return undefined;
	}
	const rivals = Math.exp(market / offerSettings.temperature);
	let best:
		| { contract: PlayerContract; chance: number; ev: number }
		| undefined;
	for (const years of lengths()) {
		const bid = bestBid(p, lineFor(outlook, t, p, years), appeal, rivals, cap);
		if (bid && (!best || bid.ev > best.ev)) {
			best = {
				contract: { amount: bid.amount, exp: expFor(years) },
				chance: bid.chance,
				ev: bid.ev,
			};
		}
	}
	if (best && Math.random() < best.chance) {
		return best.contract;
	}
	return undefined;
};

// His chance of taking his own team's offer rather than testing the market -
// for a human team's re-signing offer, decided as free agency opens, and for
// the offer screen.
export const reSignChance = async (
	p: Player,
	tid: number,
	offer: { amount: number; exp: number },
) => {
	const rankOf = await leagueRanks();
	const rosters = await rostersByTid();
	const market = await marketUtility(p, tid);
	const mine = await utilityOf(
		{ ...p, tid } as Player,
		{ ...offer, tid, pid: p.pid },
		rankOf(p),
		rosters,
	);
	return choiceOdds([mine], market)[0]!;
};

// A human team's re-signing offers, decided now: each of its players whose
// contract ran out takes its offer or tests the market, on a roll fixed for
// him for the season - so deciding again changes nothing. Offers not decided
// by the end of the phase are decided as free agency opens.
export const decideReSignings = async (tid: number) => {
	const decided: {
		pid: number;
		name: string;
		chance: number;
		signed: boolean;
		amount: number;
		exp: number;
	}[] = [];
	const t = await idb.cache.teams.get(tid);
	if (!t?.offerBoard || g.get("phase") !== PHASE.RESIGN_PLAYERS) {
		return decided;
	}
	for (const offer of t.offerBoard) {
		if (!(await reSigningWith(tid, offer.pid))) {
			continue;
		}
		const p = await idb.cache.players.get(offer.pid);
		if (!p || p.tid !== PLAYER.FREE_AGENT) {
			continue;
		}
		const roster = await idb.cache.players.indexGetAll("playersByTid", tid);
		const fits = mayPay({
			tid,
			payroll: await team.getPayroll(tid),
			amount: offer.amount,
			resigning: true,
			rosterSize: roster.length,
		});
		const chance = fits ? await reSignChance(p, tid, offer) : 0;
		const signed = uniformSeed(p.pid * 7_919 + g.get("season")) < chance;
		if (signed) {
			const contract: PlayerContract = {
				amount: offer.amount,
				exp: offer.exp,
			};
			if (p.contract.rookie) {
				contract.rookie = true;
			}
			await player.sign(p, tid, contract, PHASE.RESIGN_PLAYERS);
			await idb.cache.players.put(p);
		}
		await contractNegotiation.cancel(p.pid);
		t.offerBoard = t.offerBoard.filter((x) => x.pid !== p.pid);
		decided.push({
			pid: p.pid,
			name: `${p.firstName} ${p.lastName}`,
			chance,
			signed,
			amount: offer.amount,
			exp: offer.exp,
		});
	}
	await idb.cache.teams.put(t);
	return decided;
};

// As the re-signing phase opens, a human team's players whose contracts ran
// out start with an offer at their asking price for the length they want.
export const defaultReSignOffer = async (tid: number, p: Player) => {
	const t = await idb.cache.teams.get(tid);
	if (!t) {
		return;
	}
	t.offerBoard = [
		...(t.offerBoard ?? []).filter((x) => x.pid !== p.pid),
		{ pid: p.pid, amount: p.contract.amount, exp: p.contract.exp },
	];
	await idb.cache.teams.put(t);
};
