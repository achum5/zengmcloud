import { PHASE, PLAYER } from "../../../common/constants.ts";
import { makeSeededRandom } from "../../../common/random.ts";
import type { Player, PlayerContract } from "../../../common/types.ts";
import { idb } from "../../db/index.ts";
import { g, helpers } from "../../util/index.ts";
import moodInfo from "../player/moodInfo.ts";
import { revertBefore } from "../player/revertSnapshot.ts";
import { player, team } from "../index.ts";
import { contractOptions, mayPay } from "./contracts.ts";
import {
	aiTids,
	currentOutlook,
	loadTeam,
	marginOf,
	type TeamState,
} from "./market.ts";
import {
	ensureRetention,
	franchiseValue,
	teamStrengths,
	type LeagueOutlook,
} from "./outlook.ts";

// SIGNING PLAYERS. Whom a CPU team re-signs and which free agents it signs,
// for how long, by the same valuation it trades with (owner's call), on the
// terms a human gets: the player's own asking price for each length, mood
// included, and the same cap rules. A signing has to clear the same margin of
// error as a trade - picking the best of many free agents favours the ones
// the model overrates.

// Free agents a team looks at each day, best first, and how many of those it
// prices at every length rather than only the one the player wants.
const CANDIDATES = 20;
const SHORTLIST = 3;

// For the arena, to measure what a behaviour is worth by turning it off.
export const freeAgencySettings = {
	// Weigh signing today against waiting for his price to drop (timing below).
	valueWaiting: true,
	// A free agent no team will pay comes down toward what one would.
	marketPrices: true,
};

// The chance a free agent is still there tomorrow, by his place among the
// free agents, best first. Measured in arena free agencies (ARENA_FA_SURVEY).
const STILL_THERE: readonly (readonly [number, number])[] = [
	[0, 0.97],
	[5, 0.96],
	[10, 0.95],
	[15, 0.91],
	[25, 0.97],
	[30, 1],
];

const stillThere = (rank: number) => {
	let chance = STILL_THERE[0]![1];
	for (const [from, value] of STILL_THERE) {
		if (rank >= from) {
			chance = value;
		}
	}
	return chance;
};

// How much a free agent's ask falls by tomorrow anyway, as
// freeAgents/decreaseDemands lowers it every day.
const dailyDrop = () => {
	const base = 50 * Math.sqrt(g.get("maxContract") / 20000);
	const factor =
		g.get("phase") !== PHASE.FREE_AGENCY ? 82 / g.get("numGames") : 1;
	return Math.max(base * factor, base);
};

// Called each day of free agency with the pool as it stands, best first, and
// whom each team signed - for the arena, to measure how fast free agents go.
export type FreeAgencyDayLog = {
	pool: number[];
	signed: { pid: number; tid: number }[];
	// Asks brought down toward what a team would pay.
	repriced: { pid: number; from: number; to: number }[];
	// How each team saw the best free agents: willing to sign with it, within
	// its means at the length he wants, and the gain less the margin of error.
	looks: {
		tid: number;
		pid: number;
		willing: boolean;
		affordable: boolean;
		ask: number;
		net?: number;
	}[];
};

export const freeAgencyWatchers: ((day: FreeAgencyDayLog) => void)[] = [];

// Free agents a team's look at is recorded for the watchers.
const LOOKS_RECORDED = 10;

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
	const value = franchiseValue(
		outlook,
		teamStrengths(outlook, t.tid, [...t.players, signed], t.picks),
	);
	const gain = value - t.value;
	const margin = marginOf(outlook, t, [], [{ kind: "player", p: signed }]);
	return { gain, margin, net: gain - margin };
};

// The most a team would pay him at this length and still gain by more than
// the margin of error, if anything.
const mostItWouldPay = (
	outlook: LeagueOutlook,
	t: TeamState,
	p: Player,
	ask: number,
	exp: number,
) => {
	const pays = (amount: number) =>
		mayPay({
			tid: t.tid,
			payroll: t.payroll,
			amount,
			resigning: false,
			rosterSize: t.players.length,
		}) && priceSigning(outlook, t, p, { amount, exp }).net > 0;
	let lo = g.get("minContract");
	if (!pays(lo)) {
		return undefined;
	}
	let hi = ask;
	if (pays(hi)) {
		return hi;
	}
	for (let i = 0; i < 8; i++) {
		const mid = (lo + hi) / 2;
		if (pays(mid)) {
			lo = mid;
		} else {
			hi = mid;
		}
	}
	return lo;
};

// The length worth the most to the team, of those it may pay, if any clears
// the margin of error.
const bestOffer = (
	outlook: LeagueOutlook,
	t: TeamState,
	p: Player,
	ask: number,
	payroll: number,
	resigning: boolean,
) => {
	let best: { contract: PlayerContract; net: number } | undefined;
	for (const contract of contractOptions(p, ask)) {
		if (
			!mayPay({
				tid: t.tid,
				payroll,
				amount: contract.amount,
				resigning,
				rosterSize: t.players.length,
			})
		) {
			continue;
		}
		const { net } = priceSigning(outlook, t, p, contract);
		if (net > 0 && (!best || net > best.net)) {
			best = { contract, net };
		}
	}
	return best;
};

// Whether a CPU team re-signs its own player whose contract is up, and on what
// terms - undefined to let him go. `payroll` is the team's without him.
export const reSignOffer = async (p: Player, payroll: number) => {
	const mood = await moodInfo(p, p.tid);
	if (!mood.willing) {
		return undefined;
	}
	const outlook = await currentOutlook();
	await ensureRetention(outlook, [p], p.tid);
	const t = without(outlook, await loadTeam(outlook, p.tid), p.pid);
	return bestOffer(outlook, t, p, mood.contractAmount, payroll, true)?.contract;
};

const sign = async (p: Player, tid: number, contract: PlayerContract) => {
	// Him as he was before signing, so reverting the signing hands back the
	// free agent he was.
	const before = revertBefore(p);
	await player.sign(p, tid, contract, g.get("phase"), before);
	await idb.cache.players.put(p);
	await team.rosterAutoSort(tid);
};

// A day of free agency, or of the season: each CPU team, in an order drawn
// for the day, signs the free agent worth the most to it, if any clears the
// margin of error - one a day, as on the free agency board.
export const freeAgencyDay = async () => {
	const phase = g.get("phase");
	const inSeason =
		phase === PHASE.REGULAR_SEASON || phase === PHASE.AFTER_TRADE_DEADLINE;
	const pool = (
		await idb.cache.players.indexGetAll("playersByTid", PLAYER.FREE_AGENT)
	).sort((a, b) => b.valueNoPot - a.valueNoPot || a.pid - b.pid);
	if (pool.length === 0) {
		return;
	}
	const order = await aiTids();
	const random = makeSeededRandom(
		g.get("season") * 7_919 + phase * 101 + g.get("daysLeft"),
	);
	for (let i = order.length - 1; i > 0; i--) {
		const j = Math.floor(random() * (i + 1));
		[order[i], order[j]] = [order[j]!, order[i]!];
	}

	const outlook = await currentOutlook();
	const taken = new Set<number>();
	const signed: { pid: number; tid: number }[] = [];
	// Free agents some team would pay today, and the teams each would sign
	// with.
	const wanted = new Set<number>();
	const willingWith = new Map<number, number[]>();
	const looks: FreeAgencyDayLog["looks"] = [];
	const watched = new Set(
		freeAgencyWatchers.length > 0
			? pool.slice(0, LOOKS_RECORDED).map((p) => p.pid)
			: [],
	);
	for (const tid of order) {
		const t = await loadTeam(outlook, tid);
		// Once games are played, only with a place on the roster.
		if (inSeason && t.players.length >= g.get("maxRosterSize")) {
			continue;
		}
		const candidates = pool
			.filter((p) => !taken.has(p.pid))
			.slice(0, CANDIDATES);
		if (candidates.length === 0) {
			break;
		}
		await ensureRetention(outlook, candidates, tid);

		// Each at the length he wants - the cheapest he offers.
		const first: { p: Player; ask: number; net: number; rank: number }[] = [];
		for (const p of candidates) {
			const mood = await moodInfo(p, tid);
			if (mood.willing) {
				willingWith.set(p.pid, [...(willingWith.get(p.pid) ?? []), tid]);
			}
			const options = contractOptions(p, mood.contractAmount);
			const length =
				options.find((c) => c.exp === p.contract.exp) ?? options[0]!;
			const affordable = mayPay({
				tid,
				payroll: t.payroll,
				amount: length.amount,
				resigning: false,
				rosterSize: t.players.length,
			});
			if (!mood.willing || !affordable) {
				if (watched.has(p.pid)) {
					looks.push({
						tid,
						pid: p.pid,
						willing: mood.willing,
						affordable,
						ask: length.amount,
					});
				}
				continue;
			}
			const { net } = priceSigning(outlook, t, p, length);
			if (net > 0) {
				wanted.add(p.pid);
			}
			if (watched.has(p.pid)) {
				looks.push({
					tid,
					pid: p.pid,
					willing: true,
					affordable: true,
					ask: length.amount,
					net,
				});
			}
			first.push({ p, ask: mood.contractAmount, net, rank: pool.indexOf(p) });
		}

		// Then the best few at every length.
		let best:
			| { p: Player; contract: PlayerContract; net: number; rank: number }
			| undefined;
		for (const { p, ask, rank } of first
			.sort((a, b) => b.net - a.net)
			.slice(0, SHORTLIST)) {
			const offer = bestOffer(outlook, t, p, ask, t.payroll, false);
			if (offer && (!best || offer.net > best.net)) {
				best = { p, rank, ...offer };
			}
		}
		if (!best) {
			continue;
		}

		// Tomorrow his ask will be a little lower, if he is still there (owner's
		// call: teams value waiting).
		if (
			freeAgencySettings.valueWaiting &&
			phase === PHASE.FREE_AGENCY &&
			g.get("daysLeft") > 1
		) {
			const tomorrow = priceSigning(outlook, t, best.p, {
				amount: Math.max(
					g.get("minContract"),
					best.contract.amount - dailyDrop(),
				),
				exp: best.contract.exp,
			}).net;
			if (stillThere(best.rank) * tomorrow > best.net) {
				continue;
			}
		}
		await sign(best.p, tid, best.contract);
		taken.add(best.p.pid);
		signed.push({ pid: best.p.pid, tid });
	}

	// A free agent no team would pay today comes halfway down toward the most
	// one would - the same for every team, human or CPU (owner's call: prices
	// follow the market) - or to the minimum if none would pay even that.
	const repriced: FreeAgencyDayLog["repriced"] = [];
	if (freeAgencySettings.marketPrices) {
		for (const p of pool.slice(0, CANDIDATES)) {
			if (taken.has(p.pid) || wanted.has(p.pid)) {
				continue;
			}
			let target = g.get("minContract");
			let ratio = 0;
			for (const tid of willingWith.get(p.pid) ?? []) {
				const t = await loadTeam(outlook, tid);
				const mood = await moodInfo(p, tid);
				const most = mostItWouldPay(
					outlook,
					t,
					p,
					mood.contractAmount,
					p.contract.exp,
				);
				if (most !== undefined && most / mood.contractAmount > ratio) {
					ratio = most / mood.contractAmount;
					target = p.contract.amount * ratio;
				}
			}
			const from = p.contract.amount;
			const to = Math.max(
				g.get("minContract"),
				helpers.roundContract((from + target) / 2),
			);
			if (to < from) {
				p.contract.amount = to;
				await idb.cache.players.put(p);
				repriced.push({ pid: p.pid, from, to });
			}
		}
	}

	for (const watch of freeAgencyWatchers) {
		watch({ pool: pool.map((p) => p.pid), signed, looks, repriced });
	}
};
