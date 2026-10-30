import { PHASE, PLAYER } from "../../../common/constants.ts";
import { shuffle } from "../../../common/random.ts";
import type { Player, PlayerContract } from "../../../common/types.ts";
import { idb } from "../../db/index.ts";
import { g, local } from "../../util/index.ts";
import { contractOptionsFor } from "../contractNegotiation/generateContractOptions.ts";
import { player } from "../index.ts";
import { mayPay } from "./contracts.ts";
import { frontOfficeCuts } from "./cuts.ts";
import { priceMarket, priceSigning, sign, without } from "./freeAgency.ts";
import {
	aiTids,
	currentOutlook,
	loadTeam,
	MIN_GAIN,
	settingFor,
	type TeamState,
} from "./market.ts";
import { ensureRetention, type LeagueOutlook } from "./outlook.ts";
import { REPLACEMENT_OVR } from "./strength.ts";

// FREE AGENCY AND RE-SIGNING BY STOCK'S RULES (owner's call, with Realistic
// Free Agency off): a player signs where he is willing, for what he asks, as
// in stock - judging every team on the same terms, human or CPU - and a CPU
// team chooses whom to sign, and which of his asks to take, by the valuation
// it trades with: what signing him adds to its title chances, over the
// margin of error.
//
// What he asks is the market's (owner's call): stock prices players as if CPU
// teams spent all their room on whoever is best, and smart ones would not pay
// that - half the top free agents went unsigned all summer. So it is priced
// as the offer market prices it (priceMarket) - about the second-highest any
// CPU team after him would pay, moving halfway there each day of free
// agency, and all the way at once for players whose contracts are up, as
// re-signing opens - with stock's terms: only teams he is willing to sign
// with bid, each as the ask that comes to its most with his mood toward it.

// A CPU team's bid in the market, by stock's rules (see above).
const bidsBy = async () => {
	const cpu = new Set(await aiTids());
	return async (p: Player, tid: number, most: number) => {
		if (!cpu.has(tid)) {
			return undefined;
		}
		const mood = await player.moodInfo(p, tid, { contractAmount: most });
		return mood.willing ? (most * most) / mood.contractAmount : undefined;
	};
};

// Free agents a team prices each day at the length each wants, best first,
// and how many of those it prices at every length.
const CANDIDATES = 60;
const SHORTLIST = 4;

// The best of his asks for a team, by what each is worth to it - undefined if
// none is worth more than its margin of error, or the cap rules allow none.
// `ask` is what he asks of this team for the length he wants, in thousands.
const bestAsk = (
	outlook: LeagueOutlook,
	t: TeamState,
	p: Player,
	ask: number,
	resigning: boolean,
) => {
	let best: { contract: PlayerContract; net: number } | undefined;
	const options = contractOptionsFor(
		{ amount: ask / 1000, exp: p.contract.exp },
		p.ratings.at(-1)!.ovr,
	);
	for (const option of options) {
		const amount = Math.round(option.amount * 1000);
		if (
			!mayPay({
				tid: t.tid,
				payroll: t.payroll,
				amount,
				resigning,
				rosterSize: t.players.length,
			})
		) {
			continue;
		}
		const contract = { amount, exp: option.exp };
		const { net } = priceSigning(outlook, t, p, contract);
		if (net >= MIN_GAIN && (!best || net > best.net)) {
			best = { contract, net };
		}
	}
	return best;
};

// A day of free agency for the CPU teams, in place of stock's autoSign: each,
// in random order and at stock's pace, signs the free agent willing to sign
// with it who is worth the most to it. Returns the signings.
export const signingDay = async () => {
	// And what he asked of the team for the length he wanted.
	const signed: {
		pid: number;
		tid: number;
		contract: PlayerContract;
		ask: PlayerContract;
	}[] = [];
	const phase = g.get("phase");
	const inSeason =
		phase === PHASE.REGULAR_SEASON || phase === PHASE.AFTER_TRADE_DEADLINE;
	let pool = (
		await idb.cache.players.indexGetAll("playersByTid", PLAYER.FREE_AGENT)
	).sort((a, b) => b.valueNoPot - a.valueNoPot || a.pid - b.pid);
	if (pool.length === 0) {
		return signed;
	}
	const teams = (await idb.cache.teams.getAll()).filter((t) => !t.disabled);
	const { outlook, states } = await priceMarket(pool, teams, {
		bidOf: await bidsBy(),
	});

	shuffle(teams);
	for (const t of teams) {
		if (
			g.get("userTids").includes(t.tid) &&
			!local.autoPlayUntil &&
			!g.get("spectator")
		) {
			continue;
		}

		// Stock's pace.
		const probSkip = t.strategy === "rebuilding" ? 0.9 : 0.75;
		if (Math.random() < probSkip) {
			continue;
		}

		// (Nobody else's signing today changes its roster.)
		const state = states.get(t.tid)!;
		if (
			g.get("forceHistoricalRosters") &&
			state.players.length >= g.get("minRosterSize")
		) {
			continue;
		}
		// Once games are played, a full roster signs only by releasing whoever
		// it can best do without, as a signing is priced.
		const full = state.players.length >= g.get("maxRosterSize");
		if (inSeason && full && !settingFor(t.tid, "fullRosterSignings")) {
			continue;
		}

		// Those willing to sign with it, priced at the length each wants; the
		// few worth most to it, at every length.
		const candidates = pool.slice(0, CANDIDATES);
		await ensureRetention(outlook, candidates, t.tid);
		const screened: { p: Player; ask: number; net: number }[] = [];
		for (const p of candidates) {
			const mood = await player.moodInfo(p, t.tid);
			if (!mood.willing) {
				continue;
			}
			// (Any other length costs more.)
			const ask = mood.contractAmount;
			if (
				!mayPay({
					tid: t.tid,
					payroll: state.payroll,
					amount: ask,
					resigning: false,
					rosterSize: state.players.length,
				})
			) {
				continue;
			}
			screened.push({
				p,
				ask,
				net: priceSigning(outlook, state, p, {
					amount: ask,
					exp: p.contract.exp,
				}).net,
			});
		}
		screened.sort((a, b) => b.net - a.net);
		let best:
			| { p: Player; ask: number; contract: PlayerContract; net: number }
			| undefined;
		for (const { p, ask } of screened.slice(0, SHORTLIST)) {
			const found = bestAsk(outlook, state, p, ask, false);
			if (found && (!best || found.net > best.net)) {
				best = { p, ask, ...found };
			}
		}
		if (!best) {
			continue;
		}

		const ask = { amount: best.ask, exp: best.p.contract.exp };
		await sign(best.p, t.tid, best.contract);
		signed.push({
			pid: best.p.pid,
			tid: t.tid,
			contract: best.contract,
			ask,
		});
		pool = pool.filter((p) => p !== best.p);
		if (inSeason && full) {
			await frontOfficeCuts(
				await idb.cache.players.indexGetAll("playersByTid", t.tid),
				1,
			);
		}
	}
	return signed;
};

// A CPU team's own player whose contract is up, willing to re-sign for `ask`
// (thousands, for the length he wants): which of his asks it takes, if any.
// `payroll` is the team's without him.
export const reSignAsk = async (p: Player, payroll: number, ask: number) => {
	const outlook = await currentOutlook();
	await ensureRetention(outlook, [p], p.tid);
	const t = {
		...without(outlook, await loadTeam(outlook, p.tid), p.pid),
		payroll,
	};
	return bestAsk(outlook, t, p, ask, true)?.contract;
};

// As re-signing opens, what the market would pay each player whose contract
// is up, priced as a free agent - drafted players aside, who sign as in
// stock, and those no better than the minimum's level, who keep stock's ask.
export const marketAsks = async (pids: readonly number[]) => {
	const season = g.get("season");
	const players: Player[] = [];
	for (const pid of pids) {
		const p = await idb.cache.players.get(pid);
		if (p && p.draft.year !== season && p.valueNoPot > REPLACEMENT_OVR) {
			players.push(p);
		}
	}
	if (players.length === 0) {
		return;
	}
	players.sort((a, b) => b.valueNoPot - a.valueNoPot || a.pid - b.pid);
	const teams = (await idb.cache.teams.getAll()).filter((t) => !t.disabled);
	await priceMarket(players, teams, {
		once: true,
		numCandidates: players.length,
		bidOf: await bidsBy(),
	});
};
