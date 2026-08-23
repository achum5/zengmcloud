import { PHASE, PLAYER } from "../../../common/constants.ts";
import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import { isAiControlled } from "../../util/isAiControlled.ts";
import { last, orderBy } from "../../../common/utils.ts";
import { player, team } from "../index.ts";
import processTrade from "../trade/processTrade.ts";
import summary from "../trade/summary.ts";
import { ValueChangeCalculator } from "../team/ValueChangeCalculator.ts";
import {
	getLeagueTradeContext,
	getTradePosture,
	type TradePosture,
} from "../trade/tradePosture.ts";
import {
	frontOfficeLog,
	frontOfficeLoggingActive,
} from "../../util/frontOfficeLog.ts";
import {
	isPrize,
	MIN_PURSUIT_CONFIDENCE,
	scoreFreeAgent,
	type FaPlayer,
} from "./frontOffice.ts";
import type { Player } from "../../../common/types.ts";

// ---------------------------------------------------------------------------
// CLEARING CAP SPACE FOR A SIGNING
//
// The move a real front office makes and this game never did: you have handshake
// agreement with a free agent, you are two million over the cap, so you attach a
// pick to your worst contract and pay someone to take it. The trade itself is a
// loss - that is the price of the cap room - but you walk away with a player you
// could not otherwise have signed, so the WHOLE transaction is a big win.
//
// Nothing in the AI could express that, because a trade was only ever judged on
// its own merits. Every dump here is judged on the net: what the team gives up
// versus the player it lands, and it is only allowed to eat the loss when the
// signing more than covers it.
//
// The other half is knowing the player would actually come. moodInfo already
// answers that per team, deterministically, so a team is never dumping salary on
// a hunch.
// ---------------------------------------------------------------------------

// How much value a team may give away in the dump, as a share of the value it
// gains from the signing. Below 1 it is still a net win; the margin keeps a
// team from paying almost the whole gain away in sweeteners.
const MAX_DUMP_COST_RATIO = 0.6;

// At most this many sweeteners (picks) may be attached. A team that needs to
// bury three first-rounders to move a contract should just not sign the guy.
const MAX_SWEETENERS = 2;

// Never dump more than this many players in one deal - it is a salary dump, not
// a roster teardown.
const MAX_DUMP_PLAYERS = 3;

// One per team per offseason. A front office clears the decks for ITS guy; a
// team doing this five times in one free agency is not planning, it is churning
// - which is exactly what a long sim showed happening before this cap existed.

// Which teams have already done it this season, read back off the league's own
// event log rather than held in memory.
//
// This has to survive a change of simmer. The device in charge can hand off
// mid-free-agency, and worker memory does not travel - so an in-memory counter
// silently resets and the next simmer lets everyone dump all over again, which
// is precisely the churn the cap exists to stop. The trade event is already
// written, already carries the motivation that identifies these deals, and
// already syncs to every device, so the limit is derived from the same history
// every device can see. The events cache holds the current season, which is
// exactly the window the cap covers.
const teamsThatAlreadyCleared = async (): Promise<Set<number>> => {
	const out = new Set<number>();
	for (const event of await idb.cache.events.getAll()) {
		const aiTrade = (event as { aiTrade?: { motivation?: string } }).aiTrade;
		if (
			event.type === "trade" &&
			aiTrade?.motivation === CAP_CLEAR_MOTIVATION &&
			event.season === g.get("season")
		) {
			for (const tid of event.tids ?? []) {
				out.add(tid);
			}
		}
	}
	return out;
};

// Stamped on the trade so the deal is identifiable afterwards - by the cap above,
// and by anyone reading the transaction log wondering why a team gave a player
// away for nothing.
const CAP_CLEAR_MOTIVATION = "cap-clear";

const toFaPlayer = (p: Player, season: number): FaPlayer => {
	const ratings = last(p.ratings);
	return {
		pid: p.pid,
		ovr: ratings.ovr,
		pot: ratings.pot,
		value: p.value,
		age: season - p.born.year,
		pos: ratings.pos,
		amount: p.contract.amount,
		exp: p.contract.exp,
		injuredGames: p.injury.gamesRemaining,
	};
};

// How badly a contract is worth shedding: salary per unit of value. A cheap good
// player scores low (keep him); an expensive bad one scores high (move him).
// This is the pure ranking behind "your worst contract", exported for testing.
export const dumpPriority = (p: {
	contractAmount: number;
	value: number;
}): number => p.contractAmount / Math.max(1, p.value);

// May this contract be moved to make room for the target?
//
// Deliberately NOT the trade AI's building-block list. That list protects every
// quality player, which is right when a player is being given away for assets
// and wrong here: he is being CONVERTED into a better player, and a real front
// office will absolutely move a rotation piece to land a star. What it will not
// do is any of these:
//   - move someone better than the man it is signing (that is a downgrade);
//   - move a genuine star for cap relief;
//   - dump a young cornerstone, whose whole value is that he is still coming;
//   - bother with a minimum contract, which frees nothing.
export const isDumpable = ({
	ovr,
	pot,
	age,
	contractAmount,
	targetOvr,
	starOvr,
	minContract,
}: {
	ovr: number;
	pot: number;
	age: number;
	contractAmount: number;
	targetOvr: number;
	starOvr: number;
	minContract: number;
}): boolean => {
	if (contractAmount <= minContract) {
		return false;
	}
	if (ovr >= targetOvr || ovr >= starOvr) {
		return false;
	}
	if (age <= 23 && pot >= starOvr) {
		return false;
	}
	return true;
};

// WHAT THE PARTNER HAS TO SEND BACK.
//
// A one-way dump needs somebody with enough room under the cap to swallow the
// whole contract, and in a league running at ninety-odd percent of the cap
// there usually is not one - "no partner with room" was the single commonest
// reason a cap-clearing plan died. But taking a big contract and sending a
// small one the other way is the ordinary shape of this trade in real
// basketball, and it is legal for a team that would otherwise be over: the
// salary it sheds is what buys the room to absorb.
//
// So: the least salary that keeps the partner legal, and then the cheapest
// contract it actually has that covers it - cheapest, because every dollar
// coming back is a dollar of room the dumping team does not get. Value breaks
// ties, so the partner parts with its most expendable man rather than a
// rotation piece it would refuse over anyway.
export const planSalaryBack = <
	T extends { pid: number; contractAmount: number; value: number },
>({
	candidates,
	partnerPayroll,
	incomingSalary,
	salaryCap,
}: {
	candidates: readonly T[];
	partnerPayroll: number;
	incomingSalary: number;
	salaryCap: number;
}): T | undefined => {
	const needed = partnerPayroll + incomingSalary - salaryCap;
	if (needed <= 0) {
		return undefined;
	}
	let best: T | undefined;
	for (const c of candidates) {
		if (c.contractAmount < needed) {
			continue;
		}
		if (
			best === undefined ||
			c.contractAmount < best.contractAmount ||
			(c.contractAmount === best.contractAmount && c.value < best.value)
		) {
			best = c;
		}
	}
	return best;
};

// Pick the smallest set of contracts that covers the shortfall, worst contracts
// first. Returns undefined when the team simply cannot get there - better to
// abandon the plan than to gut the roster chasing it.
export const planDumpPackage = <
	T extends { pid: number; contractAmount: number; value: number },
>({
	candidates,
	shortfall,
	maxPlayers = MAX_DUMP_PLAYERS,
}: {
	candidates: T[];
	shortfall: number;
	maxPlayers?: number;
}): T[] | undefined => {
	const ranked = orderBy(candidates, (p) => dumpPriority(p), "desc");

	// Prefer a single contract that does the job on its own: one bad deal out is
	// far less disruptive than three useful players out.
	const single = orderBy(
		ranked.filter((p) => p.contractAmount >= shortfall),
		(p) => dumpPriority(p),
		"desc",
	)[0];
	if (single) {
		return [single];
	}

	const chosen: T[] = [];
	let total = 0;
	for (const p of ranked) {
		if (chosen.length >= maxPlayers) {
			break;
		}
		chosen.push(p);
		total += p.contractAmount;
		if (total >= shortfall) {
			return chosen;
		}
	}
	return undefined;
};

// The target: the best free agent this team both wants AND could actually get,
// who it cannot currently fit. Returns undefined when there is nothing worth
// reorganising the payroll for.
const findTarget = async ({
	tid,
	posture,
	freeAgents,
	payroll,
	salaryCap,
	starOvr,
	season,
	minContract,
}: {
	tid: number;
	posture: TradePosture;
	freeAgents: Player[];
	payroll: number;
	salaryCap: number;
	starOvr: number;
	season: number;
	minContract: number;
}) => {
	// Only ever worth doing for a genuine difference-maker.
	const prizes = freeAgents.filter((p) =>
		isPrize({ p: toFaPlayer(p, season), starOvr, minContract }),
	);
	if (prizes.length === 0) {
		return undefined;
	}

	const scored = orderBy(
		prizes.map((p) => ({
			p,
			score: scoreFreeAgent({
				p: toFaPlayer(p, season),
				posture,
				season,
				minContract,
				maxContract: g.get("maxContract"),
			}),
		})),
		(x) => x.score,
		"desc",
	);

	let unwilling = 0;
	let affordable = 0;
	for (const { p } of scored) {
		const mood = await player.moodInfo(p, tid);

		// Mood is used for two things here, and deliberately not for a third.
		//
		// It gives the PRICE this particular team would have to pay - a player
		// who dislikes a team charges it more - and that is the number the cap
		// room has to cover. It also gives a plausibility floor, so nobody tears
		// up a payroll chasing a player with no interest whatsoever.
		//
		// What it is NOT is a hard yes/no gate. AI teams never consult mood when
		// they sign (autoSign never has), so refusing to clear space for anyone
		// mood called "unwilling" would block roughly nine of every ten pursuits
		// while the ordinary signing path went on ignoring the same flag - the
		// AI would decline to make room for players it would happily have signed
		// a day later. The reason a team can act on this at all is structural,
		// not predictive: the dump and the signing happen together below, so a
		// team physically cannot clear space and then lose him.
		if (!mood.willing && mood.probWilling < MIN_PURSUIT_CONFIDENCE) {
			unwilling += 1;
			continue;
		}
		const price = mood.contractAmount;
		if (payroll + price <= salaryCap) {
			// Already affordable - autoSign will handle it, no dump needed.
			affordable += 1;
			continue;
		}
		return { p, price, shortfall: payroll + price - salaryCap };
	}
	frontOfficeLog(season, tid, "dump-no-target", {
		prizes: prizes.length,
		unwilling,
		affordable,
		best:
			frontOfficeLoggingActive() && scored[0]
				? {
						pid: scored[0].p.pid,
						ovr: last(scored[0].p.ratings).ovr,
						probWilling:
							Math.round(
								(await player.moodInfo(scored[0].p, tid)).probWilling * 1000,
							) / 1000,
					}
				: undefined,
	});
	return undefined;
};

// Try, for one team, to clear room and sign its target. Returns true if it did.
const clearSpaceForTeam = async ({
	tid,
	posture,
	postures,
	freeAgents,
	starOvr,
	valueChangeCalculator,
	season,
}: {
	tid: number;
	posture: TradePosture;
	postures: Map<number, TradePosture>;
	freeAgents: Player[];
	starOvr: number;
	valueChangeCalculator: ValueChangeCalculator;
	season: number;
}): Promise<boolean> => {
	const salaryCap = g.get("salaryCap");
	const minContract = g.get("minContract");
	const payroll = await team.getPayroll(tid);

	const target = await findTarget({
		tid,
		posture,
		freeAgents,
		payroll,
		salaryCap,
		starOvr,
		season,
		minContract,
	});
	if (!target) {
		return false;
	}

	const roster = await idb.cache.players.indexGetAll("playersByTid", tid);
	const targetOvr = last(target.p.ratings).ovr;
	const candidates = roster
		.filter((p) =>
			isDumpable({
				ovr: last(p.ratings).ovr,
				pot: last(p.ratings).pot,
				age: season - p.born.year,
				contractAmount: p.contract.amount,
				targetOvr,
				starOvr,
				minContract,
			}),
		)
		.map((p) => ({
			pid: p.pid,
			contractAmount: p.contract.amount,
			value: p.value,
		}));

	// Never dump so deep that the team cannot field a side. checkRosterSizes
	// would paper over it with minimum signings afterwards, but a team that has
	// to be rescued from its own cap plan should not have made the plan.
	const roomToShed = Math.max(0, roster.length + 1 - g.get("minRosterSize"));
	if (roomToShed <= 0) {
		frontOfficeLog(season, tid, "dump-roster-too-thin", {
			target: target.p.pid,
			roster: roster.length,
		});
		return false;
	}

	const dump = planDumpPackage({
		candidates,
		shortfall: target.shortfall,
		maxPlayers: Math.min(MAX_DUMP_PLAYERS, roomToShed),
	});
	if (!dump) {
		frontOfficeLog(season, tid, "dump-no-package", {
			target: target.p.pid,
			shortfall: Math.round(target.shortfall),
		});
		return false;
	}
	const dumpPids = dump.map((d) => d.pid);
	const dumpSalary = dump.reduce((total, d) => total + d.contractAmount, 0);

	// What landing him is worth, against what walking the salary out costs. The
	// trade may be lopsided; the transaction as a whole may not be.
	// evaluate() is signed from the team's own point of view: positive means
	// "this makes us better", which is why the trade AI accepts a deal at dv > 0.
	const signingGain = await valueChangeCalculator.evaluate({
		tid,
		pidsAdd: [target.p.pid],
		pidsRemove: [],
		dpidsAdd: [],
		dpidsRemove: [],
		tradingPartnerTid: undefined,
	});
	// A value that cannot be computed must never be read as "free". NaN loses
	// every comparison, so an unguarded one would sail through the net-gain check
	// below and authorise a dump at any price.
	if (!Number.isFinite(signingGain) || signingGain <= 0) {
		frontOfficeLog(season, tid, "dump-no-gain", {
			target: target.p.pid,
			signingGain: Math.round(signingGain * 10) / 10,
		});
		return false;
	}

	// Who can take the money? A team with real room, that is not itself chasing
	// this player, and that is happy to be paid to absorb salary.
	// Never a human's team. Without this the AI would post salary into a
	// league-mate's roster in multiplayer, with no say from the person who
	// actually runs it - a far worse outcome than simply not making the trade.
	const teams = (await idb.cache.teams.getAll()).filter(
		(t) => t.tid !== tid && isAiControlled(t),
	);
	const partners = [];
	for (const t of teams) {
		const partnerPosture = postures.get(t.tid);
		if (!partnerPosture) {
			continue;
		}
		const partnerPayroll = await team.getPayroll(t.tid);

		// Room for the whole thing: the simple one-way dump.
		let back: { pid: number; contractAmount: number } | undefined;
		if (partnerPayroll + dumpSalary > salaryCap) {
			// Not enough room, so it has to send salary out to make some. Its own
			// stars are not on the table - a team asked to do somebody a favour
			// parts with a spare part, and offering anything more just burns an
			// evaluate() call on a deal it was always going to refuse.
			const partnerRoster = await idb.cache.players.indexGetAll(
				"playersByTid",
				t.tid,
			);
			back = planSalaryBack({
				candidates: partnerRoster
					.filter((p) => last(p.ratings).ovr < starOvr)
					.map((p) => ({
						pid: p.pid,
						contractAmount: p.contract.amount,
						value: p.value,
					})),
				partnerPayroll,
				incomingSalary: dumpSalary,
				salaryCap,
			});
			if (!back) {
				continue;
			}
			// Room the dump actually delivers, once what comes back is counted.
			if (
				payroll - dumpSalary + back.contractAmount + target.price >
				salaryCap
			) {
				continue;
			}
		}

		partners.push({
			tid: t.tid,
			posture: partnerPosture,
			payroll: partnerPayroll,
			backPids: back ? [back.pid] : [],
			backSalary: back ? back.contractAmount : 0,
		});
	}
	if (partners.length === 0) {
		frontOfficeLog(season, tid, "dump-no-partner", {
			target: target.p.pid,
			dumpSalary: Math.round(dumpSalary),
		});
		return false;
	}

	// Cleanest deal first: nothing coming back beats a little, and among equals
	// the team with the most room is the easiest to actually get done.
	partners.sort((a, b) => a.backSalary - b.backSalary || a.payroll - b.payroll);

	const myPicks = orderBy(
		await idb.cache.draftPicks.indexGetAll("draftPicksByTid", tid),
		[(dp) => dp.season, (dp) => dp.round],
		["desc", "desc"],
	);

	// Why each attempted structure was rejected, so a dump that never happens is
	// explainable instead of just absent.
	const rejected: Record<string, number> = {};
	const reject = (reason: string) => {
		rejected[reason] = (rejected[reason] ?? 0) + 1;
	};

	for (const partner of partners.slice(0, 5)) {
		// Sweeten until they say yes: nothing, then the least valuable pick, then
		// two. Picks are offered worst-first, so the cost stays as low as it can.
		for (let numPicks = 0; numPicks <= MAX_SWEETENERS; numPicks++) {
			const dpids = myPicks.slice(0, numPicks).map((dp) => dp.dpid);
			if (dpids.length < numPicks) {
				break;
			}

			const teamsForTrade = [
				{ tid, pids: dumpPids, pidsExcluded: [], dpids, dpidsExcluded: [] },
				{
					tid: partner.tid,
					pids: partner.backPids,
					pidsExcluded: [],
					dpids: [],
					dpidsExcluded: [],
				},
			] as const;

			const tradeSummary = await summary(teamsForTrade as any);
			if (tradeSummary.warning) {
				reject("illegal");
				continue;
			}

			// Would the partner take it? They are being paid in assets to absorb
			// salary, so this is a normal, favorable trade FOR THEM.
			const partnerDv = await valueChangeCalculator.evaluate({
				tid: partner.tid,
				pidsAdd: dumpPids,
				pidsRemove: partner.backPids,
				dpidsAdd: dpids,
				dpidsRemove: [],
				tradingPartnerTid: tid,
			});
			if (!Number.isFinite(partnerDv)) {
				reject("partner-value-unknown");
				continue;
			}
			if (partnerDv < 0) {
				reject("partner-refuses");
				continue;
			}

			// What it costs us. Giving players and picks away for nothing scores
			// negative, so the cost is the size of that loss. It IS allowed to be a
			// loss - that is the whole point - but only one the signing covers.
			// Whatever comes back is part of the cost, not a bonus: it is a player
			// this team did not ask for, on a contract it now carries. Pricing it
			// here is what keeps MAX_DUMP_COST_RATIO honest about the new shape.
			const myDv = await valueChangeCalculator.evaluate({
				tid,
				pidsAdd: partner.backPids,
				pidsRemove: dumpPids,
				dpidsAdd: [],
				dpidsRemove: dpids,
				tradingPartnerTid: partner.tid,
			});
			if (!Number.isFinite(myDv)) {
				reject("cost-unknown");
				continue;
			}
			const dumpCost = Math.max(0, -myDv);
			if (dumpCost > signingGain * MAX_DUMP_COST_RATIO) {
				reject("too-expensive");
				continue;
			}

			// Last look before committing: is he still on the market, and will the
			// room actually be enough? Everything below this point is irreversible,
			// and the one outcome worse than not trying is dumping salary and then
			// not landing him.
			const stillAvailable = await idb.cache.players.get(target.p.pid);
			if (!stillAvailable || stillAvailable.tid !== PLAYER.FREE_AGENT) {
				frontOfficeLog(season, tid, "dump-target-gone", {
					target: target.p.pid,
				});
				return false;
			}
			if (
				payroll - dumpSalary + partner.backSalary + target.price >
				salaryCap
			) {
				reject("room-insufficient");
				continue;
			}

			// Do it, then sign him immediately. These two must not be separated: a
			// team that dumps salary and then loses the player to someone else has
			// made itself worse for nothing, which is the one outcome worse than
			// never trying.
			await processTrade(
				[tid, partner.tid],
				[dumpPids, partner.backPids],
				[dpids, []],
				{
					initiatorTid: tid,
					tiers: [posture.tier, partner.posture.tier],
					dv: Math.round(myDv * 10) / 10,
					motivation: CAP_CLEAR_MOTIVATION,
				},
			);

			const signed = await idb.cache.players.get(target.p.pid);
			if (!signed || signed.tid !== PLAYER.FREE_AGENT) {
				// Vanishingly unlikely, but never leave the dump dangling silently.
				frontOfficeLog(season, tid, "dump-target-lost", {
					target: target.p.pid,
				});
				return true;
			}
			await player.sign(
				signed,
				tid,
				{ amount: target.price, exp: signed.contract.exp },
				g.get("phase"),
			);
			await idb.cache.players.put(signed);
			await team.rosterAutoSort(tid);
			valueChangeCalculator.invalidateCache({ teams: [tid, partner.tid] });

			frontOfficeLog(season, tid, "dump-and-sign", {
				target: target.p.pid,
				targetOvr: last(signed.ratings).ovr,
				price: Math.round(target.price),
				partner: partner.tid,
				dumped: dumpPids.length,
				dumpSalary: Math.round(dumpSalary),
				backSalary: Math.round(partner.backSalary),
				picks: dpids.length,
				dumpCost: Math.round(dumpCost * 10) / 10,
				signingGain: Math.round(signingGain * 10) / 10,
			});
			return true;
		}
	}

	frontOfficeLog(season, tid, "dump-no-deal", {
		target: target.p.pid,
		shortfall: Math.round(target.shortfall),
		partnersTried: Math.min(5, partners.length),
		signingGain: Math.round(signingGain * 10) / 10,
		rejected,
	});
	return false;
};

// Once per free agency day: give every AI team a chance to clear room for
// someone it has agreed terms with.
const clearSpaceForSignings = async () => {
	if (g.get("phase") !== PHASE.FREE_AGENCY) {
		return;
	}
	if (g.get("salaryCapType") === "none" || g.get("forceHistoricalRosters")) {
		return;
	}
	if (!g.get("smartAiFrontOffice")) {
		return;
	}
	// "No free agents" is a challenge setting, and mood marks every non-minimum
	// free agent unwilling under it. Stock AI teams sign anyway (autoSign has
	// never consulted mood), and that is left exactly as it was - but building a
	// multi-team trade to get around a rule the league turned on is a step
	// further than this should ever go.
	if (g.get("challengeNoFreeAgents")) {
		return;
	}

	const freeAgents = await idb.cache.players.indexGetAll(
		"playersByTid",
		PLAYER.FREE_AGENT,
	);
	if (freeAgents.length === 0) {
		return;
	}

	const season = g.get("season");
	const allTeams = (await idb.cache.teams.getAll()).filter(isAiControlled);
	if (allTeams.length < 2) {
		return;
	}

	let postures: Map<number, TradePosture>;
	let starOvr: number;
	try {
		const context = await getLeagueTradeContext();
		starOvr = context.starOvr;
		postures = new Map();
		for (const t of await idb.cache.teams.getAll()) {
			if (!t.disabled) {
				postures.set(t.tid, await getTradePosture(t.tid, context));
			}
		}
	} catch (error) {
		console.error("clearSpaceForSignings: posture computation failed", error);
		return;
	}

	const valueChangeCalculator = new ValueChangeCalculator();

	// Best contenders first: when two teams want the same player, the one with
	// the stronger case should get to move first.
	const ordered = orderBy(
		allTeams,
		(t) => postures.get(t.tid)?.contention ?? 0,
		"desc",
	);

	// Read once per pass, then kept up to date as deals are done within it.
	let alreadyCleared: Set<number>;
	try {
		alreadyCleared = await teamsThatAlreadyCleared();
	} catch (error) {
		// Cannot tell who has already dumped, so do not risk letting everyone do
		// it again. Skipping a day of cap clearing costs nothing.
		console.error("clearSpaceForSignings: could not read trade history", error);
		return;
	}

	for (const t of ordered) {
		if (alreadyCleared.has(t.tid)) {
			continue;
		}
		const posture = postures.get(t.tid);
		if (!posture || posture.tier === "teardown") {
			frontOfficeLog(season, t.tid, "dump-skip-tier", {
				tier: posture?.tier ?? "none",
			});
			continue;
		}
		try {
			const did = await clearSpaceForTeam({
				tid: t.tid,
				posture,
				postures,
				freeAgents: await idb.cache.players.indexGetAll(
					"playersByTid",
					PLAYER.FREE_AGENT,
				),
				starOvr,
				valueChangeCalculator,
				season,
			});
			if (did) {
				alreadyCleared.add(t.tid);
			}
		} catch (error) {
			console.error(`clearSpaceForSignings: tid ${t.tid} failed`, error);
		}
	}
};

export default clearSpaceForSignings;
