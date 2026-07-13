import { idb } from "../../db/index.ts";
import { g, local } from "../../util/index.ts";
import isUntradable from "./isUntradable.ts";
import makeItWork from "./makeItWork.ts";
import processTrade from "./processTrade.ts";
import summary from "./summary.ts";
import type { Player, TradeTeams } from "../../../common/types.ts";
import { isSport } from "../../../common/sportFunctions.ts";
import { choice } from "../../../common/random.ts";
import { ValueChangeCalculator } from "../team/ValueChangeCalculator.ts";
import {
	getLeagueTradeContext,
	getTradePosture,
	type TradePosture,
} from "./tradePosture.ts";
import {
	deadlineRampMultiplier,
	isBadRental,
	isPureDowngrade,
	isSelling,
	MOTIVATED_DUMP_DV,
	NORMAL_DV_TOLERANCE,
	partnerWeight,
	shouldDumpExpiring,
} from "./tradeMotivation.ts";
import moodInfo from "../player/moodInfo.ts";
import getDaysLeftSchedule from "../season/getDaysLeftSchedule.ts";

const getAITids = async () => {
	const teams = await idb.cache.teams.getAll();
	return teams
		.filter((t) => {
			if (t.disabled) {
				return false;
			}

			if (
				(local.autoPlayUntil || g.get("spectator")) &&
				!g.get("challengeNoTrades")
			) {
				return true;
			}
			return !g.get("userTids").includes(t.tid);
		})
		.map((t) => t.tid);
};

// A contract that ends at the close of this season (during the regular season,
// where CPU trades happen).
const expiresThisSeason = (p: Player, season: number) =>
	p.contract.exp === season;

type AttemptContext = {
	postures: Map<number, TradePosture>;
	valueChangeCalculator: ValueChangeCalculator;
	aiTids: number[];
	season: number;
};

// What the initiating team puts on the table, chosen from its posture:
//   • a walk-year player it should dump (highest priority — see shouldDumpExpiring)
//   • a seller offers a veteran it's shopping
//   • a buyer/contender offers future assets (a pick, or spare depth) to chase
//     the win-now talent its lookingFor describes
// Returns undefined if it has nothing sensible to offer.
const buildSeed = async (
	initiator: number,
	posture: TradePosture,
	players: Player[],
	draftPicks: { dpid: number }[],
	season: number,
): Promise<{ pids: number[]; dpids: number[]; motivatedDump: boolean } | undefined> => {
	const pids: number[] = [];
	const dpids: number[] = [];

	// 1) Move a walk-year player who won't re-sign, most valuable first. This is
	//    the strongest motivation, so it wins over everything else.
	const expiring = players
		.filter((p) => expiresThisSeason(p, season))
		.sort((a, b) => b.value - a.value);
	for (const p of expiring) {
		const { probWilling } = await moodInfo(p, initiator);
		if (
			shouldDumpExpiring({
				isExpiring: true,
				probWillingCurrent: probWilling,
				tier: posture.tier,
			})
		) {
			return { pids: [p.pid], dpids: [], motivatedDump: true };
		}
	}

	// 2) Seed from tier.
	if (isSelling(posture.tier)) {
		const shopSet = new Set(posture.shopVeteranPids);
		const shopPlayers = players.filter((p) => shopSet.has(p.pid));
		const pool = shopPlayers.length > 0 ? shopPlayers : players;
		const p = choice(pool, (pp) => Math.max(0.01, pp.value));
		if (p) {
			pids.push(p.pid);
		}
	} else if (draftPicks.length > 0 && Math.random() < 0.6) {
		// A buyer prefers to spend future assets on present talent.
		dpids.push(choice(draftPicks).dpid);
	} else {
		// Otherwise offer spare depth (never a building block).
		const blocks = new Set(posture.buildingBlockPids);
		const spare = players.filter((p) => !blocks.has(p.pid));
		const pool = spare.length > 0 ? spare : players;
		const p = choice(pool, (pp) => Math.max(0.01, pp.value));
		if (p) {
			pids.push(p.pid);
		}
	}

	if (pids.length === 0 && dpids.length === 0) {
		return undefined;
	}
	return { pids, dpids, motivatedDump: false };
};

// Total on-court value + value-weighted age of a set of players (given or
// received in a trade).
const talentAndAge = async (
	pids: number[],
	season: number,
): Promise<{ value: number; age: number }> => {
	let value = 0;
	let ageNum = 0;
	let ageDen = 0;
	for (const pid of pids) {
		const p = await idb.cache.players.get(pid);
		if (!p) {
			continue;
		}
		const v = Math.max(0, p.value);
		const age = season - p.born.year;
		value += v;
		const w = Math.max(0.01, v);
		ageNum += age * w;
		ageDen += w;
	}
	return { value, age: ageDen > 0 ? ageNum / ageDen : 0 };
};

// Would this trade leave either side a pure downgrade (less talent, no younger,
// no picks)? A backstop against the valuation being fooled into a bad deal.
const anyPureDowngrade = async (
	teams: TradeTeams,
	season: number,
): Promise<boolean> => {
	const sides = [
		{ given: teams[0].pids, recv: teams[1].pids, recvDpids: teams[1].dpids },
		{ given: teams[1].pids, recv: teams[0].pids, recvDpids: teams[0].dpids },
	];
	for (const side of sides) {
		const given = await talentAndAge(side.given, season);
		const recv = await talentAndAge(side.recv, season);
		if (
			isPureDowngrade({
				givenValue: given.value,
				receivedValue: recv.value,
				givenAge: given.age,
				receivedAge: recv.age,
				receivedPicks: side.recvDpids.length > 0,
			})
		) {
			return true;
		}
	}
	return false;
};

// Would this trade hand a team an expiring player it can't retain (a bad
// rental)? Only a genuine win-now contender should take one on.
const hasBadRental = async (
	teams: TradeTeams,
	postures: Map<number, TradePosture>,
	season: number,
): Promise<boolean> => {
	const flows = [
		{ receiver: teams[1].tid, pids: teams[0].pids },
		{ receiver: teams[0].tid, pids: teams[1].pids },
	];
	for (const { receiver, pids } of flows) {
		const tier = postures.get(receiver)?.tier ?? "fringe";
		for (const pid of pids) {
			const p = await idb.cache.players.get(pid);
			if (!p || !expiresThisSeason(p, season)) {
				continue;
			}
			const { probWilling } = await moodInfo(p, receiver);
			if (
				isBadRental({
					isExpiring: true,
					probWillingAcquirer: probWilling,
					acquirerTier: tier,
				})
			) {
				return true;
			}
		}
	}
	return false;
};

const attempt = async (
	ctx: AttemptContext,
): Promise<[number, number] | false> => {
	const { postures, valueChangeCalculator, aiTids, season } = ctx;
	if (aiTids.length < 2) {
		return false;
	}

	// Initiator: the more motivated a team is (aggressive posture, veterans to
	// shop), the more likely it is to be the one making a move.
	const initiator = choice(aiTids, (tid) => {
		const p = postures.get(tid);
		if (!p) {
			return 0.3;
		}
		return p.aggression + (p.shopVeteranPids.length > 0 ? 0.5 : 0);
	});
	const initPosture = postures.get(initiator);
	if (!initPosture) {
		return false;
	}

	// Partner: drawn to the opposite end of the buy/sell spectrum.
	const others = aiTids.filter((t) => t !== initiator);
	if (others.length === 0) {
		return false;
	}
	const partner = choice(others, (tid) => {
		const pp = postures.get(tid);
		return pp ? partnerWeight(initPosture.tier, pp.tier) : 1;
	});

	const players = (
		await idb.cache.players.indexGetAll("playersByTid", initiator)
	).filter((p) => !isUntradable(p).untradable);
	const draftPicks = await idb.cache.draftPicks.indexGetAll(
		"draftPicksByTid",
		initiator,
	);
	if (players.length === 0 && draftPicks.length === 0) {
		return false;
	}

	const seed = await buildSeed(
		initiator,
		initPosture,
		players,
		draftPicks,
		season,
	);
	if (!seed) {
		return false;
	}

	const teams0: TradeTeams = [
		{
			tid: initiator,
			pids: seed.pids,
			pidsExcluded: [],
			dpids: seed.dpids,
			dpidsExcluded: [],
		},
		{
			tid: partner,
			pids: [],
			pidsExcluded: [],
			dpids: [],
			dpidsExcluded: [],
		},
	];

	// makeItWork fleshes out the deal until the PARTNER accepts (dv > 0 for it),
	// pulling the assets the initiator's lookingFor describes.
	const teams = await makeItWork(teams0, {
		holdUserConstant: false,
		maxAssetsToAdd: 6,
		lookingFor: initPosture.lookingFor,
		valueChangeCalculator,
	});
	if (!teams) {
		return false;
	}

	// Don't do trades of just picks, or where the partner gives nothing.
	if (teams[0].pids.length === 0 && teams[1].pids.length === 0) {
		return false;
	}
	if (teams[1].pids.length === 0 && teams[1].dpids.length === 0) {
		return false;
	}

	const tradeSummary = await summary(teams);
	if (tradeSummary.warning) {
		return false;
	}

	// No bad rentals: nobody but an all-in contender takes a low-mood walk-year.
	if (await hasBadRental(teams, postures, season)) {
		return false;
	}

	// No pure downgrades: never let a team come out worse AND older with no picks
	// to show for it (a backstop against a fooled valuation).
	if (await anyPureDowngrade(teams, season)) {
		return false;
	}

	// The initiator must find the deal roughly fair to itself — unless it's
	// dumping a walk-year player, in which case it will swallow a worse return
	// rather than lose him for nothing.
	const dv2 = await valueChangeCalculator.evaluate({
		tid: teams[0].tid,
		pidsAdd: teams[1].pids,
		pidsRemove: teams[0].pids,
		dpidsAdd: teams[1].dpids,
		dpidsRemove: teams[0].dpids,
		tradingPartnerTid: undefined,
	});
	const lowerBound = seed.motivatedDump ? MOTIVATED_DUMP_DV : -NORMAL_DV_TOLERANCE;
	if (dv2 > NORMAL_DV_TOLERANCE || dv2 < lowerBound) {
		return false;
	}

	const finalTids: [number, number] = [teams[0].tid, teams[1].tid];
	await processTrade(
		finalTids,
		[teams[0].pids, teams[1].pids],
		[teams[0].dpids, teams[1].dpids],
	);
	return finalTids;
};

const DEFAULT_NUM_TEAMS = 30;

// A modest baseline bump so strategic deals actually materialize ("somewhat more
// active"), on top of which the deadline ramp fires.
const ACTIVITY_BUMP = 1.3;

const betweenAiTeams = async () => {
	if (g.get("forceHistoricalRosters")) {
		return false;
	}

	// If aiTradesFactor is not an integer, use the fractional part as a probability.
	// Also scale so there are fewer trade attempts if there are fewer teams.
	let float = g.get("aiTradesFactor");
	if (isSport("baseball")) {
		float *= 0.25;
	}
	if (g.get("numActiveTeams") < DEFAULT_NUM_TEAMS) {
		float *= g.get("numActiveTeams") / DEFAULT_NUM_TEAMS;
	}
	float *= ACTIVITY_BUMP;

	// Trades ramp up as the deadline approaches (a deadline frenzy).
	let daysToDeadline: number | undefined;
	try {
		daysToDeadline = await getDaysLeftSchedule("tradeDeadline");
	} catch {
		// No deadline in the schedule (e.g. it's disabled) - just don't ramp.
	}
	float *= deadlineRampMultiplier(daysToDeadline);

	let numAttempts = Math.floor(float);
	const remainder = float % 1;
	if (remainder > 0 && Math.random() < remainder) {
		numAttempts += 1;
	}
	if (numAttempts <= 0) {
		return;
	}

	const aiTids = await getAITids();
	if (aiTids.length < 2) {
		return;
	}

	const valueChangeCalculator = new ValueChangeCalculator();

	// Every AI team's franchise posture, computed once for this batch of attempts.
	// If this fails for any reason, skip trading this tick rather than deal blind.
	let postures: Map<number, TradePosture>;
	try {
		const context = await getLeagueTradeContext();
		postures = new Map();
		for (const tid of aiTids) {
			postures.set(tid, await getTradePosture(tid, context));
		}
	} catch (error) {
		console.error("betweenAiTeams: posture computation failed", error);
		return;
	}

	// Price trades from the SAME posture tiers that drive the seeding, so the
	// valuation and the intent are consistent (no recompute inside the calculator).
	valueChangeCalculator.setPostureTiers(
		new Map([...postures].map(([tid, posture]) => [tid, posture.tier])),
	);

	const season = g.get("season");
	for (let i = 0; i < numAttempts; i++) {
		const tradeTids = await attempt({
			postures,
			valueChangeCalculator,
			aiTids,
			season,
		});
		if (tradeTids) {
			// Don't need to recompute draft pick value.
			valueChangeCalculator.invalidateCache({ teams: tradeTids });
		}
	}
};

export default betweenAiTeams;
