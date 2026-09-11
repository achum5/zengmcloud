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
	BLOCKBUSTER_MAX_ASSETS,
	contenderDowngradesBest,
	deadlineRampMultiplier,
	isBadRental,
	isPureDowngrade,
	contenderDowngradesBestOvr,
	isSelling,
	isStarAcquisition,
	MAX_ASSETS_PER_SIDE,
	MAX_ASSETS_PER_SIDE_HUNT,
	MOTIVATED_DUMP_DV,
	NORMAL_DV_TOLERANCE,
	NORMAL_MAX_ASSETS,
	partnerWeight,
	sellerAcquiresVet,
	shouldDumpExpiring,
	STAR_PREMIUM_DV,
	STAR_SALE_DV,
	wasTradedThisSeason,
} from "./tradeMotivation.ts";
import { last } from "../../../common/utils.ts";
import moodInfo from "../player/moodInfo.ts";
import getDaysLeftSchedule from "../season/getDaysLeftSchedule.ts";
import getPayroll from "../team/getPayroll.ts";
import {
	ballastNeeded,
	HUNT_CHANCE,
	HUNT_CHANCE_STAR_GAP,
	incomingCeiling,
	noteHunt,
	pickBallast,
} from "./starHunt.ts";

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

export type AttemptContext = {
	postures: Map<number, TradePosture>;
	valueChangeCalculator: ValueChangeCalculator;
	aiTids: number[];
	season: number;
	// A genuine star's OVR bar (from the league context), so an acquisition can be
	// recognized as a blockbuster.
	starOvr: number;
	// And the value bar, which is what decides whose own star stays home in a
	// hunt.
	starValue: number;
};

// The best current OVR among a set of players (what a side is receiving), used to
// tell whether a deal lands a genuine star.
const maxOvr = async (pids: number[]): Promise<number> => {
	let best = 0;
	for (const pid of pids) {
		const p = await idb.cache.players.get(pid);
		if (p) {
			best = Math.max(best, last(p.ratings).ovr);
		}
	}
	return best;
};

// What the initiating team puts on the table, chosen from its posture:
//   • a walk-year player it should dump (highest priority — see shouldDumpExpiring)
//   • a seller offers a veteran it's shopping — and if a legit STAR is on the
//     block, it deliberately shops him first (moving the star to a win-now team
//     is the whole point of selling)
//   • a buyer/contender offers future assets (a pick, or spare depth) to chase
//     the win-now talent its lookingFor describes
// Returns undefined if it has nothing sensible to offer.
export const buildSeed = async (
	initiator: number,
	posture: TradePosture,
	players: Player[],
	draftPicks: { dpid: number }[],
	season: number,
	starOvr: number,
	// Source of randomness. The AI-AI path uses Math.random; the user-facing
	// trade proposals pass a seeded stream so the page is stable between games.
	rand: () => number = Math.random,
): Promise<
	| {
			pids: number[];
			dpids: number[];
			motivatedDump: boolean;
			starSale: boolean;
	  }
	| undefined
> => {
	const pids: number[] = [];
	const dpids: number[] = [];
	let starSale = false;

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
			return { pids: [p.pid], dpids: [], motivatedDump: true, starSale: false };
		}
	}

	// 2) Seed from tier.
	if (isSelling(posture.tier)) {
		const shopSet = new Set(posture.shopVeteranPids);
		const shopPlayers = players.filter((p) => shopSet.has(p.pid));
		// The star gets shopped FIRST most of the time — sellers actively work the
		// phones to send him somewhere he helps, rather than waiting to be asked.
		const starsOnBlock = shopPlayers
			.filter((p) => last(p.ratings).ovr >= starOvr)
			.sort((a, b) => b.value - a.value);
		let p: Player | undefined;
		if (starsOnBlock.length > 0 && rand() < 0.6) {
			p = starsOnBlock[0];
		} else {
			// The fallback is the WHOLE roster, building blocks included, and
			// that is deliberate - it was changed to exclude them, to match the
			// buyer branch twenty lines down, and measured worse on six seeds:
			// the top five lost 1.1 points, rotation talent 0.22, dead money up
			// 11% on all six. More trades happened and they were worth less.
			//
			// The reason is a distinction worth keeping in mind anywhere else
			// this comes up. BEING ON THE BLOCK IS NOT THE SAME AS BEING
			// AVAILABLE AS FILLER. A seed is shopped to a shortlist of teams
			// that bid against each other, so a cornerstone offered here fetches
			// what the market thinks he is worth; the untouchables filter below
			// is what stops the same player being quietly folded into somebody
			// else's package for nothing. Protecting him from the first is not
			// protection, it is just a worse trade - the team ends up shopping
			// its second-best player instead and getting a second-best return.
			const pool = shopPlayers.length > 0 ? shopPlayers : players;
			p = choice(pool, (pp) => Math.max(0.01, pp.value), rand());
		}
		if (p) {
			pids.push(p.pid);
			starSale = last(p.ratings).ovr >= starOvr;
		}
	} else if (draftPicks.length > 0 && rand() < 0.6) {
		// A buyer prefers to spend future assets on present talent.
		dpids.push(choice(draftPicks, undefined, rand()).dpid);
	} else {
		// Otherwise offer spare depth (never a building block).
		const blocks = new Set(posture.buildingBlockPids);
		const spare = players.filter((p) => !blocks.has(p.pid));
		const pool = spare.length > 0 ? spare : players;
		const p = choice(pool, (pp) => Math.max(0.01, pp.value), rand());
		if (p) {
			pids.push(p.pid);
		}
	}

	if (pids.length === 0 && dpids.length === 0) {
		return undefined;
	}
	return { pids, dpids, motivatedDump: false, starSale };
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
	// A star hunt is a consolidation by design - three rotation players for
	// one star SUMS to less talent, and that is the whole point of the deal.
	// The hunter's side is judged by the contender guards instead (it must
	// come out with a better best player); only the seller's is checked here.
	consolidating = false,
): Promise<boolean> => {
	const sides = [
		{ given: teams[0].pids, recv: teams[1].pids, recvDpids: teams[1].dpids },
		{ given: teams[1].pids, recv: teams[0].pids, recvDpids: teams[0].dpids },
	];
	for (const [i, side] of sides.entries()) {
		if (consolidating && i === 0) {
			continue;
		}
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

// Does either side end up violating its TIMELINE? Two rules, independent of the
// value math:
//   • a rebuilder (seller/teardown) never acquires a real veteran unless it's
//     being paid in draft capital to absorb him ("26-56 team trades for a 33yo
//     on $60M" is never right);
//   • a contender (allIn/buyer) never comes out with a clearly worse best player
//     than it gave up — younger or picks-heavy returns don't excuse it, because
//     contenders don't collect futures at the cost of the present.
const violatesTimeline = async (
	teams: TradeTeams,
	postures: Map<number, TradePosture>,
	season: number,
): Promise<boolean> => {
	const sides = [
		{
			tid: teams[0].tid,
			incoming: teams[1].pids,
			outgoing: teams[0].pids,
			receivesPicks: teams[1].dpids.length > 0,
		},
		{
			tid: teams[1].tid,
			incoming: teams[0].pids,
			outgoing: teams[1].pids,
			receivesPicks: teams[0].dpids.length > 0,
		},
	];
	for (const side of sides) {
		const tier = postures.get(side.tid)?.tier ?? "fringe";

		let bestReceived = 0;
		let bestReceivedOvr = 0;
		for (const pid of side.incoming) {
			const p = await idb.cache.players.get(pid);
			if (!p) {
				continue;
			}
			bestReceived = Math.max(bestReceived, p.value);
			bestReceivedOvr = Math.max(bestReceivedOvr, last(p.ratings).ovr);
			if (
				sellerAcquiresVet({
					acquirerTier: tier,
					age: season - p.born.year,
					value: p.value,
					receivesPicks: side.receivesPicks,
				})
			) {
				return true;
			}
		}

		let bestGiven = 0;
		let bestGivenOvr = 0;
		for (const pid of side.outgoing) {
			const p = await idb.cache.players.get(pid);
			if (p) {
				bestGiven = Math.max(bestGiven, p.value);
				bestGivenOvr = Math.max(bestGivenOvr, last(p.ratings).ovr);
			}
		}
		if (
			contenderDowngradesBest({
				acquirerTier: tier,
				bestGivenValue: bestGiven,
				bestReceivedValue: bestReceived,
			}) ||
			// The OVR check catches what the value math masks for an AGING star: a
			// contender shipping its best on-court piece for a clearly worse one.
			contenderDowngradesBestOvr({
				acquirerTier: tier,
				bestGivenOvr,
				bestReceivedOvr,
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

const MARKET_CANDIDATES = 5;

// A weighted sample of up to `count` DISTINCT partners, favoring the tiers that
// fit (a seller courts buyers, a star-hunter courts teams with a shoppable
// star). This is the shortlist of teams the initiator "calls" this round - the
// pool it feels out, not a single pre-chosen partner.
const shortlistPartners = (
	others: number[],
	initPosture: TradePosture,
	postures: Map<number, TradePosture>,
	count: number,
): number[] => {
	const weight = (tid: number): number => {
		const pp = postures.get(tid);
		if (!pp) {
			return 1;
		}
		let w = partnerWeight(initPosture.tier, pp.tier);
		if (
			initPosture.tier === "allIn" &&
			initPosture.starGap &&
			pp.shoppableStar
		) {
			w *= 3;
		}
		if (
			isSelling(initPosture.tier) &&
			initPosture.shoppableStar &&
			(pp.tier === "allIn" || pp.tier === "buyer")
		) {
			w *= 2;
		}
		return w;
	};
	const pool = [...others];
	const picked: number[] = [];
	while (pool.length > 0 && picked.length < count) {
		const t = choice(pool, weight);
		picked.push(t);
		pool.splice(pool.indexOf(t), 1);
	}
	return picked;
};

// Every check a deal must clear beyond the value math, in one place, so the
// AI-AI market, the proposals page, and the trading block all refuse the same
// nonsense: cap-rule warnings, rentals landing anywhere but a contender, a
// side that comes out strictly worse, a team acting against its own timeline.
//
// A note for whoever reads the numbers here and reaches for the obvious fix.
// Over twenty simulated seasons the SOFT-CAP SALARY MATCH accounts for 92% of
// every offer these guards kill - 2597 of them, against 115 downgrades, 88
// timeline violations and 24 bad rentals. makeItWork builds the cheapest
// package that clears on VALUE and knows nothing about salary, so an
// over-the-cap partner is constantly handed an offer it is not allowed to
// accept, and star sales in particular almost never survive.
//
// Attaching salary filler to close the gap - which is what a real front office
// does, and summary even computes the exact shortfall - was built and measured
// twice. Any-contract filler raised trade volume by a fifth and cost nearly two
// points of league quality plus 26M a year in dead money: the value curve
// prices a bad player at roughly nothing, so the cheapest way to satisfy the
// match is always to hand somebody a long bad deal, which the receiving team
// then releases. Restricting filler to EXPIRING contracts (the realistic
// version) removed most of the extra trades while still raising dead money.
// Neither version produced a single extra star sale.
//
// So the rule stands as a wall on purpose. The deals it blocks are mostly
// deals that should not happen, and forcing them through only moves bad money
// around the league.
export const offerPassesGuards = async (
	teams: TradeTeams,
	postures: Map<number, TradePosture>,
	season: number,
	why: (reason: string) => void = () => {},
	// See anyPureDowngrade: the initiator is consolidating for a star.
	consolidating = false,
): Promise<boolean> => {
	const tradeSummary = await summary(teams);
	if (tradeSummary.warning) {
		why("salary");
		return false;
	}
	if (await hasBadRental(teams, postures, season)) {
		why("rental");
		return false;
	}
	if (await anyPureDowngrade(teams, season, consolidating)) {
		why("downgrade");
		return false;
	}
	if (await violatesTimeline(teams, postures, season)) {
		why("timeline");
		return false;
	}
	return true;
};

// One candidate partner's BEST offer for what the initiator is shopping, fully
// guarded — or null if nothing clean clears. A single feeler in the market: the
// partner (via makeItWork) assembles the cheapest package IT will accept, judged
// by ITS OWN outlook (the value calc is stock BBGM, tilted by each team's
// strategy). We return how good that offer is FOR THE INITIATOR (dv2), so the
// caller can compare competing offers and take the best one.
export const buildOfferFromPartner = async (args: {
	initiator: number;
	initPosture: TradePosture;
	seed: {
		pids: number[];
		dpids: number[];
		motivatedDump: boolean;
		starSale: boolean;
	};
	initiatorExcluded: number[];
	partner: number;
	// Assets on the PARTNER's side the initiator is specifically asking for -
	// how a team calls about YOUR player rather than shopping its own.
	partnerSeedPids?: number[];
	// Same, for the partner's draft picks - how a team calls about a PICK, the
	// draft-night trade-up.
	partnerSeedDpids?: number[];
	// A star hunt: the deal is only worth making if a genuine star comes back
	// (the hunter has put players on the table it would never move for less).
	requireStar?: boolean;
	// Told why an offer died, for diagnostics.
	why?: (reason: string) => void;
	ctx: AttemptContext;
}): Promise<{ teams: TradeTeams; dv2: number; landsStar: boolean } | null> => {
	const {
		initiator,
		initPosture,
		seed,
		initiatorExcluded,
		partner,
		partnerSeedPids = [],
		partnerSeedDpids = [],
		requireStar = false,
		why = () => {},
		ctx,
	} = args;
	const { postures, valueChangeCalculator, season, starOvr } = ctx;

	// Both sides' building blocks (and just-traded players) are off the table.
	const partnerPosture = postures.get(partner);
	const partnerPlayers = await idb.cache.players.indexGetAll(
		"playersByTid",
		partner,
	);
	const partnerExcluded = [
		...(partnerPosture?.buildingBlockPids ?? []),
		...partnerPlayers
			.filter((p) => wasTradedThisSeason(p.transactions, season))
			.map((p) => p.pid),
	];

	// In a hunt the partner gives the star and nothing else: the hunter named
	// him and pays, and a seller padding the return with its own bodies is
	// what turned most of these into salary-rule refusals.
	const partnerGivesOnlySeed = requireStar;
	const teams0: TradeTeams = [
		{
			tid: initiator,
			pids: seed.pids,
			pidsExcluded: initiatorExcluded,
			dpids: seed.dpids,
			dpidsExcluded: [],
		},
		{
			tid: partner,
			pids: partnerSeedPids,
			pidsExcluded: partnerGivesOnlySeed
				? partnerPlayers
						.map((p) => p.pid)
						.filter((pid) => !partnerSeedPids.includes(pid))
				: partnerExcluded.filter((pid) => !partnerSeedPids.includes(pid)),
			dpids: partnerSeedDpids,
			dpidsExcluded: partnerGivesOnlySeed
				? (await idb.cache.draftPicks.indexGetAll("draftPicksByTid", partner))
						.map((dp) => dp.dpid)
						.filter((dpid) => !partnerSeedDpids.includes(dpid))
				: [],
		},
	];

	// A win-now contender hunting talent may assemble a much bigger package so a
	// genuine star can actually come together; makeItWork stops at the minimal
	// clearing deal, so this only enlarges the deals that truly need it.
	const chasingTalent =
		!isSelling(initPosture.tier) && initPosture.lookingFor.bestCurrentPlayers;
	const maxAssetsToAdd = chasingTalent
		? BLOCKBUSTER_MAX_ASSETS
		: NORMAL_MAX_ASSETS;

	const teams = await makeItWork(teams0, {
		holdUserConstant: false,
		maxAssetsToAdd,
		lookingFor: initPosture.lookingFor,
		valueChangeCalculator,
	});
	if (!teams) {
		why("makeItWork");
		return null;
	}

	// Don't do trades of just picks, or where the partner gives nothing.
	if (teams[0].pids.length === 0 && teams[1].pids.length === 0) {
		why("picksOnly");
		return null;
	}
	if (teams[1].pids.length === 0 && teams[1].dpids.length === 0) {
		why("partnerGivesNothing");
		return null;
	}

	// Realism cap on package size (sub-average players are ~free under the curve).
	const maxPerSide = requireStar
		? MAX_ASSETS_PER_SIDE_HUNT
		: MAX_ASSETS_PER_SIDE;
	if (
		teams[0].pids.length + teams[0].dpids.length > maxPerSide ||
		teams[1].pids.length + teams[1].dpids.length > maxPerSide
	) {
		why("tooManyAssets");
		return null;
	}

	if (!(await offerPassesGuards(teams, postures, season, why, requireStar))) {
		return null;
	}

	// How good is this offer for the INITIATOR? (Its own strategy-tilted value.)
	const dv2 = await valueChangeCalculator.evaluate({
		tid: teams[0].tid,
		pidsAdd: teams[1].pids,
		pidsRemove: teams[0].pids,
		dpidsAdd: teams[1].dpids,
		dpidsRemove: teams[0].dpids,
		tradingPartnerTid: undefined,
	});
	const landsStar = isStarAcquisition({
		bestReceivedOvr: await maxOvr(teams[1].pids),
		acquirerTier: initPosture.tier,
		starOvr,
	});
	if (requireStar && !landsStar) {
		why("noStarLanded");
		return null;
	}
	let lowerBound = -NORMAL_DV_TOLERANCE;
	if (seed.motivatedDump) {
		lowerBound = Math.min(lowerBound, MOTIVATED_DUMP_DV);
	}
	if (seed.starSale) {
		lowerBound = Math.min(lowerBound, STAR_SALE_DV);
	}
	if (landsStar) {
		lowerBound = Math.min(lowerBound, STAR_PREMIUM_DV);
	}
	if (dv2 > NORMAL_DV_TOLERANCE || dv2 < lowerBound) {
		why(
			dv2 > NORMAL_DV_TOLERANCE ? "tooGoodForInitiator" : "tooBadForInitiator",
		);
		return null;
	}

	return { teams, dv2, landsStar };
};

// The pre-posture algorithm, verbatim from this repo's history (337a28255)
// modulo the newer ValueChangeCalculator API: random initiator, random
// partner, a value-weighted random asset on the table, makeItWork, and a
// plain |dv| <= 15 sanity bound. This is what "smart front office off" means
// for AI-AI trades - the valuation layer already reverts on its own (it
// prices by the legacy strategy flag when the setting is off), so with this
// the whole path is stock again.
const legacyAttempt = async (
	valueChangeCalculator: ValueChangeCalculator,
): Promise<boolean> => {
	const aiTids = await getAITids();
	if (aiTids.length === 0) {
		return false;
	}

	const tid = choice(aiTids);
	const otherTids = aiTids.filter((tid2) => tid !== tid2);
	if (otherTids.length === 0) {
		return false;
	}
	const otherTid = choice(otherTids);

	const players = (
		await idb.cache.players.indexGetAll("playersByTid", tid)
	).filter((p) => !isUntradable(p).untradable);
	const draftPicks = await idb.cache.draftPicks.indexGetAll(
		"draftPicksByTid",
		tid,
	);
	if (players.length === 0 && draftPicks.length === 0) {
		return false;
	}

	const r = Math.random();
	const pids: number[] = [];
	const dpids: number[] = [];
	if ((r < 0.7 || draftPicks.length === 0) && players.length > 0) {
		// Weight by player value - good player more likely to be in trade
		const p = choice(players, (p2) => p2.value);
		if (!p) {
			return false;
		}
		pids.push(p.pid);
	} else if ((r < 0.85 || players.length === 0) && draftPicks.length > 0) {
		dpids.push(choice(draftPicks).dpid);
	} else {
		const p = choice(players, (p2) => p2.value);
		const dp = choice(draftPicks);
		if (!p || !dp) {
			return false;
		}
		pids.push(p.pid);
		dpids.push(dp.dpid);
	}

	const teams0: TradeTeams = [
		{ dpids, dpidsExcluded: [], pids, pidsExcluded: [], tid },
		{
			dpids: [],
			dpidsExcluded: [],
			pids: [],
			pidsExcluded: [],
			tid: otherTid,
		},
	];

	const teams = await makeItWork(teams0, {
		holdUserConstant: false,
		maxAssetsToAdd: 5,
		valueChangeCalculator,
	});
	if (!teams) {
		return false;
	}

	// Don't do trades of just picks, it's weird usually
	if (teams[0].pids.length === 0 && teams[1].pids.length === 0) {
		return false;
	}

	// Don't do trades for nothing, it's weird usually
	if (teams[1].pids.length === 0 && teams[1].dpids.length === 0) {
		return false;
	}

	const tradeSummary = await summary(teams);
	if (tradeSummary.warning) {
		return false;
	}

	// Make sure this isn't a really shitty trade
	const dv2 = await valueChangeCalculator.evaluate({
		tid: teams[0].tid,
		pidsAdd: teams[1].pids,
		pidsRemove: teams[0].pids,
		dpidsAdd: teams[1].dpids,
		dpidsRemove: teams[0].dpids,
		tradingPartnerTid: undefined,
	});
	if (Math.abs(dv2) > 15) {
		return false;
	}

	await processTrade(
		[teams[0].tid, teams[1].tid],
		[teams[0].pids, teams[1].pids],
		[teams[0].dpids, teams[1].dpids],
	);
	return true;
};

// THE STAR HUNT. A contender in striking distance calls a partner about its
// shoppable star, names him, and pays: with the young players and picks it
// would otherwise sit on (only a star of its own stays off the table), and
// with whatever salary the cap rule makes it send back. See starHunt.ts for
// why both halves are needed before a single blockbuster can come together.
// Returns the best offer across the shortlist, or null.
export const huntStar = async ({
	initiator,
	initPosture,
	players,
	candidates,
	ctx,
}: {
	initiator: number;
	initPosture: TradePosture;
	// The initiator's tradable, not-just-traded players.
	players: Player[];
	candidates: number[];
	ctx: AttemptContext;
}): Promise<{ teams: TradeTeams; dv2: number; landsStar: boolean } | null> => {
	const { postures, season, starOvr, starValue } = ctx;

	// Only a star of its own is untouchable in a hunt; core players are the
	// price. Just-traded players stay out, as everywhere.
	const stars = new Set(
		players.filter((p) => p.value >= starValue).map((p) => p.pid),
	);
	const initiatorExcluded = [
		...initPosture.buildingBlockPids.filter((pid) => stars.has(pid)),
		...players
			.filter((p) => wasTradedThisSeason(p.transactions, season))
			.map((p) => p.pid),
	];

	// Every real star trade carries a first: the furthest-out one the hunter
	// owns goes on the table with the money, and the value machinery adds
	// what else it takes. (It also keeps the seller's timeline guard happy -
	// a rebuilder takes back a veteran only when it is being paid in picks.)
	const firsts = (
		await idb.cache.draftPicks.indexGetAll("draftPicksByTid", initiator)
	)
		.filter((dp) => dp.round === 1)
		.sort((a, b) => {
			const sa = typeof a.season === "number" ? a.season : 0;
			const sb = typeof b.season === "number" ? b.season : 0;
			return sb - sa || a.dpid - b.dpid;
		});
	const seedDpids = firsts.length > 0 ? [firsts[0]!.dpid] : [];

	const payroll = await getPayroll(initiator);
	const salaryCap = g.get("salaryCap");
	const salaryCapType = g.get("salaryCapType");
	const softCapTradeSalaryMatch = g.get("softCapTradeSalaryMatch");

	let best: { teams: TradeTeams; dv2: number; landsStar: boolean } | null =
		null;
	for (const partner of candidates) {
		const partnerPosture = postures.get(partner);
		if (!partnerPosture?.shoppableStar) {
			noteHunt("noShoppableStar");
			continue;
		}
		const blocks = new Set(partnerPosture.buildingBlockPids);
		const target = (
			await idb.cache.players.indexGetAll("playersByTid", partner)
		)
			.filter(
				(p) =>
					!blocks.has(p.pid) &&
					last(p.ratings).ovr >= starOvr &&
					!isUntradable(p).untradable &&
					!wasTradedThisSeason(p.transactions, season),
			)
			.sort((a, b) => last(b.ratings).ovr - last(a.ratings).ovr)[0];
		if (!target) {
			noteHunt("noTarget");
			continue;
		}

		// The money first, so the value machinery below builds on a package
		// the cap rule will let through - on both ends. The hunter must send
		// enough to take the star on; the seller may take back only so much.
		const needed = ballastNeeded({
			incoming: target.contract.amount,
			payroll,
			salaryCap,
			salaryCapType,
			softCapTradeSalaryMatch,
		});
		const ceiling = incomingCeiling({
			outgoing: target.contract.amount,
			payroll: await getPayroll(partner),
			salaryCap,
			salaryCapType,
			softCapTradeSalaryMatch,
		});
		const ballast = pickBallast(
			players
				.filter((p) => !initiatorExcluded.includes(p.pid))
				.map((p) => ({
					pid: p.pid,
					value: p.value,
					amount: p.contract.amount,
					yearsLeft: Math.max(0, p.contract.exp - season),
				})),
			needed,
			undefined,
			ceiling,
		);
		if (!ballast) {
			noteHunt("noBallast");
			continue;
		}
		// Whatever room is left under the seller's ceiling is what the rest of
		// the package may cost in salary: anyone paid more than that stays
		// home, so the price is paid in picks and cheap young players.
		let ballastSalary = 0;
		for (const pid of ballast) {
			ballastSalary += players.find((p) => p.pid === pid)?.contract.amount ?? 0;
		}
		const room = ceiling - ballastSalary;
		const excludedForSalary = players
			.filter((p) => !ballast.includes(p.pid) && p.contract.amount > room)
			.map((p) => p.pid);

		const offer = await buildOfferFromPartner({
			initiator,
			initPosture,
			seed: {
				pids: ballast,
				dpids: seedDpids,
				motivatedDump: false,
				starSale: false,
			},
			initiatorExcluded: [...initiatorExcluded, ...excludedForSalary],
			partner,
			partnerSeedPids: [target.pid],
			requireStar: true,
			why: noteHunt,
			ctx,
		});
		if (offer) {
			noteHunt("offer");
			if (best === null || offer.dv2 > best.dv2) {
				best = offer;
			}
		}
	}
	if (candidates.length === 0) {
		noteHunt("noCandidates");
	}
	return best;
};

const attempt = async (
	ctx: AttemptContext,
): Promise<[number, number] | false> => {
	const { postures, aiTids, season, starOvr } = ctx;
	if (aiTids.length < 2) {
		return false;
	}

	// Initiator: the more motivated a team is (aggressive posture, veterans to
	// shop), the more likely it is to be the one taking its assets to the market.
	const initiator = choice(aiTids, (tid) => {
		const p = postures.get(tid);
		if (!p) {
			return 0.3;
		}
		// An elite roster works the phones far harder than anyone — it should be
		// shocking if a top-of-the-league team doesn't land an upgrade in a season.
		return (
			p.aggression +
			(p.shopVeteranPids.length > 0 ? 0.5 : 0) +
			(p.elite ? 1.5 : 0)
		);
	});
	const initPosture = postures.get(initiator);
	if (!initPosture) {
		return false;
	}

	const allInitiatorPlayers = await idb.cache.players.indexGetAll(
		"playersByTid",
		initiator,
	);
	const players = allInitiatorPlayers.filter(
		(p) =>
			!isUntradable(p).untradable &&
			// No same-season ping-pong: don't flip a player you just traded for.
			!wasTradedThisSeason(p.transactions, season),
	);
	const draftPicks = await idb.cache.draftPicks.indexGetAll(
		"draftPicksByTid",
		initiator,
	);
	if (players.length === 0 && draftPicks.length === 0) {
		return false;
	}

	const others = aiTids.filter((t) => t !== initiator);
	if (others.length === 0) {
		return false;
	}

	// A contender in striking distance opens with a hunt some of the time:
	// name the star, then pay. Falls through to ordinary shopping when no
	// partner has one to sell or no package clears.
	if (
		initPosture.strikingDistance &&
		Math.random() < (initPosture.starGap ? HUNT_CHANCE_STAR_GAP : HUNT_CHANCE)
	) {
		const sellers = shortlistPartners(
			others.filter((t) => postures.get(t)?.shoppableStar),
			initPosture,
			postures,
			MARKET_CANDIDATES,
		);
		const hunt = await huntStar({
			initiator,
			initPosture,
			players,
			candidates: sellers,
			ctx,
		});
		if (hunt) {
			const finalTids: [number, number] = [
				hunt.teams[0].tid,
				hunt.teams[1].tid,
			];
			await processTrade(
				finalTids,
				[hunt.teams[0].pids, hunt.teams[1].pids],
				[hunt.teams[0].dpids, hunt.teams[1].dpids],
				{
					initiatorTid: initiator,
					tiers: [
						postures.get(hunt.teams[0].tid)?.tier ?? "?",
						postures.get(hunt.teams[1].tid)?.tier ?? "?",
					],
					dv: Math.round(hunt.dv2 * 10) / 10,
					motivation: "star-hunt",
				},
			);
			return finalTids;
		}
	}

	// What the initiator brings to market (a shopped vet/star, a walk-year dump,
	// or a buyer's pick/spare depth chasing talent) - the SAME offer is floated to
	// every candidate below.
	const seed = await buildSeed(
		initiator,
		initPosture,
		players,
		draftPicks,
		season,
		starOvr,
	);
	if (!seed) {
		return false;
	}

	// The initiator's OWN untouchables (building blocks + just-traded), minus
	// anything it deliberately seeded (a walk-year dump can be a building block).
	const initiatorExcluded = [
		...initPosture.buildingBlockPids,
		...allInitiatorPlayers
			.filter((p) => wasTradedThisSeason(p.transactions, season))
			.map((p) => p.pid),
	].filter((pid) => !seed.pids.includes(pid));

	// Feel out the market: float the seed to a shortlist of fitting teams, collect
	// each one's best offer, and take the one that helps US most (highest dv2
	// within tolerance). This is the market clearing to the best bid - the asset
	// goes to whoever values it most, not to whichever team we happened to call
	// first. Every offer is a real evaluation by BOTH sides' outlooks.
	const candidates = shortlistPartners(
		others,
		initPosture,
		postures,
		MARKET_CANDIDATES,
	);
	let best: { teams: TradeTeams; dv2: number; landsStar: boolean } | null =
		null;
	for (const partner of candidates) {
		const offer = await buildOfferFromPartner({
			initiator,
			initPosture,
			seed,
			initiatorExcluded,
			partner,
			ctx,
		});
		if (offer && (best === null || offer.dv2 > best.dv2)) {
			best = offer;
		}
	}
	if (!best) {
		return false;
	}

	const { teams, dv2, landsStar } = best;
	const finalTids: [number, number] = [teams[0].tid, teams[1].tid];

	// Record WHY this deal happened, for auditing intent against the trade log.
	const motivation = seed.motivatedDump
		? "dump-expiring"
		: seed.starSale
			? "star-sale"
			: landsStar
				? "star-hunt"
				: isSelling(initPosture.tier)
					? "sell"
					: "buy";

	await processTrade(
		finalTids,
		[teams[0].pids, teams[1].pids],
		[teams[0].dpids, teams[1].dpids],
		{
			initiatorTid: initiator,
			tiers: [
				postures.get(teams[0].tid)?.tier ?? "?",
				postures.get(teams[1].tid)?.tier ?? "?",
			],
			dv: Math.round(dv2 * 10) / 10,
			motivation,
		},
	);
	return finalTids;
};

const DEFAULT_NUM_TEAMS = 30;

// A modest baseline bump so strategic deals actually materialize ("somewhat more
// active"), on top of which the deadline ramp fires. Raised when the timeline /
// building-block guards landed: they reject a real share of attempts, so more
// attempts are needed to keep the same volume of (now-coherent) trades.
const ACTIVITY_BUMP = 1.6;

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

	// With the smart front office off, this is the stock algorithm end to end:
	// vanilla attempt volume (no activity bump, no deadline frenzy), random
	// partners, and the plain fairness bound. See legacyAttempt.
	if (!g.get("smartAiFrontOffice")) {
		let numAttempts = Math.floor(float);
		const remainder = float % 1;
		if (remainder > 0 && Math.random() < remainder) {
			numAttempts += 1;
		}
		if (numAttempts <= 0) {
			return;
		}
		const valueChangeCalculator = new ValueChangeCalculator();
		for (let i = 0; i < numAttempts; i++) {
			const tradeHappened = await legacyAttempt(valueChangeCalculator);
			if (tradeHappened) {
				valueChangeCalculator.invalidateCache({ teams: "all" });
			}
		}
		return;
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
	let starOvr: number;
	let starValue: number;
	try {
		const context = await getLeagueTradeContext();
		starOvr = context.starOvr;
		starValue = context.starValue;
		postures = new Map();
		for (const tid of aiTids) {
			postures.set(tid, await getTradePosture(tid, context));
		}
	} catch (error) {
		console.error("betweenAiTeams: posture computation failed", error);
		return;
	}

	// NOTE: the ValueChangeCalculator is deliberately stock BBGM — postures drive
	// WHO trades, WHAT gets offered, and what tolerances/guards apply, but never
	// what an asset is worth.
	const season = g.get("season");
	for (let i = 0; i < numAttempts; i++) {
		const tradeTids = await attempt({
			postures,
			valueChangeCalculator,
			aiTids,
			season,
			starOvr,
			starValue,
		});
		if (tradeTids) {
			// Don't need to recompute draft pick value.
			valueChangeCalculator.invalidateCache({ teams: tradeTids });
		}
	}
};

export default betweenAiTeams;
