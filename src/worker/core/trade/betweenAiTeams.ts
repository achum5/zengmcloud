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
	isSelling,
	isStarAcquisition,
	MAX_ASSETS_PER_SIDE,
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
	// A genuine star's OVR bar (from the league context), so an acquisition can be
	// recognized as a blockbuster.
	starOvr: number;
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
const buildSeed = async (
	initiator: number,
	posture: TradePosture,
	players: Player[],
	draftPicks: { dpid: number }[],
	season: number,
	starOvr: number,
): Promise<
	| { pids: number[]; dpids: number[]; motivatedDump: boolean; starSale: boolean }
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
		if (starsOnBlock.length > 0 && Math.random() < 0.6) {
			p = starsOnBlock[0];
		} else {
			const pool = shopPlayers.length > 0 ? shopPlayers : players;
			p = choice(pool, (pp) => Math.max(0.01, pp.value));
		}
		if (p) {
			pids.push(p.pid);
			starSale = last(p.ratings).ovr >= starOvr;
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
		for (const pid of side.incoming) {
			const p = await idb.cache.players.get(pid);
			if (!p) {
				continue;
			}
			bestReceived = Math.max(bestReceived, p.value);
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
		for (const pid of side.outgoing) {
			const p = await idb.cache.players.get(pid);
			if (p) {
				bestGiven = Math.max(bestGiven, p.value);
			}
		}
		if (
			contenderDowngradesBest({
				acquirerTier: tier,
				bestGivenValue: bestGiven,
				bestReceivedValue: bestReceived,
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
	const { postures, valueChangeCalculator, aiTids, season, starOvr } = ctx;
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
		if (!pp) {
			return 1;
		}
		let w = partnerWeight(initPosture.tier, pp.tier);
		// A star-hunting contender seeks out the teams that actually have a star
		// they'd move — that's where blockbusters come from.
		if (initPosture.tier === "allIn" && initPosture.starGap && pp.shoppableStar) {
			w *= 3;
		}
		// And a seller with a star on the block courts the win-now teams that
		// want him — aggressive sellers sell to aggressive buyers.
		if (
			isSelling(initPosture.tier) &&
			initPosture.shoppableStar &&
			(pp.tier === "allIn" || pp.tier === "buyer")
		) {
			w *= 2;
		}
		return w;
	});

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

	// Hard protection, applied to BOTH sides of the table. Building blocks were
	// only ever respected when a team was picking its own offer — as the RESPONDER,
	// makeItWork could strip a rebuilder's 24yo cornerstone as long as the value
	// math cleared, which is exactly the trade a rebuild exists to refuse. Same-
	// season ping-pong is excluded here too (makeItWork re-reads rosters from the
	// DB, so pool filtering alone can't enforce either rule). A seeded pid stays
	// tradable: seeding a protected expiring player IS the deliberate exception
	// (dumping a walk-year guy who won't re-sign).
	const partnerPosture = postures.get(partner);
	const partnerPlayers = await idb.cache.players.indexGetAll(
		"playersByTid",
		partner,
	);
	const initiatorExcluded = [
		...initPosture.buildingBlockPids,
		...allInitiatorPlayers
			.filter((p) => wasTradedThisSeason(p.transactions, season))
			.map((p) => p.pid),
	].filter((pid) => !seed.pids.includes(pid));
	const partnerExcluded = [
		...(partnerPosture?.buildingBlockPids ?? []),
		...partnerPlayers
			.filter((p) => wasTradedThisSeason(p.transactions, season))
			.map((p) => p.pid),
	];

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
			pids: [],
			pidsExcluded: partnerExcluded,
			dpids: [],
			dpidsExcluded: [],
		},
	];

	// A win-now contender hunting talent is allowed to assemble a much bigger
	// package, so a genuine star (which takes a stack of first-rounders to pry
	// loose) can actually come together instead of dying at the ceiling. Because
	// makeItWork stops at the minimal deal that clears, this ONLY enlarges the deals
	// that truly need it (stars) — ordinary deals still stop early.
	const chasingTalent =
		!isSelling(initPosture.tier) && initPosture.lookingFor.bestCurrentPlayers;
	const maxAssetsToAdd = chasingTalent
		? BLOCKBUSTER_MAX_ASSETS
		: NORMAL_MAX_ASSETS;

	// makeItWork fleshes out the deal until the PARTNER accepts (dv > 0 for it),
	// pulling the assets the initiator's lookingFor describes.
	const teams = await makeItWork(teams0, {
		holdUserConstant: false,
		maxAssetsToAdd,
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

	// Realism cap on package size: sub-average players are ~free under the value
	// curve, so without this a whole bench can ride along on one side of an
	// otherwise-fair deal. Six pieces a side fits every legit blockbuster.
	if (
		teams[0].pids.length + teams[0].dpids.length > MAX_ASSETS_PER_SIDE ||
		teams[1].pids.length + teams[1].dpids.length > MAX_ASSETS_PER_SIDE
	) {
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

	// Timeline fit: a rebuilder doesn't buy veterans (unless paid in picks to
	// absorb them), a contender doesn't swap its best player for a worse one.
	if (await violatesTimeline(teams, postures, season)) {
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

	// How lopsided (against itself) a deal the initiator will accept. The
	// valuation itself is stock BBGM — strategy expresses ONLY through these
	// tolerances (and the seeding/partner/guards above), never by warping what
	// assets are worth. Wider tolerance for: a contender paying a premium to
	// land a genuine star, a seller working to move its star to a win-now team,
	// and a walk-year dump (widest — he's gone for nothing otherwise).
	const landsStar = isStarAcquisition({
		bestReceivedOvr: await maxOvr(teams[1].pids),
		acquirerTier: initPosture.tier,
		starOvr,
	});
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
		return false;
	}

	const finalTids: [number, number] = [teams[0].tid, teams[1].tid];

	// Record WHY this deal happened, so trade history can be audited against the
	// AI's actual intent instead of reverse-engineered from records.
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
	try {
		const context = await getLeagueTradeContext();
		starOvr = context.starOvr;
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
		});
		if (tradeTids) {
			// Don't need to recompute draft pick value.
			valueChangeCalculator.invalidateCache({ teams: tradeTids });
		}
	}
};

export default betweenAiTeams;
