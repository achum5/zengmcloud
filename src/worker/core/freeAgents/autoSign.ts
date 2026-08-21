import { DRAFT_BY_TEAM_OVR, PHASE, PLAYER } from "../../../common/constants.ts";
import { player, team } from "../index.ts";
import getBest from "./getBest.ts";
import { idb } from "../../db/index.ts";
import { g, local } from "../../util/index.ts";
import { getHardCap } from "../../util/getHardCap.ts";
import { last, orderBy } from "../../../common/utils.ts";
import { isSport } from "../../../common/sportFunctions.ts";
import { shuffle } from "../../../common/random.ts";
import {
	getLeagueTradeContext,
	getTradePosture,
	type TradePosture,
} from "../trade/tradePosture.ts";
import {
	type CapHold,
	type FaPlayer,
	isPrize,
	planCapHold,
	pursuitScore,
	resolveCapHolds,
	scoreFreeAgent,
} from "./frontOffice.ts";
import type { Player } from "../../../common/types.ts";
import {
	frontOfficeLog,
	frontOfficeLoggingActive,
} from "../../util/frontOfficeLog.ts";

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

/**
 * AI teams sign free agents.
 *
 * Each team (in random order) signs at most one free agent per call, chosen for
 * FIT rather than raw value - and, during the offseason, a team with a credible
 * shot at a marquee free agent will sit on its cap space instead of spending it.
 * See frontOffice.ts for the reasoning; this function just fetches the data and
 * applies the decisions.
 */
const autoSign = async () => {
	const players = await idb.cache.players.indexGetAll(
		"playersByTid",
		PLAYER.FREE_AGENT,
	);

	if (players.length === 0) {
		return;
	}

	// List of free agents, sorted by value
	let playersSorted = orderBy(players, "value", "desc");

	// Randomly order teams
	const teams = await idb.cache.teams.getAll();
	shuffle(teams);

	const season = g.get("season");
	const minContract = g.get("minContract");
	const maxContract = g.get("maxContract");
	const salaryCap = g.get("salaryCap");
	const salaryCapType = g.get("salaryCapType");

	const eligibleTeams = teams.filter((t) => {
		if (t.disabled) {
			return false;
		}
		// Skip the user's team
		return !(
			g.get("userTids").includes(t.tid) &&
			!local.autoPlayUntil &&
			!g.get("spectator")
		);
	});
	if (eligibleTeams.length === 0) {
		return;
	}

	// Franchise posture for every team about to shop. Best-effort: if this fails
	// we fall back to the old value-ordered behavior rather than skip free agency.
	const postures = new Map<number, TradePosture>();
	let starOvr = Infinity;
	// The whole feature is one setting away from vanilla: with it off, no
	// postures are computed and every decision below falls back to the original
	// "sign the best free agent you can afford" behavior.
	const smart = g.get("smartAiFrontOffice");
	try {
		if (!smart) {
			throw new Error("smart front office disabled");
		}
		const context = await getLeagueTradeContext();
		starOvr = context.starOvr;
		for (const t of eligibleTeams) {
			postures.set(t.tid, await getTradePosture(t.tid, context));
		}
	} catch (error) {
		if (smart) {
			console.error("autoSign: posture computation failed", error);
		}
		postures.clear();
	}

	// Outside the free agency phase there is no countdown, so nothing is urgent
	// and fit applies in full.
	const inFreeAgency = g.get("phase") === PHASE.FREE_AGENCY;
	const daysLeftOrUndefined = inFreeAgency ? g.get("daysLeft") : undefined;

	// Cap holds are an OFFSEASON thing. Once the season is running the marquee
	// free agents are long gone, so there is nothing left to save space for and
	// a team sitting out would just play a man short.
	let capHolds = new Map<number, CapHold>();
	if (inFreeAgency && postures.size > 0) {
		const daysLeft = g.get("daysLeft");
		const prizes = playersSorted
			.map((p) => toFaPlayer(p, season))
			.filter((p) => isPrize({ p, starOvr, minContract }));

		if (prizes.length > 0) {
			const wanted = [];
			for (const t of eligibleTeams) {
				const posture = postures.get(t.tid);
				if (!posture) {
					continue;
				}
				const hold = planCapHold({
					posture,
					prizes,
					payroll: await team.getPayroll(t.tid),
					salaryCap,
					salaryCapType,
					daysLeft,
					season,
					minContract,
					maxContract,
				});
				if (hold) {
					const prize = prizes.find((p) => p.pid === hold.pid)!;
					wanted.push({
						tid: t.tid,
						hold,
						score: pursuitScore({
							p: prize,
							posture,
							season,
							minContract,
							maxContract,
						}),
					});
				}
			}
			capHolds = resolveCapHolds(wanted);
		}
	}

	for (const t of eligibleTeams) {
		const posture = postures.get(t.tid);

		const playersOnRoster = await idb.cache.players.indexGetAll(
			"playersByTid",
			t.tid,
		);

		// Needed before the skip roll now, because whether to skip depends on it.
		const payroll = await team.getPayroll(t.tid);

		// A STAR THIS TEAM CAN SIGN OUTRIGHT, at his asking price, today.
		//
		// The teams with that kind of room are almost always rebuilding - and
		// every rule below tells them to pass: the highest skip rates, an age
		// multiplier that cuts a 28-year-old to little more than half, a cap-hold
		// planner that excludes them by tier. In a league where cap space has
		// gone scarce (dead money, a few teams hoarding room), that leaves the
		// only chequebook big enough for a star in the hands of a team
		// instructed not to open it, and he sits unsigned.
		//
		// Real front offices do the opposite: a team with max room signs the
		// best player willing to take its money, whatever the timeline says - he
		// is the trade asset, the mentor, the reason the next star comes - so an
		// affordable star overrides the plan.
		const affordableStar =
			posture !== undefined &&
			playersSorted.some(
				(p2) =>
					last(p2.ratings).ovr >= starOvr &&
					p2.contract.amount > minContract &&
					p2.contract.amount + payroll <= salaryCap,
			);

		// A rebuild is a decision to be bad for a while, not a licence to stop
		// fielding a team. Once a roster is down to the bare minimum the passive
		// tiers have nothing left to strip, and sitting out only compounds it.
		//
		// Without this, teardown was an ABSORBING STATE: a team that fell into it
		// let its veterans walk, skipped 85% of free agency, dumped salary, and so
		// stayed bad enough to be a teardown again next year. Over eight seasons
		// that produced 10-man rosters and a spread of team ovrs from -58 to 74,
		// against -8 to 64 for the same league run by stock BBGM.
		//
		// Kept to the floor itself rather than a band above it. A version using
		// minRosterSize + 2 pushed rosters to 13-15 without improving them, and
		// braking the other way (going passive near the roster limit) was measured
		// too: it left MORE good players unemployed, not fewer, because a team that
		// stops shopping signs nobody at all.
		const stripped = playersOnRoster.length <= g.get("minRosterSize");

		let probSkip;
		if (isSport("basketball")) {
			// A team with a plan acts on it. The old flat 75-90% skip is what made
			// free agency feel like a lottery; now only teams with nothing much to
			// do sit out often, and a team with a real hole moves quickly.
			if (posture) {
				probSkip =
					affordableStar || stripped
						? 0.4
						: posture.tier === "teardown"
							? 0.85
							: posture.tier === "seller"
								? 0.75
								: posture.needs.length > 0
									? 0.4
									: 0.6;
			} else {
				probSkip = t.strategy === "rebuilding" ? 0.9 : 0.75;
			}
		} else {
			probSkip = 0.5;
		}

		// Skip teams sometimes
		if (Math.random() < probSkip) {
			continue;
		}

		// With forceHistoricalRosters, only sign FAs if we have to
		if (
			playersOnRoster.length >= g.get("minRosterSize") &&
			g.get("forceHistoricalRosters")
		) {
			continue;
		}

		// Ignore roster size, will drop bad player if necessary in checkRosterSizes, and getBest won't sign min contract player unless under the roster limit
		// (payroll was fetched above, before the skip roll)

		// Order the market by what THIS team should want. Falling back to the
		// league-wide value order keeps old behavior if posture is unavailable.
		//
		// Only where getBest actually READS the order. In the DRAFT_BY_TEAM_OVR
		// sports it re-sorts by team-ovr improvement anyway, so reordering buys
		// nothing there - and it is actively unsafe, because that branch prunes
		// every player at a position once it has passed a minimum-contract player
		// there, on the stated assumption that the list is in value order. Handing
		// it fit order made an 80-ovr receiver disappear behind a 30-ovr scrub.
		// Position fit is already the thing team ovr measures in those sports.
		let candidates =
			posture && !DRAFT_BY_TEAM_OVR
				? orderBy(
						playersSorted.map((p) => {
							let score = scoreFreeAgent({
								p: toFaPlayer(p, season),
								posture,
								season,
								minContract,
								maxContract,
								daysLeft: daysLeftOrUndefined,
							});
							// Fit decides between comparable players; it never discounts
							// a star this team could sign outright (see affordableStar).
							if (
								last(p.ratings).ovr >= starOvr &&
								p.contract.amount + payroll <= salaryCap
							) {
								score = Math.max(score, p.value);
							}
							return { p, score };
						}),
						(x) => x.score,
						"desc",
					).map((x) => x.p)
				: playersSorted;

		// A FULL ROSTER MAKES EVERY SIGNING A RELEASE. getBest deliberately
		// ignores the roster limit ("will drop bad player if necessary in
		// checkRosterSizes"), which is fine for the occasional clear upgrade -
		// but a fit-driven shopper acting on it every few days signs a 16th man,
		// cuts a guaranteed contract, and repeats next summer. Measured over
		// twenty simulated seasons, AI teams carried nearly triple the
		// released-contract payroll stock BBGM does; this gate trims roughly a
		// contract in six of it. At the limit, a candidate has to be a real
		// upgrade on the player he forces out - a big one if that player has
		// guaranteed years left, a marginal one if he is expiring or on a
		// minimum deal.
		if (
			isSport("basketball") &&
			posture &&
			playersOnRoster.length >= g.get("maxRosterSize")
		) {
			let worstValue = Infinity;
			let cutCostsRealMoney = false;
			for (const rp of playersOnRoster) {
				if (rp.value < worstValue) {
					worstValue = rp.value;
					cutCostsRealMoney =
						rp.contract.exp > season && rp.contract.amount > minContract;
				}
			}
			const margin = cutCostsRealMoney ? 8 : 2;
			candidates = candidates.filter((p2) => p2.value >= worstValue + margin);
		}

		// Holding space for a marquee free agent. The money is earmarked for HIM,
		// so he is exempt; everyone else has to fit under what's left. Minimum
		// deals stay available throughout, which is how a waiting team still fills
		// out its bench (and why holding can't leave it short-handed).
		const hold = capHolds.get(t.tid);
		if (hold) {
			candidates = candidates.filter(
				(p) =>
					p.pid === hold.pid ||
					p.contract.amount <= minContract ||
					payroll + p.contract.amount <= hold.spendCeiling,
			);
		}

		const p = getBest(playersOnRoster, candidates, payroll, getHardCap(t.tid));
		// Only when the smart front office is actually deciding - with it off,
		// several tests hold the log to zero entries as proof the switch is real.
		if (posture && frontOfficeLoggingActive()) {
			// What the market offered this team and what it took, so a long sim can
			// be asked why a 70-ovr free agent went unsigned all summer.
			const bestAvailable = playersSorted[0];
			frontOfficeLog(season, t.tid, p ? "fa-sign" : "fa-pass", {
				tier: posture?.tier,
				payroll,
				capSpace: salaryCap - payroll,
				rosterSize: playersOnRoster.length,
				held: capHolds.get(t.tid)?.pid,
				pid: p?.pid,
				ovr: p ? last(p.ratings).ovr : undefined,
				amount: p?.contract.amount,
				bestOvr: bestAvailable ? last(bestAvailable.ratings).ovr : undefined,
				bestAmount: bestAvailable?.contract.amount,
			});
		}
		if (p) {
			// Remove from list of free agents
			playersSorted = playersSorted.filter((p2) => p2 !== p);

			await player.sign(p, t.tid, p.contract, g.get("phase"));
			await idb.cache.players.put(p);
			await team.rosterAutoSort(t.tid);
		}
	}
};

export default autoSign;
