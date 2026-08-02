import { PHASE, PLAYER } from "../../../common/constants.ts";
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
				probSkip = stripped
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
		const payroll = await team.getPayroll(t.tid);

		// Order the market by what THIS team should want. Falling back to the
		// league-wide value order keeps old behavior if posture is unavailable.
		let candidates = posture
			? orderBy(
					playersSorted.map((p) => ({
						p,
						score: scoreFreeAgent({
							p: toFaPlayer(p, season),
							posture,
							season,
							minContract,
							daysLeft: daysLeftOrUndefined,
						}),
					})),
					(x) => x.score,
					"desc",
				).map((x) => x.p)
			: playersSorted;

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
