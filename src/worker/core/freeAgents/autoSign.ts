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
	capHoldTarget,
	type FaPlayer,
	findBargain,
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
import { signingYears } from "./frontOffice.ts";
import { cutOrder } from "../team/rosterCuts.ts";

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
	// The "someone wants him" line - see signingYears.
	let rotationOvr = Number.NEGATIVE_INFINITY;
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
		rotationOvr = context.rotationOvr;
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

	// Games are being played, so a hole in the rotation is a hole tonight.
	const inSeason =
		g.get("phase") === PHASE.REGULAR_SEASON ||
		g.get("phase") === PHASE.AFTER_TRADE_DEADLINE;
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
			// Would he actually come HERE? A cap hold is an offseason spent not
			// signing anyone, so it has to be planned against the player's real
			// stance toward this team rather than his rating. moodInfo is
			// deterministic per player and team, so every device in a shared
			// league plans the same way. Only prizes are priced this way - a
			// handful of players - and the expensive part of the mood system is
			// cached for the season (local.minFractionDiffs).
			const prizePlayers = new Map(
				playersSorted.map((p) => [p.pid, p] as const),
			);
			const wanted = [];
			// Teams that could not have fit ANY prize, kept apart from the ones
			// that could and decided against it - the two look identical in a
			// census and mean completely different things.
			const noRoomForAnyone = new Set<number>();
			for (const t of eligibleTeams) {
				const posture = postures.get(t.tid);
				if (!posture) {
					continue;
				}
				const payrollNow = await team.getPayroll(t.tid);

				// Mood is only worth reading for the prizes this team could fit
				// under the cap in the first place. Measured over twenty seasons,
				// three quarters of the prizes a team looked at were unaffordable,
				// so asking every one of them how it felt about every team was
				// mostly work thrown away.
				const prizesForTeam: typeof prizes = [];
				for (const prize of prizes) {
					if (payrollNow + capHoldTarget(prize.amount) > salaryCap) {
						continue;
					}
					const full = prizePlayers.get(prize.pid);
					let probWilling: number | undefined;
					if (full) {
						try {
							probWilling = (await player.moodInfo(full, t.tid)).probWilling;
						} catch (error) {
							// A mood the game cannot compute (an old league file with no
							// expense history, say) is not a reason to stop signing
							// players. Planning falls back to fit alone, which is what
							// it did before mood was consulted at all.
							console.error("Could not read a free agent's mood", error);
						}
					}
					prizesForTeam.push({ ...prize, probWilling });
				}
				if (prizesForTeam.length === 0) {
					noRoomForAnyone.add(t.tid);
				}
				const hold = planCapHold({
					posture,
					prizes: prizesForTeam,
					payroll: payrollNow,
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

			if (frontOfficeLoggingActive()) {
				// What each team decided to do with its cap space, and why -
				// planning around a player who was never coming is invisible in a
				// box score and expensive in an offseason.
				for (const t of eligibleTeams) {
					const posture = postures.get(t.tid);
					if (!posture || posture.tier === "teardown") {
						continue;
					}
					const w = wanted.find((x) => x.tid === t.tid);
					if (!w) {
						frontOfficeLog(
							season,
							t.tid,
							noRoomForAnyone.has(t.tid)
								? "hold-no-room"
								: "hold-none-worth-waiting-for",
							{
								prizes: prizes.length,
								tier: posture.tier,
							},
						);
					} else if (capHolds.has(t.tid)) {
						frontOfficeLog(season, t.tid, "hold-open", {
							pid: w.hold.pid,
							score: Math.round(w.score * 100) / 100,
							tier: posture.tier,
						});
					} else {
						frontOfficeLog(season, t.tid, "hold-outbid-by-rivals", {
							pid: w.hold.pid,
							tier: posture.tier,
						});
					}
				}
			}
		}
	}

	for (const t of eligibleTeams) {
		const posture = postures.get(t.tid);

		const playersOnRoster = await idb.cache.players.indexGetAll(
			"playersByTid",
			t.tid,
		);
		const healthyOnRoster = playersOnRoster.filter(
			(p2) => p2.injury.gamesRemaining === 0,
		).length;

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
		// Head count, deliberately, even in season. Reading this in HEALTHY
		// bodies instead - so a team carrying five injured men shops as hard as
		// one genuinely down to the floor - was built and measured over five
		// seeds of eight real seasons. It does what it says (short-handed
		// team-days fell on every seed, 56 to 49) and still made leagues worse:
		// team ovr down 1.6 with four of five seeds negative, and a champion
		// fewer across the run. The reason is the tier it fires hardest for. A
		// teardown's skip rate drops from 85% to 40%, so a rebuilding team with
		// an injury crisis goes shopping, signs veterans, wins games it did not
		// want to win, and comes out of the rebuild worse. findBargain already
		// covers the real hole - when a short-handed team DOES shop, it now
		// signs a body - and it turns out that is the whole fix.
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
			// ASK WHO WOULD ACTUALLY GO, rather than guessing.
			//
			// This used to take the lowest raw value on the roster and price the
			// cut off him. checkRosterSizes does not release that player: it
			// releases in cutOrder, which is value leaned by age and position
			// scarcity and then by what the cut COSTS - and cutCostLean
			// deliberately protects a guaranteed multi-year deal, so the man it
			// actually lets go is usually a cheaper one somewhere else on the
			// list. Two places answering the same question differently, and the
			// gate was pricing a release that was never going to happen.
			//
			// It mattered in the direction that costs money. When the raw-worst
			// player happened to be on a minimum the gate read "cutting is free",
			// dropped its margin to 2, and waved through a signing whose real
			// victim had guaranteed years left. Against stock BBGM this front
			// office released 35% more players and stranded 55% more money, at
			// 3.9M a release against stock's 3.3M - it was not cutting more
			// minimum men, it was cutting better-paid ones.
			//
			// Six seeds of twelve real seasons say this alone takes stars left
			// unsigned from 4.8 a run to 3.2, down on five seeds of six, and
			// costs nothing measurable anywhere else - top five 75.3 to 75.0,
			// rostered talent flat.
			//
			// MOVING THE VALUE BAR HERE TOO WAS MEASURED AND NOT TAKEN. Using
			// wouldGo.value instead of the raw minimum reads as the same fix and
			// is a different one: cutOrder's first man is not the lowest value on
			// the roster, so the bar rises and the gate tightens everywhere. That
			// buys 8% off the dead money and another half point off unsigned
			// stars, and it costs 2.2 points of the top five on five seeds of six
			// - about half of what this front office is up on stock. Worth
			// knowing about, not worth taking without somebody deciding they want
			// a flatter league.
			const wouldGo = cutOrder(
				playersOnRoster.map((rp) => ({
					pid: rp.pid,
					value: rp.value,
					age: season - rp.born.year,
					pos: last(rp.ratings).pos,
					contractAmount: rp.contract.amount,
					contractExp: rp.contract.exp,
				})),
				posture.tier,
				{ season, salaryCap },
			)[0];
			// The VALUE bar stays on the raw worst man. Moving it to wouldGo as
			// well was measured and it is a different change wearing the same
			// coat: cutOrder's first man is not the lowest value on the roster
			// (the lean and the cut cost move him), so the bar rises, the gate
			// tightens everywhere, and over six seeds the top five lost 2.2
			// points - about half of what this front office is up on stock. The
			// defect found here was the CONTRACT being priced off a player who
			// was never going to be released; that is what is fixed.
			let worstValue = Infinity;
			for (const rp of playersOnRoster) {
				if (rp.value < worstValue) {
					worstValue = rp.value;
				}
			}
			const cutCostsRealMoney =
				wouldGo !== undefined &&
				wouldGo.contractExp > season &&
				wouldGo.contractAmount > minContract;
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

		let p = getBest(playersOnRoster, candidates, payroll, getHardCap(t.tid));

		// Nothing above the minimum was worth it - but that is not the same as
		// nothing being worth it. getBest will not take a minimum-contract player
		// unless the roster is two men short of full, which in practice means
		// never, so good players sit in the pool all summer asking for the least
		// money in the game. A front office with an open seat takes one when he
		// is better than the man in the last seat. See findBargain.
		//
		// A team holding cap space is not excluded: the hold already exempts
		// minimum deals for exactly this reason, so waiting for a star never
		// leaves a roster short-handed.
		let bargain: FaPlayer | undefined;
		if (!p && posture && isSport("basketball")) {
			const minimumFas = candidates
				.filter((p2) => p2.contract.amount <= minContract)
				.map((p2) => toFaPlayer(p2, season));
			if (minimumFas.length > 0) {
				let worstRosterValue = Infinity;
				for (const rp of playersOnRoster) {
					worstRosterValue = Math.min(worstRosterValue, rp.value);
				}
				bargain = findBargain({
					posture,
					candidates: minimumFas,
					// An empty roster has no bar to clear, and no worst man to beat.
					worstRosterValue: Number.isFinite(worstRosterValue)
						? worstRosterValue
						: 0,
					rosterSize: playersOnRoster.length,
					maxRosterSize: g.get("maxRosterSize"),
					// Only during the season: out of it there is no game tomorrow,
					// injuries heal before one matters, and a team that fills its
					// last seat with a warm body in July is the churn the roster
					// gate exists to prevent.
					healthyCount: inSeason ? healthyOnRoster : undefined,
					minRosterSize: g.get("minRosterSize"),
					season,
					minContract,
					maxContract,
				});
				if (bargain) {
					const bargainPid = bargain.pid;
					p = candidates.find((p2) => p2.pid === bargainPid);
				}
			}
		}
		// Only when the smart front office is actually deciding - with it off,
		// several tests hold the log to zero entries as proof the switch is real.
		if (posture && frontOfficeLoggingActive()) {
			// What the market offered this team and what it took, so a long sim can
			// be asked why a 70-ovr free agent went unsigned all summer.
			const bestAvailable = playersSorted[0];
			frontOfficeLog(
				season,
				t.tid,
				p ? (bargain ? "fa-bargain" : "fa-sign") : "fa-pass",
				{
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
				},
			);
		}
		if (p) {
			// Remove from list of free agents
			playersSorted = playersSorted.filter((p2) => p2 !== p);

			// The plan decides the structure of the deal - see signingYears.
			if (posture && isSport("basketball")) {
				const offset = g.get("phase") <= PHASE.PLAYOFFS ? -1 : 0;
				const askedYears = p.contract.exp - season - offset;
				const years = signingYears({
					tier: posture.tier,
					age: season - p.born.year,
					askedYears,
					amount: p.contract.amount,
					ovr: last(p.ratings).ovr,
					rotationOvr,
					minContract,
					minLength: g.get("minContractLength"),
					maxLength: g.get("maxContractLength"),
				});
				if (years !== askedYears) {
					p.contract.exp = season + years + offset;
				}
			}

			await player.sign(p, t.tid, p.contract, g.get("phase"));
			await idb.cache.players.put(p);
			await team.rosterAutoSort(t.tid);
		}
	}
};

export default autoSign;
