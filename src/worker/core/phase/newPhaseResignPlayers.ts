import { PHASE, PLAYER, POSITION_COUNTS } from "../../../common/constants.ts";
import {
	contractNegotiation,
	draft,
	league,
	player,
	team,
	freeAgents,
} from "../index.ts";
import { idb } from "../../db/index.ts";
import { g, helpers, local, logEvent } from "../../util/index.ts";
import type { Conditions, PhaseReturn } from "../../../common/types.ts";
import { last, orderBy } from "../../../common/utils.ts";
import { getNumPlayersTradedAwayNormalizedAll } from "../player/getNumPlayersTradedAwayNormalized.ts";
import { bySport } from "../../../common/sportFunctions.ts";
import { ValueChangeCalculator } from "../team/ValueChangeCalculator.ts";
import { getHardCap } from "../../util/getHardCap.ts";
import { frontOfficeLog } from "../../util/frontOfficeLog.ts";
import {
	getLeagueTradeContext,
	getTradePosture,
	type TradePosture,
} from "../trade/tradePosture.ts";
import {
	RETENTION_CORE_RANK,
	retentionOverpay,
	shouldLetWalk,
} from "../freeAgents/frontOffice.ts";

export const FREE_AGENCY_DAYS = 30;

const newPhaseResignPlayers = async (
	conditions: Conditions,
): Promise<PhaseReturn> => {
	// In case some weird situation results in games still in the schedule, clear them
	await idb.cache.schedule.clear();

	// Clear any negotiations that still somehow exist, except if it's a re-signing negotiation for the user, because that could be from a prior failed attempt to run this function and we want to keep those guys. (Would rather have phase updates be transactional, but oh well.)
	const existingNegotiations = await idb.cache.negotiations.getAll();
	const userTids = g.get("userTids");
	for (const negotiation of existingNegotiations) {
		if (negotiation.resigning && userTids.includes(negotiation.tid)) {
			continue;
		}

		await idb.cache.negotiations.delete(negotiation.pid);
	}

	const repeatSeasonType = g.get("repeatSeason")?.type;

	// Reset contract demands of current free agents and undrafted players
	// KeyRange only works because PLAYER.UNDRAFTED is -2 and PLAYER.FREE_AGENT is -1
	const existingFreeAgents = await idb.cache.players.indexGetAll(
		"playersByTid",
		PLAYER.FREE_AGENT,
	);
	const undraftedPlayers =
		!repeatSeasonType && !g.get("forceHistoricalRosters")
			? (
					await idb.cache.players.indexGetAll("playersByDraftYearRetiredYear", [
						[g.get("season")],
						[g.get("season"), Infinity],
					])
				).filter((p) => p.tid === PLAYER.UNDRAFTED)
			: [];

	const numPlayersTradedAwayNormalized =
		await getNumPlayersTradedAwayNormalizedAll();
	for (const p of [...existingFreeAgents, ...undraftedPlayers]) {
		player.addToFreeAgents(p, numPlayersTradedAwayNormalized);
		await idb.cache.players.put(p);
	}

	// Re-sign players on user's team, and some AI players
	const players = await idb.cache.players.indexGetAll("playersByTid", [
		0,
		Infinity,
	]);

	// Figure out how many players are needed at each position, beyond who is already signed
	type PositionInfo = Record<
		string,
		{
			count: number;
			maxValue: number;
		}
	>;
	const positionInfoByTid = new Map<number, PositionInfo>();

	if (Object.keys(POSITION_COUNTS).length > 0) {
		for (let tid = 0; tid < g.get("numTeams"); tid++) {
			const positionInfo: PositionInfo = {};
			for (const [pos, count] of Object.entries(POSITION_COUNTS)) {
				positionInfo[pos] = {
					count,
					maxValue: 0,
				};
			}
			positionInfoByTid.set(tid, positionInfo);
		}

		for (const p of players) {
			// Only expiring contracts and hard cap rookies!
			if (p.contract.exp <= g.get("season")) {
				continue;
			}

			const positionInfo = positionInfoByTid.get(p.tid);
			const pos = last(p.ratings).pos;

			if (positionInfo !== undefined && positionInfo[pos] !== undefined) {
				positionInfo[pos].count -= 1;
				if (p.value > positionInfo[pos].maxValue) {
					positionInfo[pos].maxValue = p.value;
				}
			}
		}
	}

	const payrollsByTid = new Map<number, number>();

	// Payrolls are also needed to enforce the secondary hard cap on re-signings,
	// and to stop a team overpaying itself into oblivion under a soft cap - so
	// they are now computed unconditionally. Thirty getPayroll calls once per
	// offseason is nothing next to the posture work above.
	{
		for (let tid = 0; tid < g.get("numTeams"); tid++) {
			const payroll = await team.getPayroll(tid);
			const expiringPayroll = players
				.filter((p) => p.tid === tid && p.contract.exp <= g.get("season"))
				.reduce((total, p) => total + p.contract.amount, 0);
			payrollsByTid.set(tid, payroll - expiringPayroll);
		}
	}

	const expiringPids = orderBy(
		players.filter((p) => p.contract.exp <= g.get("season")),
		[
			"tid",
			(p) => {
				return p.draft.year === g.get("season") ? 1 : -1;
			},
			"value",
		],
		["asc", "desc", "desc"],
	).map((p) => p.pid);

	const expiredRookieContractPids = new Set(
		players
			.filter(
				(p) =>
					p.contract.exp <= g.get("season") &&
					p.contract.rookie &&
					p.draft.year < g.get("season"),
			)
			.map((p) => p.pid),
	);

	await freeAgents.normalizeContractDemands({
		type: "includeExpiringContracts",
	});

	await contractNegotiation.cancelAll();

	// Franchise posture, so a re-signing decision comes from the same plan the
	// team trades on. Best-effort: without it we fall back to the old value-only
	// logic rather than skip re-signings entirely.
	const postures = new Map<number, TradePosture>();
	let starOvrForResign = Infinity;
	// What the market could supply instead of the player being discussed - the
	// benchmark any retention premium has to beat.
	let starterOvrForResign = Infinity;
	let rotationOvrForResign = Infinity;
	try {
		if (!g.get("smartAiFrontOffice")) {
			throw new Error("smart front office disabled");
		}
		const context = await getLeagueTradeContext();
		starOvrForResign = context.starOvr;
		starterOvrForResign = context.starterOvr;
		rotationOvrForResign = context.rotationOvr;
		for (let tid = 0; tid < g.get("numTeams"); tid++) {
			postures.set(tid, await getTradePosture(tid, context));
		}
	} catch (error) {
		if (g.get("smartAiFrontOffice")) {
			console.error("newPhaseResignPlayers: posture computation failed", error);
		}
		postures.clear();
	}

	// Where each player sits on his own team by OVR, 0 being its best. Both
	// re-signing decisions below are team-relative rather than league-relative:
	// "star" is roughly the best player on an AVERAGE team, so the worst clubs
	// have nobody who qualifies and would otherwise liquidate the rotation they
	// have to rebuild around, and never fight to keep anyone.
	const rosterRankByPid = new Map<number, number>();
	if (postures.size > 0) {
		const byTid = new Map<number, typeof players>();
		for (const p of players) {
			const arr = byTid.get(p.tid);
			if (arr) {
				arr.push(p);
			} else {
				byTid.set(p.tid, [p]);
			}
		}
		for (const roster of byTid.values()) {
			const sorted = roster
				.slice()
				.sort((a, b) => last(b.ratings).ovr - last(a.ratings).ovr);
			for (const [rank, p] of sorted.entries()) {
				rosterRankByPid.set(p.pid, rank);
			}
		}
	}

	const valueChangeCalculator = new ValueChangeCalculator();
	for (const pid of expiringPids) {
		// Re-fetch players, because normalizeContractDemands might have changed some objects
		const p = await idb.cache.players.get(pid);
		if (!p) {
			continue;
		}

		if (expiredRookieContractPids.has(p.pid)) {
			p.contract.rookieResign = true;
		}

		const draftPick = p.draft.year === g.get("season");

		if (draftPick && !g.get("draftPickAutoContract")) {
			p.contract.amount /= 2;

			if (p.contract.amount < g.get("minContract")) {
				p.contract.amount = g.get("minContract");
			} else {
				p.contract.amount = helpers.roundContract(p.contract.amount);
			}

			p.contract.rookie = true;
		}

		if (
			g.get("userTids").includes(p.tid) &&
			!local.autoPlayUntil &&
			!g.get("spectator")
		) {
			const tid = p.tid;

			player.addToFreeAgents(p, numPlayersTradedAwayNormalized);

			await idb.cache.players.put(p);
			const info = await contractNegotiation.create(p.pid, true, tid);

			if (typeof info === "string") {
				logEvent(
					{
						type: "refuseToSign",
						text: info,
						pids: [p.pid],
						tids: [tid],
					},
					conditions,
				);
			} else {
				await idb.cache.negotiations.add(info);
			}
		} else {
			let reSignPlayer = true;

			const contract = {
				...p.contract,
			};
			const payroll = payrollsByTid.get(p.tid);

			const positionInfo = positionInfoByTid.get(p.tid);
			const pos = last(p.ratings).pos;

			if (g.get("salaryCapType") === "hard") {
				if (payroll === undefined) {
					throw new Error(
						"Payroll should always be defined if there is a hard cap",
					);
				}
				if (contract.amount + payroll > g.get("salaryCap")) {
					reSignPlayer = false;
				}

				// Don't go beyond roster needs by position
				if (
					bySport({
						baseball: true,
						basketball: false,
						football: true,
						hockey: true,
					}) &&
					positionInfo !== undefined &&
					positionInfo[pos] !== undefined &&
					positionInfo[pos].count <= 0 &&
					positionInfo[pos].maxValue > p.value
				) {
					reSignPlayer = false;
				}

				// Always sign rookies
				if (draftPick) {
					reSignPlayer = true;
				}
			}

			// A team going somewhere shouldn't spend real money to keep a player
			// who will be finished before it is good. This is the re-signing half
			// of the same idea free agency now runs on: a teardown that keeps
			// paying its thirty-somethings ends up neither young nor good.
			// Never applies to a newly drafted rookie or a genuine star.
			const resignPosture = postures.get(p.tid);
			if (reSignPlayer && resignPosture && !draftPick) {
				const walk = shouldLetWalk({
					tier: resignPosture.tier,
					age: g.get("season") - p.born.year,
					amount: contract.amount,
					years: Math.max(1, contract.exp - g.get("season") + 1),
					isStar: last(p.ratings).ovr >= starOvrForResign,
					isCore:
						(rosterRankByPid.get(p.pid) ?? Infinity) < RETENTION_CORE_RANK,
					minContract: g.get("minContract"),
				});
				if (walk) {
					reSignPlayer = false;
				}
			}

			// Secondary hard cap: a bound team can't re-sign a player over it.
			// Newly-drafted rookies are exempt, so a capped team never has to
			// orphan its own draft picks (their rookie-scale deals are cheap).
			const hardCap = getHardCap(p.tid);
			if (
				!draftPick &&
				Number.isFinite(hardCap) &&
				payroll !== undefined &&
				contract.amount + payroll > hardCap
			) {
				reSignPlayer = false;
			}

			if (reSignPlayer) {
				let mood = await player.moodInfo(p, p.tid, {
					contractAmount: p.contract.amount,
				});

				// He does not want to stay. Before accepting that, find out what he
				// would cost - a front office built around this player does not just
				// wave him off at the asking price.
				//
				// Everything above has already decided this team WANTS him, so the
				// only questions left are how far it will go (retentionOverpay) and
				// whether the cap lets it (the ceiling below, which re-applies the
				// same two limits the code above checked against the original
				// figure - raising the offer must not sneak past them).
				if (!mood.willing && resignPosture && !draftPick) {
					const maxMultiplier = retentionOverpay({
						tier: resignPosture.tier,
						rosterRank: rosterRankByPid.get(p.pid) ?? Infinity,
						isStar: last(p.ratings).ovr >= starOvrForResign,
						age: g.get("season") - p.born.year,
						ovr: last(p.ratings).ovr,
						// A starter is replaced from the starter market, a bench player
						// from the bench market.
						replacementOvr:
							(rosterRankByPid.get(p.pid) ?? Infinity) < RETENTION_CORE_RANK
								? starterOvrForResign
								: rotationOvrForResign,
					});

					if (maxMultiplier <= 1) {
						frontOfficeLog(g.get("season"), p.tid, "retention-not-worth-it", {
							pid: p.pid,
							ovr: last(p.ratings).ovr,
							tier: resignPosture.tier,
						});
					}

					let attempts = 0;
					if (maxMultiplier > 1) {
						let ceiling = Number.POSITIVE_INFINITY;
						if (payroll !== undefined) {
							if (g.get("salaryCapType") === "hard") {
								ceiling = Math.min(ceiling, g.get("salaryCap") - payroll);
							}
							if (Number.isFinite(hardCap)) {
								ceiling = Math.min(ceiling, hardCap - payroll);
							}

							// NO TAX-LINE CEILING. An AI team is not burdened by a
							// budget - the salary cap is a rule it has to navigate, the
							// luxury tax is only money, and money is not something it
							// should be talked out of a player by. What still bounds the
							// bidding is MAX_RETENTION_OVERPAY (how far anyone will go
							// past the asking price) plus the hard-cap and salary-cap
							// ceilings above, which are actual rules.
							//
							// Confirmed from the outside since: the AI takes no penalty
							// for the tax anywhere in the game, so a ceiling here would
							// cost basketball to save money that does not exist. The
							// harness measures the tax as a census rather than a bill
							// for the same reason, and CapPosture in
							// trade/tradePosture.ts says why overLuxury is advisory.
						}

						// Overpay relative to what he is ACTUALLY asking this team, which
						// is mood.contractAmount - it already carries the up-to-50%
						// bad-mood premium. Measuring against the raw figure instead was
						// backwards: a "10% overpay" on a player asking 30% over came out
						// as underpaying, so the lever pushed the wrong way for precisely
						// the reluctant players it exists to persuade.
						const askingPrice = Math.max(mood.contractAmount, contract.amount);

						// Walk the offer up rather than jumping straight to the maximum,
						// so a team pays the least that actually gets it done - then
						// always finish at the most it WOULD pay. Without that last rung
						// a team whose ceiling fell between two steps made no offer at
						// all: one player sitting at probWilling 0.966, who would have
						// re-signed for about 3% more, was let go because the ladder
						// started at 10%.
						const ladder = [1.1, 1.2, 1.3, 1.4].filter(
							(step) => step < maxMultiplier,
						);
						ladder.push(maxMultiplier);

						for (const step of ladder) {
							const offer = helpers.bound(
								helpers.roundContract(askingPrice * step),
								g.get("minContract"),
								g.get("maxContract"),
							);
							if (offer > ceiling || offer <= contract.amount) {
								continue;
							}

							attempts += 1;
							const improved = await player.moodInfo(p, p.tid, {
								contractAmount: p.contract.amount,
								offer,
							});
							if (improved.willing) {
								frontOfficeLog(g.get("season"), p.tid, "retention-overpay", {
									pid: p.pid,
									ovr: last(p.ratings).ovr,
									asked: contract.amount,
									paid: offer,
									tier: resignPosture.tier,
								});
								contract.amount = offer;
								p.contract.amount = offer;
								mood = improved;
								break;
							}
						}

						if (!mood.willing) {
							frontOfficeLog(g.get("season"), p.tid, "retention-gave-up", {
								pid: p.pid,
								ovr: last(p.ratings).ovr,
								asked: contract.amount,
								maxMultiplier,
								attempts,
								maxContract: g.get("maxContract"),
								probWilling: mood.probWilling,
								tier: resignPosture.tier,
							});
						}
					}
				}

				// Player must be willing to sign (includes draft picks and first year after expansion, from moodInfo)
				if (!mood.willing) {
					reSignPlayer = false;
				} else {
					// Is team better off without him?
					const dv = await valueChangeCalculator.evaluate({
						tid: p.tid,
						pidsAdd: [],
						pidsRemove: [p.pid],
						dpidsAdd: [],
						dpidsRemove: [],
						tradingPartnerTid: undefined,
					});

					// Skip re-signing some low value players, otherwise teams fill up their rosters too readily
					const skipBadPlayer =
						contract.amount < g.get("minContract") * 2 && Math.random() < 0.5;

					// More randomness if hard cap
					const whatever =
						g.get("salaryCapType") === "hard" ? Math.random() > 0.1 : true;

					if (
						draftPick ||
						(mood.willing && dv < 0 && !skipBadPlayer && whatever)
					) {
						await player.sign(p, p.tid, contract, PHASE.RESIGN_PLAYERS);

						if (positionInfo !== undefined && positionInfo[pos] !== undefined) {
							positionInfo[pos].count -= 1;
							if (p.value > positionInfo[pos].maxValue) {
								positionInfo[pos].maxValue = p.value;
							}
						}

						if (payroll !== undefined) {
							payrollsByTid.set(p.tid, contract.amount + payroll);
						}

						// Need to recompute team value stuff now that a player was signed
						await valueChangeCalculator.invalidateCache({
							teams: [p.tid],
						});
					} else {
						reSignPlayer = false;
					}
				}
			}

			if (!reSignPlayer) {
				player.addToFreeAgents(p, numPlayersTradedAwayNormalized);
			}

			// Delete rookieResign for AI players, since we're done re-signing them. Leave it for user players.
			if (expiredRookieContractPids.has(pid) || p.contract.rookieResign) {
				delete p.contract.rookieResign;
			}

			await idb.cache.players.put(p);
		}
	}

	const draftProspects = await idb.cache.players.indexGetAll(
		"playersByTid",
		PLAYER.UNDRAFTED,
	);

	if (repeatSeasonType === "players") {
		// Bump up age of draft prospects, so they stay the same
		for (const p of draftProspects) {
			p.draft.year += 1;
			p.born.year += 1;
			last(p.ratings).season += 1;
			await player.updateValues(p);
			await idb.cache.players.put(p);
		}
	} else {
		// Bump up future draft classes (not simultaneous so tid updates don't cause race conditions)
		for (const p of draftProspects) {
			if (p.draft.year !== g.get("season") + 1) {
				continue;
			}

			p.ratings[0].fuzz /= Math.sqrt(2);
			await player.develop(p, 0); // Update skills/pot based on fuzz

			await player.updateValues(p);
			await idb.cache.players.put(p);
		}

		for (const p of draftProspects) {
			if (p.draft.year !== g.get("season") + 2) {
				continue;
			}

			p.ratings[0].fuzz /= Math.sqrt(2);
			await player.develop(p, 0); // Update skills/pot based on fuzz

			await player.updateValues(p);
			await idb.cache.players.put(p);
		}

		// Generate a new draft class, while leaving existing players in that draft class in place
		await draft.genPlayers(g.get("season") + 3);
	}

	// Delete any old undrafted players that still somehow exist
	const toRemove = [];
	for (const p of draftProspects) {
		if (p.draft.year <= g.get("season")) {
			toRemove.push(p.pid);
		}
	}
	await player.remove(toRemove);

	// Set daysLeft here because this is "basically" free agency, so some functions based on daysLeft need to treat it that way (such as the trade AI being more reluctant)
	await league.setGameAttributes({
		daysLeft: FREE_AGENCY_DAYS,
	});

	return {
		redirect: {
			url: helpers.leagueUrl(["negotiation"]),
			text: "Re-sign players",
		},
		updateEvents: ["playerMovement"],
	};
};

export default newPhaseResignPlayers;
