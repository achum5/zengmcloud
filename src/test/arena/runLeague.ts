import {
	draft,
	expansionDraft,
	freeAgents,
	game,
	league,
	phase,
	player,
	season,
	team,
} from "../../worker/core/index.ts";
import { idb } from "../../worker/db/index.ts";
import { g, helpers } from "../../worker/util/index.ts";
import { PHASE, PLAYER } from "../../common/constants.ts";
import { getDefaultSettings } from "../../worker/views/newLeague.ts";
import { last } from "../../common/utils.ts";
import { defaultGameAttributes } from "../../common/defaultGameAttributes.ts";
import type { Conditions } from "../../common/types.ts";
import {
	playerLine,
	type ValueRatings,
} from "../../worker/core/realisticFrontOffice/strength.ts";
import { ARENA_SETTINGS } from "./settings.ts";

const conditions = {} as Conditions;

// What the end-of-run tables cannot reconstruct: things that only exist at a
// moment in the season (payrolls, cap space, who was a free agent).
export type SeasonSnapshot = {
	season: number;
	// The cap rule as it stood that season - a league's cap can move every year.
	salaryCap: number;
	luxuryPayroll: number;
	// End of the regular season.
	payrolls: number[];
	deadMoney: number[];
	// Trades made with at least a week still to go to the deadline, so the rest
	// went in the final week.
	tradesBeforeDeadlineWeek: number;
	tradesAtDeadline: number;
	// Start of free agency.
	capSpace: number[];
	// Every player whose contract ran out this season, as the re-signing phase
	// opened, and where he was once free agency had run its course.
	expiring: {
		pid: number;
		tid: number;
		value: number;
		tidAfter?: number;
		// His chance of being willing to re-sign with his team, by the mood
		// model, as the re-signing phase opened.
		probWilling: number;
		// Finishing a rookie deal.
		rookie: boolean;
		// What he was then: overall and age - and the deal he re-signed for,
		// if he did (share of the cap, years), to set against what the front
		// office expects him to cost.
		ovr?: number;
		age?: number;
		reSigned?: { amount: number; years: number };
	}[];
	// Everyone in the free agent pool as free agency opened, as the front
	// office's value model sees him then (roleFree, offense, usage, overall),
	// and the deal he got by the end of it - what a team's cap room buys.
	freeAgents: {
		pid: number;
		age: number;
		line: [number, number, number, number];
		tidAfter?: number;
		// As a share of that summer's cap.
		amount?: number;
		years?: number;
	}[];
	// Where each team's own first-round pick landed in this season's draft,
	// 0 for the first, by original team.
	draftOrder: number[];
	// Everyone on a roster as the playoffs began: age, overall and value
	// then, whether his contract ran past the season, and whether he had
	// retired by the draft lottery - to set the front office's odds of a
	// player still playing against the game's.
	retirements: {
		age: number;
		ovr: number;
		value: number;
		underContract: boolean;
		retired?: boolean;
	}[];
};

// A random-players league with the owner's settings, built the way the new
// league screen builds one: no file, default teams, the settings passed in at
// creation.
export const createArenaLeague = async (lid: number) => {
	const settings: Record<string, unknown> =
		helpers.deepCopy(getDefaultSettings());
	for (const [key, value] of Object.entries(ARENA_SETTINGS)) {
		if (Object.hasOwn(settings, key)) {
			settings[key] = value;
		}
	}

	await league.createStream({}, {
		confs: last(defaultGameAttributes.confs).value,
		divs: last(defaultGameAttributes.divs).value,
		fromFile: {
			gameAttributes: undefined,
			hasRookieContracts: true,
			maxGid: undefined,
			startingSeason: undefined,
			teams: undefined,
			version: undefined,
		},
		getLeagueOptions: undefined,
		keptKeys: new Set(),
		lid,
		name: "Arena",
		setLeagueCreationStatus: () => {},
		settings,
		shuffleRosters: false,
		startingSeasonFromInput: undefined,
		teamsFromInput: helpers.addPopRank(helpers.getTeamsDefault()),
		tid: 0,
	} as any);
	await league.loadGameAttributes();
	await league.setGameAttributes({
		...ARENA_SETTINGS,
		// Every team is run by the CPU.
		spectator: true,
		// Faces are drawn for the UI only and are slow to generate.
		realisticFaces: false,
	} as any);
};

const countTrades = async () =>
	(await idb.merged.events.getAll({ season: g.get("season") })).filter(
		(e) => e.type === "trade",
	).length;

const payrollsAndDeadMoney = async () => {
	const payrolls: number[] = [];
	const deadMoney: number[] = [];
	for (let tid = 0; tid < g.get("numTeams"); tid++) {
		payrolls.push(await team.getPayroll(tid));
		const released = await idb.cache.releasedPlayers.indexGetAll(
			"releasedPlayersByTid",
			tid,
		);
		deadMoney.push(released.reduce((sum, rp) => sum + rp.contract.amount, 0));
	}
	return { payrolls, deadMoney };
};

// Auto play, one phase at a time and awaited, which is what league/autoPlay.ts
// does - but driven from here so the harness can look at the league between
// phases. It never sets local.autoPlayUntil, so finalize never re-enters.
export const advanceSeasons = async (
	numSeasons: number,
	options: {
		// Days before the trade deadline to stop at and call onSurvey.
		surveyAt?: number[];
		onSurvey?: (daysToDeadline: number) => Promise<void>;
	} = {},
) => {
	const snapshots: SeasonSnapshot[] = [];
	const targetSeason = g.get("season") + numSeasons;
	let snapshot: SeasonSnapshot | undefined;
	let rostered: number[] = [];

	let guard = 0;
	while (
		!(g.get("season") === targetSeason && g.get("phase") === PHASE.PRESEASON)
	) {
		guard += 1;
		if (guard > numSeasons * 50) {
			throw new Error(
				`Arena stuck at season ${g.get("season")} phase ${g.get("phase")}`,
			);
		}

		const currentPhase = g.get("phase");
		// A league can hold scheduled expansion drafts.
		// Nothing changes phase after one, so this comes first, as in autoPlay.
		if (currentPhase === PHASE.EXPANSION_DRAFT) {
			if (g.get("expansionDraft").phase === "protection") {
				await expansionDraft.start();
			}
			if (g.get("expansionDraft").phase === "draft") {
				await draft.runPicks({ type: "untilEnd" }, conditions);
			}
			continue;
		}

		if (currentPhase === PHASE.PRESEASON) {
			snapshot = {
				season: g.get("season"),
				salaryCap: 0,
				luxuryPayroll: 0,
				payrolls: [],
				deadMoney: [],
				tradesBeforeDeadlineWeek: 0,
				tradesAtDeadline: 0,
				capSpace: [],
				expiring: [],
				freeAgents: [],
				draftOrder: [],
				retirements: [],
			};
			snapshots.push(snapshot);
			await phase.newPhase(PHASE.REGULAR_SEASON, conditions);
		} else if (currentPhase === PHASE.REGULAR_SEASON) {
			const daysToDeadline = await season.getDaysLeftSchedule("tradeDeadline");
			// Stops on the way to the deadline: the start of its final week, and
			// any survey points.
			const stops = [...new Set([7, ...(options.surveyAt ?? [])])]
				.filter((d) => d > 0 && d < daysToDeadline)
				.sort((a, b) => b - a);
			let left = daysToDeadline;
			for (const stop of stops) {
				await game.play(left - stop, conditions);
				left = stop;
				if (stop === 7) {
					snapshot!.tradesBeforeDeadlineWeek = await countTrades();
				}
				if (options.surveyAt?.includes(stop)) {
					await options.onSurvey?.(stop);
				}
			}
			await game.play(await season.getDaysLeftSchedule(), conditions);
			snapshot!.tradesAtDeadline = await countTrades();
		} else if (currentPhase === PHASE.AFTER_TRADE_DEADLINE) {
			await game.play(await season.getDaysLeftSchedule(), conditions);
		} else if (currentPhase === PHASE.PLAYOFFS) {
			Object.assign(snapshot!, await payrollsAndDeadMoney(), {
				salaryCap: g.get("salaryCap"),
				luxuryPayroll: g.get("luxuryPayroll"),
			});
			rostered = (
				await idb.cache.players.indexGetAll("playersByTid", [0, Infinity])
			).map((p) => p.pid);
			snapshot!.retirements = [];
			for (const pid of rostered) {
				const p = (await idb.cache.players.get(pid))!;
				snapshot!.retirements.push({
					age: g.get("season") - p.born.year,
					ovr: last(p.ratings).ovr,
					value: p.value,
					underContract: p.contract.exp > g.get("season"),
				});
			}
			await game.play(100, conditions);
		} else if (currentPhase === PHASE.DRAFT_LOTTERY) {
			for (const [i, pid] of rostered.entries()) {
				const p = await idb.cache.players.get(pid);
				snapshot!.retirements[i]!.retired = !p || p.tid === PLAYER.RETIRED;
			}
			await phase.newPhase(PHASE.DRAFT, conditions);
		} else if (currentPhase === PHASE.DRAFT) {
			// Where each team's own first-round pick landed.
			for (const dp of await idb.cache.draftPicks.indexGetAll(
				"draftPicksBySeason",
				g.get("season"),
			)) {
				if (dp.round === 1 && dp.pick > 0) {
					snapshot!.draftOrder[dp.originalTid] = dp.pick - 1;
				}
			}
			await draft.runPicks({ type: "untilEnd" }, conditions);
			if (g.get("phase") === PHASE.DRAFT) {
				await phase.newPhase(PHASE.AFTER_DRAFT, conditions);
			}
		} else if (currentPhase === PHASE.AFTER_DRAFT) {
			const players = await idb.cache.players.indexGetAll("playersByTid", [
				0,
				Infinity,
			]);
			snapshot!.expiring = [];
			for (const p of players) {
				if (p.contract.exp === g.get("season")) {
					snapshot!.expiring.push({
						pid: p.pid,
						tid: p.tid,
						value: p.value,
						probWilling: (await player.moodInfo(p, p.tid)).probWilling,
						ovr: last(p.ratings).ovr,
						age: g.get("season") - p.born.year,
						rookie: !!p.contract.rookie,
					});
				}
			}
			await phase.newPhase(PHASE.RESIGN_PLAYERS, conditions);
			for (const row of snapshot!.expiring) {
				const p = await idb.cache.players.get(row.pid);
				if (p && p.tid === row.tid && p.contract.exp > g.get("season")) {
					row.reSigned = {
						amount: p.contract.amount / g.get("salaryCap"),
						years: p.contract.exp - g.get("season"),
					};
				}
			}
		} else if (currentPhase === PHASE.RESIGN_PLAYERS) {
			await phase.newPhase(PHASE.FREE_AGENCY, conditions);
			// As a share of that summer's cap.
			for (let tid = 0; tid < g.get("numTeams"); tid++) {
				snapshot!.capSpace.push(
					1 - (await team.getPayroll(tid)) / g.get("salaryCap"),
				);
			}
			for (const p of await idb.cache.players.indexGetAll(
				"playersByTid",
				PLAYER.FREE_AGENT,
			)) {
				const r = last(p.ratings);
				const line = playerLine(r as unknown as ValueRatings);
				snapshot!.freeAgents.push({
					pid: p.pid,
					age: g.get("season") - p.born.year,
					line: [line.roleFree, line.offense, line.usage, r.ovr],
				});
			}
		} else if (currentPhase === PHASE.FREE_AGENCY) {
			const cap = g.get("salaryCap");
			const season0 = g.get("season");
			await freeAgents.play(g.get("daysLeft"), conditions);
			for (const row of snapshot!.freeAgents) {
				const p = await idb.cache.players.get(row.pid);
				if (p && p.tid >= 0) {
					row.tidAfter = p.tid;
					row.amount = p.contract.amount / cap;
					row.years = p.contract.exp - season0;
				}
			}
			if (g.get("phase") === PHASE.FREE_AGENCY) {
				await phase.newPhase(PHASE.PRESEASON, conditions);
			}
			for (const row of snapshot!.expiring) {
				const p = await idb.cache.players.get(row.pid);
				row.tidAfter = p ? p.tid : PLAYER.RETIRED;
			}
		} else {
			throw new Error(`Unexpected phase ${currentPhase}`);
		}
	}

	return snapshots;
};
