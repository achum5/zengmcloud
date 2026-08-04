import { PHASE, PLAYER } from "../../common/constants.ts";
import type { UpdateEvents } from "../../common/types.ts";
import { draft } from "../core/index.ts";
import { idb } from "../db/index.ts";
import { g, helpers, local } from "../util/index.ts";
import addFirstNameShort from "../util/addFirstNameShort.ts";
import { last, minBy } from "../../common/utils.ts";
import { getDraftTeamsByTid } from "./draftHistory.ts";
import { bySport } from "../../common/sportFunctions.ts";
import { getSyncEngine } from "../core/sync/engineHolder.ts";

const getUserNextPickYear = async () => {
	const userTids = g.get("userTids");

	const draftPicks = (await idb.cache.draftPicks.getAll()).filter(
		(dp) => userTids.includes(dp.tid) && typeof dp.season === "number",
	);

	// This could be the current season, but that's fine because the UI handles that case
	let nextPickYear = minBy(draftPicks, "season")?.season as number | undefined;

	// No picks at all in future drafts, so find what the next one to be generated is
	nextPickYear ??= g.get("season") + g.get("numSeasonsFutureDraftPicks");

	return nextPickYear;
};

const updateDraft = async (inputs: unknown, updateEvents: UpdateEvents) => {
	if (
		updateEvents.includes("firstRun") ||
		updateEvents.includes("playerMovement") ||
		updateEvents.includes("newPhase")
	) {
		const fantasyDraft = g.get("phase") === PHASE.FANTASY_DRAFT;
		const expansionDraft = g.get("expansionDraft");
		let expansionDraftFilteredTeamsMessage: string | undefined;

		let draftPicks = await draft.getOrder();

		// The two "dirty quick fix" mutations below repair corrupted single-player
		// saves, but they must NEVER run in a synced league: a view load is not a
		// cloud-tracked action, so their writes are invisible to the sync log -
		// each device would silently repair (or, worse, run a whole draft lottery)
		// on its own, forking the room. In a synced league the device in charge of
		// simming produces this data through real, synced actions.
		const canMutateFromView = getSyncEngine() === undefined;

		// DIRTY QUICK FIX FOR sometimes there are twice as many draft picks as needed, and one set has all pick 0
		if (
			canMutateFromView &&
			!fantasyDraft &&
			g.get("phase") !== PHASE.EXPANSION_DRAFT &&
			draftPicks.length > 2 * g.get("numActiveTeams")
		) {
			const draftPicks2 = draftPicks.filter((dp) => dp.pick > 0);

			if (draftPicks2.length === 2 * g.get("numActiveTeams")) {
				const toDelete = draftPicks.filter((dp) => dp.pick === 0);

				for (const dp of toDelete) {
					await idb.cache.draftPicks.delete(dp.dpid);
				}

				draftPicks = draftPicks2;
			}
		}

		// DIRTY QUICK FIX FOR https://github.com/zengm-games/zengm/issues/246
		// Not sure why this is needed! Maybe related to lottery running before the phase change?
		//
		// Gated to phase >= DRAFT: during the DRAFT_LOTTERY phase, pick === 0 is
		// the NORMAL state of every current-season pick (the lottery simply hasn't
		// been held yet). Without the phase gate, merely LOADING this page ran the
		// entire draft lottery silently - no reveal, no events - and in a synced
		// league that run never reached other devices, forking the room (each
		// device that visited this page held its own private lottery, compounding
		// COLA penalties on every run).
		if (
			canMutateFromView &&
			g.get("phase") >= PHASE.DRAFT &&
			draftPicks.some((dp) => dp.pick === 0) &&
			g.get("draftType") !== "freeAgents"
		) {
			await draft.genOrder(false);
			draftPicks = await draft.getOrder();
		}

		let drafted: any[];

		if (fantasyDraft) {
			// Fantasy draft results must be rebuilt from SYNCED player data, not the
			// per-device local.fantasyDraftResults — a device that isn't the one
			// simming never receives that local state, so completed picks would be
			// invisible in multiplayer. A fantasy pick leaves the player's tid set
			// plus a draft transaction whose pickNum encodes round/pick
			// (pickNum = pick + (round - 1) * numActiveTeams), so invert it.
			const season = g.get("season");
			const numActiveTeams = g.get("numActiveTeams");
			const rostered = await idb.cache.players.indexGetAll("playersByTid", [
				0,
				Infinity,
			]);
			drafted = [];
			for (const p of rostered) {
				const txn = p.transactions?.findLast(
					(t) =>
						t.type === "draft" &&
						t.phase === PHASE.FANTASY_DRAFT &&
						t.season === season,
				);
				if (!txn || txn.type !== "draft") {
					continue;
				}
				const round = Math.floor((txn.pickNum - 1) / numActiveTeams) + 1;
				const pick = ((txn.pickNum - 1) % numActiveTeams) + 1;
				const { ovr, pot, skills } = last(p.ratings);
				drafted.push({
					...p,
					draft: {
						round,
						pick,
						tid: p.tid,
						year: season,
						originalTid: p.tid,
						ovr,
						pot,
						skills,
						dpid: txn.pickNum,
					},
				});
			}
			drafted.sort(
				(a, b) =>
					100 * a.draft.round +
					a.draft.pick -
					(100 * b.draft.round + b.draft.pick),
			);
		} else if (
			g.get("phase") === PHASE.EXPANSION_DRAFT &&
			expansionDraft.phase === "draft"
		) {
			drafted = local.fantasyDraftResults;
		} else {
			drafted = await idb.cache.players.indexGetAll("playersByTid", [
				0,
				Infinity,
			]);
			drafted = drafted.filter((p) => p.draft.year === g.get("season"));
			drafted.sort(
				(a, b) =>
					100 * a.draft.round +
					a.draft.pick -
					(100 * b.draft.round + b.draft.pick),
			);
		}

		drafted = addFirstNameShort(
			await idb.getCopies.playersPlus(drafted, {
				attrs: [
					"pid",
					"tid",
					"firstName",
					"lastName",
					"age",
					"draft",
					"injury",
					"contract",
					"watch",
					"prevTid",
					"prevAbbrev",
				],
				ratings: ["ovr", "pot", "skills", "pos"],
				stats: ["per", "ewa"],
				season: g.get("season"),
				showRookies: true,
				fuzz: true,
				// Draft Results is the SAME scouting report as the Undrafted list
				// beside it - the pick doesn't change what this year's report said.
				// Without this a player dropped from "49 / 57" to "5 / 6" the instant
				// he was taken, on the same screen, one row over. (A fantasy or
				// expansion draft is unaffected: those players' rows are years past
				// their draft, so the exemption doesn't reach them.)
				prospectSeasonsExact: true,
			}),
		);

		let stats: string[];
		let undrafted: any[];

		if (fantasyDraft) {
			stats = bySport({
				baseball: ["gp", "keyStats", "war"],
				basketball: ["per", "ewa"],
				football: ["gp", "keyStats", "av"],
				hockey: ["gp", "keyStats", "ops", "dps", "ps"],
			});

			// After fantasy draft, tids are reset, so actually the remaining undrafted players are free agents
			const undraftedTID =
				draftPicks.length > 0 ? PLAYER.UNDRAFTED : PLAYER.FREE_AGENT;

			undrafted = await idb.cache.players.indexGetAll(
				"playersByTid",
				undraftedTID,
			);
		} else if (
			g.get("phase") === PHASE.EXPANSION_DRAFT &&
			expansionDraft.phase === "draft"
		) {
			stats = bySport({
				baseball: ["gp", "keyStats", "war"],
				basketball: ["per", "ewa"],
				football: ["gp", "keyStats", "av"],
				hockey: ["gp", "keyStats", "ops", "dps", "ps"],
			});
			undrafted = (
				await idb.cache.players.indexGetAll("playersByTid", [0, Infinity])
			).filter((p) => expansionDraft.availablePids.includes(p.pid));

			if (expansionDraft.numPerTeam !== undefined) {
				// Keep logic in sync with runPicks.ts
				const tidsOverLimit: number[] = [];
				for (const [tidString, numPerTeam] of Object.entries(
					expansionDraft.numPerTeamDrafted,
				)) {
					if (numPerTeam >= expansionDraft.numPerTeam) {
						const tid = Number.parseInt(tidString);
						tidsOverLimit.push(tid);
					}
				}

				if (tidsOverLimit.length > 0) {
					const numPlayersBefore = undrafted.length;
					undrafted = undrafted.filter((p) => !tidsOverLimit.includes(p.tid));
					if (undrafted.length !== numPlayersBefore) {
						const abbrevs = tidsOverLimit
							.map((tid) => helpers.getAbbrev(tid))
							.sort();
						expansionDraftFilteredTeamsMessage = `Players from some teams (${abbrevs.join(
							", ",
						)}) are no longer available to be selected because they have already reached the limit of ${
							expansionDraft.numPerTeam
						} drafted ${helpers.plural("player", expansionDraft.numPerTeam)}.`;
					}
				}
			}
		} else {
			stats = [];
			undrafted = (
				await idb.cache.players.indexGetAll("playersByDraftYearRetiredYear", [
					[g.get("season")],
					[g.get("season"), Infinity],
				])
			).filter((p) => p.tid === PLAYER.UNDRAFTED);

			// DIRTY QUICK FIX FOR v10 db upgrade bug - eventually remove
			// This isn't just for v10 db upgrade! Needed the same fix for http://www.reddit.com/r/BasketballGM/comments/2tf5ya/draft_bug/cnz58m2?context=3 - draft class not always generated with the correct seasons
			// Skipped in synced leagues: a view load is not a cloud-tracked action,
			// so this player write would be invisible to the sync log and diverge
			// this device from the room (see canMutateFromView above).
			if (canMutateFromView) {
				for (const p of undrafted) {
					const season = p.ratings[0].season;

					if (season !== g.get("season") && g.get("phase") === PHASE.DRAFT) {
						console.log("FIXING MESSED UP DRAFT CLASS");
						console.log(season);
						p.ratings[0].season = g.get("season");
						await idb.cache.players.put(p);
					}
				}
			}
		}

		undrafted.sort((a, b) => b.valueFuzz - a.valueFuzz);
		undrafted = addFirstNameShort(
			await idb.getCopies.playersPlus(undrafted, {
				attrs: [
					"pid",
					"firstName",
					"lastName",
					"age",
					"injury",
					"contract",
					"watch",
					"abbrev",
					"tid",
					"valueFuzz",
					"draft",
				],
				ratings: ["ovr", "pot", "skills", "pos"],
				stats,
				season: g.get("season"),
				showNoStats: true,
				showRookies: true,
				fuzz: true,
			}),
		);
		undrafted.sort((a, b) => b.valueFuzz - a.valueFuzz);
		undrafted = undrafted.map((p, i) => ({
			...p,
			rank: i + 1,
		}));

		for (const dp of draftPicks) {
			drafted.push({
				draft: dp,
				pid: -1,
			});
		}

		const userPlayersAll = await idb.cache.players.indexGetAll(
			"playersByTid",
			g.get("userTid"),
		);
		const userPlayers = await idb.getCopies.playersPlus(userPlayersAll, {
			attrs: [],
			ratings: ["pos"],
			stats: [],
			season: g.get("season"),
			showNoStats: true,
			showRookies: true,
		});

		const userNextPickYear = await getUserNextPickYear();

		const teamsByTid = await getDraftTeamsByTid(g.get("season"));

		return {
			challengeNoDraftPicks: g.get("challengeNoDraftPicks"),
			drafted,
			expansionDraftFilteredTeamsMessage,
			fantasyDraft,
			stats,
			teamsByTid,
			undrafted,
			userNextPickYear,
			userPlayers,
		};
	}
};

export default updateDraft;
