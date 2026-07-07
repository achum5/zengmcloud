import { PHASE, PHASE_TEXT } from "../../../common/constants.ts";
import { g } from "../../util/index.ts";
import { idb } from "../../db/index.ts";
import type { Game } from "../../../common/types.ts";
import type { Changeset } from "./changeset.ts";

// A push notification to fan out to the OTHER devices in the league room. The
// acting device (the one whose app is open, that just made the change) writes
// this to Firestore; a Cloud Function delivers it to everyone else's phones.
export type SyncNotification = {
	title: string;
	body: string;
	// Which teams this is relevant to (their managing devices get pinged). null
	// means everyone in the room. Sim summaries are per-team (each GM gets their
	// own team's results); trades/roster moves/phase changes go to everyone.
	targetTids: number[] | null;
};

// Phases that need a human to act (draft, re-signing, etc.) - the inverse of the
// phases the auto-play scheduler can advance on its own. Reaching one of these
// is the "it's your turn" signal.
const HUMAN_PHASES = new Set<number>([
	PHASE.DRAFT_LOTTERY,
	PHASE.DRAFT,
	PHASE.AFTER_DRAFT,
	PHASE.RESIGN_PLAYERS,
	PHASE.EXPANSION_DRAFT,
	PHASE.FANTASY_DRAFT,
]);

const phaseText = (phase: number): string =>
	(PHASE_TEXT as Record<string, string>)[String(phase)] ?? "a new phase";

// How the sim was triggered maps to a human description of the span.
const simPeriod = (label: string): string => {
	if (label === "playMenu.day") {
		return "today";
	}
	if (label === "playMenu.week") {
		return "this week";
	}
	if (label === "playMenu.month") {
		return "this month";
	}
	return "recently";
};

// If this changeset advanced the game phase, return the new phase number.
const newPhaseFromChangeset = (changeset: Changeset): number | undefined => {
	for (const change of changeset.changes) {
		if (
			change.store === "gameAttributes" &&
			change.id === "phase" &&
			change.type === "put"
		) {
			const value = (change.value as { value?: unknown })?.value;
			if (typeof value === "number") {
				return value;
			}
		}
	}
	return undefined;
};

// Distinct real teams (tid >= 0) that received a player in this changeset. Two
// or more usually means a trade; one means a signing/claim.
const receivingTeams = (changeset: Changeset): number[] => {
	const tids = new Set<number>();
	for (const change of changeset.changes) {
		if (change.store === "players" && change.type === "put") {
			const tid = (change.value as { tid?: unknown })?.tid;
			if (typeof tid === "number" && tid >= 0) {
				tids.add(tid);
			}
		}
	}
	return [...tids];
};

// True if the changeset touches roster state at all (signings, cuts, trades,
// draft picks). Deliberately broad - for a friend group, an occasional extra
// ping is better than a missed move.
const isRosterChange = (changeset: Changeset): boolean =>
	changeset.changes.some(
		(change) =>
			change.store === "players" ||
			change.store === "releasedPlayers" ||
			change.store === "draftPicks",
	);

// The completed games this sim added, for the current season.
const simGames = (changeset: Changeset, season: number): Game[] => {
	const games: Game[] = [];
	for (const change of changeset.changes) {
		if (change.store === "games" && change.type === "put") {
			const game = change.value as Game;
			if (game && game.won && game.lost && game.season === season) {
				games.push(game);
			}
		}
	}
	return games;
};

// One game from a team's perspective, e.g. "W vs BOS 110-105" or "L @ GSW 98-102".
const gameLine = (
	game: Game,
	tid: number,
	teamById: Map<number, { abbrev?: string }>,
): string => {
	const won = game.won.tid === tid;
	const myPts = won ? game.won.pts : game.lost.pts;
	const oppPts = won ? game.lost.pts : game.won.pts;
	const oppTid = won ? game.lost.tid : game.won.tid;
	// teams[0] is the home team in ZenGM.
	const home = game.teams[0]?.tid === tid;
	const oppAbbrev = teamById.get(oppTid)?.abbrev ?? "OPP";
	return `${won ? "W" : "L"} ${home ? "vs " : "@ "}${oppAbbrev} ${myPts}-${oppPts}`;
};

// Build one detailed notification per managed team, each targeted so a GM only
// gets their own team's results. Reads current cache state (post-sim truth) for
// team names and season records.
const buildSimNotifications = async (
	label: string,
	changeset: Changeset,
): Promise<SyncNotification[]> => {
	const period = simPeriod(label);
	const phase = g.get("phase");
	const season = g.get("season");
	const userTids = g.get("userTids");

	const games = simGames(changeset, season);
	const teams = await idb.cache.teams.getAll();
	const teamById = new Map(teams.map((t) => [t.tid, t]));

	const notifications: SyncNotification[] = [];
	for (const tid of userTids) {
		const team = teamById.get(tid);
		const teamName = team ? `${team.region} ${team.name}` : "your team";

		const teamGames = games
			.filter((game) => game.won.tid === tid || game.lost.tid === tid)
			.sort((a, b) => a.gid - b.gid);

		// No game for this team in this span (e.g. an off day, or a sim through
		// free agency) - still tell them the league moved.
		if (teamGames.length === 0) {
			notifications.push({
				title: "Sim complete",
				body: `The host advanced the league (${phaseText(phase)}). No game for your ${teamName} ${period}.`,
				targetTids: [tid],
			});
			continue;
		}

		const wins = teamGames.filter((game) => game.won.tid === tid).length;
		const losses = teamGames.length - wins;

		// Best-effort season record ("now 25-12").
		let seasonRecord = "";
		try {
			const teamSeason = await idb.cache.teamSeasons.indexGet(
				"teamSeasonsBySeasonTid",
				[season, tid],
			);
			if (teamSeason) {
				seasonRecord = ` (now ${teamSeason.won}-${teamSeason.lost})`;
			}
		} catch {
			// Ignore - the record is a nicety, not essential.
		}

		let body = `Your ${teamName} went ${wins}-${losses} ${period}${seasonRecord}`;
		// Only list individual scores when there are few games, so the notification
		// stays readable.
		if (teamGames.length <= 5) {
			body += `: ${teamGames.map((game) => gameLine(game, tid, teamById)).join(", ")}.`;
		} else {
			body += ".";
		}

		notifications.push({ title: "Sim complete", body, targetTids: [tid] });
	}

	return notifications;
};

// Turn a locally-produced changeset into push notifications for the room, or an
// empty array if nothing is worth a ping. Called on the device that made the
// change (its app is open), so `g` and the cache already reflect the post-action
// state.
//
// Sims (host only) produce a detailed, per-team summary each. A sim that lands
// on a human-decision phase is announced as "your turn" to everyone instead.
export const buildNotifications = async (
	label: string,
	changeset: Changeset,
	{ isHost, authorName }: { isHost: boolean; authorName: string },
): Promise<SyncNotification[]> => {
	const isSim = label.startsWith("playMenu.");
	const newPhase = newPhaseFromChangeset(changeset);
	const enteredHumanPhase = newPhase !== undefined && HUMAN_PHASES.has(newPhase);

	if (isSim) {
		// Non-host devices shouldn't be simming; if they somehow do, stay quiet so
		// the room doesn't get duplicate sim announcements.
		if (!isHost) {
			return [];
		}
		// Reaching the draft / re-signing is the salient thing - tell everyone.
		if (enteredHumanPhase) {
			return [
				{
					title: "Your league needs you",
					body: `The host reached ${phaseText(newPhase!)} — your input is needed.`,
					targetTids: null,
				},
			];
		}
		return buildSimNotifications(label, changeset);
	}

	// A manual phase advance (not via a sim) that reaches a human-decision phase.
	if (enteredHumanPhase) {
		return [
			{
				title: "Your league needs you",
				body: `New phase: ${phaseText(newPhase!)}.`,
				targetTids: null,
			},
		];
	}

	const teams = receivingTeams(changeset);
	if (teams.length >= 2) {
		return [
			{
				title: "Trade completed",
				body: `${authorName} completed a trade.`,
				targetTids: null,
			},
		];
	}

	if (isRosterChange(changeset)) {
		return [
			{
				title: "Roster move",
				body: `${authorName} made a roster move.`,
				targetTids: null,
			},
		];
	}

	return [];
};
