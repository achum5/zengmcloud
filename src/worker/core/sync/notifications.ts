import { PHASE, PHASE_TEXT } from "../../../common/constants.ts";
import { helpers } from "../../../common/helpers.ts";
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

// The best performer on a team in a game, by Game Score. Skips players who
// didn't play. Returns undefined if there's no usable box score (e.g. a
// non-basketball league, where Game Score doesn't apply).
const topScorer = (players: any[] | undefined): any => {
	let best: any;
	let bestScore = -Infinity;
	for (const p of players ?? []) {
		if (!p || (typeof p.min === "number" && p.min <= 0)) {
			continue;
		}
		const score = helpers.gameScore(p);
		if (typeof score === "number" && !Number.isNaN(score) && score > bestScore) {
			bestScore = score;
			best = p;
		}
	}
	return best;
};

// "Jayson Tatum: 30 PTS, 8 REB, 11 AST", or undefined if we can't build one.
const statLine = (p: any): string | undefined => {
	if (!p || typeof p.pts !== "number") {
		return undefined;
	}
	const reb = (p.orb ?? 0) + (p.drb ?? 0);
	const ast = p.ast ?? 0;
	return `${p.name}: ${p.pts} PTS, ${reb} REB, ${ast} AST`;
};

// One game from a team's perspective, split into the result line and the two
// top-performer stat lines:
//   result:    "Boston Massacre W vs DET 110-86"
//   statLines: ["Jayson Tatum: 30 PTS, 8 REB, 11 AST",
//               "Cade Cunningham: 28 PTS, 7 REB, 4 AST"]
// includeTeamName puts the team name on the result line.
const gameParts = (
	game: Game,
	tid: number,
	teamName: string,
	teamById: Map<number, { abbrev?: string }>,
	includeTeamName: boolean,
	resultSuffix = "",
): { result: string; statLines: string[] } => {
	const won = game.won.tid === tid;
	const myPts = won ? game.won.pts : game.lost.pts;
	const oppPts = won ? game.lost.pts : game.won.pts;
	const oppTid = won ? game.lost.tid : game.won.tid;
	// teams[0] is the home team in ZenGM.
	const home = game.teams[0]?.tid === tid;
	const oppAbbrev = teamById.get(oppTid)?.abbrev ?? "OPP";

	const result = `${includeTeamName ? `${teamName} ` : ""}${won ? "W" : "L"} ${home ? "vs" : "@"} ${oppAbbrev} ${myPts}-${oppPts}${resultSuffix}`;

	const myTeam = game.teams.find((t: any) => t.tid === tid);
	const oppTeam = game.teams.find((t: any) => t.tid === oppTid);

	const statLines = [
		statLine(topScorer(myTeam?.players)),
		statLine(topScorer(oppTeam?.players)),
	].filter((line): line is string => line !== undefined);

	return { result, statLines };
};

// The multi-game body block: result line followed by its stat lines.
const gameBlock = (
	game: Game,
	tid: number,
	teamName: string,
	teamById: Map<number, { abbrev?: string }>,
): string => {
	const { result, statLines } = gameParts(game, tid, teamName, teamById, false);
	return [result, ...statLines].join("\n");
};

// Show full stat-line detail for at most this many games, so a week/month sim
// doesn't produce an enormous notification.
const MAX_DETAILED_GAMES = 3;

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

		let title: string;
		let body: string;
		if (teamGames.length === 1) {
			// Single game (a day sim): the W/L result IS the title; the top
			// performer's stat line for each team is the body.
			const { result, statLines } = gameParts(
				teamGames[0]!,
				tid,
				teamName,
				teamById,
				true,
				seasonRecord,
			);
			title = result;
			// Fall back to a plain title if there's no usable box score.
			if (statLines.length > 0) {
				body = statLines.join("\n");
			} else {
				title = "Sim complete";
				body = result;
			}
		} else {
			// Multiple games: the record is the title; detailed blocks for the first
			// few games are the body, then a count of any remainder.
			title = `Your ${teamName} went ${wins}-${losses} ${period}${seasonRecord}`;
			const blocks = teamGames
				.slice(0, MAX_DETAILED_GAMES)
				.map((game) => gameBlock(game, tid, teamName, teamById));
			body = blocks.join("\n\n");
			if (teamGames.length > MAX_DETAILED_GAMES) {
				body += `\n…and ${teamGames.length - MAX_DETAILED_GAMES} more.`;
			}
		}

		notifications.push({ title, body, targetTids: [tid] });
	}

	return notifications;
};

// ---- Transaction-style descriptions (trade / signing / draft) ----------------
//
// These read whole-record changesets to narrate a move like a news blurb. For a
// standard 2-team trade the direction is recoverable: an asset's new tid is its
// destination, and (there being two teams) it came from the other one.

// At most this many individual draft-pick notifications from one changeset, so
// a full-draft sim can't fire dozens of pushes.
const MAX_DRAFT_PICK_NOTIFS = 10;

type TeamInfo = { region: string; name: string; abbrev?: string };

const teamsById = async (): Promise<Map<number, TeamInfo>> => {
	const teams = await idb.cache.teams.getAll();
	return new Map(teams.map((t) => [t.tid, t]));
};

const teamLabel = (teamById: Map<number, TeamInfo>, tid: number): string => {
	const t = teamById.get(tid);
	return t ? `${t.region} ${t.name}` : "a team";
};

const playerName = (p: any): string =>
	`${p.firstName ?? ""} ${p.lastName ?? ""}`.trim() || "a player";

const currentRating = (p: any): any =>
	Array.isArray(p.ratings) ? p.ratings[p.ratings.length - 1] : undefined;

const pickLabel = (dp: any): string => {
	const round = typeof dp.round === "number" ? dp.round : 1;
	const season = typeof dp.season === "number" ? dp.season : "future";
	return `a ${season} ${helpers.ordinal(round)}-round pick`;
};

// "X", "X and Y", or "X, Y and Z".
const joinAssets = (assets: string[]): string => {
	if (assets.length === 0) {
		return "cash considerations";
	}
	if (assets.length === 1) {
		return assets[0]!;
	}
	return `${assets.slice(0, -1).join(", ")} and ${assets[assets.length - 1]}`;
};

const changesToValues = (changeset: Changeset, store: string): any[] =>
	changeset.changes
		.filter((change) => change.store === store && change.type === "put")
		.map((change) => (change as { value: any }).value);

// A Shams-style, two-directional trade blurb, or undefined if this isn't a
// recoverable 2-team trade.
const describeTrade = (
	changeset: Changeset,
	teamById: Map<number, TeamInfo>,
): SyncNotification | undefined => {
	const players = changesToValues(changeset, "players");
	const picks = changesToValues(changeset, "draftPicks");

	const tids = new Set<number>();
	for (const p of players) {
		if (typeof p.tid === "number" && p.tid >= 0) {
			tids.add(p.tid);
		}
	}
	for (const dp of picks) {
		if (typeof dp.tid === "number" && dp.tid >= 0) {
			tids.add(dp.tid);
		}
	}
	const teamTids = [...tids];
	if (teamTids.length !== 2) {
		return undefined;
	}

	const [a, b] = teamTids as [number, number];
	const assetsGoingTo = (tid: number): string[] => [
		...players
			.filter((p) => p.tid === tid)
			.map((p) => {
				const ovr = currentRating(p)?.ovr;
				return typeof ovr === "number"
					? `${playerName(p)} (${ovr} ovr)`
					: playerName(p);
			}),
		...picks.filter((dp) => dp.tid === tid).map(pickLabel),
	];

	const aGets = assetsGoingTo(a);
	const bGets = assetsGoingTo(b);
	if (aGets.length === 0 && bGets.length === 0) {
		return undefined;
	}

	let body: string;
	if (aGets.length > 0 && bGets.length > 0) {
		body = `The ${teamLabel(teamById, a)} acquire ${joinAssets(aGets)} from the ${teamLabel(teamById, b)} in exchange for ${joinAssets(bGets)}.`;
	} else if (aGets.length > 0) {
		body = `The ${teamLabel(teamById, a)} acquire ${joinAssets(aGets)} from the ${teamLabel(teamById, b)}.`;
	} else {
		body = `The ${teamLabel(teamById, b)} acquire ${joinAssets(bGets)} from the ${teamLabel(teamById, a)}.`;
	}
	return { title: "Trade", body, targetTids: null };
};

// A free-agent signing blurb with contract terms, or undefined if this isn't a
// single player joining a single team.
const describeSigning = (
	changeset: Changeset,
	teamById: Map<number, TeamInfo>,
): SyncNotification | undefined => {
	// Picks moving means it's trade territory, not a signing.
	if (changesToValues(changeset, "draftPicks").length > 0) {
		return undefined;
	}

	const joining = changesToValues(changeset, "players").filter(
		(p) => typeof p.tid === "number" && p.tid >= 0,
	);
	if (joining.length !== 1) {
		return undefined;
	}

	const p = joining[0];
	if (!p.contract) {
		return undefined;
	}
	const rating = currentRating(p);
	const ovr = rating?.ovr;
	const pos = rating?.pos;
	const years = Math.max(
		1,
		(p.contract.exp ?? g.get("season")) - g.get("season") + 1,
	);
	const totalM = Math.round(((p.contract.amount ?? 0) * years) / 1000);
	const who =
		typeof ovr === "number"
			? `${playerName(p)} (${ovr} ovr${pos ? `, ${pos}` : ""})`
			: playerName(p);
	const body = `The ${teamLabel(teamById, p.tid)} sign ${who} to a ${years}-year, $${totalM}M contract.`;
	return { title: "Signing", body, targetTids: null };
};

// One notification per pick made in this changeset (during the draft).
const buildDraftNotifications = (
	changeset: Changeset,
	teamById: Map<number, TeamInfo>,
): SyncNotification[] => {
	const season = g.get("season");
	const numActiveTeams = g.get("numActiveTeams");

	const picks = changesToValues(changeset, "players")
		.filter(
			(p) =>
				p.draft &&
				p.draft.year === season &&
				p.draft.round >= 1 &&
				p.draft.pick >= 1 &&
				p.tid === p.draft.tid &&
				p.tid >= 0,
		)
		.sort(
			(a, b) => a.draft.round - b.draft.round || a.draft.pick - b.draft.pick,
		);

	const out: SyncNotification[] = [];
	for (const p of picks.slice(0, MAX_DRAFT_PICK_NOTIFS)) {
		const overall = (p.draft.round - 1) * numActiveTeams + p.draft.pick;
		const rating = currentRating(p);
		const ovr = p.draft.ovr ?? rating?.ovr;
		const pot = p.draft.pot ?? rating?.pot;
		const pos = rating?.pos;
		const ratingPart =
			typeof ovr === "number" && typeof pot === "number"
				? ` (${ovr}, ${pot})`
				: "";
		const posPart = pos ? `, ${pos}` : "";
		const collegePart = p.college ? ` from ${p.college}` : "";
		out.push({
			title: "Draft pick",
			body: `With the ${helpers.ordinal(overall)} pick in the ${season} draft, the ${teamLabel(teamById, p.tid)} select ${playerName(p)}${ratingPart}${posPart}${collegePart}.`,
			targetTids: null,
		});
	}
	if (picks.length > MAX_DRAFT_PICK_NOTIFS) {
		out.push({
			title: "Draft",
			body: `…and ${picks.length - MAX_DRAFT_PICK_NOTIFS} more picks.`,
			targetTids: null,
		});
	}
	return out;
};

// Turn a locally-produced changeset into push notifications for the room, or an
// empty array if nothing is worth a ping. Called on the device that made the
// change (its app is open), so `g` and the cache already reflect the post-action
// state.
//
// Sims (host only) produce a detailed, per-team summary each. A sim that lands
// on a human-decision phase is announced as "your turn" to everyone instead.
// A discrete roster move (trade/signing/cut) touches only a handful of records.
// Anything bigger is a bulk operation (a sim, season progression, a new draft
// class, etc.) and must not be mistaken for a trade.
const MAX_ROSTER_MOVE_CHANGES = 30;

export const buildNotifications = async (
	label: string,
	changeset: Changeset,
	{ isHost, authorName }: { isHost: boolean; authorName: string },
): Promise<SyncNotification[]> => {
	// Detect a sim by CONTENT, not just the label: only simulating games writes
	// `games` records, and sims arrive via several actions (playMenu.day, but also
	// actions.simToGame, live games, etc.). Relying on the label alone let a sim
	// slip through to the trade check below - and since a sim re-writes players on
	// every team, that mislabeled it "trade completed".
	const hasGames = changeset.changes.some(
		(change) => change.store === "games" && change.type === "put",
	);
	const isSim = hasGames || label.startsWith("playMenu.");

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

	// Draft picks made this changeset (announced like a broadcast). Checked before
	// the phase-change branch so picks during the draft are narrated per pick.
	if (g.get("phase") === PHASE.DRAFT) {
		const draftNotifications = buildDraftNotifications(
			changeset,
			await teamsById(),
		);
		if (draftNotifications.length > 0) {
			return draftNotifications;
		}
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

	// Only classify small changesets as trades/signings/roster moves. A bulk
	// change with no games and no phase shift (e.g. end-of-season player
	// progression) would otherwise trip the "players on 2+ teams" heuristic.
	if (changeset.changes.length <= MAX_ROSTER_MOVE_CHANGES) {
		const teamById = await teamsById();

		const trade = describeTrade(changeset, teamById);
		if (trade) {
			return [trade];
		}

		const signing = describeSigning(changeset, teamById);
		if (signing) {
			return [signing];
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
	}

	return [];
};
