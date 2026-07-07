import { PHASE, PHASE_TEXT, PLAYER } from "../../../common/constants.ts";
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
	// Where tapping the notification should go, as a league-RELATIVE path (no
	// leading "/l/{lid}"), e.g. "player/123" or "trade". The recipient's device
	// prepends its own lid, since lid differs per device. Omitted → the app root.
	path?: string;
};

// The page a "your turn" notification for a given phase should open.
const phasePath = (phase: number): string => {
	switch (phase) {
		case PHASE.DRAFT_LOTTERY:
		case PHASE.DRAFT:
		case PHASE.AFTER_DRAFT:
			return "draft";
		case PHASE.RESIGN_PLAYERS:
			return "negotiation";
		case PHASE.EXPANSION_DRAFT:
			return "expansion_draft";
		case PHASE.FANTASY_DRAFT:
			return "fantasy_draft";
		default:
			return "standings";
	}
};

const phaseText = (phase: number): string =>
	(PHASE_TEXT as Record<string, string>)[String(phase)] ?? "a new phase";

const titleCase = (s: string): string =>
	s.replace(/\b\w/g, (c) => c.toUpperCase());

// Phases where games are actually played - the only ones where a team having no
// game in a sim is worth mentioning. In the offseason (free agency, preseason,
// the draft, re-signing) nobody plays, so "no game for your team" is just noise.
const GAME_PHASES = new Set<number>([
	PHASE.REGULAR_SEASON,
	PHASE.AFTER_TRADE_DEADLINE,
	PHASE.PLAYOFFS,
]);

// Minor transitions not worth announcing on their own.
const SKIP_PHASE_ANNOUNCE = new Set<number>([
	PHASE.AFTER_DRAFT,
	PHASE.AFTER_TRADE_DEADLINE,
]);

// The current best available free agents, as "Name (ovr/pot)" lines.
const topFreeAgentsText = async (): Promise<string> => {
	let fas: any[] = [];
	try {
		fas = await idb.cache.players.indexGetAll("playersByTid", PLAYER.FREE_AGENT);
	} catch {
		fas = [];
	}
	const ranked = fas
		.map((p) => {
			const rating = currentRating(p);
			return { name: playerName(p), ovr: rating?.ovr, pot: rating?.pot };
		})
		.filter((x) => typeof x.ovr === "number")
		.sort((a, b) => b.ovr! - a.ovr! || (b.pot ?? 0) - (a.pot ?? 0))
		.slice(0, 5);
	if (ranked.length === 0) {
		return "Free agency is open.";
	}
	return ranked.map((r) => `${r.name} (${r.ovr}/${r.pot})`).join("\n");
};

// A single broadcast notification announcing a phase transition. Free agency
// lists the top available free agents; other phases get a fitting one-liner.
// Returns [] for minor transitions we don't announce.
const buildPhaseChangeNotifications = async (
	phase: number,
): Promise<SyncNotification[]> => {
	if (SKIP_PHASE_ANNOUNCE.has(phase)) {
		return [];
	}
	const season = g.get("season");
	const name = titleCase(phaseText(phase));

	let title = `Advanced to ${season} ${name}!`;
	let body = "";
	const path = phasePath(phase);

	switch (phase) {
		case PHASE.FREE_AGENCY:
			title = `Advanced to ${season} Free Agency!`;
			body = await topFreeAgentsText();
			break;
		case PHASE.DRAFT_LOTTERY:
			body = "The lottery is set — see where the picks landed.";
			break;
		case PHASE.DRAFT:
			body = "The draft is here — make your picks.";
			break;
		case PHASE.RESIGN_PLAYERS:
			body = "Re-sign your players before free agency opens.";
			break;
		case PHASE.PLAYOFFS:
			title = `The ${season} Playoffs are here!`;
			body = "The bracket is set.";
			break;
		case PHASE.PRESEASON:
			body = `The ${season} preseason has begun.`;
			break;
		case PHASE.REGULAR_SEASON:
			title = `The ${season} season is underway!`;
			body = "Games are being played.";
			break;
		default:
			body = "The league advanced.";
	}

	return [{ title, body, targetTids: null, path }];
};

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
		if (
			typeof score === "number" &&
			!Number.isNaN(score) &&
			score > bestScore
		) {
			bestScore = score;
			best = p;
		}
	}
	return best;
};

// The best N performers on a team in a game, by Game Score, best first. Skips
// players who didn't play / have no usable box score.
const topScorers = (players: any[] | undefined, n: number): any[] =>
	(players ?? [])
		.filter((p) => p && !(typeof p.min === "number" && p.min <= 0))
		.map((p) => ({ p, score: helpers.gameScore(p) }))
		.filter(({ score }) => typeof score === "number" && !Number.isNaN(score))
		.sort((a, b) => b.score - a.score)
		.slice(0, n)
		.map(({ p }) => p);

// ESPN-style short team name for a score headline ("Unicorns", "Massacre").
const headlineName = (
	team: { region?: string; name?: string; abbrev?: string } | undefined,
): string => team?.name || team?.region || team?.abbrev || "Team";

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
		const abbrev = team?.abbrev;
		// The team's game log for this season (box score of a specific game is
		// appended below when there's exactly one game).
		const gameLogPath = abbrev ? `game_log/${abbrev}/${season}` : "standings";

		const teamGames = games
			.filter((game) => game.won.tid === tid || game.lost.tid === tid)
			.sort((a, b) => a.gid - b.gid);

		// No game for this team in this span. Only worth mentioning during a
		// game-playing phase (your team had an off day while others played); in the
		// offseason nobody plays, so stay silent - the phase-change notification
		// already covered the advance.
		if (teamGames.length === 0) {
			if (GAME_PHASES.has(phase)) {
				notifications.push({
					title: "Sim complete",
					body: `The host advanced the league (${phaseText(phase)}). No game for your ${teamName} ${period}.`,
					targetTids: [tid],
					path: "standings",
				});
			}
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
		let path = gameLogPath;
		if (teamGames.length === 1) {
			// Single game (a day sim): an ESPN-style final-score headline is the
			// title (winner first), and the top TWO performers from each team - each
			// tagged with its team abbrev - are the body. Deep-link to the box score.
			const game = teamGames[0]!;
			const winner = teamById.get(game.won.tid);
			const loser = teamById.get(game.lost.tid);
			title = `${headlineName(winner)} ${game.won.pts}, ${headlineName(loser)} ${game.lost.pts}`;

			if (abbrev) {
				path = `game_log/${abbrev}/${season}/${game.gid}`;
			}

			const winnerPlayers = game.teams.find(
				(t: any) => t.tid === game.won.tid,
			)?.players;
			const loserPlayers = game.teams.find(
				(t: any) => t.tid === game.lost.tid,
			)?.players;
			const labeled = (ab: string | undefined, p: any): string | undefined => {
				const line = statLine(p);
				return line ? (ab ? `${ab} ${line}` : line) : undefined;
			};
			const lines = [
				...topScorers(winnerPlayers, 2).map((p) => labeled(winner?.abbrev, p)),
				...topScorers(loserPlayers, 2).map((p) => labeled(loser?.abbrev, p)),
			].filter((l): l is string => l !== undefined);

			// Fall back to restating the score if there's no usable box score
			// (e.g. a non-basketball league where Game Score doesn't apply).
			body =
				lines.length > 0
					? lines.join("\n")
					: `Final: ${headlineName(winner)} ${game.won.pts}, ${headlineName(loser)} ${game.lost.pts}.`;
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

		notifications.push({ title, body, targetTids: [tid], path });
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

// " (58/78)" (ovr/pot), or " (58 ovr)" if pot is missing, or "" if neither.
const ratingParen = (p: any): string => {
	const rating = currentRating(p);
	const ovr = rating?.ovr;
	const pot = rating?.pot;
	if (typeof ovr === "number" && typeof pot === "number") {
		return ` (${ovr}/${pot})`;
	}
	if (typeof ovr === "number") {
		return ` (${ovr} ovr)`;
	}
	return "";
};

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

	// Teams that RECEIVED a tracked asset (player/pick).
	const assetTids = new Set<number>();
	for (const p of players) {
		if (typeof p.tid === "number" && p.tid >= 0) {
			assetTids.add(p.tid);
		}
	}
	for (const dp of picks) {
		if (typeof dp.tid === "number" && dp.tid >= 0) {
			assetTids.add(dp.tid);
		}
	}

	// A trade logs a "trade" event carrying BOTH teams' tids. That's the reliable
	// signal this is a trade (vs a free-agent signing, which also just moves a
	// player onto a team) and the only way to recover a team that gave/received
	// NOTHING - e.g. a "traded nothing for Chaney Johnson" deal, where only one
	// team's assets moved so asset tids alone would see just one team.
	const tradeEvent = changesToValues(changeset, "events").find(
		(e) => e && e.type === "trade",
	);
	const eventTids =
		tradeEvent && Array.isArray(tradeEvent.tids)
			? tradeEvent.tids.filter(
					(t: unknown): t is number => typeof t === "number" && t >= 0,
				)
			: [];

	// Prefer the event's team pair (it includes a "gave nothing" team); otherwise
	// infer from the assets that moved. Only a clean 2-team deal is describable.
	const teamTids = eventTids.length === 2 ? eventTids : [...assetTids];
	if (teamTids.length !== 2) {
		return undefined;
	}

	const [a, b] = teamTids as [number, number];
	const assetsGoingTo = (tid: number): string[] => [
		...players
			.filter((p) => p.tid === tid)
			.map((p) => `${playerName(p)}${ratingParen(p)}`),
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
	return {
		title: "Trade",
		body,
		targetTids: null,
		path: `transactions/all/${g.get("season")}/trade`,
	};
};

// "The LA Lakers sign New Guy (80/85, PG) to a 3-year, $45M contract."
const signingBody = (
	p: any,
	teamById: Map<number, TeamInfo>,
	verb: string,
): string => {
	const rating = currentRating(p);
	const ovr = rating?.ovr;
	const pot = rating?.pot;
	const pos = rating?.pos;
	const years = Math.max(
		1,
		(p.contract?.exp ?? g.get("season")) - g.get("season") + 1,
	);
	const totalM = Math.round(((p.contract?.amount ?? 0) * years) / 1000);
	const ratingStr =
		typeof ovr === "number" && typeof pot === "number"
			? `${ovr}/${pot}`
			: typeof ovr === "number"
				? `${ovr} ovr`
				: "";
	const parenParts = [ratingStr, pos].filter(Boolean).join(", ");
	const who = parenParts ? `${playerName(p)} (${parenParts})` : playerName(p);
	return `The ${teamLabel(teamById, p.tid)} ${verb} ${who} to a ${years}-year, $${totalM}M contract.`;
};

// A free-agent signing blurb (event type "freeAgent"), or undefined if this
// isn't a single player joining a single team. Re-signings are handled
// separately (describeReSignings) so we can gate them on potential.
const describeSigning = (
	changeset: Changeset,
	teamById: Map<number, TeamInfo>,
): SyncNotification | undefined => {
	// A genuine signing logs a freeAgent event alongside the player write.
	// Without one, a lone player record change is just an EDIT (God Mode, a
	// ratings/face tweak, a note/watch toggle) - which must NOT push. Also
	// separates a signing from a one-sided trade.
	const events = changesToValues(changeset, "events");
	if (events.some((e) => e && e.type === "trade")) {
		return undefined;
	}
	if (!events.some((e) => e && e.type === "freeAgent")) {
		return undefined;
	}

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
	return {
		title: "Signing",
		body: signingBody(p, teamById, "sign"),
		targetTids: null,
		path: typeof p.pid === "number" ? `player/${p.pid}` : undefined,
	};
};

// Only re-signings of players with real upside are worth a push.
const RESIGN_MIN_POT = 60;

// One notification per re-signed player with pot >= RESIGN_MIN_POT. Handles a
// bulk "re-sign all" (each reSigned event names its player), and stays quiet for
// low-upside re-signs so the phase doesn't spam everyone's phone.
const describeReSignings = (
	changeset: Changeset,
	teamById: Map<number, TeamInfo>,
): SyncNotification[] => {
	const pids = new Set<number>();
	for (const e of changesToValues(changeset, "events")) {
		if (e && e.type === "reSigned" && Array.isArray(e.pids)) {
			for (const pid of e.pids) {
				if (typeof pid === "number") {
					pids.add(pid);
				}
			}
		}
	}
	if (pids.size === 0) {
		return [];
	}

	const byPid = new Map<number, any>();
	for (const p of changesToValues(changeset, "players")) {
		if (typeof p?.pid === "number") {
			byPid.set(p.pid, p);
		}
	}

	const out: SyncNotification[] = [];
	for (const pid of pids) {
		const p = byPid.get(pid);
		if (!p || typeof p.tid !== "number" || p.tid < 0 || !p.contract) {
			continue;
		}
		const pot = currentRating(p)?.pot;
		if (typeof pot !== "number" || pot < RESIGN_MIN_POT) {
			continue;
		}
		out.push({
			title: "Re-signing",
			body: signingBody(p, teamById, "re-sign"),
			targetTids: null,
			path: `player/${pid}`,
		});
	}
	return out;
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
			path: typeof p.pid === "number" ? `player/${p.pid}` : "draft",
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
	{ isHost }: { isHost: boolean; authorName: string },
): Promise<SyncNotification[]> => {
	// Note edits (filing an AI game recap, player/game notes, etc.) rewrite whole
	// game/player records, which would otherwise look like a sim. The note still
	// syncs so everyone sees it - it just never triggers a push.
	if (label === "main.setNote") {
		return [];
	}

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

	if (isSim) {
		// Non-host devices shouldn't be simming; if they somehow do, stay quiet so
		// the room doesn't get duplicate sim announcements.
		if (!isHost) {
			return [];
		}
		// Crossing into a new phase is the salient thing - announce it (free agency
		// lists the top FAs, etc.) instead of per-team game summaries for this tick.
		if (newPhase !== undefined) {
			return buildPhaseChangeNotifications(newPhase);
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

	// A manual phase advance (not via a sim).
	if (newPhase !== undefined) {
		return buildPhaseChangeNotifications(newPhase);
	}

	// Re-signings (only notable players ping). Handled before the small-changeset
	// block below because a "re-sign all" can be large; a re-sign event is a clear
	// signal on its own. If it's re-signs but none clear the pot bar, stay silent.
	if (
		changesToValues(changeset, "events").some((e) => e && e.type === "reSigned")
	) {
		return describeReSignings(changeset, await teamsById());
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
	}

	return [];
};
