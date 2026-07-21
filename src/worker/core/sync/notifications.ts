import { PHASE, PHASE_TEXT, PLAYER } from "../../../common/constants.ts";
import { helpers } from "../../../common/helpers.ts";
import { g } from "../../util/index.ts";
import { idb } from "../../db/index.ts";
import getOrder from "../draft/getOrder.ts";
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

// ---- Live draft-lottery reveal gating -------------------------------------
//
// When the host runs the lottery in a synced league, the result is written to
// the DB (and would normally push "X won the #1 pick!") the instant the lottery
// runs - but the reveal is still animating pick-by-pick on every device. Pushing
// the winner now spoils it on everyone's phone. So while a reveal is active we
// HOLD the lottery notification and release it once the reveal finishes.
let lotteryRevealActiveUntil = 0;
let heldLotteryNotifications: SyncNotification[] = [];

// Safety cap: keep holding for at most this long without an explicit "reveal
// done" signal, so a host that crashes / navigates away mid-reveal can't
// permanently suppress the next lottery push.
const LOTTERY_HOLD_MAX_MS = 3 * 60 * 1000;

// Called on the host right when it runs the lottery, before that result's
// changeset is turned into notifications.
export const beginLotteryReveal = () => {
	lotteryRevealActiveUntil = Date.now() + LOTTERY_HOLD_MAX_MS;
	heldLotteryNotifications = [];
};

export const isLotteryRevealActive = (): boolean =>
	Date.now() < lotteryRevealActiveUntil;

const holdLotteryNotifications = (notifs: SyncNotification[]) => {
	heldLotteryNotifications.push(...notifs);
};

// Called when the reveal finishes: stop holding and hand back whatever we held
// so the caller can finally push it. Returns the held notifications regardless
// of the (possibly already-expired) active flag, so nothing is ever dropped.
export const endLotteryReveal = (): SyncNotification[] => {
	lotteryRevealActiveUntil = 0;
	const held = heldLotteryNotifications;
	heldLotteryNotifications = [];
	return held;
};

// The current best available free agents, as "Name (ovr/pot)" lines.
const topFreeAgentsText = async (): Promise<string> => {
	let fas: any[] = [];
	try {
		fas = await idb.cache.players.indexGetAll(
			"playersByTid",
			PLAYER.FREE_AGENT,
		);
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
			// No body: advancing INTO the lottery hasn't decided anything yet, so
			// don't imply the picks have landed. Just the "Advanced to ... Draft
			// Lottery!" title.
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
			body = "";
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

// The completed REGULAR games this sim added, for the current season. The
// All-Star game (special tids -1/-2) is left out - it belongs to no user team
// and is announced on its own (see simAllStars / allStarBody).
const simGames = (changeset: Changeset, season: number): Game[] => {
	const games: Game[] = [];
	for (const change of changeset.changes) {
		if (change.store === "games" && change.type === "put") {
			const game = change.value as Game;
			if (
				game &&
				game.won &&
				game.lost &&
				game.season === season &&
				game.won.tid >= 0 &&
				game.lost.tid >= 0
			) {
				games.push(game);
			}
		}
	}
	return games;
};

// The All-Star record this sim finalized, if any: an allStars put for the
// current season whose game was actually played (a score is present). Carries
// everything the recap needs - team names + score, MVP, and the Slam Dunk /
// Three-Point contest results - so no cache lookup is required.
const simAllStars = (changeset: Changeset, season: number): any => {
	for (const change of changeset.changes) {
		if (
			change.store === "allStars" &&
			change.type === "put" &&
			(change.value as any)?.season === season &&
			Array.isArray((change.value as any)?.score)
		) {
			return change.value;
		}
	}
	return undefined;
};

// The All-Star Weekend recap body: game score (winner first), MVP, and the
// Slam Dunk / Three-Point contest winners - whichever of them took place.
const allStarBody = (allStars: any): string => {
	const names: [string, string] = allStars.teamNames ?? ["Team 1", "Team 2"];
	const [s0, s1]: [number, number] = allStars.score ?? [0, 0];
	const lines = [
		s0 >= s1
			? `${names[0]} ${s0}, ${names[1]} ${s1}`
			: `${names[1]} ${s1}, ${names[0]} ${s0}`,
	];
	if (allStars.mvp?.name) {
		lines.push(`MVP: ${allStars.mvp.name}`);
	}
	const winnerName = (contest: any): string | undefined =>
		typeof contest?.winner === "number"
			? contest.players?.[contest.winner]?.name
			: undefined;
	const dunkWinner = winnerName(allStars.dunk);
	if (dunkWinner) {
		lines.push(`Dunk contest: ${dunkWinner}`);
	}
	const threeWinner = winnerName(allStars.three);
	if (threeWinner) {
		lines.push(`3-point contest: ${threeWinner}`);
	}
	return lines.join("\n");
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
// The current playoff round's series scores as one notification body, e.g.
//   BOS 2-1 MIA
//   DEN 3-0 LAL
// Shown to a user whose team isn't playing, so they can follow the bracket.
// Undefined if there are no series to show.
const playoffSeriesScores = (
	playoffSeries: { currentRound: number; series: any[][] },
	teamById: Map<number, { abbrev?: string }>,
): string | undefined => {
	const round = playoffSeries.series[playoffSeries.currentRound];
	if (!round || round.length === 0) {
		return undefined;
	}

	const abbrevOf = (t: { tid: number; abbrev?: string }): string =>
		t.abbrev ?? teamById.get(t.tid)?.abbrev ?? "???";

	const lines: string[] = [];
	for (const matchup of round) {
		const { home, away } = matchup;
		if (!away) {
			// A bye (odd bracket) - nothing to score.
			continue;
		}
		// Leader first for quick scanning; tie keeps home first.
		const [a, b] = home.won >= away.won ? [home, away] : [away, home];
		lines.push(`${abbrevOf(a)} ${a.won}-${b.won} ${abbrevOf(b)}`);
	}

	return lines.length > 0 ? lines.join("\n") : undefined;
};

// The completed games from this sim as a simple scoreboard, winner first, e.g.
//   ATL 120-114 CHA
//   MIA 105-98 CHI
// Shown during the play-in tournament (single-elimination, not series) to a
// user whose team isn't playing that day. Undefined if there are no games.
const dayGameScores = (
	games: Game[],
	teamById: Map<number, { abbrev?: string }>,
): string | undefined => {
	const abbrevOf = (tid: number): string => teamById.get(tid)?.abbrev ?? "???";
	const lines = games
		.slice()
		.sort((a, b) => a.gid - b.gid)
		.map(
			(game) =>
				`${abbrevOf(game.won.tid)} ${game.won.pts}-${game.lost.pts} ${abbrevOf(game.lost.tid)}`,
		);
	return lines.length > 0 ? lines.join("\n") : undefined;
};

// On a bye day, show at most this many of the league's games so the
// notification stays compact.
const MAX_BYE_DAY_GAMES = 5;

// The span's most notable games, ranked by the best individual performance
// (Game Score) in each, winner first: "ATL 120-114 CHA". Shown to a user whose
// team had a bye, so they can see the day's headline results around the league.
// Undefined if there are no games to show.
const biggestGamesText = (
	games: Game[],
	teamById: Map<number, { abbrev?: string }>,
	n: number,
): string | undefined => {
	const abbrevOf = (tid: number): string => teamById.get(tid)?.abbrev ?? "???";
	const bestGameScore = (game: Game): number => {
		let best = -Infinity;
		for (const t of game.teams as any[]) {
			const p = topScorer(t?.players);
			const score = p ? helpers.gameScore(p) : undefined;
			if (typeof score === "number" && !Number.isNaN(score) && score > best) {
				best = score;
			}
		}
		return best;
	};
	const lines = games
		.map((game) => ({ game, score: bestGameScore(game) }))
		.sort((a, b) => b.score - a.score || a.game.gid - b.game.gid)
		.slice(0, n)
		.map(
			({ game }) =>
				`${abbrevOf(game.won.tid)} ${game.won.pts}-${game.lost.pts} ${abbrevOf(game.lost.tid)}`,
		);
	return lines.length > 0 ? lines.join("\n") : undefined;
};

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

	// " (25-12)" - a team's season record (post-sim), or "" if unavailable.
	const recordParen = async (recTid: number): Promise<string> => {
		try {
			const teamSeason = await idb.cache.teamSeasons.indexGet(
				"teamSeasonsBySeasonTid",
				[season, recTid],
			);
			if (teamSeason) {
				return ` (${teamSeason.won}-${teamSeason.lost})`;
			}
		} catch {
			// The record is a nicety, not essential.
		}
		return "";
	};

	const notifications: SyncNotification[] = [];

	// A simmed free agency day has no games, so it was silent - announce every
	// day to the whole room: days remaining plus the day's biggest signings.
	if (phase === PHASE.FREE_AGENCY && games.length === 0) {
		const daysLeft = g.get("daysLeft");
		const signingEvents = changesToValues(changeset, "events").filter(
			(e) =>
				e &&
				e.type === "freeAgent" &&
				e.contract &&
				Array.isArray(e.pids) &&
				Array.isArray(e.tids),
		);
		signingEvents.sort(
			(a, b) => (b.contract?.amount ?? 0) - (a.contract?.amount ?? 0),
		);

		const lines: string[] = [];
		for (const e of signingEvents) {
			if (lines.length >= 3) {
				break;
			}
			try {
				const p = await idb.cache.players.get(e.pids[0]);
				if (!p) {
					continue;
				}
				const abbrev = teamById.get(e.tids[0])?.abbrev ?? "???";
				lines.push(
					`${p.firstName} ${p.lastName} → ${abbrev} ${helpers.formatCurrencyBase(
						g.get("currencyFormat"),
						e.contract.amount / 1000,
						"M",
					)}`,
				);
			} catch {
				// Name lookup is a nicety; skip the line.
			}
		}
		const more = signingEvents.length - lines.length;

		const body = [
			`${daysLeft} ${daysLeft === 1 ? "day" : "days"} of free agency left.`,
			lines.length > 0 ? lines.join("\n") : "No signings today.",
			...(more > 0 ? [`+${more} more signings`] : []),
		].join("\n");

		notifications.push({
			title: "Free agency day simmed",
			body,
			targetTids: null,
			path: "free_agents",
		});
		return notifications;
	}

	// All-Star Weekend just wrapped: announce the game score plus the Slam Dunk
	// / Three-Point winners to the WHOLE room. The All-Star game belongs to no
	// user team, so without this everyone would just see a silent bye. When this
	// fires, the per-team loop below skips its bye-day notices (isAllStarSim).
	const allStars = simAllStars(changeset, season);
	if (allStars) {
		notifications.push({
			title: "All-Star Weekend",
			body: allStarBody(allStars),
			targetTids: null,
			path: "all_star/history",
		});
	}
	const isAllStarSim = allStars !== undefined;

	// During the playoffs, EVERY device gets the bracket - eliminated teams and
	// teams with an off day included, as ONE room-wide notification (not one
	// copy per team, and not gated on being alive in the bracket). Teams that
	// played also get their own detailed game result below.
	if (phase === PHASE.PLAYOFFS) {
		let playoffSeries;
		try {
			playoffSeries = await idb.cache.playoffSeries.get(season);
		} catch {
			// Best effort - the per-team results below still go out.
		}

		// During the play-in tournament (currentRound === -1) the games are
		// single-elimination, not series, so show that day's scoreboard.
		// Otherwise show the current round's series scores.
		if (playoffSeries?.currentRound === -1) {
			const body = dayGameScores(games, teamById);
			if (body) {
				notifications.push({
					title: "Play-in scores",
					body,
					targetTids: null,
					path: "playoffs",
				});
			}
		} else if (playoffSeries) {
			const seriesBody = playoffSeriesScores(playoffSeries, teamById);
			if (seriesBody) {
				notifications.push({
					title: "Playoff scores",
					body: seriesBody,
					targetTids: null,
					path: "playoffs",
				});
			}
		}
	}

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

		// No game for this team in this span - a bye. Only worth mentioning during
		// a regular-season phase (your team had an off day while others played); in
		// the playoffs the room-wide bracket above covers it, and in the offseason
		// nobody plays, so stay silent - the phase-change notification already
		// covered the advance. During All-Star Weekend the room-wide All-Star
		// notification above already covers everyone, so skip the bye notice.
		if (teamGames.length === 0) {
			if (!isAllStarSim && phase !== PHASE.PLAYOFFS && GAME_PHASES.has(phase)) {
				const around = biggestGamesText(games, teamById, MAX_BYE_DAY_GAMES);
				notifications.push({
					title: `Bye day for the ${team?.name ?? "team"}`,
					body: around ?? `No other games ${period}.`,
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
			const winnerRec = await recordParen(game.won.tid);
			const loserRec = await recordParen(game.lost.tid);
			title = `${headlineName(winner)}${winnerRec} ${game.won.pts}, ${headlineName(loser)}${loserRec} ${game.lost.pts}`;

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

// Later-round picks only ping a team for its OWN picks; at most this many from
// one changeset so a full-draft sim doesn't fire dozens of pushes. (Every
// first-round pick is announced regardless - see buildDraftNotifications.)
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
	// Total contract value, formatted so a sub-$1M deal reads as "$350k" instead
	// of rounding down to "$0M". contract.amount is in thousands, so dividing by
	// 1000 puts it in the millions that formatCurrencyBase expects.
	const totalValueM = ((p.contract?.amount ?? 0) * years) / 1000;
	const totalStr = helpers.formatCurrencyBase(
		g.get("currencyFormat"),
		totalValueM,
		"M",
	);
	const ratingStr =
		typeof ovr === "number" && typeof pot === "number"
			? `${ovr}/${pot}`
			: typeof ovr === "number"
				? `${ovr} ovr`
				: "";
	const parenParts = [ratingStr, pos].filter(Boolean).join(", ");
	const who = parenParts ? `${playerName(p)} (${parenParts})` : playerName(p);
	return `The ${teamLabel(teamById, p.tid)} ${verb} ${who} to a ${years}-year, ${totalStr} contract.`;
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

// Pids actually drafted IN this changeset, per its draft events. This is the
// authoritative "a pick just happened" signal: a rookie's player RECORD keeps
// matching draft-shaped predicates all offseason (draft.year === season, still
// on the drafting team), so any later changeset that happens to carry the
// record (a phase change, a free-agency day adjusting contracts) would
// otherwise re-announce the pick - hours later, and once per changeset that
// touched the record.
export const draftedPidsFromEvents = (changeset: Changeset): Set<number> => {
	const pids = new Set<number>();
	for (const e of changesToValues(changeset, "events")) {
		if (e && e.type === "draft" && Array.isArray(e.pids)) {
			for (const pid of e.pids) {
				if (typeof pid === "number") {
					pids.add(pid);
				}
			}
		}
	}
	return pids;
};

// One notification per pick made in this changeset (per its draft events).
const buildDraftNotifications = (
	changeset: Changeset,
	teamById: Map<number, TeamInfo>,
	draftedPids: Set<number>,
): SyncNotification[] => {
	const season = g.get("season");
	const numActiveTeams = g.get("numActiveTeams");
	const userTids = g.get("userTids");

	const allPicks = changesToValues(changeset, "players")
		.filter(
			(p) =>
				draftedPids.has(p.pid) &&
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

	// Announce EVERY first-round pick to the whole room (the headline of the
	// draft). In later rounds, only ping someone for their OWN picks, so a "sim to
	// end" doesn't blast the room with 60+ pushes.
	const picks = allPicks.filter(
		(p) => p.draft.round === 1 || userTids.includes(p.tid),
	);

	// Allow a full first round through (all of it is worth announcing); the cap
	// only trims a deluge of later-round picks.
	const maxNotifs = Math.max(MAX_DRAFT_PICK_NOTIFS, numActiveTeams);

	const out: SyncNotification[] = [];
	for (const p of picks.slice(0, maxNotifs)) {
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
	if (picks.length > maxNotifs) {
		out.push({
			title: "Draft",
			body: `…and ${picks.length - maxNotifs} more picks.`,
			targetTids: null,
		});
	}
	return out;
};

// EVERY trade in this changeset, narrated from its trade EVENT (which carries
// both teams and exactly what each RECEIVED). Event-based so it works for a
// changeset with MULTIPLE trades and - crucially - for CPU-vs-CPU trades that
// happen INSIDE a sim day, which the record-based path never reached because a
// sim short-circuits to game summaries. Every trade gets a ping, human or not.
const MAX_TRADE_NOTIFS = 6;
const describeTradesFromEvents = (
	changeset: Changeset,
	teamById: Map<number, TeamInfo>,
): SyncNotification[] => {
	const events = changesToValues(changeset, "events").filter(
		(e) =>
			e &&
			e.type === "trade" &&
			Array.isArray(e.tids) &&
			e.tids.length === 2 &&
			Array.isArray(e.teams),
	);
	if (events.length === 0) {
		return [];
	}

	// Traded players' records ride in the same changeset, so tag them with ovr/pot.
	const playerByPid = new Map<number, any>();
	for (const p of changesToValues(changeset, "players")) {
		if (typeof p?.pid === "number") {
			playerByPid.set(p.pid, p);
		}
	}
	const assetLabel = (asset: any): string => {
		if (typeof asset?.pid === "number") {
			const p = playerByPid.get(asset.pid);
			return `${asset.name ?? "a player"}${p ? ratingParen(p) : ""}`;
		}
		const round = typeof asset?.round === "number" ? asset.round : 1;
		const season = typeof asset?.season === "number" ? asset.season : "future";
		return `a ${season} ${helpers.ordinal(round)}-round pick`;
	};

	const out: SyncNotification[] = [];
	let shown = 0;
	for (const e of events) {
		if (shown >= MAX_TRADE_NOTIFS) {
			break;
		}
		const [a, b] = e.tids as [number, number];
		// event.teams[i].assets = what team event.tids[i] RECEIVED (see processTrade).
		const aGets = (e.teams[0]?.assets ?? []).map(assetLabel);
		const bGets = (e.teams[1]?.assets ?? []).map(assetLabel);
		if (aGets.length === 0 && bGets.length === 0) {
			continue;
		}
		let body: string;
		if (aGets.length > 0 && bGets.length > 0) {
			body = `The ${teamLabel(teamById, a)} acquire ${joinAssets(aGets)} from the ${teamLabel(teamById, b)} in exchange for ${joinAssets(bGets)}.`;
		} else if (aGets.length > 0) {
			body = `The ${teamLabel(teamById, a)} acquire ${joinAssets(aGets)} from the ${teamLabel(teamById, b)}.`;
		} else {
			body = `The ${teamLabel(teamById, b)} acquire ${joinAssets(bGets)} from the ${teamLabel(teamById, a)}.`;
		}
		out.push({
			title: "Trade",
			body,
			targetTids: null,
			path: `transactions/all/${g.get("season")}/trade`,
		});
		shown += 1;
	}
	const more = events.length - shown;
	if (more > 0) {
		out.push({
			title: "Trades",
			body: `…and ${more} more ${more === 1 ? "trade" : "trades"}.`,
			targetTids: null,
			path: `transactions/all/${g.get("season")}/trade`,
		});
	}
	return out;
};

// The draft lottery result, if this changeset landed one: the top of the order
// (who won the #1 pick, then 2-4). Fires wherever the result is written -
// advancing past the lottery, or a manual reveal.
const describeLottery = (
	changeset: Changeset,
	teamById: Map<number, TeamInfo>,
): SyncNotification[] => {
	const result = changesToValues(changeset, "draftLotteryResults").find(
		(r) =>
			r &&
			Array.isArray(r.result) &&
			r.result.some((x: any) => typeof x?.pick === "number"),
	);
	if (!result) {
		return [];
	}
	const ranked = [...result.result]
		.filter((x: any) => typeof x.pick === "number")
		.sort((a: any, b: any) => a.pick - b.pick)
		.slice(0, 4);
	if (ranked.length === 0) {
		return [];
	}
	const winner = ranked[0];
	const season = result.season ?? g.get("season");
	const lines = ranked.map(
		(x: any) => `${x.pick}. ${teamLabel(teamById, x.tid)}`,
	);
	return [
		{
			title: `${season} draft lottery results`,
			body: [
				`The ${teamLabel(teamById, winner.tid)} won the #1 pick!`,
				...lines,
			].join("\n"),
			targetTids: null,
			path: "draft_lottery",
		},
	];
};

// If a user team is now on the clock in the draft, a targeted "you're up" ping
// for that team. Evaluated AFTER the changeset applied, so getOrder() reflects
// the post-pick state - the first remaining pick is whoever is up next. Only
// called when this changeset made picks or started the draft, so a random
// mid-draft change (a trade, a note) doesn't re-ping.
const buildOnTheClockNotifications = async (
	teamById: Map<number, TeamInfo>,
): Promise<SyncNotification[]> => {
	const phase = g.get("phase");
	if (phase !== PHASE.DRAFT && phase !== PHASE.FANTASY_DRAFT) {
		return [];
	}

	let order;
	try {
		order = await getOrder();
	} catch {
		return [];
	}
	const next = order[0];
	if (!next || !g.get("userTids").includes(next.tid)) {
		return [];
	}

	const overall = (next.round - 1) * g.get("numActiveTeams") + next.pick;
	return [
		{
			title: "You're on the clock!",
			body: `The ${teamLabel(teamById, next.tid)} are up with the ${helpers.ordinal(overall)} pick (round ${next.round}, pick ${next.pick}).`,
			targetTids: [next.tid],
			path: phase === PHASE.FANTASY_DRAFT ? "fantasy_draft" : "draft",
		},
	];
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

const buildBaseNotifications = async (
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

	// Draft picks made this changeset - announced per pick to the whole room.
	// Checked FIRST, before the sim/phase branches, because the simmer advances
	// CPU picks via playMenu.onePick / untilEnd (which look like sims), and the
	// final pick lands in AFTER_DRAFT (a phase change). Detected by the DRAFT
	// EVENTS in the changeset, not by player-record shape - a rookie's record
	// keeps looking "just drafted" all offseason, and shape-detection made every
	// later changeset touching it (phase changes, free-agency days) re-announce
	// the pick.
	const draftedPids = draftedPidsFromEvents(changeset);
	if (draftedPids.size > 0) {
		const draftNotifications = buildDraftNotifications(
			changeset,
			await teamsById(),
			draftedPids,
		);
		if (draftNotifications.length > 0) {
			return draftNotifications;
		}
	}

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

		const signing = describeSigning(changeset, teamById);
		if (signing) {
			return [signing];
		}
	}

	return [];
};

// Public entry: the base notifications PLUS the always-on ones that must fire
// regardless of how the change arrived - every trade (human or CPU-vs-CPU, even
// mid-sim) and the draft lottery result. These ride ALONGSIDE the base (an AI
// trade inside a sim day still gets its own ping on top of the game summaries),
// deduped by title+body.
export const buildNotifications = async (
	label: string,
	changeset: Changeset,
	opts: { isHost: boolean; authorName: string },
): Promise<SyncNotification[]> => {
	if (label === "main.setNote") {
		return [];
	}

	const teamById = await teamsById();

	// The draft lottery push is HELD while its reveal is animating (see
	// beginLotteryReveal): firing "X won the #1 pick!" the instant the result is
	// written would spoil the pick-by-pick reveal on everyone's phone. It's
	// released once the reveal finishes (endLotteryReveal).
	const lotteryNotifs = describeLottery(changeset, teamById);
	const holdLottery = lotteryNotifs.length > 0 && isLotteryRevealActive();
	if (holdLottery) {
		holdLotteryNotifications(lotteryNotifs);
	}

	const extras = [
		...(holdLottery ? [] : lotteryNotifs),
		...describeTradesFromEvents(changeset, teamById),
	];

	// "You're on the clock" whenever picks were just made (the order advanced) or
	// the draft just started (someone is up for the first time).
	const newPhase = newPhaseFromChangeset(changeset);
	if (
		draftedPidsFromEvents(changeset).size > 0 ||
		newPhase === PHASE.DRAFT ||
		newPhase === PHASE.FANTASY_DRAFT
	) {
		extras.push(...(await buildOnTheClockNotifications(teamById)));
	}

	const base = await buildBaseNotifications(label, changeset, opts);

	const merged = [...extras, ...base];
	const seen = new Set<string>();
	return merged.filter((n) => {
		const key = `${n.title} ${n.body}`;
		if (seen.has(key)) {
			return false;
		}
		seen.add(key);
		return true;
	});
};
