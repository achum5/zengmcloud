import { PHASE } from "../../../common/constants.ts";
import { legacyAwardPids } from "../../util/legacyAwards.ts";
import { g, local, lock, toUI, logEvent } from "../../util/index.ts";
import { idb } from "../../db/index.ts";
import {
	americanToDecimal,
	combinedDecimalOdds,
	decimalToAmerican,
	formatSportsbookMoney,
	parlayConflict,
	SPORTSBOOK_PRESEASON_GRANT,
} from "../../../common/sportsbook.ts";
import { getLines } from "./getLines.ts";
import { getGameProps } from "./getGameProps.ts";
import { getSyncEngine } from "../sync/engineHolder.ts";
import type {
	Conditions,
	SportsbookBet,
	SportsbookBetLeg,
	SportsbookMarket,
} from "../../../common/types.ts";

const HISTORY_LIMIT = 60;

// Every mutating sportsbook operation (place/cancel/settle) reads a team's
// `sportsbook` object from idb.cache (a LIVE reference, not a copy - see
// Cache._get/_getAll), mutates it, and does a whole-object overwrite at the
// end. If two of these ever run concurrently on this worker - a nightly
// settle firing at the same moment as a bet click, or a sim's settle
// overlapping the Sportsbook page's own catch-up settle - they interleave
// their reads and clobber each other's writes: a bet can be double-paid (both
// calls credit it), or a payout can vanish (whichever call's stale snapshot
// writes LAST wins outright, silently dropping the other's credit and putting
// an already-settled bet back in `bets` as still-open). This was the root
// cause of "payouts sometimes don't pay out" and "money resets".
//
// Fix: serialize every place/cancel/settle through one FIFO queue, so each
// runs to completion (its full read-modify-write) before the next starts.
// Nothing here calls back into another locked operation, so there's no
// deadlock risk - just a queue.
let sportsbookQueue: Promise<unknown> = Promise.resolve();
const withSportsbookLock = <T>(fn: () => Promise<T>): Promise<T> => {
	const run = sportsbookQueue.then(fn, fn);
	// Keep the chain alive even if `fn` throws, so one failed bet can never
	// wedge every later one.
	sportsbookQueue = run.then(
		() => undefined,
		() => undefined,
	);
	return run;
};

// Bring a team's wallet into existence (a league imported mid-season, before
// its first preseason grant, is treated as already holding the standard grant).
const ensureWallet = (t: any) => {
	if (!t.sportsbook) {
		t.sportsbook = {
			balance: SPORTSBOOK_PRESEASON_GRANT,
			bets: [],
			history: [],
		};
	}
	if (!t.sportsbook.bets) {
		t.sportsbook.bets = [];
	}
	if (!t.sportsbook.history) {
		t.sportsbook.history = [];
	}
	return t.sportsbook;
};

const nextBetID = (sb: { bets: SportsbookBet[]; history: SportsbookBet[] }) => {
	let max = 0;
	for (const bet of [...sb.bets, ...sb.history]) {
		if (bet.betID > max) {
			max = bet.betID;
		}
	}
	return max + 1;
};

// The house never gives a better price than its current board, plus a tiny
// tolerance for rounding. Any client-claimed odds materially better than the
// live line are rejected - this closes the stale-line hole where a bet placed
// after a game simmed (but before the page refreshed) would be free money.
const ODDS_TOLERANCE = 1.02;

const oddsOk = (claimed: number, board: number) =>
	americanToDecimal(claimed) <=
	americanToDecimal(board) * ODDS_TOLERANCE + 1e-9;

const LINE_MOVED = "That line has moved — refresh the board and try again.";
const LINE_GONE = "That market is no longer available.";

// Per-game prop markets are validated against getGameProps(gid), NOT the
// whole-league getLines() board - computing every player/team prop for every
// upcoming game on every bet placement would be needlessly expensive. Same
// principle (a freshly re-derived, deterministic board an honest client
// always matches), just scoped to one game.
const validatePropAgainstBoard = async (
	market: Extract<
		SportsbookMarket,
		{ type: "playerProp" | "playerMilestone" | "teamGameProp" | "gameProp" }
	>,
	americanOdds: number,
) => {
	const board = await getGameProps(market.gid);
	if (!board) {
		throw new Error(LINE_GONE);
	}

	const playerOf = (pid: number) =>
		[...board.home.players, ...board.away.players].find((p) => p.pid === pid);

	if (market.type === "playerProp") {
		const player = playerOf(market.pid);
		const row = player?.props.find((p) => p.stat === market.stat);
		if (!player || !row) {
			throw new Error(LINE_GONE);
		}
		const boardOdds = market.side === "over" ? row.over : row.under;
		if (market.line !== row.line || !oddsOk(americanOdds, boardOdds)) {
			throw new Error(LINE_MOVED);
		}
		return;
	}

	if (market.type === "playerMilestone") {
		const player = playerOf(market.pid);
		if (!player) {
			throw new Error(LINE_GONE);
		}
		const boardOdds =
			market.milestone === "dd" ? player.doubleDouble : player.tripleDouble;
		if (!oddsOk(americanOdds, boardOdds)) {
			throw new Error(LINE_MOVED);
		}
		return;
	}

	if (market.type === "teamGameProp") {
		const teamProps =
			market.tid === board.home.tid
				? board.home.teamProps
				: market.tid === board.away.tid
					? board.away.teamProps
					: undefined;
		const row = teamProps?.find((p) => p.stat === market.stat);
		if (!row) {
			throw new Error(LINE_GONE);
		}
		const boardOdds = market.side === "over" ? row.over : row.under;
		if (market.line !== row.line || !oddsOk(americanOdds, boardOdds)) {
			throw new Error(LINE_MOVED);
		}
		return;
	}

	// gameProp (overtime)
	if (board.overtime === undefined || !oddsOk(americanOdds, board.overtime)) {
		throw new Error(LINE_GONE);
	}
};

// Validate a bet against the CURRENT board, server-side. The board is
// deterministic for a given league state, so an honest client matches exactly;
// anything that doesn't (stale page, simmed game, edited request) is refused.
const validateAgainstBoard = async (
	market: SportsbookMarket,
	americanOdds: number,
) => {
	if (
		market.type === "playerProp" ||
		market.type === "playerMilestone" ||
		market.type === "teamGameProp" ||
		market.type === "gameProp"
	) {
		return validatePropAgainstBoard(market, americanOdds);
	}

	const board = await getLines();

	if (
		market.type === "gameMoneyline" ||
		market.type === "gameSpread" ||
		market.type === "gameTotal"
	) {
		const game = board.games.find((gm) => gm.gid === market.gid);
		if (!game) {
			throw new Error(LINE_GONE); // already played, or not on the board
		}
		if (market.type === "gameMoneyline") {
			const boardOdds =
				market.pickTid === game.home.tid
					? game.moneyline.home
					: market.pickTid === game.away.tid
						? game.moneyline.away
						: undefined;
			if (boardOdds === undefined) {
				throw new Error(LINE_GONE);
			}
			if (!oddsOk(americanOdds, boardOdds)) {
				throw new Error(LINE_MOVED);
			}
			return;
		}
		if (market.type === "gameSpread") {
			const isHome = market.pickTid === game.home.tid;
			const isAway = market.pickTid === game.away.tid;
			if (!isHome && !isAway) {
				throw new Error(LINE_GONE);
			}
			const boardLine = isHome ? game.spread.line : -game.spread.line;
			const boardOdds = isHome ? game.spread.home : game.spread.away;
			if (market.line !== boardLine || !oddsOk(americanOdds, boardOdds)) {
				throw new Error(LINE_MOVED);
			}
			return;
		}
		// gameTotal
		const boardOdds =
			market.side === "over" ? game.total.over : game.total.under;
		if (market.line !== game.total.line || !oddsOk(americanOdds, boardOdds)) {
			throw new Error(LINE_MOVED);
		}
		return;
	}

	if (market.type === "champion") {
		const row = board.championship.find((r) => r.tid === market.pickTid);
		if (!row) {
			throw new Error(LINE_GONE);
		}
		if (!oddsOk(americanOdds, row.americanOdds)) {
			throw new Error(LINE_MOVED);
		}
		return;
	}

	if (market.type === "conf") {
		const row = board.conferences
			.find((c) => c.cid === market.cid)
			?.teams.find((r) => r.tid === market.pickTid);
		if (!row) {
			throw new Error(LINE_GONE);
		}
		if (!oddsOk(americanOdds, row.americanOdds)) {
			throw new Error(LINE_MOVED);
		}
		return;
	}

	if (market.type === "div") {
		const row = board.divisions
			.find((d) => d.did === market.did)
			?.teams.find((r) => r.tid === market.pickTid);
		if (!row) {
			throw new Error(LINE_GONE);
		}
		if (!oddsOk(americanOdds, row.americanOdds)) {
			throw new Error(LINE_MOVED);
		}
		return;
	}

	if (market.type === "winTotal") {
		const row = board.winTotals.find((r) => r.tid === market.pickTid);
		if (!row) {
			throw new Error(LINE_GONE);
		}
		const boardOdds = market.side === "over" ? row.over : row.under;
		if (market.line !== row.line || !oddsOk(americanOdds, boardOdds)) {
			throw new Error(LINE_MOVED);
		}
		return;
	}

	if (market.type === "award") {
		const row = board.awards
			.find((race) => race.award === market.award)
			?.candidates.find((c) => c.pid === market.pid);
		if (!row) {
			throw new Error(LINE_GONE);
		}
		if (!oddsOk(americanOdds, row.americanOdds)) {
			throw new Error(LINE_MOVED);
		}
		return;
	}

	if (market.type === "allStarTeam") {
		const row = board.allStar.find((c) => c.pid === market.pid);
		if (!row) {
			throw new Error(LINE_GONE);
		}
		if (!oddsOk(americanOdds, row.americanOdds)) {
			throw new Error(LINE_MOVED);
		}
		return;
	}

	if (market.type === "allLeagueTeam" || market.type === "allDefensiveTeam") {
		const tiers =
			market.type === "allLeagueTeam" ? board.allLeague : board.allDefensive;
		const row = tiers
			.find((t) => t.tier === market.tier)
			?.candidates.find((c) => c.pid === market.pid);
		if (!row) {
			throw new Error(LINE_GONE);
		}
		if (!oddsOk(americanOdds, row.americanOdds)) {
			throw new Error(LINE_MOVED);
		}
		return;
	}

	if (market.type === "allRookieTeam") {
		const row = board.allRookie.find((c) => c.pid === market.pid);
		if (!row) {
			throw new Error(LINE_GONE);
		}
		if (!oddsOk(americanOdds, row.americanOdds)) {
			throw new Error(LINE_MOVED);
		}
	}
};

type BetPick = {
	market: SportsbookMarket;
	stake: number;
	americanOdds: number;
	label: string;
};

// Place a bet slip from a user team's wallet, as ONE atomic operation: every
// pick is validated against a single fresh board snapshot and the stake is
// checked against the balance BEFORE anything is written, so a bad leg can
// never leave earlier legs already placed with money already spent. Throws with
// a user-facing message (and commits nothing) on any invalid pick.
//
// Two modes:
//   - Straight (default): each pick becomes its own bet, staked individually.
//   - Parlay: all picks combine into ONE bet with one `stake`; the odds compound
//     and every leg must win. Contradictory legs (both sides of a game/total/
//     prop) are rejected.
export const placeBetSlip = async ({
	tid,
	picks,
	parlay,
	stake,
}: {
	tid: number;
	picks: BetPick[];
	parlay?: boolean;
	stake?: number;
}) =>
	withSportsbookLock(async () => {
		if (!g.get("userTids").includes(tid)) {
			throw new Error("You can only bet from your own team.");
		}
		if (picks.length === 0) {
			throw new Error("Enter a stake.");
		}

		// Every pick must match a market currently on the board, at odds no better
		// than the board's. Rejects stale lines (a game that simmed since the page
		// loaded) before any money moves.
		for (const pick of picks) {
			await validateAgainstBoard(pick.market, pick.americanOdds);
		}

		const t = await idb.cache.teams.get(tid);
		if (!t) {
			throw new Error("Team not found.");
		}
		const sb = ensureWallet(t);
		let nextID = nextBetID(sb);
		let placed: SportsbookBet[];
		let totalStake: number;

		if (parlay) {
			if (picks.length < 2) {
				throw new Error("A parlay needs at least 2 picks.");
			}
			const conflict = parlayConflict(
				picks.map((p) => p.market),
				{ allStarRosterSize: g.get("allStarNum") * 2 },
			);
			if (conflict) {
				throw new Error(conflict);
			}
			if (!Number.isFinite(stake) || (stake ?? 0) <= 0) {
				throw new Error("Enter a stake.");
			}
			totalStake = stake!;
			const combined = combinedDecimalOdds(picks.map((p) => p.americanOdds));
			const legs: SportsbookBetLeg[] = picks.map((p) => ({
				market: p.market,
				americanOdds: p.americanOdds,
				decimalOdds: americanToDecimal(p.americanOdds),
				label: p.label,
			}));
			placed = [
				{
					betID: nextID++,
					season: g.get("season"),
					placedAt: Date.now(),
					americanOdds: decimalToAmerican(combined),
					decimalOdds: combined,
					stake: totalStake,
					label: `${picks.length}-leg parlay`,
					// Placeholder - a parlay settles via its legs, never this.
					market: picks[0]!.market,
					legs,
				},
			];
		} else {
			for (const pick of picks) {
				if (!Number.isFinite(pick.stake) || pick.stake <= 0) {
					throw new Error("Enter a stake.");
				}
			}
			totalStake = picks.reduce((sum, p) => sum + p.stake, 0);
			placed = picks.map((pick) => ({
				betID: nextID++,
				season: g.get("season"),
				placedAt: Date.now(),
				americanOdds: pick.americanOdds,
				decimalOdds: americanToDecimal(pick.americanOdds),
				stake: pick.stake,
				label: pick.label,
				market: pick.market,
			}));
		}

		if (totalStake > sb.balance) {
			throw new Error("Not enough $ for that stake.");
		}

		sb.balance -= totalStake;
		sb.bets.push(...placed);
		await idb.cache.teams.put(t);
		// Money just moved - don't leave it sitting in the cache's 4-second
		// auto-flush window where a reload would silently lose it.
		try {
			await idb.cache.flush();
		} catch {}
		return {
			balance: sb.balance,
			bets: sb.bets,
		};
	});

// Place a single bet. Thin wrapper around placeBetSlip for a one-pick slip.
export const placeBet = async (pick: { tid: number } & BetPick) =>
	placeBetSlip({ tid: pick.tid, picks: [pick] });

// Cancel an open bet, refunding the stake. Only the placing user team may.
export const cancelBet = async ({
	tid,
	betID,
}: {
	tid: number;
	betID: number;
}) =>
	withSportsbookLock(async () => {
		if (!g.get("userTids").includes(tid)) {
			throw new Error("Not your team.");
		}
		const t = await idb.cache.teams.get(tid);
		if (!t?.sportsbook) {
			return;
		}
		const sb = t.sportsbook;
		const bet = (sb.bets ?? []).find((b) => b.betID === betID);
		if (!bet) {
			return;
		}
		sb.bets = (sb.bets ?? []).filter((b) => b.betID !== betID);
		sb.balance += bet.stake;
		await idb.cache.teams.put(t);
		try {
			await idb.cache.flush();
		} catch {}
		return { balance: sb.balance, bets: sb.bets };
	});

// The number of playoff rounds this league runs, so a champion = a team that
// won them all.
const numPlayoffRounds = () => g.get("numGamesPlayoffSeries").length;

// Has season X's regular season finished (final win totals + division winners
// are known)?
const regularSeasonDone = (season: number) =>
	g.get("season") > season ||
	(g.get("season") === season && g.get("phase") >= PHASE.PLAYOFFS);

// Have season X's playoffs finished (champion + conference winners known)?
const playoffsDone = (season: number) =>
	g.get("season") > season ||
	(g.get("season") === season && g.get("phase") > PHASE.PLAYOFFS);

// Resolve a single bet against current league state. Returns the outcome, or
// undefined if it can't be settled yet (game not played, season not over, …).
// "void" means the market can no longer be resolved at all (its data is
// gone, or ambiguous) - the stake is refunded, same as a push, but it's kept
// as a distinct result so bet history is honest about why.
const resolveMarket = async (
	m: SportsbookMarket,
): Promise<"won" | "lost" | "push" | "void" | undefined> => {
	if (
		m.type === "gameMoneyline" ||
		m.type === "gameSpread" ||
		m.type === "gameTotal"
	) {
		const game = await idb.getCopy.games({ gid: m.gid }, "noCopyCache");
		if (!game || !game.won || !game.lost) {
			// Distinguish "hasn't been played yet" from "was played, but its box
			// score is gone" (deleteOldBoxScores at season rollover, or a
			// user-triggered Delete Old Data can prune the `games` store before
			// settlement runs). A bet whose game record vanished can never resolve
			// a true outcome from data alone - void it (refund) rather than hang
			// forever with the stake frozen, or guess at a result.
			const stillScheduled = await idb.cache.schedule.get(m.gid);
			if (stillScheduled) {
				return undefined; // genuinely hasn't been played yet
			}
			return "void";
		}
		const home = game.teams[0];
		const away = game.teams[1];
		if (m.type === "gameMoneyline") {
			return game.won.tid === m.pickTid ? "won" : "lost";
		}
		if (m.type === "gameTotal") {
			const total = home.pts + away.pts;
			if (total === m.line) {
				return "push";
			}
			return total > m.line === (m.side === "over") ? "won" : "lost";
		}
		// gameSpread: pick covers if its margin + line > 0.
		const pick = home.tid === m.pickTid ? home : away;
		const opp = home.tid === m.pickTid ? away : home;
		const adj = pick.pts - opp.pts + m.line;
		if (adj === 0) {
			return "push";
		}
		return adj > 0 ? "won" : "lost";
	}

	if (m.type === "winTotal") {
		if (!regularSeasonDone(m.season)) {
			return undefined;
		}
		const teamSeasons = await idb.getCopies.teamsPlus(
			{ attrs: ["tid"], seasonAttrs: ["won"], season: m.season },
			"noCopyCache",
		);
		const ts = teamSeasons.find((t) => t.tid === m.pickTid);
		if (!ts) {
			// Team no longer exists (contracted) - can't be resolved either way.
			return "void";
		}
		const wins = ts.seasonAttrs.won;
		if (wins === m.line) {
			return "push";
		}
		return wins > m.line === (m.side === "over") ? "won" : "lost";
	}

	if (m.type === "div") {
		if (!regularSeasonDone(m.season)) {
			return undefined;
		}
		const teamSeasons = await idb.getCopies.teamsPlus(
			{
				attrs: ["tid", "did"],
				seasonAttrs: ["won", "winp"],
				season: m.season,
			},
			"noCopyCache",
		);
		const inDiv = teamSeasons.filter((t) => t.did === m.did);
		if (inDiv.length === 0) {
			// The division has no teams left in it (e.g. all contracted, or
			// realigned away) - there's no winner to compare against.
			return "void";
		}
		const winner = inDiv.reduce((best, t) =>
			t.seasonAttrs.winp > best.seasonAttrs.winp ? t : best,
		);
		return winner.tid === m.pickTid ? "won" : "lost";
	}

	if (m.type === "champion" || m.type === "conf") {
		if (!playoffsDone(m.season)) {
			return undefined;
		}
		const rounds = numPlayoffRounds();
		const teamSeasons = await idb.getCopies.teamsPlus(
			{
				attrs: ["tid", "cid"],
				seasonAttrs: ["playoffRoundsWon"],
				season: m.season,
			},
			"noCopyCache",
		);
		if (m.type === "champion") {
			const champ = teamSeasons.find(
				(t) => t.seasonAttrs.playoffRoundsWon === rounds,
			);
			// No team completed every round (e.g. the playoff format changed
			// mid-season) - the outcome the bet was on was never actually decided.
			if (!champ) {
				return "void";
			}
			return champ.tid === m.pickTid ? "won" : "lost";
		}
		// Conference winner = the team from that conference that reached the
		// finals (won every round but possibly the last).
		const confFinalist = teamSeasons.find(
			(t) => t.cid === m.cid && t.seasonAttrs.playoffRoundsWon >= rounds - 1,
		);
		if (!confFinalist) {
			return "void";
		}
		return confFinalist.tid === m.pickTid ? "won" : "lost";
	}

	if (m.type === "award") {
		const awards = await idb.getCopy.awards({ season: m.season });
		if (!awards) {
			return undefined; // not decided yet
		}
		const winner = legacyAwardPids(awards).individual[m.award];
		// Some awards (e.g. MIP in year 1) can be absent - then any bet loses once
		// the awards exist.
		if (!winner) {
			return "lost";
		}
		return winner.pid === m.pid ? "won" : "lost";
	}

	if (m.type === "allStarTeam") {
		const allStars = await idb.getCopy.allStars({ season: m.season });
		if (!allStars) {
			return undefined; // roster not selected yet
		}
		const madeIt = [
			...allStars.teams[0],
			...allStars.teams[1],
			...allStars.remaining,
		].some((p) => p.pid === m.pid);
		return madeIt ? "won" : "lost";
	}

	if (m.type === "allLeagueTeam" || m.type === "allDefensiveTeam") {
		const awards = await idb.getCopy.awards({ season: m.season });
		if (!awards) {
			return undefined; // not decided yet
		}
		const legacy = legacyAwardPids(awards);
		const teams =
			m.type === "allLeagueTeam" ? legacy.allLeague : legacy.allDefensive;
		// This award category doesn't exist for this sport/season - can't resolve.
		if (teams.length === 0) {
			return "void";
		}
		const players = teams[m.tier - 1] ?? [];
		return players.some((p) => p.pid === m.pid) ? "won" : "lost";
	}

	if (m.type === "allRookieTeam") {
		const awards = await idb.getCopy.awards({ season: m.season });
		if (!awards) {
			return undefined; // not decided yet
		}
		const players = legacyAwardPids(awards).allRookie;
		return players.some((p) => p.pid === m.pid) ? "won" : "lost";
	}

	if (
		m.type === "playerProp" ||
		m.type === "playerMilestone" ||
		m.type === "teamGameProp" ||
		m.type === "gameProp"
	) {
		const game = await idb.getCopy.games({ gid: m.gid }, "noCopyCache");
		if (!game || !game.won || !game.lost) {
			// Same "genuinely not played yet" vs "data is gone" distinction as the
			// top-level game markets above.
			const stillScheduled = await idb.cache.schedule.get(m.gid);
			return stillScheduled ? undefined : "void";
		}

		if (m.type === "gameProp") {
			// Only "overtime" exists today.
			return (game.overtimes ?? 0) > 0 ? "won" : "lost";
		}

		// Teams (and players, below) don't carry a derived "trb" in the raw box
		// score - it's always orb+drb, same as the projection side in
		// getGameProps.ts.
		const statOf = (row: any, stat: string): number =>
			stat === "trb" ? (row.orb ?? 0) + (row.drb ?? 0) : (row[stat] ?? 0);

		if (m.type === "teamGameProp") {
			const team = game.teams.find((t: any) => t.tid === m.tid);
			if (!team) {
				return "void"; // team no longer exists (contracted)
			}
			const value = statOf(team, m.stat);
			if (value === m.line) {
				return "push";
			}
			return value > m.line === (m.side === "over") ? "won" : "lost";
		}

		// playerProp / playerMilestone: find this player's box-score row across
		// both teams' rosters for the game.
		const player = [...game.teams[0].players, ...game.teams[1].players].find(
			(p: any) => p.pid === m.pid,
		);
		// Didn't play (DNP, inactive, traded away before the game) - there's no
		// real outcome to grade, so refund rather than guess or auto-lose.
		if (!player || !(player.min > 0)) {
			return "void";
		}

		if (m.type === "playerMilestone") {
			const hit = m.milestone === "dd" ? player.dd : player.td;
			return hit ? "won" : "lost";
		}

		// playerProp - combo stats sum their components' real box-score values.
		let value: number;
		if (m.stat === "pra") {
			value =
				statOf(player, "pts") + statOf(player, "trb") + statOf(player, "ast");
		} else if (m.stat === "pr") {
			value = statOf(player, "pts") + statOf(player, "trb");
		} else if (m.stat === "pa") {
			value = statOf(player, "pts") + statOf(player, "ast");
		} else {
			value = statOf(player, m.stat);
		}
		if (value === m.line) {
			return "push";
		}
		return value > m.line === (m.side === "over") ? "won" : "lost";
	}

	return undefined;
};

type BetResolution = {
	// undefined = can't settle yet (a leg's game hasn't been played).
	result: "won" | "lost" | "push" | "void" | undefined;
	// For a parlay, the effective decimal multiplier after dropping pushed/voided
	// legs; undefined for a straight bet (use bet.decimalOdds).
	payoutDecimal?: number;
	// Per-leg outcomes, for a parlay (so history/UI can show which leg missed).
	legs?: SportsbookBetLeg[];
};

// Resolve a whole ticket. A straight bet resolves its one market. A parlay
// resolves every leg: it stays open until all legs are decided, then loses if
// ANY leg lost; otherwise pushed/voided legs drop out and the payout compounds
// only the surviving winners (standard parlay "reduction"). If every leg
// pushed/voided, the whole ticket is refunded.
const resolveBet = async (bet: SportsbookBet): Promise<BetResolution> => {
	if (!bet.legs || bet.legs.length === 0) {
		return { result: await resolveMarket(bet.market) };
	}

	const legResults = [];
	for (const leg of bet.legs) {
		legResults.push(await resolveMarket(leg.market));
	}
	if (legResults.some((r) => r === undefined)) {
		return { result: undefined };
	}

	const legs: SportsbookBetLeg[] = bet.legs.map((leg, i) => ({
		...leg,
		result: legResults[i] as "won" | "lost" | "push" | "void",
	}));

	if (legs.some((leg) => leg.result === "lost")) {
		return { result: "lost", legs };
	}
	const survivors = legs.filter((leg) => leg.result === "won");
	if (survivors.length === 0) {
		return { result: "push", payoutDecimal: 1, legs };
	}
	const payoutDecimal = survivors.reduce((d, leg) => d * leg.decimalOdds, 1);
	return { result: "won", payoutDecimal, legs };
};

// Settle every open bet whose outcome is now known, crediting winnings to the
// team wallet and moving the bet to history. Idempotent - safe to call after
// each day's games and on every phase change; unresolved bets are left open.
// Runs on whoever is simming; wallet changes sync to the room via the team
// record. Returns true if anything settled.
export const settleBets = async (conditions?: Conditions) =>
	withSportsbookLock(async () => {
		const teams = await idb.cache.teams.getAll();
		const userTids = new Set(g.get("userTids"));
		let anySettled = false;

		for (const t of teams) {
			const sb = t.sportsbook;
			if (!sb?.bets || sb.bets.length === 0) {
				continue;
			}

			const stillOpen: SportsbookBet[] = [];
			const settled: SportsbookBet[] = [];
			let wonCount = 0;
			let voidCount = 0;
			let netWinnings = 0;

			for (const bet of sb.bets) {
				const { result, payoutDecimal, legs } = await resolveBet(bet);
				if (result === undefined) {
					stillOpen.push(bet);
					continue;
				}
				// A reduced parlay pays a smaller multiplier than the ticket's
				// original combined odds; store the effective one so the paid-out
				// amount shown in history matches what was actually credited.
				const effectiveDecimal = payoutDecimal ?? bet.decimalOdds;
				const done = {
					...bet,
					...(legs ? { legs } : {}),
					decimalOdds: result === "won" ? effectiveDecimal : bet.decimalOdds,
					result,
					settledAt: Date.now(),
				};
				if (result === "won") {
					const payout = bet.stake * effectiveDecimal;
					sb.balance += payout;
					netWinnings += payout - bet.stake;
					wonCount += 1;
				} else if (result === "push") {
					sb.balance += bet.stake;
				} else if (result === "void") {
					// Administrative refund - not a real win or loss, so it must not
					// move netWinnings (which is reported to the user as their P&L).
					sb.balance += bet.stake;
					voidCount += 1;
				} else {
					netWinnings -= bet.stake;
				}
				settled.push(done);
			}

			if (settled.length === 0) {
				continue;
			}

			anySettled = true;
			t.sportsbook = {
				balance: sb.balance,
				bets: stillOpen,
				history: [...settled.reverse(), ...(sb.history ?? [])].slice(
					0,
					HISTORY_LIMIT,
				),
			};
			await idb.cache.teams.put(t);

			// Let the managing user know how their slips landed.
			if (userTids.has(t.tid)) {
				const net = netWinnings;
				const voidPart =
					voidCount > 0 ? `, ${voidCount} voided (refunded)` : "";
				logEvent(
					{
						type: "info",
						text: `Sportsbook: ${settled.length} bet${settled.length === 1 ? "" : "s"} settled (${wonCount} won${voidPart}), ${
							net >= 0 ? "+" : ""
						}${formatSportsbookMoney(net)}.`,
						saveToDb: false,
					},
					conditions,
				);
			}
		}

		if (anySettled) {
			// Payouts are money too - flush them, but not mid-sim/phase-change
			// (flushing then is unsafe/wasteful; those paths flush when they end).
			if (
				!lock.get("gameSim") &&
				!lock.get("newPhase") &&
				!local.autoPlayUntil
			) {
				try {
					await idb.cache.flush();
				} catch {}
			}
			void toUI("realtimeUpdate", [["watchList"]]);
		}
		return anySettled;
	});

// Settle bets only if this device is allowed to write shared state right now
// (single player, or the sim authority in a synced room). Used by the
// Sportsbook page's catch-up settle - a NON-authority device settling from its
// own (possibly slightly stale) local cache would risk computing a wrong
// result AND would race the authority device as a second writer to the same
// team records. A skipped settle here is harmless: the authority device (or
// the next phase change / sim) will settle it instead.
export const settleBetsIfAuthority = async (conditions?: Conditions) => {
	const engine = getSyncEngine();
	if (engine !== undefined && !engine.isAuthority()) {
		return false;
	}
	return settleBets(conditions);
};
