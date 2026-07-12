import { PHASE } from "../../../common/constants.ts";
import { g, toUI, logEvent } from "../../util/index.ts";
import { idb } from "../../db/index.ts";
import {
	americanToDecimal,
	formatSportsbookMoney,
	SPORTSBOOK_PRESEASON_GRANT,
} from "../../../common/sportsbook.ts";
import type {
	Conditions,
	SportsbookBet,
	SportsbookMarket,
} from "../../../common/types.ts";

const HISTORY_LIMIT = 60;

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

// Place a bet from a user team's wallet. Debits the stake immediately; the
// payout (stake × decimal odds) lands on a win at settlement. Throws with a
// user-facing message on any invalid bet.
export const placeBet = async ({
	tid,
	market,
	stake,
	americanOdds,
	label,
}: {
	tid: number;
	market: SportsbookMarket;
	stake: number;
	americanOdds: number;
	label: string;
}) => {
	if (!g.get("userTids").includes(tid)) {
		throw new Error("You can only bet from your own team.");
	}
	if (!Number.isFinite(stake) || stake <= 0) {
		throw new Error("Enter a stake.");
	}
	const t = await idb.cache.teams.get(tid);
	if (!t) {
		throw new Error("Team not found.");
	}
	const sb = ensureWallet(t);
	if (stake > sb.balance) {
		throw new Error("Not enough $ for that stake.");
	}

	const bet: SportsbookBet = {
		betID: nextBetID(sb),
		season: g.get("season"),
		placedAt: Date.now(),
		americanOdds,
		decimalOdds: americanToDecimal(americanOdds),
		stake,
		label,
		market,
	};

	sb.balance -= stake;
	sb.bets.push(bet);
	await idb.cache.teams.put(t);
	return {
		balance: sb.balance,
		bets: sb.bets,
	};
};

// Cancel an open bet, refunding the stake. Only the placing user team may.
export const cancelBet = async ({
	tid,
	betID,
}: {
	tid: number;
	betID: number;
}) => {
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
	return { balance: sb.balance, bets: sb.bets };
};

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
const resolveBet = async (
	bet: SportsbookBet,
): Promise<"won" | "lost" | "push" | undefined> => {
	const m = bet.market;

	if (
		m.type === "gameMoneyline" ||
		m.type === "gameSpread" ||
		m.type === "gameTotal"
	) {
		const game = await idb.getCopy.games({ gid: m.gid }, "noCopyCache");
		if (!game || !game.won || !game.lost) {
			return undefined; // not played yet
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
			return (total > m.line) === (m.side === "over") ? "won" : "lost";
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
			return "push";
		}
		const wins = ts.seasonAttrs.won;
		if (wins === m.line) {
			return "push";
		}
		return (wins > m.line) === (m.side === "over") ? "won" : "lost";
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
			return "lost";
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
			return champ?.tid === m.pickTid ? "won" : "lost";
		}
		// Conference winner = the team from that conference that reached the
		// finals (won every round but possibly the last).
		const confFinalist = teamSeasons.find(
			(t) => t.cid === m.cid && t.seasonAttrs.playoffRoundsWon >= rounds - 1,
		);
		return confFinalist?.tid === m.pickTid ? "won" : "lost";
	}

	if (m.type === "award") {
		const awards = await idb.getCopy.awards({ season: m.season });
		if (!awards) {
			return undefined; // not decided yet
		}
		const winner = (awards as any)[m.award];
		// Some awards (e.g. MIP in year 1) can be absent - then any bet loses once
		// the awards exist.
		if (!winner) {
			return "lost";
		}
		return winner.pid === m.pid ? "won" : "lost";
	}

	return undefined;
};

// Settle every open bet whose outcome is now known, crediting winnings to the
// team wallet and moving the bet to history. Idempotent - safe to call after
// each day's games and on every phase change; unresolved bets are left open.
// Runs on whoever is simming; wallet changes sync to the room via the team
// record. Returns true if anything settled.
export const settleBets = async (conditions?: Conditions) => {
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
		let netWinnings = 0;

		for (const bet of sb.bets) {
			const result = await resolveBet(bet);
			if (result === undefined) {
				stillOpen.push(bet);
				continue;
			}
			const done = { ...bet, result, settledAt: Date.now() };
			if (result === "won") {
				const payout = bet.stake * bet.decimalOdds;
				sb.balance += payout;
				netWinnings += payout - bet.stake;
				wonCount += 1;
			} else if (result === "push") {
				sb.balance += bet.stake;
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
			logEvent(
				{
					type: "info",
					text: `Sportsbook: ${settled.length} bet${settled.length === 1 ? "" : "s"} settled (${wonCount} won), ${
						net >= 0 ? "+" : ""
					}${formatSportsbookMoney(net)}.`,
					saveToDb: false,
				},
				conditions,
			);
		}
	}

	if (anySettled) {
		void toUI("realtimeUpdate", [["watchList"]]);
	}
	return anySettled;
};
