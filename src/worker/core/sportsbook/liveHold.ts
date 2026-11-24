import { combinedDecimalOdds, marketGid } from "../../../common/sportsbook.ts";
import type { SportsbookBet } from "../../../common/types.ts";
import type { LiveGate } from "../../util/liveGameGate.ts";

// WHAT THE BOOK HOLDS BACK WHILE A GAME IS LIVE (see util/liveGameGate.ts).
//
// A game being played out on a screen was decided the moment it tipped off,
// and the book knows it: the bets on it are graded and paid, the next game's
// line has its injuries in it, every futures price has its result in it. None
// of that is shown till the game is over on screen. The board takes those
// markets down for the while, as a book does while a game is on, and the
// wallet is shown as it stood at tip-off. On screen only: what was graded and
// paid stays graded and paid.

// A game line the board leaves off while a game is live: the next game of a
// team still playing (its line knows how this one went), or a playoff day the
// app only drew up once a live game was over.
export const offBoardWhileLive = (
	gate: Pick<LiveGate, "hidden" | "dayCutoff" | "games">,
	matchup: { day: number; homeTid: number; awayTid: number },
): boolean => {
	if (gate.hidden.size === 0) {
		return false;
	}
	if (gate.dayCutoff !== undefined && matchup.day > gate.dayCutoff) {
		return true;
	}
	for (const game of gate.games.values()) {
		for (const t of game.teams) {
			if (t.tid === matchup.homeTid || t.tid === matchup.awayTid) {
				return true;
			}
		}
	}
	return false;
};

type Wallet = {
	balance: number;
	bets: SportsbookBet[];
	history: SportsbookBet[];
};

const onHiddenGame = (
	market: SportsbookBet["market"],
	hidden: ReadonlySet<number>,
) => {
	const gid = marketGid(market);
	return gid !== undefined && hidden.has(gid);
};

// What a settled bet put back in the wallet (settleBets).
const credited = (bet: SportsbookBet) =>
	bet.result === "won"
		? bet.stake * bet.decimalOdds
		: bet.result === "push" || bet.result === "void"
			? bet.stake
			: 0;

// A settled bet as it was while open.
const reopen = (bet: SportsbookBet): SportsbookBet => {
	const { result: _result, settledAt: _settledAt, ...open } = bet;
	if (!bet.legs || bet.legs.length === 0) {
		return open;
	}
	return {
		...open,
		legs: bet.legs.map(({ result: _legResult, ...leg }) => leg),
		// (A parlay that won is stored at what it paid, its pushed legs out.)
		decimalOdds: combinedDecimalOdds(bet.legs.map((leg) => leg.americanOdds)),
	};
};

// The wallet as it stood before the games still being played live: a bet
// graded on one of them back among the open ones, and what it paid back off
// the balance. A parlay already sunk by a leg in a game that is over stays
// lost - only the live game's leg goes back to undecided.
export const walletBeforeLiveGames = <W extends Wallet>(
	wallet: W,
	hidden: ReadonlySet<number>,
): W => {
	if (hidden.size === 0) {
		return wallet;
	}
	let balance = wallet.balance;
	const reopened: SportsbookBet[] = [];
	const history: SportsbookBet[] = [];
	for (const bet of wallet.history) {
		const legs = bet.legs && bet.legs.length > 0 ? bet.legs : undefined;
		if (!legs) {
			if (onHiddenGame(bet.market, hidden)) {
				balance -= credited(bet);
				reopened.push(reopen(bet));
			} else {
				history.push(bet);
			}
			continue;
		}
		const live = legs.map((leg) => onHiddenGame(leg.market, hidden));
		if (!live.includes(true)) {
			history.push(bet);
			continue;
		}
		if (legs.some((leg, i) => !live[i] && leg.result === "lost")) {
			history.push({
				...bet,
				legs: legs.map((leg, i) => {
					if (!live[i]) {
						return leg;
					}
					const { result: _result, ...open } = leg;
					return open;
				}),
			});
			continue;
		}
		balance -= credited(bet);
		reopened.push(reopen(bet));
	}
	if (reopened.length === 0) {
		return wallet;
	}
	return {
		...wallet,
		balance,
		bets: [...wallet.bets, ...reopened].sort((a, b) => a.betID - b.betID),
		history,
	};
};
