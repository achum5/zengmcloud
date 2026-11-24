import { assert, test } from "vitest";
import type { SportsbookBet, SportsbookMarket } from "../../../common/types.ts";
import { combinedDecimalOdds } from "../../../common/sportsbook.ts";
import { offBoardWhileLive, walletBeforeLiveGames } from "./liveHold.ts";

const ml = (gid: number, pickTid = 1): SportsbookMarket => ({
	type: "gameMoneyline",
	gid,
	pickTid,
	homeTid: 1,
	awayTid: 2,
	day: 10,
});

const bet = (
	betID: number,
	extra: Partial<SportsbookBet> & Pick<SportsbookBet, "market">,
): SportsbookBet => ({
	betID,
	season: 2025,
	placedAt: 0,
	americanOdds: 100,
	decimalOdds: 2,
	stake: 10,
	label: `bet ${betID}`,
	...extra,
});

test("a bet graded on a live game is open again, its payout off the balance", () => {
	const won = bet(3, { market: ml(7), result: "won", settledAt: 5 });
	const pushed = bet(4, { market: ml(8), result: "push", settledAt: 5 });
	const lost = bet(5, { market: ml(7), result: "lost", settledAt: 5 });
	const other = bet(2, { market: ml(9), result: "won", settledAt: 4 });
	const open = bet(6, { market: ml(11) });
	const out = walletBeforeLiveGames(
		{ balance: 100, bets: [open], history: [won, pushed, lost, other] },
		new Set([7, 8]),
	);
	// 20 back for the win, 10 for the push, nothing for the loss.
	assert.strictEqual(out.balance, 70);
	assert.deepStrictEqual(
		out.bets.map((b) => b.betID),
		[3, 4, 5, 6],
	);
	for (const b of out.bets) {
		assert.strictEqual(b.result, undefined);
		assert.strictEqual(b.settledAt, undefined);
	}
	assert.deepStrictEqual(
		out.history.map((b) => b.betID),
		[2],
	);
});

test("a parlay: back to open at its full price, unless a finished leg sank it", () => {
	const legs = [
		{
			market: ml(7),
			americanOdds: 100,
			decimalOdds: 2,
			label: "a",
			result: "won" as const,
		},
		{
			market: ml(9),
			americanOdds: -110,
			decimalOdds: 1.909,
			label: "b",
			result: "push" as const,
		},
	];
	const paid = bet(3, {
		market: legs[0]!.market,
		legs,
		decimalOdds: 2,
		result: "won",
		settledAt: 5,
	});
	const sunk = bet(4, {
		market: legs[0]!.market,
		legs: [legs[0]!, { ...legs[1]!, result: "lost" }],
		result: "lost",
		settledAt: 5,
	});
	const out = walletBeforeLiveGames(
		{ balance: 100, bets: [] as SportsbookBet[], history: [paid, sunk] },
		new Set([7]),
	);
	assert.strictEqual(out.balance, 80);
	assert.strictEqual(out.bets.length, 1);
	const reopened = out.bets[0]!;
	assert.strictEqual(reopened.decimalOdds, combinedDecimalOdds([100, -110]));
	assert.deepStrictEqual(
		reopened.legs!.map((leg) => leg.result),
		[undefined, undefined],
	);
	// Lost whatever the live game does - only its leg is undecided again.
	assert.strictEqual(out.history.length, 1);
	assert.strictEqual(out.history[0]!.result, "lost");
	assert.deepStrictEqual(
		out.history[0]!.legs!.map((leg) => leg.result),
		[undefined, "lost"],
	);
});

test("nothing live, the wallet as stored", () => {
	const wallet = {
		balance: 50,
		bets: [],
		history: [bet(1, { market: ml(7), result: "won" })],
	};
	assert.strictEqual(walletBeforeLiveGames(wallet, new Set()), wallet);
	assert.strictEqual(walletBeforeLiveGames(wallet, new Set([99])), wallet);
});

test("game lines off the board while a game is live", () => {
	const gate = {
		hidden: new Set([7]),
		dayCutoff: 40,
		games: new Map([[7, { teams: [{ tid: 1 }, { tid: 2 }] } as any]]),
	};
	// The next game of a team still playing.
	assert.isTrue(offBoardWhileLive(gate, { day: 41, homeTid: 3, awayTid: 2 }));
	assert.isTrue(offBoardWhileLive(gate, { day: 40, homeTid: 1, awayTid: 5 }));
	// A playoff day drawn up after it.
	assert.isTrue(offBoardWhileLive(gate, { day: 41, homeTid: 3, awayTid: 4 }));
	assert.isFalse(offBoardWhileLive(gate, { day: 40, homeTid: 3, awayTid: 4 }));
	assert.isFalse(
		offBoardWhileLive(
			{ hidden: new Set(), dayCutoff: undefined, games: new Map() },
			{ day: 41, homeTid: 1, awayTid: 2 },
		),
	);
});
