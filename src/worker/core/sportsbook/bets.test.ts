import { afterEach, assert, beforeEach, describe, test } from "vitest";
import { mockIDBLeague, resetCache, resetG } from "../../../test/helpers.ts";
import { g } from "../../util/index.ts";
import { idb } from "../../db/index.ts";
import { cancelBet, placeBetSlip, settleBets } from "./bets.ts";
import type { SportsbookBet } from "../../../common/types.ts";

// Regression coverage for the sportsbook money-moving code (placeBet(Slip),
// cancelBet, settleBets/resolveBet). Before the concurrency fix, none of this
// had any test coverage at all - and this is exactly the layer where
// "payouts sometimes don't pay out" and "money resets" originated.

const setWallet = async (
	tid: number,
	balance: number,
	bets: SportsbookBet[] = [],
) => {
	const t = await idb.cache.teams.get(tid);
	if (!t) {
		throw new Error("team not found");
	}
	(t as any).sportsbook = { balance, bets, history: [] };
	await idb.cache.teams.put(t);
};

const getWallet = async (tid: number) => {
	const t = await idb.cache.teams.get(tid);
	return (t as any).sportsbook as {
		balance: number;
		bets: SportsbookBet[];
		history: SportsbookBet[];
	};
};

const moneylineBet = (
	overrides: Partial<SportsbookBet> = {},
): SportsbookBet => ({
	betID: 1,
	season: g.get("season"),
	placedAt: Date.now(),
	americanOdds: 100,
	decimalOdds: 2,
	stake: 1000,
	label: "Test bet",
	market: { type: "gameMoneyline", gid: 999, pickTid: 0 },
	...overrides,
});

describe("sportsbook bets", () => {
	beforeEach(async () => {
		resetG();
		g.setWithoutSavingToDB("season", 2026);
		g.setWithoutSavingToDB("userTid", 0);
		g.setWithoutSavingToDB("userTids", [0]);
		await resetCache({
			teams: [
				{ tid: 0, region: "LA", name: "Lakers", abbrev: "LAL" },
				{ tid: 1, region: "Boston", name: "Celtics", abbrev: "BOS" },
			],
		});
		// A "games"/"awards"/etc. lookup that misses the cache falls through to
		// idb.league - stub it out like the rest of the worker test suite does.
		// mockIDBLeague() doesn't implement the single-record .get(store, key)
		// form that getCopies/games.ts's gid lookup uses, so add a no-op for it
		// (a league with nothing on disk always "misses").
		idb.league = { ...mockIDBLeague(), get: async () => undefined } as any;
	});

	afterEach(() => {});

	describe("settleBets concurrency", () => {
		test("a decided bet is settled exactly once, even when settleBets is called concurrently", async () => {
			// A finished game the moneyline bet can resolve against.
			await idb.cache.games.add({
				gid: 999,
				season: g.get("season"),
				day: 5,
				teams: [{ tid: 0 }, { tid: 1 }],
				won: { tid: 0, pts: 110 },
				lost: { tid: 1, pts: 105 },
			} as any);

			await setWallet(0, 5000, [moneylineBet()]);

			// Fire several settleBets() calls "simultaneously" (no await between
			// them, so they all start in the same tick). Before the fix, each read
			// idb.cache.teams.getAll() - a LIVE reference, not a copy - and every
			// call's own read-modify-write interleaved with the others on the SAME
			// object, which could credit the payout more than once (or, depending
			// on interleaving order, lose it entirely when a stale whole-object
			// write landed last). The lock in withSportsbookLock serializes them
			// into one FIFO queue, so only the first call finds anything to settle.
			const results = await Promise.all([
				settleBets(),
				settleBets(),
				settleBets(),
				settleBets(),
			]);

			assert.deepStrictEqual(
				results,
				[true, false, false, false],
				"only the first queued settle should find the bet still open",
			);

			const wallet = await getWallet(0);
			assert.strictEqual(
				wallet.balance,
				// setWallet sets `balance` directly (no debit actually happened), so
				// a win just adds the payout (stake back + profit) on top of it.
				5000 + 1000 * 2,
				"the payout must be credited EXACTLY once",
			);
			assert.strictEqual(wallet.bets.length, 0, "the bet must leave `bets`");
			assert.strictEqual(
				wallet.history.length,
				1,
				"the bet must land in `history` exactly once (no duplicates)",
			);
			assert.strictEqual(wallet.history[0]!.result, "won");
		});

		test("settling twice sequentially is a no-op the second time", async () => {
			await idb.cache.games.add({
				gid: 999,
				season: g.get("season"),
				day: 5,
				teams: [{ tid: 0 }, { tid: 1 }],
				won: { tid: 1, pts: 100 },
				lost: { tid: 0, pts: 90 },
			} as any);
			await setWallet(0, 2000, [moneylineBet({ stake: 500 })]);

			const first = await settleBets();
			const second = await settleBets();

			assert.strictEqual(first, true);
			assert.strictEqual(second, false);

			const wallet = await getWallet(0);
			// A loss credits nothing back (the stake was already "spent" before
			// setWallet's snapshot), so the balance is just whatever was set.
			assert.strictEqual(wallet.balance, 2000);
			assert.strictEqual(wallet.history.length, 1);
			assert.strictEqual(wallet.history[0]!.result, "lost");
		});
	});

	describe("resolveBet / void handling", () => {
		test("voids (refunds) a bet whose game is gone from BOTH games and schedule, instead of hanging forever", async () => {
			// No `games` row and no `schedule` row for gid 999 - simulates a box
			// score pruned (deleteOldBoxScores / Delete Old Data) before settlement
			// ever ran. Must not stay open forever with the stake frozen, and must
			// not guess at a fabricated result either.
			await setWallet(0, 1000, [moneylineBet({ stake: 400 })]);

			const settled = await settleBets();
			assert.strictEqual(settled, true);

			const wallet = await getWallet(0);
			assert.strictEqual(wallet.bets.length, 0);
			assert.strictEqual(wallet.history.length, 1);
			assert.strictEqual(wallet.history[0]!.result, "void");
			assert.strictEqual(
				wallet.balance,
				1000 + 400, // the refunded stake, on top of the pre-set balance
				"a void must refund the stake exactly (no gain, no loss)",
			);
		});

		test("leaves a bet open while its game is still scheduled (genuinely not played yet)", async () => {
			await idb.cache.schedule.add({
				gid: 999,
				homeTid: 0,
				awayTid: 1,
				day: 10,
			} as any);
			await setWallet(0, 1000, [moneylineBet({ stake: 400 })]);

			const settled = await settleBets();
			assert.strictEqual(settled, false);

			const wallet = await getWallet(0);
			assert.strictEqual(wallet.bets.length, 1, "must stay open, not void");
			assert.strictEqual(wallet.balance, 1000);
		});

		test("a total bet landing exactly on the line is a push, not a void", async () => {
			await idb.cache.games.add({
				gid: 999,
				season: g.get("season"),
				day: 5,
				teams: [
					{ tid: 0, pts: 100 },
					{ tid: 1, pts: 100 },
				],
				won: { tid: 0, pts: 100 },
				lost: { tid: 1, pts: 100 },
			} as any);
			await setWallet(0, 1000, [
				moneylineBet({
					stake: 300,
					market: { type: "gameTotal", gid: 999, side: "over", line: 200 },
				}),
			]);

			await settleBets();
			const wallet = await getWallet(0);
			assert.strictEqual(wallet.history[0]!.result, "push");
			assert.strictEqual(wallet.balance, 1000 + 300); // stake back, same as void
		});
	});

	describe("placeBetSlip atomicity", () => {
		test("throws and leaves the balance untouched when a leg's market doesn't exist", async () => {
			await setWallet(0, 1000);

			let threw = false;
			try {
				await placeBetSlip({
					tid: 0,
					picks: [
						{
							market: {
								type: "award",
								award: "mvp",
								pid: 12345,
								season: g.get("season"),
							},
							stake: 100,
							americanOdds: 150,
							label: "nonexistent candidate",
						},
					],
				});
			} catch {
				threw = true;
			}

			assert.strictEqual(threw, true);
			const wallet = await getWallet(0);
			assert.strictEqual(
				wallet.balance,
				1000,
				"a rejected slip must not debit anything",
			);
			assert.strictEqual(wallet.bets.length, 0);
		});

		test("rejects a stake for a team the caller doesn't control", async () => {
			await setWallet(1, 1000);
			let threw = false;
			try {
				await placeBetSlip({
					tid: 1,
					picks: [
						{
							market: {
								type: "award",
								award: "mvp",
								pid: 1,
								season: g.get("season"),
							},
							stake: 100,
							americanOdds: 150,
							label: "x",
						},
					],
				});
			} catch {
				threw = true;
			}
			assert.strictEqual(threw, true);
		});
	});

	describe("cancelBet", () => {
		test("refunds the exact stake and removes the bet", async () => {
			await setWallet(0, 500, [moneylineBet({ betID: 7, stake: 250 })]);

			const result = await cancelBet({ tid: 0, betID: 7 });

			assert.strictEqual(result?.balance, 750);
			const wallet = await getWallet(0);
			assert.strictEqual(wallet.bets.length, 0);
			assert.strictEqual(wallet.balance, 750);
		});

		test("is a no-op for an unknown betID", async () => {
			await setWallet(0, 500, [moneylineBet({ betID: 7, stake: 250 })]);
			await cancelBet({ tid: 0, betID: 999 });
			const wallet = await getWallet(0);
			assert.strictEqual(wallet.balance, 500);
			assert.strictEqual(wallet.bets.length, 1);
		});
	});

	describe("game props settlement", () => {
		// A finished game with real per-player box scores, exactly the shape
		// resolveBet reads for player/team prop markets.
		const seedGame = async () => {
			await idb.cache.games.add({
				gid: 999,
				season: g.get("season"),
				day: 5,
				overtimes: 0,
				teams: [
					{
						tid: 0,
						pts: 110,
						orb: 10,
						drb: 32,
						ast: 24,
						tp: 12,
						players: [
							{
								pid: 100,
								min: 34,
								pts: 28,
								orb: 2,
								drb: 9,
								ast: 11,
								stl: 2,
								blk: 1,
								tp: 3,
								tov: 3,
								dd: 1,
								td: 1,
							},
							{
								// DNP - on the roster but didn't play this game.
								pid: 101,
								min: 0,
								pts: 0,
								orb: 0,
								drb: 0,
								ast: 0,
								stl: 0,
								blk: 0,
								tp: 0,
								tov: 0,
								dd: 0,
								td: 0,
							},
						],
					},
					{
						tid: 1,
						pts: 100,
						orb: 8,
						drb: 28,
						ast: 18,
						tp: 9,
						players: [
							{
								pid: 200,
								min: 30,
								pts: 15,
								orb: 1,
								drb: 4,
								ast: 3,
								stl: 1,
								blk: 0,
								tp: 2,
								tov: 2,
								dd: 0,
								td: 0,
							},
						],
					},
				],
				won: { tid: 0, pts: 110 },
				lost: { tid: 1, pts: 100 },
			} as any);
		};

		const propBet = (
			market: SportsbookBet["market"],
			overrides: Partial<SportsbookBet> = {},
		): SportsbookBet => ({
			betID: 1,
			season: g.get("season"),
			placedAt: Date.now(),
			americanOdds: 100,
			decimalOdds: 2,
			stake: 100,
			label: "prop",
			market,
			...overrides,
		});

		test("player points prop: real value (28) over the line (25.5) wins; under loses", async () => {
			await seedGame();
			await setWallet(0, 1000, [
				propBet(
					{
						type: "playerProp",
						gid: 999,
						pid: 100,
						stat: "pts",
						side: "over",
						line: 25.5,
					},
					{ betID: 1 },
				),
				propBet(
					{
						type: "playerProp",
						gid: 999,
						pid: 100,
						stat: "pts",
						side: "under",
						line: 25.5,
					},
					{ betID: 2 },
				),
			]);
			await settleBets();
			const wallet = await getWallet(0);
			const won = wallet.history.find((b) => b.betID === 1)!;
			const lost = wallet.history.find((b) => b.betID === 2)!;
			assert.strictEqual(won.result, "won");
			assert.strictEqual(lost.result, "lost");
		});

		test("player rebounds prop is derived from orb+drb (2+9=11), not a raw 'trb' field", async () => {
			await seedGame();
			await setWallet(0, 1000, [
				propBet({
					type: "playerProp",
					gid: 999,
					pid: 100,
					stat: "trb",
					side: "over",
					line: 10.5,
				}),
			]);
			await settleBets();
			const wallet = await getWallet(0);
			assert.strictEqual(wallet.history[0]!.result, "won"); // 11 > 10.5
		});

		test("PRA combo prop sums pts+trb+ast from the real box score (28+11+11=50)", async () => {
			await seedGame();
			await setWallet(0, 1000, [
				propBet({
					type: "playerProp",
					gid: 999,
					pid: 100,
					stat: "pra",
					side: "over",
					line: 49.5,
				}),
				propBet(
					{
						type: "playerProp",
						gid: 999,
						pid: 100,
						stat: "pra",
						side: "under",
						line: 50.5,
					},
					{ betID: 2 },
				),
			]);
			await settleBets();
			const wallet = await getWallet(0);
			// 50.5 line: real value 50 is under (won); 49.5 line: 50 is over (won).
			assert.strictEqual(wallet.history.length, 2);
			assert.ok(wallet.history.every((b) => b.result === "won"));
		});

		test("double-double / triple-double read the game engine's own dd/td flags exactly", async () => {
			await seedGame();
			await setWallet(0, 1000, [
				propBet(
					{ type: "playerMilestone", gid: 999, pid: 100, milestone: "dd" },
					{ betID: 1 },
				),
				propBet(
					{ type: "playerMilestone", gid: 999, pid: 100, milestone: "td" },
					{ betID: 2 },
				),
				propBet(
					{ type: "playerMilestone", gid: 999, pid: 200, milestone: "dd" },
					{ betID: 3 },
				),
			]);
			await settleBets();
			const wallet = await getWallet(0);
			assert.strictEqual(
				wallet.history.find((b) => b.betID === 1)!.result,
				"won",
			);
			assert.strictEqual(
				wallet.history.find((b) => b.betID === 2)!.result,
				"won",
			);
			assert.strictEqual(
				wallet.history.find((b) => b.betID === 3)!.result,
				"lost",
			);
		});

		test("a player who didn't play (0 minutes) voids the prop rather than auto-losing it", async () => {
			await seedGame();
			await setWallet(0, 1000, [
				propBet({
					type: "playerProp",
					gid: 999,
					pid: 101, // DNP in seedGame
					stat: "pts",
					side: "over",
					line: 10.5,
				}),
			]);
			await settleBets();
			const wallet = await getWallet(0);
			assert.strictEqual(wallet.history[0]!.result, "void");
			assert.strictEqual(wallet.balance, 1100); // stake refunded
		});

		test("a player never on either roster for the game voids the prop", async () => {
			await seedGame();
			await setWallet(0, 1000, [
				propBet({
					type: "playerProp",
					gid: 999,
					pid: 555,
					stat: "pts",
					side: "over",
					line: 10.5,
				}),
			]);
			await settleBets();
			const wallet = await getWallet(0);
			assert.strictEqual(wallet.history[0]!.result, "void");
		});

		test("team points prop settles off the real team total", async () => {
			await seedGame();
			await setWallet(0, 1000, [
				propBet({
					type: "teamGameProp",
					gid: 999,
					tid: 0,
					stat: "pts",
					side: "over",
					line: 105.5,
				}),
			]);
			await settleBets();
			const wallet = await getWallet(0);
			assert.strictEqual(wallet.history[0]!.result, "won"); // 110 > 105.5
		});

		test("team rebounds prop is derived from team-level orb+drb (10+32=42)", async () => {
			await seedGame();
			await setWallet(0, 1000, [
				propBet({
					type: "teamGameProp",
					gid: 999,
					tid: 0,
					stat: "trb",
					side: "under",
					line: 42.5,
				}),
			]);
			await settleBets();
			const wallet = await getWallet(0);
			assert.strictEqual(wallet.history[0]!.result, "won"); // 42 < 42.5
		});

		test("overtime game prop settles off game.overtimes", async () => {
			await seedGame(); // overtimes: 0
			await setWallet(0, 1000, [
				propBet({ type: "gameProp", gid: 999, prop: "overtime" }),
			]);
			await settleBets();
			const wallet = await getWallet(0);
			assert.strictEqual(wallet.history[0]!.result, "lost");
		});

		test("a prop bet on a game whose data is gone (and no longer scheduled) voids, same as the top-level game markets", async () => {
			// No games row, no schedule row for gid 999.
			await setWallet(0, 1000, [
				propBet({
					type: "playerProp",
					gid: 999,
					pid: 100,
					stat: "pts",
					side: "over",
					line: 10.5,
				}),
			]);
			const settled = await settleBets();
			assert.strictEqual(settled, true);
			const wallet = await getWallet(0);
			assert.strictEqual(wallet.history[0]!.result, "void");
		});

		test("a prop bet stays open while the game is still scheduled", async () => {
			await idb.cache.schedule.add({
				gid: 999,
				homeTid: 0,
				awayTid: 1,
				day: 10,
			} as any);
			await setWallet(0, 1000, [
				propBet({
					type: "gameProp",
					gid: 999,
					prop: "overtime",
				}),
			]);
			const settled = await settleBets();
			assert.strictEqual(settled, false);
			const wallet = await getWallet(0);
			assert.strictEqual(wallet.bets.length, 1);
		});
	});

	describe("parlays", () => {
		const seedGame = async (gid: number, wonTid: number, lostTid: number) => {
			await idb.cache.games.add({
				gid,
				season: g.get("season"),
				day: 5,
				teams: [{ tid: wonTid }, { tid: lostTid }],
				won: { tid: wonTid, pts: 110 },
				lost: { tid: lostTid, pts: 100 },
			} as any);
		};

		const leg = (gid: number, pickTid: number, decimalOdds = 2) => ({
			market: { type: "gameMoneyline" as const, gid, pickTid },
			americanOdds: 100,
			decimalOdds,
			label: `g${gid}`,
		});

		const parlayBet = (
			legs: ReturnType<typeof leg>[],
			overrides: Partial<SportsbookBet> = {},
		): SportsbookBet => {
			const decimalOdds = legs.reduce((d, l) => d * l.decimalOdds, 1);
			return {
				betID: 1,
				season: g.get("season"),
				placedAt: Date.now(),
				americanOdds: 0,
				decimalOdds,
				stake: 100,
				label: `${legs.length}-leg parlay`,
				market: legs[0]!.market,
				legs,
				...overrides,
			};
		};

		test("both legs win -> parlay wins and pays the compounded odds", async () => {
			await seedGame(998, 0, 1);
			await seedGame(999, 0, 1);
			await setWallet(0, 1000, [parlayBet([leg(998, 0, 2), leg(999, 0, 2)])]);
			await settleBets();
			const wallet = await getWallet(0);
			assert.strictEqual(wallet.history[0]!.result, "won");
			// Combined decimal 2*2 = 4; payout 100*4 = 400 on top of the preset.
			assert.strictEqual(wallet.balance, 1000 + 400);
		});

		test("one losing leg sinks the whole parlay", async () => {
			await seedGame(998, 0, 1); // leg picks tid 0 -> win
			await seedGame(999, 1, 0); // leg picks tid 0 -> lose
			await setWallet(0, 1000, [parlayBet([leg(998, 0, 2), leg(999, 0, 2)])]);
			await settleBets();
			const wallet = await getWallet(0);
			assert.strictEqual(wallet.history[0]!.result, "lost");
			assert.strictEqual(wallet.balance, 1000);
		});

		test("stays open until every leg's game is decided", async () => {
			await seedGame(998, 0, 1);
			await idb.cache.schedule.add({
				gid: 999,
				homeTid: 0,
				awayTid: 1,
				day: 10,
			} as any);
			await setWallet(0, 1000, [parlayBet([leg(998, 0, 2), leg(999, 0, 2)])]);
			const settled = await settleBets();
			assert.strictEqual(settled, false);
			const wallet = await getWallet(0);
			assert.strictEqual(wallet.bets.length, 1);
		});

		test("a voided leg drops out; only surviving winners compound the payout", async () => {
			await seedGame(998, 0, 1); // winning leg, decimal 2
			// gid 999: no game row, no schedule row -> that leg voids.
			await setWallet(0, 1000, [parlayBet([leg(998, 0, 2), leg(999, 0, 3)])]);
			await settleBets();
			const wallet = await getWallet(0);
			assert.strictEqual(wallet.history[0]!.result, "won");
			// Only the surviving winner's decimal (2) applies: 100*2 = 200.
			assert.strictEqual(wallet.balance, 1000 + 200);
			assert.strictEqual(wallet.history[0]!.decimalOdds, 2);
		});

		test("every leg voiding refunds the whole ticket", async () => {
			// Both gids missing from games AND schedule -> both void.
			await setWallet(0, 1000, [parlayBet([leg(998, 0, 2), leg(999, 0, 2)])]);
			await settleBets();
			const wallet = await getWallet(0);
			assert.strictEqual(wallet.history[0]!.result, "push");
			assert.strictEqual(wallet.balance, 1000 + 100);
		});
	});
});
