import { assert, describe, test } from "vitest";
import {
	americanToDecimal,
	americanToImpliedProb,
	combinedDecimalOdds,
	decimalToAmerican,
	formatAmerican,
	formatSportsbookMoney,
	parlayConflict,
	probToAmerican,
	SPORTSBOOK_VIG,
} from "./sportsbook.ts";
import type { SportsbookMarket } from "./types.ts";

describe("probToAmerican", () => {
	test("a coin-flip prices as a slight favorite once the vig is applied", () => {
		const odds = probToAmerican(0.5);
		assert.ok(odds < 0, `expected a negative (favored) price, got ${odds}`);
		assert.ok(odds <= -100);
	});

	test("a strong favorite is a big negative number", () => {
		const odds = probToAmerican(0.8);
		assert.ok(odds < -200, `expected < -200, got ${odds}`);
	});

	test("a long shot is a big positive number", () => {
		const odds = probToAmerican(0.1);
		assert.ok(odds > 200, `expected > +200, got ${odds}`);
	});

	test("higher true probability never yields a worse (more positive) price", () => {
		let prev = Number.POSITIVE_INFINITY;
		for (let p = 0.05; p <= 0.95; p += 0.05) {
			const odds = probToAmerican(p);
			assert.ok(
				odds <= prev + 1,
				`monotonic: p=${p} odds=${odds} prev=${prev}`,
			);
			prev = odds;
		}
	});

	test("the book's implied probabilities carry the vig (sum > 100% on a 2-way)", () => {
		// A perfectly even 2-way market: both sides at true 50%.
		const a = probToAmerican(0.5);
		const overround = 2 * americanToImpliedProb(a);
		assert.ok(overround > 1, `expected overround > 1, got ${overround}`);
		assert.ok(overround < 1 + 2 * SPORTSBOOK_VIG + 0.05);
	});
});

describe("americanToDecimal", () => {
	test("negative (favorite) odds", () => {
		assert.ok(Math.abs(americanToDecimal(-200) - 1.5) < 1e-9);
	});
	test("positive (underdog) odds", () => {
		assert.ok(Math.abs(americanToDecimal(150) - 2.5) < 1e-9);
	});
	test("a winning bet returns stake * decimal", () => {
		const stake = 1000;
		const payout = stake * americanToDecimal(-110);
		// -110 → risk 110 to win 100 → $1000 returns ~$1909.
		assert.ok(payout > 1900 && payout < 1920, `payout ${payout}`);
	});
});

describe("parlay odds", () => {
	test("decimalToAmerican inverts americanToDecimal (favorites and dogs)", () => {
		for (const american of [-250, -150, -110, 120, 200, 500]) {
			const back = decimalToAmerican(americanToDecimal(american));
			assert.ok(
				Math.abs(back - american) <= 5,
				`${american} -> ${americanToDecimal(american)} -> ${back}`,
			);
		}
	});

	test("combinedDecimalOdds multiplies the legs", () => {
		const d = combinedDecimalOdds([-110, -110]);
		assert.ok(Math.abs(d - americanToDecimal(-110) ** 2) < 1e-9);
		assert.ok(d > 3.6 && d < 3.7);
	});

	test("underdog legs compound into a big price (+150 & +200 -> +650)", () => {
		const d = combinedDecimalOdds([150, 200]);
		assert.strictEqual(d, 7.5);
		assert.strictEqual(decimalToAmerican(d), 650);
	});
});

describe("parlayConflict", () => {
	const ml = (gid: number, pickTid: number): SportsbookMarket => ({
		type: "gameMoneyline",
		gid,
		pickTid,
	});
	const spread = (gid: number, pickTid: number): SportsbookMarket => ({
		type: "gameSpread",
		gid,
		pickTid,
		line: -3.5,
	});
	const total = (gid: number, side: "over" | "under"): SportsbookMarket => ({
		type: "gameTotal",
		gid,
		side,
		line: 210.5,
	});
	const playerProp = (
		gid: number,
		pid: number,
		side: "over" | "under",
	): SportsbookMarket => ({
		type: "playerProp",
		gid,
		pid,
		stat: "pts",
		side,
		line: 24.5,
	});

	test("allows unrelated legs across different games", () => {
		assert.strictEqual(parlayConflict([ml(1, 10), ml(2, 20)]), undefined);
	});

	test("allows correlated same-game legs that can all hit", () => {
		assert.strictEqual(
			parlayConflict([ml(1, 10), playerProp(1, 100, "over")]),
			undefined,
		);
	});

	test("blocks betting both teams of the same game (ML + opposite spread)", () => {
		assert.ok(parlayConflict([ml(1, 20), spread(1, 10)]));
	});

	test("blocks over and under of the same total", () => {
		assert.ok(parlayConflict([total(1, "over"), total(1, "under")]));
	});

	test("blocks over and under of the same player prop", () => {
		assert.ok(
			parlayConflict([playerProp(1, 100, "over"), playerProp(1, 100, "under")]),
		);
	});

	test("blocks exact duplicate legs", () => {
		assert.ok(parlayConflict([ml(1, 10), ml(1, 10)]));
	});

	// --- Futures / awards ---------------------------------------------------
	const champion = (pickTid: number): SportsbookMarket => ({
		type: "champion",
		pickTid,
		season: 2026,
	});
	const conf = (pickTid: number, cid: number): SportsbookMarket => ({
		type: "conf",
		pickTid,
		cid,
		season: 2026,
	});
	const award = (award: "mvp" | "dpoy", pid: number): SportsbookMarket => ({
		type: "award",
		award,
		pid,
		season: 2026,
	});
	const winTotal = (
		pickTid: number,
		side: "over" | "under",
	): SportsbookMarket => ({
		type: "winTotal",
		pickTid,
		side,
		line: 41.5,
		season: 2026,
	});
	const allLeague = (pid: number, tier: 1 | 2 | 3): SportsbookMarket => ({
		type: "allLeagueTeam",
		pid,
		tier,
		season: 2026,
	});
	const allStar = (pid: number): SportsbookMarket => ({
		type: "allStarTeam",
		pid,
		season: 2026,
	});

	test("blocks backing two different champions", () => {
		assert.ok(parlayConflict([champion(10), champion(20)]));
	});

	test("allows one champion pick alongside an unrelated leg", () => {
		assert.strictEqual(
			parlayConflict([champion(10), allStar(1)]),
			undefined,
		);
	});

	test("blocks two winners of the same conference, allows different confs", () => {
		assert.ok(parlayConflict([conf(10, 0), conf(20, 0)]));
		assert.strictEqual(parlayConflict([conf(10, 0), conf(20, 1)]), undefined);
	});

	test("blocks two different winners of the same award", () => {
		assert.ok(parlayConflict([award("mvp", 1), award("mvp", 2)]));
		// Different awards are fine.
		assert.strictEqual(
			parlayConflict([award("mvp", 1), award("dpoy", 2)]),
			undefined,
		);
	});

	test("blocks over and under of the same team's win total", () => {
		assert.ok(parlayConflict([winTotal(10, "over"), winTotal(10, "under")]));
	});

	test("blocks more than 5 players making the same All-League tier", () => {
		const six = [1, 2, 3, 4, 5, 6].map((pid) => allLeague(pid, 1));
		assert.ok(parlayConflict(six));
		// Exactly 5 is possible.
		assert.strictEqual(parlayConflict(six.slice(0, 5)), undefined);
		// 6 across two different tiers is fine (5 fit each).
		assert.strictEqual(
			parlayConflict([
				...[1, 2, 3].map((pid) => allLeague(pid, 1)),
				...[4, 5, 6].map((pid) => allLeague(pid, 2)),
			]),
			undefined,
		);
	});

	test("caps All-Star legs at the roster size when provided", () => {
		const legs = [1, 2, 3].map((pid) => allStar(pid));
		assert.ok(parlayConflict(legs, { allStarRosterSize: 2 }));
		assert.strictEqual(
			parlayConflict(legs, { allStarRosterSize: 3 }),
			undefined,
		);
		// Without a size, All-Star legs aren't capped here (worker still enforces).
		assert.strictEqual(parlayConflict(legs), undefined);
	});
});

describe("formatting", () => {
	test("formatAmerican", () => {
		assert.strictEqual(formatAmerican(150), "+150");
		assert.strictEqual(formatAmerican(-150), "-150");
		assert.strictEqual(formatAmerican(0), "EVEN");
	});

	test("formatSportsbookMoney", () => {
		assert.strictEqual(formatSportsbookMoney(1_000_000), "$1M");
		assert.strictEqual(formatSportsbookMoney(1_500_000), "$1.5M");
		assert.strictEqual(formatSportsbookMoney(12_500), "$12.5K");
		assert.strictEqual(formatSportsbookMoney(500), "$500");
		assert.strictEqual(formatSportsbookMoney(-2_000_000), "-$2M");
	});
});
