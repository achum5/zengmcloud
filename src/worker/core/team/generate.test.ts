import { assert, beforeEach, describe, test } from "vitest";
import { resetG } from "../../../test/helpers.ts";
import { g } from "../../util/index.ts";
import generate from "./generate.ts";
import type { SportsbookBet } from "../../../common/types.ts";

// generate() is the whitelist every team passes through on league import (see
// core/league/createStream.ts). If it drops a field, that data is silently lost
// on any export/re-import - which is exactly what happened to the sportsbook
// wallet + bet history (and would happen to court customization too).

describe("team.generate import round-trip", () => {
	beforeEach(() => {
		resetG();
		g.setWithoutSavingToDB("defaultStadiumCapacity", 25000);
	});

	const baseTeam = {
		tid: 0,
		cid: 0,
		did: 0,
		region: "LA",
		name: "Lakers",
		abbrev: "LAL",
		pop: 1,
		popRank: 1,
	};

	test("preserves the sportsbook wallet (balance + bets + history)", () => {
		const bet: SportsbookBet = {
			betID: 1,
			season: 2026,
			placedAt: 0,
			americanOdds: 150,
			decimalOdds: 2.5,
			stake: 1000,
			label: "Lakers ML",
			market: { type: "gameMoneyline", gid: 5, pickTid: 0 },
		};
		const sportsbook = {
			balance: 1_234_567,
			bets: [bet],
			history: [{ ...bet, betID: 2, result: "won" as const, settledAt: 1 }],
		};

		const t = generate({ ...baseTeam, sportsbook } as any);

		assert.deepStrictEqual(
			t.sportsbook,
			sportsbook,
			"sportsbook wallet must survive import",
		);
	});

	test("preserves court customization", () => {
		const court = { midcourtLogo: "abc", sidelineColor: "#123456" };
		const t = generate({ ...baseTeam, court } as any);
		assert.deepStrictEqual(t.court, court, "court must survive import");
	});

	test("omits both when absent (a brand-new team has neither)", () => {
		const t = generate({ ...baseTeam } as any);
		assert.strictEqual(t.sportsbook, undefined);
		assert.strictEqual(t.court, undefined);
	});
});
