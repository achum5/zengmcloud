import { assert, describe, test } from "vitest";
import {
	isCountVariable,
	labelForTerm,
	labelForVariable,
	splitStatSuffix,
} from "./goatLabels.ts";

const LABELLER = {
	awards: {
		MVP: "Most Valuable Player",
		NBA1: "First Team All-League",
		DEF2: "Second Team All-Defensive",
	},
	stats: {
		per: "Player Efficiency Rating",
		ewa: "Estimated Wins Added",
		ortg: "Offensive Rating",
		drtg: "Defensive Rating",
		obpm: "Offensive Box Plus-Minus",
		dbpm: "Defensive Box Plus-Minus",
		tpa: "Three Pointers Attempted",
		tp: "Three Pointers Made",
	},
	short: {
		per: "PER",
		ewa: "EWA",
		ortg: "ORtg",
		drtg: "DRtg",
		obpm: "OBPM",
		dbpm: "DBPM",
		tpa: "3PA",
		tp: "3P",
	},
};

describe("stat name suffixes", () => {
	test("splits off the qualifier", () => {
		assert.deepStrictEqual(splitStatSuffix("ewaPlayoffs"), {
			base: "ewa",
			qualifier: "playoffs",
		});
		assert.deepStrictEqual(splitStatSuffix("ewaPeak"), {
			base: "ewa",
			qualifier: "peak season",
		});
	});

	test("prefers the longer suffix", () => {
		assert.deepStrictEqual(splitStatSuffix("ptsPlayoffsPerGame"), {
			base: "pts",
			qualifier: "playoffs, per game",
		});
	});

	test("leaves a plain stat alone", () => {
		assert.deepStrictEqual(splitStatSuffix("ewa"), { base: "ewa" });
	});

	test("does not eat a name that is only a suffix", () => {
		assert.deepStrictEqual(splitStatSuffix("Peak"), { base: "Peak" });
	});
});

describe("what counts as a count", () => {
	test("awards and honors are counted", () => {
		assert.isTrue(isCountVariable("awards.MVP"));
		assert.isTrue(isCountVariable("champ"));
		assert.isTrue(isCountVariable("allStar"));
	});

	test("stats are measured, not counted", () => {
		assert.isFalse(isCountVariable("per"));
		assert.isFalse(isCountVariable("ewaPeak"));
	});
});

describe("labelling one variable", () => {
	test("names an award the way the league names it", () => {
		assert.strictEqual(
			labelForVariable("awards.NBA1", LABELLER),
			"First Team All-League",
		);
	});

	test("falls back to the abbrev for an award that no longer exists", () => {
		assert.strictEqual(labelForVariable("awards.GONE", LABELLER), "GONE");
	});

	test("spells out a stat and says how it was measured", () => {
		assert.strictEqual(
			labelForVariable("ewaPlayoffs", LABELLER),
			"Estimated Wins Added (playoffs)",
		);
	});

	test("names the simple honors", () => {
		assert.strictEqual(labelForVariable("champ", LABELLER), "Champion");
	});

	test("seasons played is measured, not counted", () => {
		// "14x Seasons played" reads worse than "Seasons played: 14"
		assert.isFalse(isCountVariable("numSeasons"));
		assert.strictEqual(
			labelForVariable("numSeasons", LABELLER),
			"Seasons played",
		);
	});
});

describe("labelling a term with several variables", () => {
	test("keeps the operator, drops the scaling", () => {
		assert.strictEqual(
			labelForTerm("(ortg - drtg) /10", ["ortg", "drtg"], LABELLER),
			"ORtg - DRtg",
		);
	});

	test("says a shared qualifier once", () => {
		assert.strictEqual(
			labelForTerm(
				"(obpmPeak + dbpmPeak) /10",
				["obpmPeak", "dbpmPeak"],
				LABELLER,
			),
			"OBPM + DBPM (peak season)",
		);
	});

	test("handles a leading multiplier", () => {
		assert.strictEqual(
			labelForTerm("2 * (tpa - tp)", ["tpa", "tp"], LABELLER),
			"3PA - 3P",
		);
	});

	test("a single variable still gets its full name", () => {
		assert.strictEqual(
			labelForTerm("(awards.MVP *3) / 10", ["awards.MVP"], LABELLER),
			"Most Valuable Player",
		);
	});
});
