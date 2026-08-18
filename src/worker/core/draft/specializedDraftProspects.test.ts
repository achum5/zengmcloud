import { assert, describe, test } from "vitest";
import { mockIDBLeague, resetCache, resetG } from "../../../test/helpers.ts";
import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import { draft } from "../index.ts";
import { DEFAULT_LEVEL } from "../../../common/budgetLevels.ts";
import { SPECIALIZE_RULES, specializeRating } from "./specializeProspects.ts";

// The setting has to survive the whole generation path, not just work as a
// function: prospects are generated, developed through college years, given
// bonuses and nerfs, and only then reshaped. A wiring mistake anywhere in
// there produces a normal draft class with the setting quietly doing nothing.

const SKILL_KEYS = Object.keys(SPECIALIZE_RULES);

const generateClass = async (specialized: boolean) => {
	resetG();
	await resetCache();
	idb.league = mockIDBLeague();
	g.setWithoutSavingToDB("specializedDraftProspects", specialized);

	await draft.genPlayers(g.get("season"), DEFAULT_LEVEL);
	const players = await idb.cache.players.indexGetAll(
		"playersByDraftYearRetiredYear",
		[[g.get("season")], [g.get("season"), Infinity]],
	);

	// @ts-expect-error
	idb.league = undefined;

	return players;
};

// How lopsided a prospect is: the gap between their best and worst skill.
const spread = (p: any) => {
	const values = SKILL_KEYS.map((key) => p.ratings.at(-1)[key] as number);
	return Math.max(...values) - Math.min(...values);
};

const mean = (values: number[]) =>
	values.reduce((total, value) => total + value, 0) / values.length;

describe("specializedDraftProspects", () => {
	test("defaults to off, so existing leagues are unchanged", () => {
		resetG();
		assert.strictEqual(g.get("specializedDraftProspects"), false);
	});

	test("a specialized class is still a full-size class", async () => {
		// Reshaping must not cost the draft a single player.
		const players = await generateClass(true);
		assert.strictEqual(players.length, 70);
	});

	// A REACHABILITY FINGERPRINT, which is what makes the two tests below
	// deterministic rather than statistical. Scaling a rating up by 1.44 and
	// subtracting a flat 15 cannot land on every integer - about a third of the
	// range is arithmetically unreachable (23, 26, 29, 32, ... for the skill
	// rule). So a reshaped class can NEVER contain those values, and an
	// untouched class of 70 players lands on them constantly.
	const unreachable = (key: string) => {
		const rule = SPECIALIZE_RULES[key]!;
		const reachable = new Set(
			Array.from({ length: 101 }, (_, value) => specializeRating(value, rule)),
		);
		return (value: number) => value <= rule.cap && !reachable.has(value);
	};

	test("every specialized rating is one the reshaping can actually produce", async () => {
		const players = await generateClass(true);
		for (const p of players) {
			const ratings = p.ratings.at(-1) as any;
			for (const key of SKILL_KEYS) {
				assert.isFalse(
					unreachable(key)(ratings[key]),
					`${key}=${ratings[key]} is unreachable, so ${p.firstName} ${p.lastName} was never reshaped`,
				);
			}
		}
	});

	test("prospects come out more lopsided than normal ones", async () => {
		// The actual point of the feature, measured across a whole class: the
		// average gap between a prospect's best and worst skill.
		const normal = await generateClass(false);
		const specialized = await generateClass(true);

		const normalSpread = mean(normal.map(spread));
		const specializedSpread = mean(specialized.map(spread));

		assert.isAbove(
			specializedSpread,
			normalSpread + 5,
			`specialized ${specializedSpread.toFixed(1)} vs normal ${normalSpread.toFixed(1)}`,
		);
	});

	test("turning it off leaves generation exactly as it was", async () => {
		// The off path must not merely resemble stock BBGM - it must be
		// untouched, so nobody's league changes by upgrading. A normal class
		// scatters across the values the reshaping cannot reach.
		const players = await generateClass(false);
		assert.strictEqual(players.length, 70);

		const hits = players.flatMap((p) =>
			SKILL_KEYS.filter((key) =>
				unreachable(key)((p.ratings.at(-1) as any)[key]),
			),
		);
		assert.isAbove(
			hits.length,
			0,
			"a normal class should land on values the reshaping cannot produce",
		);
	});
});
