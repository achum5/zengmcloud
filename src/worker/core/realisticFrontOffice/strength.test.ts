import { assert, beforeAll, test } from "vitest";
import { DEFAULT_LEVEL } from "../../../common/budgetLevels.ts";
import { PLAYER } from "../../../common/constants.ts";
import { makeSeededRandom } from "../../../common/random.ts";
import { last } from "../../../common/utils.ts";
import { resetG } from "../../../test/helpers.ts";
import { player } from "../index.ts";
import ovr from "../player/ovr.ts";
import { IMPACT_RATINGS } from "./playerImpact.ts";
import {
	expectedRosterPoints,
	getReplacement,
	playerLine,
	ROSTER_PLACES,
	rosterPoints,
	type StrengthPlayer,
	type ValueRatings,
} from "./strength.ts";

let roster: StrengthPlayer[];
let build: ValueRatings;

// The same build moved up or down until it rates as this overall.
const at = (target: number): StrengthPlayer => {
	const shifted = (shift: number) => {
		const ratings = { ...build };
		for (const key of IMPACT_RATINGS) {
			ratings[key] = build[key] + shift;
		}
		ratings.ovr = ovr(ratings as any);
		return ratings;
	};
	let lo = -100;
	let hi = 100;
	for (let i = 0; i < 40; i++) {
		const mid = (lo + hi) / 2;
		if (shifted(mid).ovr < target) {
			lo = mid;
		} else {
			hi = mid;
		}
	}
	const ratings = shifted(hi);
	return { ...playerLine(ratings), valueNoPot: ratings.ovr };
};

beforeAll(() => {
	resetG();
	const realRandom = Math.random;
	Math.random = makeSeededRandom(7);
	try {
		build = last(
			player.generate(PLAYER.UNDRAFTED, 26, 2020, true, DEFAULT_LEVEL).ratings,
		) as unknown as ValueRatings;
		roster = Array.from({ length: 13 }, (_, i) => at(64 - 2 * i));
	} finally {
		Math.random = realRandom;
	}
});

test("a point of overall is worth more the better the player", () => {
	// As the sixth man of the same roster, pinned to that place.
	const sixth = (ovr: number) =>
		rosterPoints([
			...roster.slice(0, 5),
			{ ...at(ovr), valueNoPot: roster[5]!.valueNoPot },
			...roster.slice(6),
		]);
	const low = sixth(50) - sixth(45);
	const high = sixth(75) - sixth(70);
	assert.isAbove(low, 0);
	assert.isAbove(high, 2 * low);
});

test("the order the roster comes in does not matter", () => {
	const reversed = [...roster].reverse();
	assert.closeTo(rosterPoints(reversed), rosterPoints(roster), 1e-9);
});

test("a star upgrade at the top is worth a few points a game", () => {
	const base = rosterPoints(roster);
	const better = rosterPoints([at(74), ...roster.slice(1)]);
	assert.isAbove(better - base, 2);
	assert.isBelow(better - base, 7);
});

test("a starter counts for more than the end of the bench", () => {
	const base = rosterPoints(roster);
	const starter = rosterPoints([roster[0]!, at(66), ...roster.slice(2)]) - base;
	const bench = rosterPoints([...roster.slice(0, 12), at(44)]) - base;
	assert.isAbove(bench, 0);
	assert.isAbove(starter, 5 * bench);
});

test("an empty place is filled for the minimum", () => {
	const short = roster.slice(0, 10);
	const filled = [
		...short,
		...Array.from({ length: ROSTER_PLACES - 10 }, () => getReplacement()),
	];
	assert.closeTo(rosterPoints(short), rosterPoints(filled), 1e-9);
});

test("a player we have all season counts in full", () => {
	const lines = roster.map((p) => ({ ...p, availability: 1 }));
	assert.closeTo(expectedRosterPoints(lines), rosterPoints(roster), 1e-9);
});

test("a starter out for half the season costs half of him", () => {
	const full = rosterPoints(roster);
	const without = rosterPoints(roster.slice(1));
	const lines = roster.map((p, i) => ({
		...p,
		availability: i === 0 ? 0.5 : 1,
	}));
	assert.closeTo(
		expectedRosterPoints(lines),
		full - 0.5 * (full - without),
		1e-9,
	);
	assert.isAbove(full - without, 0);
});

test("a roster of players who may not be there is priced as it would play", () => {
	const realRandom = Math.random;
	Math.random = makeSeededRandom(11);
	try {
		// Varied players, so lineups and their synergy really differ.
		const varied = Array.from({ length: 22 }, () => {
			const ratings = last(
				player.generate(PLAYER.UNDRAFTED, 26, 2020, true, DEFAULT_LEVEL)
					.ratings,
			) as unknown as ValueRatings;
			return { ...playerLine(ratings), valueNoPot: ovr(ratings as any) };
		});
		// Ten we have; twelve who probably will not be there, as in a roster
		// several seasons out.
		const lines = varied.map((p, i) => ({
			...p,
			availability: i < 10 ? 1 : 0.02 + 0.03 * (i - 10),
		}));
		const simulate = (roster: typeof lines) => {
			const random = makeSeededRandom(3);
			const draws = 4000;
			let total = 0;
			for (let d = 0; d < draws; d++) {
				total += rosterPoints(roster.filter((p) => random() < p.availability));
			}
			return total / draws;
		};
		assert.closeTo(expectedRosterPoints(lines), simulate(lines), 0.3);

		// What losing one we have is worth, as the long shots would cover.
		const without = lines.filter((_, i) => i !== 7);
		assert.closeTo(
			expectedRosterPoints(without) - expectedRosterPoints(lines),
			simulate(without) - simulate(lines),
			0.1,
		);
	} finally {
		Math.random = realRandom;
	}
});
