import { assert, describe, test } from "vitest";
import {
	HANDLING_PENALTY,
	SKILL_PENALTY,
	SPECIALIZE_RULES,
	specializeProspect,
	specializeRating,
} from "./specializeProspects.ts";
import type { PlayerWithoutKey } from "../../../common/types.ts";

// The shape of the curve is the whole feature, so it is pinned here rather
// than left to "it looked right in a sim".

const SKILL = SPECIALIZE_RULES.dnk!;
const HANDLING = SPECIALIZE_RULES.drb!;

describe("specializeRating", () => {
	test("the published constants produce the published penalties", () => {
		// These come from a community script tuned against real simmed leagues;
		// if a refactor ever changes them, the whole balance changes with it.
		assert.strictEqual(SKILL_PENALTY, 15);
		assert.strictEqual(HANDLING_PENALTY, 4);
	});

	test("an average rating comes out unchanged", () => {
		// The break-even point: the boost gives back exactly the penalty, which
		// is what keeps the class's overall level roughly where it was.
		assert.strictEqual(specializeRating(35, SKILL), 35);
		assert.strictEqual(specializeRating(40, HANDLING), 40);
	});

	test("good ratings get better and bad ratings get worse", () => {
		assert.strictEqual(specializeRating(60, SKILL), 71);
		assert.strictEqual(specializeRating(20, SKILL), 14);
		assert.strictEqual(specializeRating(60, HANDLING), 62);
		assert.strictEqual(specializeRating(20, HANDLING), 18);
	});

	test("the further from average, the bigger the move", () => {
		// Which is what turns a flat prospect into a shaped one rather than just
		// a slightly better or worse flat prospect.
		const near = specializeRating(45, SKILL) - 45;
		const far = specializeRating(75, SKILL) - 75;
		assert.isAbove(far, near);
	});

	test("the cap stops good ratings becoming elite ones", () => {
		assert.strictEqual(specializeRating(80, SKILL), 90);
		assert.strictEqual(specializeRating(100, SKILL), 90);
		// ft and pss cap lower.
		assert.strictEqual(specializeRating(80, SPECIALIZE_RULES.ft!), 85);
		assert.strictEqual(specializeRating(90, SPECIALIZE_RULES.pss!), 85);
	});

	test("a bad rating never goes below zero", () => {
		assert.strictEqual(specializeRating(5, SKILL), 0);
		assert.strictEqual(specializeRating(0, SKILL), 0);
	});
});

describe("specializeProspect", () => {
	const flatProspect = () =>
		({
			ratings: [
				{
					hgt: 50,
					stre: 50,
					spd: 50,
					jmp: 50,
					endu: 50,
					oiq: 50,
					diq: 50,
					ins: 50,
					dnk: 50,
					ft: 50,
					fg: 50,
					tp: 50,
					drb: 50,
					pss: 50,
					reb: 50,
				},
			],
		}) as unknown as PlayerWithoutKey;

	test("athleticism and IQ are never touched", () => {
		// Polarizing these makes broken players, not specialists - a 20-speed,
		// 20-endurance sniper is not a real archetype.
		const p = flatProspect();
		specializeProspect(p);
		const ratings = p.ratings[0] as any;
		for (const key of ["hgt", "stre", "spd", "jmp", "endu", "oiq", "diq"]) {
			assert.strictEqual(ratings[key], 50, `${key} should be untouched`);
		}
	});

	test("a lopsided prospect comes out more lopsided", () => {
		const p = flatProspect();
		const ratings = p.ratings[0] as any;
		ratings.tp = 70;
		ratings.ins = 25;
		ratings.dnk = 25;
		specializeProspect(p);

		// The sniper gets snipier and the finishing gets worse, so the gap that
		// defines the archetype widens.
		assert.isAbove(ratings.tp, 70);
		assert.isBelow(ratings.dnk, 25);
		assert.isBelow(ratings.ins, 25);
		assert.isAbove(ratings.tp - ratings.dnk, 70 - 25);
	});

	test("only the eight skill ratings move", () => {
		assert.deepStrictEqual(Object.keys(SPECIALIZE_RULES).toSorted(), [
			"dnk",
			"drb",
			"fg",
			"ft",
			"ins",
			"pss",
			"reb",
			"tp",
		]);
	});
});
