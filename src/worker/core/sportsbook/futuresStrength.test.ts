import { assert, describe, test } from "vitest";
import { futuresStrengthFromPlayers } from "./futuresStrength.ts";
import teamOvr from "../team/ovr.ts";
import {
	futuresRatingError,
	winTotalLoad,
	FUTURES_MODEL_ERROR,
} from "./getLines.ts";

// A raw player row at one flat rating level (same shape the synergy tests
// use), plus the teamOvr-shaped row derived from it.
const rawMan = (
	pid: number,
	level: number,
	{
		value = level,
		gamesRemaining = 0,
	}: { value?: number; gamesRemaining?: number } = {},
) => {
	const ratings: any = { season: 2026, pos: "F", ovr: level };
	for (const key of [
		"hgt",
		"stre",
		"spd",
		"jmp",
		"endu",
		"ins",
		"dnk",
		"ft",
		"fg",
		"tp",
		"oiq",
		"diq",
		"drb",
		"pss",
		"reb",
	]) {
		ratings[key] = level;
	}
	return {
		pid,
		injury: { gamesRemaining },
		ratings: [ratings],
		value,
	};
};

const plusRow = (raw: ReturnType<typeof rawMan>) => ({
	pid: raw.pid,
	injury: raw.injury,
	value: raw.value,
	ratings: raw.ratings.at(-1),
});

// A 12-man roster: one star, solid rotation, thin bench.
const roster = (starGamesOut = 0) => {
	const raws = [
		rawMan(1, 75, { gamesRemaining: starGamesOut }),
		...Array.from({ length: 8 }, (_, i) => rawMan(2 + i, 55)),
		...Array.from({ length: 3 }, (_, i) => rawMan(10 + i, 40)),
	];
	return { raws, plus: raws.map(plusRow) };
};

describe("futuresStrengthFromPlayers", () => {
	test("a healthy roster is priced at full strength", () => {
		const { raws, plus } = roster();
		const s = futuresStrengthFromPlayers(plus, raws, 82);
		assert.strictEqual(s.expectedOvr, s.ovr);
		assert.ok(s.synergy !== undefined);
		assert.strictEqual(s.expectedSynergy, s.synergy);
	});

	test("a star out for the whole horizon prices like he's gone", () => {
		const { raws, plus } = roster(82);
		const s = futuresStrengthFromPlayers(plus, raws, 82);
		const withoutStar = teamOvr(
			plus.filter((p) => p.pid !== 1) as any,
			{},
		);
		assert.ok(Math.abs(s.expectedOvr - withoutStar) < 1e-9);
		assert.ok(s.expectedOvr < s.ovr);
	});

	test("a short injury dents the number by the missed fraction", () => {
		const healthy = roster();
		const brief = roster(8);
		const gone = roster(82);
		const full = futuresStrengthFromPlayers(healthy.plus, healthy.raws, 80);
		const dinged = futuresStrengthFromPlayers(brief.plus, brief.raws, 80);
		const lost = futuresStrengthFromPlayers(gone.plus, gone.raws, 80);
		const marginal = full.expectedOvr - lost.expectedOvr;
		const dent = full.expectedOvr - dinged.expectedOvr;
		// 8 of 80 games missed = 10% of the marginal value.
		assert.ok(
			Math.abs(dent - 0.1 * marginal) < 1e-9,
			`dent ${dent} vs 10% of ${marginal}`,
		);
	});

	test("an injured end-of-bench player barely moves the number", () => {
		const healthy = roster();
		const raws = [...healthy.raws];
		raws[11] = rawMan(12, 40, { gamesRemaining: 82 });
		const plus = raws.map(plusRow);
		const s = futuresStrengthFromPlayers(plus, raws, 82);
		const full = futuresStrengthFromPlayers(healthy.plus, healthy.raws, 82);
		assert.ok(full.expectedOvr - s.expectedOvr < 1);
	});
});

describe("futuresRatingError", () => {
	test("starts at the measured model error and shrinks as results blend in", () => {
		assert.ok(Math.abs(futuresRatingError(0, 13) - FUTURES_MODEL_ERROR) < 1e-9);
		assert.ok(futuresRatingError(82, 13) < futuresRatingError(0, 13));
		assert.ok(futuresRatingError(82, 13) > 0);
	});
});

describe("winTotalLoad", () => {
	const base = { gp: 0, slope: 0.03, winsSd: 4.5, sigma: 13 };
	test("charges for a full season of uncertainty, nothing for none", () => {
		const preseason = winTotalLoad({ ...base, gamesRemaining: 82 });
		assert.ok(preseason > 0.05 && preseason < 0.25, `${preseason}`);
		assert.strictEqual(winTotalLoad({ ...base, gamesRemaining: 0 }), 0);
	});

	test("a wider outcome distribution needs less protection", () => {
		const tight = winTotalLoad({ ...base, gamesRemaining: 82, winsSd: 3 });
		const wide = winTotalLoad({ ...base, gamesRemaining: 82, winsSd: 8 });
		assert.ok(tight > wide);
	});

	test("shrinks late in the season with the games left", () => {
		const early = winTotalLoad({ ...base, gamesRemaining: 82, gp: 0 });
		const late = winTotalLoad({ ...base, gamesRemaining: 12, gp: 70 });
		assert.ok(late < early / 2, `${late} vs ${early}`);
	});
});
