import { assert, describe, test } from "vitest";
import { performanceScore, projectPicks } from "./draftPickProjection.ts";

describe("draft pick projection without team ratings", () => {
	test("last season's record is the prior, regressed toward .500", () => {
		const good = performanceScore({
			tid: 1,
			lastSeason: { won: 60, lost: 22 },
			ovrNow: 60,
			ovrThen: 60,
		});
		const bad = performanceScore({
			tid: 2,
			lastSeason: { won: 22, lost: 60 },
			ovrNow: 60,
			ovrThen: 60,
		});
		assert.ok(good.score > 0.5 && good.score < 60 / 82);
		assert.ok(bad.score < 0.5 && bad.score > 22 / 82);
	});

	test("the roster's strength alone never orders the teams", () => {
		// Two teams with the same record and no moves since: the same score,
		// whatever their ratings are. A rating rank leaking through would put
		// the 70 above the 40.
		const a = performanceScore({
			tid: 1,
			lastSeason: { won: 41, lost: 41 },
			ovrNow: 70,
			ovrThen: 70,
		});
		const b = performanceScore({
			tid: 2,
			lastSeason: { won: 41, lost: 41 },
			ovrNow: 40,
			ovrThen: 40,
		});
		assert.strictEqual(a.score, b.score);
	});

	test("roster moves since last season move the projection, within bounds", () => {
		const base = { tid: 1, lastSeason: { won: 41, lost: 41 } };
		const improved = performanceScore({ ...base, ovrNow: 60, ovrThen: 50 });
		const gutted = performanceScore({ ...base, ovrNow: 40, ovrThen: 50 });
		const same = performanceScore({ ...base, ovrNow: 50, ovrThen: 50 });
		assert.ok(improved.score > same.score);
		assert.ok(gutted.score < same.score);
		// +10 rating points is 3 of margin, about 9% of winning percentage.
		assert.ok(Math.abs(improved.score - same.score - 0.09) < 0.001);
		// A wild change is capped.
		const absurd = performanceScore({ ...base, ovrNow: 100, ovrThen: 0 });
		assert.ok(absurd.score - same.score <= 0.15 + 1e-9);
	});

	test("a fresh league or expansion team sits at .500", () => {
		const s = performanceScore({ tid: 1, ovrNow: 55 });
		assert.strictEqual(s.score, 0.5);
		assert.strictEqual(s.tilt, 0);
	});

	test("a young roster is expected to rise in later seasons, an old one to fade", () => {
		const young = performanceScore({
			tid: 1,
			lastSeason: { won: 30, lost: 52 },
			ovrNow: 50,
			ovrThen: 50,
			avgAge: 24,
		});
		const old = performanceScore({
			tid: 2,
			lastSeason: { won: 52, lost: 30 },
			ovrNow: 50,
			ovrThen: 50,
			avgAge: 32,
		});
		assert.ok(young.tilt > 0 && old.tilt < 0);
		const records = new Map();
		// This season the old team picks later; several seasons out the young
		// team has caught it.
		const now = projectPicks([young, old], records, 82, 0);
		const later = projectPicks([young, old], records, 82, 6);
		assert.ok(now[1]! < now[2]!);
		assert.ok(later[1]! > later[2]!);
	});

	test("a league with no history projects everyone in the middle, not in team-id order", () => {
		const scores = [0, 1, 2, 3].map((tid) =>
			performanceScore({ tid, ovrNow: 40 + tid * 10 }),
		);
		const picks = projectPicks(scores, new Map(), 82, 0);
		assert.deepStrictEqual(Object.values(picks), [3, 3, 3, 3]);
	});

	test("in season, the record takes over from the prior as games are played", () => {
		const scores = [
			performanceScore({
				tid: 1,
				lastSeason: { won: 60, lost: 22 },
				ovrNow: 50,
				ovrThen: 50,
			}),
			performanceScore({
				tid: 2,
				lastSeason: { won: 22, lost: 60 },
				ovrNow: 50,
				ovrThen: 50,
			}),
		];
		// Last year's contender has collapsed; last year's cellar dweller is 20-0.
		const records = new Map([
			[1, { won: 0, lost: 20 }],
			[2, { won: 20, lost: 0 }],
		]);
		const picks = projectPicks(scores, records, 82, 0);
		assert.strictEqual(picks[1], 1);
		assert.strictEqual(picks[2], 2);
		// With nothing played yet, the prior alone decides.
		const preseason = projectPicks(scores, new Map(), 82, 0);
		assert.strictEqual(preseason[1], 2);
		assert.strictEqual(preseason[2], 1);
	});
});
