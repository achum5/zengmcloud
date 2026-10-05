import { assert, describe, test } from "vitest";
import { isPlayoffSeriesAward, keepSeriesWinnersOnly } from "./awards.ts";
import type { Award } from "./types.ts";

const individual = (over: Partial<Award> = {}): Award =>
	({
		shortName: "MVP",
		name: "Most Valuable Player",
		formula: "ws",
		showStats: "offense",
		winner: [1, 2, 3, 4, 5].map((pid) => ({ pid, tid: 0 })),
		...over,
	}) as Award;

const finalsMvp = individual({
	shortName: "FMVP",
	name: "Finals MVP",
	statRange: -1,
});

const conferenceFinalsMvp = individual({
	shortName: "CFMVP",
	name: "Conference Finals MVP",
	statRange: -2,
	group: { type: "playoffSeries", tids: [3, 7] },
});

describe("isPlayoffSeriesAward", () => {
	test("a series award is one scored on a single playoff series", () => {
		assert.isTrue(isPlayoffSeriesAward(finalsMvp));
		assert.isTrue(isPlayoffSeriesAward(conferenceFinalsMvp));
	});

	// The whole postseason is not a series: a playoffs MVP keeps its ballot.
	test("a regular season, playoffs or combined award is not", () => {
		assert.isFalse(isPlayoffSeriesAward(individual()));
		assert.isFalse(isPlayoffSeriesAward(individual({ statRange: "playoffs" })));
		assert.isFalse(isPlayoffSeriesAward(individual({ statRange: "combined" })));
	});
});

describe("keepSeriesWinnersOnly", () => {
	// Nine writers, one vote each: nobody finishes seventh.
	test("a series award keeps its winner and nobody else", () => {
		const [fmvp, cfmvp] = keepSeriesWinnersOnly([
			finalsMvp,
			conferenceFinalsMvp,
		]);
		assert.deepStrictEqual(fmvp!.winner, [{ pid: 1, tid: 0 }]);
		assert.deepStrictEqual(cfmvp!.winner, [{ pid: 1, tid: 0 }]);
		// Everything else about it is untouched.
		assert.strictEqual(cfmvp!.shortName, "CFMVP");
		assert.deepStrictEqual(cfmvp!.group, conferenceFinalsMvp.group);
	});

	test("every other award keeps its whole ballot", () => {
		const mvp = individual();
		const playoffsMvp = individual({
			shortName: "PMVP",
			statRange: "playoffs",
		});
		const allLeague = {
			...individual({ shortName: "ALL", name: "All-League" }),
			numTeams: 2,
			winner: [[{ pid: 1, tid: 0 }], [{ pid: 2, tid: 0 }]],
		} as Award;
		const out = keepSeriesWinnersOnly([mvp, playoffsMvp, allLeague]);
		assert.strictEqual(out[0], mvp);
		assert.strictEqual(out[1], playoffsMvp);
		assert.strictEqual(out[2], allLeague);
	});

	test("a series award that already has just a winner is left as it was", () => {
		const alone = { ...finalsMvp, winner: [{ pid: 9, tid: 2 }] } as Award;
		assert.strictEqual(keepSeriesWinnersOnly([alone])[0], alone);
	});

	test("the input is not mutated", () => {
		keepSeriesWinnersOnly([finalsMvp]);
		assert.strictEqual(finalsMvp.winner.length, 5);
	});
});
