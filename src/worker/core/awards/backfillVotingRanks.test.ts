import { assert, describe, test } from "vitest";
import {
	ballotAdditions,
	type BallotCandidate,
} from "./backfillVotingRanks.ts";

const candidates = (...pids: number[]): BallotCandidate[] =>
	pids.map((pid) => ({ pid, tid: pid * 10 }));

describe("ballotAdditions", () => {
	// A season that only ever recorded the winner: the four behind him are
	// added, ranked from 2.
	test("fills in behind a lone winner", () => {
		assert.deepStrictEqual(
			ballotAdditions({
				winner: [{ pid: 7 }],
				candidates: candidates(7, 3, 9, 4, 1, 8),
			}),
			[
				{ pid: 3, tid: 30, rank: 2 },
				{ pid: 9, tid: 90, rank: 3 },
				{ pid: 4, tid: 40, rank: 4 },
				{ pid: 1, tid: 10, rank: 5 },
			],
		);
	});

	// The winner is never moved, even when the recomputation now puts somebody
	// else on top: he stays first and the rest fall in behind him.
	test("the winner keeps first place", () => {
		const additions = ballotAdditions({
			winner: [{ pid: 4 }],
			candidates: candidates(1, 2, 4, 3),
		});
		assert.deepStrictEqual(
			additions.map((row) => [row.pid, row.rank]),
			[
				[1, 2],
				[2, 3],
				[3, 4],
			],
		);
	});

	// Nothing about that season reproduces any more, so the order behind the
	// winner is not worth writing down.
	test("nothing when the winner is not a candidate", () => {
		assert.deepStrictEqual(
			ballotAdditions({
				winner: [{ pid: 99 }],
				candidates: candidates(1, 2, 3),
			}),
			[],
		);
	});

	test("nothing when the ballot is already full", () => {
		assert.deepStrictEqual(
			ballotAdditions({
				winner: [{ pid: 1 }, { pid: 2 }, { pid: 3 }, { pid: 4 }, { pid: 5 }],
				candidates: candidates(1, 2, 3, 4, 5, 6),
			}),
			[],
		);
	});

	test("nothing when the award had no winner at all", () => {
		assert.deepStrictEqual(
			ballotAdditions({ winner: [{}], candidates: candidates(1, 2) }),
			[],
		);
		assert.deepStrictEqual(
			ballotAdditions({ winner: [], candidates: candidates(1, 2) }),
			[],
		);
	});

	// A partly filled ballot picks up where it left off rather than starting
	// over, so running this twice adds nothing the second time.
	test("continues a partly filled ballot, and is idempotent", () => {
		const winner = [{ pid: 7 }, { pid: 3 }];
		const pool = candidates(7, 3, 9, 4, 1);

		const additions = ballotAdditions({ winner, candidates: pool });
		assert.deepStrictEqual(
			additions.map((row) => [row.pid, row.rank]),
			[
				[9, 3],
				[4, 4],
				[1, 5],
			],
		);

		const full = [...winner, ...additions];
		assert.deepStrictEqual(
			ballotAdditions({ winner: full, candidates: pool }),
			[],
		);
	});

	test("a short field just adds what there is", () => {
		assert.deepStrictEqual(
			ballotAdditions({ winner: [{ pid: 1 }], candidates: candidates(1, 2) }),
			[{ pid: 2, tid: 20, rank: 2 }],
		);
	});

	// A candidate with no team that season has nowhere to be credited.
	test("a candidate with no team is skipped", () => {
		assert.deepStrictEqual(
			ballotAdditions({
				winner: [{ pid: 1 }],
				candidates: [
					{ pid: 1, tid: 10 },
					{ pid: 2, tid: undefined },
					{ pid: 3, tid: 30 },
				],
			}),
			[{ pid: 3, tid: 30, rank: 2 }],
		);
	});

	// Playoff-series awards carry the series line with them, so a page can
	// still show the stats once the box scores are gone.
	test("stat overrides come along", () => {
		const additions = ballotAdditions({
			winner: [{ pid: 1 }],
			candidates: [
				{ pid: 1, tid: 10 },
				{ pid: 2, tid: 20, statOverrides: { score: 12.5, pts: 30 } },
			],
		});
		assert.deepStrictEqual(additions, [
			{ pid: 2, tid: 20, rank: 2, statOverrides: { score: 12.5, pts: 30 } },
		]);
	});
});
