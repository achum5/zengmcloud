import { assert, describe, test } from "vitest";
import {
	clinchedTop,
	draftSlotOf,
	eliminatedFrom,
	formerTeamOf,
	newTeamOf,
	playerPast,
	standingsNewsOf,
	stretchOf,
	type CareerLine,
	type RaceRow,
} from "./recapCareer.ts";

const row = (season: number, tid: number, over: Record<string, any> = {}) => ({
	season,
	tid,
	playoffs: false,
	gp: 60,
	dd: 4,
	td: 0,
	ptsMax: [31, 1],
	trbMax: [12, 2],
	astMax: [9, 3],
	tpMax: [6, 4],
	stlMax: [4, 5],
	blkMax: [3, 6],
	...over,
});

const line = (
	day: number,
	tid: number,
	box: Record<string, number>,
	oppTid = 9,
): CareerLine => ({
	day,
	gid: day * 10,
	playoffs: false,
	tid,
	oppTid,
	row: { pts: 0, orb: 0, drb: 0, ast: 0, tp: 0, stl: 0, blk: 0, ...box },
});

describe("his career before tonight", () => {
	test("past seasons' bests and this season's earlier games", () => {
		const past = playerPast(
			{ draft: { year: 2022 }, stats: [row(2024, 1), row(2025, 1)] },
			2026,
			2020,
			[
				line(1, 1, { pts: 35, drb: 10 }),
				line(3, 1, { pts: 12, ast: 11, drb: 10, stl: 10 }),
				// Tonight and later are not his past.
				line(5, 1, { pts: 50 }),
			],
			50,
			5,
		);
		assert.deepEqual(past, {
			gp: 122,
			high: { pts: 35, reb: 12, ast: 11, tp: 6, stl: 10, blk: 3 },
			dd: 10,
			td: 1,
		});
	});

	test("a veteran who came into the league with no seasons on file has no career", () => {
		assert.isUndefined(
			playerPast({ draft: { year: 2015 }, stats: [] }, 2026, 2026, [], 1, 1),
		);
		// Drafted the summer before the league's first season: all of it is here.
		assert.isDefined(
			playerPast({ draft: { year: 2025 }, stats: [] }, 2026, 2026, [], 1, 1),
		);
	});

	test("a season without its single-game bests or double-doubles counts for nothing", () => {
		const stats = [row(2024, 1, { ptsMax: undefined }), row(2025, 1)];
		assert.isUndefined(
			playerPast({ draft: { year: 2023 }, stats }, 2026, 2020, [], 1, 1),
		);
		const noDd = [row(2025, 1, { dd: undefined })];
		assert.isUndefined(
			playerPast({ draft: { year: 2024 }, stats: noDd }, 2026, 2020, [], 1, 1),
		);
	});

	test("a real career on file from its start counts", () => {
		const past = playerPast(
			{ draft: { year: 2009 }, stats: [row(2010, 3), row(2011, 3)] },
			2026,
			2026,
			[],
			1,
			1,
		);
		assert.strictEqual(past?.gp, 120);
	});
});

describe("draft slot", () => {
	test("his first two seasons only", () => {
		const p = { draft: { year: 2025, round: 1, pick: 3 } };
		assert.deepEqual(draftSlotOf(p, 2026), { year: 2025, round: 1, pick: 3 });
		assert.isDefined(draftSlotOf(p, 2027));
		assert.isUndefined(draftSlotOf(p, 2028));
		// Next summer's draftee has not played.
		assert.isUndefined(draftSlotOf(p, 2025));
	});
});

describe("moves", () => {
	test("first game after a mid-season trade", () => {
		const player = {
			transactions: [
				{ season: 2025, tid: 4, type: "draft" },
				{ season: 2026, tid: 1, type: "trade", fromTid: 4 },
			],
		};
		const lines = [line(1, 4, {}), line(3, 4, {})];
		assert.deepEqual(newTeamOf(player, 1, 2026, lines, 50, 5), {
			fromTid: 4,
			midSeason: true,
			via: "trade",
		});
		// His second game for them is not news.
		assert.isUndefined(
			newTeamOf(player, 1, 2026, [...lines, line(4, 1, {})], 50, 5),
		);
	});

	test("first game of the season for a man signed over the summer", () => {
		const player = {
			stats: [row(2025, 7)],
			transactions: [{ season: 2025, tid: 1, type: "freeAgent" }],
		};
		assert.deepEqual(newTeamOf(player, 1, 2026, [], 10, 1), {
			fromTid: 7,
			midSeason: false,
			via: "signing",
		});
	});

	test("first meeting with the club that traded him", () => {
		const player = {
			transactions: [{ season: 2026, tid: 1, type: "trade", fromTid: 4 }],
		};
		const lines = [line(1, 4, {}, 8), line(3, 1, {}, 9)];
		assert.deepEqual(formerTeamOf(player, 1, 4, 2026, lines, 50, 5), {
			fromTid: 4,
			midSeason: true,
			via: "trade",
		});
		// Once he has faced them, it is not the first time.
		const again = [...lines, line(4, 1, {}, 4)];
		assert.isUndefined(formerTeamOf(player, 1, 4, 2026, again, 50, 5));
	});
});

describe("road trips and homestands", () => {
	const game = (gid: number, home: number, away: number, winner: number) => ({
		gid,
		day: gid,
		teams: [{ tid: home }, { tid: away }],
		won: { tid: winner, pts: 100 },
		lost: { tid: winner === home ? away : home, pts: 90 },
	});
	const games = [
		game(1, 1, 2, 1),
		game(2, 3, 1, 1),
		game(3, 4, 1, 4),
		game(4, 5, 1, 1),
	];
	const schedule = [
		{ gid: 5, day: 5, homeTid: 6, awayTid: 1 },
		{ gid: 6, day: 6, homeTid: 1, awayTid: 6 },
	];

	test("which stop of how long a trip, and the record on it", () => {
		assert.deepEqual(stretchOf(1, 4, games, schedule), {
			home: false,
			game: 3,
			games: 4,
			won: 2,
			lost: 1,
		});
		assert.deepEqual(stretchOf(1, 2, games, schedule), {
			home: false,
			game: 1,
			games: 4,
			won: 1,
			lost: 0,
		});
	});

	test("a neutral site is neither", () => {
		const neutral = [{ ...game(3, 4, 1, 4), neutralSite: true }];
		assert.isUndefined(stretchOf(1, 3, neutral));
	});
});

describe("the race", () => {
	const r = (tid: number, won: number, lost: number): RaceRow => ({
		tid,
		won,
		lost,
	});

	test("clinched only when a tie cannot cost it", () => {
		// 10 games a season. Team 1 has 8 wins; team 2 can reach 8, team 3 7.
		const group = [r(1, 8, 1), r(2, 6, 2), r(3, 5, 3)];
		assert.isFalse(clinchedTop(group, 1, 1, 10));
		assert.isTrue(clinchedTop(group, 1, 2, 10));
		assert.isFalse(eliminatedFrom(group, 3, 2, 10));
		assert.isTrue(
			eliminatedFrom([r(1, 9, 1), r(2, 8, 1), r(3, 3, 6)], 3, 2, 10),
		);
	});

	test("what the night settled", () => {
		const rules = { numGames: 10, direct: 1, reach: 2 };
		const before = [[r(1, 7, 1), r(2, 6, 2), r(3, 4, 4)]];
		const after = [[r(1, 8, 1), r(2, 6, 3), r(3, 4, 5)]];
		assert.deepEqual(standingsNewsOf(1, before, after, rules), {
			clinched: "top",
			firstToClinch: true,
		});
		assert.deepEqual(standingsNewsOf(3, before, after, rules), {
			eliminated: true,
		});
	});

	test("the first team to a round number of wins, alone", () => {
		const rules = { numGames: 82, direct: 6, reach: 10 };
		const before = [[r(1, 19, 5), r(2, 19, 6)]];
		assert.deepEqual(
			standingsNewsOf(1, before, [[r(1, 20, 5), r(2, 19, 7)]], rules),
			{ firstTo: 20 },
		);
		// Two teams there the same night: neither was first.
		assert.isUndefined(
			standingsNewsOf(1, before, [[r(1, 20, 5), r(2, 20, 6)]], rules),
		);
	});
});
