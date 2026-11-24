import { assert, describe, test } from "vitest";
import { resetG } from "../../../test/helpers.ts";
import { g, helpers } from "../../util/index.ts";
import { featureMarquee, type DatedGame } from "./marqueeNights.ts";
import newCalendarSchedule from "./newCalendarSchedule.ts";
import { dayNumber, fromDayNumber } from "../../../common/calendarDates.ts";
import {
	isAllStarGame,
	isTradeDeadline,
} from "../../../common/specialGames.ts";

const key = (game: DatedGame) => `${game.homeTid}@${game.awayTid}`;

// What a move must keep: the same games, opening night, Christmas and the
// last night as many as they had, no team twice in a night, and no more
// crowded stretches than before (three in three nights, four in five, each
// counted on its own).
const checkSame = (before: DatedGame[], after: DatedGame[]) => {
	assert.deepStrictEqual(
		before.map(key).sort(),
		after.map(key).sort(),
		"same games",
	);
	const sizes = (games: DatedGame[]) => {
		const out = new Map<string, number>();
		for (const game of games) {
			out.set(game.date, (out.get(game.date) ?? 0) + 1);
		}
		return out;
	};
	const dates = [...sizes(before).keys()].sort();
	const fixed = [
		dates[0]!,
		dates.at(-1)!,
		...dates.filter((date) => date.endsWith("-12-25")),
	];
	for (const date of fixed) {
		assert.strictEqual(
			sizes(after).get(date),
			sizes(before).get(date),
			`${date} keeps its games`,
		);
	}
	const crowded = (games: DatedGame[]) => {
		const nights = new Map<number, number[]>();
		for (const game of games) {
			for (const tid of [game.homeTid, game.awayTid]) {
				const list = nights.get(tid) ?? [];
				list.push(dayNumber(game.date));
				nights.set(tid, list);
			}
		}
		const count = { three: 0, four: 0 };
		for (const list of nights.values()) {
			list.sort((a, b) => a - b);
			for (let i = 0; i < list.length; i++) {
				assert.notStrictEqual(list[i], list[i + 1], "a team twice a night");
				if (i + 2 < list.length && list[i + 2]! - list[i]! <= 2) {
					count.three += 1;
				}
				if (i + 3 < list.length && list[i + 3]! - list[i]! <= 4) {
					count.four += 1;
				}
			}
		}
		return count;
	};
	assert.isAtMost(crowded(after).three, crowded(before).three);
	assert.isAtMost(crowded(after).four, crowded(before).four);
};

// Eight teams playing each other home and away, each round over two nights
// (two games a night) and a night off, from opening night (Wed Oct 22,
// 2025) through Christmas and past it.
const roundRobin = () => {
	const games: DatedGame[] = [];
	const tids = [0, 1, 2, 3, 4, 5, 6, 7];
	const rounds: [number, number][][] = [];
	for (let r = 0; r < 7; r++) {
		const round: [number, number][] = [];
		const rotated = [
			tids[0]!,
			...tids.slice(1).map((_, i) => tids[1 + ((i + r) % 7)]!),
		];
		for (let i = 0; i < 4; i++) {
			round.push([rotated[i]!, rotated[7 - i]!]);
		}
		rounds.push(round);
	}
	let day = dayNumber("2025-10-22");
	for (const [f, flip] of [false, true, false, true, false].entries()) {
		for (const [r, round] of rounds.entries()) {
			for (const [i, [a, b]] of round.entries()) {
				// (Which two games go first changing round to round.)
				const date = fromDayNumber(day + ((i + r + f) % 4 < 2 ? 0 : 1));
				games.push(
					flip
						? { homeTid: b, awayTid: a, date }
						: { homeTid: a, awayTid: b, date },
				);
			}
			day += 3;
		}
	}
	return games;
};

// Teams 7 and 6 draw the most, then 5, and so on down.
const draw = new Map([0, 1, 2, 3, 4, 5, 6, 7].map((tid) => [tid, tid / 7]));

describe("featureMarquee", () => {
	test("opening night: the champion at home, against the biggest draw", () => {
		const games = roundRobin();
		const out = featureMarquee(games, { draw, champion: 2 });
		checkSame(games, out);
		const opening = out.filter((game) => game.date === "2025-10-22");
		assert.strictEqual(opening.length, 2);
		const ring = opening.find((game) => game.homeTid === 2);
		assert.ok(ring, "the champion at home");
		assert.strictEqual(ring.awayTid, 7);
		// Beside it, the biggest matchup left: 6 and 5.
		assert.ok(
			opening.some(
				(game) => [game.homeTid, game.awayTid].sort().join() === "5,6",
			),
		);
	});

	test("Christmas: bigger matchups, none of opening night's again", () => {
		const games = roundRobin();
		const pair = (game: DatedGame) =>
			[game.homeTid, game.awayTid].sort((a, b) => a - b).join();
		const night = (list: DatedGame[], date: string) =>
			list.filter((game) => game.date === date);
		const total = (list: DatedGame[]) =>
			list.reduce(
				(sum, game) => sum + draw.get(game.homeTid)! + draw.get(game.awayTid)!,
				0,
			);
		const out = featureMarquee(games, { draw });
		checkSame(games, out);
		for (const date of ["2025-10-22", "2025-12-25"]) {
			assert.strictEqual(night(out, date).length, 2);
			assert.isAbove(total(night(out, date)), total(night(games, date)));
		}
		const opening = night(out, "2025-10-22").map(pair);
		const christmas = night(out, "2025-12-25").map(pair);
		// 7 and 6, the two biggest draws, both play on Christmas.
		assert.includeMembers(christmas.join().split(","), ["6", "7"]);
		for (const p of christmas) {
			assert.notInclude(opening, p);
		}
	});

	test("nothing to move: as it was", () => {
		assert.deepStrictEqual(featureMarquee([], { draw }), []);
		const games = roundRobin();
		const same = featureMarquee(games, { draw: new Map() });
		checkSame(games, same);
	});

	test("a real season keeps its shape", () => {
		resetG();
		g.setWithoutSavingToDB("season", 2027);
		g.setWithoutSavingToDB("nbaCalendar", true);
		const teams = helpers.getTeamsDefault().map((t) => ({
			tid: t.tid,
			seasonAttrs: {
				cid: t.cid,
				did: t.did,
				abbrev: t.abbrev,
				region: t.region,
			},
		}));
		const bigDraw = new Map(teams.map((t) => [t.tid, (t.tid % 10) / 10]));
		const plain = newCalendarSchedule(teams);
		const featured = newCalendarSchedule(teams, undefined, {
			draw: bigDraw,
			champion: 0,
		});
		const regular = ({ tids, dates }: typeof plain) =>
			tids.flatMap(([homeTid, awayTid], i) =>
				isAllStarGame(homeTid, awayTid) || isTradeDeadline(homeTid, awayTid)
					? []
					: [{ homeTid, awayTid, date: dates![i]! }],
			);
		const before = regular(plain);
		const after = regular(featured);
		checkSame(before, after);
		const opening = after.filter((game) => game.date === after[0]!.date);
		assert.ok(opening.some((game) => game.homeTid === 0));
		const christmas = after.filter((game) => game.date.endsWith("-12-25"));
		assert.strictEqual(christmas.length, 5);
		// The teams that draw most (9s and 8s) on Christmas.
		const tids = christmas.flatMap((game) => [game.homeTid, game.awayTid]);
		assert.isAtLeast(tids.filter((tid) => bigDraw.get(tid)! >= 0.8).length, 5);
	});
});
