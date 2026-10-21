import { assert, test } from "vitest";
import { draftPlaceMisread, sideMargin } from "./margin.ts";
import { OUTLOOK_YEARS, type LeagueOutlook } from "./outlook.ts";

const outlook = {
	titleCurves: Array.from({ length: OUTLOOK_YEARS + 1 }, () => ({
		a: -4,
		b: 0.45,
	})),
} as unknown as LeagueOutlook;

const always = Array.from({ length: OUTLOOK_YEARS + 1 }, () => 1);
const contender = Array.from({ length: OUTLOOK_YEARS + 1 }, () => 8);
const lottery = Array.from({ length: OUTLOOK_YEARS + 1 }, () => -8);

test("a starter carries more doubt than a bench player", () => {
	const starter = sideMargin(outlook, contender, [
		{ place: 0, availability: always },
	]);
	const bench = sideMargin(outlook, contender, [
		{ place: 11, availability: always },
	]);
	assert.isAbove(starter, 5 * bench);
});

test("doubt matters most where title chances move fastest", () => {
	const moved = [{ place: 0, availability: always }];
	assert.isAbove(
		sideMargin(outlook, contender, moved),
		10 * sideMargin(outlook, lottery, moved),
	);
});

test("more players moved, more doubt - but less than adding it up", () => {
	const one = sideMargin(outlook, contender, [
		{ place: 1, availability: always },
	]);
	const two = sideMargin(outlook, contender, [
		{ place: 1, availability: always },
		{ place: 1, availability: always },
	]);
	assert.isAbove(two, one);
	assert.isBelow(two, 2 * one);
});

test("a player who will not be there carries no doubt", () => {
	const gone = Array.from({ length: OUTLOOK_YEARS + 1 }, () => 0);
	assert.strictEqual(
		sideMargin(outlook, contender, [{ place: 0, availability: gone }]),
		0,
	);
});

test("a pick's player carries doubt only once he is drafted", () => {
	const fromYear = (start: number) =>
		Array.from({ length: OUTLOOK_YEARS + 1 }, (_, y) => (y >= start ? 1 : 0));
	const soon = sideMargin(outlook, contender, [
		{ place: always.map(() => 5), availability: fromYear(1) },
	]);
	const later = sideMargin(outlook, contender, [
		{ place: always.map(() => 5), availability: fromYear(4) },
	]);
	assert.isAbove(soon, later);
	assert.isAbove(later, 0);
});

test("a pick's team is misread less once its draft order is set", () => {
	const teams = new Map(
		Array.from({ length: 30 }, (_, tid) => [
			tid,
			{
				tid,
				strength: always.map(() => tid - 15),
				title: [],
				playoffs: [],
			},
		]),
	);
	const midSeason = {
		...outlook,
		season: 2030,
		offset: 0,
		progress: 0.5,
		teams,
	} as unknown as LeagueOutlook;
	const pick = { originalTid: 15, season: 2030, round: 1, pick: 0 };
	const later = draftPlaceMisread(midSeason, { ...pick, season: 2032 });
	const now = draftPlaceMisread(midSeason, pick);
	assert.isAbove(later, now);
	assert.isAbove(now, 0);
	assert.strictEqual(draftPlaceMisread(midSeason, { ...pick, pick: 3 }), 0);
});

test("a pick's own error replaces a player's", () => {
	const place = always.map(() => 5);
	const sure = sideMargin(outlook, contender, [
		{ place, availability: always, error: always.map(() => 0) },
	]);
	assert.strictEqual(sure, 0);
});
