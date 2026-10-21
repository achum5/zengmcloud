import { assert, test } from "vitest";
import { sideMargin } from "./margin.ts";
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
