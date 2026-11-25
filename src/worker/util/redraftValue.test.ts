import { assert, test } from "vitest";
import { redraftValue, type RedraftContext } from "./redraftValue.ts";

const ctx: RedraftContext = {
	season: 2027,
	leftThisSeason: 0,
	numGames: 82,
	ovrMean: 47,
	ovrStd: 10,
};

const ws = (...values: number[]) =>
	new Map(values.map((value, i) => [2027 - values.length + 1 + i, value]));

test("a retired player is his seasons, with the best five counted twice", () => {
	const value = redraftValue({ bornYear: 1980, ws: ws(1, 2, 3, 4, 5, 6) }, ctx);
	assert.closeTo(value, 21 + (2 + 3 + 4 + 5 + 6), 1e-9);
});

test("a star's prime beats the same total spread over a long career", () => {
	const star = redraftValue(
		{ bornYear: 1980, ws: ws(...Array(8).fill(12)) },
		ctx,
	);
	const compiler = redraftValue(
		{ bornYear: 1980, ws: ws(...Array(16).fill(6)) },
		ctx,
	);
	assert.isAbove(star, compiler);
});

test("a young player still has his seasons to come", () => {
	// Two seasons in: a 22-year-old rated 62 against a 27-year-old with more
	// win shares so far but rated 52.
	const young = redraftValue(
		{ bornYear: 2005, ws: ws(2, 3), current: { ovr: 62, peakOvr: 62 } },
		ctx,
	);
	const older = redraftValue(
		{ bornYear: 2000, ws: ws(5, 6), current: { ovr: 52, peakOvr: 54 } },
		ctx,
	);
	assert.isAbove(young, older);

	// And more to come than if he'd retired today.
	assert.isAbove(young, redraftValue({ bornYear: 2005, ws: ws(2, 3) }, ctx));
});

test("an old player's future runs out", () => {
	const value = redraftValue(
		{ bornYear: 1987, ws: ws(8), current: { ovr: 58, peakOvr: 70 } },
		ctx,
	);
	// At 40 and fading, a season or two at most.
	assert.isBelow(value, 8 * 2 + 10);
});

test("the rest of a season under way counts as to come", () => {
	const start = redraftValue(
		{ bornYear: 2000, ws: ws(4), current: { ovr: 60, peakOvr: 60 } },
		{ ...ctx, leftThisSeason: 1 },
	);
	const end = redraftValue(
		{ bornYear: 2000, ws: ws(4), current: { ovr: 60, peakOvr: 60 } },
		ctx,
	);
	assert.isAbove(start, end);
});
