// HOW LONG THE SHORTS WERE, season by season: where their hem comes down
// to, of the way from his hip to his knee - mid-thigh and loose in the
// forties and fifties, short and tight from the seventies to the late
// eighties, longer from the early nineties, baggy to the knee through the
// two-thousands, back up a little above it since. Between two of these the
// way from one to the next. (Everybody in a game wears his season's; a
// player's own length is for later.)

// [season (the year it ends), hem]
const ERAS: readonly (readonly [number, number])[] = [
	[1950, 0.62],
	[1962, 0.58],
	[1970, 0.5],
	[1976, 0.42],
	[1987, 0.42],
	[1991, 0.58],
	[1995, 0.8],
	[1999, 0.96],
	[2009, 0.96],
	[2015, 0.9],
	[2020, 0.84],
];

// Today's: the length the body is built with (see sculpt.ts, meshBody.ts).
export const HEM_TODAY = 0.84;

export const shortsHemFor = (season: number | undefined): number => {
	if (season === undefined || !Number.isFinite(season)) {
		return HEM_TODAY;
	}
	const first = ERAS[0]!;
	if (season <= first[0]) {
		return first[1];
	}
	for (let i = 1; i < ERAS.length; i++) {
		const [y1, h1] = ERAS[i]!;
		if (season <= y1) {
			const [y0, h0] = ERAS[i - 1]!;
			return h0 + ((h1 - h0) * (season - y0)) / (y1 - y0);
		}
	}
	return ERAS.at(-1)![1];
};

// How loose the leg of his shorts is at its hem, for a hem this long (1:
// today's) - the short shorts tight round his thigh, the long ones baggy.
export const shortsFlareFor = (hem: number): number =>
	hem <= 0.5
		? 0.82
		: hem <= HEM_TODAY
			? 0.82 + (0.18 * (hem - 0.5)) / (HEM_TODAY - 0.5)
			: 1 + (hem - HEM_TODAY);
