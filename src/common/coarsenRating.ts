// The "hide ratings ones digit" display mode shows a rating as its tens digit
// only, so 56 reads as 5 out of 10.
//
// Coarsen at the point of DISPLAY, never before a calculation: a team overall
// built from 0-10 inputs is meaningless, and ranking players by a number with a
// tenth of the resolution makes any ordering a coin flip among everyone in the
// same decade. A view that both computes with ratings and shows them should
// fetch the true ones (`coarsenRatings: false`), do its arithmetic, and run the
// output through `coarsenPlayerForDisplay` on the way to the UI.
export const coarsenRating = (value: number): number => Math.floor(value / 10);

// Tids that mean "hasn't been drafted yet". The prospect exemption is about
// scouting a draft class, so it ends the moment a player lands on a roster.
const UNDRAFTED_TIDS = new Set([-2, -4, -5]);

// Does the "prospects exempt" option spare this player? Only an undrafted
// prospect, and only when the option is on.
export const exemptFromCoarseRatings = (
	tid: number | undefined,
	exceptProspects: boolean,
): boolean => exceptProspects && tid !== undefined && UNDRAFTED_TIDS.has(tid);

// The change to show alongside a coarsened rating. It has to be the difference
// of the two DISPLAYED values, or a 56 -> 58 bump reads as "5 (+2)".
export const coarsenRatingChange = (current: number, change: number): number =>
	coarsenRating(current) - coarsenRating(current - change);

// ovrs/pots are per-position maps ({ C: 55, PG: 41 }), not plain numbers, so a
// bare typeof check walks straight past them and they reach the screen at full
// resolution (the Depth chart and the ratings CSV both read them). Anything that
// isn't a number is returned untouched.
export const coarsenRatingValue = (value: unknown): unknown => {
	if (typeof value === "number") {
		return coarsenRating(value);
	}
	if (value && typeof value === "object" && !Array.isArray(value)) {
		const out: Record<string, unknown> = {};
		for (const [key, inner] of Object.entries(value)) {
			out[key] = typeof inner === "number" ? coarsenRating(inner) : inner;
		}
		return out;
	}
	return value;
};

// Rating attrs that are NOT 0-100 ratings and must be left alone (ages, seasons,
// ids). String/array/object attrs are skipped by the typeof check instead.
export const NO_COARSEN_RATINGS = new Set([
	"season",
	"age",
	"tid",
	"fuzz",
	"injuryIndex",
	"dovr",
	"dpot",
]);

// Year-over-year changes, and the rating each one is a change in.
const CHANGE_OF: Record<string, string> = {
	dovr: "ovr",
	dpot: "pot",
};

// One ratings row, coarsened for display. Returns a copy.
export const coarsenRatingsRow = <T extends Record<string, any>>(
	row: T,
	ratings: string[],
): T => {
	const out: Record<string, any> = { ...row };
	for (const attr of ratings) {
		const base = CHANGE_OF[attr];
		if (base !== undefined) {
			if (typeof row[attr] === "number" && typeof row[base] === "number") {
				out[attr] = coarsenRatingChange(row[base], row[attr]);
			}
		} else if (!NO_COARSEN_RATINGS.has(attr)) {
			out[attr] = coarsenRatingValue(row[attr]);
		}
	}
	return out as T;
};

// A playersPlus row coarsened for display: its ratings (one season or all of
// them) and the draft-day ovr/pot that sits on the `draft` attr.
export const coarsenPlayerForDisplay = <T extends Record<string, any>>(
	p: T,
	ratings: string[],
	// The "prospects exempt" option. Requires `tid` to have been requested.
	exceptProspects = false,
): T => {
	if (exemptFromCoarseRatings(p.tid, exceptProspects)) {
		return p;
	}
	const out: Record<string, any> = { ...p };

	if (Array.isArray(p.ratings)) {
		out.ratings = p.ratings.map((row: any) => coarsenRatingsRow(row, ratings));
	} else if (p.ratings) {
		out.ratings = coarsenRatingsRow(p.ratings, ratings);
	}

	if (p.draft) {
		const draft: Record<string, any> = { ...p.draft };
		for (const attr of ["ovr", "pot"]) {
			if (typeof draft[attr] === "number") {
				draft[attr] = coarsenRating(draft[attr]);
			}
		}
		out.draft = draft;
	}

	return out as T;
};
