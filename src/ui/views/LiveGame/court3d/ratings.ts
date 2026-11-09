import type { Physique } from "./poses.ts";

// HOW GOOD HE IS AT IT. The ratings the 3D game plays a man by, as the live
// box score carries them (see courtRatings.ts in worker/util) - and, from them,
// how much of a thing he has: 0 next to none, 1 as good as they come. In this
// league a 35 is next to none and an 80 the very best. A game without them -
// an old replay, the tests - plays everybody the way it always did.
export type CourtRatings = Partial<
	Record<
		| "ovr"
		| "hgt"
		| "stre"
		| "spd"
		| "jmp"
		| "endu"
		| "ins"
		| "dnk"
		| "ft"
		| "fg"
		| "tp"
		| "oiq"
		| "diq"
		| "drb"
		| "pss"
		| "reb",
		number
	>
>;
export type RatingKey = keyof CourtRatings;

const LOW = 35;
const HIGH = 80;

// How much of it (0-1) the given ratings make, together - or nothing to go
// by, if he has none of them.
export const levelOf = (
	r: CourtRatings | undefined,
	keys: readonly RatingKey[],
): number | undefined => {
	if (!r) {
		return undefined;
	}
	let sum = 0;
	let n = 0;
	for (const k of keys) {
		const v = r[k];
		if (typeof v === "number") {
			sum += v;
			n += 1;
		}
	}
	if (n === 0) {
		return undefined;
	}
	return Math.min(1, Math.max(0, (sum / n - LOW) / (HIGH - LOW)));
};

// HIS BUILD, FROM WHAT HE IS (see poses.ts Physique): how strong he is; how
// quick and springy; and how much more reach his height rating says he has
// than his listed height (inches - that rating is his listed height's,
// give or take, so a man whose rating runs ahead of his height plays
// longer than he stands). Nothing, without ratings: the ordinary man's.
const HGT_RATING_0 = 66;
const HGT_RATING_INCH = (93 - 66) / 100;
export const physiqueOf = (
	r: CourtRatings | undefined,
	hgt: number | undefined,
): Physique | undefined => {
	if (!r) {
		return undefined;
	}
	const strength = levelOf(r, ["stre"]);
	const athletic = levelOf(r, ["jmp", "spd"]);
	return {
		...(strength === undefined ? {} : { strength }),
		...(athletic === undefined ? {} : { athletic }),
		...(typeof r.hgt === "number" && typeof hgt === "number"
			? { reach: HGT_RATING_0 + HGT_RATING_INCH * r.hgt - hgt }
			: {}),
	};
};
