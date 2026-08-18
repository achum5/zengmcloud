// MAKING GENERATED FACES LOOK LIKE A BASKETBALL LEAGUE.
//
// facesjs picks every feature uniformly at random with no idea how old the
// player is, and three things fall out of that, all of them visible on the
// draft page:
//
//  1. PROSPECTS LOOK MIDDLE-AGED. Half of all generated players get facial
//     hair, and when they do it is drawn evenly from 82 styles - so a quarter
//     of every draft class of 19-to-22-year-olds turns up in mutton chops, a
//     neckbeard, an Abraham Lincoln chin curtain or Wolverine sideburns. About
//     4 per class are also balding.
//  2. PERIOD AND NOVELTY STYLES ARE EVERYWHERE. Those same 37 styles are
//     museum pieces - great for a 1950s throwback, odd on a modern league,
//     and there are more of them than there are normal ones.
//  3. FACES LOOK SAMEY. Skin comes from a palette of two or three fixed
//     values per race and hair often from a single value, so a whole league
//     shares a handful of exact colors.
//
// This module fixes all three, and adds the thing that makes the first fix
// hold up: faces AGE. A face is generated once and kept forever, so simply
// giving prospects young faces would mean nobody in the league ever grows a
// beard. Instead, at a few threshold ages a player's look matures - facial
// hair grows in and thickens, hairlines recede - so a rookie looks like a
// rookie and a 33-year-old looks like a veteran.
//
// Aging is deliberately MONOTONIC and only fires at three ages. Re-rolling
// every preseason would have players growing and shaving a beard at random
// year after year, and would rewrite a face on every player every season -
// which in a synced league is real traffic for no benefit.

import type { FaceConfig } from "facesjs";

// The style groups, assigned by looking at all 83 rendered styles rather than
// guessing from their names. A test asserts these cover facesjs's list exactly,
// so a library update that adds styles fails loudly instead of silently
// dropping them out of circulation.
export const FACIAL_HAIR_TIERS = {
	// A 19-year-old can plausibly turn up with any of these.
	light: [
		"soul",
		"soul-stache",
		"goatee-thin",
		"goatee-thin-stache",
		"mustache-thin",
		"chin-strap",
		"chin-strapStache",
		"sideburns1",
		"sideburns2",
		"sideburns3",
		"goatee1",
		"goatee1-stache",
		"goatee2",
		"goatee3",
		"goatee4",
		"goatee4-stache",
		"goatee5",
		"goatee6",
		"goatee7",
		"goatee8",
		"goatee9",
		"goatee10",
		"goatee11",
		"goatee12",
		"goatee15",
		"goatee16",
		"goatee17",
		"goatee18",
		"goatee19",
	],
	// Connected goatees and real mustaches - reads as mid-twenties and up.
	medium: [
		"fullgoatee",
		"fullgoatee2",
		"fullgoatee3",
		"fullgoatee4",
		"fullgoatee5",
		"fullgoatee6",
		"mustache1",
		"mustache1SB1",
		"mustache1SB2",
	],
	// Full beards.
	heavy: [
		"beard1",
		"beard2",
		"beard3",
		"beard4",
		"beard5",
		"beard6",
		"beard-point",
	],
	// Mutton chops, neckbeards, chin curtains, Wolverine chops, Wilt sideburns.
	// Kept, but only as a rare touch on the oldest players - deleting them
	// outright would cost the league variety it cannot spare.
	period: [
		"harley1",
		"harley1-sb-1",
		"harley1-sb-2",
		"harley2",
		"harley2-sb-1",
		"harley2-sb-2",
		"harly3",
		"harly3-sb-1",
		"harly3-sb-2",
		"honest-abe",
		"honest-abe-stache",
		"logan",
		"loganGoatee2",
		"loganGoatee2Stache",
		"loganGoatee3",
		"loganGoatee3soul",
		"loganGoatee3soulStache",
		"loganSoul",
		"mutton",
		"muttonGoatee1",
		"muttonGoatee1Stache",
		"muttonGoatee2",
		"muttonGoatee2Stache",
		"muttonGoatee5",
		"muttonGoatee5Stache",
		"muttonSoul",
		"muttonStache",
		"muttonStacheSoul",
		"neckbeard",
		"neckbeard2",
		"neckbeard2SB1",
		"neckbeard2SB2",
		"neckbeardSB1",
		"neckbeardSB2",
		"wilt",
		"wilt-sideburns-long",
		"wilt-sideburns-short",
	],
} as const;

export type FacialHairTier = keyof typeof FACIAL_HAIR_TIERS;

// How often facial hair that already exists gets thicker at a threshold age.
// Under a half so a player's look is recognisable across their career.
const THICKEN_CHANCE = 0.35;

const TIER_ORDER: FacialHairTier[] = ["light", "medium", "heavy", "period"];

// Thinning first, then gone - so a receding hairline never grows back.
export const HAIR_THINNING = "short-bald";
export const HAIR_BALD = "bald";

export const tierOf = (facialHairId: string): FacialHairTier | undefined => {
	for (const tier of TIER_ORDER) {
		if ((FACIAL_HAIR_TIERS[tier] as readonly string[]).includes(facialHairId)) {
			return tier;
		}
	}
	return undefined;
};

// What a player of a given age should look like. Chances are per player, and
// the tier weights are relative within whatever facial hair they do have.
type AgeBand = {
	minAge: number;
	facialHair: number;
	tiers: Partial<Record<FacialHairTier, number>>;
	// Share of players this age with a visibly receding or gone hairline.
	balding: number;
	glasses: number;
};

export const FACE_AGE_BANDS: AgeBand[] = [
	{
		minAge: 0,
		facialHair: 0.25,
		tiers: { light: 1 },
		balding: 0,
		glasses: 0.02,
	},
	{
		minAge: 23,
		facialHair: 0.45,
		tiers: { light: 0.7, medium: 0.3 },
		balding: 0.02,
		glasses: 0.03,
	},
	{
		minAge: 27,
		facialHair: 0.6,
		tiers: { light: 0.4, medium: 0.35, heavy: 0.25 },
		balding: 0.06,
		glasses: 0.03,
	},
	{
		minAge: 31,
		facialHair: 0.65,
		tiers: { light: 0.3, medium: 0.33, heavy: 0.32, period: 0.05 },
		balding: 0.12,
		glasses: 0.04,
	},
];

// The ages a look matures at - the boundaries of the bands above.
export const FACE_AGE_THRESHOLDS = FACE_AGE_BANDS.slice(1).map(
	(band) => band.minAge,
);

export const bandForAge = (age: number): AgeBand => {
	let match = FACE_AGE_BANDS[0]!;
	for (const band of FACE_AGE_BANDS) {
		if (age >= band.minAge) {
			match = band;
		}
	}
	return match;
};

const pickWeighted = <T extends string>(
	weights: Partial<Record<T, number>>,
	rand: () => number,
): T => {
	const entries = Object.entries(weights) as [T, number][];
	const total = entries.reduce((sum, [, weight]) => sum + weight, 0);
	let roll = rand() * total;
	for (const [key, weight] of entries) {
		roll -= weight;
		if (roll <= 0) {
			return key;
		}
	}
	return entries.at(-1)![0];
};

const pickFrom = <T>(list: readonly T[], rand: () => number): T =>
	list[Math.floor(rand() * list.length)]!;

// A facial hair style suitable for this age, or "none".
export const facialHairForAge = (age: number, rand: () => number): string => {
	const band = bandForAge(age);
	if (rand() >= band.facialHair) {
		return "none";
	}
	const tier = pickWeighted(band.tiers, rand);
	return pickFrom(FACIAL_HAIR_TIERS[tier], rand);
};

// COLOR VARIETY. facesjs picks skin from a palette of two or three fixed values
// per race, and hair from as few as one, so a whole league shares a handful of
// exact colors and the faces blur together. Nudging the chosen color's
// lightness gives every player a shade of their own while keeping the hue - and
// therefore the palette's intent - exactly as the library chose it.
//
// Deliberately small. This is meant to break up a wall of identical faces, not
// to invent colors the palette never had.
const SKIN_JITTER = 0.06;
const HAIR_JITTER = 0.1;

const hexToRgb = (hex: string): [number, number, number] => [
	Number.parseInt(hex.slice(1, 3), 16),
	Number.parseInt(hex.slice(3, 5), 16),
	Number.parseInt(hex.slice(5, 7), 16),
];

const rgbToHex = (rgb: [number, number, number]): string =>
	`#${rgb
		.map((v) =>
			Math.max(0, Math.min(255, Math.round(v)))
				.toString(16)
				.padStart(2, "0"),
		)
		.join("")}`;

export const jitterColor = (
	hex: string,
	rand: () => number,
	amount: number,
): string => {
	if (!/^#[\da-f]{6}$/i.test(hex)) {
		return hex;
	}
	const factor = 1 + (rand() * 2 - 1) * amount;
	return rgbToHex(
		hexToRgb(hex).map((v) => v * factor) as [number, number, number],
	);
};

// Reshape a freshly generated face to suit the player's age, then give it
// colors of its own. Mutates, matching how facesjs itself is used here.
export const applyRealisticFace = (
	face: FaceConfig,
	{ age, rand = Math.random }: { age: number; rand?: () => number },
) => {
	const band = bandForAge(age);

	face.facialHair.id = facialHairForAge(age, rand);

	// Hairline. Young players are never balding; older ones may be, and a face
	// that already lost its hair keeps it lost.
	const alreadyThin =
		face.hair.id === HAIR_THINNING || face.hair.id === HAIR_BALD;
	if (band.balding === 0) {
		if (alreadyThin) {
			face.hair.id = "short";
		}
	} else if (!alreadyThin && rand() < band.balding) {
		face.hair.id = rand() < 0.5 ? HAIR_THINNING : HAIR_BALD;
	}

	if (face.glasses.id !== "none" && rand() >= band.glasses) {
		face.glasses.id = "none";
	}

	face.body.color = jitterColor(face.body.color, rand, SKIN_JITTER);
	face.hair.color = jitterColor(face.hair.color, rand, HAIR_JITTER);
};

// One year older, at one of the threshold ages: grow into the look rather than
// re-rolling it. Returns true if anything changed, so the caller only writes
// players that actually need writing.
export const ageFace = (
	face: FaceConfig,
	age: number,
	rand: () => number = Math.random,
): boolean => {
	if (!FACE_AGE_THRESHOLDS.includes(age)) {
		return false;
	}

	const band = bandForAge(age);
	let changed = false;

	const current = face.facialHair.id;
	if (current === "none") {
		// Never had any: this is the age it might come in.
		if (rand() < band.facialHair) {
			face.facialHair.id = facialHairForAge(age, rand);
			changed = face.facialHair.id !== "none";
		}
	} else {
		// Already has some: it can thicken, never thin out.
		const tier = tierOf(current);
		const available = TIER_ORDER.filter(
			(candidate) =>
				band.tiers[candidate] !== undefined &&
				(tier === undefined ||
					TIER_ORDER.indexOf(candidate) > TIER_ORDER.indexOf(tier)),
		);
		if (available.length > 0 && rand() < THICKEN_CHANCE) {
			const next = pickWeighted(
				Object.fromEntries(
					available.map((candidate) => [candidate, band.tiers[candidate]!]),
				) as Partial<Record<FacialHairTier, number>>,
				rand,
			);
			face.facialHair.id = pickFrom(FACIAL_HAIR_TIERS[next], rand);
			changed = true;
		}
	}

	// Hairlines only ever go one way.
	if (face.hair.id !== HAIR_BALD && rand() < band.balding) {
		face.hair.id = face.hair.id === HAIR_THINNING ? HAIR_BALD : HAIR_THINNING;
		changed = true;
	}

	return changed;
};
