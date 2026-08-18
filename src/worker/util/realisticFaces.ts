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
import type { Race } from "../../common/types.ts";

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

// PER YEAR, not per threshold. Aging used to fire at three fixed ages, which
// made a career a series of three jumps and left anyone past the last one
// frozen forever. Rolling every preseason instead spreads the change across a
// career the way it actually happens - and lets a 34-year-old still change.
//
// Both are small on purpose: over a fifteen-year career they compound into a
// look that clearly evolved, while any single season usually changes nothing,
// so a player stays recognisable from one year to the next.
const THICKEN_PER_YEAR = 0.12;

// Scales the band's population share into a per-year chance of first growing
// facial hair.
const GROW_PER_YEAR = 0.25;

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

// HAIR TEXTURE FOLLOWS ANCESTRY. facesjs uses race only for color palettes
// and picks hairSTYLES uniformly, so straight flowing hair (curtains, shaggy
// cuts, emo swoops) lands on Black players at the same rate as anyone - and
// afros, dreads and hi-top fades land on players whose hair could essentially
// never grow into them. Both directions read as wrong on a roster page.
//
// Styles were classified from their rendered appearance, not their names
// (same method as the facial hair groups; "juice" and "high" turn out to be
// hi-top fades, which a name alone would never tell you):
//
//  - universal: short cuts, buzzes, fades, crops and loose curls - textures
//    and lengths anyone plausibly wears.
//  - straight: styles that need straight flowing hair.
//  - coiled: styles that need tightly coiled hair.
//
// "brown" spans the widest real range of hair (Latino, Middle Eastern, South
// Asian), so it keeps everything.
export const HAIR_TEXTURES = {
	universal: [
		"bald",
		"crop",
		"crop-fade",
		"crop-fade2",
		"curly",
		"curly2",
		"curly3",
		"curlyFade1",
		"curlyFade2",
		"fauxhawk-fade",
		"short",
		"short2",
		"short3",
		"short-bald",
		"short-fade",
		"short-fade-2",
		"spike3",
	],
	straight: [
		"emo",
		"faux-hawk",
		"hair",
		"longHair",
		"messy",
		"messy-short",
		"middle-part",
		"parted",
		"shaggy1",
		"shaggy2",
		"shortBangs",
		"spike",
		"spike2",
		"spike4",
	],
	coiled: [
		"afro",
		"afro2",
		"blowoutFade",
		"cornrows",
		"dreads",
		"high",
		"juice",
		"tall-fade",
	],
} as const;

// NOT WRONG FOR ANYONE'S HAIR - WRONG FOR A BASKETBALL LEAGUE.
//
// Texture is one axis; era is another. These four grow on plenty of real
// heads, they just do not turn up on an NBA floor: hair hanging past the jaw,
// a 2000s side fringe, two shades of skater shag. Uniform selection puts one
// on roughly one player in ten, which is how a 30-year-old small forward ends
// up with curtains down to his chin.
//
// Thinned rather than deleted, the same call made for the period facial hair:
// at this rate they show up about once every couple of rosters, which is a
// character and not a pattern. Deleting them outright would cost the league
// variety it cannot spare, and some players really do look like this.
export const HAIR_RARE: readonly string[] = [
	"longHair",
	"emo",
	"shaggy1",
	"shaggy2",
];

const RARE_HAIR = new Set<string>(HAIR_RARE);

// Share of the natural rate these keep.
const RARE_HAIR_KEEP = 0.15;

const STRAIGHT_HAIR = new Set<string>(HAIR_TEXTURES.straight);
const COILED_HAIR = new Set<string>(HAIR_TEXTURES.coiled);

export const hairAllowedForRace = (
	hairId: string,
	race: Race | undefined,
): boolean => {
	if (race === undefined) {
		// A generated relative inherits a face rather than a race: nothing to
		// judge it against.
		return true;
	}
	if (RARE_HAIR.has(hairId)) {
		// Hair worn long and loose enough to hang past the jaw. Held to the one
		// ancestry it reads as - a league owner's call after a brown-skinned
		// forward turned up in curtains - and rare even there.
		return race === "white";
	}
	if (race === "brown") {
		// The widest real range of hair (Latino, Middle Eastern, South Asian):
		// nothing left to rule out.
		return true;
	}
	if (STRAIGHT_HAIR.has(hairId)) {
		return race !== "black";
	}
	if (COILED_HAIR.has(hairId)) {
		return race === "black";
	}
	// Universal, or a style outside the male catalog - leave it be.
	return true;
};

// The pool a texture-implausible style is re-rolled from. Balding looks stay
// out of it: whether a player is balding is the age logic's decision, not a
// side effect of swapping hair texture.
export const hairPoolForRace = (race: Race): readonly string[] => {
	const pool = [
		...HAIR_TEXTURES.universal,
		...(race === "black" ? HAIR_TEXTURES.coiled : []),
		...(race === "white" || race === "asian" ? HAIR_TEXTURES.straight : []),
		...(race === "brown"
			? [...HAIR_TEXTURES.straight, ...HAIR_TEXTURES.coiled]
			: []),
	];
	return pool.filter(
		(id) => id !== HAIR_THINNING && id !== HAIR_BALD && !RARE_HAIR.has(id),
	);
};

// What a player of a given age should look like. Chances are per player, and
// the tier weights are relative within whatever facial hair they do have.
type AgeBand = {
	minAge: number;
	facialHair: number;
	tiers: Partial<Record<FacialHairTier, number>>;
	// Share of players this age with a visibly receding or gone hairline, used
	// when a face is first generated.
	balding: number;
	// PER YEAR, for a player predisposed to it, used when a face ages. Distinct
	// from `balding` above: that one describes a population at a moment, this
	// one is a hazard rate applied every preseason.
	baldingPerYear: number;
	glasses: number;
};

export const FACE_AGE_BANDS: AgeBand[] = [
	{
		minAge: 0,
		facialHair: 0.25,
		tiers: { light: 1 },
		balding: 0,
		baldingPerYear: 0,
		glasses: 0.02,
	},
	{
		minAge: 23,
		facialHair: 0.45,
		tiers: { light: 0.7, medium: 0.3 },
		balding: 0.02,
		baldingPerYear: 0.015,
		glasses: 0.03,
	},
	{
		minAge: 27,
		facialHair: 0.6,
		tiers: { light: 0.4, medium: 0.35, heavy: 0.25 },
		balding: 0.06,
		baldingPerYear: 0.03,
		glasses: 0.03,
	},
	{
		minAge: 31,
		facialHair: 0.65,
		tiers: { light: 0.3, medium: 0.33, heavy: 0.32, period: 0.05 },
		balding: 0.12,
		baldingPerYear: 0.05,
		glasses: 0.04,
	},
];

// WHO IS EVEN SUSCEPTIBLE, DECIDED ONCE PER PLAYER AND NEVER STORED.
//
// Losing your hair is not something that happens to everyone who gets old -
// plenty of players reach 35 with the same hairline they were drafted with -
// and rolling the same odds against every player every season eventually
// balds most of a long-lived league. So susceptibility is a fixed trait,
// derived from the player's id: the same player always gets the same answer,
// on every device, forever, with nothing added to the save file and nothing
// to sync.
//
// The same idea covers the guy who simply never grows a beard.
const hashPid = (pid: number, salt: number): number => {
	let x = Math.imul(pid + 1, 2654435761) + Math.imul(salt, 40503);
	x = Math.imul(x ^ (x >>> 15), 2246822507);
	x = Math.imul(x ^ (x >>> 13), 3266489909);
	return ((x ^ (x >>> 16)) >>> 0) / 4294967296;
};

// Roughly the share of men who show real hairline loss over a playing career.
export const BALDING_PRONE_SHARE = 0.4;

// And the share who stay clean-shaven whatever their age.
export const NEVER_GROWS_FACIAL_HAIR_SHARE = 0.2;

export const baldingProne = (pid: number | undefined): boolean =>
	pid !== undefined && hashPid(pid, 1) < BALDING_PRONE_SHARE;

export const growsFacialHair = (pid: number | undefined): boolean =>
	pid === undefined || hashPid(pid, 2) >= NEVER_GROWS_FACIAL_HAIR_SHARE;

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

// GOING GREY, which is the change that actually happens every year.
//
// Everything else aging does is a STEP - a beard arrives, a hairline goes,
// a line deepens - and steps are lumpy by nature: nothing for four years and
// then a man is suddenly bald. Real aging is mostly continuous, and hair
// colour is the part of it a cartoon face can carry: a few percent greyer
// every season, invisible year over year, unmistakable across a decade.
//
// It compounds toward grey rather than tracking a stored fraction, so it
// needs no extra data and can never run backwards. When it starts is a
// per-player trait like the others, which is why one 38-year-old is salt and
// pepper and the next is still jet black.
const GREY = "#a8a29a";

// Share of the way to grey per season, once it has started.
const GREY_PER_YEAR = 0.07;

export const greyOnsetAge = (pid: number | undefined): number =>
	pid === undefined ? 99 : Math.round(28 + hashPid(pid, 3) * 16);

export const greyedColor = (hex: string, fraction: number): string => {
	if (!/^#[\da-f]{6}$/i.test(hex)) {
		return hex;
	}
	const [r, g, b] = hexToRgb(hex);
	const [r2, g2, b2] = hexToRgb(GREY);
	return rgbToHex([
		r + (r2 - r) * fraction,
		g + (g2 - g) * fraction,
		b + (b2 - b) * fraction,
	]);
};

// WRINKLES, WHICH ARE THE OTHER HALF OF LOOKING OLDER.
//
// Hair was only ever half the story: a 36-year-old with a full beard and a
// receding hairline still read as 22 underneath, because facesjs assigns the
// line features at random and never touches them again. It has three, and
// rendering them shows each is a clean severity ladder:
//
//   smileLine  none -> line1..line4   nasolabial folds, shallow to deep
//   eyeLine    none -> line1..line6   under-eye lines, then crow's feet
//   miscLine   none -> forehead1..5   brow lines, one to several
//
// So a face carries a single WRINKLE LEVEL, 0 to 4, and the three ladders move
// together with it. A level is capped by age - nobody is weathered at 21 - and
// climbs at most one step a year, so it creeps rather than jumps.
//
// miscLine is shared with blush, freckles and chin marks, which are not age.
// A player who has one of those keeps it: the slot only accepts brow lines
// when it is empty or already holds them, so a freckled player still ages
// through his eyes and smile without losing what makes him recognisable.
const SMILE_LINES = ["none", "line1", "line2", "line3", "line4"] as const;
const EYE_LINES = [
	"none",
	"line1",
	"line2",
	"line3",
	"line4",
	"line5",
	"line6",
] as const;
const FOREHEAD_LINES = [
	"none",
	"forehead1",
	"forehead2",
	"forehead3",
	"forehead4",
	"forehead5",
] as const;

export const MAX_WRINKLE_LEVEL = 4;

// Where each level sits on each ladder. Smile lines lead (they show first and
// on everyone), the eyes follow, the brow last.
const LEVEL_TO_SMILE = [0, 1, 2, 3, 4];
const LEVEL_TO_EYE = [0, 0, 2, 4, 6];
const LEVEL_TO_FOREHEAD = [0, 0, 1, 3, 5];

// Deep folds are a big part of reading as old, so the size grows with the
// level too - facesjs allows 0.25 to 2.25.
const LEVEL_TO_SMILE_SIZE = [0.6, 0.9, 1.2, 1.6, 2];

const clampIndex = (index: number, length: number) =>
	Math.max(0, Math.min(length - 1, index));

// One step either way at random, so two players at the same level do not have
// identical faces.
const jittered = (index: number, length: number, rand: () => number) =>
	clampIndex(index + (rand() < 0.35 ? (rand() < 0.5 ? -1 : 1) : 0), length);

// Some faces simply weather less, at any age - the 40-year-old who still
// looks 30 is a real type, and without this everyone piles up at the maximum
// the moment they are old enough for it. A quarter of players never reach the
// last step at all.
export const WEATHERS_LESS_SHARE = 0.25;

export const weathersLess = (pid: number | undefined): boolean =>
	pid !== undefined && hashPid(pid, 4) < WEATHERS_LESS_SHARE;

// The ceiling for THIS player: his age's cap, one lower if he is one of the
// ones who never really lines.
export const wrinkleCeiling = (age: number, pid: number | undefined): number =>
	Math.max(0, wrinkleLevelForAge(age) - (weathersLess(pid) ? 1 : 0));

export const wrinkleLevelForAge = (age: number): number => {
	if (age < 23) {
		return 0;
	}
	if (age < 27) {
		return 1;
	}
	if (age < 31) {
		return 2;
	}
	if (age < 35) {
		return 3;
	}
	return MAX_WRINKLE_LEVEL;
};

// Read a face's current level back off it, so aging can advance from wherever
// a face already is - including one facesjs drew before any of this existed.
export const wrinkleLevelOf = (face: FaceConfig): number => {
	const smile = SMILE_LINES.indexOf(face?.smileLine?.id as any);
	if (smile < 0) {
		return 0;
	}
	let level = 0;
	for (const [candidate, index] of LEVEL_TO_SMILE.entries()) {
		if (index <= smile) {
			level = candidate;
		}
	}
	return level;
};

export const applyWrinkles = (
	face: FaceConfig,
	level: number,
	rand: () => number = Math.random,
) => {
	const capped = clampIndex(level, MAX_WRINKLE_LEVEL + 1);

	// The smile line is NOT jittered: it is what wrinkleLevelOf reads the level
	// back off, so nudging it would let a face drift down a step and re-age
	// the same year forever. The other two carry the variety instead.
	face.smileLine.id = SMILE_LINES[LEVEL_TO_SMILE[capped]!]!;
	face.smileLine.size =
		Math.round((LEVEL_TO_SMILE_SIZE[capped]! + (rand() * 0.4 - 0.2)) * 100) /
		100;
	face.eyeLine.id =
		EYE_LINES[jittered(LEVEL_TO_EYE[capped]!, EYE_LINES.length, rand)]!;

	// Brow lines only where the slot is free or already theirs - see above.
	const misc = face.miscLine?.id ?? "none";
	if (misc === "none" || (FOREHEAD_LINES as readonly string[]).includes(misc)) {
		face.miscLine.id =
			FOREHEAD_LINES[
				jittered(LEVEL_TO_FOREHEAD[capped]!, FOREHEAD_LINES.length, rand)
			]!;
	}
};

// Chance per year of gaining a step, when age allows one. Small enough that a
// face creeps toward its age rather than jumping there.
const WRINKLE_PER_YEAR = 0.3;

// How much the folds deepen each season on their own, between level steps.
const SMILE_CREEP_PER_YEAR = 0.05;

// How much rarer the second hairline step is than the first.
const FULLY_BALD_FACTOR = 0.4;

// WHAT ANCESTRY A FACE WAS DRAWN FOR, read back off the face itself.
//
// A player does not store his race - facesjs takes it at generation and keeps
// nothing - so applying these rules to players who ALREADY EXIST has to
// recover it. Skin color is the one durable trace: facesjs picks it from a
// small fixed palette per race, so the nearest palette entry names the race it
// came from.
//
// A heuristic, and honest about it. The lighter end of the brown palette and
// the darker end of the white/asian ones sit close together, so a face can be
// read one off. That is acceptable here - the cost is one player keeping a
// hairstyle he might not have drawn - and it is only ever used for the
// retroactive pass, never for generation, which knows the real answer.
const SKIN_PALETTES: Record<Race, readonly string[]> = {
	white: ["#f2d6cb", "#ddb7a0"],
	asian: ["#fedac7", "#f0c5a3", "#eab687"],
	brown: ["#bb876f", "#aa816f", "#a67358"],
	black: ["#ad6453", "#74453d", "#5c3937"],
};

export const inferRaceFromFace = (face: FaceConfig): Race | undefined => {
	const color = face?.body?.color;
	if (typeof color !== "string" || !/^#[\da-f]{6}$/i.test(color)) {
		return undefined;
	}
	const [r, g, b] = hexToRgb(color);
	let best: { race: Race; distance: number } | undefined;
	for (const [race, palette] of Object.entries(SKIN_PALETTES) as [
		Race,
		readonly string[],
	][]) {
		for (const entry of palette) {
			const [r2, g2, b2] = hexToRgb(entry);
			const distance = (r - r2) ** 2 + (g - g2) ** 2 + (b - b2) ** 2;
			if (!best || distance < best.distance) {
				best = { race, distance };
			}
		}
	}
	return best?.race;
};

// EVERY SEASON A PLAYER HAS ALREADY LIVED, run through the aging rules at once.
//
// Turning the setting on only affects faces generated afterwards, so an
// established league keeps a roster of unaged faces forever. This replays the
// career that already happened: normalise him to what he should have looked
// like as a rookie, then age him one year at a time up to today, handing each
// change to the caller so it can be kept as history.
//
// Deterministic per player where it matters - the balding and beard traits
// come from his id - so re-running it does not produce a different man.
export const applyFaceAgingHistory = ({
	face,
	rookieAge,
	currentAge,
	pid,
	race,
	rand = Math.random,
	onChange,
}: {
	face: FaceConfig;
	rookieAge: number;
	currentAge: number;
	pid?: number;
	race?: Race;
	rand?: () => number;
	onChange?: (age: number) => void;
}) => {
	applyRealisticFace(face, {
		age: Math.min(rookieAge, currentAge),
		race: race ?? inferRaceFromFace(face),
		rand,
	});

	for (
		let age = Math.min(rookieAge, currentAge) + 1;
		age <= currentAge;
		age++
	) {
		if (ageFace(face, age, pid, rand)) {
			onChange?.(age);
		}
	}
};

// Reshape a freshly generated face to suit the player's age, then give it
// colors of its own. Mutates, matching how facesjs itself is used here.
export const applyRealisticFace = (
	face: FaceConfig,
	{
		age,
		race,
		rand = Math.random,
	}: { age: number; race?: Race; rand?: () => number },
) => {
	const band = bandForAge(age);

	face.facialHair.id = facialHairForAge(age, rand);

	// Texture and era first, so the age-based hairline logic below judges the
	// style the player will actually keep. The rare check is second and
	// short-circuits, so an ordinary style consumes no randomness.
	if (race !== undefined) {
		const implausible = !hairAllowedForRace(face.hair.id, race);
		const overexposed = RARE_HAIR.has(face.hair.id) && rand() >= RARE_HAIR_KEEP;
		if (implausible || overexposed) {
			face.hair.id = pickFrom(hairPoolForRace(race), rand);
		}
	}

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

	// Lines to match the age. Weighted toward the low end of what the age
	// allows, so a 32-year-old is usually a little weathered and occasionally
	// a lot - the same spread real faces have.
	const ceiling = wrinkleLevelForAge(age);
	applyWrinkles(face, Math.floor(rand() * rand() * (ceiling + 1)), rand);

	face.body.color = jitterColor(face.body.color, rand, SKIN_JITTER);
	face.hair.color = jitterColor(face.hair.color, rand, HAIR_JITTER);
};

// One year older, at one of the threshold ages: grow into the look rather than
// re-rolling it. Returns true if anything changed, so the caller only writes
// players that actually need writing.
export const ageFace = (
	face: FaceConfig,
	age: number,
	pid?: number,
	rand: () => number = Math.random,
): boolean => {
	const band = bandForAge(age);
	let changed = false;

	const current = face.facialHair.id;
	if (current === "none") {
		// Never had any. Some men never will, whatever their age, so this is
		// gated on the player rather than only on the roll.
		if (growsFacialHair(pid) && rand() < band.facialHair * GROW_PER_YEAR) {
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
		if (available.length > 0 && rand() < THICKEN_PER_YEAR) {
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

	// Lines deepen with age, one step at a time and never past what the age
	// allows. Unlike the hairline this happens to everyone eventually - nobody
	// reaches 38 with the face they had at 20.
	const level = wrinkleLevelOf(face);
	if (level < wrinkleCeiling(age, pid) && rand() < WRINKLE_PER_YEAR) {
		applyWrinkles(face, level + 1, rand);
		changed = true;
	}

	// THE CONTINUOUS HALF, below. Neither of these counts as a change worth
	// recording: they move a few percent a season, so a snapshot of every one
	// would store twenty near-identical faces per career to capture something
	// only visible across a decade. The player is written back every preseason
	// regardless, so they persist either way - the history just keeps the
	// steps.
	const targetSize = LEVEL_TO_SMILE_SIZE[wrinkleCeiling(age, pid)]!;
	if (face.smileLine.size < targetSize) {
		face.smileLine.size =
			Math.round(
				Math.min(targetSize, face.smileLine.size + SMILE_CREEP_PER_YEAR) * 100,
			) / 100;
	}

	if (age >= greyOnsetAge(pid)) {
		face.hair.color = greyedColor(face.hair.color, GREY_PER_YEAR);
	}

	// Hairlines only ever go one way, and only for players who were ever going
	// to lose it. Everyone else keeps what they were drafted with, at 25 and at
	// 38 alike.
	// Losing it entirely is rarer than starting to lose it, so a man does not
	// go from a full head to bald in two seasons - the jump that reads as a
	// glitch rather than as aging.
	const baldingRate =
		face.hair.id === HAIR_THINNING
			? band.baldingPerYear * FULLY_BALD_FACTOR
			: band.baldingPerYear;
	if (baldingProne(pid) && face.hair.id !== HAIR_BALD && rand() < baldingRate) {
		face.hair.id = face.hair.id === HAIR_THINNING ? HAIR_BALD : HAIR_THINNING;
		changed = true;
	}

	return changed;
};
