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
// Aging is deliberately MONOTONIC. Re-rolling every preseason would have
// players growing and shaving a beard at random year after year, and would
// rewrite a face on every player every season - which in a synced league is
// real traffic for no benefit.
//
// What a career actually does to a head, and what is modelled here:
//   - facial hair arrives and thickens          (never thins)
//   - hairlines recede                          (never grow back)
//   - lines set in                              (never smooth out)
//   - the man fills out                         (never slims)
// Every one of those is one-way, which is what lets a face be stored once and
// replayed from any age without ever contradicting itself.

import type { FaceConfig } from "facesjs";
import { makeSeededRandom } from "../../common/random.ts";
import type { Race } from "../../common/types.ts";

// EVERY FACE ROLLS ON ITS OWN STREAM, seeded by a single draw from the
// caller's. How many numbers a face consumes depends on the face - one roll a
// season for a beard that has not come in, a re-roll for a style the age rules
// turned away - so drawing them straight from Math.random made every player
// generated after a face depend on the exact shape of these rules: tune a band
// and the league's rosters, and every seeded test that builds one, came out
// different. One draw per face, whatever the face does with it.
export const faceStream = (from: () => number = Math.random): (() => number) =>
	makeSeededRandom(Math.floor(from() * 4294967296));

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
	// LISTED BUT NEVER GENERATED - see NEVER_GENERATE. The list stays because
	// it is also what classifies a face that already has one.
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

// STYLES THE LEAGUE NEVER GROWS, chosen by rendering all 83 and looking.
//
// The tiers above were built to decide WHEN a style is plausible, and they do
// that well, but plausible-for-an-age is not the same question as "does this
// belong on a basketball player at all". A field report of a 23-year-old
// wearing a mustache with flared chops is what separated them: nothing about
// the age model was wrong - mustache1SB1 is a legitimate medium-tier style and
// he was old enough for the medium tier - the style itself just does not
// belong in the league.
//
// Three groups, all judged from the rendered art:
//
//  - The whole PERIOD tier. Horseshoe biker mustaches (harley*), Amish chin
//    curtains (honest-abe*), Wolverine chops (logan*), mutton chops (mutton*),
//    neckbeards and Wilt's long sideburns. These were kept as "a rare touch on
//    the oldest players" for the sake of variety; a league that draws them at
//    all is a league where they turn up, and they read as costume every time.
//  - MUSTACHE PLUS FLARED CHOPS (mustache1SB1, mustache1SB2), which are the
//    mutton-chop look wearing a medium-tier label - and the one in the report.
//  - The BEADED novelties (beard5, beard6, fullgoatee5, fullgoatee6), which
//    render as pale blue blocks hanging under the chin and read as a drawing
//    error rather than as hair.
//
// Only GENERATION is affected. A face that already has one of these keeps it -
// nothing in this file has ever taken facial hair away, and a save full of
// silently rewritten faces is worse than the styles are - so the way to be rid
// of one already in a league is the revert control in the appearance gallery.
//  - SIDEBURNS ON A CLEAN FACE (sideburns1-3). Rendered, they are two
//    strips of hair with nothing between them, a 1970s look that no player
//    wears, and the one a league owner reported.
//  - The DOTTED STUBBLE (goatee-thin, goatee-thin-stache, mustache-thin),
//    which renders as a row of stitches rather than as hair.
//  - The SOUL PATCH alone (soul, soul-stache): a real thing, but on a
//    generated face it reads as a smudge, and it was one light-tier draw in
//    five.
//  - The CHIN STRAP (chin-strap, chin-strapStache). Filed as light because it
//    is a thin line, but the line runs ear to ear around the whole jaw, so
//    rendered it is as heavy as a full beard - on a nineteen-year-old, the
//    single most aging thing a draft class could be handed. Hand-picking real
//    players' faces settled on stubble (head.shave) for the same look.
export const NEVER_GENERATE: ReadonlySet<string> = new Set([
	...FACIAL_HAIR_TIERS.period,
	"mustache1SB1",
	"mustache1SB2",
	"fullgoatee5",
	"fullgoatee6",
	"beard5",
	"beard6",
	"sideburns1",
	"sideburns2",
	"sideburns3",
	"goatee-thin",
	"goatee-thin-stache",
	"mustache-thin",
	"soul",
	"soul-stache",
	"chin-strap",
	"chin-strapStache",
]);

// The tiers as GENERATION sees them. A tier whose every member is excluded
// (period) drops out entirely rather than becoming an empty list nothing can
// be picked from.
export const GENERATED_FACIAL_HAIR = Object.fromEntries(
	Object.entries(FACIAL_HAIR_TIERS)
		.map(([tier, ids]) => [
			tier,
			(ids as readonly string[]).filter((id) => !NEVER_GENERATE.has(id)),
		])
		.filter(([, ids]) => (ids as string[]).length > 0),
) as Partial<Record<FacialHairTier, string[]>>;

// The age a generated face first draws its facial hair at. Anything a face
// has past it was grown into, one season at a time, by the same rules aging
// uses - see facialHairForAge.
export const FACIAL_HAIR_FIRST_AGE = 19;

const TIER_ORDER: FacialHairTier[] = ["light", "medium", "heavy", "period"];
// The same list, for the test that checks a beard thickens one tier at a time.
export const TIER_ORDER_FOR_TESTS: readonly FacialHairTier[] = TIER_ORDER;

// The horseshoe. Nothing here produces it any more - a player who loses his
// hair shaves his head (see ageFace) - but facesjs still draws it as one of
// its ordinary styles, and faces already carrying it have to be recognised.
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

// HAIR THAT HAS TO GO SOMEWHERE BEFORE IT GOES AWAY.
//
// Losing your hair does not turn an afro into a shaved head overnight. What
// happens first is that the hair gets CUT - the volume goes long before the
// hairline does - and only then does the razor come out. Sending a player
// straight from dreads to a bare scalp in one preseason reads as a glitch,
// not as aging, because no real head has ever done that.
//
// So these styles take an extra rung on the way down: cut back to something
// short first, and only from there to shaved. Everything not listed is
// already short enough to skip that rung.
export const HAIR_VOLUMINOUS: readonly string[] = [
	"afro",
	"afro2",
	"blowoutFade",
	"cornrows",
	"curly3",
	"dreads",
	"emo",
	"faux-hawk",
	"hair",
	"high",
	"juice",
	"longHair",
	"messy",
	"shaggy1",
	"shaggy2",
	"spike",
	"spike2",
	"spike4",
	"tall-fade",
];

const VOLUMINOUS_HAIR = new Set<string>(HAIR_VOLUMINOUS);

// The rung below, and A CUT KEEPS ITS TEXTURE. One list for everyone used to
// hand a white player growing out of a mop a tight coiled crop (short3) or a
// buzz one time in three - both read as the wrong head of hair, the same
// thing hand-picking real players' faces kept having to undo. Straight hair
// is cut to a crop, a side part or a fade; everything else to a buzz, short
// twists or a fade.
export const SHORT_CUTS_STRAIGHT: readonly string[] = [
	"short2",
	"crop",
	"crop-fade",
	"short-fade",
	"parted",
];
export const SHORT_CUTS_COILED: readonly string[] = [
	"short",
	"short3",
	"crop-fade",
	"short-fade",
	"curlyFade1",
];

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

// PERIOD CUTS. Same problem as the period facial hair, on the other end of the
// head: `high` and `juice` are hi-top fades - a real style, worn by real
// players, and essentially extinct on a modern floor. Uniform selection puts
// one on about one Black player in twelve, which is how a draft class turns up
// with three of them.
//
// Thinned rather than deleted, and NOT restricted by ancestry the way HAIR_RARE
// is: there is nothing implausible about who wears these, only about how many
// and when. At this rate they turn up as a throwback rather than as a trend,
// which is also right for a league that can be set in any era.
export const HAIR_PERIOD: readonly string[] = ["high", "juice"];

// SPIKED AND FAUX-HAWKED. Six styles between them, and uniform selection
// plus the texture re-roll - which for a white or Asian player draws from a
// pool that is 40% these - put one on more than a fifth of every non-Black
// player. Measured on a generated league: 22% of white and Asian players, 20%
// of brown, and a roster page that looked like a boy band. A real floor has a
// few. Thinned the way the period cuts are, and kept out of the re-roll pool,
// to roughly a third of that.
export const HAIR_SPIKED: readonly string[] = [
	"spike",
	"spike2",
	"spike3",
	"spike4",
	"faux-hawk",
	"fauxhawk-fade",
	// And the jagged fringe, which is the same boy band from the front.
	"shortBangs",
];

const RARE_HAIR = new Set<string>(HAIR_RARE);
const PERIOD_HAIR = new Set<string>(HAIR_PERIOD);
const SPIKED_HAIR = new Set<string>(HAIR_SPIKED);

// Share of the natural rate these keep.
const RARE_HAIR_KEEP = 0.15;
const PERIOD_HAIR_KEEP = 0.25;
const SPIKED_HAIR_KEEP = 0.3;

const STRAIGHT_HAIR = new Set<string>(HAIR_TEXTURES.straight);
const COILED_HAIR = new Set<string>(HAIR_TEXTURES.coiled);

// What this head of hair is cut down to. Read off the style itself, which
// says what texture the hair is; a universal one (curly3) cuts like coils.
export const shortCutsFor = (hairId: string): readonly string[] =>
	STRAIGHT_HAIR.has(hairId) ? SHORT_CUTS_STRAIGHT : SHORT_CUTS_COILED;

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
		(id) =>
			id !== HAIR_THINNING &&
			id !== HAIR_BALD &&
			!RARE_HAIR.has(id) &&
			!PERIOD_HAIR.has(id) &&
			!SPIKED_HAIR.has(id),
	);
};

// FEATURES THAT READ AS A CARTOON.
//
// facesjs draws every eye, mouth and nose uniformly, and a few of them are
// drawn for a cartoon: a pop-eyed stare, a bug-eyed circle, a squint of slits,
// a gritted-teeth grimace, an open-mouthed gape, a bulb nose, a Pinocchio.
// Rendered side by side - which is how these lists were made - most variants
// read as a face at rest and a handful read as an expression, and an
// expression on a headshot is what makes a roster page look like a comic
// strip. Thinned, not removed: one in a while is a character.
export const EYES_CARTOON: readonly string[] = [
	"eye1",
	"eye2",
	"eye7",
	"eye8",
	"eye10",
	"eye11",
	"eye15",
];
export const EYES_NATURAL: readonly string[] = [
	"eye3",
	"eye4",
	"eye5",
	"eye6",
	"eye9",
	"eye12",
	"eye13",
	"eye14",
	"eye16",
	"eye17",
	"eye18",
	"eye19",
];
export const MOUTHS_CARTOON: readonly string[] = [
	"angry",
	"mouth",
	"mouth7",
	"mouth8",
];
export const MOUTHS_NATURAL: readonly string[] = [
	"closed",
	"mouth2",
	"mouth3",
	"mouth4",
	"mouth5",
	"mouth6",
	"side",
	"smile-closed",
	"smile",
	"smile2",
	"smile3",
	"smile4",
	"straight",
];
export const NOSES_CARTOON: readonly string[] = [
	"honker",
	"pinocchio",
	"nose3",
	"nose7",
	"nose9",
	"nose14",
];
export const NOSES_NATURAL: readonly string[] = [
	"nose1",
	"nose2",
	"nose4",
	"nose5",
	"nose6",
	"nose8",
	"nose10",
	"nose11",
	"nose12",
	"nose13",
	"small",
];
const CARTOON_KEEP = 0.2;

// CHEEK MARKS. facesjs puts blush or freckles on about one face in seven,
// whoever he is. Two of the three are hatching - the blush is an anime blush,
// a red oval with diagonal strokes through it, and freckles2 is the same
// strokes in brown - and they read as a cartoon on any face and as a rash on
// dark skin. Dot freckles (freckles1) are real on fair skin, and some are kept
// there.
export const CHEEK_HATCHING: readonly string[] = ["blush", "freckles2"];
export const CHEEK_FRECKLES = "freckles1";
const FRECKLES_KEEP_WHITE = 0.35;

const settleCheekMarks = (
	face: FaceConfig,
	race: Race | undefined,
	rand: () => number,
) => {
	const misc = face.miscLine?.id;
	if (misc !== undefined && CHEEK_HATCHING.includes(misc)) {
		face.miscLine.id = "none";
	} else if (
		misc === CHEEK_FRECKLES &&
		race !== undefined &&
		!(race === "white" && rand() < FRECKLES_KEEP_WHITE)
	) {
		face.miscLine.id = "none";
	}
};

// THE DIALS, AWAY FROM THEIR STOPS. A brow at 20 degrees is a scowl and at
// -15 a look of alarm; a nose at 1.25 is a caricature of one. facesjs draws
// each dial uniformly across its whole range, so a fifth of every roster was
// glowering. The middle of each range is where a face at rest sits.
export const EYE_ANGLE: readonly [number, number] = [-6, 10];
export const EYEBROW_ANGLE: readonly [number, number] = [-8, 12];
export const NOSE_SIZE: readonly [number, number] = [0.6, 1.15];

// ATHLETES. Fatness is drawn uniformly from 0 to 1, and 1 is drawn for a
// much heavier man than plays professional basketball: a third of every draft
// class arrived thick-necked. Skewed toward lean, with the tail kept - a
// heavy center is a real type - and aging adds its own from there.
//
// Leaner than the first cut of this, which still averaged 0.34 and drew 30%
// of every class above 0.5, where the jaw has visibly rounded. Rendered, a
// face reads as an athlete's up to about 0.3; hand-picking the league's real
// players from their photos put almost all of them between 0.15 and 0.3. This
// averages 0.23 with 15% of draws above 0.5 - the heavy center, not a third
// of the roster.
export const fatnessForAthlete = (rand: () => number): number =>
	Math.round(Math.min(1, 0.7 * rand() ** 2) * 100) / 100;

// SKIN, ACROSS THE WHOLE RANGE.
//
// facesjs picks skin from two or three fixed values per race, so every Black
// player in a league was one of three browns and a nudge of a few percent
// was all that told them apart. These anchors span the real range for each,
// and a tone is drawn BETWEEN two neighbouring anchors rather than from the
// list, so no two players need share one. They are also what a face's race
// is read back from (see inferRaceFromFace), so the two stay in step.
//
// Wider than the first cut of this, at both ends and in the middle: the
// lightest Black skin is a caramel, the darkest is close to ebony, and a
// white player can be anywhere from very pale to an olive tan. Each list is
// ordered light to dark, which skinTone relies on.
export const SKIN_TONES: Record<Race, readonly string[]> = {
	white: ["#fbe9de", "#f5dccd", "#eccbb5", "#e0b99e", "#d3a583", "#c4936f"],
	asian: ["#ffe2cf", "#f8d2b6", "#f0c39f", "#e8b587", "#dca471", "#cf945f"],
	brown: ["#d8ab8e", "#c99a80", "#b98a70", "#a97a5f", "#966a4e", "#7f5840"],
	black: [
		"#c4866b",
		"#b07058",
		"#9a5f4b",
		"#845041",
		"#6c4237",
		"#56352e",
		"#412826",
	],
};

// HAIR COLOUR, and what it goes with.
//
// Black players' hair is black. The first cut of these lists gave them a dark
// brown as one of three anchors - "#3b2a20", as common as the black - and with
// the brightness nudge on top it rendered as plainly brown hair, and a brown
// beard to match, on a third of the league's Black players, which a league
// owner reported. Black hair under arena light is a warm near-black, not a
// brown, so the list is near-blacks only.
//
// "brown" is facesjs's lighter-brown skin, which in an American league is
// mostly lighter-skinned Black and mixed players as much as Latino or South
// Asian ones - the default USA mix draws it for a third of all players - and
// the same reddish brown read the same wrong way on them. Near-blacks too.
//
// White hair is the one real range, and facesjs drew it flat across eight
// colours: one white player in eight a redhead and one in four blond, on a
// league where red hair is a curiosity. Weighted toward the browns instead
// (HAIR_TONE_WEIGHTS), and the light colours fall away on darker, olive skin -
// a deeply tanned man with strawberry-blond hair is a combination you can
// find, and one a roster page full of them makes look like a mistake.
export const HAIR_TONES: Record<Race, readonly string[]> = {
	white: [
		"#272421",
		"#3D2314",
		"#5A3825",
		"#CC9966",
		"#2C1608",
		"#B55239",
		"#e9c67b",
		"#D7BF91",
	],
	asian: ["#272421", "#0f0902", "#2e1f16"],
	brown: ["#141110", "#1c1008", "#211c19", "#272421"],
	black: ["#141110", "#1a1613", "#211c19", "#272421"],
};

// Relative weights, in the same order as HAIR_TONES. Missing means even.
//
// For white players, measured against what a league of mostly American and
// European players looks like: dark and mid browns most of all, then black and
// light brown, blond about one in ten and red about one in thirty.
export const HAIR_TONE_WEIGHTS: Partial<Record<Race, readonly number[]>> = {
	white: [10, 20, 20, 12, 14, 4, 6, 8],
};

// The light colours - light brown, red, the two blonds - that thin out on
// darker skin. At the darkest white anchor they keep this share of their
// weight.
const LIGHT_HAIR = new Set(["#CC9966", "#B55239", "#e9c67b", "#D7BF91"]);
const LIGHT_HAIR_ON_DARKEST_SKIN = 0.15;

// `skinDepth` is where the skin sits in its race's range, 0 lightest to 1
// darkest (see skinToneAt).
export const hairTone = (
	race: Race,
	rand: () => number,
	skinDepth = 0.5,
): string => {
	const tones = HAIR_TONES[race];
	const base = HAIR_TONE_WEIGHTS[race];
	const weights = tones.map((tone, i) => {
		const w = base?.[i] ?? 1;
		return LIGHT_HAIR.has(tone)
			? w * (1 - (1 - LIGHT_HAIR_ON_DARKEST_SKIN) * skinDepth)
			: w;
	});
	const total = weights.reduce((a, b) => a + b, 0);
	let roll = rand() * total;
	for (const [i, w] of weights.entries()) {
		roll -= w;
		if (roll < 0) {
			return tones[i]!;
		}
	}
	return tones.at(-1)!;
};

// What a player of a given age should look like.
type AgeBand = {
	minAge: number;
	// PER YEAR, for a clean-shaven man who is going to grow some at all (see
	// growsFacialHair): the chance this is the season it comes in. Highest in
	// the early twenties, which is when it happens, and falling away after - a
	// man still clean-shaven at 30 has mostly decided to stay that way, and a
	// flat rate kept sprouting goatees on him every few seasons.
	growPerYear: number;
	// PER YEAR, the chance what he has fills out by one tier, among the tiers
	// this age allows and only as far as he will ever take it (see
	// facialHairCeiling).
	thickenPerYear: number;
	tiers: readonly FacialHairTier[];
	// Share of players this age with their hair gone, used when a face is first
	// generated.
	balding: number;
	// PER YEAR, for a player predisposed to it, used when a face ages. Distinct
	// from `balding` above: that one describes a population at a moment, this
	// one is a hazard rate applied every preseason.
	//
	// ONE successful roll is a big visual step - and it is meant to be. facesjs
	// has no gentle "slightly receding" hair: the only rung between a full head
	// and none is `short-bald`, a pronounced horseshoe, and that rung was cut.
	// Players do not wear horseshoes; a man losing his hair in the league
	// shaves it, and the horseshoe read as an old man every time it turned up.
	// So a hairline that goes, goes to a shaved head, which is also the look
	// hand-picking the real league's balding players kept arriving at.
	//
	// Something that abrupt has to wait for an age where it is not
	// surprising - see baldingOnsetAge, which gives every player his own
	// starting age of at least 28 - and these rates only ever apply past it.
	//
	// Measured over 8,000 replayed careers, share of the WHOLE league with a
	// bare head at each age, against the hand-picked faces of the league's real
	// active players. Shaved by choice counts here, because on screen it is the
	// same head:
	//
	//                  19-21 22-23 24-25 26-27 28-29 30-31 32-33 34-36 37-40
	//   real players     1%    1%    1%    2%    2%    7%    7%    8%   13%
	//   replayed         2%    3%    3%    3%    4%    5%    7%   10%   14%
	baldingPerYear: number;
	glasses: number;
};

// FACIAL HAIR, AGAINST THE REAL LEAGUE. The hand-picked faces of 800 active
// players, each matched to his headshot, against 8,000 careers replayed
// through these bands. "Beard" is the heavy tier; the hand-picks drew a
// connected goatee with the light styles, so light and medium are compared
// together:
//
//                  19-21 22-23 24-25 26-27 28-29 30-31 32-33 34-36 37-40
//   clean-shaven
//     real players  62%   49%   29%   20%   16%   22%   12%   18%    9%
//     replayed      62%   45%   34%   25%   21%   20%   21%   20%   20%
//   full beard
//     real players   9%   12%   20%   23%   35%   46%   33%   41%   70%
//     replayed       6%   11%   22%   30%   34%   37%   38%   39%   42%
//
// With every man on his own schedule (see facialHairStart and cleansUpAtAge):
// a few prospects already wear a full beard, a few men never take it past
// clean, and some clean up partway through. The old bands had facial hair
// arriving half a career late - 58% of 24-year-olds clean-shaven against 29%
// on the real floor - and then a version with no full beard anywhere before
// 23, which made every draft class baby-faced.
export const FACE_AGE_BANDS: AgeBand[] = [
	// Three bands under 23, not one: a 22-year-old should look older than an
	// 18-year-old on average, and with one band for both a draft class had no
	// age in it at all.
	{
		minAge: 0,
		growPerYear: 0.1,
		thickenPerYear: 0,
		tiers: ["light"],
		balding: 0,
		baldingPerYear: 0,
		glasses: 0.02,
	},
	{
		minAge: 20,
		growPerYear: 0.2,
		thickenPerYear: 0.3,
		tiers: ["light", "medium"],
		balding: 0,
		baldingPerYear: 0,
		glasses: 0.02,
	},
	{
		minAge: 22,
		growPerYear: 0.25,
		thickenPerYear: 0.35,
		tiers: ["light", "medium"],
		balding: 0,
		baldingPerYear: 0,
		glasses: 0.03,
	},
	{
		// The first full beards. Most of the league's filling out happens
		// across the next four seasons.
		minAge: 23,
		growPerYear: 0.25,
		thickenPerYear: 0.45,
		tiers: ["light", "medium", "heavy"],
		balding: 0.02,
		// Zero, and it stays zero: no player's onset age is below 28, so this
		// band could not reach anybody anyway (see baldingOnsetAge). Written out
		// rather than left at a small number that reads as "rarely".
		baldingPerYear: 0,
		glasses: 0.03,
	},
	{
		minAge: 27,
		growPerYear: 0.12,
		thickenPerYear: 0.22,
		tiers: ["light", "medium", "heavy"],
		balding: 0.06,
		baldingPerYear: 0.04,
		glasses: 0.03,
	},
	{
		// Settled. A veteran's look still moves, but slowly - which matters
		// most to a league of hand-picked faces, where every change a season
		// makes is a change to someone's real photo.
		minAge: 31,
		growPerYear: 0.06,
		thickenPerYear: 0.12,
		tiers: ["light", "medium", "heavy"],
		balding: 0.12,
		baldingPerYear: 0.065,
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

// BALDNESS RUNS IN FAMILIES, which is the one thing everybody knows about it
// and the one thing a per-player hash cannot express. A son whose father lost
// his hair is far more likely to lose his own, and in a league where fathers,
// sons and brothers are all walking around at once, getting that wrong is
// visible: the Jr. with a full head next to his horseshoe-haired old man.
//
// A family's disposition is read off the family's own seed, and it moves the
// player's threshold rather than deciding for him - heritable, not
// deterministic, because plenty of sons of bald men keep their hair. The two
// shares are chosen so the LEAGUE-WIDE rate is unchanged:
//   0.4 x 0.75 + 0.6 x 0.15 = 0.39
export const BALDING_PRONE_SHARE_IN_FAMILY = 0.75;
export const BALDING_PRONE_SHARE_OUT_OF_FAMILY = 0.15;

// And the share who stay clean-shaven whatever their age. Roughly one real
// veteran in six is - see FACE_AGE_BANDS - and some of those simply have not
// yet, so the trait is a little under that.
export const NEVER_GROWS_FACIAL_HAIR_SHARE = 0.1;

// WHEN IT STARTS, WHICH IS NOT AT TWENTY-THREE.
//
// Predisposition alone used to decide it: a prone player rolled the hazard
// every preseason from 23, and 2% of faces built at 23-26 were generated with
// a receding hairline outright. Both put horseshoes on twenty-six-year-olds,
// which a field report duly caught, and which no amount of lowering the rate
// fixes - a rare bad outcome is still a bad outcome when you are looking at
// the man it happened to.
//
// The trouble is the step size, and it is a facesjs limit rather than a
// modelling one: the rung below a normal cut is `short-bald`, a pronounced
// horseshoe, so a first roll does not read as "starting to lose it", it reads
// as "lost it". Something that abrupt has to be reserved for an age where it
// is not surprising.
//
// So each player gets his own onset, deterministic from his id and stored
// nowhere, and nothing at all happens before it. Late twenties at the very
// earliest, with the spread running well past the end of most careers - which
// is also why plenty of prone players still finish with their hair.
export const BALDING_ONSET_MIN_AGE = 28;
export const BALDING_ONSET_SPREAD = 12;

export const baldingOnsetAge = (pid: number | undefined): number =>
	BALDING_ONSET_MIN_AGE +
	Math.floor(hashPid(pid ?? 0, 11) * BALDING_ONSET_SPREAD);

// Is this player both predisposed AND old enough for it to have begun?
export const baldingStarted = (
	age: number,
	pid: number | undefined,
	familyPid?: number,
): boolean =>
	Number.isFinite(age) &&
	age >= baldingOnsetAge(pid) &&
	baldingProne(pid, familyPid);

export const baldingProne = (
	pid: number | undefined,
	// The family's shared seed, when this player has relatives in the league.
	familyPid?: number,
): boolean => {
	if (pid === undefined) {
		return false;
	}
	if (familyPid === undefined || familyPid === pid) {
		return hashPid(pid, 1) < BALDING_PRONE_SHARE;
	}
	const familyProne = hashPid(familyPid, 1) < BALDING_PRONE_SHARE;
	return (
		hashPid(pid, 1) <
		(familyProne
			? BALDING_PRONE_SHARE_IN_FAMILY
			: BALDING_PRONE_SHARE_OUT_OF_FAMILY)
	);
};

// One seed per family, so it does not matter whether you ask the father, the
// son or the brother - they all get the same answer about the family. The
// lowest id in the group is the only choice that is stable no matter who you
// start from.
export const familySeed = (
	pid: number | undefined,
	relatives: readonly { pid: number }[] | undefined,
): number | undefined => {
	if (
		pid === undefined ||
		!Array.isArray(relatives) ||
		relatives.length === 0
	) {
		return undefined;
	}
	let seed = pid;
	for (const relative of relatives) {
		if (typeof relative?.pid === "number" && relative.pid < seed) {
			seed = relative.pid;
		}
	}
	return seed;
};

export const growsFacialHair = (pid: number | undefined): boolean =>
	pid === undefined || hashPid(pid, 2) >= NEVER_GROWS_FACIAL_HAIR_SHARE;

// HOW FAR HE TAKES IT. Left open-ended - one roll a season until the top
// tier - every goatee in the league is a full beard waiting to happen: run
// that way at these rates, by 34 the light styles were down to one face in
// eight and beards were past half the floor, where the real league still has
// four veterans in ten in a goatee. A real veteran with one has usually had
// it for a decade and is keeping it. So each man has a ceiling, fixed from his
// id like the rest - a full beard for most, a connected goatee or mustache
// for some, and for a fifth whatever light style he first grew. It caps
// aging only: nothing already on a face is taken off it.
export const FULL_BEARD_SHARE = 0.66;
export const LIGHT_ONLY_SHARE = 0.16;

export const facialHairCeiling = (pid: number | undefined): FacialHairTier => {
	if (pid === undefined) {
		return "heavy";
	}
	const roll = hashPid(pid, 15);
	if (roll < FULL_BEARD_SHARE) {
		return "heavy";
	}
	return roll < 1 - LIGHT_ONLY_SHARE ? "medium" : "light";
};

// A SHAVED HEAD IS A HAIRSTYLE, NOT A DIAGNOSIS.
//
// The age rules used to treat `bald` purely as the end state of hair loss, and
// forced it off anyone too young to have lost any - so no player in the league
// could ever turn up with his head shaved before about 27. In basketball that
// is backwards: the clean shave is one of the sport's signature looks and it
// is worn by choice, at every age, by men with perfectly good hairlines.
//
// So it gets its own trait. A player who shaves does it at a fixed age and
// keeps it, which is both how it usually goes and what stops a head
// flickering between shaved and not from season to season.
//
// A small one. At one player in ten, shaving from 21 to 28, a tenth of the
// league's 26-year-olds had bare heads, where the real league's active
// players have one in fifty under 30 - the clean shave is a veteran's look
// far more than a young man's now. facesjs already draws `bald` for about one
// face in forty at any age, which covers the young ones; this adds a few more,
// spread from the early twenties into the thirties.
export const SHAVES_HEAD_SHARE = 0.03;
export const SHAVES_HEAD_FROM_AGE = 22;
export const SHAVES_HEAD_SPREAD = 11;

export const shavesHead = (pid: number | undefined): boolean =>
	pid !== undefined && hashPid(pid, 3) < SHAVES_HEAD_SHARE;

export const shavesHeadAtAge = (pid: number | undefined): number =>
	SHAVES_HEAD_FROM_AGE +
	Math.floor(hashPid(pid ?? 0, 10) * (SHAVES_HEAD_SPREAD + 1));

// A CUT, SOMEWHERE IN HIS THIRTIES.
//
// Volume is a young man's hair. Measured over a generated league, the share
// of 38-year-olds wearing an afro, dreads, a hi-top or a mop was the same as
// the share of 22-year-olds - hair carried no age signal at all - and no floor
// looks like that. Half of players cut it back to something short at some
// point after their late twenties and keep it short; the other half wear what
// they always wore, which is also true to life. Who cuts is a fixed trait
// from the id, like the rest, and the cut itself is one of the ordinary short
// styles - the same rung the balding ladder takes first, for its own reasons -
// in the texture he already has (see shortCutsFor).
export const CUTS_HAIR_SHARE = 0.5;
export const CUTS_HAIR_FROM_AGE = 29;

// And the same for the haircut: the earliest age anyone cuts it back, plus a
// few years of his own, so the league does not visit the barber together.
//
// Its own salt. It used to read the same hash as cutsHair, and cutsHair only
// says yes below one half - so every player who cuts drew from the bottom
// half of the spread, and the whole league's cuts landed between 29 and 31.
export const CUTS_HAIR_SPREAD = 5;

export const cutsHairFromAge = (pid: number | undefined): number =>
	CUTS_HAIR_FROM_AGE +
	(pid === undefined
		? 0
		: Math.floor(hashPid(pid, 14) * (CUTS_HAIR_SPREAD + 1)));
const CUT_PER_YEAR = 0.12;

export const cutsHair = (pid: number | undefined): boolean =>
	pid !== undefined && hashPid(pid, 12) < CUTS_HAIR_SHARE;

// The shadow a shaved scalp leaves. facesjs generates this itself on a quarter
// of faces, anywhere in 0 to 0.2; a head that has just been shaved and has
// none reads as hairless rather than shaved, so it gets one.
export const SHAVED_SCALP_SHADOW = 0.15;

const shaveAlpha = (shave: string | undefined): number => {
	const match = /rgba\((?:\s*\d+\s*,){3}\s*([\d.]+)\s*\)/.exec(shave ?? "");
	const value = match ? Number.parseFloat(match[1]!) : 0;
	return Number.isFinite(value) ? value : 0;
};

// Shave the head: clear anything that was hanging off the back of it, and give
// the scalp a shadow if it has none.
const shaveScalp = (face: FaceConfig) => {
	if (face.hairBg) {
		face.hairBg.id = "none";
	}
	if (face.head && shaveAlpha(face.head.shave) < 0.05) {
		face.head.shave = `rgba(0,0,0,${SHAVED_SCALP_SHADOW})`;
	}
};

export const bandForAge = (age: number): AgeBand => {
	let match = FACE_AGE_BANDS[0]!;
	for (const band of FACE_AGE_BANDS) {
		if (age >= band.minAge) {
			match = band;
		}
	}
	return match;
};

const pickFrom = <T>(list: readonly T[], rand: () => number): T =>
	list[Math.floor(rand() * list.length)]!;

// Is there anything left in this tier to grow? See GENERATED_FACIAL_HAIR.
const generatable = (tier: string): tier is FacialHairTier =>
	(GENERATED_FACIAL_HAIR[tier as FacialHairTier]?.length ?? 0) > 0;

// EVERY MAN WEARS IT HIS OWN WAY. A league where everyone arrives baby-faced
// and fills into a beard on the same schedule is the uniform thing a real
// floor is not. So on top of how far a man takes it (facialHairCeiling) and
// whether he grows any at all (growsFacialHair), two more traits, fixed from
// his id like the rest:
//
//  - WHEN IT STARTS. Most start in their early twenties. Some are already
//    wearing it at the draft - a goatee, sometimes a full beard; not every
//    nineteen-year-old is baby-faced - and some stay clean until their mid
//    twenties.
//  - WHETHER HE CLEANS UP. Some men who wear it take it off one season - they
//    shave clean, or trim back to something light - and keep that look from
//    then on. Once, at an age of his own, never back and forth.
//
// Measured against the real league's hand-picked faces (the table on
// FACE_AGE_BANDS), this is also what that table asks for: real 19-to-21-year-
// olds include a full beard about one time in eleven, and real veterans
// include a steady share who are clean-shaven by choice.
export type FacialHairStart = "early" | "typical" | "late";
export const EARLY_FACIAL_HAIR_SHARE = 0.2;
export const LATE_FACIAL_HAIR_SHARE = 0.15;
export const LATE_FACIAL_HAIR_FROM_AGE = 25;

export const facialHairStart = (pid: number | undefined): FacialHairStart => {
	if (pid === undefined) {
		return "typical";
	}
	const roll = hashPid(pid, 16);
	if (roll < EARLY_FACIAL_HAIR_SHARE) {
		return "early";
	}
	return roll < EARLY_FACIAL_HAIR_SHARE + LATE_FACIAL_HAIR_SHARE
		? "late"
		: "typical";
};

// How fast an early starter grows and fills out, at any age, and how fast a
// late starter grows in once his time comes - at least.
const EARLY_GROW_PER_YEAR = 0.35;
const EARLY_THICKEN_PER_YEAR = 0.3;
const LATE_GROW_PER_YEAR = 0.3;
// Where the schedules meet: from here an early starter grows at everyone's
// rates, and a late one three seasons after.
const SCHEDULES_MEET_AGE = 27;

// How an early starter arrives at the draft: weights for light, medium and
// heavy, over the tiers his ceiling allows.
const EARLY_LOOK_WEIGHTS: Record<FacialHairTier, number> = {
	light: 0.3,
	medium: 0.3,
	heavy: 0.4,
	period: 0,
};

// Of the men who grow any, the share who clean up once, and when.
export const CLEANS_UP_SHARE = 0.12;
export const CLEANS_UP_FROM_AGE = 26;
export const CLEANS_UP_SPREAD = 8;
// Of those, the share who shave clean; the rest trim back to something light.
export const CLEANS_UP_TO_NONE = 0.5;

export const cleansUpAtAge = (pid: number | undefined): number | undefined =>
	pid !== undefined &&
	growsFacialHair(pid) &&
	hashPid(pid, 17) < CLEANS_UP_SHARE
		? CLEANS_UP_FROM_AGE + Math.floor(hashPid(pid, 18) * (CLEANS_UP_SPREAD + 1))
		: undefined;

// What a man who cleans up changes his look to.
const cleanedUpLook = (current: string, pid: number): string => {
	if (
		hashPid(pid, 19) < CLEANS_UP_TO_NONE ||
		tierOf(current) === "light" ||
		!generatable("light")
	) {
		return "none";
	}
	return pickStable(GENERATED_FACIAL_HAIR.light!, pid, 20);
};

// The share of typical starters already wearing some at
// FACIAL_HAIR_FIRST_AGE: a thin goatee on a college freshman.
export const FIRST_FACIAL_HAIR_SHARE = 0.2;

// ONE SEASON OF IT. Growing in comes light and fills out one tier at a time,
// so a beard arrives the way beards do - over several seasons. The one way
// back is the clean-up above, which happens once. Aging and generation both
// run on this, which is what keeps the two paths to an age in agreement.
export const growFacialHair = (
	current: string,
	age: number,
	pid: number | undefined,
	rand: () => number,
): string => {
	const cleanUp = cleansUpAtAge(pid);
	if (cleanUp !== undefined && age >= cleanUp) {
		// The season he decides, and every season after: what he chose stays.
		return age === cleanUp && current !== "none"
			? cleanedUpLook(current, pid!)
			: current;
	}

	const band = bandForAge(age);
	const start = facialHairStart(pid);
	if (current === "none") {
		// Some men never grow any, whatever their age, so this is gated on the
		// player rather than only on the roll.
		if (
			!growsFacialHair(pid) ||
			!generatable("light") ||
			(start === "late" && age < LATE_FACIAL_HAIR_FROM_AGE)
		) {
			return current;
		}
		// An early starter is on an older man's schedule while he is young, and
		// a late one catches up for a few seasons once he starts. Past those
		// years everyone is on the same rates: a man still clean-shaven in his
		// thirties has mostly decided, whoever he is.
		const rate =
			start === "early" && age < SCHEDULES_MEET_AGE
				? Math.max(band.growPerYear, EARLY_GROW_PER_YEAR)
				: start === "late" && age < SCHEDULES_MEET_AGE + 3
					? Math.max(band.growPerYear, LATE_GROW_PER_YEAR)
					: band.growPerYear;
		return rand() < rate
			? pickFrom(GENERATED_FACIAL_HAIR.light!, rand)
			: current;
	}
	const tier = tierOf(current);
	const next =
		tier === undefined ? undefined : TIER_ORDER[TIER_ORDER.indexOf(tier) + 1];
	const early = start === "early";
	const youngEarly = early && age < SCHEDULES_MEET_AGE;
	return next !== undefined &&
		(early || band.tiers.includes(next)) &&
		TIER_ORDER.indexOf(next) <= TIER_ORDER.indexOf(facialHairCeiling(pid)) &&
		generatable(next) &&
		rand() <
			(youngEarly
				? Math.max(band.thickenPerYear, EARLY_THICKEN_PER_YEAR)
				: band.thickenPerYear)
		? pickFrom(GENERATED_FACIAL_HAIR[next]!, rand)
		: current;
};

// What an early starter is wearing when he arrives: anything up to how far he
// will ever take it.
const earlyLook = (
	age: number,
	pid: number | undefined,
	rand: () => number,
): string => {
	const ceiling = TIER_ORDER.indexOf(facialHairCeiling(pid));
	const tiers = (["light", "medium", "heavy"] as const).filter(
		(tier) =>
			TIER_ORDER.indexOf(tier) <= ceiling &&
			generatable(tier) &&
			// A full beard on a man drafted at seventeen is a stretch.
			(tier !== "heavy" || age >= FACIAL_HAIR_FIRST_AGE),
	);
	if (tiers.length === 0) {
		return "none";
	}
	const total = tiers.reduce((sum, tier) => sum + EARLY_LOOK_WEIGHTS[tier], 0);
	let roll = rand() * total;
	for (const tier of tiers) {
		roll -= EARLY_LOOK_WEIGHTS[tier];
		if (roll < 0) {
			return pickFrom(GENERATED_FACIAL_HAIR[tier]!, rand);
		}
	}
	return pickFrom(GENERATED_FACIAL_HAIR[tiers.at(-1)!]!, rand);
};

// A facial hair style suitable for this age, or "none": what a man drawn at
// FACIAL_HAIR_FIRST_AGE would be wearing after growing into every season
// since. The same question ageFace asks every preseason, asked all at once -
// the bands used to describe a generated face and an aged one separately, and
// the two drifted apart until a veteran built at 34 was clean-shaven 35% of
// the time against 22% for one who had aged there.
export const facialHairForAge = (
	age: number,
	rand: () => number,
	pid?: number,
): string => {
	const first = Math.min(age, FACIAL_HAIR_FIRST_AGE);
	const start = facialHairStart(pid);
	let id = "none";
	if (growsFacialHair(pid) && generatable("light")) {
		if (start === "early") {
			id = earlyLook(first, pid, rand);
		} else if (start === "typical" && rand() < FIRST_FACIAL_HAIR_SHARE) {
			id = pickFrom(GENERATED_FACIAL_HAIR.light!, rand);
		}
	}
	for (let year = first + 1; year <= age; year++) {
		id = growFacialHair(id, year, pid, rand);
	}
	return id;
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
// Light hair takes a much smaller nudge. Brightening a blond by a tenth turns
// it from hair into a highlighter - "#e9c67b" became "#f9d483", and the beard
// that grows in that colour later lit up the whole face.
const LIGHT_HAIR_JITTER = 0.03;

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

const hexLightness = (hex: string): number =>
	/^#[\da-f]{6}$/i.test(hex) ? rgbToHsl(hexToRgb(hex))[2] : 0;

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

// A tone somewhere between two neighbouring anchors, with an undertone of
// its own. Two men can share a shade and still not share a colour - one
// warmer, one greyer - and that, more than the shade, is what tells faces
// apart on a roster page. So the hue and saturation get a small nudge each,
// well inside what real skin does, on top of the lightness one applied later.
export const skinToneAt = (
	race: Race,
	rand: () => number,
): { color: string; depth: number } => {
	const tones = SKIN_TONES[race];
	const i = Math.min(tones.length - 2, Math.floor(rand() * (tones.length - 1)));
	const t = rand();
	const a = hexToRgb(tones[i]!);
	const b = hexToRgb(tones[i + 1]!);
	const mixed = a.map((v, k) => v + (b[k]! - v) * t) as [
		number,
		number,
		number,
	];
	const [h, sat, l] = rgbToHsl(mixed);
	const hue = h + (rand() * 2 - 1) * SKIN_HUE_JITTER;
	const saturation = Math.max(
		0,
		Math.min(1, sat * (1 + (rand() * 2 - 1) * SKIN_SATURATION_JITTER)),
	);
	return {
		color: rgbToHex(hslToRgb([hue, saturation, l])),
		depth: (i + t) / (tones.length - 1),
	};
};

export const skinTone = (race: Race, rand: () => number): string =>
	skinToneAt(race, rand).color;

// Degrees, and a share of the saturation.
const SKIN_HUE_JITTER = 4;
const SKIN_SATURATION_JITTER = 0.12;

const rgbToHsl = ([r, g, b]: [number, number, number]): [
	number,
	number,
	number,
] => {
	const R = r / 255;
	const G = g / 255;
	const B = b / 255;
	const max = Math.max(R, G, B);
	const min = Math.min(R, G, B);
	const l = (max + min) / 2;
	if (max === min) {
		return [0, 0, l];
	}
	const d = max - min;
	const s = l > 0.5 ? d / (2 - max - min) : d / (max + min);
	let h: number;
	if (max === R) {
		h = (G - B) / d + (G < B ? 6 : 0);
	} else if (max === G) {
		h = (B - R) / d + 2;
	} else {
		h = (R - G) / d + 4;
	}
	return [h * 60, s, l];
};

const hslToRgb = ([h, s, l]: [number, number, number]): [
	number,
	number,
	number,
] => {
	const hue = (((h % 360) + 360) % 360) / 360;
	if (s === 0) {
		return [l * 255, l * 255, l * 255];
	}
	const q = l < 0.5 ? l * (1 + s) : l + s - l * s;
	const p = 2 * l - q;
	const channel = (t0: number) => {
		let t = t0;
		if (t < 0) {
			t += 1;
		}
		if (t > 1) {
			t -= 1;
		}
		if (t < 1 / 6) {
			return p + (q - p) * 6 * t;
		}
		if (t < 1 / 2) {
			return q;
		}
		if (t < 2 / 3) {
			return p + (q - p) * (2 / 3 - t) * 6;
		}
		return p;
	};
	return [
		channel(hue + 1 / 3) * 255,
		channel(hue) * 255,
		channel(hue - 1 / 3) * 255,
	];
};

// Inside the range, or re-drawn inside it.
const inRange = (
	value: number | undefined,
	[lo, hi]: readonly [number, number],
	rand: () => number,
	integer: boolean,
): number => {
	if (typeof value === "number" && value >= lo && value <= hi) {
		return value;
	}
	const drawn = lo + rand() * (hi - lo);
	return integer ? Math.round(drawn) : Math.round(drawn * 100) / 100;
};

// GOING GREY IS GONE. It ran for a while: 40% of players, starting somewhere
// between 28 and 40, drifting 5.5% of the way to a warm grey every preseason,
// with the crossings recorded in the appearance history. It was removed on
// request - facesjs paints the beard and the scalp from one colour, so there
// was no way to grey a man the way men actually grey, and a whole league
// drifting toward the same pale hair read worse than nobody greying at all.
//
// Nothing replaces it, and nothing un-does it: a face already carrying grey
// in a save keeps it (see the note on NEVER_GENERATE - this file does not
// rewrite faces behind the user's back), and the way to put one back is the
// revert control in the appearance gallery.

// FILLING OUT. Nobody finishes a career at the weight he started it, and
// facesjs draws fatness into the jaw and neck. Small and one-way: over the
// back half of a career it is the difference between a lean twenty-two and a
// heavier thirty-six, and in any single season it is invisible.
const FATNESS_START_AGE = 27;
const FATNESS_PER_YEAR = 0.012;
const FATNESS_MAX = 1;

// THE PARTS THAT KEEP LENGTHENING. Ears and noses really do get bigger with
// age, but not because anything grows: skin and cartilage lose their
// elasticity and gravity does the rest, so the lobe stretches and the tip of
// the nose drops. The measured rate is about a fifth of a millimetre of ear
// length a year, so a nineteen-year career is worth a few millimetres on a
// sixty-millimetre ear - call it seven percent. The rates below are set to
// land there rather than at something you would notice as a change of face.
//
// They are also the thing that keeps a career from being pixel-identical
// between style changes, which is why they are wanted here at all: something
// has to move EVERY season or the whole progression reads as three or four
// events.
//
// These two round to three decimals, not two. At two a rate this small would
// round back to the same number every season, so ageFace would freeze while
// applyRealisticFace kept accumulating and the two paths would disagree. Both
// rates are exact multiples of a thousandth so the stepwise and the
// all-at-once sums come out equal.
//
// One-way and never recorded on its own, exactly like fatness.
const GROWTH_START_AGE = 19;
const EAR_GROWTH_PER_YEAR = 0.004;
const EAR_SIZE_MAX = 1.5;
const NOSE_GROWTH_PER_YEAR = 0.003;
// Bounded by the module's own plausible range, not facesjs's wider one: a
// generated nose is drawn inside NOSE_SIZE and an aged one has to stay there
// too, or a veteran ends up with a nose no draft class could produce. The
// generation test holds both paths to it.
const NOSE_SIZE_MAX = NOSE_SIZE[1];

const growthYears = (age: number): number =>
	Math.max(0, Math.floor(age) - GROWTH_START_AGE);

const roundGrowth = (v: number): number => Math.round(v * 1000) / 1000;

export const earGrowthByAge = (age: number): number =>
	growthYears(age) * EAR_GROWTH_PER_YEAR;

export const noseGrowthByAge = (age: number): number =>
	growthYears(age) * NOSE_GROWTH_PER_YEAR;

export const fatnessGainByAge = (age: number): number =>
	Math.max(0, Math.floor(age) - FATNESS_START_AGE) * FATNESS_PER_YEAR;

// THE EYES COME DOWN AT THE OUTSIDE. The outer corner of the eye sits on a
// tendon that slackens over a lifetime, so it descends while the inner corner
// stays put - the eye that read as level at twenty reads as slightly downturned
// at forty. It is a real and well-measured change, and it is the one facesjs
// dial that maps onto it: `angle` rotates each eye about its own centre,
// mirrored left to right, so a negative value drops the outer corner. Rendering
// the range settles the sign, the same way the line features were settled.
//
// The BROW angle was looked at for the same treatment and left alone. It reads
// as expression, not as age - negative is worried, positive is stern - so
// drifting it would age nobody and would slowly turn the league angry.
//
// This is also what the late thirties were missing. Measured over a career the
// steps peak around 31 and then TAPER, because the wrinkle ceiling has been
// reached and the hairline has resolved - so a man changed less at 40 than he
// did at 31, which is backwards. This runs to the end of a career and does not
// care that the steps have stopped.
const EYE_DROOP_PER_YEAR = 0.12;

// Below the generation band on purpose: a career's worth of droop has to be
// able to take a man past the most downturned eye a rookie is drawn with, or
// the players who start low simply stop aging. Rendering the range shows
// there is plenty of room before it reads as a caricature.
export const EYE_ANGLE_AGED_MIN = -10;

// The same men whose lines come in slowly hold their eyes up longer. Nobody
// ages on one schedule, and this is free variety - weathersLess is already
// fixed per player, so both paths to an age still agree.
const EYE_DROOP_WEATHERS_LESS = 0.6;

export const eyeDroopPerYear = (pid: number | undefined): number =>
	EYE_DROOP_PER_YEAR * (weathersLess(pid) ? EYE_DROOP_WEATHERS_LESS : 1);

export const eyeDroopByAge = (age: number, pid: number | undefined): number =>
	growthYears(age) * eyeDroopPerYear(pid);

// WRINKLES, WHICH ARE THE OTHER HALF OF LOOKING OLDER.
//
// THE LINE FEATURES ARE VARIANTS, NOT DEGREES, and getting that wrong is what
// made a player's face improve late in his career. facesjs offers five smile
// lines, seven eye lines and six brow lines, and the names read like severity
// ladders - line1, line2, line3 - so they were treated as ones. They are not.
// Rendering each and measuring what it actually draws settles it:
//
//   smileLine   line1 305   line2 259   line3 328   line4 252
//   miscLine    fh1 89   fh2 130   fh3 98   fh4 98   fh5 153
//
// Not monotonic in either case, because these are different SHAPES of fold and
// different ARRANGEMENTS of brow line, not the same mark getting deeper. Worse
// for the eyes: line4 and line5 draw under-eye bags while line6 draws
// something else entirely, so "advancing" from line5 to line6 removes the bags
// and the man looks younger than he did at 31. That is exactly what was
// reported.
//
// So nothing walks a ladder any more. Each player gets ONE style per feature,
// fixed for life from his id, and aging only turns features ON - smile lines
// first, then the eyes, then the brow. A feature never switches style and
// never turns off, so a face cannot un-age no matter how the rolls land.
//
// The one genuinely continuous dial facesjs has is smileLine.size, and that
// grows toward an age target and is never allowed to shrink. It is what makes
// most seasons look slightly different when no feature is turning on.
const SMILE_LINES = ["line1", "line2", "line3", "line4"] as const;
const EYE_LINES = [
	"line1",
	"line2",
	"line3",
	"line4",
	"line5",
	"line6",
] as const;
const FOREHEAD_LINES = [
	"forehead1",
	"forehead2",
	"forehead3",
	"forehead4",
	"forehead5",
] as const;

// 0 none, 1 smile, 2 + eyes, 3 + brow.
export const MAX_WRINKLE_LEVEL = 3;

const pickStable = <T>(
	list: readonly T[],
	pid: number | undefined,
	salt: number,
): T => list[Math.floor(hashPid(pid ?? 0, salt) * list.length)]!;

// One set of marks per player, the same every time they are asked for, so a
// face keeps its own character as it ages instead of cycling through styles.
export const lineStylesFor = (pid: number | undefined) => ({
	smile: pickStable(SMILE_LINES, pid, 5),
	eye: pickStable(EYE_LINES, pid, 6),
	forehead: pickStable(FOREHEAD_LINES, pid, 7),
});

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
	return MAX_WRINKLE_LEVEL;
};

// Some faces simply weather less, at any age - the 40-year-old who still looks
// 30 is a real type, and without this everyone piles up at the maximum the
// moment they are old enough for it.
export const WEATHERS_LESS_SHARE = 0.25;

export const weathersLess = (pid: number | undefined): boolean =>
	pid !== undefined && hashPid(pid, 4) < WEATHERS_LESS_SHARE;

// NOBODY AGES ON THE SAME SCHEDULE. wrinkleLevelForAge is one curve for the
// whole league, and running every player off it meant every player became
// eligible for his next set of lines in the same three preseasons - so a
// measured career had its steps piled at 23, 27 and 31, where roughly a third
// of the league changed at once. That is what makes aging read as a few big
// events rather than something always quietly happening: on any other night
// nothing moved, and on those three the whole roster did.
//
// Each player gets his own delay on that curve, so the same progression is
// spread across a band of ages instead of landing on one. It only ever DELAYS.
// The league curve is an upper bound on what an age permits - weathersLess
// already works that way, and the tests hold every player to it - so a
// personal schedule may run behind it and never ahead.
//
// Its own salt. It used to share one with baldingOnsetAge, which tied the two
// together exactly: the man whose hair went at 28 was always the man whose
// lines came early, and a late balder always had a smooth face.
export const WRINKLE_DELAY_MAX = 4;

export const wrinkleDelay = (pid: number | undefined): number =>
	pid === undefined
		? 0
		: Math.floor(hashPid(pid, 13) * (WRINKLE_DELAY_MAX + 1));

export const wrinkleCeiling = (age: number, pid: number | undefined): number =>
	Math.max(
		0,
		wrinkleLevelForAge(age - wrinkleDelay(pid)) - (weathersLess(pid) ? 1 : 0),
	);

// Read the level back off a face. Highest feature present wins, so a face that
// somehow has brow lines but no smile lines reads as 3 - and applying level 3
// then ADDS the missing ones rather than removing what is there. That is what
// makes every write additive no matter what state a face arrives in.
export const wrinkleLevelOf = (face: FaceConfig): number => {
	if ((FOREHEAD_LINES as readonly string[]).includes(face?.miscLine?.id)) {
		return 3;
	}
	if (face?.eyeLine?.id && face.eyeLine.id !== "none") {
		return 2;
	}
	if (face?.smileLine?.id && face.smileLine.id !== "none") {
		return 1;
	}
	return 0;
};

// How deep the folds are at a given age, which is the continuous part.
const SMILE_SIZE_MIN = 0.6;
const SMILE_SIZE_MAX = 2;
const SMILE_SIZE_START_AGE = 22;
const SMILE_SIZE_FULL_AGE = 38;

export const smileSizeForAge = (age: number): number => {
	const t = Math.max(
		0,
		Math.min(
			1,
			(age - SMILE_SIZE_START_AGE) /
				(SMILE_SIZE_FULL_AGE - SMILE_SIZE_START_AGE),
		),
	);
	return (
		Math.round((SMILE_SIZE_MIN + (SMILE_SIZE_MAX - SMILE_SIZE_MIN) * t) * 100) /
		100
	);
};

export const applyWrinkles = (
	face: FaceConfig,
	level: number,
	pid?: number,
	size?: number,
) => {
	const capped = Math.max(0, Math.min(MAX_WRINKLE_LEVEL, level));
	const styles = lineStylesFor(pid);

	// A mark the face ALREADY carries is kept exactly as it is. The rule is
	// that a feature never switches style, and reading the pid style over the
	// top would break it in the one case that matters: a face built before its
	// player had an id (a generated player has no pid until the row is written)
	// picks up marks from nobody's style, and would then swap them for its own
	// the first time it aged. Keeping what is there means the swap can never
	// happen, whoever drew the mark.
	const keep = (
		current: string | undefined,
		wanted: boolean,
		fallback: string,
	) => (!wanted ? "none" : current && current !== "none" ? current : fallback);

	face.smileLine.id = keep(face.smileLine?.id, capped >= 1, styles.smile);
	face.eyeLine.id = keep(face.eyeLine?.id, capped >= 2, styles.eye);

	// Brow lines share a slot with blush, freckles and chin marks, which are not
	// age. A player who has one keeps it and ages through his eyes and smile.
	const misc = face.miscLine?.id ?? "none";
	if (misc === "none" || (FOREHEAD_LINES as readonly string[]).includes(misc)) {
		face.miscLine.id = keep(misc, capped >= 3, styles.forehead);
	}

	if (size !== undefined) {
		face.smileLine.size = size;
	}
};

// Chance per year of gaining a feature, when age allows one.
const WRINKLE_PER_YEAR = 0.3;

// The first age a line is allowed at all - see wrinkleLevelForAge.
const WRINKLE_START_AGE = 23;

// The level a face AGED INTO this age would have: the yearly roll, replayed.
//
// A face built at 36 used to draw its level weighted toward the low end,
// which left 60% of generated 36-year-olds with no lines at all - while a man
// aged a season at a time from 23 (see ageFace) is almost always at his own
// ceiling by then. A new league full of veterans looked a decade younger than
// the same league would after being played. So generation asks the same
// question aging does, one season at a time.
export const wrinkleLevelAgedTo = (
	age: number,
	pid: number | undefined,
	rand: () => number,
): number => {
	let level = 0;
	for (let year = WRINKLE_START_AGE; year <= age; year++) {
		if (level < wrinkleCeiling(year, pid) && rand() < WRINKLE_PER_YEAR) {
			level += 1;
		}
	}
	return level;
};

// How much the folds deepen each season on their own, between those.
//
// It MUST keep up with the ramp smileSizeForAge describes, or the creep - not
// the curve - becomes what actually decides how deep a fold gets. At 0.05 a
// season against a curve climbing (2 - 0.6) / (38 - 22) = 0.0875, it never
// caught up: a face aged from the draft reached 1.4 by 38 where the curve
// asked for the full 2, and worse, a veteran GENERATED at 38 got 2 while one
// AGED to 38 got 1.4. The two paths to the same age are supposed to agree -
// that is the contract the whole module is built on - so this is derived from
// the curve rather than guessed at, and a test pins the two together.
const SMILE_CREEP_PER_YEAR =
	Math.ceil(
		((SMILE_SIZE_MAX - SMILE_SIZE_MIN) /
			(SMILE_SIZE_FULL_AGE - SMILE_SIZE_START_AGE)) *
			1000,
	) / 1000;

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

export const inferRaceFromFace = (face: FaceConfig): Race | undefined => {
	const color = face?.body?.color;
	if (typeof color !== "string" || !/^#[\da-f]{6}$/i.test(color)) {
		return undefined;
	}
	const [r, g, b] = hexToRgb(color);
	let best: { race: Race; distance: number } | undefined;
	for (const [race, palette] of Object.entries(SKIN_TONES) as [
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

// THE OLD DARK BROWN, ON A FACE THAT READS AS BLACK - see HAIR_TONES and
// worker/core/player/agePlayerFace.ts. Recognised by what it is rather than by
// the exact value, because the brightness nudge moved every copy of it: warm,
// clearly coloured, and lighter than any near-black but far darker than a
// colour anyone chooses (a bleached or dyed head is left alone). Swapped for a
// near-black picked from the old colour, so the same man gets the same one on
// every device and in every season of his history.
export const fixBrownHairOnBlackFace = (face: FaceConfig): boolean => {
	const color = face?.hair?.color;
	if (
		typeof color !== "string" ||
		!/^#[\da-f]{6}$/i.test(color) ||
		inferRaceFromFace(face) !== "black"
	) {
		return false;
	}
	const rgb = hexToRgb(color);
	const [h, saturation, l] = rgbToHsl(rgb);
	if (l <= 0.15 || l > 0.3 || saturation < 0.18 || h < 5 || h > 50) {
		return false;
	}
	const tones = HAIR_TONES.black;
	face.hair.color = tones[(rgb[0] + rgb[1] + rgb[2]) % tones.length]!;
	return true;
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
	familyPid,
	race,
	rand = faceStream(),
	onChange,
	onYear,
}: {
	face: FaceConfig;
	rookieAge: number;
	currentAge: number;
	pid?: number;
	familyPid?: number;
	race?: Race;
	rand?: () => number;
	// A season the look took a step in (see ageFace's return).
	onChange?: (age: number) => void;
	// Every season, the first one included, with the face as it was that
	// season - which is what a per-season history is built from.
	onYear?: (age: number) => void;
}) => {
	const startAge = Math.min(rookieAge, currentAge);
	applyRealisticFace(face, {
		age: startAge,
		race: race ?? inferRaceFromFace(face),
		pid,
		// This man already has his colours. Replaying his career is not an
		// occasion to give him new ones, and doing it on every run would move
		// him a little further from himself each time.
		keepColors: true,
		rand,
	});
	onYear?.(startAge);

	for (let age = startAge + 1; age <= currentAge; age++) {
		if (ageFace(face, age, pid, rand, familyPid)) {
			onChange?.(age);
		}
		onYear?.(age);
	}
};

// Reshape a freshly generated face to suit the player's age, then give it
// colors of its own. Mutates, matching how facesjs itself is used here.
export const applyRealisticFace = (
	face: FaceConfig,
	{
		age,
		race,
		pid,
		// Leave the skin and hair colour exactly as they arrived. Two callers
		// need this and both for the same reason - the colours on the face are
		// already the right ones, and a nudge would move them off:
		//
		//  - A RELATIVE. facesjs hands a son his father's skin and hair verbatim,
		//    and that IS the family resemblance. Nudging it pulls the two apart
		//    by a few points every generation, which is plain to see side by side
		//    on a roster page.
		//  - A CAREER REPLAY. Aging an existing league starts from the face the
		//    player already has, so nudging on every pass drifts him further from
		//    himself each time it is run - and would drift a father and son
		//    apart independently.
		keepColors = false,
		// The eyes, mouth, nose, dials and build likewise: a face that already
		// exists keeps its own. Defaults to keepColors because the two callers
		// that keep colours are the two that have a face.
		keepFeatures = keepColors,
		rand = faceStream(),
	}: {
		age: number;
		race?: Race;
		pid?: number;
		keepColors?: boolean;
		keepFeatures?: boolean;
		rand?: () => number;
	},
) => {
	const band = bandForAge(age);

	face.facialHair.id = facialHairForAge(age, rand, pid);

	// Texture and era first, so the age-based hairline logic below judges the
	// style the player will actually keep. The rare check is second and
	// short-circuits, so an ordinary style consumes no randomness.
	if (race !== undefined) {
		const implausible = !hairAllowedForRace(face.hair.id, race);
		const overexposed =
			(RARE_HAIR.has(face.hair.id) && rand() >= RARE_HAIR_KEEP) ||
			(PERIOD_HAIR.has(face.hair.id) && rand() >= PERIOD_HAIR_KEEP) ||
			(SPIKED_HAIR.has(face.hair.id) && rand() >= SPIKED_HAIR_KEEP);
		if (implausible || overexposed) {
			face.hair.id = pickFrom(hairPoolForRace(race), rand);
		}
	}

	// The cut he may already have had by this age - see cutsHair. Replayed the
	// way the lines are, so a face built at 34 wears what one aged to 34 would.
	if (cutsHair(pid) && VOLUMINOUS_HAIR.has(face.hair.id)) {
		for (let year = cutsHairFromAge(pid); year <= age; year++) {
			if (rand() < CUT_PER_YEAR) {
				face.hair.id = pickFrom(shortCutsFor(face.hair.id), rand);
				break;
			}
		}
	}

	// Hairline. A head that has LOST its hair is age-coded, so a young player
	// never has it. A SHAVED head is not: it is a haircut, worn at every age,
	// so it is left alone whoever is wearing it (see shavesHead).
	const shaved = face.hair.id === HAIR_BALD;
	// facesjs draws the horseshoe as one of its ordinary styles, about one
	// face in forty, at any age. Nothing in the league wears one (see
	// AgeBand.baldingPerYear), so it becomes a plain short cut in this man's
	// texture, and the roll below decides whether he has lost his hair, the
	// same as anyone's.
	if (face.hair.id === HAIR_THINNING) {
		face.hair.id = pickFrom(
			race === "white" || race === "asian"
				? SHORT_CUTS_STRAIGHT
				: SHORT_CUTS_COILED,
			rand,
		);
	}
	// A face BUILT at some age has to arrive where one aged into that age would
	// be, so it asks the same question ageFace asks: is this player old enough
	// for his own balding to have started?
	//
	// The ONSET only, not predisposition. `band.balding` is a population share -
	// what fraction of men this age have lost the hairline - and it is already
	// doing the selecting; asking baldingProne as well would select twice and
	// quietly thin the veterans out. ageFace is the opposite case and does ask,
	// because there it is one named player rolling year after year.
	const couldBeBalding = Number.isFinite(age) && age >= baldingOnsetAge(pid);
	if (couldBeBalding && !shaved && rand() < band.balding) {
		face.hair.id = HAIR_BALD;
	}
	if (face.hair.id === HAIR_BALD) {
		// Long hair hanging off the back of a head that has none on top is the
		// one combination facesjs will happily draw and no head has ever worn.
		shaveScalp(face);
	}

	if (face.glasses.id !== "none" && rand() >= band.glasses) {
		face.glasses.id = "none";
	}

	// Blush and freckles share the slot the brow lines use. Before the lines,
	// so a face that loses one can still have them - see applyWrinkles.
	if (!keepFeatures) {
		settleCheekMarks(face, race, rand);
	}

	// Lines to match the age - the same age a face aged into would show. See
	// wrinkleLevelAgedTo.
	applyWrinkles(
		face,
		wrinkleLevelAgedTo(age, pid, rand),
		pid,
		smileSizeForAge(age),
	);

	if (!keepColors) {
		// The whole range when the race is known, the library's pick nudged
		// when it is not. Hair after skin, because the light colours depend on
		// how dark the skin came out - see hairTone.
		const skin = race === undefined ? undefined : skinToneAt(race, rand);
		face.body.color = jitterColor(
			skin?.color ?? face.body.color,
			rand,
			SKIN_JITTER,
		);
		const hair =
			race === undefined ? face.hair.color : hairTone(race, rand, skin?.depth);
		face.hair.color = jitterColor(
			hair,
			rand,
			hexLightness(hair) > 0.4 ? LIGHT_HAIR_JITTER : HAIR_JITTER,
		);
	}

	if (typeof face.fatness === "number") {
		face.fatness = Math.min(
			FATNESS_MAX,
			Math.round((face.fatness + fatnessGainByAge(age)) * 100) / 100,
		);
	}

	if (!keepFeatures) {
		// The cartoon variants, mostly re-drawn from the natural ones - see
		// EYES_CARTOON and the two lists below it. Last, so a face that is
		// missing a part (a test fixture, an old save) is left as it is, and so
		// the draws here come after every decision above rather than shifting
		// them.
		const settle = (
			part: { id: string } | undefined,
			cartoon: readonly string[],
			natural: readonly string[],
		) => {
			if (part && cartoon.includes(part.id) && rand() >= CARTOON_KEEP) {
				part.id = pickFrom(natural, rand);
			}
		};
		settle(face.eye, EYES_CARTOON, EYES_NATURAL);
		settle(face.mouth, MOUTHS_CARTOON, MOUTHS_NATURAL);
		settle(face.nose, NOSES_CARTOON, NOSES_NATURAL);

		if (face.eye) {
			face.eye.angle = inRange(face.eye.angle, EYE_ANGLE, rand, true);
		}
		if (face.eyebrow) {
			face.eyebrow.angle = inRange(
				face.eyebrow.angle,
				EYEBROW_ANGLE,
				rand,
				true,
			);
		}
		if (face.nose) {
			face.nose.size = inRange(face.nose.size, NOSE_SIZE, rand, false);
		}
		// The build, with the years already lived added on top.
		if (typeof face.fatness === "number") {
			face.fatness = Math.min(
				FATNESS_MAX,
				Math.round((fatnessForAthlete(rand) + fatnessGainByAge(age)) * 100) /
					100,
			);
		}
	}

	// The years of growth this age has already had, added last so nothing
	// above can overwrite it: a face built at 34 wears the ears and nose one
	// aged to 34 would. Same contract the fatness gain keeps.
	if (typeof face.ear?.size === "number") {
		face.ear.size = roundGrowth(
			Math.min(EAR_SIZE_MAX, face.ear.size + earGrowthByAge(age)),
		);
	}
	if (typeof face.nose?.size === "number") {
		face.nose.size = roundGrowth(
			Math.min(NOSE_SIZE_MAX, face.nose.size + noseGrowthByAge(age)),
		);
	}
	if (typeof face.eye?.angle === "number") {
		face.eye.angle = roundGrowth(
			Math.max(EYE_ANGLE_AGED_MIN, face.eye.angle - eyeDroopByAge(age, pid)),
		);
	}
};

// One year older, at one of the threshold ages: grow into the look rather than
// re-rolling it. Returns true if anything changed, so the caller only writes
// players that actually need writing.
export const ageFace = (
	face: FaceConfig,
	age: number,
	pid?: number,
	rand: () => number = faceStream(),
	// The family's shared seed, so baldness runs in one - see baldingProne.
	familyPid?: number,
): boolean => {
	const band = bandForAge(age);
	let changed = false;

	// ONE STEP AT A TIME. A season used to be able to take a clean-shaven
	// 31-year-old straight to a full beard, or a goatee to one, because the
	// first growth and each thickening drew from everything the age allowed.
	// Year to year a real face barely changes; it is the decade that shows.
	// See growFacialHair.
	const grown = growFacialHair(face.facialHair.id, age, pid, rand);
	if (grown !== face.facialHair.id) {
		face.facialHair.id = grown;
		changed = true;
	}

	// Lines deepen with age, one step at a time and never past what the age
	// allows. Unlike the hairline this happens to everyone eventually - nobody
	// reaches 38 with the face they had at 20.
	const level = wrinkleLevelOf(face);
	if (level < wrinkleCeiling(age, pid) && rand() < WRINKLE_PER_YEAR) {
		// Additive only - see wrinkleLevelOf. Nothing switches style and nothing
		// turns off, so a face can never un-age.
		applyWrinkles(face, level + 1, pid);
		changed = true;
	}

	// THE CONTINUOUS HALF. Steps are lumpy by nature - nothing for four years
	// and then a man is suddenly bald - so the folds also deepen a little every
	// season, which is what keeps most years from looking identical.
	//
	// It does not count as a change worth recording: it moves a few percent a
	// season, so a snapshot of every one would store twenty near-identical
	// faces per career to capture something only visible across a decade. The
	// player is written back every preseason regardless, so it persists either
	// way - the history just keeps the steps.
	const targetSize = smileSizeForAge(age);
	if (face.smileLine.size < targetSize) {
		face.smileLine.size =
			Math.round(
				Math.min(targetSize, face.smileLine.size + SMILE_CREEP_PER_YEAR) * 100,
			) / 100;
	}

	// THE HAIR HE CHOSE. A player who shaves his head does it once, at his own
	// age, and that is that - it is a haircut, not a symptom, so it does not
	// wait for a balding roll and does not need him to be prone to anything.
	if (
		shavesHead(pid) &&
		age >= shavesHeadAtAge(pid) &&
		face.hair.id !== HAIR_BALD
	) {
		// Through the clippers first. This is the same objection the balding
		// ladder below answers - "straight from dreads to a bare scalp in a
		// single preseason is the jump that reads as a glitch" - and it applies
		// just as much to a man who decides to shave: he takes an afro down to
		// something short, and the razor comes out the season after. Going from
		// a full head of hair to a bare scalp between two roster pages was the
		// largest single jump left in a career.
		if (VOLUMINOUS_HAIR.has(face.hair.id)) {
			face.hair.id = pickFrom(shortCutsFor(face.hair.id), rand);
		} else {
			face.hair.id = HAIR_BALD;
			shaveScalp(face);
		}
		changed = true;
	}

	// THE CUT - see cutsHair. Only ever from volume to short, so like everything
	// else here it cannot be undone by a later roll, and it never touches a
	// head the balding ladder below has already started on.
	if (
		cutsHair(pid) &&
		age >= cutsHairFromAge(pid) &&
		VOLUMINOUS_HAIR.has(face.hair.id) &&
		rand() < CUT_PER_YEAR
	) {
		face.hair.id = pickFrom(shortCutsFor(face.hair.id), rand);
		changed = true;
	}

	// Hairlines only ever go one way, and only for players who were ever going
	// to lose it. Everyone else keeps what they were drafted with, at 25 and at
	// 38 alike.
	//
	// Two rungs, not one. Volume goes before the hairline does - a man cuts
	// an afro or a mop back to something short well before anyone can see the
	// hairline behind it - and from short, the razor. Straight from dreads to
	// a bare scalp in a single preseason is the jump that reads as a glitch
	// rather than as aging. There is no horseshoe rung between (see
	// AgeBand.baldingPerYear); a face that already wears one goes the rest of
	// the way from it.
	if (
		baldingStarted(age, pid, familyPid) &&
		face.hair.id !== HAIR_BALD &&
		rand() < band.baldingPerYear
	) {
		if (VOLUMINOUS_HAIR.has(face.hair.id)) {
			face.hair.id = pickFrom(shortCutsFor(face.hair.id), rand);
		} else {
			face.hair.id = HAIR_BALD;
			shaveScalp(face);
		}
		changed = true;
	}

	// Filling out, also continuous, also never recorded on its own.
	if (typeof face.fatness === "number" && age > FATNESS_START_AGE) {
		face.fatness =
			Math.round(Math.min(FATNESS_MAX, face.fatness + FATNESS_PER_YEAR) * 100) /
			100;
	}

	// And the two that never stop - see the note above them.
	if (age > GROWTH_START_AGE) {
		if (typeof face.ear?.size === "number") {
			face.ear.size = roundGrowth(
				Math.min(EAR_SIZE_MAX, face.ear.size + EAR_GROWTH_PER_YEAR),
			);
		}
		if (typeof face.nose?.size === "number") {
			face.nose.size = roundGrowth(
				Math.min(NOSE_SIZE_MAX, face.nose.size + NOSE_GROWTH_PER_YEAR),
			);
		}
		if (typeof face.eye?.angle === "number") {
			face.eye.angle = roundGrowth(
				Math.max(EYE_ANGLE_AGED_MIN, face.eye.angle - eyeDroopPerYear(pid)),
			);
		}
	}

	return changed;
};
