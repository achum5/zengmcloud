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
export const NEVER_GENERATE: ReadonlySet<string> = new Set([
	...FACIAL_HAIR_TIERS.period,
	"mustache1SB1",
	"mustache1SB2",
	"fullgoatee5",
	"fullgoatee6",
	"beard5",
	"beard6",
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

// HAIR THAT HAS TO GO SOMEWHERE BEFORE IT GOES AWAY.
//
// Losing your hair does not turn an afro into a horseshoe overnight. What
// happens first is that the hair gets CUT - the volume goes long before the
// hairline does - and only then does the hairline itself show. Sending a
// player straight from dreads to `short-bald` in one preseason reads as a
// glitch, not as aging, because no real head has ever done that.
//
// So these styles take an extra rung on the way down: cut back to something
// short first, and only from there to a receding hairline. Everything not
// listed is already short enough to skip that rung.
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

// The rung below. All universal styles, so this is safe whatever hair the
// player has - a cut is a cut.
const SHORT_CUTS: readonly string[] = [
	"short",
	"short2",
	"short3",
	"crop",
	"crop-fade",
	"short-fade",
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

const RARE_HAIR = new Set<string>(HAIR_RARE);
const PERIOD_HAIR = new Set<string>(HAIR_PERIOD);

// Share of the natural rate these keep.
const RARE_HAIR_KEEP = 0.15;
const PERIOD_HAIR_KEEP = 0.25;

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
		(id) =>
			id !== HAIR_THINNING &&
			id !== HAIR_BALD &&
			!RARE_HAIR.has(id) &&
			!PERIOD_HAIR.has(id),
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
	//
	// ONE successful roll is a big visual step: facesjs has no gentle "slightly
	// receding" hair, so the rung below a normal cut is `short-bald`, a
	// pronounced horseshoe. A player who takes a single rung does not look like
	// he is starting to lose it, he looks like he has lost it.
	//
	// The first answer to that was to make it RARER, and it was the wrong one -
	// a rare bad outcome is still a bad outcome when you are looking at the man
	// it happened to, and a field report duly arrived about twenty-six-year-olds
	// with horseshoes. The right answer is to make it LATER: see
	// baldingOnsetAge, which gives every player his own starting age of at least
	// 28, so these rates now only ever apply to a man old enough for the step
	// not to be surprising.
	//
	// Which means the rates could go back UP, and had to. Gating on onset alone
	// took the share of a league that had visibly lost hair by 38 from 7.7% to
	// 2.9% - a second change nobody asked for, turning an occasional thing into
	// a nearly absent one. These restore the total and leave only the timing
	// changed.
	//
	// Measured over 20,000 replayed careers (rookie at 20, aged one season at a
	// time), share of the WHOLE league who have LOST hair by each age. Hair a
	// face was GENERATED with is not hair loss and is not counted: facesjs
	// draws `bald` as one of its ordinary styles at any age, and shavesHead
	// adds more on purpose.
	//
	//                    24     26     28     30     32     34     36     38
	//   before onset    0.3%   0.5%   1.7%   2.5%   3.8%   5.1%   6.0%   7.7%
	//   onset only      0.0%   0.0%   0.0%   0.2%   0.6%   1.2%   1.9%   2.9%
	//   onset + rates   0.0%   0.0%   0.1%   0.4%   1.5%   2.9%   5.1%   7.3%
	//
	// So: nothing at all before 28, and the same league by the end of a career.
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
		// Zero, and it stays zero: no player's onset age is below 28, so this
		// band could not reach anybody anyway (see baldingOnsetAge). Written out
		// rather than left at a small number that reads as "rarely".
		baldingPerYear: 0,
		glasses: 0.03,
	},
	{
		minAge: 27,
		facialHair: 0.6,
		tiers: { light: 0.4, medium: 0.35, heavy: 0.25 },
		balding: 0.06,
		baldingPerYear: 0.05,
		glasses: 0.03,
	},
	{
		minAge: 31,
		facialHair: 0.65,
		tiers: { light: 0.3, medium: 0.33, heavy: 0.32 },
		balding: 0.12,
		baldingPerYear: 0.08,
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

// And the share who stay clean-shaven whatever their age.
export const NEVER_GROWS_FACIAL_HAIR_SHARE = 0.2;

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

// A SHAVED HEAD IS A HAIRSTYLE, NOT A DIAGNOSIS.
//
// The age rules used to treat `bald` purely as the end state of hair loss, and
// forced it off anyone too young to have lost any - so no player in the league
// could ever turn up with his head shaved before about 27. In basketball that
// is backwards: the clean shave is one of the sport's signature looks and it
// is worn by choice, at every age, by men with perfectly good hairlines.
//
// So it gets its own trait. A player who shaves does it at a fixed age in his
// early twenties and keeps it, which is both how it usually goes and what
// stops a head flickering between shaved and not from season to season.
export const SHAVES_HEAD_SHARE = 0.1;

export const shavesHead = (pid: number | undefined): boolean =>
	pid !== undefined && hashPid(pid, 3) < SHAVES_HEAD_SHARE;

export const shavesHeadAtAge = (pid: number | undefined): number =>
	21 + Math.floor(hashPid(pid ?? 0, 10) * 8);

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

// Is there anything left in this tier to grow? See GENERATED_FACIAL_HAIR.
const generatable = (tier: string): tier is FacialHairTier =>
	(GENERATED_FACIAL_HAIR[tier as FacialHairTier]?.length ?? 0) > 0;

// A facial hair style suitable for this age, or "none".
export const facialHairForAge = (age: number, rand: () => number): string => {
	const band = bandForAge(age);
	if (rand() >= band.facialHair) {
		return "none";
	}
	// Only tiers that still have something to give - see GENERATED_FACIAL_HAIR.
	const tiers = Object.fromEntries(
		Object.entries(band.tiers).filter(([tier]) => generatable(tier)),
	) as Partial<Record<FacialHairTier, number>>;
	if (Object.keys(tiers).length === 0) {
		return "none";
	}
	const tier = pickWeighted(tiers, rand);
	return pickFrom(GENERATED_FACIAL_HAIR[tier]!, rand);
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

export const fatnessGainByAge = (age: number): number =>
	Math.max(0, Math.floor(age) - FATNESS_START_AGE) * FATNESS_PER_YEAR;

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

export const wrinkleCeiling = (age: number, pid: number | undefined): number =>
	Math.max(0, wrinkleLevelForAge(age) - (weathersLess(pid) ? 1 : 0));

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

// How much the folds deepen each season on their own, between those.
const SMILE_CREEP_PER_YEAR = 0.05;

// How much rarer losing it entirely is than starting to lose it, so a man does
// not go from a full head to bald in two seasons.
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
	familyPid,
	race,
	rand = Math.random,
	onChange,
}: {
	face: FaceConfig;
	rookieAge: number;
	currentAge: number;
	pid?: number;
	familyPid?: number;
	race?: Race;
	rand?: () => number;
	onChange?: (age: number) => void;
}) => {
	applyRealisticFace(face, {
		age: Math.min(rookieAge, currentAge),
		race: race ?? inferRaceFromFace(face),
		pid,
		// This man already has his colours. Replaying his career is not an
		// occasion to give him new ones, and doing it on every run would move
		// him a little further from himself each time.
		keepColors: true,
		rand,
	});

	for (
		let age = Math.min(rookieAge, currentAge) + 1;
		age <= currentAge;
		age++
	) {
		if (ageFace(face, age, pid, rand, familyPid)) {
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
		rand = Math.random,
	}: {
		age: number;
		race?: Race;
		pid?: number;
		keepColors?: boolean;
		rand?: () => number;
	},
) => {
	const band = bandForAge(age);

	face.facialHair.id = facialHairForAge(age, rand);

	// Texture and era first, so the age-based hairline logic below judges the
	// style the player will actually keep. The rare check is second and
	// short-circuits, so an ordinary style consumes no randomness.
	if (race !== undefined) {
		const implausible = !hairAllowedForRace(face.hair.id, race);
		const overexposed =
			(RARE_HAIR.has(face.hair.id) && rand() >= RARE_HAIR_KEEP) ||
			(PERIOD_HAIR.has(face.hair.id) && rand() >= PERIOD_HAIR_KEEP);
		if (implausible || overexposed) {
			face.hair.id = pickFrom(hairPoolForRace(race), rand);
		}
	}

	// Hairline. A RECEDING one is age-coded, so a young player never has it and
	// a face that already lost the hairline keeps it lost. A SHAVED head is
	// not: it is a haircut, worn at every age, so it is left alone whoever is
	// wearing it (see shavesHead).
	const shaved = face.hair.id === HAIR_BALD;
	const receding = face.hair.id === HAIR_THINNING;
	// A face BUILT at some age has to arrive where one aged into that age would
	// be, so it asks the same question ageFace asks: is this player old enough
	// for his own balding to have started? Before he is, a receding hairline is
	// undone rather than merely not added - facesjs generates `short-bald` on
	// its own, and leaving it would put one on a rookie.
	//
	// The ONSET only, not predisposition. `band.balding` is a population share -
	// what fraction of men this age have lost the hairline - and it is already
	// doing the selecting; asking baldingProne as well would select twice and
	// quietly thin the veterans out. ageFace is the opposite case and does ask,
	// because there it is one named player rolling year after year.
	const couldBeBalding = Number.isFinite(age) && age >= baldingOnsetAge(pid);
	if (!couldBeBalding) {
		if (receding) {
			face.hair.id = "short";
		}
	} else if (!receding && !shaved && rand() < band.balding) {
		face.hair.id = rand() < 0.5 ? HAIR_THINNING : HAIR_BALD;
	}
	if (face.hair.id === HAIR_THINNING || face.hair.id === HAIR_BALD) {
		// Long hair hanging off the back of a head that has none on top is the
		// one combination facesjs will happily draw and no head has ever worn.
		shaveScalp(face);
	}

	if (face.glasses.id !== "none" && rand() >= band.glasses) {
		face.glasses.id = "none";
	}

	// Lines to match the age. Weighted toward the low end of what the age
	// allows, so a 32-year-old is usually a little weathered and occasionally
	// a lot - the same spread real faces have.
	const ceiling = wrinkleLevelForAge(age);
	applyWrinkles(
		face,
		Math.floor(rand() * rand() * (ceiling + 1)),
		pid,
		smileSizeForAge(age),
	);

	if (!keepColors) {
		face.body.color = jitterColor(face.body.color, rand, SKIN_JITTER);
		face.hair.color = jitterColor(face.hair.color, rand, HAIR_JITTER);
	}

	if (typeof face.fatness === "number") {
		face.fatness = Math.min(
			FATNESS_MAX,
			Math.round((face.fatness + fatnessGainByAge(age)) * 100) / 100,
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
	rand: () => number = Math.random,
	// The family's shared seed, so baldness runs in one - see baldingProne.
	familyPid?: number,
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
				generatable(candidate) &&
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
			face.facialHair.id = pickFrom(GENERATED_FACIAL_HAIR[next]!, rand);
			changed = true;
		}
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

	// THE HAIR HE CHOSE. A player who shaves his head does it once, in his
	// early twenties, and that is that - it is a haircut, not a symptom, so it
	// does not wait for a balding roll and does not need him to be prone to
	// anything. Skipped if the hairline is already going, because from there
	// the ladder below is already headed to the same place.
	if (
		shavesHead(pid) &&
		age >= shavesHeadAtAge(pid) &&
		face.hair.id !== HAIR_BALD &&
		face.hair.id !== HAIR_THINNING
	) {
		face.hair.id = HAIR_BALD;
		shaveScalp(face);
		changed = true;
	}

	// Hairlines only ever go one way, and only for players who were ever going
	// to lose it. Everyone else keeps what they were drafted with, at 25 and at
	// 38 alike.
	//
	// Three rungs, not one. Volume goes before the hairline does - a man cuts
	// an afro or a mop back to something short well before anyone can see the
	// hairline behind it - and losing it entirely is rarer than starting to
	// lose it. Straight from dreads to a horseshoe in a single preseason is
	// the jump that reads as a glitch rather than as aging.
	const baldingRate =
		face.hair.id === HAIR_THINNING
			? band.baldingPerYear * FULLY_BALD_FACTOR
			: band.baldingPerYear;
	if (
		baldingStarted(age, pid, familyPid) &&
		face.hair.id !== HAIR_BALD &&
		rand() < baldingRate
	) {
		if (VOLUMINOUS_HAIR.has(face.hair.id)) {
			face.hair.id = pickFrom(SHORT_CUTS, rand);
		} else if (face.hair.id !== HAIR_THINNING) {
			face.hair.id = HAIR_THINNING;
		} else {
			face.hair.id = HAIR_BALD;
		}
		if (face.hair.id === HAIR_THINNING || face.hair.id === HAIR_BALD) {
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

	return changed;
};
