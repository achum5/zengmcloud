// WHAT SOMEBODY WHO IS NOT PLAYING WEARS.
//
// The feed's cast is mostly people who have never worn a uniform - beat
// writers, a national insider, cap obsessives, fans - and drawing all of them
// in a game jersey was the single thing that made the timeline read as a
// roster with captions rather than as a room full of people.
//
// facesjs draws clothing by looking an id up in svgs.jersey, and that table is
// a plain object, so a suit becomes a "jersey" the same way a custom uniform
// does (see uniformJersey.ts): build the SVG string, register it under a
// synthetic id, and the library draws it exactly like a preset - right
// z-order, right body-size transform, and it survives a screenshot because
// nothing about the render path changes.
//
// THE CANVAS, read off the library's own art rather than guessed. The body
// path puts the neck between (140,480) and (260,480), rising to (200,300)
// behind the head; the shoulders run from there out and down to (390,600);
// and every stock jersey paints x 80..320, y 505..610. So a collar belongs at
// about y 480-500, which is where the neck actually meets the shoulders, and
// anything below y 610 is off the bottom of the frame.
//
// The three colour slots are the library's: $[primary] is the garment,
// $[secondary] whatever shows under it (a shirt beneath a jacket, the placket
// of a polo), $[accent] the small thing (tie, buttons, drawstring). What gets
// passed into them is chosen per account - see civilianOutfit.

// THE TORSO, SLEEVED. The stock basketball jersey paints only x 80..320, which
// is right for a tank top and wrong for everything else - drawn that way, a
// beat writer had bare arms. These sweep out to the edges of the frame the way
// the library's own hockey sweater does, so the shoulders are covered.
//
// A crew opening at the base of the neck, for tees and anything pulled on.
const CREW_BODY =
	"M152 481c6 24 22 36 48 36s42-12 48-36c40 4 70 13 88 24 22 13 34 40 40 105H16c6-65 18-92 40-105 18-11 48-20 96-24z";

// The same shoulders with a V deep enough for a collar to sit in.
const OPEN_BODY =
	"M152 481l48 92 48-92c40 4 70 13 88 24 22 13 34 40 40 105H16c6-65 18-92 40-105 18-11 48-20 96-24z";

// A collar, as two points folded either side of the opening. Shared so every
// collared thing folds the same way.
const COLLAR =
	"M150 479l50 96 20-38-44-62zM250 479l-50 96-20-38 44-62z";

export const CIVILIAN_CLOTHES: Record<string, string> = {
	// A crew tee. The plainest thing in the set and what most of the fans wear,
	// usually in their team's colours.
	"civ-tee": `<path fill="$[primary]" stroke="#000" stroke-width="6" d="${CREW_BODY}"/><path fill="none" stroke="#000" stroke-width="5" d="M152 481c6 24 22 36 48 36s42-12 48-36"/>`,

	// A tee with a contrast collar band and a sleeve seam - reads as a
	// different shirt at avatar size, not a recolour.
	"civ-tee2": `<path fill="$[primary]" stroke="#000" stroke-width="6" d="${CREW_BODY}"/><path fill="$[secondary]" d="M152 481c6 24 22 36 48 36s42-12 48-36l-11-1c-5 20-19 31-37 31s-32-11-37-31z"/><path fill="none" stroke="#000" stroke-width="5" d="M152 481c6 24 22 36 48 36s42-12 48-36M96 497c14 34 20 68 20 113M304 497c-14 34-20 68-20 113"/>`,

	// A hoodie: the hood bunched behind the neck, and a drawstring.
	"civ-hoodie": `<path fill="$[primary]" stroke="#000" stroke-width="6" d="${CREW_BODY}"/><path fill="$[secondary]" stroke="#000" stroke-width="5" d="M146 474c-6 30 22 54 54 54s60-24 54-54c16 7 25 21 23 39-5 34-40 52-77 52s-72-18-77-52c-2-18 7-32 23-39z"/><path fill="none" stroke="$[accent]" stroke-width="7" stroke-linecap="round" d="M176 560l-8 44M224 560l8 44"/><circle cx="168" cy="608" r="7" fill="$[accent]"/><circle cx="232" cy="608" r="7" fill="$[accent]"/>`,

	// A polo: soft collar, short placket, two buttons.
	"civ-polo": `<path fill="$[primary]" stroke="#000" stroke-width="6" d="${CREW_BODY}"/><path fill="$[primary]" stroke="#000" stroke-width="5" d="M186 492h28v84h-28z"/><path fill="$[primary]" stroke="#000" stroke-width="5" d="${COLLAR}"/><circle cx="200" cy="520" r="6" fill="$[accent]"/><circle cx="200" cy="556" r="6" fill="$[accent]"/>`,

	// An open-collar dress shirt, no jacket. The uniform of a beat writer on a
	// Tuesday.
	"civ-shirt": `<path fill="$[primary]" stroke="#000" stroke-width="6" d="${OPEN_BODY}"/><path fill="none" stroke="#000" stroke-width="5" d="M200 578v32"/><path fill="$[primary]" stroke="#000" stroke-width="5" d="${COLLAR}"/><circle cx="200" cy="596" r="6" fill="$[accent]"/>`,

	// Shirt and tie, no jacket.
	"civ-tie": `<path fill="$[primary]" stroke="#000" stroke-width="6" d="${OPEN_BODY}"/><path fill="$[primary]" stroke="#000" stroke-width="5" d="${COLLAR}"/><path fill="$[accent]" stroke="#000" stroke-width="4" d="M200 496l-21 18 10 26h22l10-26z"/><path fill="$[accent]" stroke="#000" stroke-width="4" d="M189 540l-11 70h44l-11-70z"/>`,

	// The full thing: jacket, shirt, tie. What the national insider wears on
	// camera. The shirt is laid down first and the jacket painted over it, so
	// the V is the shirt showing through rather than a shape drawn to match.
	"civ-suit": `<path fill="$[secondary]" stroke="#000" stroke-width="5" d="M158 476h84v134h-84z"/><path fill="$[primary]" stroke="#000" stroke-width="6" d="M150 477l50 114 50-114c42 4 72 13 90 24 22 13 34 40 40 105H16c6-65 18-92 40-105 18-11 48-20 94-24z"/><path fill="$[primary]" stroke="#000" stroke-width="5" d="M156 481l46 112 6-62-28-52zM244 481l-46 112-6-62 28-52z"/><path fill="$[accent]" stroke="#000" stroke-width="4" d="M200 490l-19 16 9 24h20l9-24z"/><path fill="$[accent]" stroke="#000" stroke-width="4" d="M190 530l-9 61h38l-9-61z"/>`,

	// A jacket over an open collar - no tie. The columnist who does not own one.
	"civ-blazer": `<path fill="$[secondary]" stroke="#000" stroke-width="5" d="M160 476h80v134h-80z"/><path fill="$[primary]" stroke="#000" stroke-width="6" d="M150 477l50 114 50-114c42 4 72 13 90 24 22 13 34 40 40 105H16c6-65 18-92 40-105 18-11 48-20 94-24z"/><path fill="$[primary]" stroke="#000" stroke-width="5" d="M156 481l46 112 6-62-28-52zM244 481l-46 112-6-62 28-52z"/><path fill="none" stroke="#000" stroke-width="4" d="M186 500l14 70M214 500l-14 70"/>`,
};

export type CivilianOutfit =
	| "tee"
	| "tee2"
	| "hoodie"
	| "polo"
	| "shirt"
	| "tie"
	| "suit"
	| "blazer";

export const CIVILIAN_OUTFITS: readonly CivilianOutfit[] = [
	"tee",
	"tee2",
	"hoodie",
	"polo",
	"shirt",
	"tie",
	"suit",
	"blazer",
];

// WHAT IT IS PAINTED IN.
//
// The three slots are fixed per garment - $[primary] is the thing itself,
// $[secondary] whatever shows under it, $[accent] the small detail - so the
// palette has to know which garment it is dressing. A navy tie on a navy suit
// is invisible, and a white shirt under a white shirt is a smear.
//
// Fans wear their team. Everyone else gets a wardrobe of their own, because a
// press room in which all thirty writers wear the same two colours as the home
// side is the thing this was supposed to stop.

const JACKETS = ["#262a36", "#1f2a3a", "#34323a", "#2f3b34", "#3a2f33"];
const SHIRTS = ["#f4f4f2", "#eef2f7", "#e8eef0", "#f2eee6", "#dfe7ef"];
const TIES = [
	"#8f2d3b",
	"#2f5f8f",
	"#6b3f7a",
	"#1f6b5c",
	"#a85a1f",
	"#3c4a63",
	"#7a2230",
];
const CASUAL = [
	"#c8102e",
	"#2f6f5e",
	"#1f4e79",
	"#d98324",
	"#4b3f72",
	"#2b2b2b",
	"#7a8b99",
	"#8c3b4a",
	"#3f6f3f",
	"#b5651d",
];

const from = <T>(rng: () => number, list: readonly T[]): T =>
	list[Math.floor(rng() * list.length)]!;

export const outfitPalette = (
	outfit: CivilianOutfit,
	rng: () => number,
	// Set only for accounts that would actually wear a team's colours.
	teamColors?: [string, string, string],
): [string, string, string] => {
	switch (outfit) {
		case "suit":
		case "blazer":
			return [from(rng, JACKETS), from(rng, SHIRTS), from(rng, TIES)];
		case "tie":
			return [from(rng, SHIRTS), from(rng, SHIRTS), from(rng, TIES)];
		case "shirt":
			return [from(rng, SHIRTS), from(rng, SHIRTS), from(rng, TIES)];
		case "polo": {
			const body = teamColors?.[0] ?? from(rng, CASUAL);
			return [body, from(rng, SHIRTS), teamColors?.[1] ?? "#2b2b2b"];
		}
		default: {
			// Tees and hoodies. A fan wears the team; anyone else does not.
			if (teamColors) {
				return [teamColors[0], teamColors[1], teamColors[2]];
			}
			const body = from(rng, CASUAL);
			return [body, from(rng, SHIRTS), "#2b2b2b"];
		}
	}
};
