// A FACE FOR EVERYONE IN THE ROOM.
//
// Players already have one; the rest of the cast - the insider, the beat
// writers, the cap obsessive, the fans - had a monogram, a pair of initials on
// a tinted circle. Seven hundred accounts and only the players looked like
// people, which is what made a timeline read as a roster with captions.
//
// NOTHING IS STORED, exactly like the posts themselves. A face is derived from
// the account id, so every device computes the same one and a league that has
// never opened the feed carries no extra bytes. That is the same contract the
// rest of socialFeed keeps and the reason two people in one league see the
// same thing without syncing.
//
// WHY NOT facesjs generate(). It reads Math.random directly, so it cannot be
// seeded, and swapping the global inside a worker that is also simming is not
// a trade worth making. This mirrors its structure against the same catalogue
// and its own number ranges, and then diverges where a civilian should: no
// headbands, an age drawn for the ROLE rather than for an athlete, reading
// glasses where they belong, and a wardrobe from civilianClothes instead of a
// game jersey.

import { svgsIndex, type FaceConfig } from "facesjs";
import { hashSeed, rngFromSeed } from "../../common/phrasePool.ts";
import {
	CIVILIAN_CLOTHES,
	outfitPalette,
	type CivilianOutfit,
} from "../../common/civilianClothes.ts";
import type { AccountPicture } from "../../common/socialMetrics.ts";
import {
	applyWrinkles,
	EYES_NATURAL,
	HAIR_BALD,
	HAIR_THINNING,
	HAIR_TONES,
	hairPoolForRace,
	lineStylesFor,
	MOUTHS_NATURAL,
	NOSES_NATURAL,
	skinTone,
} from "./realisticFaces.ts";

type Gender = "male" | "female";

// SKIN AND HAIR come from the same anchors the players use (realisticFaces),
// drawn between neighbours rather than from a short fixed list. The library's
// own palette has two or three exact values per race, so a press room of
// seven hundred shared about ten skin colours between them.
const RACES = ["white", "asian", "brown", "black"] as const;

const pick = <T>(rng: () => number, list: readonly T[]): T =>
	list[Math.floor(rng() * list.length)]!;

// THE CARTOON PARTS, kept out of the room the way they are thinned out of a
// roster. The women's set has three pairs of round, staring eyes that read as
// a different art style next to everyone else's, and one ponytail that sends
// a single stray strand straight up like an antenna. Rendered side by side,
// that is how these were picked.
const FEMALE_EYES_CARTOON = new Set(["female3", "female11", "female12"]);
const FEMALE_HAIR_ODD = new Set(["female8"]);
// A civilian is allowed the odd character, less often than a player is.
const CARTOON_KEEP = 0.1;

const pickNatural = (
	rng: () => number,
	all: readonly string[],
	natural: readonly string[],
): string => {
	const keepAny = rng() < CARTOON_KEEP;
	const pool = keepAny ? all : all.filter((id) => natural.includes(id));
	return pick(rng, pool.length > 0 ? pool : all);
};

// GREY COMES IN WITH AGE. Players retire before most of them turn; the room
// does not, and a sixty-year-old historian with jet-black hair was the most
// obviously wrong face in it.
const GREYS = ["#8f8c89", "#a8a6a3", "#c2c0bd", "#d9d7d4"];
const SALT_AND_PEPPER = ["#5f5b57", "#6e6a66"];
// Women colour theirs far more often, so fewer of them show it.
const greyChance = (age: number, gender: Gender): number =>
	(age < 40 ? 0 : age < 46 ? 0.1 : age < 52 ? 0.25 : age < 60 ? 0.45 : 0.7) *
	(gender === "female" ? 0.5 : 1);

// A MAN'S HAIR, from the same pool the players draw from: textures that fit
// him, and none of the novelty or period cuts. Balding is left to age rather
// than to the dice, so it arrives in the fifties and not at twenty-five.
const balding = (age: number): number =>
	age < 35 ? 0 : age < 42 ? 0.08 : age < 50 ? 0.18 : age < 58 ? 0.3 : 0.4;

// THE CATALOGUE, BY GENDER, without the table that says which is which.
//
// facesjs keeps a parallel `svgsGenders` array, but it is not exported and the
// package's exports map allows only "." and "./react", so a deep import would
// be refused by the bundler. It has to be derived, and it is only PARTLY
// derivable: every female-tagged id is prefixed "female", but the male-only
// ones are not marked at all - eye1..eye19 are male-only and look exactly like
// ids that are not.
//
// So the two directions are handled differently, which is safe in both:
//
//   A MAN may wear anything that is not a woman's, and male-only and
//   both-tagged art are equally his. Dropping the "female" prefix is therefore
//   exactly right and needs no table.
//
//   A WOMAN gets the female art where the library draws any - hair, eyes,
//   brows, head, the hair behind it - and for the few types where it draws
//   none, the short list of male-only ids is written out below. Those lists
//   are tiny and have not changed in the library's lifetime.
//
// socialFaces.test.ts reads the library's own table off disk and fails if any
// of this drifts, so the copy cannot rot silently.
const isFemaleArt = (id: string) => id.startsWith("female");

// The only male-only ids in types that have no female art of their own.
export const MALE_ONLY_BODY = ["body3", "body5"];
export const MALE_ONLY_EAR = ["ear3"];

const idsFor = (type: keyof typeof svgsIndex, gender: Gender): string[] => {
	const all = svgsIndex[type] as readonly string[];
	if (gender === "male") {
		return all.filter((id) => !isFemaleArt(id));
	}
	const female = all.filter(isFemaleArt);
	if (female.length > 0) {
		return female;
	}
	const excluded =
		type === "body" ? MALE_ONLY_BODY : type === "ear" ? MALE_ONLY_EAR : [];
	return all.filter((id) => !excluded.includes(id));
};

const uniform = (rng: () => number, lo: number, hi: number): number =>
	Math.round((lo + rng() * (hi - lo)) * 100) / 100;

// ------------------------------------------------------------------ THE ROLE
//
// What somebody does decides roughly how old they are, what they wear, and
// whether they need glasses to read a spreadsheet. A national insider is a
// television face in a suit; a beat writer is in a polo in a press room; a
// casual fan is twenty-two in a team tee.

type Role = {
	// Inclusive age band the account is drawn from.
	age: [number, number];
	// Weighted wardrobe. Repeats are the weights.
	wardrobe: readonly CivilianOutfit[];
	// How often this role wears glasses.
	glasses: number;
	// Share of the role that is women. Sports media skews male and this is a
	// league of people rather than a point, but a cast with no women in it
	// looked like an oversight every time the page was read.
	women: number;
	// Fans wear their team's colours; media do not.
	teamColored?: boolean;
	// A fan in a cap. Nobody else gets one.
	caps?: number;
};

const ROLES: Record<string, Role> = {
	insider: {
		age: [38, 58],
		wardrobe: ["suit", "suit", "blazer", "tie"],
		glasses: 0.25,
		women: 0.3,
	},
	nationalPundit: {
		age: [35, 60],
		wardrobe: ["suit", "blazer", "blazer", "tie"],
		glasses: 0.3,
		women: 0.35,
	},
	aggregator: {
		age: [22, 34],
		wardrobe: ["tee", "tee2", "hoodie", "polo"],
		glasses: 0.2,
		women: 0.3,
	},
	analytics: {
		age: [26, 44],
		wardrobe: ["shirt", "polo", "tee2", "hoodie"],
		glasses: 0.4,
		women: 0.35,
	},
	capNerd: {
		age: [28, 50],
		wardrobe: ["shirt", "shirt", "polo", "blazer"],
		glasses: 0.38,
		women: 0.3,
	},
	draftHead: {
		age: [24, 42],
		wardrobe: ["polo", "shirt", "tee2", "hoodie"],
		glasses: 0.35,
		women: 0.3,
	},
	historian: {
		age: [45, 68],
		wardrobe: ["shirt", "blazer", "tie", "polo"],
		glasses: 0.5,
		women: 0.25,
	},
	beatWriter: {
		age: [28, 52],
		wardrobe: ["polo", "shirt", "shirt", "tie"],
		glasses: 0.35,
		women: 0.35,
	},
	localRadio: {
		age: [32, 58],
		wardrobe: ["polo", "tee2", "shirt", "blazer"],
		glasses: 0.3,
		women: 0.3,
	},
	troll: {
		age: [17, 32],
		wardrobe: ["hoodie", "tee", "tee2"],
		glasses: 0.12,
		women: 0.15,
		caps: 0.25,
	},
	homerFan: {
		age: [19, 55],
		wardrobe: ["tee", "tee", "tee2", "hoodie"],
		glasses: 0.18,
		women: 0.4,
		teamColored: true,
		caps: 0.35,
	},
	doomerFan: {
		age: [20, 50],
		wardrobe: ["hoodie", "tee", "tee2"],
		glasses: 0.2,
		women: 0.35,
		teamColored: true,
		caps: 0.3,
	},
	casualFan: {
		age: [18, 48],
		wardrobe: ["tee", "tee2", "hoodie", "polo"],
		glasses: 0.18,
		women: 0.45,
		teamColored: true,
		caps: 0.3,
	},
};

const DEFAULT_ROLE: Role = {
	age: [25, 50],
	wardrobe: ["shirt", "polo", "tee"],
	glasses: 0.25,
	women: 0.3,
};

export const roleFor = (archetypeId: string): Role =>
	ROLES[archetypeId] ?? DEFAULT_ROLE;

// Everything a civilian should never be caught in. `accessories` is where
// facesjs keeps headbands and eye black, which belong on a court.
const CAPS = ["hat", "hat2", "hat3"] as const;

// ------------------------------------------------------------------ THE FACE

export const socialFace = (
	accountId: string,
	archetypeId: string,
): { face: FaceConfig; age: number; gender: Gender } => {
	const rng = rngFromSeed(hashSeed(`face|${accountId}`));
	// Burn one, as the rest of the module does - the first value out of a
	// freshly seeded stream is the least mixed.
	rng();

	const role = roleFor(archetypeId);
	const gender: Gender = rng() < role.women ? "female" : "male";
	const race = pick(rng, RACES);
	const age = Math.round(role.age[0] + rng() * (role.age[1] - role.age[0]));

	const skinColor = skinTone(race, rng);
	let hairColor = pick(rng, HAIR_TONES[race]);
	if (rng() < greyChance(age, gender)) {
		// Salt and pepper first, full grey later.
		hairColor = age < 52 ? pick(rng, SALT_AND_PEPPER) : pick(rng, GREYS);
	}

	const longHair = gender === "female" && rng() < 0.75;

	const face: FaceConfig = {
		fatness: uniform(rng, 0.1, gender === "female" ? 0.45 : 0.8),
		teamColors: ["#89bfd3", "#7a1319", "#07364f"],
		hairBg: { id: longHair ? pick(rng, idsFor("hairBg", gender)) : "none" },
		body: {
			id: pick(rng, idsFor("body", gender)),
			color: skinColor,
			size: uniform(
				rng,
				gender === "female" ? 0.8 : 0.95,
				gender === "female" ? 0.9 : 1.05,
			),
		},
		jersey: { id: "jersey" },
		ear: {
			id: pick(rng, idsFor("ear", gender)),
			size: uniform(rng, 0.6, gender === "female" ? 1 : 1.3),
		},
		head: {
			id: pick(rng, idsFor("head", gender)),
			shave: "rgba(0,0,0,0)",
		},
		eyeLine: { id: "none" },
		smileLine: { id: "none", size: uniform(rng, 0.6, 1.6) },
		miscLine: { id: "none" },
		facialHair: { id: "none" },
		eye: {
			id:
				gender === "female"
					? pick(
							rng,
							idsFor("eye", gender).filter(
								(id) => !FEMALE_EYES_CARTOON.has(id),
							),
						)
					: pickNatural(rng, idsFor("eye", gender), EYES_NATURAL),
			angle: Math.round(uniform(rng, -6, 10)),
		},
		eyebrow: {
			id: pick(rng, idsFor("eyebrow", gender)),
			angle: Math.round(uniform(rng, -8, 12)),
		},
		hair: {
			id:
				gender === "female"
					? pick(
							rng,
							idsFor("hair", gender).filter((id) => !FEMALE_HAIR_ODD.has(id)),
						)
					: rng() < balding(age)
						? rng() < 0.65
							? HAIR_THINNING
							: HAIR_BALD
						: pick(rng, hairPoolForRace(race)),
			color: hairColor,
			flip: rng() < 0.5,
		},
		mouth: {
			id: pickNatural(rng, idsFor("mouth", gender), MOUTHS_NATURAL),
			flip: rng() < 0.5,
		},
		nose: {
			id: pickNatural(rng, idsFor("nose", gender), NOSES_NATURAL),
			flip: rng() < 0.5,
			size: uniform(rng, 0.6, gender === "female" ? 1 : 1.15),
		},
		glasses: {
			id:
				rng() < role.glasses
					? pick(rng, ["glasses1-primary", "glasses2-black"])
					: "none",
		},
		accessories: {
			id:
				role.caps !== undefined && rng() < role.caps ? pick(rng, CAPS) : "none",
		},
	} as FaceConfig;

	// A man who is not on a roster is allowed a beard, and most of the older
	// ones have one. Drawn here rather than left to the library's flat coin
	// toss so it tracks age the way the players' does.
	if (gender === "male") {
		const chance = age < 25 ? 0.35 : age < 40 ? 0.5 : 0.45;
		if (rng() < chance) {
			face.facialHair.id = pick(rng, idsFor("facialHair", "male"));
		}
		if (age > 34 && rng() < 0.35) {
			face.head.shave = `rgba(0,0,0,${uniform(rng, 0.04, 0.16)})`;
		}
	}

	// LINES BY AGE, borrowed from the players' aging rather than reinvented:
	// same three levels, same per-player styles, so a 55-year-old columnist is
	// marked the way a 38-year-old center is and the whole league ages on one
	// set of rules. applyWrinkles only touches the line features, so it is safe
	// for either gender - unlike the rest of that module, which grows beards.
	// A WOMAN'S LINES STOP AT THE SMILE. The library's eye lines and brow lines
	// are every one of them male-only art - it draws no female version - so
	// level 2 and 3 would put a man's marks on a woman's face. The smile lines
	// are tagged for both, so level 1 is hers and the rest is done with the
	// fold DEPTH, which is a number rather than a piece of art.
	const byAge = age < 26 ? 0 : age < 34 ? 1 : age < 46 ? 2 : 3;
	const level = gender === "female" ? Math.min(1, byAge) : byAge;
	if (level > 0) {
		applyWrinkles(face, level, hashSeed(`lines|${accountId}`));
		face.smileLine.size = Math.min(2.2, 0.7 + (age - 22) * 0.045);
	} else {
		// Keep the style choice stable even when nothing is shown yet, so a
		// young account does not change character if this is ever replayed.
		lineStylesFor(hashSeed(`lines|${accountId}`));
	}

	return { face, age, gender };
};

// The whole picture for a human account: the face, what they are wearing, and
// the colours that wardrobe is painted in.
export const socialAccountPicture = (
	accountId: string,
	archetypeId: string,
	teamColors: [string, string, string] | undefined,
): AccountPicture => {
	const { face } = socialFace(accountId, archetypeId);
	const role = roleFor(archetypeId);

	const rng = rngFromSeed(hashSeed(`fit|${accountId}`));
	rng();
	const outfit = pick(rng, role.wardrobe);

	return {
		face,
		jersey:
			CIVILIAN_CLOTHES[`civ-${outfit}`] !== undefined
				? `civ-${outfit}`
				: "civ-tee",
		colors: outfitPalette(
			outfit,
			rng,
			role.teamColored ? teamColors : undefined,
		),
	};
};
