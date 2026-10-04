// Some plays can be called more than one way: a make at the rim "throws it
// down" or "the layup is good". Which wording a line gets is picked from the
// play itself rather than at random, so a game reads the same every time it is
// shown - live, rewound, replayed, followed in multiplayer - and the 2.5D court
// can act out the finish the line describes.

// What each wording says happened, in the order getText lists them.
export type Finish =
	| "dunk"
	| "layup"
	| "tip"
	| "rollOut"
	| "rimOut"
	| "brick"
	| "swish"
	| "rattle"
	| "airball"
	| "plain";

type Gender = "female" | "male";

type Wording = {
	finishes: readonly Finish[];
	// Relative odds of each wording (equal when undefined).
	weights?: (gender: Gender) => readonly number[] | undefined;
};

const TIP_IN: Wording = {
	finishes: ["dunk", "tip"],
	weights: (gender) => (gender === "male" ? [1, 1] : [0, 1]),
};
const PUT_BACK: Wording = {
	finishes: ["dunk", "layup"],
	weights: (gender) => (gender === "male" ? [1, 1] : [0, 1]),
};
const AT_RIM: Wording = {
	finishes: ["dunk", "dunk", "layup"],
	weights: (gender) => (gender === "male" ? [1, 2, 2] : [1, 10, 1000]),
};
const BLOCKED_AT_RIM: Wording = {
	finishes: ["layup", "dunk"],
	weights: (gender) => (gender === "female" ? [1, 0] : undefined),
};
const MISSED_TIP_IN: Wording = {
	finishes: ["layup", "dunk", "plain"],
	weights: (gender) => (gender === "female" ? [1, 0, 1] : undefined),
};
const MISSED_AT_RIM: Wording = {
	finishes: ["layup", "rollOut", "plain"],
	weights: () => [1, 1, 3],
};
const MISSED_JUMPER: Wording = {
	finishes: ["rimOut", "plain", "brick"],
	weights: () => [1, 4, 1],
};
const SHOOTOUT_MADE: Wording = {
	finishes: ["plain", "swish", "rattle"],
	weights: () => [1, 0.25, 0.25],
};
const SHOOTOUT_MISSED: Wording = {
	finishes: ["rimOut", "brick", "airball"],
	weights: () => [1, 0.1, 0.01],
};

const wordingFor = (event: {
	type: string;
	made?: boolean;
}): Wording | undefined => {
	switch (event.type) {
		case "fgTipIn":
		case "fgTipInAndOne":
			return TIP_IN;
		case "fgPutBack":
		case "fgPutBackAndOne":
			return PUT_BACK;
		case "fgAtRim":
		case "fgAtRimAndOne":
			return AT_RIM;
		case "blkAtRim":
		case "blkTipIn":
		case "blkPutBack":
			return BLOCKED_AT_RIM;
		case "missTipIn":
			return MISSED_TIP_IN;
		case "missAtRim":
		case "missPutBack":
			return MISSED_AT_RIM;
		case "missLowPost":
		case "missMidRange":
		case "missTp":
			return MISSED_JUMPER;
		case "shootoutShot":
			return event.made ? SHOOTOUT_MADE : SHOOTOUT_MISSED;
	}
	return undefined;
};

// A number in [0, 1) from the play: the same play always gets the same one.
const playUniform = (event: any, gid: number | undefined): number => {
	const key = [
		gid ?? "",
		event.type,
		event.t ?? "",
		event.pid ?? "",
		event.period ?? "",
		event.clock ?? "",
	].join("|");
	// FNV-1a, then a murmur3 finish so similar keys land far apart.
	let h = 0x811c9dc5;
	for (let i = 0; i < key.length; i++) {
		h ^= key.charCodeAt(i);
		h = Math.imul(h, 0x01000193);
	}
	h ^= h >>> 16;
	h = Math.imul(h, 0x85ebca6b);
	h ^= h >>> 13;
	h = Math.imul(h, 0xc2b2ae35);
	h ^= h >>> 16;
	return (h >>> 0) / 2 ** 32;
};

// Which of a line's wordings to use (0 for a line with only one).
export const wordingIndex = (
	event: any,
	gid: number | undefined,
	gender: Gender,
): number => {
	const wording = wordingFor(event);
	if (!wording) {
		return 0;
	}
	const weights = wording.weights?.(gender) ?? wording.finishes.map(() => 1);
	let total = 0;
	for (const w of weights) {
		total += Math.max(0, w);
	}
	let r = playUniform(event, gid) * total;
	for (let i = 0; i < weights.length; i++) {
		const w = Math.max(0, weights[i]!);
		if (r < w) {
			return i;
		}
		r -= w;
	}
	return weights.length - 1;
};

// How the line says the play finished, for a line that could have said it more
// than one way.
export const finishOf = (
	event: any,
	gid: number | undefined,
	gender: Gender,
): Finish | undefined =>
	wordingFor(event)?.finishes[wordingIndex(event, gid, gender)];
