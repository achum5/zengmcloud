import {
	synergyCompositeRating,
	synergyFromSkillCounts,
	synergySkillCounts,
	synergyTotal,
	type SynergyCompositeRating,
} from "../GameSim.basketball/synergy.ts";
import {
	IMPACT_RATINGS,
	shotWeight,
	type ImpactRatings,
} from "./playerImpact.ts";

// HOW GOOD A ROSTER IS, in points per game against an average team.
//
// Fit on this game's own sim, on whole games only (src/test/impact):
//
//   - Swap tests: a player dropped into one rotation spot of a team, with that
//     spot's place in the pecking order, played against the whole league
//     hundreds of times on the same random numbers as the team without him.
//   - Matchups: real rosters with players swapped in, each matchup played 40
//     times.
//
// Stint regressions were tried first and undersold stars by a third - the sim
// sends the benches out in blowouts, so bench lineups collect blowout margins.
//
// A player is two numbers, both from his full ratings: what he adds whatever
// his share of the ball (defense, rebounding, ...), and what his offense is
// worth at a full share. On the floor the sim hands the shot to one of the
// five in proportion to usage to the 1.25 power, so a scorer is worth his
// offense times his share - and two stars dilute each other. That is what
// makes talent add up to less than the sum of its parts: summing single-player
// swap values over a roster overstates the gaps between teams by about 40%.
// Lineup synergy is added on top, and once the ratings are known a season's
// box score adds nothing.
//
// A roster is its players in the order the sim plays them - by valueNoPot,
// which is what its substitutions rank on - each with his place's minutes and
// the share of the ball he can expect alongside whoever usually shares the
// floor with him. Empty places are filled with what a team can always get for
// the minimum, so a roster is never worth more for being short.
//
// Against fresh matchups it misses the true margin by 1.25 points a game where
// stock's spread misses by 2.16, and it predicts single swaps about as well as
// a model fit on swaps alone. First version: fit under the owner's league
// settings on five host teams' swaps; the mass-sim model replaces it.

const MODEL = {
	// Points a game, per full game on the floor, whatever his share of the ball.
	roleFree: {
		ovr: [-2.4452, 5.4354, -1.4085],
		linear: {
			hgt: -0.564,
			stre: 2.5267,
			spd: -3.0502,
			jmp: 1.0181,
			endu: 1.7557,
			ins: 6.3739,
			dnk: -1.746,
			ft: -1.7486,
			fg: 0.2186,
			tp: -3.675,
			oiq: 4.3686,
			diq: 3.2528,
			drb: -0.0382,
			pss: 0.9701,
			reb: 0.5375,
		},
		squared: {
			hgt: 2.5175,
			stre: 1.2017,
			spd: -4.369,
			jmp: 6.205,
			endu: -2.9495,
			ins: -5.3252,
			dnk: -2.2039,
			ft: 0.7951,
			fg: 3.2995,
			tp: 9.5101,
			oiq: -0.9548,
			diq: 2.39,
			drb: 6.8471,
			pss: -1.5745,
			reb: -1.2555,
		},
	},
	// Points a game, per full game on the floor, if every shot were his.
	offense: {
		ovr: [21.5748, 38.1932, 8.2915],
		linear: {
			hgt: 36.0405,
			stre: -1.167,
			spd: 34.7166,
			jmp: -9.9785,
			endu: -17.8762,
			ins: -28.2421,
			dnk: 1.2124,
			ft: 14.9941,
			fg: -25.376,
			tp: -20.0786,
			oiq: -18.1542,
			diq: -18.7327,
			drb: 5.6905,
			pss: 23.8591,
			reb: -1.4377,
		},
		squared: {
			hgt: -7.8313,
			stre: -8.4873,
			spd: 14.7144,
			jmp: -17.3272,
			endu: 23.0884,
			ins: 21.6196,
			dnk: 21.6558,
			ft: -0.788,
			fg: 1.5602,
			tp: 2.963,
			oiq: 6.9028,
			diq: -6.3729,
			drb: -20.2046,
			pss: -13.473,
			reb: 10.3475,
		},
	},
	// Points a game per unit of pregame lineup synergy.
	synergy: 3.1245,
	home: 3.1432,
};

// Share of the game on the floor, by place in the pecking order.
export const SLOT_MINUTES = [
	0.7619, 0.6977, 0.6552, 0.6204, 0.5825, 0.4598, 0.4015, 0.33, 0.2323, 0.1327,
	0.0638, 0.0319, 0.019, 0.0123, 0.0085,
];

// How often the man in place k is on the floor while the man in place j is.
const TOGETHER = [
	[
		1, 0.715, 0.668, 0.629, 0.586, 0.424, 0.357, 0.28, 0.183, 0.094, 0.039,
		0.015, 0.006, 0.002, 0.001,
	],
	[
		0.78, 1, 0.673, 0.636, 0.593, 0.402, 0.333, 0.258, 0.169, 0.09, 0.038,
		0.016, 0.007, 0.003, 0.001,
	],
	[
		0.775, 0.716, 1, 0.641, 0.599, 0.383, 0.316, 0.245, 0.166, 0.09, 0.039,
		0.016, 0.007, 0.003, 0.002,
	],
	[
		0.772, 0.715, 0.677, 1, 0.605, 0.365, 0.301, 0.241, 0.164, 0.089, 0.039,
		0.017, 0.008, 0.004, 0.002,
	],
	[
		0.766, 0.71, 0.674, 0.645, 1, 0.349, 0.295, 0.236, 0.164, 0.089, 0.039,
		0.017, 0.009, 0.004, 0.002,
	],
	[
		0.702, 0.609, 0.546, 0.494, 0.442, 1, 0.434, 0.341, 0.221, 0.115, 0.05,
		0.022, 0.011, 0.006, 0.003,
	],
	[
		0.679, 0.58, 0.517, 0.467, 0.428, 0.498, 1, 0.362, 0.236, 0.124, 0.055,
		0.025, 0.013, 0.007, 0.004,
	],
	[
		0.646, 0.547, 0.488, 0.453, 0.417, 0.475, 0.439, 1, 0.264, 0.141, 0.063,
		0.029, 0.016, 0.009, 0.006,
	],
	[
		0.599, 0.509, 0.469, 0.438, 0.41, 0.438, 0.407, 0.374, 1, 0.18, 0.083, 0.04,
		0.022, 0.013, 0.009,
	],
	[
		0.542, 0.472, 0.444, 0.418, 0.39, 0.399, 0.376, 0.351, 0.315, 1, 0.132,
		0.066, 0.038, 0.024, 0.016,
	],
	[
		0.468, 0.421, 0.399, 0.382, 0.357, 0.363, 0.346, 0.328, 0.305, 0.275, 1,
		0.139, 0.085, 0.056, 0.036,
	],
	[
		0.368, 0.343, 0.335, 0.326, 0.314, 0.324, 0.315, 0.304, 0.292, 0.277, 0.279,
		1, 0.2, 0.138, 0.092,
	],
	[
		0.253, 0.254, 0.262, 0.263, 0.268, 0.279, 0.278, 0.281, 0.272, 0.27, 0.289,
		0.339, 1, 0.292, 0.201,
	],
	[
		0.15, 0.158, 0.179, 0.194, 0.208, 0.225, 0.239, 0.255, 0.254, 0.258, 0.293,
		0.359, 0.451, 1, 0.385,
	],
	[
		0.082, 0.093, 0.12, 0.144, 0.163, 0.186, 0.208, 0.226, 0.237, 0.244, 0.271,
		0.346, 0.445, 0.553, 1,
	],
];

export type ValueRatings = ImpactRatings & { ovr: number };

// What a roster needs to know about a player.
export type PlayerLine = {
	roleFree: number;
	offense: number;
	// usage ** 1.25, what the sim hands the ball out by.
	usage: number;
	synergy: SynergyCompositeRating;
};

const evaluate = (
	coefficients: {
		ovr: readonly number[];
		linear: Record<string, number>;
		squared: Record<string, number>;
	},
	ratings: ValueRatings,
) => {
	const o = Math.min(95, Math.max(25, ratings.ovr)) / 100;
	let value =
		coefficients.ovr[0]! * o +
		coefficients.ovr[1]! * o * o +
		coefficients.ovr[2]! * o * o * o;
	for (const key of IMPACT_RATINGS) {
		const r = ratings[key] / 100;
		value += coefficients.linear[key]! * r + coefficients.squared[key]! * r * r;
	}
	return value;
};

export const lineSynergy = (ratings: ValueRatings) =>
	synergyCompositeRating(ratings as any);

export const playerLine = (ratings: ValueRatings): PlayerLine => ({
	roleFree: evaluate(MODEL.roleFree, ratings),
	offense: evaluate(MODEL.offense, ratings),
	usage: shotWeight(ratings),
	synergy: lineSynergy(ratings),
});

// A typical 43 overall, about what is always there for the minimum. A stand-in
// until free agency's real supply is modelled.
const REPLACEMENT_RATINGS: ValueRatings = {
	hgt: 47.2,
	stre: 44.3,
	spd: 48.1,
	jmp: 45.5,
	endu: 38.1,
	ins: 41.3,
	dnk: 48.7,
	ft: 44.6,
	fg: 45.7,
	tp: 41,
	oiq: 44.3,
	diq: 42.9,
	drb: 44.4,
	pss: 42.9,
	reb: 51.2,
	ovr: 43,
};

export type StrengthPlayer = PlayerLine & {
	// What the sim's substitutions rank him by.
	valueNoPot: number;
};

let replacement: StrengthPlayer | undefined;
// A player worse than this is not worth a roster place.
export const REPLACEMENT_OVR = REPLACEMENT_RATINGS.ovr;

export const getReplacement = () => {
	if (!replacement) {
		replacement = {
			...playerLine(REPLACEMENT_RATINGS),
			valueNoPot: -Infinity,
		};
	}
	return replacement;
};

export const ROSTER_PLACES = SLOT_MINUTES.length;

// Synergy the way the sim's pregame spread reads it: 70% the first five, 30%
// the next five.
const STARTER_SYNERGY = 0.7;
const SYNERGY_PLACES = 10;

// A player's synergy skills are worked out once per line: rosters are priced
// many thousands of times a day, and the lines are cached upstream. They are
// kept as eight numbers in a row, in SKILLS order, so a place can add up its
// share of everyone who may play it cheaply. A lineup of five each sure of
// his place has its synergy remembered by the lines in it, for the lineups
// seen most recently.
const SKILLS = ["3", "A", "B", "Di", "Dp", "Po", "Ps", "R"] as const;
const NUM_SKILLS = SKILLS.length;
type SynergyInput = { skills: Float64Array; id: number };
const synergyInputs = new WeakMap<SynergyCompositeRating, SynergyInput>();
let nextSynergyId = 0;
const synergyInput = (synergy: SynergyCompositeRating) => {
	let input = synergyInputs.get(synergy);
	if (!input) {
		const counts = synergySkillCounts([{ compositeRating: synergy }]);
		input = {
			skills: Float64Array.from(SKILLS, (key) => counts[key]),
			id: nextSynergyId,
		};
		nextSynergyId = (nextSynergyId + 1) | 0;
		synergyInputs.set(synergy, input);
	}
	return input;
};
const LINEUP_SLOTS = 1 << 15;
const lineupInputs: (SynergyInput | undefined)[] = new Array(
	5 * LINEUP_SLOTS,
).fill(undefined);
const lineupSynergy = new Float64Array(LINEUP_SLOTS);

// The places' synergy skills: those of whoever is sure to hold a place (the
// minimum player, if nobody may), in placeHeld, or else what the place is
// expected to bring, in placeSkills.
const placeHeld: (SynergyInput | undefined)[] = new Array(SYNERGY_PLACES).fill(
	undefined,
);
const placeSkills = new Float64Array(SYNERGY_PLACES * NUM_SKILLS);

// Synergy of a lineup whose skills add up to unitSkills.
const unitSkills = new Float64Array(NUM_SKILLS);
const synergyOfUnitSkills = () =>
	synergyTotal(
		synergyFromSkillCounts({
			"3": unitSkills[0]!,
			A: unitSkills[1]!,
			B: unitSkills[2]!,
			Di: unitSkills[3]!,
			Dp: unitSkills[4]!,
			Po: unitSkills[5]!,
			Ps: unitSkills[6]!,
			R: unitSkills[7]!,
		}),
	);

// A lineup with someone not sure of his place has its synergy remembered by
// the skills it adds up to, for the lineups seen most recently: a roster is
// priced again for every deal that leaves that season of it alone.
const unitBits = new Int32Array(unitSkills.buffer);
const MIXED_SLOTS = 1 << 14;
const mixedSkills = new Float64Array(NUM_SKILLS * MIXED_SLOTS).fill(Number.NaN);
const mixedSynergy = new Float64Array(MIXED_SLOTS);

const addSkills = (skills: Float64Array, at: number) => {
	for (let s = 0; s < NUM_SKILLS; s++) {
		unitSkills[s]! += skills[at + s]!;
	}
};

// Synergy of the five places starting at `from`.
const unitSynergy = (from: number) => {
	const s0 = placeHeld[from];
	const s1 = placeHeld[from + 1];
	const s2 = placeHeld[from + 2];
	const s3 = placeHeld[from + 3];
	const s4 = placeHeld[from + 4];
	if (s0 && s1 && s2 && s3 && s4) {
		let hash = s0.id;
		hash = Math.imul(hash ^ s1.id, 0x9e3779b1);
		hash = Math.imul(hash ^ s2.id, 0x9e3779b1);
		hash = Math.imul(hash ^ s3.id, 0x9e3779b1);
		hash = Math.imul(hash ^ s4.id, 0x9e3779b1);
		const slot = (hash ^ (hash >>> 15)) & (LINEUP_SLOTS - 1);
		const at = 5 * slot;
		if (
			lineupInputs[at] === s0 &&
			lineupInputs[at + 1] === s1 &&
			lineupInputs[at + 2] === s2 &&
			lineupInputs[at + 3] === s3 &&
			lineupInputs[at + 4] === s4
		) {
			return lineupSynergy[slot]!;
		}
		unitSkills.fill(0);
		addSkills(s0.skills, 0);
		addSkills(s1.skills, 0);
		addSkills(s2.skills, 0);
		addSkills(s3.skills, 0);
		addSkills(s4.skills, 0);
		const value = synergyOfUnitSkills();
		lineupInputs[at] = s0;
		lineupInputs[at + 1] = s1;
		lineupInputs[at + 2] = s2;
		lineupInputs[at + 3] = s3;
		lineupInputs[at + 4] = s4;
		lineupSynergy[slot] = value;
		return value;
	}
	unitSkills.fill(0);
	for (let k = from; k < from + 5; k++) {
		const held = placeHeld[k];
		if (held) {
			addSkills(held.skills, 0);
		} else {
			addSkills(placeSkills, k * NUM_SKILLS);
		}
	}
	let hash = 0;
	for (let i = 0; i < unitBits.length; i++) {
		hash = Math.imul(hash ^ unitBits[i]!, 0x9e3779b1);
	}
	const slot = (hash ^ (hash >>> 15)) & (MIXED_SLOTS - 1);
	const at = NUM_SKILLS * slot;
	if (
		mixedSkills[at] === unitSkills[0] &&
		mixedSkills[at + 1] === unitSkills[1] &&
		mixedSkills[at + 2] === unitSkills[2] &&
		mixedSkills[at + 3] === unitSkills[3] &&
		mixedSkills[at + 4] === unitSkills[4] &&
		mixedSkills[at + 5] === unitSkills[5] &&
		mixedSkills[at + 6] === unitSkills[6] &&
		mixedSkills[at + 7] === unitSkills[7]
	) {
		return mixedSynergy[slot]!;
	}
	const value = synergyOfUnitSkills();
	mixedSkills.set(unitSkills, at);
	mixedSynergy[slot] = value;
	return value;
};

const byValueNoPot = (a: StrengthPlayer, b: StrengthPlayer) =>
	b.valueNoPot - a.valueNoPot;

const TOGETHER_FLAT = Float64Array.from(TOGETHER.flat());

// The roster's places, one array per part of a player's line.
const placeRoleFree = new Float64Array(ROSTER_PLACES);
const placeOffense = new Float64Array(ROSTER_PLACES);
const placeUsage = new Float64Array(ROSTER_PLACES);

// Points of the roster in placeRoleFree, placeOffense, placeUsage, placeHeld
// and placeSkills.
const pointsOfPlaces = () => {
	let total = 0;
	for (let k = 0; k < ROSTER_PLACES; k++) {
		const row = k * ROSTER_PLACES;
		let others = 0;
		for (let j = 0; j < k; j++) {
			others += TOGETHER_FLAT[row + j]! * placeUsage[j]!;
		}
		for (let j = k + 1; j < ROSTER_PLACES; j++) {
			others += TOGETHER_FLAT[row + j]! * placeUsage[j]!;
		}
		const usage = placeUsage[k]!;
		const share = usage / (usage + others);
		total += SLOT_MINUTES[k]! * (placeRoleFree[k]! + share * placeOffense[k]!);
	}
	const synergy =
		STARTER_SYNERGY * unitSynergy(0) + (1 - STARTER_SYNERGY) * unitSynergy(5);
	return total + MODEL.synergy * synergy;
};

// The roster's places from players already in pecking order.
const pointsOfSorted = (sorted: readonly StrengthPlayer[]) => {
	const r = getReplacement();
	for (let k = 0; k < ROSTER_PLACES; k++) {
		const p = sorted[k] ?? r;
		placeRoleFree[k] = p.roleFree;
		placeOffense[k] = p.offense;
		placeUsage[k] = p.usage;
		if (k < SYNERGY_PLACES) {
			placeHeld[k] = synergyInput(p.synergy);
		}
	}
	return pointsOfPlaces();
};

// Points per game the roster is worth, before subtracting the league average.
// Only differences between rosters mean anything.
export const rosterPoints = (players: readonly StrengthPlayer[]) =>
	pointsOfSorted([...players].sort(byValueNoPot));

// What one player is worth as a single number: his points at the sixth man's
// minutes with a typical share of the ball, over a minimum player's. For
// ranking and explaining; rosters are always priced whole.
// Points a game a player adds over the minimum, per full game on the floor at
// a typical share of the shots.
export const valueOverReplacement = (
	line: Pick<PlayerLine, "roleFree" | "offense">,
) => {
	const r = getReplacement();
	const typicalShare = 0.2;
	return line.roleFree - r.roleFree + typicalShare * (line.offense - r.offense);
};

export const playerValue = (line: Pick<PlayerLine, "roleFree" | "offense">) =>
	SLOT_MINUTES[5]! * valueOverReplacement(line);

// A roster of players we may not all have - injured for part of the season,
// leaving in free agency, a pick who may not stick, a free agent the team may
// not land - is priced place by place: each place in the pecking order goes to
// whoever is likely to fill it, given everyone's chance of being there, and a
// minimum player where nobody may. Pricing every player as there and then
// taking each one's absence off separately, as this once did, went wrong
// when a future roster held twenty players who might be there: the imaginary
// lineup of all of them, and its synergy, swung by points when any one moved.
// The most valuable players we may not have are played out both ways
// exactly, so a single injured starter costs exactly his share.
//
// The players who may be there are laid out in pecking order in the arrays
// below, each with his chance of being there. The enumerated ones (0 and 1)
// are there for sure if their bit of `mask` is set, and not at all if not.
let capacity = 0;
let sortedAvailability = new Float64Array(0);
let sortedRoleFree = new Float64Array(0);
let sortedOffense = new Float64Array(0);
let sortedUsage = new Float64Array(0);
let sortedEnumerated = new Int8Array(0);
const sortedSynergy: SynergyCompositeRating[] = [];
const sortedSynergyInput: (SynergyInput | undefined)[] = [];

const layOut = (
	sorted: readonly (StrengthPlayer & { availability: number })[],
	enumerated: readonly StrengthPlayer[],
) => {
	const n = sorted.length;
	if (n > capacity) {
		capacity = Math.max(n, 2 * capacity);
		sortedAvailability = new Float64Array(capacity);
		sortedRoleFree = new Float64Array(capacity);
		sortedOffense = new Float64Array(capacity);
		sortedUsage = new Float64Array(capacity);
		sortedEnumerated = new Int8Array(capacity);
	}
	for (let i = 0; i < n; i++) {
		const p = sorted[i]!;
		sortedAvailability[i] = Math.min(1, p.availability);
		sortedRoleFree[i] = p.roleFree;
		sortedOffense[i] = p.offense;
		sortedUsage[i] = p.usage;
		sortedSynergy[i] = p.synergy;
		sortedSynergyInput[i] = undefined;
		sortedEnumerated[i] =
			p === enumerated[0] ? 0 : p === enumerated[1] ? 1 : -1;
	}
};

const sortedInput = (i: number) =>
	(sortedSynergyInput[i] ??= synergyInput(sortedSynergy[i]!));

// Everyone up to the last enumerated player is usually there for sure or not
// at all, so each of them just takes the next place. Who fills the places
// after them depends only on how many of them are there, not on which, so
// those places are worked out once, as if they started at the first, and
// moved down into place for each way of playing it out.
const ahead = new Float64Array(ROSTER_PLACES + 1);
const restFilled = new Float64Array(ROSTER_PLACES);
const restRoleFree = new Float64Array(ROSTER_PLACES);
const restOffense = new Float64Array(ROSTER_PLACES);
const restUsage = new Float64Array(ROSTER_PLACES);
// Each place's synergy skills, by everyone's chance of playing it, and who is
// sure to play it (-1 if nobody is).
const restSkills = new Float64Array(SYNERGY_PLACES * NUM_SKILLS);
const restHeld = new Int32Array(SYNERGY_PLACES);

const fillRest = (from: number, n: number, mask: number) => {
	// The chance that exactly j of the players ahead are there, nonzero only
	// from lo to hi.
	ahead.fill(0);
	ahead[0] = 1;
	let lo = 0;
	let hi = 0;
	restFilled.fill(0);
	restRoleFree.fill(0);
	restOffense.fill(0);
	restUsage.fill(0);
	restSkills.fill(0);
	restHeld.fill(-1);
	for (let i = from; i < n; i++) {
		let a;
		const e = sortedEnumerated[i]!;
		if (e >= 0) {
			if (((mask >> e) & 1) === 0) {
				continue;
			}
			a = 1;
		} else {
			a = sortedAvailability[i]!;
		}
		const roleFree = sortedRoleFree[i]!;
		const offense = sortedOffense[i]!;
		const usage = sortedUsage[i]!;
		const top = Math.min(hi, ROSTER_PLACES - 1);
		for (let k = lo; k <= top; k++) {
			const chance = a * ahead[k]!;
			restFilled[k]! += chance;
			restRoleFree[k]! += chance * roleFree;
			restOffense[k]! += chance * offense;
			restUsage[k]! += chance * usage;
		}
		if (lo < SYNERGY_PLACES) {
			const skills = sortedInput(i).skills;
			const s0 = skills[0]!;
			const s1 = skills[1]!;
			const s2 = skills[2]!;
			const s3 = skills[3]!;
			const s4 = skills[4]!;
			const s5 = skills[5]!;
			const s6 = skills[6]!;
			const s7 = skills[7]!;
			const last = Math.min(hi, SYNERGY_PLACES - 1);
			for (let k = lo; k <= last; k++) {
				const chance = a * ahead[k]!;
				if (chance === 1) {
					restHeld[k] = i;
				}
				const at = k * NUM_SKILLS;
				restSkills[at]! += chance * s0;
				restSkills[at + 1]! += chance * s1;
				restSkills[at + 2]! += chance * s2;
				restSkills[at + 3]! += chance * s3;
				restSkills[at + 4]! += chance * s4;
				restSkills[at + 5]! += chance * s5;
				restSkills[at + 6]! += chance * s6;
				restSkills[at + 7]! += chance * s7;
			}
		}
		for (let j = hi; j >= lo; j--) {
			const x = ahead[j]!;
			if (j < ROSTER_PLACES) {
				ahead[j + 1]! += x * a;
			}
			ahead[j] = x * (1 - a);
		}
		if (a === 1) {
			lo += 1;
		}
		hi = Math.min(hi + 1, ROSTER_PLACES);
	}
};

const placeFilled = new Float64Array(ROSTER_PLACES);
let replacementInput: SynergyInput | undefined;

// The first `sure` players each take the next place if they are there; the
// rest of the places are from fillRest.
//
// Each place's synergy skills are those of whoever may play it, by his chance
// of playing it, and the minimum player's for the rest - as with everything
// else about the place. Handing the place the skills of whoever was likeliest
// to fill it, as this once did, made a lineup's synergy jump whenever that
// changed hands: a long shot with a 5% chance of being there could move a
// roster by a point.
const byPlace = (sure: number, mask: number) => {
	let next = 0;
	for (let i = 0; i < sure; i++) {
		const e = sortedEnumerated[i]!;
		if (e >= 0 && ((mask >> e) & 1) === 0) {
			continue;
		}
		if (next < ROSTER_PLACES) {
			placeFilled[next] = 1;
			placeRoleFree[next] = sortedRoleFree[i]!;
			placeOffense[next] = sortedOffense[i]!;
			placeUsage[next] = sortedUsage[i]!;
			if (next < SYNERGY_PLACES) {
				placeHeld[next] = sortedInput(i);
			}
		}
		next += 1;
	}
	for (let k = next; k < ROSTER_PLACES; k++) {
		placeFilled[k] = restFilled[k - next]!;
		placeRoleFree[k] = restRoleFree[k - next]!;
		placeOffense[k] = restOffense[k - next]!;
		placeUsage[k] = restUsage[k - next]!;
	}
	const r = getReplacement();
	for (let k = 0; k < ROSTER_PLACES; k++) {
		const empty = Math.max(0, 1 - placeFilled[k]!);
		placeRoleFree[k]! += empty * r.roleFree;
		placeOffense[k]! += empty * r.offense;
		placeUsage[k]! += empty * r.usage;
	}
	replacementInput ??= synergyInput(r.synergy);
	const replacementSkills = replacementInput.skills;
	for (let k = next; k < SYNERGY_PLACES; k++) {
		const rest = k - next;
		const held = restHeld[rest]!;
		const filled = restFilled[rest]!;
		if (held >= 0) {
			placeHeld[k] = sortedInput(held);
		} else if (filled === 0) {
			placeHeld[k] = replacementInput;
		} else {
			placeHeld[k] = undefined;
			const empty = Math.max(0, 1 - filled);
			const from = rest * NUM_SKILLS;
			const to = k * NUM_SKILLS;
			for (let s = 0; s < NUM_SKILLS; s++) {
				placeSkills[to + s] =
					restSkills[from + s]! + empty * replacementSkills[s]!;
			}
		}
	}
	return pointsOfPlaces();
};

// Everyone who may be there, in pecking order: the same order as sorting by
// valueNoPot keeps ties as they come, which an insertion sort does too.
const order: (StrengthPlayer & { availability: number })[] = [];
const orderValue: number[] = [];
const inPeckingOrder = (
	players: readonly (StrengthPlayer & { availability: number })[],
) => {
	order.length = 0;
	orderValue.length = 0;
	for (const p of players) {
		if (!(p.availability > 0)) {
			continue;
		}
		const v = p.valueNoPot;
		if (!Number.isFinite(v)) {
			return players.filter((x) => x.availability > 0).sort(byValueNoPot);
		}
		let j = order.length;
		while (j > 0 && v > orderValue[j - 1]!) {
			order[j] = order[j - 1]!;
			orderValue[j] = orderValue[j - 1]!;
			j -= 1;
		}
		order[j] = p;
		orderValue[j] = v;
	}
	return order;
};

// Played out both ways, exactly: the best players we may or may not have.
const ENUMERATED = 2;

export const expectedRosterPoints = (
	players: readonly (StrengthPlayer & { availability: number })[],
) => {
	const sorted = inPeckingOrder(players);
	const n = sorted.length;
	const uncertain: (StrengthPlayer & { availability: number })[] = [];
	for (const p of sorted) {
		if (p.availability < 1) {
			uncertain.push(p);
			if (uncertain.length === ENUMERATED) {
				break;
			}
		}
	}
	layOut(sorted, uncertain);
	let sure = n;
	if (uncertain.length > 0) {
		sure = 0;
		for (let i = 0; i < n; i++) {
			if (sortedEnumerated[i]! >= 0) {
				sure = i + 1;
			}
		}
		for (let i = 0; i < sure; i++) {
			if (sortedEnumerated[i]! < 0 && sortedAvailability[i] !== 1) {
				sure = -1;
				break;
			}
		}
	}
	if (sure >= 0) {
		fillRest(sure, n, 0);
	}
	if (uncertain.length === 0) {
		return byPlace(sure, 0);
	}
	// Each enumerated player wherever he appears, in turn.
	const which: number[] = [];
	const odds: number[] = [];
	for (const p of players) {
		const i = uncertain.indexOf(p);
		if (i >= 0) {
			which.push(i);
			odds.push(p.availability);
		}
	}
	let total = 0;
	for (let mask = 0; mask < 1 << uncertain.length; mask++) {
		let chance = 1;
		for (let j = 0; j < which.length; j++) {
			chance *= (mask >> which[j]!) & 1 ? odds[j]! : 1 - odds[j]!;
		}
		if (chance > 0) {
			if (sure < 0) {
				fillRest(0, n, mask);
			}
			total += chance * byPlace(Math.max(0, sure), mask);
		}
	}
	return total;
};

export const HOME_COURT_POINTS = MODEL.home;
