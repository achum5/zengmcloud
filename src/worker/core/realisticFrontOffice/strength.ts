import {
	synergyCompositeRating,
	synergyForLineup,
	synergyTotal,
	type SynergyCompositeRating,
	type SynergySkills,
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

export const playerLine = (ratings: ValueRatings): PlayerLine => ({
	roleFree: evaluate(MODEL.roleFree, ratings),
	offense: evaluate(MODEL.offense, ratings),
	usage: shotWeight(ratings),
	synergy: synergyCompositeRating(ratings as any),
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

// A player's synergy skills are worked out once per line: rosters are priced
// many thousands of times a day, and the lines are cached upstream.
const synergyInputs = new WeakMap<
	SynergyCompositeRating,
	{ compositeRating: SynergyCompositeRating }
>();
const synergySkills = new WeakMap<object, SynergySkills>();
const synergyInput = (synergy: SynergyCompositeRating) => {
	let input = synergyInputs.get(synergy);
	if (!input) {
		input = { compositeRating: synergy };
		synergyInputs.set(synergy, input);
	}
	return input;
};

const unitSynergy = (five: readonly StrengthPlayer[]) =>
	synergyTotal(
		synergyForLineup(
			five.map((p) => synergyInput(p.synergy)),
			synergySkills,
		),
	);

const byValueNoPot = (a: StrengthPlayer, b: StrengthPlayer) =>
	b.valueNoPot - a.valueNoPot;

// The roster's places from players already in pecking order, leaving one out.
const pointsOfSorted = (sorted: readonly StrengthPlayer[], skip = -1) => {
	const roster: StrengthPlayer[] = [];
	for (let i = 0; i < sorted.length && roster.length < ROSTER_PLACES; i++) {
		if (i !== skip) {
			roster.push(sorted[i]!);
		}
	}
	while (roster.length < ROSTER_PLACES) {
		roster.push(getReplacement());
	}
	let total = 0;
	for (let k = 0; k < ROSTER_PLACES; k++) {
		const p = roster[k]!;
		let others = 0;
		for (let j = 0; j < ROSTER_PLACES; j++) {
			if (j !== k) {
				others += TOGETHER[k]![j]! * roster[j]!.usage;
			}
		}
		const share = p.usage / (p.usage + others);
		total += SLOT_MINUTES[k]! * (p.roleFree + share * p.offense);
	}
	const synergy =
		STARTER_SYNERGY * unitSynergy(roster.slice(0, 5)) +
		(1 - STARTER_SYNERGY) * unitSynergy(roster.slice(5, 10));
	return total + MODEL.synergy * synergy;
};

// Points per game the roster is worth, before subtracting the league average.
// Only differences between rosters mean anything.
export const rosterPoints = (players: readonly StrengthPlayer[]) =>
	pointsOfSorted([...players].sort(byValueNoPot));

// What one player is worth as a single number: his points at the sixth man's
// minutes with a typical share of the ball, over a minimum player's. For
// ranking and explaining; rosters are always priced whole.
export const playerValue = (line: Pick<PlayerLine, "roleFree" | "offense">) => {
	const r = getReplacement();
	const typicalShare = 0.2;
	return (
		SLOT_MINUTES[5]! *
		(line.roleFree - r.roleFree + typicalShare * (line.offense - r.offense))
	);
};

// Below this, a player we may not have counts in part; above it, in full. Saves
// pricing every roster once per player for a 1% chance of retirement.
const AVAILABILITY_CUTOFF = 0.97;

// A player we expect to miss part of the season, or may not have at all,
// counts for the share of it we expect him - measured as what the roster loses
// without him.
export const expectedRosterPoints = (
	players: readonly (StrengthPlayer & { availability: number })[],
) => {
	const sorted = [...players].sort(byValueNoPot);
	const full = pointsOfSorted(sorted);
	let expected = full;
	// Only the men who make the rotation's places can change what it is worth.
	for (let i = 0; i < sorted.length && i < ROSTER_PLACES; i++) {
		const p = sorted[i]!;
		if (p.availability < AVAILABILITY_CUTOFF) {
			expected -= (1 - p.availability) * (full - pointsOfSorted(sorted, i));
		}
	}
	return expected;
};

export const HOME_COURT_POINTS = MODEL.home;
