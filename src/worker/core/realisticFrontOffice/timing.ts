import { makeSeededRandom } from "../../../common/random.ts";
import { fitLogistic } from "./logistic.ts";
import { simulateSeason, type SeasonSimTeam } from "./seasonSim.ts";

// WHEN TO MAKE A DEAL. A deal made today helps every game left; made at the
// trade deadline it misses the regular-season games before it, but the
// playoffs get all of it - and by then a team knows much better how good it
// really is, so it can still say no. Against that, a deal left for later may
// not be there. A team makes a deal now only when that beats deciding at the
// deadline.
//
// Contenders buying for the playoffs gain the most from waiting; a team
// building for later seasons gains nothing from this season's information, so
// it would as soon deal today. Deals between the two bunch up towards the
// deadline only as far as waiting is really worth it.

// This season's title chances as a function of regular-season and playoff
// strength (points a game against an average team), as read at the deadline.
export type DeadlineCurve = { a: number; regular: number; playoff: number };

export const deadlineCurveAt = (
	curve: DeadlineCurve,
	regular: number,
	playoff: number,
) =>
	1 /
	(1 +
		Math.exp(-(curve.a + curve.regular * regular + curve.playoff * playoff)));

export type DeadlineView = {
	daysToDeadline: number;
	daysLeft: number;
	// How much of our uncertainty about a team's strength is gone by the
	// deadline, in points a game.
	revealed: number;
	curve: DeadlineCurve;
};

// How much of the best deal a team can make today is still to be had at the
// deadline, by days to go - not that exact deal, but the best one on offer
// then. Measured in arena leagues (ARENA_DEAL_SURVEY): at stops before each
// deadline, every team's best deal with anyone, found but not made, against
// its best at the deadline, for teams that made no trade in between.
const STILL_ON_OFFER: readonly (readonly [number, number])[] = [
	[0, 1],
	[7, 0.89],
	[14, 0.81],
	[28, 0.64],
	[42, 0.6],
];

export const stillOnOffer = (days: number) => {
	const last = STILL_ON_OFFER.at(-1)!;
	if (days >= last[0]) {
		return last[1];
	}
	let i = 0;
	while (STILL_ON_OFFER[i + 1]![0] < days) {
		i += 1;
	}
	const [d0, v0] = STILL_ON_OFFER[i]!;
	const [d1, v1] = STILL_ON_OFFER[i + 1]!;
	return v0 + ((v1 - v0) * (days - d0)) / (d1 - d0);
};

// Teams are given made-up offsets between their regular-season and playoff
// strength, so the fit can tell the two apart.
const OFFSET_SPREAD = 2.5;
const FIT_REPS = 8;
const FIT_ITERATIONS = 250;

const fitDeadlineCurve = (
	points: { regular: number; playoff: number; p: number }[],
): DeadlineCurve => {
	const [a, regular, playoff] = fitLogistic(
		points.map(({ regular, playoff, p }) => ({ x: [regular, playoff], p })),
		[0, 0.15, 0.15],
	);
	return { a: a!, regular: regular!, playoff: playoff! };
};

export const buildDeadlineView = ({
	teams,
	numGamesPlayoffSeries,
	playIn,
	byConference,
	uncertaintyNow,
	uncertaintyAtDeadline,
	daysToDeadline,
	daysLeft,
	seed,
}: {
	teams: readonly SeasonSimTeam[];
	numGamesPlayoffSeries: readonly number[];
	playIn: boolean;
	byConference: boolean;
	uncertaintyNow: number;
	uncertaintyAtDeadline: number;
	daysToDeadline: number;
	daysLeft: number;
	seed: number;
}): DeadlineView => {
	const random = makeSeededRandom(seed);
	const gauss = () => {
		const u = Math.max(1e-12, random());
		return Math.sqrt(-2 * Math.log(u)) * Math.cos(2 * Math.PI * random());
	};
	const points: { regular: number; playoff: number; p: number }[] = [];
	for (let rep = 0; rep < FIT_REPS; rep++) {
		const shifted = teams.map((t) => ({
			...t,
			strength: t.strength + OFFSET_SPREAD * gauss(),
			playoffStrength: t.strength + OFFSET_SPREAD * gauss(),
		}));
		const result = simulateSeason({
			teams: shifted,
			numGamesPlayoffSeries,
			playIn,
			byConference,
			uncertainty: uncertaintyAtDeadline,
			iterations: FIT_ITERATIONS,
			seed: seed * 17 + rep,
		});
		for (const t of shifted) {
			points.push({
				regular: t.strength,
				playoff: t.playoffStrength,
				p: result.get(t.tid)!.title,
			});
		}
	}
	return {
		daysToDeadline,
		daysLeft,
		revealed: Math.sqrt(
			Math.max(0, uncertaintyNow ** 2 - uncertaintyAtDeadline ** 2),
		),
		curve: fitDeadlineCurve(points),
	};
};

// Seven-point Gauss-Hermite rule for an average over a standard normal.
const NORMAL_NODES = [
	[-3.750439717725742, 0.000548268855972],
	[-2.366759410734541, 0.030757123967586],
	[-1.154405394739968, 0.240123178605013],
	[0, 0.457142857142857],
	[1.154405394739968, 0.240123178605013],
	[2.366759410734541, 0.030757123967586],
	[3.750439717725742, 0.000548268855972],
] as const;

// Whether a team would rather decide on a deal at the deadline than make it
// today. `before` and `after` are this season's strength without and with the
// deal; `later` is what it is worth in the seasons after this one, in titles.
export const prefersWaiting = ({
	view,
	before,
	after,
	later,
}: {
	view: DeadlineView;
	before: number;
	after: number;
	later: number;
}) => {
	// Of the regular-season games left, the share played after the deadline.
	const afterDeadline =
		view.daysLeft > 0
			? Math.max(0, view.daysLeft - view.daysToDeadline) / view.daysLeft
			: 0;
	const stillThere = stillOnOffer(view.daysToDeadline);
	let now = 0;
	let wait = 0;
	for (const [z, weight] of NORMAL_NODES) {
		// What the team will have learned about itself by the deadline.
		const d = z * view.revealed;
		const without = deadlineCurveAt(view.curve, before + d, before + d);
		now +=
			weight *
			(deadlineCurveAt(view.curve, after + d, after + d) - without + later);
		const atDeadline =
			deadlineCurveAt(
				view.curve,
				before + d + afterDeadline * (after - before),
				after + d,
			) -
			without +
			later;
		wait += weight * Math.max(0, atDeadline);
	}
	return stillThere * wait > now;
};
