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
	// Chance a given player is traded on any one day, from this season's trades
	// so far: how likely a deal left for later is to have come apart.
	playerTradeRate: number;
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
	playerTradeRate,
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
	playerTradeRate: number;
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
		playerTradeRate,
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
// deal; `later` is what it is worth in the seasons after this one, in titles;
// `numPlayers` is how many players it moves.
export const prefersWaiting = ({
	view,
	before,
	after,
	later,
	numPlayers,
}: {
	view: DeadlineView;
	before: number;
	after: number;
	later: number;
	numPlayers: number;
}) => {
	// Of the regular-season games left, the share played after the deadline.
	const afterDeadline =
		view.daysLeft > 0
			? Math.max(0, view.daysLeft - view.daysToDeadline) / view.daysLeft
			: 0;
	const stillThere = Math.exp(
		-view.playerTradeRate * numPlayers * view.daysToDeadline,
	);
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
