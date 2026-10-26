// WIN PROBABILITY. How likely each team is to win from where a game stands:
// for picking out the plays that mattered most (see court3d/keyPlays.ts), and
// drawn over the game in the box score (see GameCharts).
//
// What the score says at the end is where it stands now, plus what each team
// was expected to do over the time still to play, plus luck:
//
// - Expected: the pregame spread (see getGameSpread - team strength and home
//   court, stored on the game), as much of it as there is time left for. A
//   good team down 5-0 a minute in is still the favorite: the first few
//   possessions barely move it, because there is a whole game for the better
//   team to be better in.
// - Luck: over a whole game the final margin lands about MARGIN_SIGMA points
//   either side of the spread, and the time still to play has its share of
//   that - the square root of it (each stretch of a game as unpredictable as
//   any other). Late, there is little left: a few points is everything.
// - The ball: having it is worth about a possession's points more to whoever
//   has it, which counts for nothing early and a great deal in the last
//   minute.
//
// Overtime is more of the same: a few minutes more of it, from level.

// (Matching the sportsbook's own - common/sportsbookOdds.ts.)
const MARGIN_SIGMA = 13;
// Regulation in a league of the default length (seconds).
export const GAME_S = 48 * 60;
// Home court, when the game has no spread stored (points over a game).
export const HOME_EDGE = 3.35;
// What having the ball is worth (points): about half a possession's - the
// team with it gets, on average, half a possession more than the other in
// what remains.
const BALL_PTS = 0.55;

// The normal curve's area below z.
const phi = (z: number): number => {
	// (Abramowitz and Stegun 7.1.26, through the error function.)
	const x = Math.abs(z) / Math.SQRT2;
	const t = 1 / (1 + 0.3275911 * x);
	const y =
		1 -
		((((1.061405429 * t - 1.453152027) * t + 1.421413741) * t - 0.284496736) *
			t +
			0.254829592) *
			t *
			Math.exp(-x * x);
	return z >= 0 ? (1 + y) / 2 : (1 - y) / 2;
};

// The chance team 0 wins: `margin` ahead (team 0's points less team 1's)
// with `left` of regulation still to play (1 at the tip, 0 at the end - an
// overtime, a sliver of it), expected to outscore team 1 by `spread` over a
// whole game, the game `length` times as long as 48 minutes, with the ball
// (`ball` 1), without it (-1), or neither (0: between possessions).
export const winProb = ({
	margin,
	left,
	spread = 0,
	length = 1,
	ball = 0,
}: {
	margin: number;
	left: number;
	spread?: number;
	length?: number;
	ball?: -1 | 0 | 1;
}): number => {
	const f = Math.max(0, left);
	const sd = MARGIN_SIGMA * Math.sqrt(Math.max(0.05, length) * f);
	const mean = margin + spread * Math.min(1, f) + (f > 0 ? ball * BALL_PTS : 0);
	if (sd < 0.05) {
		return mean > 0 ? 1 : mean < 0 ? 0 : 0.5;
	}
	return Math.min(0.999, Math.max(0.001, phi(mean / sd)));
};

// A game's events (see the live game's play-by-play), and before each, how
// it stood: the period, the clock, the score - and the chance team 0 wins.
export type GameMoment = {
	period: number;
	clock: number;
	score: [number, number];
	p0: number;
};

// How long a period, and an overtime, runs (seconds): as long as its clock
// starts at - read off the game's own events, the league's length whatever
// it is.
export const lengthsOf = (
	events: any[],
	numPeriods: number,
): { regS: number; otS: number } => {
	let period = 1;
	let reg = 0;
	let ot = 0;
	for (const e of events) {
		if (e?.type === "period") {
			period += 1;
		}
		if (typeof e?.clock === "number") {
			if (period <= numPeriods) {
				reg = Math.max(reg, e.clock);
			} else {
				ot = Math.max(ot, e.clock);
			}
		}
	}
	const up = (s: number) => Math.ceil(s / 60) * 60;
	const regS = reg > 0 ? up(reg) : 12 * 60;
	return { regS, otS: ot > 0 ? up(ot) : Math.round((regS * 5) / 12) };
};

// Whose ball it is after an event (raw team 0 or 1), if that is settled -
// and undefined while it is up for grabs (a shot or a free throw missed, a
// period over).
const ballAfter = (e: any, was: 0 | 1 | undefined): 0 | 1 | undefined => {
	const type: unknown = e?.type;
	const t = e?.t === 0 || e?.t === 1 ? (e.t as 0 | 1) : undefined;
	if (typeof type !== "string") {
		return was;
	}
	const other = t === undefined ? undefined : ((1 - t) as 0 | 1);
	if (type === "jumpBall" || type === "drb" || type === "orb") {
		return t;
	}
	// (Stolen: the stealing team's. Turned over, or knocked out of bounds by
	// a team: the other's.)
	if (type === "stl") {
		return t;
	}
	if (type === "tov" || type === "outOfBounds") {
		return other;
	}
	if (type === "ft") {
		return other;
	}
	if (
		type === "missFt" ||
		type.startsWith("miss") ||
		type.startsWith("blk") ||
		type === "endOfPeriod" ||
		type === "period" ||
		type === "overtime"
	) {
		return undefined;
	}
	// A basket (and-ones: the free throw still to come - the ball the other
	// team's once it is taken).
	if (
		/^(fg|tp)/.test(type) &&
		!type.startsWith("fga") &&
		!type.startsWith("fgaTp")
	) {
		return other;
	}
	return was;
};

export const momentsOf = (
	events: any[],
	{
		numPeriods = 4,
		home0 = true,
		spread,
		edge = HOME_EDGE,
	}: {
		numPeriods?: number;
		home0?: boolean;
		// The pregame spread: how many points team 0 was expected to win by
		// (the game's stored one - see Game.spread; home court in it).
		spread?: number;
		// Failing that, home court alone (points over a game; 0 on a neutral
		// floor).
		edge?: number;
	} = {},
): GameMoment[] => {
	const { regS, otS } = lengthsOf(events, numPeriods);
	const total = regS * numPeriods;
	const expected =
		spread !== undefined && Number.isFinite(spread)
			? spread
			: ((home0 ? 1 : -1) * edge * total) / GAME_S;
	const out: GameMoment[] = [];
	const score: [number, number] = [0, 0];
	let period = 1;
	let clock = regS;
	let ball: 0 | 1 | undefined;
	const left = () =>
		(period <= numPeriods ? clock + (numPeriods - period) * regS : clock) /
		total;
	for (const e of events) {
		if (e?.type === "period") {
			period += 1;
			clock = period <= numPeriods ? regS : otS;
		}
		if (typeof e?.clock === "number") {
			clock = e.clock;
		}
		out.push({
			period,
			clock,
			score: [score[0], score[1]],
			p0: winProb({
				margin: score[0] - score[1],
				left: left(),
				spread: expected,
				length: total / GAME_S,
				ball: ball === undefined ? 0 : ball === 0 ? 1 : -1,
			}),
		});
		if (
			e?.type === "stat" &&
			e.s === "pts" &&
			(e.t === 0 || e.t === 1) &&
			typeof e.amt === "number"
		) {
			score[e.t as 0 | 1] += e.amt;
		}
		ball = ballAfter(e, ball);
	}
	// And once it is all over.
	const decided = score[0] > score[1] ? 1 : score[0] < score[1] ? 0 : 0.5;
	out.push({ period, clock: 0, score: [score[0], score[1]], p0: decided });
	return out;
};
