// WIN PROBABILITY. How likely each team is to win from where a game stands:
// the margin, against how much the score can still move in the time left.
//
// Over a whole game the final margin wanders about NBA_SD points either side
// of where it was expected to finish - each second about as much as any
// other - so with `s` seconds to go it can still move about
// NBA_SD * sqrt(s / GAME_S), and the chance the team ahead stays ahead is
// how many of those it leads by (a normal curve). The home team's edge is
// worth so many points over a game, as much of it as there is time left for.
// Overtime is more of the same: a few minutes more of it.

const NBA_SD = 13;
const GAME_S = 48 * 60;
const HOME_EDGE = 2;

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

// The chance team 0 wins, `margin` ahead (team 0's points less team 1's)
// with `secondsLeft` to play, team 0 at home or not.
export const winProb = (
	margin: number,
	secondsLeft: number,
	home0 = true,
	edge = HOME_EDGE,
): number => {
	const f = Math.max(0, secondsLeft) / GAME_S;
	const mean = margin + (home0 ? edge : -edge) * Math.min(1, f);
	const sd = NBA_SD * Math.sqrt(f);
	if (sd < 0.05) {
		return margin > 0 ? 1 : margin < 0 ? 0 : 0.5;
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

export const momentsOf = (
	events: any[],
	{
		numPeriods = 4,
		home0 = true,
		edge = HOME_EDGE,
	}: {
		numPeriods?: number;
		home0?: boolean;
		// (0 on a neutral floor.)
		edge?: number;
	} = {},
): GameMoment[] => {
	const { regS, otS } = lengthsOf(events, numPeriods);
	// (A game of other than 48 minutes: the margin can move as much in it,
	// over its length, as in one of 48.)
	const scale = GAME_S / (regS * numPeriods);
	const out: GameMoment[] = [];
	const score: [number, number] = [0, 0];
	let period = 1;
	let clock = regS;
	const left = () =>
		(period <= numPeriods ? clock + (numPeriods - period) * regS : clock) *
		scale;
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
			p0: winProb(score[0] - score[1], left(), home0, edge),
		});
		if (
			e?.type === "stat" &&
			e.s === "pts" &&
			(e.t === 0 || e.t === 1) &&
			typeof e.amt === "number"
		) {
			score[e.t as 0 | 1] += e.amt;
		}
	}
	// And once it is all over.
	const decided = score[0] > score[1] ? 1 : score[0] < score[1] ? 0 : 0.5;
	out.push({ period, clock: 0, score: [score[0], score[1]], p0: decided });
	return out;
};

// How far into the game (0 to 1, more in overtime) a moment is.
export const elapsedOf = (
	m: GameMoment,
	{ regS, otS }: { regS: number; otS: number },
	numPeriods = 4,
): number => {
	const total = regS * numPeriods;
	const s =
		m.period <= numPeriods
			? (m.period - 1) * regS + (regS - m.clock)
			: total + (m.period - numPeriods - 1) * otS + (otS - m.clock);
	return s / total;
};
