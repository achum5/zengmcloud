// WIN PROBABILITY. How likely each team is to win from where a game stands:
// drawn over the game on its page (see gamecast/FlowCharts and GameCharts),
// and for picking out the plays that mattered most (see court3d/keyPlays.ts).
//
// Measured against the game's own sim, not reasoned out: every number here
// comes from tens of thousands of simmed games and how often the team given
// each chance went on to win (see worker/core/game/winProbCalibration.test.ts,
// which measures it all again).
//
// Until the last minute, the margin at the end is where it stands now, plus
// what each team was expected to do in the time still to play, less some of
// any lead, plus luck:
//
// - Expected: the pregame spread (see getGameSpread - team strength and home
//   court, stored on the game), as much of it as there is time left for.
// - Leads fade: a team further ahead than it was expected to be gives some of
//   it back (its starters sit, the team behind presses) - by the end, about
//   a fifth of it from halftime, nearly a third from the end of the first
//   quarter.
// - Luck: the scoring still to come (less than in proportion to the time
//   left: what fades does not add up), and the finish's own - the fouling,
//   the last shots - however far off the finish is.
// - The ball: worth about half a point to the team that has it.
// - Level at the end is an overtime: a game of its own, from level.
//
// In the last minute a possession is everything - down 3 with the ball is a
// shot at a tie, down 4 is not - and no curve says that. There the chances
// are read straight off the sim's own finishes (FINISH).

// Regulation in a league of the default length (seconds).
export const GAME_S = 48 * 60;
// Home court, when the game has no spread stored (points over a game).
export const HOME_EDGE = 3.35;

// How fast a lead beyond what was expected fades: e^-FADE of it is left over
// a whole game still to play.
const FADE = 0.48;
// The scoring's luck: its variance over a whole game (points squared), before
// any of it fades.
const LUCK = 290;
// The finish's own luck (points squared).
const FINISH_LUCK = 2.3;
// What having the ball is worth (points): BALL late in a game, BALL_EARLY
// less at the tip (who has it at the start of each quarter is settled by the
// opening tip, whoever has it now).
const BALL = 0.53;
const BALL_EARLY = 0.18;

// THE FINISH: the chance (per thousand) the team with the ball wins, by how
// far ahead of the other it is (a row each, down FINISH_MARGIN to up it) and
// the seconds left (FINISH_S, between them in proportion) - in the last
// minute of the last period, or of an overtime, the two alike. Measured (see
// above): every half second of the last minute of 160,000 games. A better
// team's chances are better than these (the worse team's, worse), by
// FINISH_SPREAD on the log-odds for every point of the spread.
const FINISH_S = [
	0, 1, 2, 3, 4.5, 6, 8, 10, 13, 16, 20, 25, 30, 36, 42, 48, 54, 60,
];
const FINISH_MARGIN = 6;
const FINISH: number[][] = [
	// Down 6
	[0, 0, 0, 0, 0, 1, 1, 2, 3, 5, 7, 10, 12, 18, 24, 28, 29, 30],
	[0, 0, 1, 1, 2, 5, 8, 11, 14, 18, 21, 27, 38, 51, 58, 59, 67, 73],
	[0, 1, 3, 6, 10, 16, 22, 30, 40, 51, 62, 69, 77, 91, 100, 109, 119, 129],
	// Down 3
	[
		83, 95, 110, 119, 126, 130, 130, 129, 128, 132, 139, 143, 155, 173, 179,
		183, 194, 200,
	],
	[
		127, 163, 201, 229, 248, 256, 253, 248, 246, 257, 272, 279, 285, 291, 300,
		314, 319, 320,
	],
	[
		185, 241, 294, 340, 371, 387, 390, 394, 406, 420, 424, 414, 404, 416, 432,
		434, 440, 442,
	],
	// Level
	[
		595, 624, 648, 657, 659, 653, 644, 630, 610, 593, 592, 598, 599, 593, 584,
		581, 580, 574,
	],
	[
		995, 978, 952, 932, 906, 872, 833, 813, 800, 799, 788, 771, 754, 749, 742,
		730, 722, 717,
	],
	[
		996, 990, 981, 967, 956, 945, 928, 915, 909, 900, 890, 875, 860, 848, 838,
		828, 816, 812,
	],
	// Up 3
	[
		998, 998, 998, 995, 988, 983, 977, 972, 964, 955, 950, 940, 933, 928, 920,
		907, 895, 889,
	],
	[
		1000, 1000, 1000, 1000, 1000, 1000, 999, 998, 995, 992, 988, 982, 974, 968,
		964, 959, 949, 943,
	],
	[
		1000, 1000, 1000, 1000, 1000, 1000, 1000, 999, 998, 997, 994, 992, 989, 987,
		984, 980, 973, 970,
	],
	// Up 6
	[
		1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 999, 998, 996,
		996, 995, 993, 991, 990,
	],
];
const FINISH_SPREAD = 0.026;
// Read off FINISH with up to FINISH_TO seconds left, the curve with
// FINISH_FROM or more, and in between, some of each.
const FINISH_TO = 45;
const FINISH_FROM = 60;

// Never quite certain until it is over.
const SURE = 0.999;

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

// The curve: the chances team 0 is ahead when this stretch of the game
// (regulation, or an overtime) ends, and that it is level - `secs` of it to
// play, `run` seconds long all told, the ball team 0's (`ball` 1) or the
// other's (-1).
const ends = (
	margin: number,
	secs: number,
	spread: number,
	regS: number,
	run: number,
	ball: 1 | -1,
): [number, number] => {
	const fade = FADE / GAME_S;
	// (What was expected of team 0 in the stretch so far, and still to come.)
	const soFar = (spread * Math.max(0, run - secs)) / regS;
	const toCome = (spread * secs) / regS;
	const mean =
		margin +
		toCome -
		(1 - Math.exp(-fade * secs)) * (margin - soFar) +
		ball * (BALL - BALL_EARLY * Math.min(1, secs / regS));
	const sd = Math.sqrt(
		(LUCK / GAME_S) * ((1 - Math.exp(-2 * fade * secs)) / (2 * fade)) +
			FINISH_LUCK,
	);
	// (Points are whole: ahead is half a point or more, level is within half
	// of one.)
	const ahead = phi((mean - 0.5) / sd);
	return [ahead, phi((mean + 0.5) / sd) - ahead];
};

// The chance team 0 wins an overtime, from its tip (and any after it).
const overtimeChance = (spread: number, regS: number, otS: number) => {
	let ahead = 0;
	let level = 0;
	for (const ball of [1, -1] as const) {
		const [a, l] = ends(0, otS, spread, regS, otS, ball);
		ahead += a / 2;
		level += l / 2;
	}
	return ahead / (1 - level);
};

// The finish: the chance the team with the ball wins, `ahead` points up with
// `secs` left, expected to win by `spread` over a game - if FINISH says.
const finishChance = (
	ahead: number,
	secs: number,
	spread: number,
): number | undefined => {
	const row = FINISH[ahead + FINISH_MARGIN];
	if (!row || !Number.isInteger(ahead)) {
		return undefined;
	}
	let k = 0;
	while (FINISH_S[k + 1]! <= secs) {
		k += 1;
	}
	const a = FINISH_S[k]!;
	const b = FINISH_S[k + 1]!;
	const w = (secs - a) / (b - a);
	const p = (row[k]! * (1 - w) + row[k + 1]! * w) / 1000;
	if (p <= 0 || p >= 1) {
		return p;
	}
	const odds = (p / (1 - p)) * Math.exp(FINISH_SPREAD * spread);
	return odds / (1 + odds);
};

export type Standing = {
	// Team 0's points less team 1's.
	margin: number;
	// The seconds still to play: of regulation, or in an overtime, of it.
	secs: number;
	overtime?: boolean;
	// How many points team 0 was expected to win by over regulation (the
	// pregame spread).
	spread?: number;
	// How long regulation is, and an overtime (seconds).
	regS?: number;
	otS?: number;
	// The chance team 0 has the ball: 1 it has, 0 the other team has, 0.5
	// nobody knows.
	ball?: number;
};

// The chance team 0 wins.
export const winProb = ({
	margin,
	secs,
	overtime = false,
	spread = 0,
	regS = GAME_S,
	otS = (regS * 5) / 48,
	ball = 0.5,
}: Standing): number => {
	const ot = overtimeChance(spread, regS, otS);
	if (secs <= 0) {
		return margin > 0 ? 1 : margin < 0 ? 0 : ot;
	}
	const p = Math.min(1, Math.max(0, ball));
	const curve = () => {
		let out = 0;
		for (const [side, share] of [
			[1, p],
			[-1, 1 - p],
		] as const) {
			if (share > 0) {
				const [ahead, level] = ends(
					margin,
					secs,
					spread,
					regS,
					overtime ? otS : regS,
					side,
				);
				out += share * (ahead + level * ot);
			}
		}
		return out;
	};
	let out: number;
	const mine =
		secs < FINISH_FROM ? finishChance(margin, secs, spread) : undefined;
	const theirs =
		secs < FINISH_FROM ? finishChance(-margin, secs, -spread) : undefined;
	if (mine === undefined || theirs === undefined) {
		out = curve();
	} else {
		const finish = p * mine + (1 - p) * (1 - theirs);
		const w = Math.max(0, (secs - FINISH_TO) / (FINISH_FROM - FINISH_TO));
		out = w > 0 ? (1 - w) * finish + w * curve() : finish;
	}
	return Math.min(SURE, Math.max(1 - SURE, out));
};

// The plain curve, for a game this was not measured on (football's key
// plays): the margin at the end a normal curve about the spread, as wide as
// the time left allows - MARGIN_SIGMA points over a whole game (matching
// the sportsbook's - common/sportsbookOdds.ts) - with `left` the share of
// regulation still to play (1 at the tip, 0 at the end), `length` the game
// as many times as long as 48 minutes, and `ball` 1 (team 0's), -1 (the
// other's) or 0.
const MARGIN_SIGMA = 13;
const BALL_PTS = 0.55;
export const normalWinProb = ({
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

// Who had the ball after a score `since` seconds ago: the team scored on,
// to start with, and then, as the possessions go by, nobody can say.
// (Measured: still theirs 90% of the time 6 seconds on, about 80% after 10,
// a toss-up by 20.)
const BALL_AFTER_SCORE_S = 13;
export const ballAfterScore = (since: number) =>
	0.5 + 0.5 * Math.exp(-((Math.max(0, since) / BALL_AFTER_SCORE_S) ** 2));

// A game drawn from its scores (see BoxScoreFlow): the chance team 0 wins at
// any moment of it (seconds since the tip) - from the score then, the clock,
// and who probably had the ball, the last team scored on. Once it is over
// (`over`), the result.
export const flowChance = (
	{
		scores,
		periodEnds,
		total,
	}: {
		// The score after each score, from 0-0: when, team 0's, team 1's.
		scores: readonly (readonly [number, number, number])[];
		periodEnds: readonly number[];
		total: number;
	},
	{
		numPeriods,
		spread,
		over = false,
	}: { numPeriods: number; spread: number; over?: boolean },
) => {
	const regS = periodEnds[numPeriods - 1] ?? total;
	// (An overtime as long as the first one played, or five minutes of a
	// 48-minute game.)
	const otS =
		periodEnds.length > numPeriods
			? periodEnds[numPeriods]! - regS
			: (regS * 5) / 48;
	return (t: number): number => {
		// The last score at or before t.
		let lo = 0;
		let hi = scores.length - 1;
		while (lo < hi) {
			const mid = (lo + hi + 1) >> 1;
			if (scores[mid]![0] <= t) {
				lo = mid;
			} else {
				hi = mid - 1;
			}
		}
		const s = scores[lo]!;
		const margin = s[1] - s[2];
		if (over && t >= total) {
			return margin > 0 ? 1 : margin < 0 ? 0 : 0.5;
		}
		const k = periodEnds.findIndex((end) => t < end);
		const period = k < 0 ? periodEnds.length - 1 : k;
		const start = period > 0 ? periodEnds[period - 1]! : 0;
		const overtime = period >= numPeriods;
		const end = overtime ? (periodEnds[period] ?? total) : regS;
		let ball = 0.5;
		const prev = scores[lo - 1];
		if (prev && s[0] >= start) {
			const p = ballAfterScore(t - s[0]);
			// (Scored on: team 0, if team 1's score went up.)
			ball = s[2] > prev[2] ? p : 1 - p;
		}
		return winProb({
			margin,
			secs: Math.max(0, end - t),
			overtime,
			spread,
			regS,
			otS,
			ball,
		});
	};
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
		if (e?.type === "period" || e?.type === "overtime") {
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

// (A rebound the defense gets this often.)
const DEFENSIVE_BOARD = 0.75;

// The chance team 0 has the ball after an event, from the chance it had it
// before: settled most of the time, a rebound's odds after a miss, a toss-up
// as a period ends.
const ballAfter = (e: any, was: number): number => {
	const type: unknown = e?.type;
	const t = e?.t === 0 || e?.t === 1 ? (e.t as 0 | 1) : undefined;
	if (typeof type !== "string") {
		return was;
	}
	// (Team 0's chance, were it team `side`'s this often.)
	const has = (side: 0 | 1 | undefined, chance = 1) =>
		side === undefined ? was : side === 0 ? chance : 1 - chance;
	const other = t === undefined ? undefined : ((1 - t) as 0 | 1);
	if (type === "jumpBall" || type === "drb" || type === "orb") {
		return has(t);
	}
	// (Stolen: the stealing team's. Turned over, or knocked out of bounds by
	// a team: the other's.)
	if (type === "stl") {
		return has(t);
	}
	if (type === "tov" || type === "outOfBounds" || type === "ft") {
		return has(other);
	}
	if (type === "endOfPeriod" || type === "period" || type === "overtime") {
		return 0.5;
	}
	// Off the rim: most likely the other team's - blocked, the blocker's (a
	// block is the blocking team's line).
	if (type.startsWith("miss")) {
		return has(other, DEFENSIVE_BOARD);
	}
	if (type.startsWith("blk")) {
		return has(t, DEFENSIVE_BOARD);
	}
	// A basket (and-ones: the free throw still to come - the ball the other
	// team's once it is taken).
	if (
		/^(fg|tp)/.test(type) &&
		!type.startsWith("fga") &&
		!type.startsWith("fgaTp")
	) {
		return has(other);
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
	const { regS: periodS, otS } = lengthsOf(events, numPeriods);
	const regS = periodS * numPeriods;
	const expected =
		spread !== undefined && Number.isFinite(spread)
			? spread
			: ((home0 ? 1 : -1) * edge * regS) / GAME_S;
	const out: GameMoment[] = [];
	const score: [number, number] = [0, 0];
	let period = 1;
	let clock = periodS;
	let ball = 0.5;
	for (const e of events) {
		if (e?.type === "period" || e?.type === "overtime") {
			period += 1;
			clock = period <= numPeriods ? periodS : otS;
		}
		if (typeof e?.clock === "number") {
			clock = e.clock;
		}
		const overtime = period > numPeriods;
		out.push({
			period,
			clock,
			score: [score[0], score[1]],
			p0: winProb({
				margin: score[0] - score[1],
				secs: overtime ? clock : clock + (numPeriods - period) * periodS,
				overtime,
				spread: expected,
				regS,
				otS,
				ball,
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
