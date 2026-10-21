import { makeSeededRandom } from "../../../common/random.ts";
import { HOME_COURT_POINTS } from "./strength.ts";

// THE REST OF A SEASON, many times over: what a front office means by "our
// chances". Deterministic for a seed, so every device in a shared league reads
// the same odds off the same league.
//
// Deliberately coarse where coarse is enough. Remaining games are against the
// field rather than the real schedule, and ties in the standings break at
// random. What it has to get right is the shape of the thing a team is trying
// to win: a conference bracket, a play-in, best-of series with home court, and
// a final.

// Spread of a single game's margin around its expectation, in points: what is
// left over in src/test/impact once both rosters and their minutes are known.
const GAME_SIGMA = 13.1;

export type SeasonSimTeam = {
	tid: number;
	cid: number;
	// Points per game against an average team.
	strength: number;
	// In the playoffs, if different - a team that will change by then, like one
	// trading at the deadline.
	playoffStrength?: number;
	won: number;
	lost: number;
	gamesRemaining: number;
};

export type SeasonSimResult = {
	title: number;
	finals: number;
	playoffs: number;
	// Playoff series won, on average.
	roundsWon: number;
};

export const normalCdf = (x: number) => {
	// Abramowitz and Stegun 7.1.26, good to 1e-7.
	const t = 1 / (1 + 0.3275911 * (Math.abs(x) / Math.SQRT2));
	const y =
		1 -
		((((1.061405429 * t - 1.453152027) * t + 1.421413741) * t - 0.284496736) *
			t +
			0.254829592) *
			t *
			Math.exp(-(x * x) / 2);
	return x >= 0 ? (1 + y) / 2 : (1 - y) / 2;
};

export const winProbability = (margin: number) =>
	normalCdf(margin / GAME_SIGMA);

// Whether a game with this margin goes to the home side for this random
// number: random < winProbability(margin), exactly. The chance only rises with
// the margin, so it is nearly always settled by the chances at grid points
// either side of it, and only worked out in full when the random number falls
// between them.
const GRID_PER_UNIT = 256;
const GRID_FROM = -8;
const GRID_POINTS = 16 * GRID_PER_UNIT;
const gridP = new Float64Array(GRID_POINTS + 1);
for (let k = 0; k <= GRID_POINTS; k++) {
	gridP[k] = normalCdf(GRID_FROM + k / GRID_PER_UNIT);
}
// Far more than normalCdf's rounding could ever take it off its rise.
const GRID_SLACK = 1e-9;
const wins = (random: number, margin: number) => {
	const x = margin / GAME_SIGMA;
	// x is between grid points k and k + 1, give or take rounding, so
	// certainly between k - 1 and k + 2.
	const k = Math.floor((x - GRID_FROM) * GRID_PER_UNIT);
	if (k >= 1 && k < GRID_POINTS - 1) {
		if (random < gridP[k - 1]! - GRID_SLACK) {
			return true;
		}
		if (random >= gridP[k + 2]! + GRID_SLACK) {
			return false;
		}
	}
	return random < normalCdf(x);
};

export const simulateSeason = ({
	teams,
	numGamesPlayoffSeries,
	playIn,
	byConference,
	uncertainty,
	iterations,
	seed,
}: {
	teams: readonly SeasonSimTeam[];
	// Best-of lengths, first round first.
	numGamesPlayoffSeries: readonly number[];
	playIn: boolean;
	byConference: boolean;
	// How far each team's true strength may be from our read of it, in points.
	uncertainty: number;
	iterations: number;
	seed: number;
}): Map<number, SeasonSimResult> => {
	const random = makeSeededRandom(seed);
	const gauss = () => {
		const u = Math.max(1e-12, random());
		const v = random();
		return Math.sqrt(-2 * Math.log(u)) * Math.cos(2 * Math.PI * v);
	};

	const results = new Map<number, SeasonSimResult>();
	for (const t of teams) {
		results.set(t.tid, { title: 0, finals: 0, playoffs: 0, roundsWon: 0 });
	}

	const numRounds = numGamesPlayoffSeries.length;
	const groups = byConference
		? [...new Set(teams.map((t) => t.cid))].sort((a, b) => a - b)
		: [0];
	// Teams per group that make the bracket proper.
	const perGroup = Math.max(1, 2 ** numRounds / groups.length);

	// Everything after the regular season - play-in and playoffs - is played at
	// playoff strength.
	const playSeries = <T extends { tid: number; playoffStrength: number }>(
		better: T,
		other: T,
		bestOf: number,
	) => {
		const needed = Math.ceil(bestOf / 2);
		// The better seed is at home for the larger half of the games.
		const homeShare = needed / bestOf;
		const p = winProbability(
			better.playoffStrength -
				other.playoffStrength +
				HOME_COURT_POINTS * (2 * homeShare - 1),
		);
		let a = 0;
		let b = 0;
		while (a < needed && b < needed) {
			if (random() < p) {
				a += 1;
			} else {
				b += 1;
			}
		}
		return a >= needed ? better : other;
	};

	const playGame = <T extends { tid: number; playoffStrength: number }>(
		home: T,
		away: T,
	) =>
		random() <
		winProbability(
			home.playoffStrength - away.playoffStrength + HOME_COURT_POINTS,
		)
			? home
			: away;

	const n = teams.length;
	const strengths = new Float64Array(n);
	const gamesRemaining = teams.map((t) => t.gamesRemaining);

	// Each team's season, played again every iteration.
	const sim = teams.map((t) => ({
		tid: t.tid,
		cid: byConference ? t.cid : 0,
		strength: 0,
		playoffStrength: 0,
		wins: 0,
		tiebreak: 0,
	}));

	for (let it = 0; it < iterations; it++) {
		for (let ti = 0; ti < n; ti++) {
			const t = teams[ti]!;
			const s = sim[ti]!;
			const error = uncertainty * gauss();
			s.strength = t.strength + error;
			s.playoffStrength = (t.playoffStrength ?? t.strength) + error;
			s.wins = t.won;
			s.tiebreak = random();
		}

		// The rest of the regular season, against the field.
		for (let ti = 0; ti < n; ti++) {
			strengths[ti] = sim[ti]!.strength;
		}
		for (let ti = 0; ti < n; ti++) {
			const games = gamesRemaining[ti]!;
			let won = 0;
			for (let i = 0; i < games; i++) {
				const oi = Math.floor(random() * n);
				if (oi === ti) {
					i -= 1;
					continue;
				}
				const margin =
					strengths[ti]! -
					strengths[oi]! +
					(i % 2 === 0 ? 1 : -1) * HOME_COURT_POINTS;
				if (wins(random(), margin)) {
					won += 1;
				}
			}
			sim[ti]!.wins += won;
		}

		// Seeds within each group, then the play-in for the last two places.
		const champions = [];
		for (const group of groups) {
			const ranked = sim
				.filter((t) => t.cid === group)
				.sort((a, b) => b.wins - a.wins || b.tiebreak - a.tiebreak);
			let seeds = ranked.slice(0, perGroup);
			if (playIn && perGroup >= 2 && ranked.length >= perGroup + 2) {
				const [s7, s8, s9, s10] = ranked.slice(perGroup - 2, perGroup + 2);
				const w78 = playGame(s7!, s8!);
				const l78 = w78 === s7 ? s8! : s7!;
				const w910 = playGame(s9!, s10!);
				const last = playGame(l78, w910);
				seeds = [...ranked.slice(0, perGroup - 2), w78, last];
			}
			for (const t of seeds) {
				results.get(t.tid)!.playoffs += 1;
			}

			// 1 v n, 2 v n-1, ... laid out so the top seeds meet as late as
			// possible.
			let bracket = seeds;
			const order: number[] = [0];
			while (order.length < bracket.length) {
				const size = order.length * 2;
				const next: number[] = [];
				for (const i of order) {
					next.push(i, size - 1 - i);
				}
				order.splice(0, order.length, ...next);
			}
			bracket = order.map((i) => seeds[i]!).filter(Boolean);

			let round = 0;
			while (bracket.length > 1) {
				if (groups.length === 1 && bracket.length === 2) {
					for (const t of bracket) {
						results.get(t.tid)!.finals += 1;
					}
				}
				const winners = [];
				for (let i = 0; i < bracket.length; i += 2) {
					const a = bracket[i]!;
					const b = bracket[i + 1];
					if (!b) {
						winners.push(a);
						continue;
					}
					const better =
						a.wins > b.wins || (a.wins === b.wins && a.tiebreak > b.tiebreak)
							? a
							: b;
					const other = better === a ? b : a;
					const winner = playSeries(
						better,
						other,
						numGamesPlayoffSeries[round] ?? 7,
					);
					results.get(winner.tid)!.roundsWon += 1;
					winners.push(winner);
				}
				bracket = winners;
				round += 1;
			}
			champions.push({ team: bracket[0]!, round });
		}

		// Group champions meet in the final rounds.
		let finalists = champions.map((c) => c.team);
		let round = champions[0]?.round ?? 0;
		while (finalists.length > 1) {
			if (finalists.length === 2) {
				for (const t of finalists) {
					results.get(t.tid)!.finals += 1;
				}
			}
			const winners = [];
			for (let i = 0; i < finalists.length; i += 2) {
				const a = finalists[i]!;
				const b = finalists[i + 1];
				if (!b) {
					winners.push(a);
					continue;
				}
				const better = a.wins >= b.wins ? a : b;
				const other = better === a ? b : a;
				const winner = playSeries(
					better,
					other,
					numGamesPlayoffSeries[round] ?? 7,
				);
				results.get(winner.tid)!.roundsWon += 1;
				winners.push(winner);
			}
			finalists = winners;
			round += 1;
		}
		if (finalists[0]) {
			results.get(finalists[0].tid)!.title += 1;
		}
	}

	for (const r of results.values()) {
		r.title /= iterations;
		r.finals /= iterations;
		r.playoffs /= iterations;
		r.roundsWon /= iterations;
	}
	return results;
};
