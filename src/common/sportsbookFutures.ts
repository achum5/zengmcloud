import {
	marginToWinProb,
	seriesWinProb,
	toHalfPointLine,
} from "./sportsbookOdds.ts";

// Bookmaker-grade futures: a Monte Carlo simulation of the rest of the season
// and the whole playoff bracket. Every market (division, conference, title,
// win totals) is read off the SAME simulated outcomes, so they can never
// contradict each other - a team's title probability is exactly the subset of
// simulations where it also won its conference, and a 46-3 juggernaut prices
// like one because it actually has to lose four times in a series to be denied.
//
// Deterministic for a given seed, so lines are stable between sims (they only
// move when league state changes) and the server can re-derive the same board
// to validate a bet.

export type FuturesTeam = {
	tid: number;
	cid: number;
	did: number;
	won: number;
	gamesRemaining: number;
	// Point margin vs an average team (rating + performance blend).
	rating: number;
};

export type FuturesResult = {
	titleProb: Map<number, number>;
	confProb: Map<number, number>;
	divProb: Map<number, number>;
	winTotals: Map<number, { line: number; pOver: number }>;
};

// Small deterministic PRNG (mulberry32).
const mulberry32 = (seed: number) => {
	let s = seed | 0;
	return () => {
		s = (s + 0x6d2b79f5) | 0;
		let t = Math.imul(s ^ (s >>> 15), 1 | s);
		t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
		return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
	};
};

// Standard normal via Box-Muller.
const normalSample = (rand: () => number): number => {
	const u = Math.max(1e-12, 1 - rand());
	const v = rand();
	return Math.sqrt(-2 * Math.log(u)) * Math.cos(2 * Math.PI * v);
};

const largestPowerOfTwoAtMost = (n: number): number =>
	2 ** Math.floor(Math.log2(Math.max(1, n)));

export const simulateFutures = ({
	teams,
	numGamesPlayoffSeries,
	iterations = 4000,
	seed = 1,
	ratingUncertainty = 3.5,
}: {
	teams: FuturesTeam[];
	// Best-of lengths per playoff round, first round first (e.g. [7,7,7,7]).
	numGamesPlayoffSeries: number[];
	iterations?: number;
	seed?: number;
	// How unsure the book is about each team's true strength, in points. Each
	// simulation jitters every rating by Normal(0, this) - real books never
	// treat strength as known exactly, which is why a solid 3rd-best team gets
	// genuine title equity (+2500, not 99-1) and no tail collapses to zero.
	ratingUncertainty?: number;
}): FuturesResult => {
	const rounds = Math.max(1, numGamesPlayoffSeries.length);
	const cids = [...new Set(teams.map((t) => t.cid))];
	const dids = [...new Set(teams.map((t) => t.did))];
	const perConfCap = Math.max(
		1,
		Math.round(2 ** rounds / Math.max(1, cids.length)),
	);

	const rand = mulberry32(seed);

	const titleCount = new Map<number, number>();
	const confCount = new Map<number, number>();
	const divCount = new Map<number, number>();
	const winsSamples = new Map<number, number[]>(
		teams.map((t) => [t.tid, []]),
	);
	const bump = (m: Map<number, number>, tid: number) =>
		m.set(tid, (m.get(tid) ?? 0) + 1);

	// The best-of for a series when `fieldLen` teams remain ANYWHERE in the
	// bracket path: a field of 2 in a conference is the conference finals
	// (second-to-last round overall), the finals themselves are the last entry.
	const bestOfForField = (fieldLen: number, isFinals: boolean): number => {
		const idx = isFinals
			? rounds - 1
			: Math.min(rounds - 1, Math.max(0, rounds - 1 - Math.log2(fieldLen)));
		return numGamesPlayoffSeries[idx] ?? 7;
	};

	type SimTeam = FuturesTeam & { simWins: number };

	// Play a seeded single-elimination-of-series bracket; better seed gets a
	// ~1 point home edge. Field must be a power of 2, sorted best-first.
	const runBracket = (field: SimTeam[], isFinals: boolean): SimTeam => {
		while (field.length > 1) {
			const bestOf = bestOfForField(field.length, isFinals);
			const next: SimTeam[] = [];
			for (let i = 0; i < field.length / 2; i++) {
				const a = field[i]!;
				const b = field[field.length - 1 - i]!;
				const pA = seriesWinProb(
					marginToWinProb(a.rating - b.rating + 1),
					bestOf,
				);
				next.push(rand() < pA ? a : b);
			}
			next.sort((x, y) => y.simWins - x.simWins);
			field = next;
		}
		return field[0]!;
	};

	for (let iter = 0; iter < iterations; iter++) {
		// 1. Draw each team's TRUE strength for this simulated world (the book's
		// rating is an estimate, not a fact), then simulate the rest of the
		// regular season (normal approximation of the binomial over remaining
		// games), with a tiny jitter for tie-breaks.
		const simTeams: SimTeam[] = teams.map((t) => {
			const simRating = t.rating + normalSample(rand) * ratingUncertainty;
			const p = marginToWinProb(simRating);
			let wins = t.won;
			if (t.gamesRemaining > 0) {
				const mean = t.gamesRemaining * p;
				const sd = Math.sqrt(
					Math.max(0.25, t.gamesRemaining * p * (1 - p)),
				);
				const extra = Math.round(mean + normalSample(rand) * sd);
				wins += Math.min(t.gamesRemaining, Math.max(0, extra));
			}
			winsSamples.get(t.tid)!.push(wins);
			return { ...t, rating: simRating, simWins: wins + rand() * 0.5 };
		});

		// 2. Division winners: best simulated record in each division.
		for (const did of dids) {
			let best: SimTeam | undefined;
			for (const t of simTeams) {
				if (t.did === did && (!best || t.simWins > best.simWins)) {
					best = t;
				}
			}
			if (best) {
				bump(divCount, best.tid);
			}
		}

		// 3. Conference playoffs.
		const confChamps: SimTeam[] = [];
		for (const cid of cids) {
			const confTeams = simTeams
				.filter((t) => t.cid === cid)
				.sort((a, b) => b.simWins - a.simWins);
			if (confTeams.length === 0) {
				continue;
			}
			const K = largestPowerOfTwoAtMost(
				Math.min(perConfCap, confTeams.length),
			);
			confChamps.push(runBracket(confTeams.slice(0, K), false));
		}
		for (const champ of confChamps) {
			bump(confCount, champ.tid);
		}

		// 4. Finals between the conference champions.
		if (confChamps.length > 0) {
			const field = [...confChamps].sort((a, b) => b.simWins - a.simWins);
			const fieldPow2 = field.slice(0, largestPowerOfTwoAtMost(field.length));
			bump(titleCount, runBracket(fieldPow2, true).tid);
		}
	}

	// Win totals: scan half-point lines around the median and take the one
	// closest to a coin flip, so the juice stays near-balanced (-110/-110 style)
	// instead of a lopsided +215/-265 market.
	const winTotals = new Map<number, { line: number; pOver: number }>();
	for (const t of teams) {
		const samples = winsSamples.get(t.tid)!.sort((a, b) => a - b);
		const median = samples[Math.floor(samples.length / 2)] ?? t.won;
		const base = toHalfPointLine(median);
		let best = { line: base, pOver: 0, dist: Infinity };
		for (let offset = -3; offset <= 3; offset++) {
			const line = base + offset;
			const pOver =
				samples.filter((w) => w > line).length / Math.max(1, samples.length);
			const dist = Math.abs(pOver - 0.5);
			if (dist < best.dist) {
				best = { line, pOver, dist };
			}
		}
		winTotals.set(t.tid, { line: best.line, pOver: best.pOver });
	}

	const toProb = (m: Map<number, number>) => {
		const out = new Map<number, number>();
		for (const t of teams) {
			out.set(t.tid, (m.get(t.tid) ?? 0) / iterations);
		}
		return out;
	};

	return {
		titleProb: toProb(titleCount),
		confProb: toProb(confCount),
		divProb: toProb(divCount),
		winTotals,
	};
};
