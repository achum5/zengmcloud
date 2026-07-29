import {
	marginToWinProb,
	mulberry32,
	normalSample,
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
	const winsSamples = new Map<number, number[]>(teams.map((t) => [t.tid, []]));
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
				const sd = Math.sqrt(Math.max(0.25, t.gamesRemaining * p * (1 - p)));
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
			const K = largestPowerOfTwoAtMost(Math.min(perConfCap, confTeams.length));
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

// --- In-playoffs futures: simulate the REAL bracket from its current state ---
//
// Once the playoffs start, the hypothetical simulateFutures bracket above is
// wrong in the worst way: it keeps seeding a fantasy round-1 tournament from
// regular-season records, so an already-ELIMINATED team with a great record
// stays the title favorite and a team up 3-0 in a series gets no credit. This
// simulator instead takes the actual bracket - who's still alive, each series'
// current wins - and Monte Carlos only what's left to play.

export type BracketTeam = {
	tid: number;
	cid: number;
	// Series wins so far in the CURRENT round's matchup.
	won: number;
};

export type BracketMatchup = {
	home: BracketTeam;
	// Missing away = a bye; home advances automatically.
	away?: BracketTeam;
};

export type BracketFuturesResult = {
	titleProb: Map<number, number>;
	// P(reach the final series) - the "wins the conference" market in a standard
	// two-conference bracket.
	confProb: Map<number, number>;
};

// Which futures markets a bracket still leaves open.
//
// A real book takes a market down the moment its outcome is knowable, and
// `confProb` here means "reaches the final series" - which is settled as soon
// as the final series exists. Without this the Conference Winner market stayed
// up through the Finals with both finalists priced at a certainty, so either
// one was a guaranteed payout on a publicly known result.
//
// Two ways a conference gets settled:
//   - The final series is set, so both participants have already reached it.
//   - Only one of that conference's teams is still alive in the round being
//     played. Conference finals rarely end on the same day, so this closes the
//     first one as it finishes rather than waiting for the round to turn over.
export const bracketMarketsOpen = ({
	matchups,
	bestOf,
}: {
	// The in-progress round's matchups.
	matchups: BracketMatchup[];
	// Games in that round's series.
	bestOf: number;
}): { conferenceCids: Set<number>; title: boolean } => {
	const winsNeeded = Math.ceil(bestOf / 2);

	// Who can still win their current series.
	const alive: BracketTeam[] = [];
	let anyUndecided = false;
	for (const m of matchups) {
		if (!m.away) {
			alive.push(m.home);
			continue;
		}
		if (m.home.won >= winsNeeded) {
			alive.push(m.home);
		} else if (m.away.won >= winsNeeded) {
			alive.push(m.away);
		} else {
			alive.push(m.home, m.away);
			anyUndecided = true;
		}
	}

	// One matchup left is the final series, so every conference's representative
	// is already known - whoever wins it, both got there.
	const conferenceCids = new Set<number>();
	if (matchups.length > 1) {
		const perConf = new Map<number, number>();
		for (const t of alive) {
			perConf.set(t.cid, (perConf.get(t.cid) ?? 0) + 1);
		}
		for (const [cid, count] of perConf) {
			if (count > 1) {
				conferenceCids.add(cid);
			}
		}
	}

	// The title is settled once the last series left has a winner.
	return { conferenceCids, title: anyUndecided || matchups.length > 1 };
};

export const simulatePlayoffBracket = ({
	matchups,
	startRound,
	numGamesPlayoffSeries,
	ratings,
	iterations = 4000,
	seed = 1,
	ratingUncertainty = 3.5,
}: {
	// The in-progress round's matchups, in bracket order (winners of matchups
	// 2i and 2i+1 meet next round - BBGM's fill order; with reseeding enabled the
	// real pairings can differ, an acceptable pricing approximation).
	matchups: BracketMatchup[];
	// Index into numGamesPlayoffSeries for the in-progress round.
	startRound: number;
	numGamesPlayoffSeries: number[];
	// Point margin vs an average team, per tid (same scale as FuturesTeam.rating).
	ratings: Map<number, number>;
	iterations?: number;
	seed?: number;
	ratingUncertainty?: number;
}): BracketFuturesResult => {
	const rand = mulberry32(seed);
	const titleCount = new Map<number, number>();
	const confCount = new Map<number, number>();
	const bump = (m: Map<number, number>, tid: number) =>
		m.set(tid, (m.get(tid) ?? 0) + 1);

	// Every tid in the bracket, so the result maps cover eliminated-from-here
	// teams with an explicit 0 only for participants; absent tids are simply not
	// in the market.
	const allTids: number[] = [];
	for (const m of matchups) {
		allTids.push(m.home.tid);
		if (m.away) {
			allTids.push(m.away.tid);
		}
	}

	// Win the rest of a best-of series from the current score. The matchup's home
	// side (the better seed) gets a flat ~1 point edge, matching the pre-playoffs
	// simulator's convention.
	const simSeries = (
		homeRating: number,
		awayRating: number,
		bestOf: number,
		homeWon: number,
		awayWon: number,
	): boolean => {
		const winsNeeded = Math.ceil(bestOf / 2);
		const pHome = marginToWinProb(homeRating - awayRating + 1);
		let h = homeWon;
		let a = awayWon;
		while (h < winsNeeded && a < winsNeeded) {
			if (rand() < pHome) {
				h += 1;
			} else {
				a += 1;
			}
		}
		return h >= winsNeeded;
	};

	for (let iter = 0; iter < iterations; iter++) {
		// The book's ratings are estimates; jitter each team's true strength once
		// per simulated world.
		const simRating = new Map<number, number>();
		for (const tid of allTids) {
			simRating.set(
				tid,
				(ratings.get(tid) ?? 0) + normalSample(rand) * ratingUncertainty,
			);
		}

		let field = matchups;
		let round = startRound;
		let finalists: BracketTeam[] = [];
		while (field.length > 0) {
			const bestOf = numGamesPlayoffSeries[round] ?? 7;
			if (field.length === 1) {
				const only = field[0]!;
				finalists = only.away ? [only.home, only.away] : [only.home];
			}
			const winners: BracketTeam[] = [];
			for (const m of field) {
				if (!m.away) {
					winners.push(m.home);
					continue;
				}
				const homeWins = simSeries(
					simRating.get(m.home.tid)!,
					simRating.get(m.away.tid)!,
					bestOf,
					m.home.won,
					m.away.won,
				);
				winners.push(homeWins ? m.home : m.away);
			}
			if (winners.length === 1) {
				bump(titleCount, winners[0]!.tid);
				break;
			}
			// Pair sequential winners for the next round; an odd leftover gets a bye.
			const next: BracketMatchup[] = [];
			for (let i = 0; i + 1 < winners.length; i += 2) {
				next.push({
					home: { ...winners[i]!, won: 0 },
					away: { ...winners[i + 1]!, won: 0 },
				});
			}
			if (winners.length % 2 === 1) {
				next.push({ home: { ...winners.at(-1)!, won: 0 } });
			}
			field = next;
			round += 1;
		}
		for (const t of finalists) {
			bump(confCount, t.tid);
		}
	}

	const toProb = (m: Map<number, number>) => {
		const out = new Map<number, number>();
		for (const tid of allTids) {
			out.set(tid, (m.get(tid) ?? 0) / iterations);
		}
		return out;
	};

	return {
		titleProb: toProb(titleCount),
		confProb: toProb(confCount),
	};
};
