import {
	betterSeedHome,
	MARGIN_SIGMA,
	marginToWinProb,
	mulberry32,
	normalSample,
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

// One remaining regular-season game, so the season sim prices each team's
// ACTUAL schedule - who it still plays and where - instead of a hypothetical
// slate of league-average opponents.
export type FuturesScheduleGame = {
	homeTid: number;
	awayTid: number;
};

// Play out the rest of a best-of series game by game. `better` is the seed
// holding home court: it hosts exactly the games betterSeedHome says (the
// engine's own scheduling rule), each worth +/-hcaPoints of margin. Works from
// any current score, so a series in progress prices off where it actually
// stands.
const simSeriesGames = ({
	rand,
	betterRating,
	otherRating,
	bestOf,
	hcaPoints,
	sigma,
	betterWon = 0,
	otherWon = 0,
}: {
	rand: () => number;
	betterRating: number;
	otherRating: number;
	bestOf: number;
	hcaPoints: number;
	sigma: number;
	betterWon?: number;
	otherWon?: number;
}): boolean => {
	const winsNeeded = Math.ceil(bestOf / 2);
	let b = betterWon;
	let o = otherWon;
	while (b < winsNeeded && o < winsNeeded) {
		// Same rule as the real schedule: the next game's number is the games
		// already played.
		const hca = betterSeedHome(bestOf, b + o) ? hcaPoints : -hcaPoints;
		const pBetter = marginToWinProb(betterRating - otherRating + hca, sigma);
		if (rand() < pBetter) {
			b += 1;
		} else {
			o += 1;
		}
	}
	return b >= winsNeeded;
};

export type FuturesResult = {
	titleProb: Map<number, number>;
	confProb: Map<number, number>;
	divProb: Map<number, number>;
	winTotals: Map<
		number,
		{
			line: number;
			pOver: number;
			// Spread of the simulated final-wins distribution, and the mean
			// dP(win a game)/d(rating point) over the team's slate - what the
			// pricing layer needs to charge for its own rating uncertainty (see
			// getLines' win-total load).
			winsSd: number;
			slope: number;
		}
	>;
};

const largestPowerOfTwoAtMost = (n: number): number =>
	2 ** Math.floor(Math.log2(Math.max(1, n)));

export const simulateFutures = ({
	teams,
	numGamesPlayoffSeries,
	iterations = 4000,
	seed = 1,
	ratingUncertainty = 3.5,
	schedule,
	hcaPoints = 0,
	sigma = MARGIN_SIGMA,
	playoffsNeutral = false,
	finalsNeutral = false,
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
	// The remaining regular-season games. When present, each team's per-game win
	// probability comes from its actual slate - opponents' ratings and the
	// home/away split - instead of a league-average opponent. Omitted (or a team
	// absent from it): balanced schedule vs the average of the OTHER teams.
	schedule?: FuturesScheduleGame[];
	// Home team's expected margin bump, in points (engine-calibrated,
	// length-scaled by the caller). Drives both the schedule weighting above and
	// the playoff series' game-by-game home pattern.
	hcaPoints?: number;
	// Per-game margin sigma (length-scaled by the caller).
	sigma?: number;
	// League set to neutral-site playoffs: no home edge in any playoff series
	// (regular-season HCA still applies).
	playoffsNeutral?: boolean;
	// League set to a neutral-site finals: no home edge in the final series.
	finalsNeutral?: boolean;
}): FuturesResult => {
	const rounds = Math.max(1, numGamesPlayoffSeries.length);
	const cids = [...new Set(teams.map((t) => t.cid))];
	const dids = [...new Set(teams.map((t) => t.did))];
	const perConfCap = Math.max(
		1,
		Math.round(2 ** rounds / Math.max(1, cids.length)),
	);

	const rand = mulberry32(seed);

	// Each team's remaining slate, aggregated EXACTLY: the mean per-game win
	// probability over its actual games (each with its opponent's rating and
	// home/away HCA), not the win probability of the mean margin - Phi is
	// concave above a half, so collapsing a varied slate to its mean margin
	// quietly overpaid every strong team about a win. `slope` is the mean
	// dP/d(rating), so per-iteration jitter moves the probability to first
	// order without re-walking the schedule. (Opponents' jitters average out
	// over a slate - noise an order of magnitude below the team's own.)
	const normalPdf = (z: number) =>
		Math.exp(-0.5 * z * z) / Math.sqrt(2 * Math.PI);
	const ratingByTid = new Map(teams.map((t) => [t.tid, t.rating]));
	const ratingSum = teams.reduce((s, t) => s + t.rating, 0);
	const scheduleStats = new Map<
		number,
		{ n: number; pSum: number; slopeSum: number }
	>();
	const addGame = (tid: number, margin: number) => {
		let s = scheduleStats.get(tid);
		if (!s) {
			s = { n: 0, pSum: 0, slopeSum: 0 };
			scheduleStats.set(tid, s);
		}
		s.n += 1;
		s.pSum += marginToWinProb(margin, sigma);
		s.slopeSum += normalPdf(margin / sigma) / sigma;
	};
	if (schedule) {
		for (const game of schedule) {
			const home = ratingByTid.get(game.homeTid);
			const away = ratingByTid.get(game.awayTid);
			if (home === undefined || away === undefined) {
				continue;
			}
			addGame(game.homeTid, home - away + hcaPoints);
			addGame(game.awayTid, away - home - hcaPoints);
		}
	}
	// Per team: mean per-game win probability over the slate, and its
	// sensitivity to the team's own rating.
	const baseP = new Map<number, { p: number; slope: number }>();
	for (const t of teams) {
		const s = scheduleStats.get(t.tid);
		if (s && s.n > 0) {
			baseP.set(t.tid, { p: s.pSum / s.n, slope: s.slopeSum / s.n });
		} else {
			// No schedule info: balanced slate against the average of everyone else
			// (excluding yourself - the league mean includes you, which quietly
			// shaved a top team's edge).
			const others = teams.length > 1 ? teams.length - 1 : 1;
			const meanMargin = t.rating - (ratingSum - t.rating) / others;
			baseP.set(t.tid, {
				p: marginToWinProb(meanMargin, sigma),
				slope: normalPdf(meanMargin / sigma) / sigma,
			});
		}
	}

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

	// Play a seeded single-elimination-of-series bracket, each series game by
	// game with the real home pattern (the better seed - here, the better
	// simulated record - holds home court). Field must be a power of 2, sorted
	// best-first.
	const runBracket = (field: SimTeam[], isFinals: boolean): SimTeam => {
		while (field.length > 1) {
			const bestOf = bestOfForField(field.length, isFinals);
			const seriesHca =
				playoffsNeutral || (isFinals && field.length === 2 && finalsNeutral)
					? 0
					: hcaPoints;
			const next: SimTeam[] = [];
			for (let i = 0; i < field.length / 2; i++) {
				const a = field[i]!;
				const b = field[field.length - 1 - i]!;
				const aWins = simSeriesGames({
					rand,
					betterRating: a.rating,
					otherRating: b.rating,
					bestOf,
					hcaPoints: seriesHca,
					sigma,
				});
				next.push(aWins ? a : b);
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
		//
		// No pace clamp here, on purpose. The old [0.15, 0.85] backstop encoded
		// "even the 73-9 Warriors only won 89%" - true of the NBA, false of the
		// engine, which happily lets a +20 roster win 94% of its games. Capping
		// the book at 0.85 while the engine kept winning made every juggernaut
		// win-total Over free money. marginToWinProb's own [0.005, 0.995] clamp
		// still applies.
		const simTeams: SimTeam[] = teams.map((t) => {
			const jitter = normalSample(rand) * ratingUncertainty;
			const simRating = t.rating + jitter;
			const base = baseP.get(t.tid)!;
			const p = Math.min(0.995, Math.max(0.005, base.p + base.slope * jitter));
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
	const winTotals = new Map<
		number,
		{ line: number; pOver: number; winsSd: number; slope: number }
	>();
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
		const mean =
			samples.reduce((a, b) => a + b, 0) / Math.max(1, samples.length);
		const winsSd = Math.sqrt(
			samples.reduce((s, w) => s + (w - mean) ** 2, 0) /
				Math.max(1, samples.length),
		);
		winTotals.set(t.tid, {
			line: best.line,
			pOver: best.pOver,
			winsSd,
			slope: baseP.get(t.tid)!.slope,
		});
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
	// By the playoffs a whole season has priced every team; the book's rating
	// error is around a point, not the 3.5 this once defaulted to - which
	// flattened a genuine 26% title favorite to 14% and made it free money.
	ratingUncertainty = 1,
	hcaPoints = 0,
	sigma = MARGIN_SIGMA,
	finalsNeutral = false,
	seedOrder,
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
	// Home team's expected margin bump in points (engine-calibrated,
	// length-scaled; 0 when the league plays neutral-site playoffs).
	hcaPoints?: number;
	// Per-game margin sigma (length-scaled by the caller).
	sigma?: number;
	// League set to a neutral-site finals: no home edge in the final series.
	finalsNeutral?: boolean;
	// Regular-season finish order (lower = better record), so home court in
	// SIMULATED later rounds goes to the better record - the engine's actual
	// rule - instead of whoever sat higher in the bracket. Omitted: bracket
	// position decides, the old approximation.
	seedOrder?: Map<number, number>;
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

	// Win the rest of a best-of series from the current score. The matchup's
	// home side (the better seed) hosts exactly the games the engine's schedule
	// gives it - see simSeriesGames.
	const simSeries = (
		homeRating: number,
		awayRating: number,
		bestOf: number,
		homeWon: number,
		awayWon: number,
		seriesHca: number,
	): boolean =>
		simSeriesGames({
			rand,
			betterRating: homeRating,
			otherRating: awayRating,
			bestOf,
			hcaPoints: seriesHca,
			sigma,
			betterWon: homeWon,
			otherWon: awayWon,
		});

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
			const seriesHca = field.length === 1 && finalsNeutral ? 0 : hcaPoints;
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
					seriesHca,
				);
				winners.push(homeWins ? m.home : m.away);
			}
			if (winners.length === 1) {
				bump(titleCount, winners[0]!.tid);
				break;
			}
			// Pair sequential winners for the next round; an odd leftover gets a
			// bye. Home court by regular-season finish when known.
			const next: BracketMatchup[] = [];
			for (let i = 0; i + 1 < winners.length; i += 2) {
				const a = winners[i]!;
				const b = winners[i + 1]!;
				const aHome =
					seedOrder === undefined ||
					(seedOrder.get(a.tid) ?? Infinity) <=
						(seedOrder.get(b.tid) ?? Infinity);
				next.push(
					aHome
						? { home: { ...a, won: 0 }, away: { ...b, won: 0 } }
						: { home: { ...b, won: 0 }, away: { ...a, won: 0 } },
				);
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
