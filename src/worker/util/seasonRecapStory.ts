// The story hooks a team-season recap is built from, worked out here rather than
// left to the AI. Handed a season's raw results, a model writes "they had an up
// and down year"; handed "won 11 straight in the middle third, 4-10 over the
// last fourteen", it writes the season that actually happened. Every one of
// these is a fact the AI would otherwise have to derive - and get wrong - from
// numbers it was never given.
//
// Pure functions over plain data, so they can be tested without a league.

// One regular-season game from a team's point of view, in the order played.
export type TeamGameResult = {
	won: boolean;
	tied?: boolean;
	pts: number;
	oppPts: number;
	opp: string;
};

export type SeasonShape = {
	// The season cut into three equal stretches, so a hot start, a mid-season
	// slump or a late push is stated instead of hidden inside the final record.
	stretches: { label: string; won: number; lost: number }[];
	longestWinStreak: number;
	longestLosingStreak: number;
	// Games decided by this many points or fewer, overtime included.
	close: { won: number; lost: number };
	biggestWin?: string;
	worstLoss?: string;
	lastTen?: { won: number; lost: number };
};

export const CLOSE_GAME_MARGIN = 5;

const record = (games: TeamGameResult[]) => ({
	won: games.filter((game) => game.won).length,
	lost: games.filter((game) => !game.won && !game.tied).length,
});

const score = (game: TeamGameResult) =>
	`${game.pts}-${game.oppPts} vs ${game.opp}`;

export const seasonShape = (
	games: TeamGameResult[],
): SeasonShape | undefined => {
	// Too few games and the thirds are noise, and a recap built on them would be
	// reading tea leaves.
	if (games.length < 9) {
		return undefined;
	}

	const stretches: SeasonShape["stretches"] = [];
	const cuts = [
		0,
		Math.round(games.length / 3),
		Math.round((games.length * 2) / 3),
		games.length,
	];
	for (let i = 0; i < 3; i++) {
		const chunk = games.slice(cuts[i], cuts[i + 1]);
		stretches.push({
			label: `games ${cuts[i]! + 1}-${cuts[i + 1]}`,
			...record(chunk),
		});
	}

	let longestWinStreak = 0;
	let longestLosingStreak = 0;
	let winRun = 0;
	let lossRun = 0;
	for (const game of games) {
		if (game.won) {
			winRun += 1;
			lossRun = 0;
		} else if (game.tied) {
			winRun = 0;
			lossRun = 0;
		} else {
			lossRun += 1;
			winRun = 0;
		}
		longestWinStreak = Math.max(longestWinStreak, winRun);
		longestLosingStreak = Math.max(longestLosingStreak, lossRun);
	}

	const close = record(
		games.filter(
			(game) => Math.abs(game.pts - game.oppPts) <= CLOSE_GAME_MARGIN,
		),
	);

	// Largest margin each way. Ties go to the earlier game, which is as good a
	// rule as any and keeps the output stable.
	let biggestWin: TeamGameResult | undefined;
	let worstLoss: TeamGameResult | undefined;
	for (const game of games) {
		const margin = game.pts - game.oppPts;
		if (
			game.won &&
			(!biggestWin || margin > biggestWin.pts - biggestWin.oppPts)
		) {
			biggestWin = game;
		}
		if (
			!game.won &&
			!game.tied &&
			(!worstLoss || margin < worstLoss.pts - worstLoss.oppPts)
		) {
			worstLoss = game;
		}
	}

	return {
		stretches,
		longestWinStreak,
		longestLosingStreak,
		close,
		biggestWin: biggestWin ? score(biggestWin) : undefined,
		worstLoss: worstLoss ? score(worstLoss) : undefined,
		lastTen: games.length >= 10 ? record(games.slice(-10)) : undefined,
	};
};

// A franchise's season-by-season playoff record, oldest first. playoffRoundsWon
// follows BBGM: -1 missed, 0 lost in the first round, ..., numRounds = title.
export type FranchiseSeasonResult = {
	season: number;
	playoffRoundsWon: number;
};

// The one line about where this season sits in the franchise's history that a
// writer would lead with if it applies: a drought ended, a streak extended, a
// first-ever title. Returns nothing when there is no streak worth naming, so the
// prompt never pads every team with a non-fact.
export const franchiseStreak = (
	history: FranchiseSeasonResult[],
	season: number,
	numPlayoffRounds: number,
): string[] => {
	const sorted = [...history]
		.filter((row) => row.season <= season)
		.sort((a, b) => a.season - b.season);
	const current = sorted.at(-1);
	if (!current || current.season !== season) {
		return [];
	}
	const before = sorted.slice(0, -1);
	const out: string[] = [];

	const made = (row: FranchiseSeasonResult) => row.playoffRoundsWon >= 0;
	const won = (row: FranchiseSeasonResult) =>
		row.playoffRoundsWon >= numPlayoffRounds;

	// Run of the same outcome (made / missed) ending with this season.
	let run = 1;
	for (let i = before.length - 1; i >= 0; i--) {
		if (made(before[i]!) !== made(current)) {
			break;
		}
		run += 1;
	}

	if (made(current)) {
		if (run >= 3) {
			out.push(`${ordinal(run)} straight playoff appearance`);
		} else if (run === 1 && before.length > 0) {
			const last = before.findLast(made);
			if (last) {
				const gap = season - last.season;
				if (gap >= 3) {
					out.push(`first playoff appearance since ${last.season}`);
				}
			} else if (before.length >= 2) {
				out.push("first playoff appearance in franchise history");
			}
		}
	} else if (run >= 3) {
		out.push(`missed the playoffs for the ${ordinal(run)} straight season`);
	}

	if (won(current)) {
		const lastTitle = before.findLast(won);
		if (!lastTitle) {
			out.push("first championship in franchise history");
		} else if (season - lastTitle.season >= 5) {
			out.push(`first championship since ${lastTitle.season}`);
		} else if (season - lastTitle.season === 1) {
			// Back to back (or more).
			let titles = 1;
			for (let i = before.length - 1; i >= 0 && won(before[i]!); i--) {
				titles += 1;
			}
			out.push(`${ordinal(titles)} straight championship`);
		}
	}

	return out;
};

export const ordinal = (n: number) => {
	const rem100 = n % 100;
	if (rem100 >= 11 && rem100 <= 13) {
		return `${n}th`;
	}
	return `${n}${["th", "st", "nd", "rd"][n % 10] ?? "th"}`;
};

// 1-based rank of each entry by value, best first. `higherIsBetter` false is for
// things like points allowed. Equal values share a rank.
export const rankBy = <T>(
	items: T[],
	value: (item: T) => number | undefined,
	higherIsBetter = true,
): Map<T, number> => {
	const scored = items
		.map((item) => ({ item, v: value(item) }))
		.filter((x): x is { item: T; v: number } => typeof x.v === "number");
	scored.sort((a, b) => (higherIsBetter ? b.v - a.v : a.v - b.v));
	const out = new Map<T, number>();
	let rank = 0;
	let prev: number | undefined;
	for (const [i, x] of scored.entries()) {
		if (prev === undefined || x.v !== prev) {
			rank = i + 1;
			prev = x.v;
		}
		out.set(x.item, rank);
	}
	return out;
};
