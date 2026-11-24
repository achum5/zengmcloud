import { dayNumber } from "../../../common/calendarDates.ts";

// MARQUEE NIGHTS, as the NBA has them: opening night the champion's, its
// banner raised at home against one of the league's draws, with the other
// big matchups beside it; Christmas the five matchups the whole league
// wants to see. The games stay the same - only nights trade: a marquee game
// moves onto the night and one of the night's games takes its old night,
// never into a team's second game of a night, three games in three nights
// or four in five, and with as few new back-to-backs as can be.

export type DatedGame = { homeTid: number; awayTid: number; date: string };

export type MarqueeDraw = {
	// How much each team draws: its strength, hype and market, and last
	// season's finals (see marqueeDraw.ts). 0 to about 1.
	draw: Map<number, number>;
	// Last season's champion, who opens the season at home.
	champion?: number;
};

// New back-to-backs a move may cost.
const MAX_NEW_B2BS = 2;

// How big a matchup is: the two teams' draws, the lesser counting again
// (two draws beat one great team and a dud).
const matchupScore = (draw: Map<number, number>, a: number, b: number) => {
	const da = draw.get(a) ?? 0;
	const db = draw.get(b) ?? 0;
	return da + db + 0.5 * Math.min(da, db);
};

export const featureMarquee = <T extends DatedGame>(
	input: T[],
	{ draw, champion }: MarqueeDraw,
): T[] => {
	const games = input.map((game) => ({ ...game }));
	if (games.length === 0) {
		return games;
	}

	// Each team's nights, as day numbers.
	const nights = new Map<number, Set<number>>();
	const add = (game: T) => {
		for (const tid of [game.homeTid, game.awayTid]) {
			let set = nights.get(tid);
			if (!set) {
				set = new Set();
				nights.set(tid, set);
			}
			set.add(dayNumber(game.date));
		}
	};
	const remove = (game: T) => {
		for (const tid of [game.homeTid, game.awayTid]) {
			nights.get(tid)?.delete(dayNumber(game.date));
		}
	};
	for (const game of games) {
		add(game);
	}
	const plays = (tid: number, date: string) =>
		nights.get(tid)?.has(dayNumber(date)) ?? false;
	const sorted = (tid: number) =>
		[...(nights.get(tid) ?? [])].sort((a, b) => a - b);
	// A team's stretches of three games in three nights or four in five, and
	// its back-to-backs.
	const load = (tid: number) => {
		const days = sorted(tid);
		let crowded = 0;
		let b2bs = 0;
		for (let i = 0; i < days.length; i++) {
			if (
				(i + 2 < days.length && days[i + 2]! - days[i]! <= 2) ||
				(i + 3 < days.length && days[i + 3]! - days[i]! <= 4)
			) {
				crowded += 1;
			}
			if (i > 0 && days[i]! - days[i - 1]! === 1) {
				b2bs += 1;
			}
		}
		return { crowded, b2bs };
	};
	const loadOf = (tids: number[]) => {
		let crowded = 0;
		let b2bs = 0;
		for (const tid of tids) {
			const l = load(tid);
			crowded += l.crowded;
			b2bs += l.b2bs;
		}
		return { crowded, b2bs };
	};
	const trade = (x: T, y: T) => {
		remove(x);
		remove(y);
		[x.date, y.date] = [y.date, x.date];
		add(x);
		add(y);
	};
	// What trading x's and y's nights costs in back-to-backs, or undefined
	// if it can't be done (a schedule already crowded may stay so, no more).
	const tradeCost = (x: T, y: T) => {
		const xTids = [x.homeTid, x.awayTid];
		const yTids = [y.homeTid, y.awayTid];
		if (
			xTids.some((tid) => !yTids.includes(tid) && plays(tid, y.date)) ||
			yTids.some((tid) => !xTids.includes(tid) && plays(tid, x.date))
		) {
			return;
		}
		const tids = Array.from(new Set([...xTids, ...yTids]));
		const before = loadOf(tids);
		trade(x, y);
		const after = loadOf(tids);
		trade(x, y);
		return after.crowded > before.crowded
			? undefined
			: after.b2bs - before.b2bs;
	};

	// Each matchup's games, the bigger draw's home games first.
	const matchups = Map.groupBy(games, (game) =>
		[game.homeTid, game.awayTid].sort((a, b) => a - b).join("-"),
	);
	const ranked = [...matchups.entries()]
		.map(([key, list]) => ({
			key,
			score: matchupScore(draw, list[0]!.homeTid, list[0]!.awayTid),
			games: [...list].sort(
				(a, b) => (draw.get(b.homeTid) ?? 0) - (draw.get(a.homeTid) ?? 0),
			),
		}))
		.sort((a, b) => b.score - a.score || (a.key < b.key ? -1 : 1));

	// Puts the picks on the night, as many as it has games, the first first.
	// Returns the matchups it now has.
	const feature = (date: string, picks: { key: string; games: T[] }[]) => {
		const size = games.filter((game) => game.date === date).length;
		const featured = new Set<T>();
		const keys = new Set<string>();
		for (const pick of picks) {
			if (featured.size >= size) {
				break;
			}
			if (keys.has(pick.key)) {
				continue;
			}
			const there = pick.games.find((game) => game.date === date);
			if (there) {
				featured.add(there);
				keys.add(pick.key);
				continue;
			}
			const tonight = games.filter(
				(game) => game.date === date && !featured.has(game),
			);
			let best: { x: T; y: T; cost: number } | undefined;
			for (const x of pick.games) {
				for (const y of tonight) {
					const cost = tradeCost(x, y);
					if (
						cost !== undefined &&
						cost <= MAX_NEW_B2BS &&
						(!best || cost < best.cost)
					) {
						best = { x, y, cost };
					}
				}
			}
			if (best) {
				trade(best.x, best.y);
				featured.add(best.x);
				keys.add(pick.key);
			}
		}
		return keys;
	};

	const dates = Array.from(new Set(games.map((game) => game.date))).sort();
	const opening = dates[0]!;

	// Opening night: the champion at home, against the biggest draw it can
	// have; then the biggest matchups.
	const ringNight =
		champion === undefined
			? []
			: ranked
					.flatMap((m) =>
						m.games
							.filter((game) => game.homeTid === champion)
							.map((game) => ({ key: m.key, games: [game] })),
					)
					.sort(
						(a, b) =>
							(draw.get(b.games[0]!.awayTid) ?? 0) -
							(draw.get(a.games[0]!.awayTid) ?? 0),
					);
	const onOpening = feature(opening, [...ringNight, ...ranked]);

	// Christmas: the biggest matchups not already seen on opening night.
	const christmas = dates.find((date) => date.endsWith("-12-25"));
	if (christmas !== undefined) {
		feature(
			christmas,
			ranked.filter((m) => !onOpening.has(m.key)),
		);
	}

	return games;
};
