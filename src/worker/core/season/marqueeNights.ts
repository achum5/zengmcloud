import { dayNumber } from "../../../common/calendarDates.ts";
import { distanceMiles, type Coords } from "./calendarTravel.ts";

// MARQUEE NIGHTS, as the NBA has them: opening night the champion's, its
// banner raised at home against one of the league's draws, with the other
// big matchups beside it; Christmas the five matchups the whole league
// wants to see. The games stay the same - only nights trade: a marquee game
// moves onto the night and one of the night's games takes its old night.
// A trade never puts a team in two games a night, adds three games in three
// nights or four in five, a back-to-back two time zones or 1,000 miles apart
// (as generateCalendar.ts plans them), or much travel; of those it can make,
// the one with the fewest new back-to-backs and miles.

export type DatedGame = { homeTid: number; awayTid: number; date: string };

export type MarqueeDraw = {
	// How much each team draws: its strength, hype and market, and last
	// season's finals (see marqueeDraw.ts). 0 to about 1.
	draw: Map<number, number>;
	// Last season's champion, who opens the season at home.
	champion?: number;
};

// New back-to-backs a trade may cost.
const MAX_NEW_B2BS = 2;
// Miles a trade may add, all its teams' together and any one team's, and
// what a back-to-back is worth in miles when choosing.
const MAX_NEW_MILES = 2500;
const MAX_NEW_TEAM_MILES = 1500;
const MILES_PER_B2B = 1500;
// What moving a game to another night than the one it traded with costs
// (in back-to-backs): two nights a game more and less than planned.
const RESIZE_COST = 0.5;
// How many of a night's first picks may move a game to any other night.
const REACH = 20;
// Back-to-backs the NBA doesn't play (generateCalendar.ts).
const FAR_B2B_MILES = 1000;
const FAR_ZONE_DEGREES = 18;

// How big a matchup is: the two teams' draws, the lesser counting again
// (two draws beat one great team and a dud).
const matchupScore = (draw: Map<number, number>, a: number, b: number) => {
	const da = draw.get(a) ?? 0;
	const db = draw.get(b) ?? 0;
	return da + db + 0.5 * Math.min(da, db);
};

// `locations`: where each team plays, for travel (teams off the map travel
// nowhere).
export const featureMarquee = <T extends DatedGame>(
	input: T[],
	{ draw, champion }: MarqueeDraw,
	locations: Map<number, Coords | undefined> = new Map(),
): T[] => {
	const games = input.map((game) => ({ ...game }));
	if (games.length === 0) {
		return games;
	}

	// Each team's nights, as day numbers, and where it plays them.
	const nights = new Map<number, Map<number, number>>();
	const add = (game: T) => {
		for (const tid of [game.homeTid, game.awayTid]) {
			let map = nights.get(tid);
			if (!map) {
				map = new Map();
				nights.set(tid, map);
			}
			map.set(dayNumber(game.date), game.homeTid);
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
	const milesBetween = (a: number, b: number) => {
		const la = locations.get(a);
		const lb = locations.get(b);
		return la && lb ? distanceMiles(la, lb) : 0;
	};
	const zonesApart = (a: number, b: number) => {
		const la = locations.get(a);
		const lb = locations.get(b);
		return (
			!!la && !!lb && Math.abs(la.longitude - lb.longitude) > FAR_ZONE_DEGREES
		);
	};
	// A team's season: its stretches of three games in three nights and of
	// four in five, its back-to-backs (and those far apart), and its miles,
	// from home to each night's arena in turn and home again.
	const load = (tid: number) => {
		const days = [...(nights.get(tid) ?? new Map<number, number>())].sort(
			(a, b) => a[0] - b[0],
		);
		const out = { three: 0, four: 0, b2b: 0, far: 0, zone: 0, miles: 0 };
		let at = tid;
		for (let i = 0; i < days.length; i++) {
			const [day, venue] = days[i]!;
			if (i + 2 < days.length && days[i + 2]![0] - day <= 2) {
				out.three += 1;
			}
			if (i + 3 < days.length && days[i + 3]![0] - day <= 4) {
				out.four += 1;
			}
			const hop = milesBetween(at, venue);
			out.miles += hop;
			if (i > 0 && day - days[i - 1]![0] === 1) {
				out.b2b += 1;
				if (hop > FAR_B2B_MILES) {
					out.far += 1;
				}
				if (zonesApart(at, venue)) {
					out.zone += 1;
				}
			}
			at = venue;
		}
		out.miles += milesBetween(at, tid);
		return out;
	};
	const loadOf = (tids: number[]) => tids.map((tid) => ({ tid, ...load(tid) }));
	// Plays each game on its night (returning what undoes it).
	const relocate = (moves: [T, string][]) => {
		const undo: [T, string][] = moves.map(([game]) => [game, game.date]);
		for (const [game] of moves) {
			remove(game);
		}
		for (const [game, date] of moves) {
			game.date = date;
		}
		for (const [game] of moves) {
			add(game);
		}
		return undo;
	};
	// What moving x onto y's night and y to `to` (x's own night, unless
	// given) costs - back-to-backs, and miles in back-to-backs' worth - or
	// undefined if it can't be done (a schedule already crowded may stay so,
	// no more).
	const moveCost = (x: T, y: T, to: string = x.date) => {
		const xTids = [x.homeTid, x.awayTid];
		const yTids = [y.homeTid, y.awayTid];
		if (
			xTids.some((tid) => !yTids.includes(tid) && plays(tid, y.date)) ||
			yTids.some(
				(tid) => (to !== x.date || !xTids.includes(tid)) && plays(tid, to),
			)
		) {
			return;
		}
		const tids = Array.from(new Set([...xTids, ...yTids]));
		const before = loadOf(tids);
		const undo = relocate([
			[x, y.date],
			[y, to],
		]);
		const after = loadOf(tids);
		relocate(undo);
		const sum = (list: typeof before, key: keyof (typeof before)[number]) =>
			list.reduce((total, l) => total + (l[key] as number), 0);
		const b2bs = sum(after, "b2b") - sum(before, "b2b");
		const miles = sum(after, "miles") - sum(before, "miles");
		if (
			(["three", "four", "far", "zone"] as const).some(
				(key) => sum(after, key) > sum(before, key),
			) ||
			b2bs > MAX_NEW_B2BS ||
			miles > MAX_NEW_MILES ||
			after.some((l, i) => l.miles - before[i]!.miles > MAX_NEW_TEAM_MILES)
		) {
			return;
		}
		return b2bs + miles / MILES_PER_B2B + (to === x.date ? 0 : RESIZE_COST);
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

	const dates = Array.from(new Set(games.map((game) => game.date))).sort();

	// Puts the picks on the night, as many as it has games, the first first.
	// A pick trades nights with one of the night's games; failing that, the
	// first few (`reach`) can send that game to some other night instead.
	// Returns the matchups the night now has.
	const feature = (
		date: string,
		picks: { key: string; games: T[] }[],
		reach: number,
	) => {
		const size = games.filter((game) => game.date === date).length;
		const featured = new Set<T>();
		const keys = new Set<string>();
		for (const [i, pick] of picks.entries()) {
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
			// (Opening night, Christmas and the last night keep their games.)
			const movable = pick.games.filter((game) => !fixed.has(game.date));
			let best: { x: T; y: T; to: string; cost: number } | undefined;
			const consider = (x: T, y: T, to: string) => {
				const cost = moveCost(x, y, to);
				if (cost !== undefined && (!best || cost < best.cost)) {
					best = { x, y, to, cost };
				}
			};
			for (const x of movable) {
				for (const y of tonight) {
					consider(x, y, x.date);
				}
			}
			if (!best && i < reach) {
				for (const x of movable) {
					for (const y of tonight) {
						for (const to of dates) {
							if (!fixed.has(to) && to !== x.date) {
								consider(x, y, to);
							}
						}
					}
				}
			}
			if (best) {
				const { x, y, to } = best;
				relocate([
					[x, y.date],
					[y, to],
				]);
				featured.add(x);
				keys.add(pick.key);
			}
		}
		return keys;
	};

	const opening = dates[0]!;
	const christmas = dates.find((date) => date.endsWith("-12-25"));
	const fixed = new Set([opening, christmas, dates.at(-1)]);

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
	// (Every one of the champion's home games may reach, and the first few
	// matchups.)
	const onOpening = feature(
		opening,
		[...ringNight, ...ranked],
		ringNight.length + REACH,
	);

	// Christmas: the biggest matchups not already seen on opening night.
	if (christmas !== undefined) {
		feature(
			christmas,
			ranked.filter((m) => !onOpening.has(m.key)),
			REACH,
		);
	}

	return games;
};
