import {
	dayNumber as toDayNumber,
	weekdayOf,
} from "../../../common/calendarDates.ts";
import {
	nationalGamesOn,
	slateOf,
} from "../../../common/nationalBroadcasts.ts";
import { distanceMiles, type Coords } from "./calendarTravel.ts";
import { pairKey } from "./rivalries.ts";

// MARQUEE NIGHTS, as the NBA has them: opening night the champion's, its
// banner raised at home against one of the league's draws, with the other
// big matchups beside it; Christmas the five matchups the whole league
// wants to see; Rivals Week's national games the league's rivalries
// (rivalries.ts). The games stay the same - only nights trade: a marquee
// game moves onto the night and one of the night's games takes its old
// night (and for the champion's opener, if it must, a few games around it
// move too, to keep everyone as rested as before).
// A trade never puts a team in two games a night, adds three games in three
// nights or four in five, a back-to-back two time zones or 1,000 miles apart
// (as generateCalendar.ts plans them), or much travel; of those it can make,
// the one with the fewest new back-to-backs and miles. In Rivals Week, no
// team at all gets a back-to-back more, or any of those, or much travel.

export type DatedGame = { homeTid: number; awayTid: number; date: string };

export type MarqueeDraw = {
	// How much each team draws: its strength, hype and market, and last
	// season's finals (see marqueeDraw.ts). 0 to about 1.
	draw: Map<number, number>;
	// Last season's champion, who opens the season at home.
	champion?: number;
	// For Rivals Week: the user teams and each matchup's playoff history
	// (rivalries.ts), and from them the league's rivalries, the biggest first.
	users?: number[];
	playoffs?: Map<string, number>;
	rivals?: [number, number][];
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
// Making room for the champion's opener: how many more games may move, how
// far (in nights), and how many tries it gets, an opponent and in all. Its
// opponent may fly in from farther away, the way a real opener's does: the
// miles and back-to-backs it may add, the teams' together and any one's.
const ROOM_MOVES = 3;
// A move that leaves nobody crowded, costing no more than this (in
// back-to-backs), ends the search.
const GOOD_ENOUGH = 1;
const ROOM_NIGHTS = 7;
const ROOM_TRIES_EACH = 4000;
const ROOM_TRIES = 60_000;
const RING_NEW_B2BS = 3;
const RING_NEW_MILES = 4500;
const RING_NEW_TEAM_MILES = 3500;
// Rivals Week: how many of its matchups a team may play, and the miles a
// trade may add a team.
const RIVALS_A_TEAM = 2;
const RIVALS_TEAM_MILES = 1000;
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
// nowhere). Returns the games on their nights, and the rivalries Rivals Week
// got.
export const featureMarqueeNights = <T extends DatedGame>(
	input: T[],
	{ draw, champion, rivals = [] }: MarqueeDraw,
	locations: Map<number, Coords | undefined> = new Map(),
): { games: T[]; rivals: [number, number][] } => {
	const games = input.map((game) => ({ ...game }));
	if (games.length === 0) {
		return { games, rivals: [] };
	}
	const dayNumbers = new Map<string, number>();
	const dayNumber = (date: string) => {
		let day = dayNumbers.get(date);
		if (day === undefined) {
			day = toDayNumber(date);
			dayNumbers.set(date, day);
		}
		return day;
	};

	// Each team's nights, as day numbers, and the game it plays each (and
	// its load, below, while they stay the same).
	const nights = new Map<number, Map<number, T>>();
	// (The same nights in order.)
	const ordered = new Map<number, number[]>();
	const loads = new Map<number, Load>();
	const add = (game: T) => {
		const day = dayNumber(game.date);
		for (const tid of [game.homeTid, game.awayTid]) {
			loads.delete(tid);
			let map = nights.get(tid);
			let list = ordered.get(tid);
			if (!map || !list) {
				map = new Map();
				list = [];
				nights.set(tid, map);
				ordered.set(tid, list);
			}
			if (!map.has(day)) {
				let i = list.length;
				while (i > 0 && list[i - 1]! > day) {
					i -= 1;
				}
				list.splice(i, 0, day);
			}
			map.set(day, game);
		}
	};
	const remove = (game: T) => {
		const day = dayNumber(game.date);
		for (const tid of [game.homeTid, game.awayTid]) {
			loads.delete(tid);
			const map = nights.get(tid);
			if (map?.get(day) === game) {
				map.delete(day);
				const list = ordered.get(tid)!;
				list.splice(list.indexOf(day), 1);
			}
		}
	};
	for (const game of games) {
		add(game);
	}
	const miles = new Map<number, number>();
	const milesBetween = (a: number, b: number) => {
		const key = a * 10_000 + b;
		let out = miles.get(key);
		if (out === undefined) {
			const la = locations.get(a);
			const lb = locations.get(b);
			out = la && lb ? distanceMiles(la, lb) : 0;
			miles.set(key, out);
		}
		return out;
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
	type Load = {
		three: number;
		four: number;
		b2b: number;
		far: number;
		zone: number;
		miles: number;
	};
	// A team's load over its nights (in order) and where each is played.
	const measure = (tid: number, days: number[], venues: number[]): Load => {
		const out = { three: 0, four: 0, b2b: 0, far: 0, zone: 0, miles: 0 };
		let at = tid;
		for (let i = 0; i < days.length; i++) {
			const day = days[i]!;
			const venue = venues[i]!;
			if (i + 2 < days.length && days[i + 2]! - day <= 2) {
				out.three += 1;
			}
			if (i + 3 < days.length && days[i + 3]! - day <= 4) {
				out.four += 1;
			}
			const hop = milesBetween(at, venue);
			out.miles += hop;
			if (i > 0 && day - days[i - 1]! === 1) {
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
	const load = (tid: number): Load => {
		const known = loads.get(tid);
		if (known) {
			return known;
		}
		const days = ordered.get(tid) ?? [];
		const map = nights.get(tid);
		const out = measure(
			tid,
			days,
			days.map((day) => map!.get(day)!.homeTid),
		);
		loads.set(tid, out);
		return out;
	};
	// A team's load were the games played on their new nights (without
	// moving them), or undefined if it would play two games a night.
	const loadAfter = (tid: number, moves: [T, string][]) => {
		const map = nights.get(tid);
		const mine = moves.filter(
			([game]) => game.homeTid === tid || game.awayTid === tid,
		);
		const entries: [number, number][] = [];
		for (const day of ordered.get(tid) ?? []) {
			const game = map!.get(day)!;
			if (!mine.some(([moving]) => moving === game)) {
				entries.push([day, game.homeTid]);
			}
		}
		for (const [game, date] of mine) {
			entries.push([dayNumber(date), game.homeTid]);
		}
		entries.sort((a, b) => a[0] - b[0]);
		for (let i = 1; i < entries.length; i++) {
			if (entries[i]![0] === entries[i - 1]![0]) {
				return;
			}
		}
		return measure(
			tid,
			entries.map(([day]) => day),
			entries.map(([, venue]) => venue),
		);
	};
	const loadOf = (tids: number[]) => tids.map((tid) => ({ tid, ...load(tid) }));
	const teamsOf = (list: [T, string][]) =>
		Array.from(new Set(list.flatMap(([game]) => [game.homeTid, game.awayTid])));
	// The teams' loads before and after playing the games on their new
	// nights (and then back as they were).
	const trial = (list: [T, string][]) => {
		const tids = teamsOf(list);
		const before = loadOf(tids);
		const undo = relocate(list);
		const after = loadOf(tids);
		relocate(undo);
		for (const l of before) {
			loads.set(l.tid, l);
		}
		return { tids, before, after };
	};
	// How much more crowded a team is than it was: stretches of three in
	// three and four in five, and far back-to-backs.
	const excess = (now: Load, was: Load) =>
		Math.max(0, now.three - was.three) +
		Math.max(0, now.four - was.four) +
		Math.max(0, now.far - was.far) +
		Math.max(0, now.zone - was.zone);
	// A team's games in its crowded stretches (and back-to-backs, for one
	// with far ones).
	const crowdedGames = (tid: number, b2bs: boolean) => {
		const days = ordered.get(tid) ?? [];
		const map = nights.get(tid);
		const out = new Set<T>();
		for (let i = 0; i < days.length; i++) {
			const span =
				i + 3 < days.length && days[i + 3]! - days[i]! <= 4
					? 4
					: i + 2 < days.length && days[i + 2]! - days[i]! <= 2
						? 3
						: b2bs && i + 1 < days.length && days[i + 1]! - days[i]! === 1
							? 2
							: 0;
			for (let j = i; j < i + span; j++) {
				out.add(map!.get(days[j]!)!);
			}
		}
		return out;
	};
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
	// Whether every team can play the games on their new nights: none in two
	// games a night.
	const free = (moves: [T, string][]) => {
		const moving = new Set(moves.map(([game]) => game));
		const taken = new Set<string>();
		for (const [game, date] of moves) {
			const day = dayNumber(date);
			for (const tid of [game.homeTid, game.awayTid]) {
				const there = nights.get(tid)?.get(day);
				if ((there && !moving.has(there)) || taken.has(`${tid} ${day}`)) {
					return false;
				}
				taken.add(`${tid} ${day}`);
			}
		}
		return true;
	};
	// What playing the games on their new nights costs - back-to-backs, and
	// miles in back-to-backs' worth - or undefined if it can't be done (a
	// schedule already crowded may stay so, no more).
	const movesCost = (moves: [T, string][]) => {
		if (!free(moves)) {
			return;
		}
		const { before, after } = trial(moves);
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
		return b2bs + miles / MILES_PER_B2B;
	};

	// Plays the games on their new nights, and then, while that leaves a
	// team more crowded than it was, moves one of its games in the crowded
	// stretch to a night nearby (trading with a game there, or not) - the
	// move that helps most, a few at most. Returns every game moved, with its
	// new night, and the cost, or undefined if nobody ends up as rested as
	// before (or it would cost too many back-to-backs or miles).
	let roomTries = 0;
	const settle = (moves: [T, string][], depth = ROOM_MOVES) => {
		if (!free(moves)) {
			return;
		}
		// Each team's load before, and how much more crowded it is now.
		const was = new Map<number, Load>();
		const extra = new Map<number, number>();
		const touch = (list: [T, string][]) => {
			for (const [game] of list) {
				for (const tid of [game.homeTid, game.awayTid]) {
					if (!was.has(tid)) {
						was.set(tid, load(tid));
						extra.set(tid, 0);
					}
				}
			}
		};
		const undos: [T, string][][] = [];
		const moved = new Set<T>();
		// The nights that changed: a stretch crowded now is near one.
		const changed = new Set<number>();
		const apply = (list: [T, string][]) => {
			touch(list);
			for (const [game, date] of list) {
				changed.add(dayNumber(game.date));
				changed.add(dayNumber(date));
			}
			undos.push(relocate(list));
			for (const [game] of list) {
				moved.add(game);
			}
			for (const tid of teamsOf(list)) {
				extra.set(tid, excess(load(tid), was.get(tid)!));
			}
		};
		const total = () => [...extra.values()].reduce((sum, e) => sum + e, 0);
		const undoAll = () => {
			for (const undo of undos.reverse()) {
				relocate(undo);
			}
		};
		apply(moves);
		// (Too far to fly already: no use making room.)
		if (
			[...was].some(
				([tid, start]) => load(tid).miles - start.miles > RING_NEW_TEAM_MILES,
			)
		) {
			undoAll();
			return;
		}
		for (let step = 0; step < depth && total() > 0; step++) {
			const byDay = Map.groupBy(
				games.filter((game) => !fixed.has(game.date)),
				(game) => dayNumber(game.date),
			);
			const now = total();
			let best: { list: [T, string][]; left: number; cost: number } | undefined;
			for (const [tid, e] of extra) {
				if (e === 0) {
					continue;
				}
				const was0 = was.get(tid)!;
				const now0 = load(tid);
				const far = now0.far > was0.far || now0.zone > was0.zone;
				for (const z of crowdedGames(tid, far)) {
					const day = dayNumber(z.date);
					if (
						fixed.has(z.date) ||
						![...changed].some((c) => Math.abs(c - day) <= 4)
					) {
						continue;
					}
					// (Trading with a game nearby, or on its own to any night,
					// the nearest first.)
					const options: [T, string][][] = [];
					for (const [d, there] of [...byDay].sort(
						(a, b) => Math.abs(a[0] - day) - Math.abs(b[0] - day),
					)) {
						if (d !== day) {
							options.push([[z, there[0]!.date]]);
							if (Math.abs(d - day) <= ROOM_NIGHTS) {
								for (const w of there) {
									options.push([
										[z, w.date],
										[w, z.date],
									]);
								}
							}
						}
					}
					for (const list of options) {
						// (Nobody crowded any more, and cheap: good enough.)
						if (best && best.left === 0 && best.cost <= GOOD_ENOUGH) {
							break;
						}
						if (!free(list)) {
							continue;
						}
						roomTries += 1;
						const { tids, before, after } = trial(list);
						const fresh = tids.filter((t) => !was.has(t));
						let left = now;
						let cost = 0;
						for (const [i, l] of after.entries()) {
							const start = fresh.includes(l.tid)
								? before[i]!
								: was.get(l.tid)!;
							left += excess(l, start) - (extra.get(l.tid) ?? 0);
							cost +=
								l.b2b -
								before[i]!.b2b +
								(l.miles - before[i]!.miles) / MILES_PER_B2B;
						}
						if (
							left < now &&
							(!best ||
								left < best.left ||
								(left === best.left && cost < best.cost))
						) {
							best = { list, left, cost };
						}
					}
				}
			}
			if (!best) {
				break;
			}
			apply(best.list);
		}
		const done = total() === 0;
		const tids = [...was.keys()];
		const after = loadOf(tids);
		const placed: [T, string][] = [...moved].map((game) => [game, game.date]);
		undoAll();
		if (!done) {
			return;
		}
		let b2bs = 0;
		let miles = 0;
		for (const l of after) {
			const start = was.get(l.tid)!;
			b2bs += l.b2b - start.b2b;
			miles += l.miles - start.miles;
			if (l.miles - start.miles > RING_NEW_TEAM_MILES) {
				return;
			}
		}
		if (b2bs > RING_NEW_B2BS || miles > RING_NEW_MILES) {
			return;
		}
		return { moves: placed, cost: b2bs + miles / MILES_PER_B2B };
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

	const opening = dates[0]!;
	const christmas = dates.find((date) => date.endsWith("-12-25"));
	// (Opening night, Christmas and the last night keep their games.)
	const fixed = new Set([opening, christmas, dates.at(-1)]);
	// Each night's featured games and matchups so far.
	const featuredOn = new Map<string, { games: Set<T>; keys: Set<string> }>();

	// Puts the picks on the night, as many as it has games, the first first.
	// A pick trades nights with one of the night's games; failing that, the
	// first few (`reach`) can send that game to some other night instead, and
	// with `repair` games around them may move too, to make room (see
	// settle). Returns the matchups the night now has.
	const feature = (
		date: string,
		picks: { key: string; games: T[] }[],
		{
			reach = 0,
			repair = false,
			rotate = false,
		}: { reach?: number; repair?: boolean; rotate?: boolean },
	) => {
		const size = games.filter((game) => game.date === date).length;
		let on = featuredOn.get(date);
		if (!on) {
			on = { games: new Set(), keys: new Set() };
			featuredOn.set(date, on);
		}
		const { games: featured, keys } = on;
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
			const movable = pick.games.filter((game) => !fixed.has(game.date));
			let best: { moves: [T, string][]; cost: number } | undefined;
			const consider = (moves: [T, string][], extra = 0) => {
				const cost = movesCost(moves);
				if (cost !== undefined && (!best || cost + extra < best.cost)) {
					best = { moves, cost: cost + extra };
				}
			};
			for (const x of movable) {
				for (const y of tonight) {
					consider([
						[x, date],
						[y, x.date],
					]);
				}
			}
			if (!best && i < reach) {
				for (const x of movable) {
					for (const y of tonight) {
						for (const to of dates) {
							if (!fixed.has(to) && to !== x.date) {
								consider(
									[
										[x, date],
										[y, to],
									],
									RESIZE_COST,
								);
							}
						}
					}
				}
			}
			// For the champion's opener: the pick onto the night, and one of
			// the night's games to one of the pick's teams' nights soon after,
			// that game to the pick's old night; or the night's game to the
			// pick's old night, one nearby or one early in the season.
			const ways = (x: T) => {
				const out: [T, string][][] = [];
				const soon = new Set<T>();
				for (const tid of [x.homeTid, x.awayTid]) {
					for (let d = 1; d <= ROOM_NIGHTS; d++) {
						const z = nights.get(tid)?.get(dayNumber(date) + d);
						if (z && z !== x && !fixed.has(z.date)) {
							soon.add(z);
						}
					}
				}
				for (const z of soon) {
					for (const y of tonight) {
						out.push([
							[x, date],
							[z, x.date],
							[y, z.date],
						]);
					}
				}
				const near = (from: string, n: number) =>
					dates.filter(
						(d) =>
							!fixed.has(d) && Math.abs(dayNumber(d) - dayNumber(from)) <= n,
					);
				for (const to of new Set([
					x.date,
					...near(x.date, 3),
					...near(date, ROOM_NIGHTS),
				])) {
					for (const y of tonight) {
						out.push([
							[x, date],
							[y, to],
						]);
					}
				}
				return out;
			};
			if (!best && (rotate || repair)) {
				const tries = roomTries;
				for (const x of movable) {
					for (const way of ways(x)) {
						if (
							best ||
							(repair &&
								(roomTries > ROOM_TRIES || roomTries - tries > ROOM_TRIES_EACH))
						) {
							break;
						}
						// (First every way as it is, then making room.)
						const settled = settle(way, repair ? ROOM_MOVES : 0);
						if (settled) {
							best = {
								// (The pick first, as featured.)
								moves: [
									[x, date],
									...settled.moves.filter(([game]) => game !== x),
								],
								cost: settled.cost,
							};
						}
					}
				}
			}
			if (best) {
				relocate(best.moves);
				featured.add(best.moves[0]![0]);
				keys.add(pick.key);
			}
		}
		return keys;
	};

	// Opening night: the champion at home, against the biggest draw it can
	// have; then the biggest matchups.
	if (champion !== undefined) {
		const ringNight = ranked
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
		const hosts = () =>
			games.some((game) => game.date === opening && game.homeTid === champion);
		feature(opening, ringNight, { reach: ringNight.length });
		if (!hosts()) {
			feature(opening, ringNight, { rotate: true });
		}
		if (!hosts()) {
			feature(opening, ringNight, { repair: true });
		}
	}
	const onOpening = feature(opening, ranked, { reach: REACH });

	// Christmas: the biggest matchups not already seen on opening night.
	if (christmas !== undefined) {
		feature(
			christmas,
			ranked.filter((m) => !onOpening.has(m.key)),
			{ reach: REACH },
		);
	}

	// RIVALS WEEK: the rivalries, the biggest first, each onto the week's
	// night that still has a national game to fill (nationalGamesOn) where it
	// costs least. One of their games moves onto the night, either with the
	// games chained to it (see chain), or with a game either team had that
	// night moving off it - to the game's old night, or any other - or if
	// neither had one, one of the night's other games taking the game's old
	// night (or none). No team gets a back-to-back more, or is more crowded,
	// or flies much farther; none plays more than RIVALS_A_TEAM of them, and
	// no matchup is there twice. A rivalry that can't be had that way isn't.
	const rivalsWeek = (pairs: [number, number][]) => {
		const out: [number, number][] = [];
		const week = dates.filter(
			(date) => slateOf(date, opening) === "rivalsWeek",
		);
		if (pairs.length === 0 || week.length === 0) {
			return out;
		}
		const room = new Map(
			week.map((date) => [
				date,
				nationalGamesOn(
					date,
					weekdayOf(date),
					opening,
					games.filter((game) => game.date === date).length,
				),
			]),
		);
		const featured = new Set<T>();
		const played = new Map<number, number>();
		const open = (date: string) => (room.get(date) ?? 0) > 0;
		const movableTo = dates.filter((date) => !fixed.has(date));
		// What playing the games on their new nights costs the teams
		// (`tids`, theirs if not given) - back-to-backs, and miles in
		// back-to-backs' worth - or undefined if one would play two games a
		// night, be more crowded, have a back-to-back more, or fly much
		// farther.
		const strictCost = (
			moves: [T, string][],
			tids: number[] = teamsOf(moves),
		) => {
			let cost = 0;
			for (const tid of tids) {
				const was = load(tid);
				const now = loadAfter(tid, moves);
				if (
					!now ||
					now.b2b > was.b2b ||
					now.three > was.three ||
					now.four > was.four ||
					now.far > was.far ||
					now.zone > was.zone ||
					now.miles - was.miles > RIVALS_TEAM_MILES
				) {
					return;
				}
				cost += now.b2b - was.b2b + (now.miles - was.miles) / MILES_PER_B2B;
			}
			return cost;
		};
		// The nights a game more or less than planned, in what that costs.
		const resizeCost = (moves: [T, string][]) => {
			const change = new Map<string, number>();
			for (const [game, date] of moves) {
				change.set(game.date, (change.get(game.date) ?? 0) - 1);
				change.set(date, (change.get(date) ?? 0) + 1);
			}
			let resized = 0;
			for (const n of change.values()) {
				resized += Math.abs(n);
			}
			return (RESIZE_COST * resized) / 2;
		};
		const busy = (tid: number, date: string) =>
			nights.get(tid)?.has(dayNumber(date)) ?? false;
		// A game and every game linked to it through a team playing on both
		// its night and `date` - those on its night onto `date`, those on
		// `date` onto its night - so every team in the chain plays the same
		// nights as before, but the two at its ends (if it has ends) trade
		// one for the other. Undefined if it takes a featured game.
		const chain = (x: T, date: string): [T, string][] | undefined => {
			const from = x.date;
			const days = [dayNumber(from), dayNumber(date)];
			const found = new Set<T>([x]);
			const stack = [x];
			while (stack.length > 0) {
				const game = stack.pop()!;
				const other = game.date === from ? days[1]! : days[0]!;
				for (const tid of [game.homeTid, game.awayTid]) {
					const next = nights.get(tid)?.get(other);
					if (next && !found.has(next)) {
						if (featured.has(next)) {
							return;
						}
						found.add(next);
						stack.push(next);
					}
				}
			}
			return [...found].map((game) => [game, game.date === from ? date : from]);
		};

		for (const [a, b] of pairs) {
			if (!week.some(open)) {
				break;
			}
			if (
				(played.get(a) ?? 0) >= RIVALS_A_TEAM ||
				(played.get(b) ?? 0) >= RIVALS_A_TEAM
			) {
				continue;
			}
			const theirs = matchups.get(pairKey(a, b));
			if (!theirs) {
				continue;
			}
			let best:
				| { date: string; game: T; moves: [T, string][]; cost: number }
				| undefined;
			// (One of their games on such a night already: nothing to move.)
			const there = theirs.find((game) => open(game.date));
			if (there) {
				best = { date: there.date, game: there, moves: [], cost: 0 };
			}
			for (const x of best ? [] : theirs) {
				if (fixed.has(x.date)) {
					continue;
				}
				for (const date of week.filter(open)) {
					const day = dayNumber(date);
					const onto: [T, string] = [x, date];
					const linked = chain(x, date);
					const linkedCost = linked && strictCost(linked);
					if (linked && linkedCost !== undefined) {
						const total = linkedCost + resizeCost(linked);
						if (!best || total < best.cost) {
							best = { date, game: x, moves: linked, cost: total };
						}
					}
					const clashes = [a, b].flatMap((tid) => {
						const game = nights.get(tid)?.get(day);
						return game ? [{ tid, game }] : [];
					});
					if (clashes.some(({ game }) => featured.has(game))) {
						continue;
					}
					// (A team without a clash first: it plays the night as it is.)
					const clear = [a, b].filter(
						(tid) => !clashes.some((clash) => clash.tid === tid),
					);
					if (clear.length > 0 && strictCost([onto], clear) === undefined) {
						continue;
					}
					// Where each clash goes, the cheapest for its two teams.
					const moves: [T, string][] = [onto];
					let failed = false;
					for (const { tid, game } of clashes) {
						const other = game.homeTid === tid ? game.awayTid : game.homeTid;
						let pick: { to: string; cost: number } | undefined;
						for (const to of [x.date, ...movableTo]) {
							if (
								to === date ||
								(to !== x.date && (busy(tid, to) || busy(other, to)))
							) {
								continue;
							}
							const cost = strictCost([onto, [game, to]], [tid, other]);
							if (cost === undefined) {
								continue;
							}
							const total = cost + (to === x.date ? 0 : RESIZE_COST);
							if (!pick || total < pick.cost) {
								pick = { to, cost: total };
							}
							if (to === x.date) {
								break;
							}
						}
						if (!pick) {
							failed = true;
							break;
						}
						moves.push([game, pick.to]);
					}
					if (failed) {
						continue;
					}
					// No clash: one of the night's games may take the game's old
					// night, to keep both their sizes.
					if (clashes.length === 0) {
						let swap: { y: T; cost: number } | undefined;
						for (const y of games) {
							if (
								y.date !== date ||
								featured.has(y) ||
								busy(y.homeTid, x.date) ||
								busy(y.awayTid, x.date)
							) {
								continue;
							}
							const cost = strictCost([[y, x.date]]);
							if (cost !== undefined && (!swap || cost < swap.cost)) {
								swap = { y, cost };
							}
						}
						if (swap) {
							moves.push([swap.y, x.date]);
						}
					}
					const cost = strictCost(moves);
					if (cost === undefined) {
						continue;
					}
					const total = cost + resizeCost(moves);
					if (!best || total < best.cost) {
						best = { date, game: x, moves, cost: total };
					}
				}
			}
			if (best) {
				relocate(best.moves);
				featured.add(best.game);
				room.set(best.date, room.get(best.date)! - 1);
				played.set(a, (played.get(a) ?? 0) + 1);
				played.set(b, (played.get(b) ?? 0) + 1);
				out.push([a, b]);
			}
		}
		return out;
	};

	return { games, rivals: rivalsWeek(rivals) };
};

export const featureMarquee = <T extends DatedGame>(
	input: T[],
	marquee: MarqueeDraw,
	locations?: Map<number, Coords | undefined>,
): T[] => featureMarqueeNights(input, marquee, locations).games;
