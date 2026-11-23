import {
	addDays,
	dayNumber,
	makeDate,
	nthWeekday,
	seasonStartYear,
	weekdayOf,
} from "../../../common/calendarDates.ts";
import { distanceMatrix, type Coords } from "./calendarTravel.ts";

// AN NBA-STYLE CALENDAR FOR ANY LEAGUE. The matchups come from the usual
// generator untouched; this only decides which night each one is played on,
// so it changes nothing the sim decides. A night is a sim day.
//
// What it aims for, measured from the NBA's own seasons (2021-22 to 2023-24
// mostly; the study is "How the NBA schedule works"):
// - about 164 game nights for 82 games, opening on a Tuesday in late
//   October and ending on the second Sunday in April;
// - no games on Election Day, Thanksgiving, Christmas Eve or the night of the
//   NCAA final, five games on Christmas, six dark nights around the All-Star
//   Game (Friday to Wednesday), every team on the last night;
// - nights sized like the NBA's: Wednesday and Friday big, Tuesday and
//   Thursday small, and no two Wednesdays alike;
// - each team about 14 back-to-backs, never three games in three nights or
//   four in five, no back-to-back two time zones apart, and a quarter of games
//   with one team on a back-to-back and the other rested;
// - road trips that loop through neighboring cities, at most six games except
//   when a team's arena is booked (the NBA's rodeo and Grammys trips), and
//   about one two-game series (same teams, same arena, a night between) for
//   every team.
//
// It anneals: random moves (a game to another night, or two of a team's games
// trading nights), kept when they help and sometimes when they don't, less
// often as it goes.

export type CalendarTeam = {
	tid: number;
	// Where it plays (teamLocation.ts), if it's on the map.
	location?: Coords;
};

export type CalendarInput = {
	season: number;
	teams: CalendarTeam[];
	// [homeTid, awayTid], regular games only.
	games: [number, number][];
	// Where in the season the All-Star Game falls, 0 to 1, or undefined for
	// no All-Star break.
	allStarFraction: number | undefined;
	random?: () => number;
	// Stop improving after this long, whatever the iteration count.
	timeBudgetMs?: number;
	maxIterations?: number;
	// For tests: check at the end that the costs kept move by move match a
	// recount.
	checkCosts?: boolean;
};

export type CalendarOutput = {
	// Parallel to input.games.
	dates: string[];
	allStarDate: string | undefined;
};

// Games on a night, by weekday from Sunday: the NBA's 10th, 25th, 50th, 75th
// and 90th percentiles, 2021-22 to 2023-24.
const NIGHT_SIZES = [
	[4, 6, 8, 9, 11],
	[5, 6, 8, 10, 11],
	[3, 4, 5, 7, 10],
	[7, 8, 10, 12, 13],
	[2, 3, 5, 6, 8],
	[7, 8, 10, 11, 12],
	[4, 6, 7, 8, 10],
];
const QUANTILES = [0, 0.1, 0.25, 0.5, 0.75, 0.9, 1];
const NIGHT_SPREAD = 2;

const NIGHTS_PER_GAME = 2;
// All-Star break: dark from the Friday before the game to the Wednesday after.
const BREAK_FROM = -2;
const BREAK_TO = 3;
const MAX_TRIP = 6;
const MAX_BOOKED_TRIP = 9;
const MAX_HOMESTAND = 7;
// About 15 degrees of longitude to a time zone; the NBA's back-to-backs stay
// within 17.
const FAR_ZONE_DEGREES = 18;

const W_B2B = 300;
const W_FAR_B2B = 900;
const FAR_B2B_MILES = 1000;
const W_ZONE_B2B = 20_000;
const W_THREE_IN_THREE = 60_000;
const W_FOUR_IN_FIVE = 4000;
const W_FIVE_IN_SEVEN = 150;
const W_STINT = 2500;
const W_GAP = 500;
const W_NIGHT = 120;
const W_SPECIAL_NIGHT = 4000;
const W_BOOKED = 6000;
const W_SERIES = 1200;
// Two in a row in the same arena that weren't planned as a series: the NBA
// had none before 2020 and plans the ones it has now.
const W_UNPLANNED_SERIES = 600;
// Long road trips and homestands save miles, but the NBA's are mostly short:
// more than half its trips are one or two games, about one in eight is five
// or more.
const W_LONG_STINT = 250;
const LONG_STINT = 4;
const W_REST_EDGE = 35;
// And each team's rest edges, for less against, within 5 either way.
const W_REST_BALANCE = 12;

const secondSundayOfApril = (year: number) => nthWeekday(year, 4, 0, 2);

const previousWeekday = (date: string, weekday: number) =>
	addDays(date, -((weekdayOf(date) - weekday + 7) % 7));

const nextWeekday = (date: string, weekday: number) =>
	addDays(date, (weekday - weekdayOf(date) + 7) % 7);

// A number in [0, 1) that depends only on x, so a team's arena booking falls
// in the same weeks every season.
const hash01 = (x: number) => {
	let h = Math.imul(x ^ 0x9e_37_79_b9, 0x85_eb_ca_6b);
	h ^= h >>> 13;
	h = Math.imul(h, 0xc2_b2_ae_35);
	h ^= h >>> 16;
	return (h >>> 0) / 4_294_967_296;
};

// A night's size drawn from the NBA's spread for its weekday.
const sampleNightSize = (weekday: number, random: () => number) => {
	const q = NIGHT_SIZES[weekday]!;
	const points = [Math.max(1, 2 * q[0]! - q[1]!), ...q, 2 * q[4]! - q[3]!];
	const u = random();
	let size = points.at(-1)!;
	for (let i = 1; i < QUANTILES.length; i++) {
		if (u <= QUANTILES[i]!) {
			const f = (u - QUANTILES[i - 1]!) / (QUANTILES[i]! - QUANTILES[i - 1]!);
			size = points[i - 1]! + f * (points[i]! - points[i - 1]!);
			break;
		}
	}
	// Spread twice as wide as the NBA's: travel pulls nights back toward
	// the middle.
	return Math.max(1, q[2]! + NIGHT_SPREAD * (size - q[2]!));
};

export type CalendarNights = {
	nights: string[];
	allStarDate: string | undefined;
	christmas: number | undefined;
	mlk: number | undefined;
};

// The game nights of a season with this many games per team.
export const buildNights = (
	season: number,
	gamesPerTeam: number,
	allStarFraction: number | undefined,
): CalendarNights => {
	const startYear = seasonStartYear(season);
	const numGameNights = Math.max(
		gamesPerTeam + 4,
		Math.min(NIGHTS_PER_GAME * gamesPerTeam, 200),
	);
	const hasBreak = allStarFraction !== undefined;
	const darkDays = [
		// Election Day, the Tuesday after the first Monday in November.
		addDays(nthWeekday(startYear, 11, 1, 1), 1),
		// Thanksgiving.
		nthWeekday(startYear, 11, 4, 4),
		makeDate(startYear, 12, 24),
		// The NCAA final, the Monday after the first Saturday in April.
		addDays(nthWeekday(startYear + 1, 4, 6, 1), 2),
	];

	// Work back from the second Sunday in April; a longer season than fits
	// from mid-October runs on past it.
	const span =
		numGameNights +
		darkDays.length +
		(hasBreak ? BREAK_TO - BREAK_FROM + 1 : 0);
	let end = secondSundayOfApril(startYear + 1);
	let start = previousWeekday(addDays(end, -(span - 1)), 2);
	if (darkDays.includes(start)) {
		// A shorter season would open on Election Day.
		start = addDays(start, -7);
	}
	const earliest = nextWeekday(makeDate(startYear, 10, 14), 2);
	if (dayNumber(start) < dayNumber(earliest)) {
		start = earliest;
		end = addDays(start, span - 1);
	}

	const length = dayNumber(end) - dayNumber(start) + 1;
	let allStarDate: string | undefined;
	const blackout = new Set(darkDays);
	if (hasBreak) {
		const at = addDays(
			start,
			Math.round(Math.min(1, Math.max(0, allStarFraction)) * (length - 1)),
		);
		// The nearest Sunday, or the NBA's third Sunday in February when that's
		// close, kept off the first and last weeks.
		let sunday = addDays(
			at,
			weekdayOf(at) <= 3 ? -weekdayOf(at) : 7 - weekdayOf(at),
		);
		const nbaSunday = nthWeekday(startYear + 1, 2, 0, 3);
		if (Math.abs(dayNumber(nbaSunday) - dayNumber(sunday)) <= 14) {
			sunday = nbaSunday;
		}
		if (dayNumber(sunday) - dayNumber(start) < 7) {
			sunday = nextWeekday(addDays(start, 7), 0);
		}
		if (dayNumber(end) - dayNumber(sunday) < 7) {
			sunday = previousWeekday(addDays(end, -7), 0);
		}
		allStarDate = sunday;
		for (let i = BREAK_FROM; i <= BREAK_TO; i++) {
			blackout.add(addDays(sunday, i));
		}
	}

	const nights: string[] = [];
	for (let i = 0; i < length; i++) {
		const date = addDays(start, i);
		if (!blackout.has(date)) {
			nights.push(date);
		}
	}

	const christmasIndex = nights.indexOf(makeDate(startYear, 12, 25));
	const mlkIndex = nights.indexOf(nthWeekday(startYear + 1, 1, 1, 3));
	return {
		nights,
		allStarDate,
		christmas: christmasIndex >= 0 ? christmasIndex : undefined,
		mlk: mlkIndex >= 0 ? mlkIndex : undefined,
	};
};

export const generateCalendar = ({
	season,
	teams,
	games,
	allStarFraction,
	random = Math.random,
	timeBudgetMs = 3000,
	maxIterations,
	checkCosts = false,
}: CalendarInput): CalendarOutput | undefined => {
	const T = teams.length;
	const G = games.length;
	if (T < 2 || G === 0) {
		return;
	}
	const idxByTid = new Map(teams.map((t, i) => [t.tid, i]));
	const home = new Int32Array(G);
	const away = new Int32Array(G);
	const gamesPerTeam = new Int32Array(T);
	const gamesOf: number[][] = teams.map(() => []);
	for (let g = 0; g < G; g++) {
		const h = idxByTid.get(games[g]![0]);
		const a = idxByTid.get(games[g]![1]);
		if (h === undefined || a === undefined || h === a) {
			return;
		}
		home[g] = h;
		away[g] = a;
		gamesPerTeam[h]! += 1;
		gamesPerTeam[a]! += 1;
		gamesOf[h]!.push(g);
		gamesOf[a]!.push(g);
	}
	let maxGames = 0;
	for (let t = 0; t < T; t++) {
		maxGames = Math.max(maxGames, gamesPerTeam[t]!);
	}

	const { nights, allStarDate, christmas, mlk } = buildNights(
		season,
		maxGames,
		allStarFraction,
	);
	const N = nights.length;
	if (N < maxGames) {
		return;
	}
	const dayNum = Int32Array.from(nights, dayNumber);
	const firstDay = dayNum[0]!;
	const numDays = dayNum[N - 1]! - firstDay + 1;
	// The night on each day of the season, or -1 for a dark day.
	const nightOnDay = new Int32Array(numDays).fill(-1);
	for (let n = 0; n < N; n++) {
		nightOnDay[dayNum[n]! - firstDay] = n;
	}
	const nightAt = (day: number) =>
		day < firstDay || day >= firstDay + numDays
			? -1
			: nightOnDay[day - firstDay]!;
	const nextDay = new Int32Array(N);
	for (let n = 0; n < N; n++) {
		nextDay[n] = nightAt(dayNum[n]! + 1);
	}

	const dist = distanceMatrix(teams.map((t) => t.location));
	// Back-to-backs the NBA doesn't play: two time zones apart.
	const farZone = new Uint8Array(T * T);
	for (let i = 0; i < T; i++) {
		for (let j = 0; j < T; j++) {
			const a = teams[i]!.location;
			const b = teams[j]!.location;
			if (a && b && Math.abs(a.longitude - b.longitude) > FAR_ZONE_DEGREES) {
				farZone[i * T + j] = 1;
			}
		}
	}

	// Each arena's nearest arenas, and each team's games by arena, for moves
	// that put a road game next to a neighbor.
	const neighbors: number[][] = teams.map((_, v) =>
		teams
			.map((_, u) => u)
			.filter((u) => u !== v)
			.sort((x, y) => dist[v * T + x]! - dist[v * T + y]!)
			.slice(0, 6),
	);
	const byVenue: number[][] = Array.from({ length: T * T }, () => []);
	for (let g = 0; g < G; g++) {
		byVenue[home[g]! * T + home[g]!]!.push(g);
		byVenue[away[g]! * T + home[g]!]!.push(g);
	}

	// How many games each night should hold.
	const target = new Float64Array(N);
	const special = new Uint8Array(N);
	const fixed = new Map<number, number>();
	const fullSlate = Math.floor(T / 2);
	if (T >= 10) {
		fixed.set(0, Math.min(2, fullSlate));
		if (christmas !== undefined && christmas !== N - 1) {
			fixed.set(christmas, Math.min(5, fullSlate));
		}
	}
	fixed.set(N - 1, fullSlate);
	let fixedTotal = 0;
	for (const [n, count] of fixed) {
		target[n] = count;
		special[n] = 1;
		fixedTotal += count;
	}
	let weightTotal = 0;
	const weights = new Float64Array(N);
	for (let n = 0; n < N; n++) {
		if (special[n]) {
			continue;
		}
		let w = sampleNightSize(weekdayOf(nights[n]!), random);
		if (n === 1) {
			w = 12;
		} else if (n === mlk) {
			w = 11;
		}
		weights[n] = w;
		weightTotal += w;
	}
	for (let n = 0; n < N; n++) {
		if (!special[n]) {
			target[n] = Math.min(
				fullSlate,
				((G - fixedTotal) * weights[n]!) / weightTotal,
			);
		}
	}

	// Back-to-backs to aim for: 14 in 82 at the NBA's pace, more when the
	// nights are tighter.
	const b2bTarget = new Float64Array(T);
	for (let t = 0; t < T; t++) {
		const p = gamesPerTeam[t]!;
		const ratio = N / Math.max(1, p);
		b2bTarget[t] = p * Math.min(0.95, Math.max(0.17, (2.2 - ratio) / 1.2));
	}

	// Arena bookings: about a third of teams lose their arena for two to three
	// weeks in midseason, the same weeks every year.
	const bookedFrom = new Int32Array(T).fill(-1);
	const bookedTo = new Int32Array(T).fill(-1);
	for (let t = 0; t < T; t++) {
		const tid = teams[t]!.tid;
		if (gamesPerTeam[t]! >= 50 && hash01(3 * tid + 1) < 1 / 3) {
			const days = 12 + Math.floor(hash01(3 * tid + 2) * 7);
			const from =
				firstDay + Math.round((0.3 + 0.4 * hash01(3 * tid + 3)) * numDays);
			bookedFrom[t] = from;
			bookedTo[t] = from + days - 1;
		}
	}

	// Two-game series: the same two teams in the same arena with a night
	// between, about four for every three teams, more often for a visitor from
	// far away. Each is two of a home team's games against one visitor.
	const seriesA: number[] = [];
	const seriesB: number[] = [];
	const seriesOf: number[][] = teams.map(() => []);
	const seriesOfGame = new Int32Array(G).fill(-1);
	if (T >= 6) {
		const byPair = new Map<number, number[]>();
		for (let g = 0; g < G; g++) {
			const key = home[g]! * T + away[g]!;
			const list = byPair.get(key);
			if (list) {
				list.push(g);
			} else {
				byPair.set(key, [g]);
			}
		}
		const candidates: { a: number; b: number; key: number }[] = [];
		for (const list of byPair.values()) {
			for (let i = 0; i + 1 < list.length; i += 2) {
				const g = list[i]!;
				const weight = 300 + dist[home[g]! * T + away[g]!]!;
				// Weighted order (Efraimidis-Spirakis).
				candidates.push({
					a: g,
					b: list[i + 1]!,
					key: Math.log(random()) / weight,
				});
			}
		}
		candidates.sort((x, y) => y.key - x.key);
		const want = Math.round((4 * T) / 3);
		const inSeries = new Int32Array(T);
		for (const { a, b } of candidates) {
			if (seriesA.length >= want) {
				break;
			}
			const h = home[a]!;
			const v = away[a]!;
			if (inSeries[h]! >= 4 || inSeries[v]! >= 4) {
				continue;
			}
			inSeries[h]! += 1;
			inSeries[v]! += 1;
			seriesOf[h]!.push(seriesA.length);
			seriesOf[v]!.push(seriesA.length);
			seriesOfGame[a] = seriesA.length;
			seriesOfGame[b] = seriesA.length;
			seriesA.push(a);
			seriesB.push(b);
		}
	}

	const occ = new Int32Array(T * N).fill(-1);
	const nightOf = new Int32Array(G).fill(-1);
	const count = new Int32Array(N);

	const place = (g: number, n: number) => {
		occ[home[g]! * N + n] = g;
		occ[away[g]! * N + n] = g;
		nightOf[g] = n;
		count[n]! += 1;
	};
	const unplace = (g: number) => {
		const n = nightOf[g]!;
		occ[home[g]! * N + n] = -1;
		occ[away[g]! * N + n] = -1;
		nightOf[g] = -1;
		count[n]! -= 1;
	};
	const free = (t: number, n: number) => occ[t * N + n] === -1;
	// Opening night and Christmas hold no more than their few games.
	const full = (n: number) => special[n] === 1 && count[n]! >= target[n]!;
	// Would a game on night n make three games in three nights for team t?
	const triples = (t: number, n: number) => {
		const day = dayNum[n]!;
		const playsOn = (d: number) => {
			const m = nightAt(d);
			return m >= 0 && m !== n && occ[t * N + m] !== -1;
		};
		const before1 = playsOn(day - 1);
		const after1 = playsOn(day + 1);
		return (
			(before1 && (after1 || playsOn(day - 2))) || (after1 && playsOn(day + 2))
		);
	};

	// Start: each game on a random open night, the last night first so every
	// team can be on it.
	const order = Array.from({ length: G }, (_, i) => i);
	for (let i = G - 1; i > 0; i--) {
		const j = Math.floor(random() * (i + 1));
		[order[i], order[j]] = [order[j]!, order[i]!];
	}
	for (const g of order) {
		const h = home[g]!;
		const a = away[g]!;
		let chosen = -1;
		if (count[N - 1]! < fullSlate && free(h, N - 1) && free(a, N - 1)) {
			chosen = N - 1;
		}
		for (let tries = 0; chosen < 0 && tries < 60; tries++) {
			const n = Math.floor(random() * N);
			if (
				free(h, n) &&
				free(a, n) &&
				!full(n) &&
				count[n]! < Math.ceil(target[n]!) + 2 &&
				!triples(h, n) &&
				!triples(a, n)
			) {
				chosen = n;
			}
		}
		if (chosen < 0) {
			const start = Math.floor(random() * N);
			for (let k = 0; k < N; k++) {
				const n = (start + k) % N;
				if (free(h, n) && free(a, n) && !full(n)) {
					chosen = n;
					break;
				}
			}
		}
		if (chosen < 0) {
			// Too many games for the nights - leave it to the old days.
			return;
		}
		place(g, chosen);
	}

	const stintCost = (length: number, isHome: boolean, booked: boolean) => {
		if (booked && !isHome) {
			return length > MAX_BOOKED_TRIP
				? W_STINT * (length - MAX_BOOKED_TRIP) ** 2
				: 0;
		}
		const cap = isHome ? MAX_HOMESTAND : MAX_TRIP;
		return (
			(length > cap ? W_STINT * (length - cap) ** 2 : 0) +
			(length > LONG_STINT ? W_LONG_STINT * (length - LONG_STINT) ** 2 : 0)
		);
	};

	// A team's cost is a sum of terms, each tied to one of its games (miles
	// from the game before, rest, how far into a road trip it is), plus its
	// back-to-back count against the target and its series. A move changes
	// only the terms near the nights it touches, so only those are recounted.
	let windowB2b = 0;
	// The terms tied to team t's games on nights lo to hi; at hi = N - 1 also
	// the flight home after the last game.
	const termsBetween = (t: number, lo: number, hi: number) => {
		const row = t * N;
		const bookFrom = bookedFrom[t]!;
		const bookTo = bookedTo[t]!;
		// Where things stand before lo.
		let loc = t;
		let prev = -1;
		let prevGame = -1;
		let prev2 = -1;
		let prev3 = -1;
		let prev4 = -1;
		let stintHome = false;
		let stint = 0;
		let stintBooked = false;
		let found = 0;
		let stintOpen = true;
		for (let m = lo - 1; m >= 0; m--) {
			const g = occ[row + m]!;
			if (g < 0) {
				continue;
			}
			const isHome = home[g] === t;
			const day = dayNum[m]!;
			const booked = day >= bookFrom && day <= bookTo;
			if (found === 0) {
				prev = m;
				prevGame = g;
				loc = home[g]!;
				stintHome = isHome;
				stint = 1;
				stintBooked = booked;
			} else {
				if (found === 1) {
					prev2 = m;
				} else if (found === 2) {
					prev3 = m;
				} else if (found === 3) {
					prev4 = m;
				}
				if (stintOpen) {
					if (isHome === stintHome) {
						stint += 1;
						stintBooked ||= booked;
					} else {
						stintOpen = false;
					}
				}
			}
			found += 1;
			if (found >= 4 && !stintOpen) {
				break;
			}
		}

		let cost = 0;
		let b2b = 0;
		for (let n = lo; n <= hi; n++) {
			const g = occ[row + n]!;
			if (g < 0) {
				continue;
			}
			const venue = home[g]!;
			const miles = dist[loc * T + venue]!;
			cost += miles;
			const day = dayNum[n]!;
			if (prev >= 0) {
				const gap = day - dayNum[prev]!;
				if (gap === 1) {
					b2b += 1;
					if (miles > FAR_B2B_MILES) {
						cost += W_FAR_B2B;
					}
					if (farZone[loc * T + venue]) {
						cost += W_ZONE_B2B;
					}
				}
				const idleNights = n - prev - 1;
				if (idleNights >= 4) {
					cost += W_GAP * (idleNights - 3) * (idleNights - 3);
				}
				if (prev2 >= 0 && day - dayNum[prev2]! <= 2) {
					cost += W_THREE_IN_THREE;
				}
				if (prev3 >= 0 && day - dayNum[prev3]! <= 4) {
					cost += W_FOUR_IN_FIVE;
				}
				if (prev4 >= 0 && day - dayNum[prev4]! <= 6) {
					cost += W_FIVE_IN_SEVEN;
				}
			}
			const isHome = venue === t;
			if (
				!isHome &&
				prevGame >= 0 &&
				home[prevGame] === venue &&
				away[prevGame] === t &&
				(seriesOfGame[g]! < 0 || seriesOfGame[g] !== seriesOfGame[prevGame])
			) {
				cost += W_UNPLANNED_SERIES;
			}
			const booked = day >= bookFrom && day <= bookTo;
			if (isHome && booked) {
				cost += W_BOOKED;
			}
			// A stint's cost, a game at a time.
			if (stint > 0 && isHome === stintHome) {
				const before = stintCost(stint, stintHome, stintBooked);
				stint += 1;
				stintBooked ||= booked;
				cost += stintCost(stint, stintHome, stintBooked) - before;
			} else {
				stintHome = isHome;
				stint = 1;
				stintBooked = booked;
			}
			loc = venue;
			prev4 = prev3;
			prev3 = prev2;
			prev2 = prev;
			prev = n;
			prevGame = g;
		}
		if (hi === N - 1) {
			cost += dist[loc * T + t]!;
		}
		windowB2b = b2b;
		return cost;
	};

	// The last night whose terms for team t can change with a game added on or
	// removed from night c: the next four games, any after them still in the
	// same stint, and the flight home if that runs to the end.
	const windowEnd = (t: number, c: number) => {
		const row = t * N;
		let k = 0;
		let firstHome = false;
		let sameStint = true;
		let hi = c;
		for (let m = c + 1; m < N; m++) {
			const g = occ[row + m]!;
			if (g < 0) {
				continue;
			}
			k += 1;
			const isHome = home[g] === t;
			if (k === 1) {
				firstHome = isHome;
			} else if (isHome !== firstHome) {
				sameStint = false;
			}
			if (k > 4 && !sameStint) {
				return hi;
			}
			hi = m;
		}
		return N - 1;
	};

	const seriesCost = (t: number) => {
		// Each team carries half of a broken series; a team that plays on the
		// night between breaks it on its own.
		let cost = 0;
		const row = t * N;
		for (const s of seriesOf[t]!) {
			const d1 = dayNum[nightOf[seriesA[s]!]!]!;
			const d2 = dayNum[nightOf[seriesB[s]!]!]!;
			if (Math.abs(d1 - d2) !== 2) {
				cost += W_SERIES / 2;
			} else {
				const between = nightAt(Math.min(d1, d2) + 1);
				if (between >= 0 && occ[row + between] !== -1) {
					cost += W_SERIES / 2;
				}
			}
		}
		return cost;
	};
	const b2bCost = (t: number, b2b: number) => {
		const off = b2b - b2bTarget[t]!;
		return W_B2B * off * off;
	};
	const nightCost = (n: number, c: number) => {
		const off = c - target[n]!;
		return (special[n] ? W_SPECIAL_NIGHT : W_NIGHT) * off * off;
	};

	// Rest edge: one team on the second night of a back-to-back, the other
	// not. It depends on both teams, so it's counted on the games around a move.
	const playedDayBefore = (t: number, n: number) =>
		n > 0 && dayNum[n - 1] === dayNum[n]! - 1 && occ[t * N + n - 1] !== -1;
	const stamp = new Int32Array(G);
	let stampNow = 0;
	// The teams and nights a move touches: at most 2 games, so 4 teams and 4
	// nights.
	const touchedTeams = new Int32Array(4);
	let numTouchedTeams = 0;
	const touchedNights = new Int32Array(4);
	let numTouchedNights = 0;
	// Each team's rest edges for less those against, and the change a move
	// makes to it.
	const restNet = new Int32Array(T);
	const restNetDelta = new Int32Array(T);
	const restNetTeams: number[] = [];
	const addRestNet = (t: number, d: number) => {
		if (restNetDelta[t] === 0 && !restNetTeams.includes(t)) {
			restNetTeams.push(t);
		}
		restNetDelta[t]! += d;
	};
	// The rest edges among the games around the move, counted with sign (+1
	// after it, -1 before), with each team's share added to restNetDelta.
	const restEdges = (sign: number) => {
		stampNow += 1;
		let edges = 0;
		for (let i = 0; i < numTouchedTeams; i++) {
			const t = touchedTeams[i]!;
			for (let j = 0; j < numTouchedNights; j++) {
				const n = touchedNights[j]!;
				for (let k = 0; k < 2; k++) {
					const m = k === 0 ? n : nextDay[n]!;
					if (m < 0) {
						continue;
					}
					const g = occ[t * N + m]!;
					if (g >= 0 && stamp[g] !== stampNow) {
						stamp[g] = stampNow;
						const homeTired = playedDayBefore(home[g]!, m);
						if (homeTired !== playedDayBefore(away[g]!, m)) {
							edges += 1;
							addRestNet(home[g]!, homeTired ? -sign : sign);
							addRestNet(away[g]!, homeTired ? sign : -sign);
						}
					}
				}
			}
		}
		return edges;
	};
	const restBalanceDelta = () => {
		let delta = 0;
		for (const t of restNetTeams) {
			const before = restNet[t]!;
			const after = before + restNetDelta[t]!;
			delta += after * after - before * before;
		}
		return W_REST_BALANCE * delta;
	};
	const settleRestNet = (keep: boolean) => {
		for (const t of restNetTeams) {
			if (keep) {
				restNet[t]! += restNetDelta[t]!;
			}
			restNetDelta[t] = 0;
		}
		restNetTeams.length = 0;
	};

	const nightCosts = () => {
		let sum = 0;
		for (let i = 0; i < numTouchedNights; i++) {
			const n = touchedNights[i]!;
			let seen = false;
			for (let j = 0; j < i; j++) {
				seen ||= touchedNights[j] === n;
			}
			if (!seen) {
				sum += nightCost(n, count[n]!);
			}
		}
		return sum;
	};

	// Each team's terms, back-to-backs and series cost as they stand.
	const terms = new Float64Array(T);
	const b2bs = new Int32Array(T);
	const series = new Float64Array(T);
	for (let t = 0; t < T; t++) {
		terms[t] = termsBetween(t, 0, N - 1);
		b2bs[t] = windowB2b;
		series[t] = seriesCost(t);
	}
	const restNetOf = () => {
		const net = new Int32Array(T);
		for (let g = 0; g < G; g++) {
			const n = nightOf[g]!;
			const homeTired = playedDayBefore(home[g]!, n);
			if (homeTired !== playedDayBefore(away[g]!, n)) {
				net[home[g]!]! += homeTired ? -1 : 1;
				net[away[g]!]! += homeTired ? 1 : -1;
			}
		}
		return net;
	};
	restNet.set(restNetOf());

	// Move games moveGames[i] to nights moveTo[i], keeping it if the annealing
	// says so.
	const moveGames = [0, 0];
	const moveTo = [0, 0];
	const moveFrom = [0, 0];
	// Night ranges to recount, per team: up to 8 [lo, hi] pairs.
	const RANGES = 16;
	const ranges = new Int32Array(T * RANGES);
	const numRanges = new Int32Array(T);
	const addRanges = (t: number, k: number) => {
		const base = t * RANGES;
		for (let i = 0; i < k; i++) {
			const g = moveGames[i]!;
			if (home[g] === t || away[g] === t) {
				let at = base + numRanges[t]!;
				ranges[at++] = moveFrom[i]!;
				ranges[at++] = windowEnd(t, moveFrom[i]!);
				ranges[at++] = moveTo[i]!;
				ranges[at++] = windowEnd(t, moveTo[i]!);
				numRanges[t]! += 4;
			}
		}
	};
	// Sort the pairs by start and join the ones that overlap.
	const mergeRanges = (t: number) => {
		const base = t * RANGES;
		const len = numRanges[t]!;
		for (let i = 2; i < len; i += 2) {
			const lo = ranges[base + i]!;
			const hi = ranges[base + i + 1]!;
			let j = i - 2;
			while (j >= 0 && ranges[base + j]! > lo) {
				ranges[base + j + 2] = ranges[base + j]!;
				ranges[base + j + 3] = ranges[base + j + 1]!;
				j -= 2;
			}
			ranges[base + j + 2] = lo;
			ranges[base + j + 3] = hi;
		}
		let out = 0;
		for (let i = 0; i < len; i += 2) {
			const lo = ranges[base + i]!;
			const hi = ranges[base + i + 1]!;
			if (out > 0 && lo <= ranges[base + out - 1]!) {
				ranges[base + out - 1] = Math.max(ranges[base + out - 1]!, hi);
			} else {
				ranges[base + out] = lo;
				ranges[base + out + 1] = hi;
				out += 2;
			}
		}
		numRanges[t] = out;
	};
	const recount = (t: number) => {
		const base = t * RANGES;
		let cost = 0;
		let b2b = 0;
		for (let i = 0; i < numRanges[t]!; i += 2) {
			cost += termsBetween(t, ranges[base + i]!, ranges[base + i + 1]!);
			b2b += windowB2b;
		}
		windowB2b = b2b;
		return cost;
	};
	const termsBefore = new Float64Array(T);
	const b2bBefore = new Int32Array(T);
	const stashSeries = new Float64Array(T);
	const touch = (t: number) => {
		for (let i = 0; i < numTouchedTeams; i++) {
			if (touchedTeams[i] === t) {
				return;
			}
		}
		touchedTeams[numTouchedTeams++] = t;
		numRanges[t] = 0;
	};
	const toNights = (k: number, nightsTo: number[]) => {
		for (let i = 0; i < k; i++) {
			unplace(moveGames[i]!);
		}
		for (let i = 0; i < k; i++) {
			place(moveGames[i]!, nightsTo[i]!);
		}
	};

	const tryMove = (k: number, temp: number) => {
		numTouchedTeams = 0;
		numTouchedNights = 0;
		for (let i = 0; i < k; i++) {
			const g = moveGames[i]!;
			moveFrom[i] = nightOf[g]!;
			touch(home[g]!);
			touch(away[g]!);
			touchedNights[numTouchedNights++] = moveFrom[i]!;
			touchedNights[numTouchedNights++] = moveTo[i]!;
		}

		// Does it fit?
		for (let i = 0; i < k; i++) {
			unplace(moveGames[i]!);
		}
		let placed = 0;
		for (; placed < k; placed++) {
			const g = moveGames[placed]!;
			const n = moveTo[placed]!;
			if (
				!free(home[g]!, n) ||
				!free(away[g]!, n) ||
				full(n) ||
				triples(home[g]!, n) ||
				triples(away[g]!, n)
			) {
				break;
			}
			place(g, n);
		}
		if (placed < k) {
			for (let i = 0; i < placed; i++) {
				unplace(moveGames[i]!);
			}
			for (let i = 0; i < k; i++) {
				place(moveGames[i]!, moveFrom[i]!);
			}
			return;
		}

		// The nights each touched team's terms can change on, before and after.
		let delta = nightCosts() + W_REST_EDGE * restEdges(1);
		for (let i = 0; i < numTouchedTeams; i++) {
			addRanges(touchedTeams[i]!, k);
		}
		toNights(k, moveFrom);
		delta -= nightCosts() + W_REST_EDGE * restEdges(-1);
		delta += restBalanceDelta();
		for (let i = 0; i < numTouchedTeams; i++) {
			const t = touchedTeams[i]!;
			addRanges(t, k);
			mergeRanges(t);
			termsBefore[t] = recount(t);
			b2bBefore[t] = windowB2b;
		}
		toNights(k, moveTo);
		for (let i = 0; i < numTouchedTeams; i++) {
			const t = touchedTeams[i]!;
			const termsNow = terms[t]! - termsBefore[t]! + recount(t);
			const b2bNow = b2bs[t]! - b2bBefore[t]! + windowB2b;
			const seriesNow = seriesCost(t);
			delta +=
				termsNow -
				terms[t]! +
				b2bCost(t, b2bNow) -
				b2bCost(t, b2bs[t]!) +
				seriesNow -
				series[t]!;
			// Kept if accepted below.
			termsBefore[t] = termsNow;
			b2bBefore[t] = b2bNow;
			stashSeries[t] = seriesNow;
		}
		if (delta <= 0 || random() < Math.exp(-delta / temp)) {
			for (let i = 0; i < numTouchedTeams; i++) {
				const t = touchedTeams[i]!;
				terms[t] = termsBefore[t]!;
				b2bs[t] = b2bBefore[t]!;
				series[t] = stashSeries[t]!;
			}
			settleRestNet(true);
		} else {
			toNights(k, moveFrom);
			settleRestNet(false);
		}
	};

	const iterations = maxIterations ?? Math.max(50_000, 4000 * G);
	const startTime = Date.now();
	const T0 = 3000;
	const T1 = 2;
	let temp = T0;
	for (let it = 0; it < iterations; it++) {
		if ((it & 1023) === 0) {
			const progress = Math.max(
				it / iterations,
				(Date.now() - startTime) / timeBudgetMs,
			);
			if (progress >= 1) {
				break;
			}
			temp = T0 * (T1 / T0) ** progress;
		}
		const r = random();

		if (r < 0.6) {
			// Two of a team's games trade nights: reroutes a road trip without
			// changing how full the nights are.
			const g1 = Math.floor(random() * G);
			const t = random() < 0.5 ? home[g1]! : away[g1]!;
			const n1 = nightOf[g1]!;
			let g2 = -1;
			const kind = random();
			if (kind < 0.25) {
				const list = gamesOf[t]!;
				g2 = list[Math.floor(random() * list.length)]!;
			} else if (kind < 0.5) {
				// One nearby, to reorder a trip.
				const n2 =
					n1 + (random() < 0.5 ? -1 : 1) * (1 + Math.floor(random() * 12));
				if (n2 >= 0 && n2 < N) {
					g2 = occ[t * N + n2]!;
				}
			} else {
				// A game in a city near the one before, to follow it.
				let before = t;
				for (let m = n1 - 1; m >= Math.max(0, n1 - 8); m--) {
					const prevGame = occ[t * N + m]!;
					if (prevGame >= 0) {
						before = home[prevGame]!;
						break;
					}
				}
				const near = neighbors[before]!;
				const venue =
					random() < 0.15 ? before : near[Math.floor(random() * near.length)]!;
				const list = byVenue[t * T + venue]!;
				if (list.length > 0) {
					g2 = list[Math.floor(random() * list.length)]!;
				}
			}
			if (g2 < 0 || g2 === g1) {
				continue;
			}
			moveGames[0] = g1;
			moveTo[0] = nightOf[g2]!;
			moveGames[1] = g2;
			moveTo[1] = n1;
			tryMove(2, temp);
			continue;
		}

		let g: number;
		let to: number;
		if (r < 0.68 && seriesA.length > 0) {
			// Bring a series together.
			const s = Math.floor(random() * seriesA.length);
			const flip = random() < 0.5;
			g = flip ? seriesA[s]! : seriesB[s]!;
			const other = flip ? seriesB[s]! : seriesA[s]!;
			to = nightAt(dayNum[nightOf[other]!]! + (random() < 0.5 ? -2 : 2));
		} else {
			g = Math.floor(random() * G);
			const from = nightOf[g]!;
			if (random() < 0.75) {
				const step = 1 + Math.floor(random() * 10);
				to = random() < 0.5 ? from - step : from + step;
			} else {
				to = Math.floor(random() * N);
			}
		}
		if (to < 0 || to >= N || to === nightOf[g]) {
			continue;
		}
		moveGames[0] = g;
		moveTo[0] = to;
		tryMove(1, temp);
	}

	if (checkCosts) {
		const net = restNetOf();
		for (let t = 0; t < T; t++) {
			if (net[t] !== restNet[t]) {
				throw new Error(`Team ${t}'s rest balance drifted`);
			}
			const whole = termsBetween(t, 0, N - 1);
			if (Math.abs(whole - terms[t]!) > 1e-6 || windowB2b !== b2bs[t]) {
				throw new Error(
					`Team ${t}'s cost drifted: ${terms[t]} kept, ${whole} recounted`,
				);
			}
		}
	}
	return {
		dates: Array.from(nightOf, (n) => nights[n]!),
		allStarDate,
	};
};

// Stats on a finished calendar, for tests and tuning. Miles are flown arena
// to arena, starting and ending at home.
export const calendarStats = (
	teams: CalendarTeam[],
	games: [number, number][],
	dates: string[],
) => {
	const dist = distanceMatrix(teams.map((t) => t.location));
	const T = teams.length;
	const idx = new Map(teams.map((t, i) => [t.tid, i]));
	const byTeam: { day: number; venue: number; opp: number }[][] = teams.map(
		() => [],
	);
	const lastPlayed = new Map<string, number>();
	games.forEach(([h, a], i) => {
		const day = dayNumber(dates[i]!);
		const hi = idx.get(h)!;
		const ai = idx.get(a)!;
		byTeam[hi]!.push({ day, venue: hi, opp: ai });
		byTeam[ai]!.push({ day, venue: hi, opp: hi });
		lastPlayed.set(`${hi}:${day}`, 1);
		lastPlayed.set(`${ai}:${day}`, 1);
	});
	let restEdges = 0;
	games.forEach(([h, a], i) => {
		const day = dayNumber(dates[i]!);
		const hb = lastPlayed.has(`${idx.get(h)}:${day - 1}`);
		const ab = lastPlayed.has(`${idx.get(a)}:${day - 1}`);
		if (hb !== ab) {
			restEdges += 1;
		}
	});
	const trips = new Map<number, number>();
	let series = 0;
	let zoneB2b = 0;
	const perTeam = byTeam.map((list, t) => {
		list.sort((x, y) => x.day - y.day);
		let miles = 0;
		let loc = t;
		let b2b = 0;
		let threeInThree = 0;
		let threeInFour = 0;
		let fourInFive = 0;
		let fiveInSeven = 0;
		let doubles = 0;
		let longestStint = 0;
		let longestTrip = 0;
		let stint = 0;
		let stintHome: boolean | undefined;
		list.forEach((game, i) => {
			miles += dist[loc * T + game.venue]!;
			const isHome = game.venue === t;
			if (i > 0) {
				const before = list[i - 1]!;
				const gap = game.day - before.day;
				if (gap === 0) {
					doubles += 1;
				}
				if (gap === 1) {
					b2b += 1;
					const a = teams[loc]!.location;
					const b = teams[game.venue]!.location;
					if (
						a &&
						b &&
						Math.abs(a.longitude - b.longitude) > FAR_ZONE_DEGREES
					) {
						zoneB2b += 1;
					}
				}
				if (!isHome && before.opp === game.opp && before.venue === game.venue) {
					series += 1;
				}
			}
			if (i > 1 && game.day - list[i - 2]!.day <= 2) {
				threeInThree += 1;
			}
			if (i > 1 && game.day - list[i - 2]!.day <= 3) {
				threeInFour += 1;
			}
			if (i > 2 && game.day - list[i - 3]!.day <= 4) {
				fourInFive += 1;
			}
			if (i > 3 && game.day - list[i - 4]!.day <= 6) {
				fiveInSeven += 1;
			}
			if (isHome === stintHome) {
				stint += 1;
			} else {
				if (stintHome === false) {
					trips.set(stint, (trips.get(stint) ?? 0) + 1);
				}
				stint = 1;
			}
			stintHome = isHome;
			longestStint = Math.max(longestStint, stint);
			if (!isHome) {
				longestTrip = Math.max(longestTrip, stint);
			}
			loc = game.venue;
		});
		if (stintHome === false) {
			trips.set(stint, (trips.get(stint) ?? 0) + 1);
		}
		miles += dist[loc * T + t]!;
		return {
			miles,
			b2b,
			threeInThree,
			threeInFour,
			fourInFive,
			fiveInSeven,
			doubles,
			longestStint,
			longestTrip,
		};
	});
	const perNight = new Map<string, number>();
	for (const date of dates) {
		perNight.set(date, (perNight.get(date) ?? 0) + 1);
	}
	return {
		perTeam,
		perNight,
		restEdgeShare: restEdges / Math.max(1, games.length),
		series,
		zoneB2b,
		trips,
	};
};
