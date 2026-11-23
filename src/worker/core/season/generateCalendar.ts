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
// What it aims for, from the NBA's own seasons (2010-11 to 2022-23):
// - about 164 game nights for 82 games, opening on a Tuesday in late
//   October and ending on the second Sunday in April;
// - no games on Thanksgiving or Christmas Eve, five games on Christmas, a
//   five-night All-Star break around the All-Star Game's Sunday, every team on
//   the last night;
// - nights as full as the NBA's weekdays are (Wednesday and Friday heavy,
//   Thursday light);
// - each team about 14 back-to-backs, never three games in three nights, road
//   trips and homestands of at most six, and as few miles as the rest allows.
//
// It anneals: random moves of one game to another night, kept when they help
// and sometimes when they don't, less often as it goes.

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
};

export type CalendarOutput = {
	// Parallel to input.games.
	dates: string[];
	allStarDate: string | undefined;
};

// Average games per night, Sunday to Saturday, NBA 2010-11 to 2022-23.
const WEEKDAY_WEIGHTS = [6.73, 7.75, 5.8, 10.38, 4.01, 9.99, 7.78];

const NIGHTS_PER_GAME = 2;
const MAX_STINT = 7;

const W_B2B = 300;
const W_FAR_B2B = 900;
const FAR_B2B_MILES = 1000;
const W_THREE_IN_THREE = 60_000;
const W_FOUR_IN_FIVE = 4000;
const W_STINT = 2500;
const W_GAP = 500;
const W_NIGHT = 120;
const W_SPECIAL_NIGHT = 4000;

const secondSundayOfApril = (year: number) => nthWeekday(year, 4, 0, 2);

const previousWeekday = (date: string, weekday: number) =>
	addDays(date, -((weekdayOf(date) - weekday + 7) % 7));

const nextWeekday = (date: string, weekday: number) =>
	addDays(date, (weekday - weekdayOf(date) + 7) % 7);

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
	const thanksgiving = nthWeekday(startYear, 11, 4, 4);
	const christmasEve = makeDate(startYear, 12, 24);

	// Work back from the second Sunday in April; a longer season than fits
	// from mid-October runs on past it.
	const span = numGameNights + 2 + (hasBreak ? 5 : 0);
	let end = secondSundayOfApril(startYear + 1);
	let start = previousWeekday(addDays(end, -(span - 1)), 2);
	const earliest = nextWeekday(makeDate(startYear, 10, 14), 2);
	if (dayNumber(start) < dayNumber(earliest)) {
		start = earliest;
		end = addDays(start, span - 1);
	}

	const length = dayNumber(end) - dayNumber(start) + 1;
	let allStarDate: string | undefined;
	const blackout = new Set([thanksgiving, christmasEve]);
	if (hasBreak) {
		const at = addDays(
			start,
			Math.round(Math.min(1, Math.max(0, allStarFraction)) * (length - 1)),
		);
		// The nearest Sunday, kept off the first and last weeks.
		let sunday = addDays(
			at,
			weekdayOf(at) <= 3 ? -weekdayOf(at) : 7 - weekdayOf(at),
		);
		if (dayNumber(sunday) - dayNumber(start) < 7) {
			sunday = nextWeekday(addDays(start, 7), 0);
		}
		if (dayNumber(end) - dayNumber(sunday) < 7) {
			sunday = previousWeekday(addDays(end, -7), 0);
		}
		allStarDate = sunday;
		for (let i = -2; i <= 2; i++) {
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
	timeBudgetMs = 1500,
	maxIterations,
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
	const dist = distanceMatrix(teams.map((t) => t.location));

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
		let w = WEEKDAY_WEIGHTS[weekdayOf(nights[n]!)]!;
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
		let before1 = false;
		let before2 = false;
		let after1 = false;
		let after2 = false;
		for (let m = Math.max(0, n - 2); m <= Math.min(N - 1, n + 2); m++) {
			if (m === n || occ[t * N + m] === -1) {
				continue;
			}
			const offset = dayNum[m]! - dayNum[n]!;
			if (offset === -1) {
				before1 = true;
			} else if (offset === -2) {
				before2 = true;
			} else if (offset === 1) {
				after1 = true;
			} else if (offset === 2) {
				after2 = true;
			}
		}
		return (before2 && before1) || (before1 && after1) || (after1 && after2);
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

	const teamCost = (t: number) => {
		const row = t * N;
		let loc = t;
		let cost = 0;
		let b2b = 0;
		let prev = -1;
		let prev2 = -1;
		let prev3 = -1;
		let stintHome = -1;
		let stint = 0;
		for (let n = 0; n < N; n++) {
			const g = occ[row + n]!;
			if (g < 0) {
				continue;
			}
			const venue = home[g]!;
			const miles = dist[loc * T + venue]!;
			cost += miles;
			if (prev >= 0) {
				const gap = dayNum[n]! - dayNum[prev]!;
				if (gap === 1) {
					b2b += 1;
					if (miles > FAR_B2B_MILES) {
						cost += W_FAR_B2B;
					}
				}
				const idleNights = n - prev - 1;
				if (idleNights >= 4) {
					cost += W_GAP * (idleNights - 3) * (idleNights - 3);
				}
				if (prev2 >= 0 && dayNum[n]! - dayNum[prev2]! <= 2) {
					cost += W_THREE_IN_THREE;
				}
				if (prev3 >= 0 && dayNum[n]! - dayNum[prev3]! <= 4) {
					cost += W_FOUR_IN_FIVE;
				}
			}
			const isHome = venue === t ? 1 : 0;
			if (isHome === stintHome) {
				stint += 1;
			} else {
				if (stint > MAX_STINT) {
					cost += W_STINT * (stint - MAX_STINT) * (stint - MAX_STINT);
				}
				stintHome = isHome;
				stint = 1;
			}
			loc = venue;
			prev3 = prev2;
			prev2 = prev;
			prev = n;
		}
		if (stint > MAX_STINT) {
			cost += W_STINT * (stint - MAX_STINT) * (stint - MAX_STINT);
		}
		cost += dist[loc * T + t]!;
		const off = b2b - b2bTarget[t]!;
		cost += W_B2B * off * off;
		return cost;
	};
	const nightCost = (n: number, c: number) => {
		const off = c - target[n]!;
		return (special[n] ? W_SPECIAL_NIGHT : W_NIGHT) * off * off;
	};

	const costs = new Float64Array(T);
	for (let t = 0; t < T; t++) {
		costs[t] = teamCost(t);
	}

	const iterations = maxIterations ?? Math.max(50_000, 1000 * G);
	const startTime = Date.now();
	const T0 = 3000;
	const T1 = 2;
	let progress = 0;
	for (let it = 0; it < iterations; it++) {
		if ((it & 1023) === 0) {
			progress = Math.max(
				it / iterations,
				(Date.now() - startTime) / timeBudgetMs,
			);
			if (progress >= 1) {
				break;
			}
		}
		const temp = T0 * (T1 / T0) ** progress;

		const g = Math.floor(random() * G);
		const h = home[g]!;
		const a = away[g]!;
		const from = nightOf[g]!;
		let to: number;
		if (random() < 0.75) {
			const step = 1 + Math.floor(random() * 10);
			to = random() < 0.5 ? from - step : from + step;
		} else {
			to = Math.floor(random() * N);
		}
		if (
			to < 0 ||
			to >= N ||
			to === from ||
			!free(h, to) ||
			!free(a, to) ||
			full(to)
		) {
			continue;
		}

		const nightDelta =
			nightCost(from, count[from]! - 1) -
			nightCost(from, count[from]!) +
			nightCost(to, count[to]! + 1) -
			nightCost(to, count[to]!);
		unplace(g);
		if (triples(h, to) || triples(a, to)) {
			place(g, from);
			continue;
		}
		place(g, to);
		const newH = teamCost(h);
		const newA = teamCost(a);
		const delta = newH - costs[h]! + newA - costs[a]! + nightDelta;
		if (delta <= 0 || random() < Math.exp(-delta / temp)) {
			costs[h] = newH;
			costs[a] = newA;
		} else {
			unplace(g);
			place(g, from);
		}
	}

	return {
		dates: Array.from(nightOf, (n) => nights[n]!),
		allStarDate,
	};
};

// Stats on a finished calendar, for tests and tuning.
export const calendarStats = (
	teams: CalendarTeam[],
	games: [number, number][],
	dates: string[],
) => {
	const dist = distanceMatrix(teams.map((t) => t.location));
	const T = teams.length;
	const idx = new Map(teams.map((t, i) => [t.tid, i]));
	const byTeam: { day: number; venue: number }[][] = teams.map(() => []);
	games.forEach(([h, a], i) => {
		const day = dayNumber(dates[i]!);
		byTeam[idx.get(h)!]!.push({ day, venue: idx.get(h)! });
		byTeam[idx.get(a)!]!.push({ day, venue: idx.get(h)! });
	});
	const perTeam = byTeam.map((list, t) => {
		list.sort((x, y) => x.day - y.day);
		let miles = 0;
		let loc = t;
		let b2b = 0;
		let threeInThree = 0;
		let doubles = 0;
		let longestStint = 0;
		let stint = 0;
		let stintHome: boolean | undefined;
		list.forEach((game, i) => {
			miles += dist[loc * T + game.venue]!;
			loc = game.venue;
			if (i > 0) {
				const gap = game.day - list[i - 1]!.day;
				if (gap === 0) {
					doubles += 1;
				}
				if (gap === 1) {
					b2b += 1;
				}
			}
			if (i > 1 && game.day - list[i - 2]!.day <= 2) {
				threeInThree += 1;
			}
			const isHome = game.venue === t;
			stint = isHome === stintHome ? stint + 1 : 1;
			stintHome = isHome;
			longestStint = Math.max(longestStint, stint);
		});
		miles += dist[loc * T + t]!;
		return { miles, b2b, threeInThree, doubles, longestStint };
	});
	const perNight = new Map<string, number>();
	for (const date of dates) {
		perNight.set(date, (perNight.get(date) ?? 0) + 1);
	}
	return { perTeam, perNight };
};
