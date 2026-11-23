import { addDays, weekdayOf } from "../../../common/calendarDates.ts";

// THE PLAYOFFS ON THE CALENDAR, scheduled the way the NBA schedules them -
// worked out from the real game dates of the 2011 to 2024 playoffs (not the
// 2020 bubble or the late 2021 playoffs):
// - The regular season ends on a Sunday. The play-in is Tuesday (one
//   conference's 7-8 and 9-10 games), Wednesday (the other's) and Friday
//   (both conferences' games for the 8th seed).
// - Round 1 starts the next day, half the series on Saturday and half on
//   Sunday. Later rounds start a night or two after the last one ends, again
//   split over two nights; the Finals start on a Thursday.
// - A series plays every two or three nights (how often each gap came up, by
//   game, is in GAPS), with a longer break after Game 2 in the later rounds.
// - A night has at most half the round's series: up to 4 games in round 1, 2
//   in round 2, 1 in the Conference Finals (so they alternate nights) and the
//   Finals.
// - No team ever plays on back-to-back nights.
// Every game of a series gets its night when the matchup is set, the way the
// NBA publishes Games 1-7 with "if necessary" on the last ones. As in BBGM,
// a round starts once the whole round before it is over.

export type RoundKind = "first" | "second" | "conferenceFinals" | "finals";

// Rounds are named from the end: the last is the Finals, and a league with
// more rounds than the NBA plays its early ones like a first round.
export const roundKind = (rnd: number, numRounds: number): RoundKind => {
	const fromEnd = numRounds - 1 - rnd;
	if (fromEnd <= 0) {
		return "finals";
	}
	if (fromEnd === 1) {
		return "conferenceFinals";
	}
	if (fromEnd === 2) {
		return "second";
	}
	return "first";
};

type Weights = Partial<Record<number, number>>;

// Days from one game of a series to the next, Game 1 to 2 first: the share of
// real series with each gap.
const GAPS: Record<Exclude<RoundKind, "finals">, Weights[]> = {
	first: [
		{ 2: 45, 3: 55 },
		{ 2: 22, 3: 78 },
		{ 2: 85, 3: 15 },
		{ 2: 64, 3: 36 },
		{ 2: 76, 3: 24 },
		{ 2: 100 },
	],
	second: [
		{ 2: 94, 3: 6 },
		{ 2: 65, 3: 6, 4: 29 },
		{ 2: 85, 3: 15 },
		{ 2: 95, 3: 5 },
		{ 2: 93, 3: 7 },
		{ 2: 33, 3: 67 },
	],
	conferenceFinals: [
		{ 2: 96, 3: 4 },
		// Both series take the long break or neither does (see planRound).
		{ 2: 67, 4: 33 },
		{ 2: 100 },
		{ 2: 100 },
		{ 2: 100 },
		{ 2: 100 },
	],
};

// Thursday, Sunday, Wednesday, Friday, Monday, Thursday, Sunday.
const FINALS_GAPS = [3, 3, 2, 3, 3, 3];

const SATURDAY = 6;
const TUESDAY = 2;
const THURSDAY = 4;

const pick = (weights: Weights, random: () => number) => {
	const entries = Object.entries(weights).map(
		([gap, weight]) => [Number(gap), weight!] as const,
	);
	const total = entries.reduce((sum, [, weight]) => sum + weight, 0);
	let u = random() * total;
	for (const [gap, weight] of entries) {
		u -= weight;
		if (u < 0) {
			return gap;
		}
	}
	return entries.at(-1)![0];
};

// The first night on or after `from` that's the given weekday.
const onOrAfter = (from: string, weekday: number) =>
	addDays(from, (weekday - weekdayOf(from) + 7) % 7);

const later = (a: string, b: string) => (a > b ? a : b);

// THE PLAY-IN. The first games of each play-in (one per conference) get a
// night each from the Tuesday after the season, and every play-in's last game
// is two nights after that - Tuesday, Wednesday, Friday. (The real seasons
// from before the play-in ended on a Wednesday; theirs waits for the next
// Tuesday, so the playoffs still keep the NBA's days of the week.)
export const planPlayIn = (numPlayIns: number, lastNight: string) => {
	const start = onOrAfter(addDays(lastNight, 2), TUESDAY);
	const firstNights: string[] = [];
	for (let i = 0; i < numPlayIns; i++) {
		firstNights.push(addDays(start, i));
	}
	return firstNights;
};

// The night of the last play-in games, from the first games' nights.
export const playInFinalNight = (firstNights: string[]) => {
	const sorted = [...firstNights].sort();
	return later(addDays(sorted[0]!, 3), addDays(sorted.at(-1)!, 2));
};

// The first night a round can start, after the night of the last game before
// it (the regular season's, the play-in's or the last round's).
export const roundStartNight = ({
	rnd,
	kind,
	lastNight,
	afterPlayIn,
	random,
}: {
	rnd: number;
	kind: RoundKind;
	lastNight: string;
	afterPlayIn: boolean;
	random: () => number;
}) => {
	if (rnd === 0) {
		// The day after the play-in, or the Saturday after the season (it ended
		// on a Wednesday before the play-in).
		return afterPlayIn
			? addDays(lastNight, 1)
			: onOrAfter(addDays(lastNight, 3), SATURDAY);
	}
	if (kind === "finals") {
		return onOrAfter(addDays(lastNight, 3), THURSDAY);
	}
	if (kind === "conferenceFinals") {
		return addDays(lastNight, pick({ 1: 25, 2: 50, 3: 25 }, random));
	}
	return addDays(lastNight, pick({ 1: 60, 2: 40 }, random));
};

export type RoundSeries = {
	tids: [number, number];
	numGames: number;
};

// Every game's night for each series of a round, in order: Games 1 to
// numGames. A team's first game is at least two nights after its last
// (`lastPlayed`), and no night has more than half the round's series.
export const planRound = ({
	kind,
	firstNight,
	series,
	lastPlayed,
	random = Math.random,
}: {
	kind: RoundKind;
	firstNight: string;
	series: RoundSeries[];
	lastPlayed: Map<number, string>;
	random?: () => number;
}): string[][] => {
	const cap = Math.max(1, Math.ceil(series.length / 2));

	// Each series' gaps, drawn up front.
	const longBreak =
		kind === "conferenceFinals" ? pick(GAPS.conferenceFinals[1]!, random) : 2;
	const gaps = series.map(({ numGames }) => {
		const out: number[] = [];
		for (let i = 0; i < numGames - 1; i++) {
			if (kind === "finals") {
				out.push(FINALS_GAPS[Math.min(i, FINALS_GAPS.length - 1)]!);
			} else if (kind === "conferenceFinals" && i === 1) {
				out.push(longBreak);
			} else {
				const table = GAPS[kind];
				out.push(pick(table[Math.min(i, table.length - 1)]!, random));
			}
		}
		return out;
	});

	const dates: string[][] = series.map(() => []);
	const games = new Map<string, number>();

	// Each series' next game and the first night it can be played. Ties go in
	// a random order, so which series opens on the first night is a draw.
	const order = series.map((_, i) => ({ i, tiebreak: random() }));
	const pending = order.map(({ i, tiebreak }) => {
		let earliest = firstNight;
		for (const tid of series[i]!.tids) {
			const last = lastPlayed.get(tid);
			if (last !== undefined) {
				earliest = later(earliest, addDays(last, 2));
			}
		}
		return { i, earliest, tiebreak };
	});

	while (pending.length > 0) {
		pending.sort((a, b) =>
			a.earliest !== b.earliest
				? a.earliest < b.earliest
					? -1
					: 1
				: a.tiebreak - b.tiebreak,
		);
		const next = pending.shift()!;
		let night = next.earliest;
		while ((games.get(night) ?? 0) >= cap) {
			night = addDays(night, 1);
		}
		games.set(night, (games.get(night) ?? 0) + 1);
		const seriesDates = dates[next.i]!;
		seriesDates.push(night);
		const k = seriesDates.length;
		if (k < series[next.i]!.numGames) {
			pending.push({
				i: next.i,
				earliest: addDays(night, gaps[next.i]![k - 1]!),
				tiebreak: next.tiebreak,
			});
		}
	}

	return dates;
};

// Whether a series' game (0 for Game 1) might not be needed: past the games
// it's sure to play, given the wins so far.
export const ifNecessary = (
	index: number,
	homeWon: number,
	awayWon: number,
	numGamesToWin: number,
) => index >= homeWon + awayWon + numGamesToWin - Math.max(homeWon, awayWon);

// The All-Star Game in the playoffs (allStarGame -1) is the Tuesday before the
// Finals, two nights before Game 1.
export const playoffsAllStarNight = (finalsGame1: string) =>
	addDays(finalsGame1, -2);
