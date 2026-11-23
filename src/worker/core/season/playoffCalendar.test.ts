import { assert, describe, test } from "vitest";
import type {
	Game,
	PlayoffSeries,
	PlayoffSeriesTeam,
} from "../../../common/types.ts";
import { makeSeededRandom } from "../../../common/random.ts";
import { daysBetween, weekdayOf } from "../../../common/calendarDates.ts";
import {
	ifNecessary,
	planPlayIn,
	planRound,
	playInFinalNight,
	roundKind,
	roundStartNight,
	type RoundKind,
} from "./playoffCalendar.ts";
import { playoffNightTick } from "../game/calendarTicks.ts";

const SEASON = 2027;
// A Sunday, as the NBA's regular season ends.
const LAST_REGULAR_SEASON_NIGHT = "2027-04-11";
const NUM_TEAMS = 30;
const NUM_GAMES = [7, 7, 7, 7];

type Matchup = {
	home: PlayoffSeriesTeam;
	away: PlayoffSeriesTeam;
	dates?: string[];
};

const team = (tid: number, seed: number): PlayoffSeriesTeam => ({
	tid,
	seed,
	cid: tid < 15 ? 0 : 1,
	won: 0,
});

// A whole NBA-style postseason (play-in, four best-of-7 rounds) played night by
// night off the plan, with random winners. Returns every night's games, each
// team's ticks, and the classic days the same series would have taken.
const simulate = (seed: number) => {
	const random = makeSeededRandom(seed);
	const games: Game[] = [];
	const nights: {
		night: string;
		tids: [number, number][];
		kind?: RoundKind;
	}[] = [];
	const ticks = new Map<number, number>();
	let classicDays = 0;
	let day = 0;

	const playIns = [0, 1].map((cid) => {
		const tid = (seed: number) => cid * 15 + seed - 1;
		return [
			{ home: team(tid(7), 7), away: team(tid(8), 8) },
			{ home: team(tid(9), 9), away: team(tid(10), 10) },
		] as Matchup[];
	});
	const playoffSeries: PlayoffSeries = {
		season: SEASON,
		currentRound: -1,
		series: NUM_GAMES.map(() => []),
		playIns: playIns as unknown as PlayoffSeries["playIns"],
	};

	const playNight = (
		night: string,
		matchups: Matchup[],
		numGamesToWin: number,
		kind?: RoundKind,
	) => {
		day += 1;
		const tids: [number, number][] = [];
		for (const matchup of matchups) {
			if (random() < 0.55) {
				matchup.home.won += 1;
			} else {
				matchup.away.won += 1;
			}
			tids.push([matchup.home.tid, matchup.away.tid]);
			games.push({
				gid: games.length,
				season: SEASON,
				day,
				date: night,
				playoffs: true,
				teams: [{ tid: matchup.home.tid }, { tid: matchup.away.tid }],
			} as unknown as Game);
		}
		nights.push({ night, tids, kind });
		const tick = playoffNightTick({
			games,
			season: SEASON,
			day,
			playoffSeries,
			numGamesToWin,
		});
		for (let tid = 0; tid < NUM_TEAMS; tid++) {
			const n =
				tick.ticksByTid === undefined
					? 1
					: (tick.ticksByTid.get(tid) ?? tick.restTicks ?? 0);
			ticks.set(tid, (ticks.get(tid) ?? 0) + n);
		}
	};

	// The play-in.
	const firstNights = planPlayIn(playIns.length, LAST_REGULAR_SEASON_NIGHT);
	const finalNight = playInFinalNight(firstNights);
	for (const [i, playIn] of playIns.entries()) {
		playNight(firstNights[i]!, playIn, 1);
		const lost7 = playIn[0]!.home.won > 0 ? playIn[0]!.away : playIn[0]!.home;
		const won9 = playIn[1]!.home.won > 0 ? playIn[1]!.home : playIn[1]!.away;
		playIn.push({
			home: { ...lost7, won: 0 },
			away: { ...won9, won: 0 },
			dates: [finalNight],
		});
	}
	playNight(
		finalNight,
		playIns.map((playIn) => playIn[2]!),
		1,
	);
	classicDays += 2;

	// The rounds, each once the last is over.
	let teams = playIns.flatMap((playIn, cid) => {
		const tid = (seed: number) => cid * 15 + seed - 1;
		const seven = playIn[0]!.home.won > 0 ? playIn[0]!.home : playIn[0]!.away;
		const eight = playIn[2]!.home.won > 0 ? playIn[2]!.home : playIn[2]!.away;
		return [
			team(tid(1), 1),
			team(eight.tid, 8),
			team(tid(4), 4),
			team(tid(5), 5),
			team(tid(3), 3),
			team(tid(6), 6),
			team(tid(2), 2),
			team(seven.tid, 7),
		];
	});
	let lastNight = finalNight;
	for (let rnd = 0; rnd < NUM_GAMES.length; rnd++) {
		playoffSeries.currentRound = rnd;
		const kind = roundKind(rnd, NUM_GAMES.length);
		const numGamesToWin = Math.ceil(NUM_GAMES[rnd]! / 2);
		const series: Matchup[] = [];
		for (let i = 0; i < teams.length; i += 2) {
			series.push({
				home: { ...teams[i]!, won: 0 },
				away: { ...teams[i + 1]!, won: 0 },
			});
		}
		playoffSeries.series[rnd] = series;

		const lastPlayed = new Map<number, string>();
		for (const game of games) {
			for (const { tid } of game.teams) {
				lastPlayed.set(tid, game.date!);
			}
		}
		const planned = planRound({
			kind,
			firstNight: roundStartNight({
				rnd,
				kind,
				lastNight,
				afterPlayIn: true,
				random,
			}),
			series: series.map((matchup) => ({
				tids: [matchup.home.tid, matchup.away.tid],
				numGames: NUM_GAMES[rnd]!,
			})),
			lastPlayed,
			random,
		});
		for (const [i, matchup] of series.entries()) {
			matchup.dates = planned[i];
		}

		const over = (matchup: Matchup) =>
			Math.max(matchup.home.won, matchup.away.won) >= numGamesToWin;
		while (series.some((matchup) => !over(matchup))) {
			let tonight: string | undefined;
			for (const matchup of series) {
				if (!over(matchup)) {
					const night = matchup.dates![matchup.home.won + matchup.away.won]!;
					if (tonight === undefined || night < tonight) {
						tonight = night;
					}
				}
			}
			playNight(
				tonight!,
				series.filter(
					(matchup) =>
						!over(matchup) &&
						matchup.dates![matchup.home.won + matchup.away.won] === tonight,
				),
				numGamesToWin,
				kind,
			);
			lastNight = tonight!;
		}
		classicDays += Math.max(
			...series.map((matchup) => matchup.home.won + matchup.away.won),
		);
		teams = series.map((matchup) =>
			matchup.home.won > matchup.away.won ? matchup.home : matchup.away,
		);
	}

	return { nights, ticks, classicDays, playoffSeries };
};

describe("playoff calendar", () => {
	test("the play-in is Tuesday, Wednesday and Friday after a Sunday finish", () => {
		const firstNights = planPlayIn(2, LAST_REGULAR_SEASON_NIGHT);
		assert.deepStrictEqual(firstNights, ["2027-04-13", "2027-04-14"]);
		assert.strictEqual(playInFinalNight(firstNights), "2027-04-16");
	});

	test("a season ending midweek waits for Tuesday", () => {
		// A Wednesday.
		const firstNights = planPlayIn(2, "2027-04-14");
		assert.deepStrictEqual(firstNights, ["2027-04-20", "2027-04-21"]);
		assert.strictEqual(playInFinalNight(firstNights), "2027-04-23");
	});

	test("rounds are named from the end", () => {
		assert.deepStrictEqual(
			[0, 1, 2, 3].map((rnd) => roundKind(rnd, 4)),
			["first", "second", "conferenceFinals", "finals"],
		);
		assert.deepStrictEqual(
			[0, 1].map((rnd) => roundKind(rnd, 2)),
			["conferenceFinals", "finals"],
		);
	});

	test("if necessary: games past the ones a series is sure to play", () => {
		const flags = (homeWon: number, awayWon: number) =>
			[0, 1, 2, 3, 4, 5, 6].map((i) => ifNecessary(i, homeWon, awayWon, 4));
		assert.deepStrictEqual(flags(0, 0), [
			false,
			false,
			false,
			false,
			true,
			true,
			true,
		]);
		assert.deepStrictEqual(flags(3, 0).slice(3), [false, true, true, true]);
		assert.deepStrictEqual(flags(3, 3).slice(6), [false]);
		assert.deepStrictEqual(flags(2, 1).slice(3), [false, false, true, true]);
	});

	test("whole postseasons follow the NBA's rules", () => {
		for (let seed = 1; seed <= 200; seed++) {
			const { nights } = simulate(seed);

			// No team on back-to-back nights, ever.
			const lastNight = new Map<number, string>();
			for (const { night, tids } of nights) {
				for (const tid of tids.flat()) {
					const last = lastNight.get(tid);
					assert.isTrue(
						last === undefined || daysBetween(last, night) >= 2,
						`seed ${seed}: team ${tid} on ${last} and ${night}`,
					);
					lastNight.set(tid, night);
				}
			}

			// A night at most half the round's series.
			const cap: Record<RoundKind, number> = {
				first: 4,
				second: 2,
				conferenceFinals: 1,
				finals: 1,
			};
			for (const { kind, tids } of nights) {
				if (kind) {
					assert.isAtMost(tids.length, cap[kind]);
				}
			}

			// Round 1 opens on Saturday and Sunday, the day after the play-in.
			const first = nights.filter(({ kind }) => kind === "first");
			assert.strictEqual(weekdayOf(first[0]!.night), 6);
			assert.strictEqual(first[0]!.night, "2027-04-17");
			assert.strictEqual(first[0]!.tids.length, 4);
			assert.strictEqual(first[1]!.night, "2027-04-18");
			assert.strictEqual(first[1]!.tids.length, 4);

			// The Finals: Thursday, Sunday, Wednesday, Friday, Monday, Thursday,
			// Sunday.
			const finals = nights.filter(({ kind }) => kind === "finals");
			assert.deepStrictEqual(
				finals.map(({ night }) => weekdayOf(night)),
				[4, 0, 3, 5, 1, 4, 0].slice(0, finals.length),
			);
			const conferenceFinals = nights.filter(
				({ kind }) => kind === "conferenceFinals",
			);
			const gap = daysBetween(conferenceFinals.at(-1)!.night, finals[0]!.night);
			assert.isAtLeast(gap, 3);
			assert.isAtMost(gap, 9);
		}
	});

	test("every team ticks as many times as on classic days", () => {
		for (let seed = 1; seed <= 200; seed++) {
			const { ticks, classicDays } = simulate(seed);
			for (let tid = 0; tid < NUM_TEAMS; tid++) {
				assert.strictEqual(
					ticks.get(tid),
					classicDays,
					`seed ${seed}: team ${tid}`,
				);
			}
		}
	});

	test("a series that ends behind the round catches up", () => {
		// Series B is already through Game 5; tonight A sweeps in Game 4.
		const playoffSeries = {
			season: SEASON,
			currentRound: 0,
			series: [
				[
					{ home: { ...team(0, 1), won: 4 }, away: team(1, 8) },
					{ home: { ...team(2, 4), won: 3 }, away: { ...team(3, 5), won: 2 } },
				],
			],
		} as PlayoffSeries;
		const tonight = {
			gid: 1,
			season: SEASON,
			day: 9,
			date: "2027-04-25",
			playoffs: true,
			teams: [{ tid: 1 }, { tid: 0 }],
		} as unknown as Game;
		const tick = playoffNightTick({
			games: [tonight],
			season: SEASON,
			day: 9,
			playoffSeries,
			numGamesToWin: 4,
		});
		assert.strictEqual(tick.ticksByTid!.get(0), 2);
		assert.strictEqual(tick.ticksByTid!.get(1), 2);
		assert.strictEqual(tick.ticksByTid!.get(2), 0);
		assert.strictEqual(tick.ticksByTid!.get(3), 0);
		assert.strictEqual(tick.restTicks, 0);
		assert.strictEqual(tick.leagueRuns, 0);
	});
});
