import { assert, describe, test } from "vitest";
import {
	buildNights,
	calendarStats,
	generateCalendar,
} from "./generateCalendar.ts";
import { weekdayOf } from "../../../common/calendarDates.ts";

const NBA_REGIONS = [
	"Boston",
	"Brooklyn",
	"New York",
	"Philadelphia",
	"Toronto",
	"Chicago",
	"Cleveland",
	"Detroit",
	"Indiana",
	"Milwaukee",
	"Atlanta",
	"Charlotte",
	"Miami",
	"Orlando",
	"Washington",
	"Denver",
	"Minnesota",
	"Oklahoma City",
	"Portland",
	"Utah",
	"Golden State",
	"Los Angeles",
	"Los Angeles",
	"Phoenix",
	"Sacramento",
	"Dallas",
	"Houston",
	"Memphis",
	"New Orleans",
	"San Antonio",
];

// A seeded generator, so a failure can be replayed.
const seeded = (seed: number) => () => {
	// mulberry32
	seed = (seed + 0x6d2b79f5) | 0;
	let t = Math.imul(seed ^ (seed >>> 15), 1 | seed);
	t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
	return ((t ^ (t >>> 14)) >>> 0) / 4_294_967_296;
};

// The NBA's format: 4 games with each division rival, 3 or 4 with the rest of
// the conference, 2 with the other conference.
const nbaGames = (random: () => number) => {
	const games: [number, number][] = [];
	const div = (t: number) => Math.floor(t / 5);
	const conf = (t: number) => Math.floor(t / 15);
	for (let a = 0; a < 30; a++) {
		for (let b = a + 1; b < 30; b++) {
			if (div(a) === div(b)) {
				games.push([a, b], [a, b], [b, a], [b, a]);
			} else if (conf(a) === conf(b)) {
				if ((a + b) % 5 < 2) {
					games.push([a, b], [b, a], random() < 0.5 ? [a, b] : [b, a]);
				} else {
					games.push([a, b], [a, b], [b, a], [b, a]);
				}
			} else {
				games.push([a, b], [b, a]);
			}
		}
	}
	return games;
};

const roundRobin = (numTeams: number, rounds: number) => {
	const games: [number, number][] = [];
	for (let r = 0; r < rounds; r++) {
		for (let a = 0; a < numTeams; a++) {
			for (let b = a + 1; b < numTeams; b++) {
				games.push(r % 2 === 0 ? [a, b] : [b, a]);
			}
		}
	}
	return games;
};

const checkHard = (
	teams: { tid: number }[],
	games: [number, number][],
	dates: string[],
) => {
	assert.strictEqual(dates.length, games.length);
	const seen = new Set<string>();
	games.forEach(([h, a], i) => {
		for (const tid of [h, a]) {
			const key = `${tid}:${dates[i]}`;
			assert.isFalse(seen.has(key), `team ${tid} twice on ${dates[i]}`);
			seen.add(key);
		}
	});
	const { perTeam } = calendarStats(teams, games, dates);
	return perTeam;
};

describe("generateCalendar", () => {
	test("an NBA-shaped season looks like the NBA's", () => {
		const random = seeded(7);
		const teams = NBA_REGIONS.map((region, tid) => ({ tid, region }));
		const games = nbaGames(random);
		const out = generateCalendar({
			season: 2027,
			teams,
			games,
			allStarFraction: 0.7,
			random,
			timeBudgetMs: 60_000,
		})!;
		assert.isDefined(out);

		const perTeam = checkHard(teams, games, out.dates);
		const { perNight } = calendarStats(teams, games, out.dates);

		const nights = Array.from(perNight.keys()).sort();
		assert.strictEqual(
			nights[0],
			"2026-10-20",
			"opens on a Tuesday in late October",
		);
		assert.strictEqual(
			nights.at(-1),
			"2027-04-11",
			"ends on the second Sunday of April",
		);
		assert.isAtLeast(nights.length, 155);
		assert.isAtMost(nights.length, 172);

		assert.isUndefined(perNight.get("2026-11-26"), "no games on Thanksgiving");
		assert.isUndefined(perNight.get("2026-12-24"), "no games on Christmas Eve");
		assert.strictEqual(
			perNight.get("2026-12-25"),
			5,
			"five games on Christmas",
		);
		assert.strictEqual(
			perNight.get(nights[0]!),
			2,
			"two games on opening night",
		);
		assert.isAtLeast(
			perNight.get(nights.at(-1)!)!,
			13,
			"nearly everyone on the last night",
		);

		assert.strictEqual(
			weekdayOf(out.allStarDate!),
			0,
			"All-Star Game on a Sunday",
		);
		assert.strictEqual(out.allStarDate!.slice(5, 7), "02");
		for (let i = -2; i <= 2; i++) {
			const d = new Date(`${out.allStarDate}T00:00:00Z`);
			d.setUTCDate(d.getUTCDate() + i);
			assert.isUndefined(perNight.get(d.toISOString().slice(0, 10)));
		}

		for (const team of perTeam) {
			assert.strictEqual(team.threeInThree, 0);
			assert.isAtLeast(team.b2b, 10);
			assert.isAtMost(team.b2b, 19);
			assert.isAtMost(team.longestStint, 8);
		}
		const miles =
			perTeam.reduce((sum, team) => sum + team.miles, 0) / perTeam.length;
		// The NBA flies about 41,000 to 47,000 venue to venue; the random order
		// of classic days about 66,000.
		assert.isBelow(miles, 55_000);
	});

	test("a small league with a short season", () => {
		const random = seeded(3);
		const teams = Array.from({ length: 8 }, (_, tid) => ({
			tid: tid + 2,
			region: tid % 2 === 0 ? "Chicago" : undefined,
		}));
		const games = roundRobin(8, 6).map(
			([h, a]) => [h + 2, a + 2] as [number, number],
		);
		const out = generateCalendar({
			season: 2030,
			teams,
			games,
			allStarFraction: undefined,
			random,
		})!;
		assert.isDefined(out);
		assert.isUndefined(out.allStarDate);
		const perTeam = checkHard(teams, games, out.dates);
		for (const team of perTeam) {
			assert.strictEqual(team.threeInThree, 0);
		}
		const nights = Array.from(new Set(out.dates)).sort();
		assert.strictEqual(nights.at(-1)!.slice(0, 7), "2030-04");
	});

	test("an odd number of teams", () => {
		const random = seeded(11);
		const teams = Array.from({ length: 7 }, (_, tid) => ({ tid }));
		const games = roundRobin(7, 4);
		const out = generateCalendar({
			season: 2030,
			teams,
			games,
			allStarFraction: 0.5,
			random,
		})!;
		assert.isDefined(out);
		checkHard(teams, games, out.dates);
	});

	test("a long season still fits", () => {
		const random = seeded(5);
		const teams = Array.from({ length: 6 }, (_, tid) => ({ tid }));
		// 150 games each.
		const games = roundRobin(6, 30);
		const out = generateCalendar({
			season: 2030,
			teams,
			games,
			allStarFraction: 0.7,
			random,
		});
		assert.isDefined(out);
		checkHard(teams, games, out!.dates);
	});

	test("nights for a full season", () => {
		const { nights, allStarDate } = buildNights(2027, 82, 0.7);
		assert.strictEqual(nights[0], "2026-10-20");
		assert.strictEqual(nights.at(-1), "2027-04-11");
		assert.isTrue(allStarDate! > "2027-02-07" && allStarDate! < "2027-02-28");
	});
});
