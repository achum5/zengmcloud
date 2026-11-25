import { assert, describe, test } from "vitest";
import {
	biggestGame,
	nationalGamesOn,
	nationalNight,
	networkFor,
	nightNetworks,
	nightOf,
	parseNationalBroadcasts,
	slateOf,
	strengthOf,
} from "./nationalBroadcasts.ts";
import { recordText } from "./scoreBug.ts";
import { GENERIC_NATIONAL } from "./genericNational.ts";
import type { NationalBroadcast, ScoreBugStyle } from "./types.ts";

const bug = (text: string): ScoreBugStyle => ({
	width: 400,
	height: 50,
	pieces: [{ show: "series", x: 0, y: 0, w: 400, h: 50, text }],
});

const NBC: NationalBroadcast = {
	network: "NBC",
	nights: ["Tue"],
	scoreBug: bug("COAST 2 COAST TUESDAY"),
};
const NBC_SUNDAY: NationalBroadcast = {
	network: "NBC",
	nights: ["Sun"],
	scoreBug: bug("SUNDAY NIGHT BASKETBALL"),
};
const ESPN: NationalBroadcast = {
	network: "ESPN",
	nights: ["Wed", "Fri"],
	finals: true,
	scoreBug: bug("NBA WEDNESDAY"),
};

describe("parseNationalBroadcasts", () => {
	test("takes networks as they are", () => {
		const list = [
			NBC,
			{
				...ESPN,
				logo: "https://example.com/espn.png",
				logoDark: "https://example.com/espn-white.png",
			},
		];
		assert.deepStrictEqual(parseNationalBroadcasts(list), list);
		assert.deepStrictEqual(parseNationalBroadcasts([]), []);
		const christmas = { ...ESPN, nights: undefined, slate: "christmas" };
		delete christmas.nights;
		assert.deepStrictEqual(parseNationalBroadcasts([christmas]), [christmas]);
		// (Ours, as a file could have it.)
		assert.deepStrictEqual(parseNationalBroadcasts([GENERIC_NATIONAL]), [
			GENERIC_NATIONAL,
		]);
	});

	test("says what's wrong", () => {
		for (const bad of [
			{},
			[{ nights: ["Tue"], scoreBug: NBC.scoreBug }],
			[{ ...NBC, nights: ["Tuesday"] }],
			[{ ...NBC, finals: "yes" }],
			[{ ...NBC, logo: 4 }],
			[{ ...NBC, logoDark: 4 }],
			[{ ...NBC, channel: 4 }],
			[{ ...NBC, scoreBug: { width: 10 } }],
			[{ ...NBC, slate: "boxingDay" }],
		]) {
			assert.strictEqual(
				typeof parseNationalBroadcasts(bad),
				"string",
				JSON.stringify(bad),
			);
		}
	});
});

describe("nightOf", () => {
	test("a date's own weekday", () => {
		assert.strictEqual(nightOf({ date: "2026-11-17", day: 30 }, 1), 2);
		assert.strictEqual(nightOf({ date: "2026-11-22" }, undefined), 0);
	});

	test("no calendar: opening night a Tuesday, a week a week", () => {
		assert.strictEqual(nightOf({ day: 5 }, 5), 2);
		assert.strictEqual(nightOf({ day: 6 }, 5), 3);
		assert.strictEqual(nightOf({ day: 10 }, 5), 0);
		assert.strictEqual(nightOf({ day: 12 }, 5), 2);
		assert.strictEqual(nightOf({}, 5), undefined);
	});
});

describe("the night's biggest game", () => {
	test("by rating at first, by record once teams have played", () => {
		assert.strictEqual(strengthOf(undefined, 0.8), 0.8);
		// 9-1 on a rating of 0.2: mostly the record by now.
		assert.closeTo(strengthOf({ won: 9, lost: 1 }, 0.2), 11 / 20, 1e-9);
		assert.closeTo(strengthOf({ won: 30, lost: 0 }, 0), 0.75, 1e-9);
	});

	test("the two best teams between them; the earliest game if even", () => {
		const s = new Map([
			[1, 0.9],
			[2, 0.2],
			[3, 0.6],
			[4, 0.6],
			[5, 0.6],
			[6, 0.6],
		]);
		const strength = (tid: number) => s.get(tid) ?? 0;
		const night = [
			{ gid: 10, tids: [1, 2] as [number, number] },
			{ gid: 12, tids: [5, 6] as [number, number] },
			{ gid: 11, tids: [3, 4] as [number, number] },
		];
		assert.strictEqual(biggestGame(night, strength)?.gid, 11);
		assert.strictEqual(biggestGame([], strength), undefined);
	});
});

describe("networkFor", () => {
	const networks = [NBC, ESPN, NBC_SUNDAY];
	const game = { season: 2026, tids: [3, 7] as [number, number] };

	test("the night's biggest game, on that night's network", () => {
		assert.strictEqual(networkFor(networks, game, 2, true), NBC);
		assert.strictEqual(networkFor(networks, game, 5, true), ESPN);
		assert.strictEqual(networkFor(networks, game, 0, true), NBC_SUNDAY);
		// Not the biggest, or a night nobody has.
		assert.strictEqual(networkFor(networks, game, 2, false), undefined);
		assert.strictEqual(networkFor(networks, game, 4, true), undefined);
		assert.strictEqual(networkFor(networks, game, undefined, true), undefined);
	});

	test("the playoffs: every game, a series on one network; the Finals on theirs", () => {
		const playoff = { ...game, playoffs: true };
		const seen = new Set<string>();
		for (let a = 0; a < 8; a++) {
			for (let b = a + 1; b < 8; b++) {
				const tids = [a, b] as [number, number];
				const n = networkFor(networks, { ...playoff, tids }, undefined, false);
				assert.ok(n);
				// Either way round, any night.
				assert.strictEqual(
					networkFor(networks, { ...playoff, tids: [b, a] }, 3, false),
					n,
				);
				seen.add(n.network);
			}
		}
		assert.deepStrictEqual([...seen].sort(), ["ESPN", "NBC"]);
		// (NBC's first bug, not its Sunday one.)
		for (let a = 0; a < 8; a++) {
			const n = networkFor(
				networks,
				{ ...playoff, tids: [a, a + 1] },
				undefined,
				false,
			);
			assert.notStrictEqual(n, NBC_SUNDAY);
		}
		assert.strictEqual(
			networkFor(networks, { ...playoff, finals: true }, 0, false),
			ESPN,
		);
		assert.strictEqual(
			networkFor([NBC], { ...playoff, finals: true }, 0, false),
			NBC,
		);
		assert.strictEqual(
			networkFor([{ ...ESPN, nights: [] }], playoff, 0, false),
			undefined,
		);
	});
});

describe("the NBA's national slate", () => {
	const OPENING = "2025-10-21";

	test("the special nights", () => {
		assert.strictEqual(slateOf(OPENING, OPENING), "openingNight");
		assert.strictEqual(slateOf("2025-12-25", OPENING), "christmas");
		// MLK Day 2026: Monday, January 19; Rivals Week the Tuesday to Saturday
		// after.
		assert.strictEqual(slateOf("2026-01-19", OPENING), "mlkDay");
		assert.strictEqual(slateOf("2026-01-20", OPENING), "rivalsWeek");
		assert.strictEqual(slateOf("2026-01-24", OPENING), "rivalsWeek");
		assert.isUndefined(slateOf("2026-01-25", OPENING));
		assert.isUndefined(slateOf("2025-10-22", OPENING));
	});

	test("how many games a night is national", () => {
		const on = (date: string, numGames = 10) =>
			nationalGamesOn(date, nightOf({ date }, undefined), OPENING, numGames);
		// Every game on opening night and Christmas.
		assert.strictEqual(on(OPENING, 2), 2);
		assert.strictEqual(on("2025-12-25", 5), 5);
		// Opening week, a doubleheader a night - even Thursday.
		assert.strictEqual(on("2025-10-23"), 2);
		// Tuesdays and Wednesdays, two; Thursdays none till mid-January.
		assert.strictEqual(on("2025-11-04"), 2);
		assert.strictEqual(on("2025-11-05"), 2);
		assert.strictEqual(on("2025-11-06"), 0);
		assert.strictEqual(on("2026-01-15"), 2);
		// Fridays two, then four from January.
		assert.strictEqual(on("2025-11-07"), 2);
		assert.strictEqual(on("2026-01-09"), 4);
		// Saturdays one, two from January to March; Sundays from February.
		assert.strictEqual(on("2025-11-08"), 1);
		assert.strictEqual(on("2026-01-10"), 2);
		assert.strictEqual(on("2026-04-04"), 1);
		assert.strictEqual(on("2026-01-11"), 0);
		assert.strictEqual(on("2026-02-01"), 2);
		assert.strictEqual(on("2026-04-05"), 1);
		// The night before Thanksgiving, three; MLK Day four; Rivals Week's
		// Friday three.
		assert.strictEqual(on("2025-11-26"), 3);
		assert.strictEqual(on("2026-01-19"), 4);
		assert.strictEqual(on("2026-01-23"), 3);
		// Never more than the night has; no calendar, one.
		assert.strictEqual(on("2026-01-19", 2), 2);
		assert.strictEqual(nationalGamesOn(undefined, 2, undefined, 6), 1);
		assert.strictEqual(nationalGamesOn(undefined, 2, undefined, 0), 0);
	});

	const XMAS: NationalBroadcast = {
		network: "ESPN",
		slate: "christmas",
		scoreBug: bug("CHRISTMAS"),
	};
	const PRIME: NationalBroadcast = {
		network: "Prime",
		nights: ["Thu", "Fri"],
		scoreBug: bug("TNF"),
	};

	test("who carries a night: its slate's network, else its weekday's", () => {
		const networks = [XMAS, NBC, PRIME, ESPN];
		assert.deepStrictEqual(nightNetworks(networks, 4, "christmas"), [XMAS]);
		assert.deepStrictEqual(nightNetworks(networks, 5, undefined), [
			PRIME,
			ESPN,
		]);
		// A slate with no network of its own: the weekday's.
		assert.deepStrictEqual(nightNetworks(networks, 2, "openingNight"), [NBC]);
		// ...or, nobody having that weekday either, the league's in turn.
		assert.deepStrictEqual(nightNetworks([NBC, ESPN], 1, "mlkDay"), [
			NBC,
			ESPN,
		]);
		assert.deepStrictEqual(nightNetworks([NBC, ESPN], 1, undefined), []);
		// The slate's network has nothing else.
		assert.deepStrictEqual(nightNetworks([XMAS], 4, undefined), []);
		assert.strictEqual(
			networkFor(
				[XMAS, NBC],
				{ season: 2026, tids: [1, 2], playoffs: true },
				4,
				false,
			),
			NBC,
		);
	});

	test("a night's national games: the biggest, shared out in order", () => {
		const strength = (tid: number) => tid / 10;
		const night = [
			{ gid: 1, tids: [1, 2] as [number, number] },
			{ gid: 2, tids: [9, 8] as [number, number] },
			{ gid: 3, tids: [3, 4] as [number, number] },
			{ gid: 4, tids: [7, 6] as [number, number] },
			{ gid: 5, tids: [5, 0] as [number, number] },
		];
		const picks = (
			date: string,
			networks: NationalBroadcast[],
			rivals?: (a: number, b: number) => number | boolean,
		) =>
			[
				...nationalNight(
					networks,
					night,
					{ date, night: nightOf({ date }, undefined), openingDate: OPENING },
					strength,
					rivals,
				),
			].map(([gid, n]) => `${gid} ${n.network}`);
		// A January Friday: four, Prime the two biggest, ESPN the next two.
		assert.deepStrictEqual(picks("2026-01-09", [NBC, PRIME, ESPN]), [
			"2 Prime",
			"4 Prime",
			"3 ESPN",
			"5 ESPN",
		]);
		// Christmas: all five, on its own network.
		assert.deepStrictEqual(
			picks("2025-12-25", [XMAS, ESPN]).map((p) => p.split(" ")[1]),
			["ESPN", "ESPN", "ESPN", "ESPN", "ESPN"],
		);
		// A Thursday in November: none.
		assert.deepStrictEqual(picks("2025-11-06", [NBC, PRIME, ESPN]), []);
		// Rivals Week: the division rivals first.
		assert.deepStrictEqual(
			picks("2026-01-20", [NBC], (a, b) => a + b === 7),
			["3 NBC", "2 NBC"],
		);
		// The week's own rivalries before the division rivals, however weak.
		assert.deepStrictEqual(
			picks("2026-01-20", [NBC], (a, b) =>
				a + b === 3 ? 2 : a + b === 7 ? 1 : 0,
			),
			["1 NBC", "3 NBC"],
		);
	});
});

describe("recordText", () => {
	const t = { record: "11-3", rank: 2, conf: "East" };
	test("the record with where the team stands", () => {
		assert.strictEqual(
			recordText("{rec} ({rank} in {conf})", t),
			"11-3 (2nd in East)",
		);
		assert.strictEqual(
			recordText("{rec} ({rank})", { ...t, rank: 13 }),
			"11-3 (13th)",
		);
		assert.strictEqual(recordText(undefined, t), "11-3");
		assert.strictEqual(recordText("({rank} IN {CONF})", t), "(2nd IN EAST)");
	});

	test("the record alone where the standing isn't known", () => {
		assert.strictEqual(
			recordText("{rec} ({rank} in {conf})", { record: "11-3" }),
			"11-3",
		);
		assert.strictEqual(recordText("{rec} ({rank} in {conf})", {}), "");
		assert.strictEqual(
			recordText("({rank} IN {CONF})", { record: "11-3" }),
			"",
		);
	});
});
