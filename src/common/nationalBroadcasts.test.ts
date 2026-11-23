import { assert, describe, test } from "vitest";
import {
	biggestGame,
	networkFor,
	nightOf,
	parseNationalBroadcasts,
	strengthOf,
} from "./nationalBroadcasts.ts";
import { recordText } from "./scoreBug.ts";
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
		const list = [NBC, { ...ESPN, logo: "https://example.com/espn.png" }];
		assert.deepStrictEqual(parseNationalBroadcasts(list), list);
		assert.deepStrictEqual(parseNationalBroadcasts([]), []);
	});

	test("says what's wrong", () => {
		for (const bad of [
			{},
			[{ nights: ["Tue"], scoreBug: NBC.scoreBug }],
			[{ ...NBC, nights: ["Tuesday"] }],
			[{ ...NBC, finals: "yes" }],
			[{ ...NBC, logo: 4 }],
			[{ ...NBC, channel: 4 }],
			[{ ...NBC, scoreBug: { width: 10 } }],
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
