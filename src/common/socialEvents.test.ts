import { assert, describe, test } from "vitest";
import {
	assignEventDays,
	eventFromLeagueEvent,
	eventsFromGame,
	gameSalience,
	isFeedableLeagueEvent,
	leagueEventSalience,
	performanceSalience,
	plainEventText,
	trimDayEvents,
	type GameForEvents,
	type GameTeamForEvents,
	type SocialEvent,
	placeLeagueEvents,
	OFFSEASON_DAY,
} from "./socialEvents.ts";

const line = (
	pid: number,
	name: string,
	stats: Partial<GameTeamForEvents["players"][number]> = {},
) => ({
	pid,
	name,
	min: 32,
	pts: 10,
	reb: 4,
	ast: 3,
	stl: 1,
	blk: 0,
	tov: 2,
	fga: 10,
	fta: 2,
	...stats,
});

const teamFor = (
	tid: number,
	region: string,
	name: string,
	pts: number,
	players: GameTeamForEvents["players"] = [],
): GameTeamForEvents => ({
	tid,
	region,
	name,
	abbrev: region.slice(0, 3).toUpperCase(),
	pts,
	players,
});

const gameFor = (overrides: Partial<GameForEvents> = {}): GameForEvents => ({
	gid: 100,
	day: 5,
	season: 2013,
	overtimes: 0,
	winnerTid: 0,
	playoffs: false,
	teams: [
		teamFor(0, "Boston", "Celtics", 110, [line(1, "Paul Pierce")]),
		teamFor(1, "Sacramento", "Kings", 100, [line(2, "Tyreke Evans")]),
	],
	...overrides,
});

describe("gameSalience", () => {
	test("a nine-point win is the boring case", () => {
		const dull = gameSalience({ margin: 9, overtimes: 0, playoffs: false });
		const close = gameSalience({ margin: 1, overtimes: 0, playoffs: false });
		const blowout = gameSalience({ margin: 32, overtimes: 0, playoffs: false });
		assert.strictEqual(close > dull, true);
		assert.strictEqual(blowout > dull, true);
	});

	test("both ends of the margin are interesting, which a slope would miss", () => {
		// The reason this is a curve: ranking purely by margin would call a
		// one-point thriller the least interesting game of the night.
		const close = gameSalience({ margin: 2, overtimes: 0, playoffs: false });
		const mid = gameSalience({ margin: 11, overtimes: 0, playoffs: false });
		assert.strictEqual(close > mid, true);
	});

	test("overtime, playoffs, elimination and upsets all add", () => {
		const base = gameSalience({ margin: 6, overtimes: 0, playoffs: false });
		assert.strictEqual(
			gameSalience({ margin: 6, overtimes: 1, playoffs: false }) > base,
			true,
		);
		assert.strictEqual(
			gameSalience({ margin: 6, overtimes: 0, playoffs: true }) > base,
			true,
		);
		assert.strictEqual(
			gameSalience({
				margin: 6,
				overtimes: 0,
				playoffs: true,
				elimination: true,
			}) > gameSalience({ margin: 6, overtimes: 0, playoffs: true }),
			true,
		);
		assert.strictEqual(
			gameSalience({ margin: 6, overtimes: 0, playoffs: false, upset: true }) >
				base,
			true,
		);
	});

	test("a long streak matters and a short one does not", () => {
		const base = gameSalience({ margin: 6, overtimes: 0, playoffs: false });
		assert.strictEqual(
			gameSalience({ margin: 6, overtimes: 0, playoffs: false, streak: 3 }),
			base,
		);
		assert.strictEqual(
			gameSalience({ margin: 6, overtimes: 0, playoffs: false, streak: 12 }) >
				base,
			true,
		);
	});

	test("stays inside 0 and 1 even when everything happens at once", () => {
		const max = gameSalience({
			margin: 1,
			overtimes: 4,
			playoffs: true,
			elimination: true,
			streak: 20,
			upset: true,
		});
		assert.strictEqual(max <= 1, true);
		assert.strictEqual(max > 0.9, true);
	});
});

describe("performanceSalience", () => {
	const plain = { pts: 12, reb: 4, ast: 2, stl: 1, blk: 0 };

	test("a triple-double is a different kind of night, not just a bigger one", () => {
		// Ranking on points alone would put a 30-point night on 28 shots over
		// this, which is the thing that makes a generated feed read wrong.
		const td = performanceSalience({
			pts: 14,
			reb: 11,
			ast: 10,
			stl: 1,
			blk: 0,
		});
		const volumeScorer = performanceSalience({
			pts: 30,
			reb: 3,
			ast: 2,
			stl: 0,
			blk: 0,
		});
		assert.strictEqual(td > volumeScorer, true);
	});

	test("a quiet line scores near nothing", () => {
		assert.strictEqual(performanceSalience(plain) < 0.15, true);
	});

	test("scoring scales, and a huge night approaches the top", () => {
		const good = performanceSalience({ ...plain, pts: 28 });
		const huge = performanceSalience({ ...plain, pts: 50 });
		assert.strictEqual(huge > good, true);
		assert.strictEqual(good > performanceSalience(plain), true);
	});

	test("efficiency only counts once the volume is there", () => {
		// A 9-point night on perfect shooting is not a story.
		const tiny = performanceSalience({ ...plain, pts: 9, tsp: 95 });
		const tinyNoTsp = performanceSalience({ ...plain, pts: 9 });
		assert.strictEqual(tiny, tinyNoTsp);

		const big = performanceSalience({ ...plain, pts: 30, tsp: 95 });
		const bigNoTsp = performanceSalience({ ...plain, pts: 30 });
		assert.strictEqual(big > bigNoTsp, true);
	});

	test("the other columns can carry a night on their own", () => {
		assert.strictEqual(
			performanceSalience({ pts: 6, reb: 25, ast: 2, stl: 0, blk: 0 }) > 0.2,
			true,
		);
		assert.strictEqual(
			performanceSalience({ pts: 8, reb: 3, ast: 20, stl: 0, blk: 0 }) > 0.2,
			true,
		);
	});
});

describe("eventsFromGame", () => {
	test("a game always produces its result", () => {
		const events = eventsFromGame(gameFor());
		const result = events.find((e) => e.type === "gameResult")!;
		assert.strictEqual(result.id, "g:100");
		assert.strictEqual(result.day, 5);
		assert.strictEqual(result.facts.winnerPts, 110);
		assert.strictEqual(result.facts.loserPts, 100);
		assert.strictEqual(result.facts.margin, 10);
		assert.deepStrictEqual(result.tids, [0, 1]);
	});

	test("the winner is read off winnerTid, not off which side scored more", () => {
		// Home and away order is a rendering choice; the game record says who won.
		const events = eventsFromGame(gameFor({ winnerTid: 0 }));
		const result = events.find((e) => e.type === "gameResult")!;
		assert.strictEqual(result.facts.winnerTid, 0);
		assert.strictEqual(result.facts.winnerName, "Boston Celtics");
	});

	test("a quiet game produces no performances", () => {
		const events = eventsFromGame(gameFor());
		assert.strictEqual(
			events.some((e) => e.type === "performance"),
			false,
		);
	});

	test("performances are ranked across both teams, so a losing star counts", () => {
		const events = eventsFromGame(
			gameFor({
				teams: [
					teamFor(0, "Boston", "Celtics", 110, [
						line(1, "Winner", { pts: 22 }),
					]),
					teamFor(1, "Sacramento", "Kings", 100, [
						line(2, "Losing Star", { pts: 46, reb: 12 }),
					]),
				],
			}),
		);
		const perfs = events.filter((e) => e.type === "performance");
		assert.strictEqual(perfs[0]!.facts.name, "Losing Star");
		assert.strictEqual(perfs[0]!.facts.won, false);
	});

	test("only players who actually played are considered", () => {
		const events = eventsFromGame(
			gameFor({
				teams: [
					teamFor(0, "Boston", "Celtics", 110, [
						line(1, "Did Not Play", { min: 0, pts: 60 }),
					]),
					teamFor(1, "Sacramento", "Kings", 100, []),
				],
			}),
		);
		assert.strictEqual(
			events.some((e) => e.type === "performance"),
			false,
		);
	});

	test("one game never floods the day with performances", () => {
		const many = Array.from({ length: 8 }, (_, i) =>
			line(i + 1, `Star ${i}`, { pts: 40 + i, reb: 12 }),
		);
		const events = eventsFromGame(
			gameFor({
				teams: [
					teamFor(0, "Boston", "Celtics", 200, many),
					teamFor(1, "Sacramento", "Kings", 100, []),
				],
			}),
		);
		assert.strictEqual(
			events.filter((e) => e.type === "performance").length <= 2,
			true,
		);
	});

	test("a triple-double is flagged, with its facts intact", () => {
		const events = eventsFromGame(
			gameFor({
				teams: [
					teamFor(0, "Boston", "Celtics", 110, [
						line(1, "Rajon Rondo", { pts: 15, reb: 11, ast: 14 }),
					]),
					teamFor(1, "Sacramento", "Kings", 100, []),
				],
			}),
		);
		const perf = events.find((e) => e.type === "performance")!;
		assert.strictEqual(perf.facts.tripleDouble, true);
		assert.strictEqual(perf.facts.pts, 15);
		assert.strictEqual(perf.facts.reb, 11);
		assert.strictEqual(perf.facts.ast, 14);
		assert.strictEqual(perf.facts.opponentAbbrev, "SAC");
	});

	test("ties in salience break on pid, so two devices agree", () => {
		const twins = [
			line(9, "Later Pid", { pts: 40 }),
			line(3, "Earlier Pid", { pts: 40 }),
		];
		const a = eventsFromGame(
			gameFor({
				teams: [
					teamFor(0, "Boston", "Celtics", 110, twins),
					teamFor(1, "Sacramento", "Kings", 100, []),
				],
			}),
		);
		const b = eventsFromGame(
			gameFor({
				teams: [
					teamFor(0, "Boston", "Celtics", 110, [...twins].reverse()),
					teamFor(1, "Sacramento", "Kings", 100, []),
				],
			}),
		);
		assert.deepStrictEqual(
			a.map((e) => e.id),
			b.map((e) => e.id),
		);
	});

	test("an upset is recognised from the pregame line", () => {
		const events = eventsFromGame(
			gameFor({ winnerTid: 0, spread: { favTid: 1, points: 6 } }),
		);
		assert.strictEqual(
			events.find((e) => e.type === "gameResult")!.facts.upset,
			true,
		);
	});

	test("a pick'em is never an upset", () => {
		const events = eventsFromGame(
			gameFor({ winnerTid: 0, spread: { favTid: 1, points: 0 } }),
		);
		assert.strictEqual(
			events.find((e) => e.type === "gameResult")!.facts.upset,
			false,
		);
	});

	test("event ids are stable across repeated derivation", () => {
		const a = eventsFromGame(gameFor());
		const b = eventsFromGame(gameFor());
		assert.deepStrictEqual(a, b);
	});
});

describe("league events", () => {
	test("known types map to a social type and topic", () => {
		const trade = eventFromLeagueEvent(
			{ eid: 7, type: "trade", season: 2013, text: "A for B", score: 20 },
			5,
		)!;
		assert.strictEqual(trade.type, "trade");
		assert.strictEqual(trade.topic, "trade");
		assert.strictEqual(trade.id, "e:7");
		assert.strictEqual(trade.day, 5);
	});

	test("unknown types are skipped rather than posted about generically", () => {
		// A feed that says "something happened" about a database upgrade notice
		// is exactly the cheapness this design is avoiding.
		assert.strictEqual(
			eventFromLeagueEvent({ eid: 1, type: "upgrade", season: 2013 }, 1),
			undefined,
		);
		assert.strictEqual(isFeedableLeagueEvent("upgrade"), false);
		assert.strictEqual(isFeedableLeagueEvent("trade"), true);
	});

	test("the league's importance maps onto the same scale games use", () => {
		assert.strictEqual(leagueEventSalience(undefined) < 0.5, true);
		assert.strictEqual(leagueEventSalience(25) > leagueEventSalience(5), true);
		assert.strictEqual(leagueEventSalience(1000) <= 1, true);
	});

	test("link markup is stripped so the text reads as prose", () => {
		assert.strictEqual(
			plainEventText('<a href="/x">Paul Pierce</a> signed with   Boston.'),
			"Paul Pierce signed with Boston.",
		);
		assert.strictEqual(plainEventText(undefined), "");
	});

	test("league events sort after a day's games", () => {
		const game = eventsFromGame(gameFor())[0]!;
		const event = eventFromLeagueEvent(
			{ eid: 1, type: "trade", season: 2013 },
			5,
		)!;
		assert.strictEqual(event.order > game.order, true);
	});
});

describe("assignEventDays", () => {
	test("events keep their order and land on days that have games", () => {
		const days = [1, 2, 3, 4];
		const assigned = assignEventDays([10, 20, 30, 40, 50, 60, 70, 80], days);
		const placed = [...assigned.values()];
		assert.strictEqual(
			placed.every((d) => days.includes(d)),
			true,
		);
		// Monotonic: a later event never lands on an earlier day.
		for (let i = 1; i < placed.length; i++) {
			assert.strictEqual(placed[i]! >= placed[i - 1]!, true);
		}
	});

	test("the same input always places the same way", () => {
		assert.deepStrictEqual(
			[...assignEventDays([1, 2, 3], [7, 8]).entries()],
			[...assignEventDays([1, 2, 3], [7, 8]).entries()],
		);
	});

	test("a season with no games yet puts everything on one stretch", () => {
		const assigned = assignEventDays([1, 2, 3], []);
		assert.deepStrictEqual([...assigned.values()], [0, 0, 0]);
	});

	test("no events is not an error", () => {
		assert.strictEqual(assignEventDays([], [1, 2]).size, 0);
	});

	test("days are used in ascending order however they arrive", () => {
		const assigned = assignEventDays([1, 2, 3, 4], [9, 3, 6]);
		assert.strictEqual([...assigned.values()][0], 3);
	});
});

describe("trimDayEvents", () => {
	const ev = (id: string, salience: number, order: number): SocialEvent => ({
		id,
		type: "gameResult",
		topic: "gameResult",
		season: 2013,
		day: 1,
		order,
		salience,
		tids: [],
		pids: [],
		facts: {},
	});

	test("keeps the most salient and returns them in stream order", () => {
		const trimmed = trimDayEvents(
			[ev("g:1", 0.2, 1), ev("g:2", 0.9, 2), ev("g:3", 0.5, 3)],
			{ limit: 2 },
		);
		assert.deepStrictEqual(
			trimmed.map((e) => e.id),
			["g:2", "g:3"],
		);
	});

	test("one game can never crowd out the rest of the night", () => {
		// Fifteen posts about one result is the redundancy the whole design is
		// trying to avoid, so the cap bites before the limit does.
		const events = [
			ev("g:1", 0.99, 1),
			ev("perf:1:10", 0.98, 2),
			ev("perf:1:11", 0.97, 3),
			ev("perf:1:12", 0.96, 4),
			ev("g:2", 0.3, 5),
		];
		const trimmed = trimDayEvents(events, { limit: 5, maxPerGame: 3 });
		assert.strictEqual(trimmed.filter((e) => e.id.includes("1")).length, 3);
		assert.strictEqual(
			trimmed.some((e) => e.id === "g:2"),
			true,
		);
	});

	test("non-game events are never capped by the per-game rule", () => {
		const events = [
			ev("e:1", 0.5, 10),
			ev("e:2", 0.5, 11),
			ev("e:3", 0.5, 12),
			ev("e:4", 0.5, 13),
		];
		assert.strictEqual(
			trimDayEvents(events, { limit: 10, maxPerGame: 1 }).length,
			4,
		);
	});

	test("a quiet day is returned whole", () => {
		const events = [ev("g:1", 0.4, 1), ev("g:2", 0.4, 2)];
		assert.strictEqual(trimDayEvents(events, { limit: 30 }).length, 2);
	});

	test("ties break on order, so the trim is deterministic", () => {
		const events = [ev("g:2", 0.5, 2), ev("g:1", 0.5, 1)];
		assert.deepStrictEqual(
			trimDayEvents(events, { limit: 1 }).map((e) => e.id),
			["g:1"],
		);
	});
});

// STABLE PLACEMENT. The news log records no day, so the days are recovered
// from the box scores where they can be and interpolated where they cannot.
// What matters is that a day, once placed, stays placed as the season grows.
describe("placeLeagueEvents", () => {
	type G = Parameters<typeof placeLeagueEvents>[0]["games"][number];
	const game = (
		gid: number,
		day: number,
		tids: [number, number],
		extra: {
			playoffs?: boolean;
			rows?: { pid: number; tid: number; hurt?: boolean }[];
		} = {},
	): G => ({
		gid,
		day,
		playoffs: extra.playoffs === true,
		teams: tids.map((tid) => ({
			tid,
			players: (extra.rows ?? [])
				.filter((row) => row.tid === tid)
				.map((row) => ({ pid: row.pid, injuryNew: row.hurt })),
		})),
	});

	test("an injury lands on the game it happened in", () => {
		const games = [
			game(1, 1, [0, 1], { rows: [{ pid: 7, tid: 0 }] }),
			game(2, 3, [0, 2], { rows: [{ pid: 7, tid: 0, hurt: true }] }),
			game(3, 5, [0, 3], { rows: [] }),
		];
		const placed = placeLeagueEvents({
			events: [
				{
					eid: 10,
					type: "injured",
					text: "x was injured.",
					pids: [7],
					tids: [0],
				},
			],
			games,
			offseason: false,
		});
		assert.strictEqual(placed.get(10), 3);
	});

	test("a second injury to the same player pairs with his second hurt game", () => {
		const games = [
			game(1, 2, [0, 1], { rows: [{ pid: 7, tid: 0, hurt: true }] }),
			game(2, 9, [0, 2], { rows: [{ pid: 7, tid: 0, hurt: true }] }),
		];
		const placed = placeLeagueEvents({
			events: [
				{ eid: 1, type: "injured", pids: [7], tids: [0] },
				{ eid: 2, type: "injured", pids: [7], tids: [0] },
			],
			games,
			offseason: false,
		});
		assert.strictEqual(placed.get(1), 2);
		assert.strictEqual(placed.get(2), 9);
	});

	test("a trade lands on the traded player's first game for his new team", () => {
		const games = [
			game(1, 1, [0, 1], { rows: [{ pid: 7, tid: 0 }] }),
			game(2, 4, [0, 1], { rows: [{ pid: 7, tid: 0 }] }),
			game(3, 6, [1, 2], { rows: [{ pid: 7, tid: 1 }] }),
			game(4, 8, [1, 2], { rows: [{ pid: 7, tid: 1 }] }),
		];
		const placed = placeLeagueEvents({
			events: [{ eid: 5, type: "trade", pids: [7], tids: [0, 1] }],
			games,
			offseason: false,
		});
		assert.strictEqual(placed.get(5), 6);
	});

	test("news between anchors is interpolated, and snapped to a real game day", () => {
		const games = [
			game(1, 1, [0, 1], { rows: [{ pid: 7, tid: 0, hurt: true }] }),
			game(2, 4, [0, 1]),
			game(3, 7, [0, 1]),
			game(4, 10, [0, 1], { rows: [{ pid: 8, tid: 0, hurt: true }] }),
		];
		const placed = placeLeagueEvents({
			events: [
				{ eid: 1, type: "injured", pids: [7] },
				{ eid: 2, type: "award", text: "somebody won something" },
				{ eid: 3, type: "injured", pids: [8] },
			],
			games,
			offseason: false,
		});
		assert.strictEqual(placed.get(1), 1);
		assert.strictEqual(placed.get(3), 10);
		const mid = placed.get(2)!;
		assert.ok([4, 7].includes(mid), `expected a middle game day, got ${mid}`);
	});

	test("the past does not move when the season grows", () => {
		// The whole reason this exists. The even spread it replaces moved every
		// past event each time a day was played.
		const early = [
			game(1, 1, [0, 1], { rows: [{ pid: 7, tid: 0, hurt: true }] }),
			game(2, 3, [0, 1]),
			game(3, 5, [0, 1]),
			game(4, 7, [0, 1], { rows: [{ pid: 8, tid: 0, hurt: true }] }),
			game(5, 9, [0, 1]),
		];
		const events = [
			{ eid: 1, type: "injured", pids: [7] },
			{ eid: 2, type: "award", text: "a" },
			{ eid: 3, type: "award", text: "b" },
			{ eid: 4, type: "injured", pids: [8] },
		];
		const before = placeLeagueEvents({
			events,
			games: early,
			offseason: false,
		});

		const later = [
			...early,
			game(6, 11, [0, 1]),
			game(7, 13, [0, 1]),
			game(8, 15, [0, 1], { rows: [{ pid: 9, tid: 0, hurt: true }] }),
		];
		const moreEvents = [
			...events,
			{ eid: 5, type: "award", text: "c" },
			{ eid: 6, type: "injured", pids: [9] },
			{ eid: 7, type: "award", text: "d" },
		];
		const after = placeLeagueEvents({
			events: moreEvents,
			games: later,
			offseason: false,
		});

		for (const event of events) {
			assert.strictEqual(
				after.get(event.eid),
				before.get(event.eid),
				`event ${event.eid} moved from ${before.get(event.eid)} to ${after.get(event.eid)}`,
			);
		}
	});

	test("a playoff series result lands on the series' last game", () => {
		const games = [
			game(1, 80, [0, 1]),
			game(2, 90, [0, 3], { playoffs: true }),
			game(3, 92, [0, 3], { playoffs: true }),
			game(4, 94, [0, 3], { playoffs: true }),
			game(5, 96, [0, 3], { playoffs: true }),
		];
		const placed = placeLeagueEvents({
			events: [
				{
					eid: 1,
					type: "playoffs",
					text: "X made a game-winning shot in game 2 of the 1st round.",
					tids: [0, 3],
				},
				{
					eid: 2,
					type: "playoffs",
					text: "The X defeated the Y in the 1st round, 4-0.",
					tids: [0, 3],
				},
			],
			games,
			offseason: false,
		});
		assert.strictEqual(placed.get(1), 92);
		assert.strictEqual(placed.get(2), 96);
	});

	test("after the title, news belongs to the offseason", () => {
		const games = [
			game(1, 80, [0, 1]),
			game(2, 99, [0, 3], { playoffs: true }),
		];
		const events = [
			{
				eid: 1,
				type: "playoffs",
				text: "The X finished in 1st place and are league champions!",
				tids: [0],
			},
			{ eid: 2, type: "award", text: "MVP" },
			{ eid: 3, type: "draft", text: "drafted" },
		];
		const during = placeLeagueEvents({ events, games, offseason: false });
		assert.strictEqual(during.get(1), 99);
		assert.strictEqual(during.get(3), 99);
		const after = placeLeagueEvents({ events, games, offseason: true });
		assert.strictEqual(after.get(1), 99);
		assert.strictEqual(after.get(2), OFFSEASON_DAY);
		assert.strictEqual(after.get(3), OFFSEASON_DAY);
	});
});
