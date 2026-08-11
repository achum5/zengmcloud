import { assert, describe, test } from "vitest";
import {
	buildTickerStream,
	TICKER_LIMITS,
	tickerDurationSeconds,
	tickerMayUpdate,
	type TickerItem,
} from "./ticker.ts";

const score = (gid: number): TickerItem => ({
	type: "score",
	key: `score-${gid}`,
	gid,
	season: 2026,
	boxScoreTeam: `BOS_1`,
	away: { tid: 2, abbrev: "NYC", pts: 100 },
	home: { tid: 1, abbrev: "BOS", pts: 105 },
});
const upcoming = (gid: number): TickerItem => ({
	type: "upcoming",
	key: `up-${gid}`,
	away: { tid: 2, abbrev: "NYC" },
	home: { tid: 1, abbrev: "BOS" },
	line: "BOS -3.5",
});
const perf = (n: number): TickerItem => ({
	type: "performance",
	key: `perf-${n}`,
	gid: n,
	season: 2026,
	boxScoreTeam: "BOS_1",
	pid: n,
	name: "Some Player",
	stat: "40 PTS",
	game: "NYC 100-105 BOS",
});
const race = (n: number): TickerItem => ({
	type: "race",
	key: `race-${n}`,
	label: "MVP",
	entries: [{ pid: n, name: "Someone", odds: "+180" }],
});
const news = (eid: number): TickerItem => ({
	type: "news",
	key: `news-${eid}`,
	eid,
	text: `event ${eid}`,
});

describe("buildTickerStream", () => {
	// Everything the ticker shows is league-wide. The first version reused the
	// user's own games array and turned into their personal game log crawling
	// past, which is the bug these sections exist to make impossible.
	test("runs in sections: scores, upcoming, performances, races, news", () => {
		const items = buildTickerStream([
			news(1),
			race(1),
			perf(1),
			upcoming(1),
			score(1),
		]);
		assert.deepStrictEqual(
			items.map((i) => i.type),
			["score", "upcoming", "performance", "race", "news"],
		);
	});

	test("each section is capped, so one busy kind cannot crowd out the rest", () => {
		const items = buildTickerStream([
			...Array.from({ length: 500 }, (_, i) => news(i)),
			score(1),
			race(1),
		]);
		assert.strictEqual(
			items.filter((i) => i.type === "news").length,
			TICKER_LIMITS.news,
		);
		// The single score and race still make it in, behind 500 events.
		assert.strictEqual(items.filter((i) => i.type === "score").length, 1);
		assert.strictEqual(items.filter((i) => i.type === "race").length, 1);
	});

	test("nothing appears twice in one pass", () => {
		const items = buildTickerStream([score(1), score(1), score(2)]);
		assert.strictEqual(items.length, 2);
	});

	test("an empty league produces an empty ticker rather than a broken one", () => {
		assert.deepStrictEqual(buildTickerStream([]), []);
	});

	test("one section alone is enough", () => {
		assert.strictEqual(buildTickerStream([news(1), news(2)]).length, 2);
	});
});

// The spoiler rule. Getting this wrong does not look like a bug, it looks like
// the app telling you the score of the game you are currently watching.
describe("tickerMayUpdate", () => {
	test("frozen while you are watching your own live sim", () => {
		assert.strictEqual(
			tickerMayUpdate({ liveGameInProgress: true, watchingBroadcast: false }),
			false,
		);
	});

	test("frozen while following another player's broadcast", () => {
		assert.strictEqual(
			tickerMayUpdate({ liveGameInProgress: false, watchingBroadcast: true }),
			false,
		);
	});

	test("runs normally the rest of the time", () => {
		assert.strictEqual(
			tickerMayUpdate({ liveGameInProgress: false, watchingBroadcast: false }),
			true,
		);
	});
});

describe("tickerDurationSeconds", () => {
	test("scales with how much there is to show", () => {
		assert.ok(tickerDurationSeconds(40) > tickerDurationSeconds(10));
	});

	test("a nearly empty ticker still scrolls slowly enough to read", () => {
		assert.ok(tickerDurationSeconds(1) >= 20);
		assert.ok(tickerDurationSeconds(0) >= 20);
	});
});
