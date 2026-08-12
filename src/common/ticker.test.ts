import { assert, describe, test } from "vitest";
import {
	buildTickerSegments,
	buildTickerStream,
	segmentDurationSeconds,
	SEGMENT_MAX_SECONDS,
	SEGMENT_MIN_SECONDS,
	segmentTravelPx,
	TICKER_LIMITS,
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
const perfFor = (gid: number, n: number): TickerItem => ({
	type: "performance",
	key: `perf-${gid}-${n}`,
	gid,
	season: 2026,
	boxScoreTeam: "BOS_1",
	pid: n,
	name: "Some Player",
	stat: "40 PTS",
	game: "NYC 100-105 BOS",
});
const perf = (n: number): TickerItem => perfFor(n, n);
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

// THE BLOCKS. A ticker that is one undifferentiated crawl needs a label on
// every item; one that runs in blocks needs it once, in the pane. These are the
// blocks.
describe("buildTickerSegments", () => {
	test("an empty feed makes no blocks at all", () => {
		assert.deepStrictEqual(buildTickerSegments([]), []);
	});

	test("all the finals go in one Scores block", () => {
		const segments = buildTickerSegments([score(1), score(2), score(3)]);
		assert.strictEqual(segments.length, 1);
		assert.deepStrictEqual(segments[0]!.header, {
			kind: "label",
			text: "Scores",
		});
		assert.strictEqual(segments[0]!.items.length, 3);
	});

	// The thing this exists for: the pane holds the score of one game while that
	// game's stat lines go past beside it.
	test("a game's lines become their own block, with the score in the pane", () => {
		const segments = buildTickerSegments([
			score(7),
			perfFor(7, 1),
			perfFor(7, 2),
		]);
		const game = segments.find((s) => s.key === "game-7");
		assert.ok(game, "no block for the game");
		assert.deepStrictEqual(game.header, {
			kind: "final",
			away: { tid: 2, abbrev: "NYC", pts: 100 },
			home: { tid: 1, abbrev: "BOS", pts: 105 },
		});
		assert.strictEqual(game.items.length, 2);
	});

	test("lines from different games do not share a block", () => {
		const segments = buildTickerSegments([
			score(7),
			score(8),
			perfFor(7, 1),
			perfFor(8, 2),
		]);
		assert.ok(segments.some((s) => s.key === "game-7"));
		assert.ok(segments.some((s) => s.key === "game-8"));
	});

	// A line can outlive its score - the scores are capped, the lines are picked
	// separately - and a block with no score still has to say something.
	test("lines with no score fall back to a named block", () => {
		const segments = buildTickerSegments([perfFor(9, 1)]);
		assert.deepStrictEqual(segments[0]!.header, {
			kind: "label",
			text: "Top Performers",
		});
	});

	test("each award is its own block, named for the award", () => {
		const mvp: TickerItem = {
			type: "race",
			key: "race-mvp",
			label: "MVP",
			entries: [{ pid: 1, name: "Someone", odds: "+180" }],
		};
		const dpoy: TickerItem = { ...mvp, key: "race-dpoy", label: "DPOY" };
		const segments = buildTickerSegments([mvp, dpoy]);
		assert.deepStrictEqual(
			segments.map((s) => s.header),
			[
				{ kind: "label", text: "MVP" },
				{ kind: "label", text: "DPOY" },
			],
		);
	});

	test("news is blocked by category, not run together", () => {
		const segments = buildTickerSegments([
			{ ...news(1), category: "transaction" },
			{ ...news(2), category: "injury" },
			{ ...news(3), category: "transaction" },
		]);
		assert.strictEqual(segments.length, 2);
		// Two transactions together, the injury on its own.
		assert.deepStrictEqual(
			segments.map((s) => s.items.length),
			[2, 1],
		);
	});

	test("a block is never empty", () => {
		for (const segment of buildTickerSegments([
			score(1),
			upcoming(1),
			perfFor(1, 1),
			news(1),
		])) {
			assert.ok(segment.items.length > 0, `${segment.key} is empty`);
		}
	});

	test("every block key is distinct, so the player can key on it", () => {
		const segments = buildTickerSegments([
			score(1),
			score(2),
			perfFor(1, 1),
			perfFor(2, 2),
			upcoming(1),
			news(1),
		]);
		assert.strictEqual(
			new Set(segments.map((s) => s.key)).size,
			segments.length,
		);
	});
});

// How far a block travels. Running it in from off-screen and out the other side
// left the bar empty for seconds at both ends of every block; travelling only
// the overflow keeps it full.
describe("segmentTravelPx", () => {
	test("a block wider than the bar travels its overflow", () => {
		assert.strictEqual(segmentTravelPx(1400, 1000), 400);
	});

	test("a block that already fits does not move", () => {
		assert.strictEqual(segmentTravelPx(600, 1000), 0);
	});

	test("unmeasurable geometry moves nothing", () => {
		assert.strictEqual(segmentTravelPx(Number.NaN, 1000), 0);
	});
});

describe("segmentDurationSeconds", () => {
	test("longer blocks take longer", () => {
		assert.ok(segmentDurationSeconds(3000) > segmentDurationSeconds(1000));
	});

	// A block that fits still has to be readable before the next one arrives.
	test("a block that does not move still gets its dwell", () => {
		assert.strictEqual(segmentDurationSeconds(0), SEGMENT_MIN_SECONDS);
	});

	test("nothing is left on screen forever", () => {
		assert.strictEqual(segmentDurationSeconds(500_000), SEGMENT_MAX_SECONDS);
	});
});
