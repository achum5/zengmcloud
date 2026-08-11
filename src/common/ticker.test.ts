import { assert, describe, test } from "vitest";
import {
	buildTickerStream,
	tickerDurationSeconds,
	tickerMayUpdate,
} from "./ticker.ts";

const game = (gid: number, final = true) => ({ gid, final });
const news = (eid: number) => ({ eid, text: `event ${eid}` });

describe("buildTickerStream", () => {
	test("today's scores lead, news follows", () => {
		const items = buildTickerStream({
			games: [game(1), game(2)],
			news: [news(10), news(11)],
		});
		assert.deepStrictEqual(
			items.map((i) => i.type),
			["game", "game", "news", "news"],
		);
	});

	test("news is capped, because an offseason day logs hundreds", () => {
		const items = buildTickerStream({
			games: [],
			news: Array.from({ length: 200 }, (_, i) => news(i)),
			maxNews: 25,
		});
		assert.strictEqual(items.length, 25);
	});

	test("the same event never appears twice in one pass", () => {
		const items = buildTickerStream({
			games: [],
			news: [news(1), news(1), news(2)],
		});
		assert.deepStrictEqual(
			items.map((i) => (i.type === "news" ? i.eid : undefined)),
			[1, 2],
		);
	});

	test("an empty league produces an empty ticker rather than a broken one", () => {
		assert.deepStrictEqual(buildTickerStream({ games: [], news: [] }), []);
	});

	test("scores alone are enough - news is not required", () => {
		const items = buildTickerStream({ games: [game(1)], news: [] });
		assert.strictEqual(items.length, 1);
		assert.strictEqual(items[0]!.type, "game");
	});
});

// The spoiler rule. Getting this wrong doesn't look like a bug, it looks like
// the app telling you the score of the game you are currently watching.
describe("tickerMayUpdate", () => {
	test("frozen while you are watching your own live sim", () => {
		assert.strictEqual(
			tickerMayUpdate({ liveGameInProgress: true, watchingBroadcast: false }),
			false,
		);
	});

	test("frozen while following someone else's broadcast", () => {
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
