import { assert, describe, test } from "vitest";
import {
	FOOTBALL_SCORE_BUG_PRESETS,
	mapScoreBugURLs,
	parseScoreBug,
	SCORE_BUG_PRESETS,
	scoreBugForGame,
	scoreBugPictureIds,
} from "./scoreBug.ts";
import type { ScoreBugStyle } from "./types.ts";

const ok: ScoreBugStyle = {
	width: 400,
	height: 50,
	pieces: [{ show: "clock", x: 0, y: 0, w: 60, h: 50 }],
};

describe("parseScoreBug", () => {
	test("takes every preset as it is", () => {
		for (const { name, style } of SCORE_BUG_PRESETS) {
			assert.deepStrictEqual(parseScoreBug(style), style, name);
		}
	});

	test("football's: the down and distance for the shot clock, no fouls, all inside", () => {
		assert.strictEqual(
			FOOTBALL_SCORE_BUG_PRESETS.length,
			SCORE_BUG_PRESETS.length,
		);
		for (const { name, style } of FOOTBALL_SCORE_BUG_PRESETS) {
			assert.deepStrictEqual(parseScoreBug(style), style, name);
			const shows = style.pieces.map((p) => p.show);
			assert.include(shows, "down", name);
			for (const gone of ["shotClock", "awayFouls", "homeFouls"] as const) {
				assert.notInclude(shows, gone, name);
			}
			for (const p of style.pieces) {
				assert.isAtMost(p.x + p.w, style.width, `${name} ${p.show}`);
				assert.isAtMost(p.y + p.h, style.height, `${name} ${p.show}`);
			}
		}
	});

	test("takes a small bug with its options", () => {
		const bug = {
			...ok,
			span: 0.5,
			place: "left",
			image: "pic:3",
			pieces: [
				{
					show: "text",
					x: 0,
					y: 0,
					w: 10,
					h: 10,
					text: "LIVE",
					align: "center",
					italic: true,
					color: "home0",
				},
			],
		};
		assert.deepStrictEqual(parseScoreBug(bug), bug as unknown);
	});

	test("takes a broadcast's look: fonts, team colors in CSS, shapes", () => {
		const bug = {
			...ok,
			shadow: "none",
			font: "Broadcast Sans",
			fonts: [{ name: "Broadcast Sans", src: "pic:7" }],
			pieces: [
				{
					show: "homeScore",
					x: 0,
					y: 0,
					w: 60,
					h: 50,
					background: "linear-gradient(90deg, home0, home1)",
					border: "2px solid home2",
					clip: "polygon(8% 0, 100% 0, 92% 100%, 0 100%)",
					shadow: "0 0.05em 0.1em rgba(0,0,0,.6)",
					blend: "multiply",
					spacing: 1.5,
					upper: true,
				},
				{
					show: "image",
					x: 0,
					y: 0,
					w: 10,
					h: 10,
					image: "pic:8",
					fit: "fill",
				},
			],
		};
		assert.deepStrictEqual(parseScoreBug(bug), bug as unknown);
	});

	test("takes what the broadcasts' bugs need", () => {
		const bug = {
			...ok,
			bottom: 0.07,
			edge: 0.026,
			place: "left",
			pieces: [
				{
					show: "awayBonus",
					x: 0,
					y: 0,
					w: 6,
					h: 22,
					rotate: -90,
					text: "BONUS",
				},
				{ show: "homeLogo", x: 0, y: 0, w: 30, h: 22, zoom: 1.2 },
				{ show: "period", x: 0, y: 0, w: 20, h: 20, ordinal: "sup" },
				{ show: "clock", x: 0, y: 0, w: 20, h: 20, colon: true },
				{
					show: "shotClock",
					x: 0,
					y: 0,
					w: 20,
					h: 20,
					colon: true,
					low: "#fff",
					lowBackground: "#d3202f",
				},
				{
					show: "awayFouls",
					x: 0,
					y: 0,
					w: 20,
					h: 20,
					text: "FOULS: {n}",
					limit: "FOUL LIMIT",
				},
				{
					show: "homeTimeouts",
					x: 0,
					y: 0,
					w: 20,
					h: 4,
					off: "transparent",
					spacing: 2,
				},
			],
		};
		assert.deepStrictEqual(parseScoreBug(bug), bug as unknown);
	});

	test("says what's wrong", () => {
		const bad: [unknown, RegExp][] = [
			[[], /object/],
			[{ ...ok, extra: 1 }, /Unknown field "extra"/],
			[{ ...ok, width: 0 }, /positive/],
			[{ ...ok, span: 2 }, /span/],
			[{ ...ok, place: "top" }, /place/],
			[{ ...ok, pieces: "no" }, /pieces/],
			[{ ...ok, pieces: [{ ...ok.pieces[0], show: "x" }] }, /piece 1: "show"/],
			[{ ...ok, pieces: [{ ...ok.pieces[0], size: "big" }] }, /"size"/],
			[{ ...ok, pieces: [{ ...ok.pieces[0], align: "up" }] }, /"align"/],
			[{ ...ok, pieces: [{ ...ok.pieces[0], foo: 1 }] }, /unknown field/],
			[{ ...ok, pieces: [{ ...ok.pieces[0], upper: "yes" }] }, /"upper"/],
			[{ ...ok, pieces: [{ ...ok.pieces[0], fit: "zoom" }] }, /"fit"/],
			[{ ...ok, pieces: [{ ...ok.pieces[0], ordinal: "big" }] }, /"ordinal"/],
			[{ ...ok, pieces: [{ ...ok.pieces[0], colon: 1 }] }, /"colon"/],
			[{ ...ok, bottom: "low" }, /"bottom"/],
			[{ ...ok, fonts: [{ name: "A" }] }, /font 1/],
			[{ ...ok, fonts: "A" }, /"fonts"/],
			[
				{ ...ok, pieces: [{ show: "clock", x: 0, y: 0, w: 1 }] },
				/"h" is needed/,
			],
		];
		for (const [raw, re] of bad) {
			const out = parseScoreBug(raw);
			assert.strictEqual(typeof out, "string", JSON.stringify(raw));
			assert.match(out as string, re);
		}
	});
});

test("scoreBugPictureIds lists the uploaded pictures", () => {
	assert.deepStrictEqual(scoreBugPictureIds(null), []);
	assert.deepStrictEqual(
		scoreBugPictureIds({
			...ok,
			image: "pic:4",
			pieces: [
				{ show: "image", x: 0, y: 0, w: 1, h: 1, image: "pic:9" },
				{ show: "image", x: 0, y: 0, w: 1, h: 1, image: "https://x/y.png" },
			],
		}),
		["4", "9"],
	);
});

test("scoreBugPictureIds lists the fonts uploaded too", () => {
	assert.deepStrictEqual(
		scoreBugPictureIds({
			...ok,
			fonts: [
				{ name: "A", src: "pic:5" },
				{ name: "B", src: "https://x/b.woff2" },
			],
		}),
		["5"],
	);
});

test("mapScoreBugURLs rewrites every picture and font, dropping what's gone", async () => {
	const bug: ScoreBugStyle = {
		...ok,
		image: "pic:1",
		fonts: [
			{ name: "A", src: "pic:2" },
			{ name: "B", src: "pic:gone" },
		],
		pieces: [
			{ show: "clock", x: 0, y: 0, w: 60, h: 50 },
			{ show: "image", x: 0, y: 0, w: 1, h: 1, image: "pic:gone" },
			{ show: "image", x: 0, y: 0, w: 1, h: 1, image: "pic:3" },
		],
	};
	const out = await mapScoreBugURLs(bug, async (u) =>
		u === "pic:gone" ? undefined : `url-${u}`,
	);
	assert.strictEqual(out.image, "url-pic:1");
	assert.deepStrictEqual(out.fonts, [{ name: "A", src: "url-pic:2" }]);
	assert.deepStrictEqual(out.pieces, [
		bug.pieces[0],
		{ show: "image", x: 0, y: 0, w: 1, h: 1 },
		{ show: "image", x: 0, y: 0, w: 1, h: 1, image: "url-pic:3" },
	]);
	// (The bug itself untouched.)
	assert.strictEqual(bug.image, "pic:1");
});

describe("scoreBugForGame", () => {
	const league = { ...ok, width: 1 };
	const homeBug = { ...ok, width: 2 };
	const awayBug = { ...ok, width: 3 };

	test("your team's broadcast when it plays", () => {
		const teams = [
			{ tid: 1, scoreBug: homeBug },
			{ tid: 2, scoreBug: awayBug },
		];
		assert.strictEqual(scoreBugForGame(league, teams, 2), awayBug);
		assert.strictEqual(scoreBugForGame(league, teams, 1), homeBug);
	});

	test("the home team's otherwise, then the league's, then the default", () => {
		assert.strictEqual(
			scoreBugForGame(
				league,
				[
					{ tid: 1, scoreBug: homeBug },
					{ tid: 2, scoreBug: awayBug },
				],
				9,
			),
			homeBug,
		);
		assert.strictEqual(
			scoreBugForGame(league, [{ tid: 1, scoreBug: homeBug }, { tid: 2 }], 2),
			homeBug,
		);
		assert.strictEqual(
			scoreBugForGame(league, [{ tid: 1 }, { tid: 2 }], 1),
			league,
		);
		assert.strictEqual(
			scoreBugForGame(null, [{ tid: 1 }, { tid: 2 }], 1),
			null,
		);
	});
});
