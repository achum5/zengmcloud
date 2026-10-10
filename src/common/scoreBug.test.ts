import { assert, describe, test } from "vitest";
import {
	parseScoreBug,
	SCORE_BUG_PRESETS,
	scoreBugPictureIds,
} from "./scoreBug.ts";

const ok = {
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
