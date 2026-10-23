import { assert, describe, test } from "vitest";
import { drawHoop, JUMBO_FRONT, paintJumboStrip } from "./arena.ts";
import { makeCamera, REPLAY_RIG } from "./camera.ts";
import { COURT_W } from "./geometry.ts";

// A picture that only remembers its boxes and its words: where each went,
// in what color, and how wide each word was.
type Box = { x: number; y: number; w: number; h: number; fill: string };
type Word = { text: string; x: number; w: number; fill: string };
const recorder = () => {
	const boxes: Box[] = [];
	const words: Word[] = [];
	const ctx = {
		fillStyle: "",
		font: "10px Arial",
		textAlign: "left",
		textBaseline: "alphabetic",
		fillRect(x: number, y: number, w: number, h: number) {
			boxes.push({ x, y, w, h, fill: ctx.fillStyle });
		},
		// (Lettering about six tenths as wide as it is tall.)
		measureText(text: string) {
			const size = Number(/(\d+)px/.exec(ctx.font)?.[1] ?? 10);
			return { width: text.length * size * 0.6 };
		},
		fillText(text: string, x: number) {
			words.push({
				text,
				x,
				w: ctx.measureText(text).width,
				fill: ctx.fillStyle,
			});
		},
	};
	return { ctx, boxes, words };
};

const away = { abbrev: "bos", colors: ["#007a33", "#ffffff", "#000000"] };
const home = { abbrev: "lal", colors: ["#552583", "#fdb927", "#000000"] };

describe("the board over center court", () => {
	// Along its foot, each side in its colors - the road team first - and,
	// as the game goes, its score beside it in white on black, the way the
	// board in a building carries it. Three figures fit in the box.
	test("each side's score beside it, road team first", () => {
		const { ctx, boxes, words } = recorder();
		paintJumboStrip(
			ctx as unknown as CanvasRenderingContext2D,
			home as never,
			away as never,
			[87, 103],
		);
		const said = words.map((w) => w.text);
		assert.deepEqual(said, ["BOS", "87", "LAL", "103"]);
		for (const w of words) {
			const box = boxes.find(
				(b) => b.x <= w.x && w.x <= b.x + b.w && b.y >= JUMBO_FRONT.h * 0.7,
			)!;
			assert.isDefined(box, w.text);
			assert.isAtMost(w.w, box.w * 0.9, w.text);
			if (/^\d+$/.test(w.text)) {
				assert.strictEqual(box.fill, "#050507", w.text);
				assert.strictEqual(w.fill, "#ffffff", w.text);
			}
		}
		// Each side's name on its own color.
		assert.isTrue(boxes.some((b) => b.fill === "#007a33"));
		assert.isTrue(boxes.some((b) => b.fill === "#552583"));
		assert.isBelow(words[0]!.x, words[2]!.x);

		// No score yet (the warm-ups): the names alone.
		const before = recorder();
		paintJumboStrip(
			before.ctx as unknown as CanvasRenderingContext2D,
			home as never,
			away as never,
		);
		assert.deepEqual(
			before.words.map((w) => w.text),
			["BOS", "LAL"],
		);
	});
});

// A picture that only remembers the colors it stroked lines in.
const strokes = () => {
	const seen: string[] = [];
	const state: Record<string, unknown> = {
		strokeStyle: "",
		fillStyle: "",
		lineWidth: 1,
	};
	const ctx = new Proxy(state, {
		get: (target, key: string) => {
			if (key in target) {
				return target[key];
			}
			if (key === "stroke") {
				return () => seen.push(String(target.strokeStyle));
			}
			if (key === "measureText") {
				return () => ({ width: 0 });
			}
			if (key.startsWith("create")) {
				return () => ({ addColorStop: () => {} });
			}
			return () => {};
		},
		set: (target, key: string, v) => {
			target[key] = v;
			return true;
		},
	});
	return { ctx: ctx as unknown as CanvasRenderingContext2D, seen };
};

describe("the backboard at the horn", () => {
	// The period's clock run out, the lights round the glass are lit red -
	// and only then.
	test("lit red round the glass once the clock is out", () => {
		const cam = makeCamera(
			{ x: COURT_W - 8, width: 26, y: 25, z: 6 },
			1280,
			720,
			REPLAY_RIG,
		);
		const red = (out: boolean) => {
			const { ctx, seen } = strokes();
			drawHoop(ctx, cam, 1, "#8c1d40", "", {
				swish: 0,
				clank: 0,
				dunk: 0,
				out,
				t: 0,
			});
			return seen.filter((c) => /ff4a32|255, 52, 36/.test(c)).length;
		};
		assert.strictEqual(red(false), 0);
		assert.isAtLeast(red(true), 2);
	});
});
