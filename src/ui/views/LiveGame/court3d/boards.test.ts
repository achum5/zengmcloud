import { assert, describe, test } from "vitest";
import { drawHoop } from "./arena.ts";
import { JUMBO_STYLES, jumboShape, paintScoreBar } from "./jumbotron.ts";
import { makeCamera, REPLAY_RIG } from "./camera.ts";
import { COURT_W } from "./geometry.ts";

// A picture that only remembers its boxes and its words: where each went,
// in what color, and how wide each word was.
type Box = { x: number; y: number; w: number; h: number; fill: string };
type Word = { text: string; x: number; w: number; fill: string };
const recorder = () => {
	const boxes: Box[] = [];
	const words: Word[] = [];
	// (Where it has been moved to, and how squeezed.)
	let at = 0;
	let wide = 1;
	const kept: [number, number][] = [];
	const ctx = {
		fillStyle: "" as unknown,
		font: "10px Arial",
		textAlign: "left",
		textBaseline: "alphabetic",
		fillRect(x: number, y: number, w: number, h: number) {
			boxes.push({ x, y, w, h, fill: String(ctx.fillStyle) });
		},
		// (Lettering about six tenths as wide as it is tall.)
		measureText(text: string) {
			const size = Number(/(\d+)px/.exec(ctx.font)?.[1] ?? 10);
			return { width: text.length * size * 0.6 };
		},
		fillText(text: string, x: number) {
			words.push({
				text,
				x: at + x,
				w: ctx.measureText(text).width * wide,
				fill: String(ctx.fillStyle),
			});
		},
		save() {
			kept.push([at, wide]);
		},
		restore() {
			[at, wide] = kept.pop() ?? [0, 1];
		},
		translate(x: number) {
			at += x;
		},
		scale(x: number) {
			wide *= x;
		},
		createLinearGradient: () => ({ addColorStop: () => {} }),
		drawImage() {},
	};
	return { ctx, boxes, words };
};

const away = { abbrev: "bos", colors: ["#007a33", "#ffffff", "#000000"] };
const home = { abbrev: "lal", colors: ["#552583", "#fdb927", "#000000"] };

describe("the board over center court", () => {
	// Round its top, the score: each side's name in its colors - the road
	// team first - its score beside it in white on black, the period and the
	// clock between. Three figures fit in the box.
	test("each side's score beside it, road team first, the clock between", () => {
		const { ctx, boxes, words } = recorder();
		paintScoreBar(
			ctx as unknown as CanvasRenderingContext2D,
			360,
			60,
			{ home: home as never, away: away as never, logos: [] },
			{ score: [87, 103], clock: "2:31", period: "Q4" },
		);
		const said = words.map((w) => w.text);
		assert.sameMembers(said, ["BOS", "87", "LAL", "103", "Q4", "2:31"]);
		const x = (t: string) => words.find((w) => w.text === t)!.x;
		assert.isBelow(x("BOS"), x("87"));
		assert.isBelow(x("87"), x("2:31"));
		assert.isBelow(x("2:31"), x("103"));
		assert.isBelow(x("103"), x("LAL"));
		for (const w of words) {
			const box = boxes.find((b) => b.x <= w.x && w.x <= b.x + b.w && b.h > 30);
			assert.isDefined(box, w.text);
			assert.isAtMost(w.w, box!.w * 0.92, w.text);
		}
		for (const pts of ["87", "103"]) {
			const w = words.find((v) => v.text === pts)!;
			assert.strictEqual(w.fill, "#ffffff", pts);
			const box = boxes.findLast(
				(b) => b.x <= w.x && w.x <= b.x + b.w && b.h > 30,
			)!;
			assert.strictEqual(box.fill, "#000000", pts);
		}

		// Over a break, the word in place of the clock - and no period.
		const half = recorder();
		paintScoreBar(
			half.ctx as unknown as CanvasRenderingContext2D,
			360,
			60,
			{ home: home as never, away: away as never, logos: [] },
			{ score: [50, 48], clock: "HALF", period: "Q2" },
		);
		assert.include(
			half.words.map((w) => w.text),
			"HALF",
		);
		assert.notInclude(
			half.words.map((w) => w.text),
			"Q2",
		);
	});

	// Each building its own: whichever shape, the board hangs over center
	// court, clear of the floor, and the side facing the main camera shows
	// its big screen.
	test("hung over center court, its screen toward the camera", () => {
		for (const style of JUMBO_STYLES) {
			const shape = jumboShape(style);
			assert.approximately((shape.x0 + shape.x1) / 2, COURT_W / 2, 0.01, style);
			assert.isAbove(shape.z0, 20, style);
			const main = shape.tiers.find((t) =>
				t.sides.some((s) => "pic" in s && s.pic === "mainLong"),
			)!;
			// (The first side reads left to right along the court, facing the
			// near sideline.)
			const [a, b] = main.foot;
			assert.isAbove(b!.x, a!.x, style);
			assert.isAbove(a!.y, 25, style);
			const first = main.sides[0]!;
			assert.isTrue("pic" in first && first.pic === "mainLong", style);
		}
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
