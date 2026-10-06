import { assert, describe, test } from "vitest";
import { STORES } from "../../../../worker/db/Cache.ts";
import { SIM_INERT_STORES } from "../../../../worker/core/sync/publishGuards.ts";
import { serializeUniform } from "../../../../common/uniform.ts";
import { kitsFor } from "./figure.ts";
import {
	ART_H,
	ART_JERSEY,
	ART_PANELS,
	ART_SHORTS,
	ART_SWATCHES,
	ART_W,
	artAt,
	artColor,
	artScaleOf,
	dressKit,
	kitArtFromPixels,
} from "./kitArt.ts";
import { bodyOf } from "./poses.ts";
import { wrapsFor } from "./sculpt.ts";

const at = (
	wrap: ReturnType<typeof wrapsFor>["jersey"],
	U: number,
	S: number,
	F: number,
	notSide = false,
) => {
	const out = new Float64Array(2);
	artAt(wrap, U, S, F, notSide, out);
	return { x: out[0]!, y: out[1]! };
};

const inside = (x: number, p: { x: number; w: number }) =>
	x >= p.x && x < p.x + p.w;

describe("uniforms drawn from a picture", () => {
	const body = bodyOf();
	const { jersey, shorts } = wrapsFor(body);
	const chest = body.torso * 0.6;

	test("each side of him comes from its own panel, the right way round", () => {
		// Facing out, his chest from the middle of the front panel.
		const mid = at(jersey, chest, 0, body.depth * 0.5);
		assert.closeTo(mid.x, ART_PANELS.front.x + ART_PANELS.front.w / 2, 1);
		// His right (S < 0) on the left of the front panel, as you look at him.
		assert.isBelow(at(jersey, chest, -0.2, body.depth * 0.5).x, mid.x);
		// His back from the back panel, his left (S > 0) on its left: seen
		// from behind.
		const back = at(jersey, chest, 0, -body.depth * 0.5);
		assert.isTrue(inside(back.x, ART_PANELS.back));
		assert.isBelow(at(jersey, chest, 0.2, -body.depth * 0.5).x, back.x);
		// His sides from the side panels.
		assert.isTrue(inside(at(jersey, chest, -jersey.a, 0).x, ART_PANELS.right));
		assert.isTrue(inside(at(jersey, chest, jersey.a, 0).x, ART_PANELS.left));
		// The inside of a leg of his shorts never from a side panel.
		const inner = at(shorts, -0.5, -0.02, 0.01, true);
		assert.isTrue(inside(inner.x, ART_PANELS.front));
	});

	test("the panels meet where his front turns into his sides", () => {
		// Just either side of the turn: the front panel's edge, then the
		// side panel's.
		const r = Math.hypot(jersey.a, jersey.b);
		const before = jersey.turn - 0.001;
		const after = jersey.turn + 0.001;
		const point = (phi: number) =>
			at(jersey, chest, -jersey.a * Math.sin(phi), jersey.b * Math.cos(phi));
		assert.isAbove(r, 0);
		assert.closeTo(point(before).x, ART_PANELS.front.x, 1);
		assert.closeTo(
			point(after).x,
			ART_PANELS.right.x + ART_PANELS.right.w - 1,
			1,
		);
	});

	test("top to bottom: shoulders to the top of the jersey's band, hems to the bottom", () => {
		assert.closeTo(at(jersey, jersey.top, 0, 1).y, ART_JERSEY.y, 0.01);
		assert.closeTo(
			at(jersey, jersey.bottom, 0, 1).y,
			ART_JERSEY.y + ART_JERSEY.h - 1,
			0.01,
		);
		assert.closeTo(at(shorts, shorts.top, 0, 1).y, ART_SHORTS.y, 0.01);
		assert.closeTo(
			at(shorts, shorts.bottom - 1, 0, 1).y,
			ART_SHORTS.y + ART_SHORTS.h - 1,
			0.01,
		);
	});

	test("a picture is read: its swatches, its guides gone, the wrong shape refused", () => {
		assert.strictEqual(artScaleOf(ART_W, ART_H), 1);
		assert.strictEqual(artScaleOf(ART_W * 2, ART_H * 2), 2);
		assert.isUndefined(artScaleOf(ART_W * 5, ART_H * 5));
		assert.isUndefined(artScaleOf(ART_W, ART_W));
		assert.isUndefined(
			kitArtFromPixels(new Uint8ClampedArray(4 * 10 * 10), 10, 10, "x"),
		);

		const data = new Uint8ClampedArray(ART_W * ART_H * 4);
		const paint = (
			x0: number,
			y0: number,
			w: number,
			h: number,
			rgba: [number, number, number, number],
		) => {
			for (let y = y0; y < y0 + h; y++) {
				for (let x = x0; x < x0 + w; x++) {
					data.set(rgba, (y * ART_W + x) * 4);
				}
			}
		};
		// A green jersey with a magenta guide line left down the middle of
		// it, and a guide box left round nothing out in a corner.
		paint(0, ART_JERSEY.y, ART_W, ART_JERSEY.h, [0, 120, 60, 255]);
		paint(80, ART_JERSEY.y, 1, ART_JERSEY.h, [255, 0, 255, 255]);
		paint(0, ART_SHORTS.y, 10, 10, [255, 0, 255, 255]);
		// Gold numbers; the other swatches left clear.
		paint(
			ART_SWATCHES.number,
			ART_SWATCHES.y,
			ART_SWATCHES.size,
			ART_SWATCHES.size,
			[240, 180, 20, 255],
		);
		const art = kitArtFromPixels(data, ART_W, ART_H, "x")!;
		assert.strictEqual(art.number, "#f0b414");
		assert.isUndefined(art.numberEdge);
		assert.isUndefined(art.trim);
		const out = new Float64Array(3);
		// The guide line takes the green either side of it.
		assert.closeTo(artColor(art, 80, 40, out), 1, 1e-9);
		assert.deepEqual([...out], [0, 120, 60]);
		// The guide box out where nothing was painted is clear.
		assert.strictEqual(artColor(art, 4, ART_SHORTS.y + 4, out), 0);

		const kit = kitsFor(undefined, undefined)[0];
		const dressed = dressKit(kit, art);
		assert.strictEqual(dressed.number, "#f0b414");
		assert.strictEqual(dressed.numberEdge, kit.numberEdge);
		assert.strictEqual(dressKit(kit, undefined), kit);
	});

	test("the pictures are kept with the league: synced, and never in a sim's way", () => {
		assert.include(STORES, "jerseySkins");
		assert.isTrue(SIM_INERT_STORES.has("jerseySkins"));
	});
});

describe("a team's own jersey on the 2.5D floor", () => {
	const colors: [string, string, string] = ["#007a33", "#ba9653", "#ffffff"];

	test("a dark custom jersey is worn away, its colors at home", () => {
		const jersey = serializeUniform({
			base: "#111111",
			collar: [{ color: "#ffd700", width: 8 }],
			number: { color: "#ffd700", outline: "#ffffff" },
			shorts: { side: "#ff0000" },
			wordmark: { text: "Gold", color: "#ffd700" },
		});
		const [away, home] = kitsFor({ colors, jersey }, { colors, jersey });
		assert.strictEqual(away.jersey, "#111111");
		assert.strictEqual(away.trim, "#ffd700");
		assert.strictEqual(away.number, "#ffd700");
		assert.strictEqual(away.numberEdge, "#ffffff");
		assert.strictEqual(away.stripe, "#ff0000");
		assert.strictEqual(away.chestText, "Gold");
		// At home: white, in the custom jersey's color.
		assert.notStrictEqual(home.jersey, "#111111");
		assert.strictEqual(home.trim, "#111111");
		assert.isUndefined(home.chestText);
	});

	test("a light custom jersey is worn at home", () => {
		const jersey = serializeUniform({ base: "#fafafa" });
		const [away, home] = kitsFor({ colors, jersey }, { colors, jersey });
		assert.strictEqual(home.jersey, "#fafafa");
		assert.notStrictEqual(away.jersey, "#fafafa");
	});

	test("no custom jersey: the usual uniforms", () => {
		const [away, home] = kitsFor({ colors }, { colors, jersey: "jersey3" });
		const [away2, home2] = kitsFor({ colors }, { colors });
		assert.deepEqual(away, away2);
		assert.deepEqual(home, home2);
	});
});
