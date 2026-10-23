import { assert, describe, test } from "vitest";
import { stamp, type Kept } from "./sprite.ts";

// A picture that only remembers what was drawn on it: how strongly, which
// way up, about which line, and from which rows of the sprite to where.
type Draw = {
	alpha: number;
	flip: number;
	line: number;
	sy: number;
	sh: number;
	// The top and bottom of it on the picture.
	top: number;
	bottom: number;
};
const recorder = () => {
	const draws: Draw[] = [];
	let m = { d: 1, f: 0 };
	const ctx = {
		globalAlpha: 1,
		imageSmoothingEnabled: true,
		getTransform: () => ({ ...m }),
		setTransform: (x: { d: number; f: number }) => {
			m = { d: x.d, f: x.f };
		},
		transform: (...t: number[]) => {
			m = { d: m.d * t[3]!, f: m.f + m.d * t[5]! };
		},
		drawImage: (img: { height: number }, ...a: number[]) => {
			const [sy, sh, dy, dh] =
				a.length === 8
					? [a[1]!, a[3]!, a[5]!, a[7]!]
					: [0, img.height, a[1]!, a[3]!];
			const y0 = m.f + m.d * dy;
			const y1 = m.f + m.d * (dy + dh);
			draws.push({
				alpha: ctx.globalAlpha,
				flip: m.d,
				line: m.f / 2,
				sy,
				sh,
				top: Math.min(y0, y1),
				bottom: Math.max(y0, y1),
			});
		},
	};
	return { ctx, draws };
};

// A man 40 px tall, the sprite's last 2 rows under his feet.
const kept = (): Kept => ({
	canvas: { width: 20, height: 40 } as HTMLCanvasElement,
	w: 20,
	h: 40,
	dx: -10,
	dy: -38,
});

describe("3D polished floor", () => {
	// Everybody on the floor shows in it: upside down under his feet, drawn
	// before him (so he stands over it), from his feet up and fading away
	// from the floor - and the picture left as it was for whatever is next.
	test("a man's reflection: upside down under his feet, fading, under him", () => {
		const { ctx, draws } = recorder();
		const k = kept();
		const floorY = 100;
		stamp(ctx as unknown as CanvasRenderingContext2D, k, 50, floorY, 1, {
			alpha: 0.24,
			floorY,
		});
		const him = draws.at(-1)!;
		const shine = draws.slice(0, -1);
		assert.isAtLeast(shine.length, 1);
		// Him, last and as he is.
		assert.strictEqual(him.alpha, 1);
		assert.strictEqual(him.flip, 1);
		assert.strictEqual(him.top, floorY + k.dy);
		// His reflection, upside down about the floor under him - his feet
		// first, then on up him, each band fainter.
		let rows = k.h;
		let alpha = 0.24 + 1e-9;
		for (const d of shine) {
			assert.strictEqual(d.flip, -1);
			assert.strictEqual(d.line, floorY);
			assert.strictEqual(d.sy + d.sh, rows);
			rows = d.sy;
			assert.isBelow(d.alpha, alpha);
			alpha = d.alpha;
			assert.isAtLeast(d.top, floorY - 2);
		}
		assert.strictEqual(ctx.globalAlpha, 1);
		assert.deepEqual(ctx.getTransform(), { d: 1, f: 0 });
	});

	test("up in the air, he shows as far under the floor as he is over it", () => {
		const { ctx, draws } = recorder();
		const k = kept();
		const floorY = 100;
		const lift = 12;
		stamp(ctx as unknown as CanvasRenderingContext2D, k, 50, floorY - lift, 1, {
			alpha: 0.24,
			floorY,
		});
		const soles = floorY - lift + k.dy + k.h;
		assert.strictEqual(draws[0]!.top, floorY + (floorY - soles));
	});
});
