import { assert, test } from "vitest";
import { drawHoop } from "./arena.ts";
import { makeCamera, MAIN_RIG, REPLAY_RIG, type Camera } from "./camera.ts";
import { COURT_W } from "./geometry.ts";

// The shot clock's seconds are on its sides, the way a broadcast shows them
// from the sideline - not flat across the top of the board, seen edge on
// and floating over it - and each reads the right way round, whichever end
// of the floor and wherever the camera is.

type M = [number, number, number, number, number, number];
type Text = { text: string; m: M };
// A picture that only remembers the text drawn on it, and the transform it
// was drawn with (a, b, c, d, e, f).
const recorder = () => {
	const texts: Text[] = [];
	let m: M = [1, 0, 0, 1, 0, 0];
	const stack: M[] = [];
	const mul = ([A, B, C, D, E, F]: M) => {
		const [a, b, c, d, e, f] = m;
		m = [
			a * A + c * B,
			b * A + d * B,
			a * C + c * D,
			b * C + d * D,
			a * E + c * F + e,
			b * E + d * F + f,
		];
	};
	const own: Record<string, unknown> = {
		save: () => stack.push([...m]),
		restore: () => {
			m = stack.pop() ?? m;
		},
		setTransform: (...t: number[]) => {
			m = t.length === 6 ? (t as M) : [1, 0, 0, 1, 0, 0];
		},
		transform: (...t: M) => mul(t),
		translate: (x: number, y: number) => mul([1, 0, 0, 1, x, y]),
		scale: (x: number, y: number) => mul([x, 0, 0, y, 0, 0]),
		measureText: (s: string) => ({ width: s.length * 6 }),
		fillText: (text: string) => texts.push({ text, m: [...m] }),
	};
	const ctx: unknown = new Proxy(own, {
		get: (target, prop: string) =>
			prop in target ? target[prop] : () => ({ addColorStop: () => undefined }),
		set: (target, prop: string, v) => {
			target[prop] = v;
			return true;
		},
	});
	return { ctx: ctx as CanvasRenderingContext2D, texts };
};

const seconds = (cam: Camera, side: 0 | 1) => {
	const { ctx, texts } = recorder();
	drawHoop(ctx, cam, side, "#8c1d40", "14", {
		swish: 0,
		clank: 0,
		dunk: 0,
		t: 0,
	});
	return texts;
};

const readable = (t: Text) => {
	const [a, b, c, d] = t.m;
	// Across it runs left to right, down it runs down - not mirrored, not
	// upside down, not seen edge on.
	assert.isAbove(a, 0.5, JSON.stringify(t.m));
	assert.isAbove(d, 0.5, JSON.stringify(t.m));
	assert.isAbove(a * d - b * c, 0.3, JSON.stringify(t.m));
};

test("the shot clock's seconds read from the sideline, at either end", () => {
	for (const side of [0, 1] as const) {
		const x = side === 0 ? 20 : COURT_W - 20;
		const cam = makeCamera({ x, width: 60, y: 24 }, 1200, 675, MAIN_RIG);
		const texts = seconds(cam, side);
		assert.isAtLeast(texts.length, 1);
		for (const t of texts) {
			assert.strictEqual(t.text, "14");
			readable(t);
		}
	}
});

test("from a replay camera turned toward the basket, they read the right way round", () => {
	for (const side of [0, 1] as const) {
		const x = side === 0 ? 8 : COURT_W - 8;
		const cam = makeCamera({ x, width: 22, y: 25, z: 9 }, 1200, 675, {
			...REPLAY_RIG,
			slide: 0.6,
		});
		const texts = seconds(cam, side);
		assert.isAtLeast(texts.length, 1);
		for (const t of texts) {
			readable(t);
		}
	}
});
