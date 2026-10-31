import { project, type Camera } from "../court3d/camera.ts";
import type { Play } from "./director.ts";
import { FIELD_W } from "./geometry.ts";

// CONFETTI for the championship won (the 3D court's, for football: see
// court3d/scene.ts): down out of the top of the stadium over the middle of
// the field, where they all are, from a moment after the final whistle - in
// the winners' colors, white and gold - fluttering as it falls, and lying
// where it lands. Each piece is its own from the moment it was let go, so
// the same instant always looks the same.

export type Confetti = { t: number; x: number; colors: string[] };

const CONFETTI = 3200;
// Let go this high (feet), this long after the game is over (ms), the last
// of it this much later; over this far along the field either way of where
// the head coaches meet, and all the way across it and a little past.
const FROM_Z = 50;
const AFTER = 400;
const SPREAD_MS = 12_000;
const ALONG = 65;
const PAST = 8;
// Each piece this big (feet).
const PIECE = 0.6;

export const confettiFor = (
	play: Play | undefined,
	title: boolean,
	colors: [string[] | undefined, string[] | undefined],
): Confetti | undefined => {
	const post = play?.postgame;
	if (!title || !post || post.winner === undefined) {
		return undefined;
	}
	const own = colors[post.winner]?.slice(0, 2) ?? [];
	return {
		t: post.from + AFTER,
		x: post.at.x,
		colors: [...own, "#ffffff", "#f5cf4a"],
	};
};

const unitHash = (a: number, b: number): number => {
	let h =
		Math.imul((a | 0) ^ 0x9e3779b9, 0x85ebca6b) ^ Math.imul(b + 1, 0xc2b2ae35);
	h ^= h >>> 15;
	h = Math.imul(h, 0x2c1b3c6d);
	h ^= h >>> 12;
	return (h >>> 0) / 2 ** 32;
};

// Those still in the air (over everything), or those down on the turf
// (under the men).
export const drawConfetti = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	c: Confetti,
	t: number,
	falling: boolean,
) => {
	if (t < c.t) {
		return;
	}
	for (let k = 0; k < CONFETTI; k++) {
		const dt = (t - c.t - unitHash(k, 1) * SPREAD_MS) / 1000;
		if (dt < 0) {
			continue;
		}
		const v = 4 + unitHash(k, 4) * 4;
		const down = FROM_Z / v;
		const air = dt < down;
		if (air !== falling) {
			continue;
		}
		const u = Math.min(dt, down);
		const x =
			c.x - ALONG + unitHash(k, 2) * ALONG * 2 + Math.sin(u * 2.3 + k) * 1.8;
		const y =
			-PAST +
			unitHash(k, 3) * (FIELD_W + PAST * 2) +
			Math.cos(u * 1.7 + k) * 1.2;
		const z = Math.max(0, FROM_Z - v * u);
		const p = project(cam, { x, y, z });
		if (p.x < -4 || p.y < -4 || p.x > cam.viewW + 4 || p.y > cam.viewH + 4) {
			continue;
		}
		const size = Math.max(1.2, p.k * PIECE);
		// Turning over as it falls: edge on, then flat.
		const w = air ? size * Math.max(0.2, Math.abs(Math.sin(dt * 7 + k))) : size;
		ctx.fillStyle = c.colors[k % c.colors.length]!;
		ctx.fillRect(p.x - w / 2, p.y - size / 2, w, air ? size : size * 0.6);
	}
};
