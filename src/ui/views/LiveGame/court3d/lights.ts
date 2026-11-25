import {
	END_WALL,
	LED_WALL,
	NEAR_RIBBON,
	NEAR_WALL,
	type Plane,
	RIBBON,
	TABLE_FRONT,
} from "./arena.ts";
import { depthOf, project, type Camera } from "./camera.ts";
import type { PlayerState } from "./evaluate.ts";
import { callAt, calledBy, darkAt, type Intro } from "./intro.ts";

// THE LIGHTS DOWN for the starting lineups (see intro.ts): the whole building
// dark but for a spotlight on the man being called - a pool on the floor
// round his feet and the beam coming down to it - and a softer one on each
// man already out in his team's line. For the home team, and for everybody
// in the playoffs, colored lights sweep the floor too. The boards stay lit
// in the dark - the ribbons round the decks, the boards along the front of
// the stands, the table's - the way they glow in a real building with the
// lights down. And in the pregame show (see pregameShow.ts), whatever the
// building has going off lights the floor round it: the fire, the beams,
// the one light on the logo at center court.

// The darkness is drawn into a layer of its own and the lit places cut out
// of it, then laid over the picture.
let layer: HTMLCanvasElement | undefined;
const layerFor = (w: number, h: number): HTMLCanvasElement => {
	if (!layer) {
		layer = document.createElement("canvas");
	}
	if (layer.width !== w || layer.height !== h) {
		layer.width = w;
		layer.height = h;
	}
	return layer;
};

// How dark it gets (the darkness layer's opacity, all the way down).
const DARK = 0.84;
// The spotlight's pool on the floor, and how far round a man the light
// takes in his whole body (feet).
const POOL_R = 4.3;
const BODY_R = 1.6;
const HEAD_Z = 7.6;
// The one light on the logo at center court (feet across).

// An ellipse on the floor round a point - a circle `r` feet across, as
// the camera sees it - filled from the middle out.
const floorPool = (
	c: CanvasRenderingContext2D,
	cam: Camera,
	x: number,
	y: number,
	r: number,
	inner: string,
	outer: string,
) => {
	const p = project(cam, { x, y, z: 0 });
	const far = project(cam, { x, y: y - r, z: 0 });
	const rx = Math.max(1, p.k * r);
	const ry = Math.max(1, Math.abs(p.y - far.y));
	c.save();
	c.translate(p.x, p.y);
	c.scale(1, ry / rx);
	const g = c.createRadialGradient(0, 0, 0, 0, 0, rx);
	g.addColorStop(0, inner);
	g.addColorStop(0.55, inner);
	g.addColorStop(1, outer);
	c.fillStyle = g;
	c.beginPath();
	c.arc(0, 0, rx, 0, Math.PI * 2);
	c.fill();
	c.restore();
};

// The light taking in a man standing there, head to toe.
const bodyGlow = (
	c: CanvasRenderingContext2D,
	cam: Camera,
	x: number,
	y: number,
	inner: string,
	outer: string,
) => {
	const feet = project(cam, { x, y, z: 0 });
	const head = project(cam, { x, y, z: HEAD_Z });
	const rx = Math.max(1, feet.k * BODY_R);
	const ry = Math.max(1, Math.abs(feet.y - head.y) * 0.58);
	c.save();
	c.translate(feet.x, (feet.y + head.y) / 2);
	c.scale(1, ry / rx);
	const g = c.createRadialGradient(0, 0, 0, 0, 0, rx);
	g.addColorStop(0, inner);
	g.addColorStop(0.6, inner);
	g.addColorStop(1, outer);
	c.fillStyle = g;
	c.beginPath();
	c.arc(0, 0, rx, 0, Math.PI * 2);
	c.fill();
	c.restore();
};

// The beam from the rafters down to his pool.
const beam = (
	c: CanvasRenderingContext2D,
	cam: Camera,
	x: number,
	y: number,
	alpha: number,
) => {
	const p = project(cam, { x, y, z: 0 });
	const r = Math.max(1, p.k * POOL_R * 0.8);
	const top = Math.min(p.y - 1, 0);
	// Down from a little toward the middle of the picture.
	const lean = (cam.viewW / 2 - p.x) * 0.18;
	const g = c.createLinearGradient(0, top, 0, p.y);
	g.addColorStop(0, `rgba(255, 244, 220, 0)`);
	g.addColorStop(1, `rgba(255, 244, 220, ${alpha})`);
	c.fillStyle = g;
	c.beginPath();
	c.moveTo(p.x + lean - r * 0.18, top);
	c.lineTo(p.x + lean + r * 0.18, top);
	c.lineTo(p.x + r, p.y);
	c.lineTo(p.x - r, p.y);
	c.closePath();
	c.fill();
};

const rgba = (hex: string, a: number): string => {
	const h = hex.replace("#", "");
	const full =
		h.length === 3
			? h
					.split("")
					.map((ch) => ch + ch)
					.join("")
			: h.padEnd(6, "0");
	const n = Number.parseInt(full.slice(0, 6), 16);
	if (!Number.isFinite(n)) {
		return `rgba(255,255,255,${a})`;
	}
	return `rgba(${(n >> 16) & 255}, ${(n >> 8) & 255}, ${n & 255}, ${a})`;
};

// The LED boards, cut out of the dark: each a strip at a time, any of it
// behind the camera left out.
const BOARDS: Plane[] = [
	RIBBON,
	NEAR_RIBBON,
	LED_WALL,
	NEAR_WALL,
	END_WALL[0],
	END_WALL[1],
	TABLE_FRONT,
];
const BOARD_STRIPS = 14;
const cutBoards = (c: CanvasRenderingContext2D, cam: Camera, a: number) => {
	c.fillStyle = `rgba(0,0,0,${a})`;
	for (const b of BOARDS) {
		const across = {
			x: b.alongX.x * b.w,
			y: b.alongX.y * b.w,
			z: b.alongX.z * b.w,
		};
		const down = {
			x: b.alongY.x * b.h,
			y: b.alongY.y * b.h,
			z: b.alongY.z * b.h,
		};
		c.beginPath();
		for (let i = 0; i < BOARD_STRIPS; i++) {
			const at = (u: number, v: number) => ({
				x: b.origin.x + across.x * u + down.x * v,
				y: b.origin.y + across.y * u + down.y * v,
				z: b.origin.z + across.z * u + down.z * v,
			});
			const u0 = i / BOARD_STRIPS;
			const u1 = (i + 1) / BOARD_STRIPS;
			const pts = [at(u0, 0), at(u1, 0), at(u1, 1), at(u0, 1)];
			if (pts.some((q) => depthOf(cam, q) < 1)) {
				continue;
			}
			const sp = pts.map((q) => project(cam, q));
			c.moveTo(sp[0]!.x, sp[0]!.y);
			for (let k = 1; k < 4; k++) {
				c.lineTo(sp[k]!.x, sp[k]!.y);
			}
			c.closePath();
		}
		c.fill();
	}
};

// The colored lights sweeping the floor: where each is at t.
const SWEEPS = 3;
const sweepAt = (i: number, t: number) => ({
	x: 47 + 36 * Math.sin(t / 1400 + i * 2.1),
	y: 24 + 17 * Math.sin(t / 1900 + i * 1.3 + 0.7),
});

export const drawLights = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	intro: Intro | undefined,
	t: number,
	players: PlayerState[],
	// Each side's colors [road, home], for the sweeping lights.
	colors: [string, string][],
) => {
	const dark = darkAt(intro, t);
	if (!intro || dark <= 0.002) {
		return;
	}
	const W = ctx.canvas.width;
	const H = ctx.canvas.height;
	const lc = layerFor(W, H).getContext("2d");
	if (!lc) {
		return;
	}
	const call = callAt(intro, t);
	const out = new Set(calledBy(intro, t).map((c) => c.pid));
	// All ten out, waiting for the lights.
	const allOut = t >= (intro.calls.at(-1)?.t1 ?? Infinity);
	const hype = intro.big || call?.team === 1 || allOut;
	const strength = (pid: number): number =>
		pid === call?.pid ? 1 : out.has(pid) ? (allOut ? 0.8 : 0.5) : 0;

	lc.setTransform(1, 0, 0, 1, 0, 0);
	lc.globalCompositeOperation = "source-over";
	lc.clearRect(0, 0, W, H);
	lc.fillStyle = `rgba(4, 3, 10, ${DARK * dark})`;
	lc.fillRect(0, 0, W, H);
	lc.globalCompositeOperation = "destination-out";
	for (const st of players) {
		const s = strength(st.pid);
		if (s <= 0) {
			continue;
		}
		floorPool(
			lc,
			cam,
			st.x,
			st.y,
			POOL_R * (s === 1 ? 1 : 0.75),
			`rgba(0,0,0,${s})`,
			"rgba(0,0,0,0)",
		);
		bodyGlow(lc, cam, st.x, st.y, `rgba(0,0,0,${s})`, "rgba(0,0,0,0)");
	}
	if (hype) {
		for (let i = 0; i < SWEEPS; i++) {
			const at = sweepAt(i, t);
			floorPool(lc, cam, at.x, at.y, 5.5, "rgba(0,0,0,0.45)", "rgba(0,0,0,0)");
		}
	}
	cutBoards(lc, cam, 0.82);
	ctx.save();
	ctx.setTransform(1, 0, 0, 1, 0, 0);
	ctx.drawImage(layerFor(W, H), 0, 0);

	// And the light itself, added: the beam and the warm pool on the floor
	// under the man called; the sweeping lights in the team's colors.
	ctx.globalCompositeOperation = "lighter";
	const now = call && players.find((p) => p.pid === call.pid);
	if (now) {
		beam(ctx, cam, now.x, now.y, 0.16 * dark);
		floorPool(
			ctx,
			cam,
			now.x,
			now.y,
			POOL_R,
			`rgba(255, 238, 205, ${0.2 * dark})`,
			"rgba(255, 238, 205, 0)",
		);
	}
	if (hype) {
		const team = call?.team ?? 1;
		const pal = colors[team] ?? colors[1] ?? ["#ffffff", "#ffffff"];
		for (let i = 0; i < SWEEPS; i++) {
			const at = sweepAt(i, t);
			const c = pal[i % 2] ?? "#ffffff";
			floorPool(ctx, cam, at.x, at.y, 5.5, rgba(c, 0.32 * dark), rgba(c, 0));
		}
	}
	ctx.restore();
};
