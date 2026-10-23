import { project, type Camera } from "../court3d/camera.ts";
import type { Pt3 } from "./geometry.ts";
import type { Move } from "./poses.ts";
import type { FieldPlayerState } from "./sprite.ts";

// THE SHADOWS ON THE TURF: as the court has them, a soft dark pool under
// every man on his feet, and under the ball on its own - smaller and fainter
// as either gets up off the ground; under a man down on the turf, the length
// of him.

export type Pool = { x: number; y: number; r: number; k: number };

// How big round (feet), and how dark at the middle.
const MAN_R = 1.35;
const BALL_R = 0.45;
const LYING_R = 1.15;
const DARK = 0.48;
// A man down: pools this far apart along him (feet).
const LENGTH = [-1.7, 0, 1.7];

const LYING = new Set<Move>(["prone", "supine", "hurt", "hurtProne"]);
const FALLING = new Set<Move>(["tackled", "tackledBack", "dive"]);
const RISING = new Set<Move>(["getUp", "getUpBack"]);
const isDown = (st: FieldPlayerState) =>
	LYING.has(st.move) ||
	(FALLING.has(st.move) && st.phase > 0.55) ||
	(RISING.has(st.move) && st.phase < 0.3);

export const poolsOf = (states: FieldPlayerState[], ball?: Pt3): Pool[] => {
	const out: Pool[] = [];
	for (const st of states) {
		if (isDown(st)) {
			const c = Math.cos(st.yaw);
			const s = Math.sin(st.yaw);
			for (const f of LENGTH) {
				out.push({ x: st.x + c * f, y: st.y + s * f, r: LYING_R, k: 0.8 });
			}
			continue;
		}
		const lift = Math.min(1, Math.max(0, st.z) / 4);
		out.push({
			x: st.x,
			y: st.y,
			r: MAN_R * (1 - lift * 0.35),
			k: 1 - lift * 0.6,
		});
	}
	if (ball) {
		out.push({
			x: ball.x,
			y: ball.y,
			r: BALL_R,
			k: Math.max(0, 1 - ball.z / 14) * 0.9,
		});
	}
	return out;
};

// One pool, drawn once and stamped.
let blob: HTMLCanvasElement | undefined;
const blobOf = () => {
	if (!blob) {
		const n = 64;
		blob = document.createElement("canvas");
		blob.width = n;
		blob.height = n;
		const g = blob.getContext("2d")!;
		const grad = g.createRadialGradient(n / 2, n / 2, 0, n / 2, n / 2, n / 2);
		grad.addColorStop(0, `rgba(0,0,0,${DARK})`);
		grad.addColorStop(0.6, `rgba(0,0,0,${DARK * 0.6})`);
		grad.addColorStop(1, "rgba(0,0,0,0)");
		g.fillStyle = grad;
		g.fillRect(0, 0, n, n);
	}
	return blob;
};

export const drawPools = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	pools: Pool[],
) => {
	const img = blobOf();
	const alpha = ctx.globalAlpha;
	for (const p of pools) {
		if (p.k <= 0.01) {
			continue;
		}
		const c = project(cam, { x: p.x, y: p.y, z: 0 });
		const rx = p.r * c.k;
		if (
			c.k <= 0 ||
			c.x + rx < 0 ||
			c.x - rx > cam.viewW ||
			c.y < -rx ||
			c.y > cam.viewH + rx
		) {
			continue;
		}
		const near = project(cam, { x: p.x, y: p.y + p.r, z: 0 });
		const far = project(cam, { x: p.x, y: p.y - p.r, z: 0 });
		const ry = Math.max(0.5, Math.abs(near.y - far.y) / 2);
		ctx.globalAlpha = alpha * Math.min(1, p.k);
		ctx.drawImage(img, c.x - rx, c.y - ry, rx * 2, ry * 2);
	}
	ctx.globalAlpha = alpha;
};
