import { depthOf, project, type Camera } from "../court3d/camera.ts";
import { FIGURE_SCALE } from "./body.ts";
import { shade } from "./color.ts";
import { BENCH_OFF, FIELD_W, type Pt3, type Side } from "./geometry.ts";
import { BENCHES, netX, sidelineOf } from "./sideline.ts";

// WHAT STANDS ON THE SIDELINES: each team's benches - long, dark, a seat in
// the team's color - and the kickers' net down at the end of the team
// area, a square of black netting on a frame.

// (Dark, a rail along the top of its back in the team's color.)
const BACK = "#2a2c31";
// (Seat and back as high as the men sat on them, see body.ts.)
const SEAT_Z = 1.6 * FIGURE_SCALE;
const SEAT_D = 1.5;
const BACK_Z = 3.3 * FIGURE_SCALE;
const NET_W = 7;
const NET_H = 8;
// The net stands this far off the sideline, at its near edge.
const NET_OFF = 14;

type Item = { depth: number; draw: () => void };
type P = { x: number; y: number };

const quad = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	pts: Pt3[],
	fill: string,
): P[] => {
	const q = pts.map((p) => project(cam, p));
	ctx.fillStyle = fill;
	ctx.beginPath();
	ctx.moveTo(q[0]!.x, q[0]!.y);
	for (const p of q.slice(1)) {
		ctx.lineTo(p.x, p.y);
	}
	ctx.closePath();
	ctx.fill();
	return q;
};

export const sidelineGear = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	colors: [string, string],
): Item[] => {
	const items: Item[] = [];
	for (const team of [0, 1] as Side[]) {
		const line = sidelineOf(team);
		const y = (off: number) => (line === 0 ? -off : FIELD_W + off);
		const front = y(BENCH_OFF);
		const back = y(BENCH_OFF + SEAT_D);
		const main = colors[team];
		const frame = "#1c1d21";
		for (const [x0, x1] of BENCHES) {
			const mid = (x0 + x1) / 2;
			// The seat, drawn before anyone sat on it; its back, in its place
			// - behind him across the field, in front of him on the near side.
			const far = Math.max(
				depthOf(cam, { x: mid, y: front, z: 0 }),
				depthOf(cam, { x: mid, y: back, z: 0 }),
			);
			if (far <= 1) {
				continue;
			}
			items.push(
				{
					depth: far + 0.6,
					draw: () => {
						quad(
							ctx,
							cam,
							[
								{ x: x0, y: front, z: SEAT_Z },
								{ x: x1, y: front, z: SEAT_Z },
								{ x: x1, y: front, z: 0 },
								{ x: x0, y: front, z: 0 },
							],
							frame,
						);
						quad(
							ctx,
							cam,
							[
								{ x: x0, y: front, z: SEAT_Z },
								{ x: x1, y: front, z: SEAT_Z },
								{ x: x1, y: back, z: SEAT_Z },
								{ x: x0, y: back, z: SEAT_Z },
							],
							shade(main, -0.3),
						);
					},
				},
				{
					depth: depthOf(cam, { x: mid, y: back, z: 0 }),
					draw: () => {
						quad(
							ctx,
							cam,
							[
								{ x: x0, y: back, z: BACK_Z },
								{ x: x1, y: back, z: BACK_Z },
								{ x: x1, y: back, z: SEAT_Z },
								{ x: x0, y: back, z: SEAT_Z },
							],
							BACK,
						);
						quad(
							ctx,
							cam,
							[
								{ x: x0, y: back, z: BACK_Z + 0.25 },
								{ x: x1, y: back, z: BACK_Z + 0.25 },
								{ x: x1, y: back, z: BACK_Z },
								{ x: x0, y: back, z: BACK_Z },
							],
							main,
						);
					},
				},
			);
		}
		// The net.
		const nx = netX(line);
		const n0 = y(NET_OFF);
		const n1 = y(NET_OFF + NET_W);
		const at = { x: nx, y: (n0 + n1) / 2, z: 0 };
		if (depthOf(cam, at) > 1) {
			items.push({
				depth: depthOf(cam, at),
				draw: () => {
					drawNet(ctx, cam, nx, n0, n1);
				},
			});
		}
	}
	return items;
};

const drawNet = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	x: number,
	y0: number,
	y1: number,
) => {
	ctx.save();
	const q = quad(
		ctx,
		cam,
		[
			{ x, y: y0, z: NET_H },
			{ x, y: y1, z: NET_H },
			{ x, y: y1, z: 0 },
			{ x, y: y0, z: 0 },
		],
		"rgba(16, 16, 18, 0.42)",
	);
	const k = project(cam, { x, y: y0, z: 0 }).k;
	ctx.strokeStyle = "#151518";
	ctx.lineWidth = Math.max(1, 0.2 * k);
	ctx.beginPath();
	ctx.moveTo(q[3]!.x, q[3]!.y);
	ctx.lineTo(q[0]!.x, q[0]!.y);
	ctx.lineTo(q[1]!.x, q[1]!.y);
	ctx.lineTo(q[2]!.x, q[2]!.y);
	ctx.stroke();
	// Its mesh, if near enough to see.
	if (k > 4) {
		ctx.globalAlpha = 0.35;
		ctx.lineWidth = 0.5;
		ctx.beginPath();
		for (let i = 1; i < 8; i++) {
			const u = i / 8;
			const a = project(cam, { x, y: y0 + (y1 - y0) * u, z: 0 });
			const b = project(cam, { x, y: y0 + (y1 - y0) * u, z: NET_H });
			ctx.moveTo(a.x, a.y);
			ctx.lineTo(b.x, b.y);
			const c = project(cam, { x, y: y0, z: NET_H * u });
			const d = project(cam, { x, y: y1, z: NET_H * u });
			ctx.moveTo(c.x, c.y);
			ctx.lineTo(d.x, d.y);
		}
		ctx.stroke();
	}
	ctx.restore();
};
