import { depthOf, project, type Camera } from "../court3d/camera.ts";
import { FIGURE_SCALE } from "./body.ts";
import { shade } from "./color.ts";
import { BENCH_OFF, FIELD_W, MID_X, type Pt3, type Side } from "./geometry.ts";
import { drawStandingCooler } from "./shower.ts";
import { BENCHES, NET_OFF, NET_W, netX, sidelineOf } from "./sideline.ts";

// WHAT STANDS ON THE SIDELINES: each team's benches - long, dark, a seat in
// the team's color - a drinks table at either end of them, the coolers and
// a stack of cups on it, and the kickers' net down at the end of the team
// area, a square of black netting on a frame.

// (Dark, a rail along the top of its back in the team's color.)
const BACK = "#2a2c31";
// (Seat and back as high as the men sat on them, see body.ts.)
const SEAT_Z = 1.6 * FIGURE_SCALE;
const SEAT_D = 1.5;
const BACK_Z = 3.3 * FIGURE_SCALE;
const NET_H = 8;
// The drinks tables: from this far along the field (feet off the middle),
// this long, this deep, this high (as high as the men stood at them), a
// cloth over them in the team's color down to the turf; a cooler this far
// along from each end, the cups between, this big round and this high.
const TABLES = [
	[MID_X - 62, MID_X - 56],
	[MID_X + 32, MID_X + 38],
] as const;
const TABLE_D = 2.6;
const TABLE_Z = 2.4 * FIGURE_SCALE;
const TABLE_TOP = "#ecebe6";
const COOLER_IN = 1.2;
const CUPS_R = 0.22 * FIGURE_SCALE;
const CUPS_H = 1.1 * FIGURE_SCALE;
const CUPS = "#2fa84f";
const CUPS_RIM = "#f2f2ee";

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
		for (const [x0, x1] of TABLES) {
			items.push(
				...tableGear(
					ctx,
					cam,
					x0,
					x1,
					y(BENCH_OFF),
					y(BENCH_OFF + TABLE_D),
					main,
				),
			);
		}
		// The net, its face to the field.
		const nx = netX(line);
		const ny = y(NET_OFF);
		const at = { x: nx, y: ny, z: 0 };
		if (depthOf(cam, at) > 1) {
			items.push({
				depth: depthOf(cam, at),
				draw: () => {
					drawNet(ctx, cam, nx - NET_W / 2, nx + NET_W / 2, ny);
				},
			});
		}
	}
	return items;
};

// A drinks table from x0 to x1, its edges at y0 (to the field) and y1:
// its sides, the far ones first, and its top; on it, the two coolers and
// the cups.
const tableGear = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	x0: number,
	x1: number,
	y0: number,
	y1: number,
	cloth: string,
): Item[] => {
	const mid = { x: (x0 + x1) / 2, y: (y0 + y1) / 2, z: 0 };
	const depth = depthOf(cam, mid);
	if (depth <= 1) {
		return [];
	}
	const corner = (x: number, y: number, z: number): Pt3 => ({ x, y, z });
	const sides: [Pt3, Pt3][] = [
		[corner(x0, y0, 0), corner(x1, y0, 0)],
		[corner(x1, y0, 0), corner(x1, y1, 0)],
		[corner(x1, y1, 0), corner(x0, y1, 0)],
		[corner(x0, y1, 0), corner(x0, y0, 0)],
	];
	const at = (x: number): Pt3 => ({ x, y: mid.y, z: TABLE_Z });
	return [
		{
			depth: depth + 0.3,
			draw: () => {
				const far = (s: [Pt3, Pt3]) =>
					depthOf(cam, {
						x: (s[0].x + s[1].x) / 2,
						y: (s[0].y + s[1].y) / 2,
						z: 0,
					});
				for (const [a, b] of [...sides].sort((p, q) => far(q) - far(p))) {
					// (Its ends a little darker than its long sides.)
					const end = a.x === b.x;
					quad(
						ctx,
						cam,
						[{ ...a, z: TABLE_Z }, { ...b, z: TABLE_Z }, b, a],
						shade(cloth, end ? -0.3 : -0.12),
					);
				}
				quad(
					ctx,
					cam,
					[
						corner(x0, y0, TABLE_Z),
						corner(x1, y0, TABLE_Z),
						corner(x1, y1, TABLE_Z),
						corner(x0, y1, TABLE_Z),
					],
					TABLE_TOP,
				);
			},
		},
		...[at(x0 + COOLER_IN), at(x1 - COOLER_IN)].map((p) => ({
			depth: depthOf(cam, { ...p, z: 0 }),
			draw: () => drawStandingCooler(ctx, cam, p),
		})),
		{
			depth: depthOf(cam, mid),
			draw: () => drawCups(ctx, cam, at(mid.x)),
		},
	];
};

// A stack of cups stood up at `p`: a column, its top a white ring.
const drawCups = (ctx: CanvasRenderingContext2D, cam: Camera, p: Pt3) => {
	const lo = project(cam, p);
	const hi = project(cam, { ...p, z: p.z + CUPS_H });
	const w = Math.max(1, CUPS_R * lo.k);
	ctx.fillStyle = CUPS;
	ctx.fillRect(lo.x - w, hi.y, 2 * w, lo.y - hi.y);
	ctx.fillStyle = CUPS_RIM;
	ctx.beginPath();
	ctx.ellipse(hi.x, hi.y, w, Math.max(0.5, w * 0.35), 0, 0, Math.PI * 2);
	ctx.fill();
};

const drawNet = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	x0: number,
	x1: number,
	y: number,
) => {
	ctx.save();
	const q = quad(
		ctx,
		cam,
		[
			{ x: x0, y, z: NET_H },
			{ x: x1, y, z: NET_H },
			{ x: x1, y, z: 0 },
			{ x: x0, y, z: 0 },
		],
		"rgba(16, 16, 18, 0.42)",
	);
	const k = project(cam, { x: x0, y, z: 0 }).k;
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
			const a = project(cam, { x: x0 + (x1 - x0) * u, y, z: 0 });
			const b = project(cam, { x: x0 + (x1 - x0) * u, y, z: NET_H });
			ctx.moveTo(a.x, a.y);
			ctx.lineTo(b.x, b.y);
			const c = project(cam, { x: x0, y, z: NET_H * u });
			const d = project(cam, { x: x1, y, z: NET_H * u });
			ctx.moveTo(c.x, c.y);
			ctx.lineTo(d.x, d.y);
		}
		ctx.stroke();
	}
	ctx.restore();
};
