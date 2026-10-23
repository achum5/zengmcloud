import { depthOf, project, type Camera } from "../court3d/camera.ts";
import { drawTexturedPlane } from "../court3d/planes.ts";
import { drawFieldLines } from "./fieldLines.ts";
import { drawMarks, type Mark } from "./marks.ts";
import { drawPools, type Pool } from "./pools.ts";
import {
	END_ZONE_PLANES,
	MID_LOGO_PLANE,
	TURF_PLANE,
	type FieldStyle,
	type NumberMark,
} from "./fieldPaint.ts";
import {
	CROSSBAR_Z,
	END_ROOM,
	FIELD_L,
	FIELD_W,
	GOAL_L,
	GOAL_R,
	MID_Y,
	POST_BACK,
	POST_W,
	SIDE_ROOM,
	UPRIGHT_Z,
	type Pt3,
} from "./geometry.ts";
import {
	CLUB,
	drawSky,
	END_BOARDS,
	END_STANDS,
	END_WALLS,
	LOWER,
	RIM,
	UPPER,
	ROOF,
	WALL,
	type StadiumPaint,
} from "./stadium.ts";

// ONE FRAME OF THE FIELD: the sky, the stadium from the top of the upper deck
// down to the wall, the turf and its paint, the lines, then everything
// standing on the field from the back of the picture to the front - the
// goalposts among the players - as the 3D court draws its own (see
// court3d/scene.ts).

export type FieldPaint = {
	style: FieldStyle;
	turf: HTMLCanvasElement;
	endZones: [HTMLCanvasElement, HTMLCanvasElement];
	numbers: NumberMark[];
	logo?: HTMLCanvasElement;
	stadium: StadiumPaint;
};

export type FieldFrame = {
	ctx: CanvasRenderingContext2D;
	cam: Camera;
	paint: FieldPaint;
	// The crowd on its feet (0 to 1), and waving rather than arms up.
	crowd?: { up: number; wave: boolean };
	// The broadcast's lines on the turf (see marks.ts).
	marks?: Mark[];
	// The shadows under the men and the ball (see pools.ts).
	pools?: Pool[];
	// Drawn among the posts, back to front: each with how far it is from the
	// camera.
	items?: { depth: number; draw: () => void }[];
};

const POST_YELLOW = "#f5d31c";
const POST_PAD = "#1d2a4a";

// A goalpost at the end line `side` (0 left, 1 right): the padded base back
// behind the end line, the gooseneck up and forward to over the line, the
// crossbar ten feet up, the uprights thirty-five feet over it with a ribbon
// at each top.
const postParts = (side: 0 | 1) => {
	const s = side === 0 ? -1 : 1;
	const xLine = side === 0 ? 0 : FIELD_L;
	const xBase = xLine + s * POST_BACK;
	const y = MID_Y;
	const h = POST_W / 2;
	return {
		base: { x: xBase, y, z: 0 },
		bend: { x: xBase, y, z: CROSSBAR_Z - 2.5 },
		neck: { x: xLine, y, z: CROSSBAR_Z },
		barL: { x: xLine, y: y - h, z: CROSSBAR_Z },
		barR: { x: xLine, y: y + h, z: CROSSBAR_Z },
		topL: { x: xLine, y: y - h, z: UPRIGHT_Z },
		topR: { x: xLine, y: y + h, z: UPRIGHT_Z },
	};
};

const drawPost = (ctx: CanvasRenderingContext2D, cam: Camera, side: 0 | 1) => {
	const p = postParts(side);
	const P = (q: Pt3) => project(cam, q);
	const base = P(p.base);
	const k = base.k;
	ctx.save();
	ctx.lineCap = "round";
	ctx.lineJoin = "round";
	// The pad round the base.
	ctx.strokeStyle = POST_PAD;
	ctx.lineWidth = Math.max(2, k * 2.2);
	const padTop = P({ ...p.base, z: 6 });
	ctx.beginPath();
	ctx.moveTo(base.x, base.y);
	ctx.lineTo(padTop.x, padTop.y);
	ctx.stroke();
	ctx.strokeStyle = POST_YELLOW;
	ctx.lineWidth = Math.max(1.5, k * 0.6);
	const bend = P(p.bend);
	const neck = P(p.neck);
	ctx.beginPath();
	ctx.moveTo(padTop.x, padTop.y);
	ctx.lineTo(bend.x, bend.y);
	ctx.quadraticCurveTo(bend.x, neck.y, neck.x, neck.y);
	ctx.stroke();
	const barL = P(p.barL);
	const barR = P(p.barR);
	const topL = P(p.topL);
	const topR = P(p.topR);
	ctx.lineWidth = Math.max(1.2, k * 0.42);
	ctx.beginPath();
	ctx.moveTo(barL.x, barL.y);
	ctx.lineTo(barR.x, barR.y);
	ctx.moveTo(barL.x, barL.y);
	ctx.lineTo(topL.x, topL.y);
	ctx.moveTo(barR.x, barR.y);
	ctx.lineTo(topR.x, topR.y);
	ctx.stroke();
	// The ribbons at the tops.
	ctx.strokeStyle = "#d8391f";
	ctx.lineWidth = Math.max(1, k * 0.3);
	for (const top of [p.topL, p.topR]) {
		const a = P(top);
		const b = P({ ...top, z: top.z + 4, x: top.x + 1.2 });
		ctx.beginPath();
		ctx.moveTo(a.x, a.y);
		ctx.lineTo(b.x, b.y);
		ctx.stroke();
	}
	ctx.restore();
};

// The pylons: orange, four inches square and eighteen tall, on the outside
// corners of both goal lines and both end lines.
const PYLON = "#ff6a14";
const PYLON_SIDE = "#c24a08";
const PYLON_W = 4 / 12;
const PYLON_H = 1.5;
const PYLONS: Pt3[] = [0, GOAL_L, GOAL_R, FIELD_L].flatMap((x) =>
	[0, FIELD_W].map((y) => ({
		x: x === 0 || x === GOAL_R ? x - PYLON_W / 2 : x + PYLON_W / 2,
		y: y === 0 ? -PYLON_W / 2 : y + PYLON_W / 2,
		z: 0,
	})),
);

const drawPylon = (ctx: CanvasRenderingContext2D, cam: Camera, at: Pt3) => {
	const a = project(cam, at);
	const b = project(cam, { ...at, z: PYLON_H });
	const w = Math.max(2, PYLON_W * a.k);
	ctx.fillStyle = PYLON;
	ctx.fillRect(a.x - w / 2, b.y, w, a.y - b.y);
	ctx.fillStyle = PYLON_SIDE;
	ctx.fillRect(a.x + w / 4, b.y, w / 4, a.y - b.y);
};

// A day game's shadow of the stands behind the camera across the near part
// of the field, its edge a little soft.
const SHADE = "rgba(10, 20, 42, 0.1)";
const SHADE_EDGE = [0, 0.9, 1.8];
const drawShadow = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	edge: [number, number],
) => {
	const x0 = -END_ROOM - 40;
	const x1 = FIELD_L + END_ROOM + 40;
	const at = (x: number) => edge[0] + (x / FIELD_L) * (edge[1] - edge[0]);
	const near = FIELD_W + SIDE_ROOM;
	ctx.fillStyle = SHADE;
	for (const off of SHADE_EDGE) {
		const pts = [
			{ x: x0, y: at(x0) + off },
			{ x: x1, y: at(x1) + off },
			{ x: x1, y: near },
			{ x: x0, y: near },
		].map((p) => project(cam, { ...p, z: 0 }));
		ctx.beginPath();
		pts.forEach((p, k) =>
			k === 0 ? ctx.moveTo(p.x, p.y) : ctx.lineTo(p.x, p.y),
		);
		ctx.closePath();
		ctx.fill();
	}
};

export const postDepth = (cam: Camera, side: 0 | 1): number =>
	depthOf(cam, postParts(side).neck);

export const drawFieldFrame = (f: FieldFrame) => {
	const { ctx, cam, paint } = f;
	const st = paint.stadium;
	ctx.setTransform(1, 0, 0, 1, 0, 0);
	drawSky(ctx, st.style);
	if (st.roof) {
		drawTexturedPlane(ctx, cam, ROOF, st.roof, 24, 16);
	}
	// The far side, top down: the rim and its lights, the upper deck, the
	// club level, the lower bowl, the wall.
	drawTexturedPlane(ctx, cam, RIM, st.rim, 24, 1);
	drawTexturedPlane(ctx, cam, UPPER, st.upper, 24, 6);
	drawTexturedPlane(ctx, cam, CLUB, st.club, 24, 1);
	const up = f.crowd?.up ?? 0;
	drawTexturedPlane(ctx, cam, LOWER, st.lower[0], 24, 6);
	if (up > 0.01) {
		drawTexturedPlane(
			ctx,
			cam,
			LOWER,
			st.lower[f.crowd!.wave ? 2 : 1],
			24,
			6,
			up,
		);
	}
	// Behind each end zone: the board high over the stands, the stands, the
	// wall.
	for (const side of [0, 1] as const) {
		drawTexturedPlane(ctx, cam, END_BOARDS[side], st.board, 8, 4);
		drawTexturedPlane(ctx, cam, END_STANDS[side], st.endStands[0], 16, 8);
		if (up > 0.01) {
			drawTexturedPlane(
				ctx,
				cam,
				END_STANDS[side],
				st.endStands[f.crowd!.wave ? 2 : 1],
				16,
				8,
				up,
			);
		}
		drawTexturedPlane(ctx, cam, END_WALLS[side], st.endWall, 16, 1);
	}
	drawTexturedPlane(ctx, cam, WALL, st.wall, 24, 1);
	// The ground: the turf, the end zones' paint, the logo, the numbers.
	drawTexturedPlane(ctx, cam, TURF_PLANE, paint.turf, 32, 16);
	drawTexturedPlane(ctx, cam, END_ZONE_PLANES[0], paint.endZones[0], 4, 16);
	drawTexturedPlane(ctx, cam, END_ZONE_PLANES[1], paint.endZones[1], 4, 16);
	if (paint.logo) {
		drawTexturedPlane(ctx, cam, MID_LOGO_PLANE, paint.logo, 8, 8);
	}
	for (const n of paint.numbers) {
		drawTexturedPlane(ctx, cam, n.plane, n.img, 2, 1);
	}
	drawFieldLines(ctx, cam, paint.style.lines);
	if (st.style.shadow) {
		drawShadow(ctx, cam, st.style.shadow);
	}
	// (The broadcast's lines over it all, the shadow too.)
	if (f.marks && f.marks.length > 0) {
		drawMarks(ctx, cam, f.marks);
	}
	if (f.pools) {
		drawPools(ctx, cam, f.pools);
	}
	// Everything standing up, back to front.
	const items = [
		...(f.items ?? []),
		...PYLONS.filter((at) => depthOf(cam, at) > 1).map((at) => ({
			depth: depthOf(cam, at),
			draw: () => {
				drawPylon(ctx, cam, at);
			},
		})),
		...([0, 1] as const).map((side) => ({
			depth: postDepth(cam, side),
			draw: () => {
				drawPost(ctx, cam, side);
			},
		})),
	];
	items.sort((a, b) => b.depth - a.depth);
	for (const it of items) {
		it.draw();
	}
};
