import { depthOf, project, type Camera } from "../court3d/camera.ts";
import { drawTexturedLayers, drawTexturedPlane } from "../court3d/planes.ts";
import { drawConfetti, type Confetti } from "./confetti.ts";
import type { Crowd } from "./crowd.ts";
import { drawFieldLines } from "./fieldLines.ts";
import { drawMarks, type Mark } from "./marks.ts";
import { drawNet, type NetUp } from "./net.ts";
import { drawPools, type Pool } from "./pools.ts";
import {
	END_ZONE_PLANES,
	FIELD_PICTURE_PLANE,
	MID_LOGO_PLANE,
	MID_TROPHY_PLANE,
	TURF_PLANE,
	type FieldStyle,
	type DecalMark,
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
	FASCIA,
	drawSky,
	END_BOARDS,
	END_STANDS,
	END_WALLS,
	LOWER,
	RIM,
	UPPER,
	ROOF,
	TUNNEL,
	WALL,
	type BoardScreen,
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
	// (The championship game's, in its place: see paintMidTrophy.)
	trophy?: HTMLCanvasElement;
	// The league's decals for the game (see paintDecals).
	decals?: DecalMark[];
	// The team's picture of its whole field, laid over the turf in place of
	// the end zones' paint - and with its own lines, maybe (see StadiumLook).
	field?: HTMLCanvasElement;
	ownLines?: boolean;
	stadium: StadiumPaint;
};

export type FieldFrame = {
	ctx: CanvasRenderingContext2D;
	cam: Camera;
	paint: FieldPaint;
	// The crowd on its feet, and how full the stands are; what the boards
	// show (see crowd.ts), and the score on the ribbon (the visitors', the
	// home team's).
	crowd?: Crowd;
	board?: BoardScreen;
	score?: [number, number];
	// The broadcast's lines on the turf (see marks.ts).
	marks?: Mark[];
	// The shadows under the men and the ball (see pools.ts).
	pools?: Pool[];
	// The net behind the posts at a kick (see net.ts).
	net?: NetUp;
	// Drawn among the posts, back to front: each with how far it is from the
	// camera.
	items?: { depth: number; draw: () => void }[];
	// The championship's confetti (see confetti.ts), and when it is.
	confetti?: { of: Confetti; t: number };
	// Photographers' cameras going off (see media.ts flashesAt).
	flashes?: Pt3[];
	// In close on a man, a long lens's: the stadium behind him this far out
	// of focus (picture pixels; the 3D court's - see court3d/scene.ts).
	soft?: number;
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

// The nearest the camera anything standing up is drawn (feet).
const ITEM_NEAR = 1;
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

// The picture so far (the stadium, behind everything on the field) drawn
// again over itself out of focus.
let behind: HTMLCanvasElement | undefined;
const softenBehind = (ctx: CanvasRenderingContext2D, px: number) => {
	const c = ctx.canvas;
	behind ??= document.createElement("canvas");
	if (behind.width !== c.width || behind.height !== c.height) {
		behind.width = c.width;
		behind.height = c.height;
	}
	const bc = behind.getContext("2d");
	if (!bc) {
		return;
	}
	bc.setTransform(1, 0, 0, 1, 0, 0);
	bc.clearRect(0, 0, c.width, c.height);
	bc.drawImage(c, 0, 0);
	ctx.save();
	ctx.setTransform(1, 0, 0, 1, 0, 0);
	ctx.filter = `blur(${px}px)`;
	ctx.drawImage(behind, 0, 0);
	ctx.restore();
};

// The wave (see crowd.ts rollingAt): the crowd on its feet over a band of
// the lower bowl - all of them across the middle of it, some up already
// ahead of it, some sitting back down behind - each stretch of it (feet
// ahead of its middle the way it goes, and how many up) the picture of
// them up, arms going, laid over the seats, clipped to that stretch.
const WAVE_BANDS: [number, number, number][] = [
	[14, 24, 0.45],
	[-8, 14, 1],
	[-20, -8, 0.55],
	[-32, -20, 0.25],
];
const drawWave = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	roll: { x: number; dir: 1 | -1 },
	up: HTMLCanvasElement,
	fill: number,
) => {
	const o = LOWER.origin;
	const down = LOWER.alongY;
	const at = (x: number, v: number) => ({
		x,
		y: o.y + down.y * v,
		z: o.z + down.z * v,
	});
	for (const [a, b, share] of WAVE_BANDS) {
		const xa = roll.x + roll.dir * a;
		const xb = roll.x + roll.dir * b;
		const corners = [at(xa, 0), at(xb, 0), at(xb, LOWER.h), at(xa, LOWER.h)];
		if (corners.some((c) => depthOf(cam, c) <= 1)) {
			continue;
		}
		const pts = corners.map((c) => project(cam, c));
		ctx.save();
		ctx.beginPath();
		pts.forEach((p, k) => {
			if (k === 0) {
				ctx.moveTo(p.x, p.y);
			} else {
				ctx.lineTo(p.x, p.y);
			}
		});
		ctx.closePath();
		ctx.clip();
		drawTexturedPlane(ctx, cam, LOWER, up, 24, 6, share * fill);
		ctx.restore();
	}
};

export const drawFieldFrame = (f: FieldFrame) => {
	const { ctx, cam, paint } = f;
	const st = paint.stadium;
	ctx.setTransform(1, 0, 0, 1, 0, 0);
	drawSky(ctx, st.style);
	if (st.roof) {
		drawTexturedPlane(ctx, cam, ROOF, st.roof, 24, 16);
	}
	// The far side, top down: the rim and its lights, the upper deck and the
	// ring of honor along its front, the club level, the lower bowl, the
	// wall.
	drawTexturedPlane(ctx, cam, RIM, st.rim, 24, 1);
	drawTexturedPlane(ctx, cam, UPPER, st.upper, 24, 6);
	drawTexturedPlane(ctx, cam, FASCIA, st.fascia, 24, 1);
	const board = f.board ?? "name";
	drawTexturedPlane(ctx, cam, CLUB, st.screen("club", board, f.score), 24, 1);
	// (Fewer in their seats: the half-empty stands, the full ones over them
	// as faint as the seats are empty - and as few of them up.)
	const crowd = f.crowd ?? { up: 0, wave: false, full: 1 };
	const thin = crowd.full < 0.999;
	const fill = thin ? Math.max(0, (crowd.full - 0.55) / 0.45) : 1;
	const stands = (
		sparse: HTMLCanvasElement,
		[sat, up, wave]: [HTMLCanvasElement, HTMLCanvasElement, HTMLCanvasElement],
		// (Up on their own, behind an end zone: see crowd.ts postsAt.)
		own?: { up: number; wave: boolean },
	) => {
		const on = own ? { up: Math.max(own.up, crowd.up), wave: own.wave } : crowd;
		return [
			...(thin ? [{ img: sparse, alpha: 1 }] : []),
			{ img: sat, alpha: fill },
			{ img: on.wave ? wave : up, alpha: on.up * fill },
		];
	};
	drawTexturedLayers(ctx, cam, LOWER, stands(st.lowerSparse, st.lower), 24, 6);
	if (crowd.rolling) {
		drawWave(ctx, cam, crowd.rolling, st.lower[2], fill);
	}
	// Behind each end zone: the board high over the stands, the stands, the
	// wall.
	for (const side of [0, 1] as const) {
		drawTexturedPlane(ctx, cam, END_BOARDS[side], st.board, 8, 4);
		drawTexturedLayers(
			ctx,
			cam,
			END_STANDS[side],
			stands(st.endSparse, st.endStands, crowd.ends?.[side]),
			16,
			8,
		);
		drawTexturedPlane(
			ctx,
			cam,
			END_WALLS[side],
			st.screen("end", board),
			16,
			1,
		);
	}
	drawTexturedPlane(ctx, cam, WALL, st.screen("wall", board), 24, 1);
	drawTexturedPlane(ctx, cam, TUNNEL, st.tunnel, 4, 2);
	if (f.soft) {
		softenBehind(ctx, f.soft);
	}
	// The ground: the turf, the end zones' paint, the logo, the numbers.
	drawTexturedPlane(ctx, cam, TURF_PLANE, paint.turf, 32, 16);
	if (paint.field) {
		drawTexturedPlane(ctx, cam, FIELD_PICTURE_PLANE, paint.field, 32, 16);
	} else {
		drawTexturedPlane(ctx, cam, END_ZONE_PLANES[0], paint.endZones[0], 4, 16);
		drawTexturedPlane(ctx, cam, END_ZONE_PLANES[1], paint.endZones[1], 4, 16);
	}
	if (paint.trophy) {
		drawTexturedPlane(ctx, cam, MID_TROPHY_PLANE, paint.trophy, 8, 12);
	} else if (paint.logo) {
		drawTexturedPlane(ctx, cam, MID_LOGO_PLANE, paint.logo, 8, 8);
	}
	for (const d of paint.decals ?? []) {
		const cells = Math.min(24, Math.max(4, Math.ceil(d.plane.w / 32)));
		drawTexturedPlane(ctx, cam, d.plane, d.img, cells, cells);
	}
	if (!paint.ownLines) {
		for (const n of paint.numbers) {
			drawTexturedPlane(ctx, cam, n.plane, n.img, 2, 1);
		}
	}
	drawFieldLines(ctx, cam, paint.style.lines, !paint.ownLines);
	if (st.style.shadow) {
		drawShadow(ctx, cam, st.style.shadow);
	}
	// (The broadcast's lines over it all, the shadow too.)
	if (f.marks && f.marks.length > 0) {
		drawMarks(ctx, cam, f.marks, paint.style.turf);
	}
	if (f.pools) {
		drawPools(ctx, cam, f.pools);
	}
	if (f.confetti) {
		drawConfetti(ctx, cam, f.confetti.of, f.confetti.t, false);
	}
	// (Behind everything else standing up.)
	if (f.net) {
		drawNet(ctx, cam, f.net);
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
		// (Nothing behind a camera down on the field, or right up against
		// it - a down box on the near sideline, say - drawn up huge across
		// the picture: see sprite.ts NEAR_FT.)
		if (it.depth > ITEM_NEAR) {
			it.draw();
		}
	}
	if (f.confetti) {
		drawConfetti(ctx, cam, f.confetti.of, f.confetti.t, true);
	}
	// A photographer's flash (the 3D court's: see court3d/scene.ts).
	for (const at of f.flashes ?? []) {
		const p = project(cam, at);
		const r = Math.max(3, p.k * 1.3);
		const g = ctx.createRadialGradient(p.x, p.y, 0, p.x, p.y, r);
		g.addColorStop(0, "rgba(255,255,255,1)");
		g.addColorStop(0.3, "rgba(240,244,255,0.7)");
		g.addColorStop(1, "rgba(220,230,255,0)");
		ctx.fillStyle = g;
		ctx.beginPath();
		ctx.arc(p.x, p.y, r, 0, Math.PI * 2);
		ctx.fill();
	}
};
