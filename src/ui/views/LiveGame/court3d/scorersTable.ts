import { makeCourtRng } from "../courtRng.ts";
import {
	type ArenaTeam,
	drawFan,
	type Fan,
	TABLE_D,
	TABLE_H,
	TABLE_X0,
	TABLE_X1,
	TABLE_Y,
} from "./arena.ts";
import { depthOf, project, type Camera } from "./camera.ts";
import { TABLE_SEAT_Y, TABLE_SEATS } from "./crew.ts";
import type { CrowdFace } from "./crowdFaces.ts";
import { shade } from "./figure.ts";
import type { Pt3 } from "./geometry.ts";
import { drawTexturedPlane, type TexturedPlane } from "./planes.ts";

// THE SCORER'S TABLE AS IT IS ON TV. Behind it the officials and the media
// sit in two rows: the first at the table itself, its top covered in
// monitors, laptops, papers and water bottles; the second up a step behind
// them at a counter of their own, its top in the home color; and a black
// curtain behind them all. The four at the table who keep the book and the
// clocks are men of the game's own (see crew.ts); the rest are painted once
// a game, each in his seat, and stood up in the world where he sits each
// frame, the size the camera has him there (as the people courtside are -
// see drawCourtside).

// The front row: the rest of the seats along the table, between the four.
const FRONT_X = [38.45, 42.75, 47.25, 51.75, 55.9];
// The counter behind them (feet): its front, its back, how high its top.
const COUNTER_Y0 = -9.25;
const COUNTER_Y1 = -10.05;
const COUNTER_Z = 4;
// The second row, up on a step behind it: so many of them, along it.
const BACK_N = 8;
const BACK_Y = -10.75;
const RISER_Y1 = -11.5;
const RISER_Z = 1.6;
// The curtain, hung behind it all from end to end of the table.
const CURTAIN_Y = -11.6;
const CURTAIN_H = 7.6;
const CURTAIN_X0 = TABLE_X0 - 0.4;
const CURTAIN_X1 = TABLE_X1 + 0.4;
// How high each row sits (feet off what he sits on): high enough at the
// table to be seen over it, chest and head, the way they are on TV.
const FRONT_SEAT = 2.45;
const BACK_SEAT = 2.1;
// Each one's picture: this big (feet), this many px to the foot, this much
// space between them in the atlas (px).
const CELL_W = 2.4;
const CELL_H = 4.8;
const PX = 16;
const GAP = 3;
// None drawn nearer the camera than this (feet) - a replay's from among them.
const NEAREST = 2.5;

type Item = {
	kind: "monitor" | "laptop" | "bottle";
	at: Pt3;
	color: string;
};
export type ScorersTable = {
	atlas: HTMLCanvasElement;
	cols: number;
	people: { at: Pt3; cell: number; back: boolean }[];
	items: Item[];
	curtain: HTMLCanvasElement;
	// The counter's top.
	felt: string;
};

const CURTAIN: TexturedPlane = {
	origin: { x: CURTAIN_X0, y: CURTAIN_Y, z: CURTAIN_H },
	alongX: { x: 1 / 4, y: 0, z: 0 },
	alongY: { x: 0, y: 0, z: -1 / 4 },
	w: Math.round((CURTAIN_X1 - CURTAIN_X0) * 4),
	h: Math.round(CURTAIN_H * 4),
};

// What they wear: suits and ties, sweaters, the home team's polos.
const SUITS = [
	"#1d2333",
	"#24262c",
	"#151517",
	"#3a3f4a",
	"#2f2924",
	"#33415e",
];
const SHIRTS = ["#f4f4f0", "#f4f4f0", "#dfe8f4"];
const TIES = ["#7a1319", "#23466e", "#2b2b30", "#c9a227", "#5b2a52"];
const SWEATERS = ["#c9c3b8", "#9a9ca3", "#d8d5cf", "#6b6f78", "#b8aa92"];
const POLOS = ["#16181d", "#22252c", "#f2f2f0"];
const SKINS = ["#f1c7a5", "#d9a77f", "#b07a52", "#8a5a3b", "#5e3b26"];
const PANTS = ["#1d2330", "#24262c", "#3a3e47"];
const LIDS = ["#b9b6ae", "#c8b99c", "#c9cbcf", "#2a2c31", "#d4c7ab"];
const CAPS = ["#2d5fa8", "#2d5fa8", "#c8202a", "#1b1c20"];

export const scorersTableFor = (
	home: ArenaTeam | undefined,
	faces?: CrowdFace[],
): ScorersTable => {
	const rng = makeCourtRng(`scorersTable|${home?.abbrev ?? home?.name ?? ""}`);
	const pick = <T>(a: readonly T[]): T => a[Math.floor(rng() * a.length)]!;
	const homeC = home?.colors?.[0] || "#8c1d40";
	const homeC2 = home?.colors?.[1] || "#f2c14e";
	const people: ScorersTable["people"] = [];
	for (const x of FRONT_X) {
		people.push({
			at: { x: x + (rng() - 0.5) * 0.3, y: TABLE_SEAT_Y, z: 0 },
			cell: people.length,
			back: false,
		});
	}
	const step = (TABLE_X1 - TABLE_X0 - 2.6) / (BACK_N - 1);
	for (let i = 0; i < BACK_N; i++) {
		people.push({
			at: {
				x: TABLE_X0 + 1.3 + i * step + (rng() - 0.5) * 0.5,
				y: BACK_Y,
				z: RISER_Z,
			},
			cell: people.length,
			back: true,
		});
	}

	const cols = 8;
	const cw = Math.round(CELL_W * PX);
	const ch = Math.round(CELL_H * PX);
	const atlas = document.createElement("canvas");
	atlas.width = cols * (cw + GAP);
	atlas.height = Math.ceil(people.length / cols) * (ch + GAP);
	const g = atlas.getContext("2d")!;
	for (const p of people) {
		const ox = (p.cell % cols) * (cw + GAP);
		const oy = Math.floor(p.cell / cols) * (ch + GAP);
		const x = ox + cw / 2;
		const floor = oy + ch;
		const seat = p.back ? BACK_SEAT : FRONT_SEAT;
		const yb = floor - (seat - 0.5) * PX;
		const face = faces && faces.length > 0 ? pick(faces) : undefined;
		const skin = face?.skin ?? pick(SKINS);
		const pants = pick(PANTS);
		// A suit and tie, a sweater over a shirt, or a polo - the home
		// team's, now and then.
		const r = rng();
		const dress: Pick<Fan, "color" | "jacket" | "tie"> =
			r < 0.38
				? {
						color: pick(SHIRTS),
						jacket: pick(SUITS),
						...(rng() < 0.75 ? { tie: pick(TIES) } : {}),
					}
				: r < 0.58
					? { color: pick(SHIRTS), jacket: pick(SWEATERS) }
					: r < 0.82
						? { color: rng() < 0.75 ? homeC : homeC2 }
						: { color: pick(POLOS) };
		// His legs down to the floor, his shoes (seen only from low down).
		g.fillStyle = pants;
		for (const side of [-1, 1]) {
			g.fillRect(
				x + side * 0.08 * PX - (side < 0 ? 0.42 * PX : 0),
				yb - 0.5 * PX,
				0.42 * PX,
				floor - yb + 0.3 * PX,
			);
		}
		g.fillStyle = "#16171b";
		g.fillRect(x - 0.5 * PX, floor - 0.2 * PX, PX, 0.2 * PX);
		drawFan(
			g,
			{
				x,
				yb,
				bodyW: (1.3 + rng() * 0.3) * PX,
				skin,
				...(face ? { face } : {}),
				pants,
				...dress,
				...(rng() < (p.back ? 0.3 : 0.45) ? { headset: true } : {}),
				...(p.back && rng() < 0.15 ? { holding: "phone" as const } : {}),
			},
			PX,
		);
	}

	// On the table: in front of each seat a monitor, its back to the floor,
	// or a laptop, open; water bottles here and there. On the counter: a
	// laptop in front of most of them.
	const items: Item[] = [];
	const backY = TABLE_Y - TABLE_D;
	const seats = [...TABLE_SEATS, ...FRONT_X].sort((a, b) => a - b);
	let last = "";
	for (const x of seats) {
		const r = rng();
		const kind = r < 0.55 && last !== "monitor" ? "monitor" : "laptop";
		last = kind;
		items.push({
			kind,
			at: {
				x: x + (rng() - 0.5) * 0.5,
				y: backY + (kind === "monitor" ? 0.5 : 0.75),
				z: TABLE_H,
			},
			color: kind === "monitor" ? "#23252a" : pick(LIDS),
		});
		if (rng() < 0.6) {
			items.push({
				kind: "bottle",
				at: {
					x: x + (rng() < 0.5 ? -1 : 1) * (0.75 + rng() * 0.2),
					y: TABLE_Y - 0.6 - rng() * 0.6,
					z: TABLE_H,
				},
				color: pick(CAPS),
			});
		}
	}
	for (const p of people) {
		if (!p.back) {
			continue;
		}
		if (rng() < 0.7) {
			items.push({
				kind: "laptop",
				at: {
					x: p.at.x + (rng() - 0.5) * 0.4,
					y: COUNTER_Y0 - 0.3,
					z: COUNTER_Z,
				},
				color: pick(LIDS),
			});
		} else if (rng() < 0.6) {
			items.push({
				kind: "bottle",
				at: {
					x: p.at.x + (rng() - 0.5) * 0.8,
					y: COUNTER_Y0 - 0.3,
					z: COUNTER_Z,
				},
				color: pick(CAPS),
			});
		}
	}

	// The curtain: black, in folds, on its pipe.
	const curtain = document.createElement("canvas");
	curtain.width = CURTAIN.w;
	curtain.height = CURTAIN.h;
	const cc = curtain.getContext("2d")!;
	cc.fillStyle = "#0b0b0e";
	cc.fillRect(0, 0, curtain.width, curtain.height);
	const fold = 0.55 * 4;
	for (let fx = 0; fx < curtain.width; fx += fold) {
		const grad = cc.createLinearGradient(fx, 0, fx + fold, 0);
		grad.addColorStop(0, "#08080a");
		grad.addColorStop(0.45, "#16161b");
		grad.addColorStop(1, "#08080a");
		cc.fillStyle = grad;
		cc.fillRect(fx, 1, fold, curtain.height - 1);
	}
	cc.fillStyle = "#2a2b30";
	cc.fillRect(0, 0, curtain.width, 1);

	return {
		atlas,
		cols,
		people,
		items,
		curtain,
		felt: shade(homeC, -0.12),
	};
};

// A flat four-cornered face, in one color.
const quad = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	pts: Pt3[],
	color: string,
) => {
	ctx.fillStyle = color;
	ctx.beginPath();
	for (const p of pts) {
		const q = project(cam, p);
		ctx.lineTo(q.x, q.y);
	}
	ctx.closePath();
	ctx.fill();
};
// A box: the faces of it toward the camera.
const box = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	x0: number,
	x1: number,
	y0: number,
	y1: number,
	z: number,
	front: string,
	top: string,
	ends: string,
) => {
	const { pos } = cam;
	if (pos.x < x0) {
		quad(
			ctx,
			cam,
			[
				{ x: x0, y: y0, z: 0 },
				{ x: x0, y: y1, z: 0 },
				{ x: x0, y: y1, z },
				{ x: x0, y: y0, z },
			],
			ends,
		);
	} else if (pos.x > x1) {
		quad(
			ctx,
			cam,
			[
				{ x: x1, y: y0, z: 0 },
				{ x: x1, y: y1, z: 0 },
				{ x: x1, y: y1, z },
				{ x: x1, y: y0, z },
			],
			ends,
		);
	}
	if (pos.y > y0) {
		quad(
			ctx,
			cam,
			[
				{ x: x0, y: y0, z: 0 },
				{ x: x1, y: y0, z: 0 },
				{ x: x1, y: y0, z },
				{ x: x0, y: y0, z },
			],
			front,
		);
	}
	if (pos.z > z) {
		quad(
			ctx,
			cam,
			[
				{ x: x0, y: y0, z },
				{ x: x1, y: y0, z },
				{ x: x1, y: y1, z },
				{ x: x0, y: y1, z },
			],
			top,
		);
	}
};

const drawPeople = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	t: ScorersTable,
	back: boolean,
) => {
	const cw = Math.round(CELL_W * PX);
	const ch = Math.round(CELL_H * PX);
	const placed: { d: number; x: number; y: number; k: number; cell: number }[] =
		[];
	for (const p of t.people) {
		if (p.back !== back || depthOf(cam, p.at) < NEAREST) {
			continue;
		}
		const q = project(cam, p.at);
		const w = CELL_W * q.k;
		const h = CELL_H * q.k;
		if (q.x + w < 0 || q.x - w > cam.viewW || q.y < 0 || q.y - h > cam.viewH) {
			continue;
		}
		placed.push({ d: q.depth, x: q.x, y: q.y, k: q.k, cell: p.cell });
	}
	placed.sort((a, b) => b.d - a.d);
	const smoothing = ctx.imageSmoothingEnabled;
	ctx.imageSmoothingEnabled = true;
	for (const q of placed) {
		const w = CELL_W * q.k;
		const h = CELL_H * q.k;
		ctx.drawImage(
			t.atlas,
			(q.cell % t.cols) * (cw + GAP),
			Math.floor(q.cell / t.cols) * (ch + GAP),
			cw,
			ch,
			q.x - w / 2,
			q.y - h,
			w,
			h,
		);
	}
	ctx.imageSmoothingEnabled = smoothing;
};

const drawItem = (ctx: CanvasRenderingContext2D, cam: Camera, it: Item) => {
	const { x, y, z } = it.at;
	const face = (x0: number, x1: number, z0: number, z1: number, c: string) =>
		quad(
			ctx,
			cam,
			[
				{ x: x0, y, z: z0 },
				{ x: x1, y, z: z0 },
				{ x: x1, y, z: z1 },
				{ x: x0, y, z: z1 },
			],
			c,
		);
	if (it.kind === "monitor") {
		// On its stand: a dark back, its edge a shade lighter.
		face(x - 0.08, x + 0.08, z, z + 0.3, "#1b1c20");
		face(x - 0.85, x + 0.85, z + 0.25, z + 1.22, "#3a3d44");
		face(x - 0.8, x + 0.8, z + 0.29, z + 1.18, it.color);
	} else if (it.kind === "laptop") {
		// Its lid up, leant back a little, toward whoever is at it.
		const lid = it.color;
		quad(
			ctx,
			cam,
			[
				{ x: x - 0.52, y, z },
				{ x: x + 0.52, y, z },
				{ x: x + 0.52, y: y - 0.22, z: z + 0.64 },
				{ x: x - 0.52, y: y - 0.22, z: z + 0.64 },
			],
			lid,
		);
		quad(
			ctx,
			cam,
			[
				{ x: x - 0.52, y, z },
				{ x: x + 0.52, y, z },
				{ x: x + 0.52, y, z: z + 0.05 },
				{ x: x - 0.52, y, z: z + 0.05 },
			],
			shade(lid, -0.3),
		);
	} else {
		face(x - 0.09, x + 0.09, z, z + 0.6, "rgba(214, 226, 234, 0.85)");
		face(x - 0.06, x + 0.06, z + 0.6, z + 0.72, it.color);
	}
};

// Behind the table: the curtain, the step the second row is up on, them,
// their counter and what is on it. Before anything else at the table or
// the benches (the furthest back of it all).
export const drawTableBack = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	t: ScorersTable,
) => {
	if (depthOf(cam, { x: (TABLE_X0 + TABLE_X1) / 2, y: CURTAIN_Y, z: 0 }) < 0) {
		return;
	}
	drawTexturedPlane(ctx, cam, CURTAIN, t.curtain, 6, 2);
	box(
		ctx,
		cam,
		TABLE_X0,
		TABLE_X1,
		COUNTER_Y1,
		RISER_Y1,
		RISER_Z,
		"#0b0b0e",
		"#111216",
		"#0b0b0e",
	);
	drawPeople(ctx, cam, t, true);
	box(
		ctx,
		cam,
		TABLE_X0,
		TABLE_X1,
		COUNTER_Y0,
		COUNTER_Y1,
		COUNTER_Z,
		"#0d0e12",
		t.felt,
		"#0a0a0d",
	);
	for (const it of t.items) {
		if (it.at.z === COUNTER_Z) {
			drawItem(ctx, cam, it);
		}
	}
};
// The first row, at the table (before the four of the game's own there).
export const drawTableRow = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	t: ScorersTable,
) => {
	drawPeople(ctx, cam, t, false);
};
// What is on the table, after its top.
export const drawTableItems = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	t: ScorersTable,
) => {
	const on = t.items
		.filter((it) => it.at.z === TABLE_H && depthOf(cam, it.at) >= NEAREST)
		.map((it) => ({ it, d: depthOf(cam, it.at) }))
		.sort((a, b) => b.d - a.d);
	for (const { it } of on) {
		drawItem(ctx, cam, it);
	}
};
