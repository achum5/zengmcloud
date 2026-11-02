import { fieldLineBoxes } from "./fieldLines.ts";
import {
	BORDER,
	FIELD_L,
	FIELD_W,
	MID_X,
	MID_Y,
	NUMBER_IN,
} from "./geometry.ts";

// THE CHAMPIONSHIP GAME, ON A PICTURE OF THE FIELD: its own midfield logo
// painted out, for the trophy and the home logo laid on in front (see
// paintTrophyPicture) - as the 3D court's finals have it (see court3d's
// coverCenter).
//
// The logo is found on the picture itself: what differs from the grass
// round it, joined up to midfield - not the lines - and no further out than
// the cover (an ellipse, the stadium's own "cover" sliders sizing and moving
// it). Each column of it is then carried in from the grass above it and
// below it - the yard lines and the mowing stripes run that way, so they go
// on through - from no nearer the numbers than NUMBER_CLEAR, its edges
// feathered in; and, on a picture that has its own lines, the hash marks
// are drawn on through it, the color they are beside it.

// Looked for on a grid this fine (ft), its cells this far from the lines
// passed over, and joined up across gaps this wide (cells: over a line it
// is painted across, and more).
const GRID = 0.25;
const LINE_CLEAR = 0.45;
const JOIN = 8;
// Painted out this far round it, feathered over this much, and carried in
// from no more than this much grass beside it.
const MARGIN = 0.6;
const FEATHER = 0.6;
const BAND = 6;
// (A logo so little it is no logo at all: left be. Cells.)
const LEAST = 20;
// (The numbers, and this much grass short of them, left be.)
const NUMBER_CLEAR = 2;
const Y_LO = NUMBER_IN + NUMBER_CLEAR;
const Y_HI = FIELD_W - NUMBER_IN - NUMBER_CLEAR;
// The midfield cover, ft: how far it goes from midfield each way, before
// the stadium's sliders (see StadiumLook.adjust.cover).
export const MID_COVER = { rx: 24, ry: 24 };

type Box = ReturnType<typeof fieldLineBoxes>[number];
const near = (b: Box, u: number, v: number, d: number) =>
	u >= b.x0 - d && u <= b.x1 + d && v >= b.y0 - d && v <= b.y1 + d;

// The picture, its midfield logo painted out - or nothing to paint out.
export const coverMidfield = (
	picture: HTMLCanvasElement | HTMLImageElement,
	[cx, cy, rx, ry]: readonly [number, number, number, number],
	// (The picture's own lines are what the field shows: see
	// StadiumLook.fieldLines.)
	ownLines: boolean,
): HTMLCanvasElement | undefined => {
	const W = picture.width;
	const H = picture.height;
	if (W <= 0 || H <= 0 || rx <= 0 || ry <= 0) {
		return undefined;
	}
	// (Field feet to the picture's pixels: it is laid over the field and its
	// border - see FIELD_PICTURE_PLANE.)
	const ppx = W / (FIELD_L + 2 * BORDER);
	const ppy = H / (FIELD_W + 2 * BORDER);
	const pxX = (u: number) => (u + BORDER) * ppx;
	const pxY = (v: number) => (v + BORDER) * ppy;
	const vOf = (y: number) => y / ppy - BORDER;
	const uOf = (x: number) => x / ppx - BORDER;

	// (The grass is looked at a little way out each side, past where any
	// logo is painted out.)
	const side = 32;
	const x0 = Math.max(0, Math.floor(pxX(cx - rx - side)));
	const x1 = Math.min(W, Math.ceil(pxX(cx + rx + side)));
	const y0 = Math.max(
		0,
		Math.floor(pxY(Math.max(Y_LO - 1, cy - ry - BAND - 2))),
	);
	const y1 = Math.min(
		H,
		Math.ceil(pxY(Math.min(Y_HI + 1, cy + ry + BAND + 2))),
	);
	if (x1 <= x0 || y1 <= y0) {
		return undefined;
	}
	const out = document.createElement("canvas");
	out.width = W;
	out.height = H;
	const ctx = out.getContext("2d", { willReadFrequently: true });
	if (!ctx) {
		return undefined;
	}
	ctx.drawImage(picture, 0, 0);
	let img: ImageData;
	try {
		img = ctx.getImageData(x0, y0, x1 - x0, y1 - y0);
	} catch {
		return undefined;
	}
	const w = x1 - x0;
	const h = y1 - y0;
	const src = img.data;
	const idx = (u: number, v: number) => {
		const X = Math.max(0, Math.min(w - 1, Math.round(pxX(u)) - x0));
		const Y = Math.max(0, Math.min(h - 1, Math.round(pxY(v)) - y0));
		return (Y * w + X) * 4;
	};
	// The lines there are about it: all of them, and the short ones (the
	// hash marks), which a column is not carried in through.
	const lines = fieldLineBoxes().filter(
		(b) =>
			b.x1 >= uOf(x0) - 1 &&
			b.x0 <= uOf(x1) + 1 &&
			b.y1 >= vOf(y0) - 1 &&
			b.y0 <= vOf(y1) + 1 &&
			b.x0 >= 0 &&
			b.x1 <= FIELD_L,
	);
	const ticks = lines.filter((b) => !b.long);
	const onLine = (u: number, v: number) =>
		lines.some((b) => near(b, u, v, LINE_CLEAR));

	// The grass: either side of the cover, clear of the lines - both shades
	// of a striped field (a stripe each five yards).
	const grass: [number, number, number][] = [];
	for (let v = Math.max(Y_LO, cy - ry); v <= Math.min(Y_HI, cy + ry); v += 1) {
		for (let du = 2; du <= side; du += 0.5) {
			for (const u of [cx - rx - du, cx + rx + du]) {
				if (!onLine(u, v)) {
					const i = idx(u, v);
					grass.push([src[i]!, src[i + 1]!, src[i + 2]!]);
				}
			}
		}
	}
	if (grass.length < 50) {
		return undefined;
	}
	const sorted = [0, 1, 2].map((k) =>
		grass.map((c) => c[k]!).sort((a, b) => a - b),
	);
	const med = sorted.map((s) => s[s.length >> 1]!);
	// (How far grass alone goes from it - its stripes, its blades.)
	const spread = Math.max(
		...sorted.map(
			(s) => s[Math.floor(s.length * 0.9)]! - s[Math.floor(s.length * 0.1)]!,
		),
	);
	const far = Math.max(55, spread * 1.6);

	// The logo's cells, on the grid over the cover.
	const nu = Math.ceil((2 * rx) / GRID) + 1;
	const nv = Math.ceil((2 * ry) / GRID) + 1;
	const cell = new Uint8Array(nu * nv);
	for (let j = 0; j < nv; j++) {
		const v = cy - ry + j * GRID;
		if (v < Y_LO || v > Y_HI) {
			continue;
		}
		for (let k = 0; k < nu; k++) {
			const u = cx - rx + k * GRID;
			if (((u - cx) / rx) ** 2 + ((v - cy) / ry) ** 2 > 1 || onLine(u, v)) {
				continue;
			}
			const i = idx(u, v);
			const d =
				Math.abs(src[i]! - med[0]!) +
				Math.abs(src[i + 1]! - med[1]!) +
				Math.abs(src[i + 2]! - med[2]!);
			if (d > far) {
				cell[j * nu + k] = 1;
			}
		}
	}
	// Joined up to midfield.
	const logo = new Uint8Array(nu * nv);
	const stack: number[] = [];
	for (let j = 0; j < nv; j++) {
		for (let k = 0; k < nu; k++) {
			const u = cx - rx + k * GRID;
			const v = cy - ry + j * GRID;
			if (cell[j * nu + k] && Math.hypot(u - cx, v - cy) < 6) {
				logo[j * nu + k] = 1;
				stack.push(j * nu + k);
			}
		}
	}
	let n = 0;
	while (stack.length > 0) {
		const c = stack.pop()!;
		n += 1;
		const j = Math.floor(c / nu);
		const k = c % nu;
		for (let dj = -JOIN; dj <= JOIN; dj++) {
			for (let dk = -JOIN; dk <= JOIN; dk++) {
				const jj = j + dj;
				const kk = k + dk;
				if (jj < 0 || jj >= nv || kk < 0 || kk >= nu) {
					continue;
				}
				const o = jj * nu + kk;
				if (cell[o] && !logo[o]) {
					logo[o] = 1;
					stack.push(o);
				}
			}
		}
	}
	if (n < LEAST) {
		return undefined;
	}
	// Each grid column's reach (ft), spread a little either way so its edge
	// runs smooth.
	const lo = new Float64Array(nu).fill(Infinity);
	const hi = new Float64Array(nu).fill(-Infinity);
	for (let j = 0; j < nv; j++) {
		for (let k = 0; k < nu; k++) {
			if (logo[j * nu + k]) {
				const v = cy - ry + j * GRID;
				lo[k] = Math.min(lo[k]!, v);
				hi[k] = Math.max(hi[k]!, v);
			}
		}
	}
	const reach = (u: number): [number, number] | undefined => {
		const k0 = Math.floor((u - (cx - rx)) / GRID);
		let a = Infinity;
		let b = -Infinity;
		for (let k = k0 - JOIN; k <= k0 + JOIN; k++) {
			if (k >= 0 && k < nu && hi[k]! >= lo[k]!) {
				a = Math.min(a, lo[k]!);
				b = Math.max(b, hi[k]!);
			}
		}
		return b >= a ? [a - MARGIN, b + MARGIN] : undefined;
	};

	const next = new Uint8ClampedArray(src);
	const feather = FEATHER * ppy;
	const painted: [number, number, number][] = [];
	for (let x = 0; x < w; x++) {
		const u = uOf(x0 + x + 0.5);
		const span = reach(u);
		if (!span) {
			continue;
		}
		const a = Math.max(span[0], Y_LO + 1.5);
		const b = Math.min(span[1], Y_HI - 1.5);
		if (b <= a) {
			continue;
		}
		painted.push([u, a, b]);
		// (Carried in past a hash mark, not through it.)
		const tickAt = (v: number) => ticks.some((t) => near(t, u, v, 0.2));
		for (const dir of [-1, 1] as const) {
			const edge = dir < 0 ? a : b;
			const band =
				Math.max(1.5, Math.min(BAND, dir < 0 ? edge - Y_LO : Y_HI - edge)) *
				ppy;
			const E = pxY(edge) - y0;
			const M = pxY((a + b) / 2) - y0;
			const from = Math.round(dir < 0 ? E - feather : M);
			const to = Math.round(dir < 0 ? M : E + feather);
			for (let y = Math.max(0, from); y <= Math.min(h - 1, to); y++) {
				const inside = dir < 0 ? y - E : E - y;
				const t = Math.max(0, Math.min(1, (inside + feather) / (2 * feather)));
				if (t <= 0) {
					continue;
				}
				let k = Math.abs(inside) % band;
				let sy = Math.round(dir < 0 ? E - 1 - k : E + 1 + k);
				for (let tries = 0; tries < 4 && tickAt(vOf(y0 + sy)); tries++) {
					k += 3 * ppy;
					sy = Math.round(dir < 0 ? E - 1 - k : E + 1 + k);
				}
				sy = Math.max(0, Math.min(h - 1, sy));
				const i = (y * w + x) * 4;
				const s = (sy * w + x) * 4;
				for (let c = 0; c < 3; c++) {
					next[i + c] = src[i + c]! + (src[s + c]! - src[i + c]!) * t;
				}
			}
		}
	}
	img.data.set(next);
	ctx.putImageData(img, x0, y0);

	// The hash marks on through it (the picture's own lines only): their
	// color beside it, where that differs from the grass.
	if (ownLines && painted.length > 0) {
		const left = painted[0]![0];
		const right = painted.at(-1)![0];
		const seen: number[][] = [];
		for (const t of ticks) {
			const u = (t.x0 + t.x1) / 2;
			if (u > left - 1 && u < right + 1) {
				continue;
			}
			const i = idx(u, (t.y0 + t.y1) / 2);
			const c = [src[i]!, src[i + 1]!, src[i + 2]!];
			if (
				Math.abs(c[0]! - med[0]!) +
					Math.abs(c[1]! - med[1]!) +
					Math.abs(c[2]! - med[2]!) >
				far
			) {
				seen.push(c);
			}
		}
		if (seen.length >= 2) {
			const line = [0, 1, 2].map((k) => {
				const s = seen.map((c) => c[k]!).sort((p, q) => p - q);
				return s[s.length >> 1]!;
			});
			ctx.fillStyle = `rgb(${line[0]}, ${line[1]}, ${line[2]})`;
			for (const t of ticks) {
				const u = (t.x0 + t.x1) / 2;
				const col = painted.find(([pu]) => Math.abs(pu - u) < 0.5 / ppx);
				if (!col || t.y1 < col[1] || t.y0 > col[2]) {
					continue;
				}
				const ya = Math.max(t.y0, col[1]);
				const yb = Math.min(t.y1, col[2]);
				ctx.fillRect(
					pxX(t.x0),
					pxY(ya),
					Math.max(1, (t.x1 - t.x0) * ppx),
					(yb - ya) * ppy,
				);
			}
		}
	}
	return out;
};

// Where it goes, ft: midfield, moved and sized by the stadium's sliders.
export const midCover = (
	adjust: { dx?: number; dy?: number; scale?: number } | undefined,
): [number, number, number, number] => [
	MID_X + (adjust?.dx ?? 0),
	MID_Y + (adjust?.dy ?? 0),
	MID_COVER.rx * (adjust?.scale ?? 1),
	MID_COVER.ry * (adjust?.scale ?? 1),
];
