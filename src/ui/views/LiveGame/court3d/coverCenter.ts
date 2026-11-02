// THE FINALS, ON A FLOOR PICTURE: its own center-court logo painted out,
// for the trophy and the logo laid on in front (see LiveCourt) - as a finals
// floor has it.
//
// The logo is found on the picture itself: what differs from the wood
// round it, joined up to center court - not the lines (the half-court line,
// the center circle) - and no further out than the cover (an ellipse, the
// court's own "cover" sliders sizing and moving it). Each row of it is then
// carried in from the wood beside it along the same row - the planks run
// the length of the floor - from no further out than inside the
// three-point line, its edges feathered in; and the half-court line is drawn
// on through it, the color it is above and below.

// Court feet.
const COURT_W = 94;
const COURT_H = 50;
const THREE_R = 23.75;
const RIM_X = 5.25;
const LINE_FT = 0.17;
// Looked for on a grid this fine (ft), its cells this far from the lines
// passed over, and joined up across gaps this wide.
const GRID = 0.25;
const LINE_CLEAR = 0.45;
const JOIN = 5;
// Painted out this far round it, feathered over this much, and carried in
// from no more than this much wood beside it.
const MARGIN = 0.6;
const FEATHER = 0.6;
const BAND = 6;
// (A logo so little it is no logo at all: left be.)
const LEAST = 20;

// `at` the picture's pixel for court feet; the canvas's pixels per foot.
export type CourtPx = {
	x: (u: number) => number;
	y: (v: number) => number;
	// (And back: a pixel row's feet up the court.)
	v: (y: number) => number;
	ppf: number;
};

export const coverCenter = (
	ctx: CanvasRenderingContext2D,
	[cx, cy, rx, ry]: readonly [number, number, number, number],
	px: CourtPx,
): boolean => {
	const W = ctx.canvas.width;
	const H = ctx.canvas.height;
	// (The wood is looked at a little way out each side, past where any logo
	// is painted out: inside the three-point line, across from the lane.)
	const x0 = Math.max(0, Math.floor(px.x(Math.min(29, cx - rx - BAND))));
	const x1 = Math.min(
		W,
		Math.ceil(px.x(Math.max(COURT_W - 29, cx + rx + BAND))),
	);
	const y0 = Math.max(0, Math.floor(px.y(cy - ry - 2)));
	const y1 = Math.min(H, Math.ceil(px.y(cy + ry + 2)));
	if (x1 <= x0 || y1 <= y0) {
		return false;
	}
	let img: ImageData;
	try {
		img = ctx.getImageData(x0, y0, x1 - x0, y1 - y0);
	} catch {
		return false;
	}
	const w = x1 - x0;
	const h = y1 - y0;
	const src = img.data;
	const idx = (u: number, v: number) => {
		const X = Math.max(0, Math.min(w - 1, Math.round(px.x(u)) - x0));
		const Y = Math.max(0, Math.min(h - 1, Math.round(px.y(v)) - y0));
		return (Y * w + X) * 4;
	};

	// The wood: the middle of two strips, one each side.
	const wood: [number, number, number][] = [];
	for (let v = 17; v <= 33; v += GRID) {
		for (const u of [
			30,
			30.5,
			31,
			COURT_W - 31,
			COURT_W - 30.5,
			COURT_W - 30,
		]) {
			const i = idx(u, v);
			wood.push([src[i]!, src[i + 1]!, src[i + 2]!]);
		}
	}
	const sorted = [0, 1, 2].map((k) =>
		wood.map((c) => c[k]!).sort((a, b) => a - b),
	);
	const med = sorted.map((s) => s[s.length >> 1]!);
	// (How far wood alone goes from it - grain, a parquet's squares.)
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
		for (let k = 0; k < nu; k++) {
			const u = cx - rx + k * GRID;
			if (((u - cx) / rx) ** 2 + ((v - cy) / ry) ** 2 > 1) {
				continue;
			}
			const r = Math.hypot(u - COURT_W / 2, v - COURT_H / 2);
			if (
				Math.abs(u - COURT_W / 2) < LINE_CLEAR ||
				Math.abs(r - 6) < LINE_CLEAR
			) {
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
	// Joined up to center court.
	const logo = new Uint8Array(nu * nv);
	const stack: number[] = [];
	for (let j = 0; j < nv; j++) {
		for (let k = 0; k < nu; k++) {
			const u = cx - rx + k * GRID;
			const v = cy - ry + j * GRID;
			if (cell[j * nu + k] && Math.hypot(u - cx, v - cy) < 4) {
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
		return false;
	}
	// Each grid row's reach (feet), spread a little up and down so its edge
	// runs smooth.
	const lo = new Float64Array(nv).fill(Infinity);
	const hi = new Float64Array(nv).fill(-Infinity);
	for (let j = 0; j < nv; j++) {
		for (let k = 0; k < nu; k++) {
			if (logo[j * nu + k]) {
				const u = cx - rx + k * GRID;
				lo[j] = Math.min(lo[j]!, u);
				hi[j] = Math.max(hi[j]!, u);
			}
		}
	}
	const reach = (v: number): [number, number] | undefined => {
		const j0 = Math.floor((v - (cy - ry)) / GRID);
		let a = Infinity;
		let b = -Infinity;
		for (let j = j0 - JOIN; j <= j0 + JOIN; j++) {
			if (j >= 0 && j < nv && hi[j]! >= lo[j]!) {
				a = Math.min(a, lo[j]!);
				b = Math.max(b, hi[j]!);
			}
		}
		return b >= a ? [a - MARGIN, b + MARGIN] : undefined;
	};

	const out = new Uint8ClampedArray(src);
	const feather = FEATHER * px.ppf;
	const painted: [number, number, number][] = [];
	for (let y = 0; y < h; y++) {
		const v = px.v(y0 + y + 0.5);
		const span = reach(v);
		if (!span) {
			continue;
		}
		// (Inside the three-point line on either side, clear of it.)
		const arc =
			Math.abs(v - COURT_H / 2) < THREE_R
				? RIM_X + Math.sqrt(THREE_R ** 2 - (v - COURT_H / 2) ** 2) + 0.6
				: 0;
		const a = Math.max(span[0], arc + 1.5);
		const b = Math.min(span[1], COURT_W - arc - 1.5);
		if (b <= a) {
			continue;
		}
		painted.push([v, a, b]);
		for (const side of [-1, 1] as const) {
			const edge = side < 0 ? a : b;
			const band =
				Math.max(
					1.5,
					Math.min(BAND, side < 0 ? edge - arc : COURT_W - arc - edge),
				) * px.ppf;
			const E = px.x(edge) - x0;
			const M = px.x((a + b) / 2) - x0;
			const from = Math.round(side < 0 ? E - feather : M);
			const to = Math.round(side < 0 ? M : E + feather);
			for (let x = Math.max(0, from); x <= Math.min(w - 1, to); x++) {
				const inside = side < 0 ? x - E : E - x;
				const t = Math.max(0, Math.min(1, (inside + feather) / (2 * feather)));
				if (t <= 0) {
					continue;
				}
				const k = Math.abs(inside) % band;
				const sx = Math.max(
					0,
					Math.min(w - 1, Math.round(side < 0 ? E - 1 - k : E + 1 + k)),
				);
				const i = (y * w + x) * 4;
				const s = (y * w + sx) * 4;
				for (let c = 0; c < 3; c++) {
					out[i + c] = src[i + c]! + (src[s + c]! - src[i + c]!) * t;
				}
			}
		}
	}
	img.data.set(out);
	ctx.putImageData(img, x0, y0);

	// The half-court line on through it: its color just above it and just
	// below, where that differs from the wood beside it.
	const samples: number[][] = [];
	const top = painted.length > 0 ? painted[0]![0] : cy;
	const bottom = painted.length > 0 ? painted.at(-1)![0] : cy;
	for (const v of [top - 2.5, top - 1.2, bottom + 1.2, bottom + 2.5]) {
		let best: number[] | undefined;
		let diff = 0;
		for (let du = -0.2; du <= 0.2; du += 0.05) {
			const i = idx(COURT_W / 2 + du, v);
			const j = idx(COURT_W / 2 + du + 1.5, v);
			const d =
				Math.abs(src[i]! - src[j]!) +
				Math.abs(src[i + 1]! - src[j + 1]!) +
				Math.abs(src[i + 2]! - src[j + 2]!);
			if (d > diff) {
				diff = d;
				best = [src[i]!, src[i + 1]!, src[i + 2]!];
			}
		}
		if (best && diff > 60) {
			samples.push(best);
		}
	}
	if (samples.length >= 2) {
		const line = [0, 1, 2].map((k) => {
			const s = samples.map((c) => c[k]!).sort((p, q) => p - q);
			return s[s.length >> 1]!;
		});
		ctx.save();
		ctx.fillStyle = `rgb(${line[0]}, ${line[1]}, ${line[2]})`;
		for (const [v, a, b] of painted) {
			if (a < COURT_W / 2 && b > COURT_W / 2) {
				ctx.fillRect(
					px.x(COURT_W / 2 - LINE_FT / 2),
					px.y(v) - 0.5,
					LINE_FT * px.ppf,
					1.5,
				);
			}
		}
		ctx.restore();
	}
	return true;
};
