// THE HOME TEAM'S FLOOR, AS A PICTURE.
//
// The floor is whatever the 2D court draws for the home team - its wood, its
// paint, its logos and lettering, every custom touch - so a team's own court
// is its own court here too. That drawing is an SVG; it is turned into a
// picture once a game, for the 3D court to lay on the floor in perspective.
//
// Two things are left out of the SVG: its lines (the 3D court draws its
// own, see courtLines), and its pictures - an SVG drawn as an image is not
// allowed to fetch anything, so each logo is drawn onto the picture
// separately, exactly where the SVG puts it.

import { coverCenter } from "./coverCenter.ts";

// (A picture hosted elsewhere asked for as one we may read back, where its
// host allows that - so the floor picture stays readable; if not, as it is.)
const load = (
	src: string,
	cors = /^https?:\/\//.test(src),
): Promise<HTMLImageElement> =>
	new Promise<HTMLImageElement>((resolve, reject) => {
		const img = new Image();
		img.decoding = "async";
		if (cors) {
			img.crossOrigin = "anonymous";
		}
		img.onload = () => {
			resolve(img);
		};
		img.onerror = () => {
			if (cors) {
				load(src, false).then(resolve, reject);
			} else {
				reject(new Error("image failed"));
			}
		};
		img.src = src;
	});

type Pic = {
	href: string;
	m: DOMMatrix;
	x: number;
	y: number;
	w: number;
	h: number;
	opacity: number;
	fill: boolean;
	// The whole floor (see LiveCourt's floor picture): laid down first.
	floor: boolean;
	// And, the finals, its own center logo painted out (see coverCenter):
	// where, court units - across, up, and how far each way.
	cover?: [number, number, number, number];
};

// The picture, and where it lies: the SVG's own units (the court's feet), its
// top left corner and size. The 2D court's frame - or, for a floor picture
// with more of the apron than that frame holds (see isWideFloor), all of it.
export type CourtPicture = {
	canvas: HTMLCanvasElement;
	x: number;
	y: number;
	w: number;
	h: number;
};

// (Never further out than a wide floor picture goes, however a team has
// sized its floor picture up.)
const OUT_X = 7;
const OUT_Y = 5.5;

export const courtTexture = async (
	svg: SVGSVGElement,
	pxPerUnit: number,
): Promise<CourtPicture | undefined> => {
	const vb = svg.viewBox.baseVal;
	if (!vb || vb.width <= 0 || vb.height <= 0) {
		return undefined;
	}

	// Where each picture sits, in the SVG's own units.
	const root = svg.getScreenCTM();
	const pics: Pic[] = [];
	if (root) {
		const toUser = root.inverse();
		for (const img of svg.querySelectorAll("image")) {
			const href = img.getAttribute("href") ?? img.getAttribute("xlink:href");
			const own = img.getScreenCTM();
			if (!href || !own) {
				continue;
			}
			pics.push({
				href,
				m: toUser.multiply(own),
				x: img.x.baseVal.value,
				y: img.y.baseVal.value,
				w: img.width.baseVal.value,
				h: img.height.baseVal.value,
				opacity: Number(img.getAttribute("opacity") ?? 1),
				fill: img.getAttribute("preserveAspectRatio") === "none",
				floor: img.getAttribute("data-floor") === "1",
				...(img.getAttribute("data-cover")
					? {
							cover: img
								.getAttribute("data-cover")!
								.split(" ")
								.map(Number) as Pic["cover"],
						}
					: {}),
			});
		}
	}

	// The frame, and anything of the floor picture out past it.
	let x0 = vb.x;
	let y0 = vb.y;
	let x1 = vb.x + vb.width;
	let y1 = vb.y + vb.height;
	for (const pic of pics) {
		if (!pic.floor) {
			continue;
		}
		for (const [cx, cy] of [
			[pic.x, pic.y],
			[pic.x + pic.w, pic.y],
			[pic.x, pic.y + pic.h],
			[pic.x + pic.w, pic.y + pic.h],
		] as const) {
			const at = new DOMPoint(cx, cy).matrixTransform(pic.m);
			x0 = Math.min(x0, at.x);
			y0 = Math.min(y0, at.y);
			x1 = Math.max(x1, at.x);
			y1 = Math.max(y1, at.y);
		}
	}
	x0 = Math.max(x0, vb.x - OUT_X);
	y0 = Math.max(y0, vb.y - OUT_Y);
	x1 = Math.min(x1, vb.x + vb.width + OUT_X);
	y1 = Math.min(y1, vb.y + vb.height + OUT_Y);
	const canvas = document.createElement("canvas");
	canvas.width = Math.round((x1 - x0) * pxPerUnit);
	canvas.height = Math.round((y1 - y0) * pxPerUnit);
	const ctx = canvas.getContext("2d")!;

	const clone = svg.cloneNode(true) as SVGSVGElement;
	for (const g of clone.querySelectorAll('g[stroke-width="0.25"]')) {
		g.remove();
	}
	for (const img of clone.querySelectorAll("image")) {
		img.remove();
	}
	const svgW = Math.round(vb.width * pxPerUnit);
	const svgH = Math.round(vb.height * pxPerUnit);
	clone.setAttribute("width", String(svgW));
	clone.setAttribute("height", String(svgH));
	clone.setAttribute("xmlns", "http://www.w3.org/2000/svg");
	const xml = new XMLSerializer().serializeToString(clone);
	const url = URL.createObjectURL(
		new Blob([xml], { type: "image/svg+xml;charset=utf-8" }),
	);
	// A picture of the whole floor goes down first, everything else over it.
	const drawPic = async (pic: Pic) => {
		let img: HTMLImageElement;
		try {
			img = await load(pic.href);
		} catch {
			return;
		}
		let { x, y, w, h } = pic;
		const nw = img.naturalWidth;
		const nh = img.naturalHeight;
		if (!pic.fill && nw > 0 && nh > 0) {
			// Fit inside its box, centered, keeping its shape.
			const s = Math.min(w / nw, h / nh);
			x += (w - nw * s) / 2;
			y += (h - nh * s) / 2;
			w = nw * s;
			h = nh * s;
		}
		const m = pic.m;
		ctx.save();
		ctx.globalAlpha = pic.opacity;
		ctx.setTransform(
			m.a * pxPerUnit,
			m.b * pxPerUnit,
			m.c * pxPerUnit,
			m.d * pxPerUnit,
			(m.e - x0) * pxPerUnit,
			(m.f - y0) * pxPerUnit,
		);
		ctx.drawImage(img, x, y, w, h);
		ctx.restore();
	};
	for (const pic of pics) {
		if (pic.floor) {
			await drawPic(pic);
			if (pic.cover) {
				coverCenter(ctx, pic.cover, {
					x: (u) => (u - x0) * pxPerUnit,
					y: (v) => (v - y0) * pxPerUnit,
					v: (y) => y / pxPerUnit + y0,
					ppf: pxPerUnit,
				});
			}
		}
	}
	try {
		ctx.drawImage(
			await load(url),
			Math.round((vb.x - x0) * pxPerUnit),
			Math.round((vb.y - y0) * pxPerUnit),
			svgW,
			svgH,
		);
	} catch {
		return undefined;
	} finally {
		URL.revokeObjectURL(url);
	}

	// The pictures, on top, each in its own place.
	for (const pic of pics) {
		if (!pic.floor) {
			await drawPic(pic);
		}
	}
	return { canvas, x: x0, y: y0, w: x1 - x0, h: y1 - y0 };
};

// The color round the edge of the floor's picture - the middle of what's
// there, so the lettering and logos on the apron don't count - for the floor
// on out past it to the stands. Undefined if it can't be read (a picture
// from another site the browser won't let us read back).
export const edgeColor = (canvas: HTMLCanvasElement): string | undefined => {
	const w = canvas.width;
	const h = canvas.height;
	const band = Math.max(2, Math.round(Math.min(w, h) * 0.012));
	const reds: number[] = [];
	const greens: number[] = [];
	const blues: number[] = [];
	try {
		const g = canvas.getContext("2d", { willReadFrequently: true });
		if (!g) {
			return undefined;
		}
		for (const [x, y, cw, ch] of [
			[0, 0, w, band],
			[0, h - band, w, band],
			[0, 0, band, h],
			[w - band, 0, band, h],
		] as const) {
			const d = g.getImageData(x, y, cw, ch).data;
			for (let i = 0; i < d.length; i += 16) {
				if (d[i + 3]! > 200) {
					reds.push(d[i]!);
					greens.push(d[i + 1]!);
					blues.push(d[i + 2]!);
				}
			}
		}
	} catch {
		return undefined;
	}
	if (reds.length === 0) {
		return undefined;
	}
	const middle = (a: number[]) => a.sort((x, y) => x - y)[a.length >> 1]!;
	return `rgb(${middle(reds)}, ${middle(greens)}, ${middle(blues)})`;
};
