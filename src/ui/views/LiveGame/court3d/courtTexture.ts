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
};

export const courtTexture = async (
	svg: SVGSVGElement,
	pxPerUnit: number,
): Promise<HTMLCanvasElement | undefined> => {
	const vb = svg.viewBox.baseVal;
	if (!vb || vb.width <= 0 || vb.height <= 0) {
		return undefined;
	}
	const canvas = document.createElement("canvas");
	canvas.width = Math.round(vb.width * pxPerUnit);
	canvas.height = Math.round(vb.height * pxPerUnit);
	const ctx = canvas.getContext("2d")!;

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
			});
		}
	}

	const clone = svg.cloneNode(true) as SVGSVGElement;
	for (const g of clone.querySelectorAll('g[stroke-width="0.25"]')) {
		g.remove();
	}
	for (const img of clone.querySelectorAll("image")) {
		img.remove();
	}
	clone.setAttribute("width", String(canvas.width));
	clone.setAttribute("height", String(canvas.height));
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
			(m.e - vb.x) * pxPerUnit,
			(m.f - vb.y) * pxPerUnit,
		);
		ctx.drawImage(img, x, y, w, h);
		ctx.restore();
	};
	for (const pic of pics) {
		if (pic.floor) {
			await drawPic(pic);
		}
	}
	try {
		ctx.drawImage(await load(url), 0, 0, canvas.width, canvas.height);
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
	return canvas;
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
