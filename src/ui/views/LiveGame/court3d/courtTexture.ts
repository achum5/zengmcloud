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

const load = (src: string) =>
	new Promise<HTMLImageElement>((resolve, reject) => {
		const img = new Image();
		img.decoding = "async";
		img.onload = () => {
			resolve(img);
		};
		img.onerror = () => {
			reject(new Error("image failed"));
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
	try {
		ctx.drawImage(await load(url), 0, 0, canvas.width, canvas.height);
	} catch {
		return undefined;
	} finally {
		URL.revokeObjectURL(url);
	}

	// The pictures, on top, each in its own place.
	for (const pic of pics) {
		let img: HTMLImageElement;
		try {
			img = await load(pic.href);
		} catch {
			continue;
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
	}
	return canvas;
};
