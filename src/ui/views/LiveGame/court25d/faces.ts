import { display, type FaceConfig } from "facesjs";
import { stubbleShave } from "../../../../common/stubbleShade.ts";

// EACH PLAYER'S HEAD, ready to draw: his faces.js face (or his photo) drawn
// once into a small canvas, cropped to the head, so the court can stamp it on
// his shoulders every frame for the cost of one image copy.

export type HeadSprite = {
	img: HTMLCanvasElement;
	// The middle of his face in it, and its height from crown to chin (px).
	cx: number;
	cy: number;
	h: number;
	photo: boolean;
};

// faces.js draws in a 400 x 600 box: the head is centered near (200, 300),
// about 400 units from the top of the hair to the chin, with the shoulders and
// jersey below it.
const CROP = { x: 20, y: 60, w: 360, h: 445 };
const FACE_CENTER = { x: 200, y: 300 };
const FACE_H = 400;
const SCALE = 0.6;

const loadImage = (src: string, crossOrigin: boolean) =>
	new Promise<HTMLImageElement>((resolve, reject) => {
		const img = new Image();
		if (crossOrigin) {
			img.crossOrigin = "anonymous";
		}
		img.decoding = "async";
		img.onload = () => {
			resolve(img);
		};
		img.onerror = () => {
			reject(new Error("image failed"));
		};
		img.src = src;
	});

const faceSprite = async (
	face: FaceConfig,
	colors: [string, string, string] | undefined,
): Promise<HeadSprite> => {
	const shave = stubbleShave(face.head?.shave, face.hair?.color);
	const overrides: Record<string, unknown> = {};
	if (colors) {
		overrides.teamColors = colors;
	}
	if (shave !== undefined) {
		overrides.head = { shave };
	}
	// faces.js measures its parts as it draws, so it draws into the page (out
	// of sight), and the result is copied out as an image.
	const holder = document.createElement("div");
	holder.style.cssText =
		"position:fixed;left:-10000px;top:0;width:400px;height:600px;visibility:hidden;pointer-events:none";
	document.body.append(holder);
	let svg: string;
	try {
		// display() writes the overrides into the face it is given.
		display(holder, structuredClone(face), {
			...overrides,
			jersey: { id: "jersey" },
		} as any);
		svg = holder.innerHTML
			.replace('width="100%"', 'width="400"')
			.replace('height="100%"', 'height="600"');
	} finally {
		holder.remove();
	}
	if (!svg.includes("xmlns=")) {
		svg = svg.replace("<svg", '<svg xmlns="http://www.w3.org/2000/svg"');
	}
	const url = URL.createObjectURL(
		new Blob([svg], { type: "image/svg+xml;charset=utf-8" }),
	);
	try {
		const img = await loadImage(url, false);
		const canvas = document.createElement("canvas");
		canvas.width = Math.round(CROP.w * SCALE);
		canvas.height = Math.round(CROP.h * SCALE);
		const ctx = canvas.getContext("2d")!;
		ctx.drawImage(
			img,
			CROP.x,
			CROP.y,
			CROP.w,
			CROP.h,
			0,
			0,
			canvas.width,
			canvas.height,
		);
		// Keep the neck, lose the shoulders and jersey beside it.
		ctx.globalCompositeOperation = "destination-out";
		const neckL = (150 - CROP.x) * SCALE;
		const neckR = (250 - CROP.x) * SCALE;
		const below = (470 - CROP.y) * SCALE;
		ctx.fillRect(0, below, neckL, canvas.height);
		ctx.fillRect(neckR, below, canvas.width - neckR, canvas.height);
		return {
			img: canvas,
			cx: (FACE_CENTER.x - CROP.x) * SCALE,
			cy: (FACE_CENTER.y - CROP.y) * SCALE,
			h: FACE_H * SCALE,
			photo: false,
		};
	} finally {
		URL.revokeObjectURL(url);
	}
};

// A photo is a head-and-shoulders shot: the face is in the upper middle.
const photoSprite = async (
	imgURL: string,
): Promise<{ sprite: HeadSprite; skin?: string }> => {
	let img: HTMLImageElement;
	let readable = true;
	try {
		img = await loadImage(imgURL, true);
	} catch {
		// No CORS headers: still drawable, just not readable for a skin tone.
		img = await loadImage(imgURL, false);
		readable = false;
	}
	const size = 96;
	const canvas = document.createElement("canvas");
	canvas.width = size;
	canvas.height = size;
	const ctx = canvas.getContext("2d")!;
	const w = img.naturalWidth || img.width;
	const h = img.naturalHeight || img.height;
	const side = Math.min(w * 0.62, h * 0.72);
	const sx = (w - side) / 2;
	const sy = Math.max(0, h * 0.04);
	ctx.beginPath();
	ctx.ellipse(size / 2, size / 2, size * 0.4, size / 2, 0, 0, Math.PI * 2);
	ctx.clip();
	ctx.drawImage(img, sx, sy, side, side, 0, 0, size, size);
	let skin: string | undefined;
	if (readable) {
		try {
			const d = ctx.getImageData(
				size * 0.36,
				size * 0.5,
				size * 0.28,
				size * 0.16,
			).data;
			let r = 0;
			let g = 0;
			let b = 0;
			let n = 0;
			for (let i = 0; i < d.length; i += 4) {
				if (d[i + 3]! > 200) {
					r += d[i]!;
					g += d[i + 1]!;
					b += d[i + 2]!;
					n += 1;
				}
			}
			if (n > 0) {
				skin = `rgb(${Math.round(r / n)}, ${Math.round(g / n)}, ${Math.round(b / n)})`;
			}
		} catch {
			// A tainted canvas: no sampling.
		}
	}
	return {
		sprite: {
			img: canvas,
			cx: size / 2,
			cy: size / 2,
			h: size * 1.02,
			photo: true,
		},
		skin,
	};
};

export type HeadLook = {
	sprite?: HeadSprite;
	skin: string;
	hair: string;
};

const DEFAULT_SKIN = "#b07a52";
const DEFAULT_HAIR = "#1f1612";

// Skin and hair for drawing the rest of him, straight from the face config;
// the sprite itself arrives later.
export const headColors = (
	face: FaceConfig | undefined,
): { skin: string; hair: string } => {
	const skin = face?.body?.color || DEFAULT_SKIN;
	const bald = !face?.hair?.id || face.hair.id === "bald";
	return {
		skin,
		hair: bald ? skin : face.hair.color || DEFAULT_HAIR,
	};
};

export const loadHead = async (
	face: FaceConfig | undefined,
	imgURL: string | undefined,
	colors: [string, string, string] | undefined,
): Promise<{ sprite?: HeadSprite; skin?: string }> => {
	try {
		if (imgURL) {
			return await photoSprite(imgURL);
		}
		if (face) {
			return { sprite: await faceSprite(face, colors) };
		}
	} catch {
		// A face that will not draw just leaves him with a plain head.
	}
	return {};
};
