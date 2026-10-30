import { display, generate } from "facesjs";

// THE CROWD'S FACES. A few dozen faces.js faces - men and women, every kind
// of face and hair - each drawn once, small, just the head, for the stands to
// give one to every fan in a seat (see paintStands). The stands are painted
// once a game, so a face costs nothing a frame: it is in the picture.

export type CrowdFace = {
	img: HTMLCanvasElement;
	// His skin, for his arms when he is up out of his seat.
	skin: string;
};

const POOL = 48;
const WOMEN = 0.35;
// A cap now and then, in a color of anybody's - not faces.js's own run of
// them (a Santa hat, a player's headband, eye black).
const CAPS = ["hat", "hat2", "hat3"];
const CAP_SHARE = 0.15;
const CAP_COLORS = [
	"#1d2330",
	"#f2f2f0",
	"#7a1319",
	"#23466e",
	"#3d4a2f",
	"#c9a227",
];
const pick = <T>(a: T[]) => a[Math.floor(Math.random() * a.length)]!;
// faces.js draws in a 400 x 600 box, the head about 400 units from the top
// of the hair to the chin: just that, drawn this many px across (a few times
// what a fan's head comes to in the stands, scaled down into it smoothly).
const CROP = { x: 20, y: 60, w: 360, h: 445 };
const PX_W = 36;

const loadImage = (src: string) =>
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

const faceOf = async (n: number): Promise<CrowdFace | undefined> => {
	const face = generate(undefined, {
		gender: (n * 0.618) % 1 < WOMEN ? "female" : "male",
	});
	face.accessories = {
		id: Math.random() < CAP_SHARE ? pick(CAPS) : "none",
	};
	face.teamColors = [pick(CAP_COLORS), pick(CAP_COLORS), pick(CAP_COLORS)];
	// (faces.js measures its parts as it draws, so it draws into the page, out
	// of sight, and the result is copied out as an image - as a player's head
	// is: see faces.ts.)
	const holder = document.createElement("div");
	holder.style.cssText =
		"position:fixed;left:-10000px;top:0;width:400px;height:600px;visibility:hidden;pointer-events:none";
	document.body.append(holder);
	let svg: string;
	try {
		display(holder, face, {
			body: { id: "none" },
			jersey: { id: "none" },
		} as any);
		svg = holder.innerHTML
			.replace('width="100%"', 'width="400"')
			.replace('height="100%"', 'height="600"');
	} catch {
		return undefined;
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
		const img = await loadImage(url);
		const canvas = document.createElement("canvas");
		canvas.width = PX_W;
		canvas.height = Math.round((PX_W * CROP.h) / CROP.w);
		const ctx = canvas.getContext("2d")!;
		ctx.imageSmoothingQuality = "high";
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
		return { img: canvas, skin: face.body?.color ?? "#d9a77f" };
	} catch {
		return undefined;
	} finally {
		URL.revokeObjectURL(url);
	}
};

// Made once for the page, a face at a time - out of the way of anything else
// going on.
let pool: Promise<CrowdFace[]> | undefined;
export const crowdFaces = (): Promise<CrowdFace[]> =>
	(pool ??= (async () => {
		const out: CrowdFace[] = [];
		for (let n = 0; n < POOL; n++) {
			const f = await faceOf(n);
			if (f) {
				out.push(f);
			}
			await new Promise((resolve) => setTimeout(resolve, 0));
		}
		return out;
	})());
