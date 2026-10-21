import { display, type FaceConfig } from "facesjs";
import { stubbleShave } from "../../../../common/stubbleShade.ts";

// EACH PLAYER'S HEAD, ready to draw: his faces.js face drawn once into a
// small canvas, cropped to the head, so the court can stamp it on
// his shoulders every frame for the cost of one image copy.

export type HeadSprite = {
	img: HTMLCanvasElement;
	// The middle of his face in it, and its height from crown to chin (px).
	cx: number;
	cy: number;
	h: number;
};

// faces.js draws in a 400 x 600 box: the head is centered near (200, 300),
// about 400 units from the top of the hair to the chin, with the shoulders and
// jersey below it.
const CROP = { x: 20, y: 60, w: 360, h: 445 };
const FACE_CENTER = { x: 200, y: 300 };
const FACE_H = 400;
const SCALE = 0.6;

const loadImage = (src: string) =>
	new Promise<HTMLImageElement>((resolve, reject) => {
		const img = new Image();
		img.crossOrigin = "anonymous";
		img.decoding = "async";
		img.onload = () => {
			resolve(img);
		};
		img.onerror = () => {
			reject(new Error("image failed"));
		};
		img.src = src;
	});

// Nobody plays in a cap (or a Santa hat): off it comes. Headbands and eye
// black stay - they belong on a court.
const CAPS = new Set(["hat", "hat2", "hat3", "santa-hat"]);

const faceSprite = async (
	face: FaceConfig,
	colors: [string, string, string] | undefined,
): Promise<HeadSprite> => {
	const shave = stubbleShave(face.head?.shave, face.hair?.color);
	const overrides: Record<string, unknown> = {};
	if (colors) {
		overrides.teamColors = colors;
	}
	if (CAPS.has(face.accessories?.id)) {
		overrides.accessories = { id: "none" };
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
		// Just his head: no shoulders or jersey, no neck - the body draws its own.
		display(holder, structuredClone(face), {
			...overrides,
			body: { id: "none" },
			jersey: { id: "none" },
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
		const img = await loadImage(url);
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
		return {
			img: canvas,
			cx: (FACE_CENTER.x - CROP.x) * SCALE,
			cy: (FACE_CENTER.y - CROP.y) * SCALE,
			h: FACE_H * SCALE,
		};
	} finally {
		URL.revokeObjectURL(url);
	}
};

// How his hair sits on the back of his head, for drawing it from the side
// and from behind: none; cropped close to his skull (or standing up off the
// top of it - see HairStyle); full round his head, over his ears (an afro,
// a shag); or hanging down past his neck (long hair).
export type HairCut = "bald" | "short" | "big" | "long";
const BIG_HAIR = /^(afro$|shaggy|emo$)/;
const LONG_HAIR = /^(longHair|female(?!8$))/;
export const hairCut = (id: string | undefined): HairCut =>
	!id || id === "bald"
		? "bald"
		: LONG_HAIR.test(id)
			? "long"
			: BIG_HAIR.test(id)
				? "big"
				: "short";

// The shape of his hair beyond that, as his face has it, for the side and
// the back of his head to match: how high it stands off his skull (head
// radii), and its top - smooth, curly, spiky, or cut flat (a high top). And
// whether it is faded at the sides and back, or thin all over (a buzz);
// in rows back over his skull (cornrows); a strip down the middle, the sides
// faded (a faux-hawk); bald on top, round the sides only; or gathered up in
// a bun (locs, tied up).
export type HairStyle = {
	height: number;
	top: "smooth" | "curly" | "spiky" | "flat";
	fade?: boolean;
	thin?: boolean;
	rows?: boolean;
	strip?: boolean;
	crown?: boolean;
	bun?: boolean;
};
const STYLES: [RegExp, HairStyle][] = [
	[/^afro$/, { height: 0.42, top: "curly" }],
	[/^afro2$/, { height: 0.3, top: "spiky" }],
	[/^blowoutFade$/, { height: 0.28, top: "spiky", fade: true }],
	[/^cornrows$/, { height: 0.04, top: "smooth", rows: true }],
	[/^crop-fade/, { height: 0.08, top: "smooth", fade: true }],
	[/^curlyFade/, { height: 0.2, top: "curly", fade: true }],
	[/^curly/, { height: 0.26, top: "curly" }],
	[/^dreads$/, { height: 0.12, top: "curly", bun: true }],
	[/^faux-?hawk/, { height: 0.34, top: "spiky", strip: true, fade: true }],
	[/^female8$/, { height: 0.1, top: "smooth", bun: true }],
	[/^(high|juice)$/, { height: 0.58, top: "flat" }],
	[/^tall-fade$/, { height: 0.46, top: "flat", fade: true }],
	[/^(messy|spike)/, { height: 0.24, top: "spiky" }],
	[/^shaggy/, { height: 0.2, top: "spiky" }],
	[/^short-bald$/, { height: 0.04, top: "smooth", crown: true }],
	[/^short-fade/, { height: 0.02, top: "smooth", thin: true }],
];
const CROPPED: HairStyle = { height: 0.08, top: "smooth" };
export const hairStyle = (id: string | undefined): HairStyle =>
	STYLES.find(([re]) => re.test(id ?? ""))?.[1] ?? CROPPED;

// What of his face still shows side on, where the face itself does not: a
// beard - along his jaw, on his chin, over his lip, or sideburns - a
// headband (low on his brow, or high), eye black.
export type Profile = {
	jaw?: boolean;
	chin?: boolean;
	lip?: boolean;
	burns?: boolean;
	band?: { high: boolean; color: string; stripe: string };
	eyeBlack?: boolean;
	// How big his nose and his ears are (1 about average), and how full his
	// face (0 lean to 1).
	nose?: number;
	ear?: number;
	full?: number;
};

export const profileOf = (
	face: FaceConfig | undefined,
	colors?: [string, string, string],
): Profile => {
	const out: Profile = {};
	const beard = face?.facialHair?.id ?? "none";
	if (beard !== "none") {
		const stache = /stache|^mustache|^beard|^fullgoatee/i.test(beard);
		out.lip = stache;
		out.jaw = /^(beard|neckbeard|honest-abe|chin-strap|logan|mutton)/.test(
			beard,
		);
		out.chin =
			/^(beard|fullgoatee|goatee|soul|neckbeard|honest-abe|chin-strap)/.test(
				beard,
			) || /goatee|soul/i.test(beard);
		out.burns =
			out.jaw || /^(sideburns|wilt|harl|mutton|logan)|SB\d/.test(beard);
	}
	const acc = face?.accessories?.id ?? "none";
	if (acc === "headband" || acc === "headband-high") {
		out.band = {
			high: acc === "headband-high",
			color: colors?.[0] ?? "#ffffff",
			stripe: colors?.[1] ?? "#ffffff",
		};
	}
	if (acc === "eye-black") {
		out.eyeBlack = true;
	}
	const nose = face?.nose;
	if (nose && typeof nose.size === "number") {
		out.nose =
			(0.6 + 0.5 * nose.size) *
			(nose.id === "honker" || nose.id === "pinocchio" ? 1.25 : 1);
	}
	if (face?.ear && typeof face.ear.size === "number") {
		out.ear = face.ear.size;
	}
	if (typeof face?.fatness === "number") {
		out.full = face.fatness;
	}
	return out;
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
): { skin: string; hair: string; cut: HairCut; style: HairStyle } => {
	const skin = face?.body?.color || DEFAULT_SKIN;
	const cut = face ? hairCut(face.hair?.id) : "short";
	return {
		skin,
		hair: cut === "bald" ? skin : face?.hair?.color || DEFAULT_HAIR,
		cut,
		style: hairStyle(face?.hair?.id),
	};
};

export const loadHead = async (
	face: FaceConfig | undefined,
	colors: [string, string, string] | undefined,
): Promise<{ sprite?: HeadSprite; skin?: string }> => {
	try {
		if (face) {
			return { sprite: await faceSprite(face, colors) };
		}
	} catch {
		// A face that will not draw just leaves him with a plain head.
	}
	return {};
};
