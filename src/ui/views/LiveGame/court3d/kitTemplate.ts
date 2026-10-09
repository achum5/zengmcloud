import {
	makeCamera,
	MAIN_RIG,
	project,
	REPLAY_RIG,
	type Rig,
} from "./camera.ts";
import type { PlayerState } from "./evaluate.ts";
import { kitsFor, type Look } from "./figure.ts";
import {
	ART_GUIDE,
	ART_H,
	ART_JERSEY,
	ART_PANELS,
	ART_SHORTS,
	ART_SWATCHES,
	ART_W,
	artAt,
	type ArtWrap,
	type KitArt,
} from "./kitArt.ts";
import { bodyOf } from "./poses.ts";
import { LETTERING, sculpt, wrapsFor } from "./sculpt.ts";

// THE TEMPLATE A TEAM'S UNIFORM IS PAINTED ON (see kitArt.ts): the jersey's
// and the shorts' panels, clear wherever they show on a player, magenta
// where they never do and where the trim goes, with the panels, his number
// and name and the swatches marked out in magenta too - all of which a
// picture loses when it is read.

// Which of the picture's pixels show on a player (1), and which the trim
// covers (2): him seen from all round, from up in the stands and from low
// down, arms up and legs wide, so his sides and the insides of his legs
// count.
const seenOnHim = async (): Promise<Uint8Array> => {
	const seen = new Uint8Array(ART_W * ART_H);
	const body = bodyOf();
	const art: KitArt = {
		id: "template",
		data: new Uint8ClampedArray(ART_W * ART_H * 4),
		scale: 1,
	};
	const look: Look = {
		kit: kitsFor(undefined, undefined)[0],
		kitArt: art,
		skin: "#8d5524",
		hair: "#1f1612",
		jerseyNumber: "",
		name: "",
		lastName: "",
		wordmark: "",
	};
	const views: [Rig, string, number][] = [
		[MAIN_RIG, "rebound", 12],
		[MAIN_RIG, "guard", 12],
		[REPLAY_RIG, "guard", 8],
	];
	for (const [rig, anim, turns] of views) {
		const viewW = 600;
		const viewH = 800;
		const cam = makeCamera(
			{ x: 47, width: viewW / 70, y: 25, z: 3.6 },
			viewW,
			viewH,
			rig,
		);
		const base = project(cam, { x: 47, y: 25.5, z: 0 });
		const k = base.k;
		const left = base.x - (body.H * 0.62 + 0.6) * k;
		const top = base.y - (body.H * 1.4 + 0.5) * k;
		const w = Math.ceil((body.H * 1.24 + 1.2) * k);
		const h = Math.ceil((body.H * 1.4 + 1.4) * k);
		for (let i = 0; i < turns; i++) {
			const st = {
				pid: 0,
				team: 0,
				shown: true,
				x: 47,
				y: 25.5,
				z: 0,
				yaw: ((i + 0.5) / turns) * Math.PI * 2,
				anim,
				phase: anim === "rebound" ? 0.5 : 0,
				moving: false,
				holding: false,
			} as PlayerState;
			sculpt(cam, st, body, look, 1, left, top, w, h, seen);
			// A breath between turns, so the page doesn't stall.
			await new Promise((resolve) => setTimeout(resolve, 0));
		}
	}
	// Close the gaps between the pixels he showed: one mostly surrounded by
	// pixels that showed, showed too.
	let from = seen;
	for (let pass = 0; pass < 3; pass++) {
		const out = from.slice();
		for (let y = 1; y < ART_H - 1; y++) {
			for (let x = 1; x < ART_W - 1; x++) {
				const i = y * ART_W + x;
				if (from[i] !== 0) {
					continue;
				}
				let n = 0;
				let most = 0;
				for (const j of [
					i - ART_W - 1,
					i - ART_W,
					i - ART_W + 1,
					i - 1,
					i + 1,
					i + ART_W - 1,
					i + ART_W,
					i + ART_W + 1,
				]) {
					if (from[j]! > 0) {
						n++;
						most = Math.max(most, from[j]!);
					}
				}
				if (n >= 4) {
					out[i] = most;
				}
			}
		}
		from = out;
	}
	return from;
};

// A box on the picture round his lettering: `size` heights tall at `u`
// torso lengths up him, `maxW` sheet half-widths across, on his front or
// his back.
const letteringBox = (
	wrap: ArtWrap,
	at: { u: number; size: number; maxW: number },
	front: boolean,
) => {
	const body = bodyOf();
	const aS = body.shoulderW * 1.1;
	const half = aS * Math.sin(at.maxW / 2);
	const u = body.torso * at.u;
	const tall = body.H * at.size * 0.38;
	const xy = new Float64Array(2);
	const F = front ? 1 : -1;
	artAt(wrap, u + tall, front ? -half : half, F, true, xy);
	const x0 = xy[0]!;
	const y0 = xy[1]!;
	artAt(wrap, u - tall, front ? half : -half, F, true, xy);
	return { x: x0, y: y0, w: xy[0]! - x0, h: xy[1]! - y0 };
};

export const kitTemplate = async (): Promise<HTMLCanvasElement> => {
	const seen = await seenOnHim();
	const cv = document.createElement("canvas");
	cv.width = ART_W;
	cv.height = ART_H;
	const g = cv.getContext("2d")!;
	const img = g.createImageData(ART_W, ART_H);
	const d = img.data;
	// Where nothing shows, a check; where the trim goes, solid.
	for (const band of [ART_JERSEY, ART_SHORTS]) {
		for (let y = band.y; y < band.y + band.h; y++) {
			for (let x = 0; x < ART_W; x++) {
				const i = y * ART_W + x;
				if (seen[i] === 1) {
					continue;
				}
				const dark = seen[i] === 2 || ((x >> 2) + (y >> 2)) % 2 === 0;
				d[i * 4] = dark ? 255 : 236;
				d[i * 4 + 1] = 0;
				d[i * 4 + 2] = dark ? 255 : 236;
				d[i * 4 + 3] = 255;
			}
		}
	}
	g.putImageData(img, 0, 0);
	g.fillStyle = ART_GUIDE;
	g.strokeStyle = ART_GUIDE;
	// The panels, marked off, and named in the gap under each band.
	g.textAlign = "center";
	g.textBaseline = "top";
	g.font = "bold 8px sans-serif";
	for (const band of [ART_JERSEY, ART_SHORTS]) {
		for (const [label, p] of [
			["R", ART_PANELS.right],
			[band === ART_JERSEY ? "JERSEY FRONT" : "SHORTS FRONT", ART_PANELS.front],
			["L", ART_PANELS.left],
			[band === ART_JERSEY ? "JERSEY BACK" : "SHORTS BACK", ART_PANELS.back],
		] as const) {
			if (p.x > 0) {
				g.fillRect(p.x, band.y, 1, band.h + 8);
			}
			g.fillText(label, p.x + p.w / 2, band.y + band.h);
		}
	}
	// His number on his chest; his name and number on his back.
	const { jersey } = wrapsFor(bodyOf());
	g.setLineDash([3, 2]);
	g.lineWidth = 1;
	for (const [at, front] of [
		[LETTERING.number, true],
		[LETTERING.name, false],
		[LETTERING.backNumber, false],
	] as const) {
		const b = letteringBox(jersey, at, front);
		g.strokeRect(
			Math.round(b.x) + 0.5,
			Math.round(b.y) + 0.5,
			Math.round(b.w),
			Math.round(b.h),
		);
	}
	g.setLineDash([]);
	// The swatches: his number, its edge, his name, the trim.
	const S = ART_SWATCHES.size;
	g.textAlign = "left";
	g.textBaseline = "middle";
	g.font = "bold 8px sans-serif";
	for (const [label, x] of [
		["NUMBER", ART_SWATCHES.number],
		["EDGE", ART_SWATCHES.numberEdge],
		["NAME", ART_SWATCHES.name],
		["TRIM", ART_SWATCHES.trim],
	] as const) {
		g.strokeRect(x - 0.5, ART_SWATCHES.y - 0.5, S + 1, S + 1);
		g.fillText(label, x + S + 3, ART_SWATCHES.y + S / 2);
	}
	return cv;
};
