// A team's custom uniform. faces.js draws jerseys from a fixed table of
// finished SVG strings with three color slots, which is why "make it look like
// a real jersey" was impossible: the jersey body color IS the team's primary
// color, and the collar and armholes are one path sharing one trim. This spec
// describes the jersey as parts - body color, collar bands, armhole bands,
// yoke, chest band, pinstripes, a printed image - and buildJerseySvg() turns it
// into an SVG string that gets registered into the faces.js table at render
// time, so the library draws it like any preset.
//
// STORAGE: serialized INSIDE the team's existing `jersey` string, behind a
// prefix. The jersey string already flows as an opaque value through every
// render site, the per-season team snapshots, league import/export, cloud
// sync, real team info, scheduled events and expansion drafts - encoding the
// spec in it means all of that keeps working untouched, and a team's uniform
// history is preserved season by season for free.
//
// SAFETY: everything embedded into the SVG string is sanitized here - colors
// must be hex, the image URL is scheme-checked and XML-escaped - because
// league files are shared between people and the string is ultimately
// inserted into the DOM.

export type UniformTrim = {
	color: string;
	// Stroke width in face units (the whole face is 400 wide). Bands stack in
	// array order, first at the bottom, so [black 16, green 9] reads as a green
	// band with a black rim - the same construction the presets use.
	width: number;
};

export type UniformImage = {
	url: string;
	scale?: number; // 1 = the designed slot size
	dx?: number; // face units
	dy?: number;
	opacity?: number; // 0-1
	stretch?: boolean; // stretch to the slot instead of covering it
};

export type UniformSpec = {
	base?: string; // jersey color; default the team's first color
	collar?: UniformTrim[]; // bands along the neckline
	arm?: UniformTrim[]; // bands along the armholes
	yoke?: string; // solid shoulder yoke
	band?: string; // horizontal chest band above the hem
	pinstripes?: { color: string; gap?: number; width?: number };
	image?: UniformImage; // printed on the jersey, clipped to it
	// The rest is only drawn where the whole player is shown:
	shorts?: {
		base?: string; // default the jersey color
		belt?: string; // waistband
		side?: string; // side-panel stripe
		trim?: UniformTrim[]; // leg-opening bands
	};
	wordmark?: { text?: string; color?: string; outline?: string };
	number?: { color?: string; outline?: string };
};

export const UNIFORM_JERSEY_PREFIX = "uniform1:";

export const isUniformJersey = (jersey: string | undefined): boolean =>
	typeof jersey === "string" && jersey.startsWith(UNIFORM_JERSEY_PREFIX);

// #rgb, #rgba, #rrggbb or #rrggbbaa - the editor only produces the third, but
// hand-edited league files get the same latitude CSS gives hex.
const HEX = /^#(?:[\da-f]{3,4}|[\da-f]{6}|[\da-f]{8})$/i;

export const safeColor = (
	color: unknown,
	fallback?: string,
): string | undefined => {
	if (typeof color === "string" && HEX.test(color)) {
		return color;
	}
	return fallback;
};

const clamp = (
	value: unknown,
	min: number,
	max: number,
): number | undefined => {
	if (typeof value !== "number" || !Number.isFinite(value)) {
		return undefined;
	}
	return Math.min(max, Math.max(min, value));
};

const cleanTrims = (trims: unknown): UniformTrim[] | undefined => {
	if (!Array.isArray(trims)) {
		return undefined;
	}
	const out: UniformTrim[] = [];
	for (const trim of trims.slice(0, 4)) {
		const color = safeColor((trim as any)?.color);
		const width = clamp((trim as any)?.width, 1, 24);
		if (color !== undefined && width !== undefined) {
			out.push({ color, width });
		}
	}
	return out.length > 0 ? out : undefined;
};

// The image transforms are clamped tighter than "any finite number" on
// purpose: the print is drawn as a pattern fill with a 400x700 tile, and these
// bounds keep a shifted or shrunken image's neighboring tiles from ever
// reaching the jersey.
const cleanImage = (image: unknown): UniformImage | undefined => {
	const url = (image as any)?.url;
	if (
		typeof url !== "string" ||
		url.length > 2000 ||
		!/^(?:https?:\/\/|data:image\/)/i.test(url)
	) {
		return undefined;
	}
	const out: UniformImage = { url };
	const scale = clamp((image as any).scale, 0.2, 4);
	const dx = clamp((image as any).dx, -100, 100);
	const dy = clamp((image as any).dy, -100, 100);
	const opacity = clamp((image as any).opacity, 0, 1);
	if (scale !== undefined && scale !== 1) {
		out.scale = scale;
	}
	if (dx !== undefined && dx !== 0) {
		out.dx = dx;
	}
	if (dy !== undefined && dy !== 0) {
		out.dy = dy;
	}
	if (opacity !== undefined && opacity !== 1) {
		out.opacity = opacity;
	}
	if ((image as any).stretch === true) {
		out.stretch = true;
	}
	return out;
};

// Keep only well-formed fields. A field that doesn't validate is dropped
// rather than failing the whole spec, so a slightly-off hand edit degrades to
// a plainer jersey instead of no jersey.
export const cleanUniformSpec = (raw: unknown): UniformSpec => {
	if (typeof raw !== "object" || raw === null) {
		return {};
	}
	const spec = raw as Record<string, unknown>;
	const out: UniformSpec = {};

	const base = safeColor(spec.base);
	if (base !== undefined) {
		out.base = base;
	}
	const collar = cleanTrims(spec.collar);
	if (collar) {
		out.collar = collar;
	}
	const arm = cleanTrims(spec.arm);
	if (arm) {
		out.arm = arm;
	}
	const yoke = safeColor(spec.yoke);
	if (yoke !== undefined) {
		out.yoke = yoke;
	}
	const band = safeColor(spec.band);
	if (band !== undefined) {
		out.band = band;
	}

	const pinColor = safeColor((spec.pinstripes as any)?.color);
	if (pinColor !== undefined) {
		out.pinstripes = { color: pinColor };
		const gap = clamp((spec.pinstripes as any).gap, 6, 60);
		const width = clamp((spec.pinstripes as any).width, 1, 12);
		if (gap !== undefined) {
			out.pinstripes.gap = gap;
		}
		if (width !== undefined) {
			out.pinstripes.width = width;
		}
	}

	const image = cleanImage(spec.image);
	if (image) {
		out.image = image;
	}

	if (typeof spec.shorts === "object" && spec.shorts !== null) {
		const shorts: NonNullable<UniformSpec["shorts"]> = {};
		const sBase = safeColor((spec.shorts as any).base);
		const belt = safeColor((spec.shorts as any).belt);
		const side = safeColor((spec.shorts as any).side);
		const trim = cleanTrims((spec.shorts as any).trim);
		if (sBase !== undefined) {
			shorts.base = sBase;
		}
		if (belt !== undefined) {
			shorts.belt = belt;
		}
		if (side !== undefined) {
			shorts.side = side;
		}
		if (trim) {
			shorts.trim = trim;
		}
		if (Object.keys(shorts).length > 0) {
			out.shorts = shorts;
		}
	}

	if (typeof spec.wordmark === "object" && spec.wordmark !== null) {
		const wordmark: NonNullable<UniformSpec["wordmark"]> = {};
		const text = (spec.wordmark as any).text;
		if (typeof text === "string" && text.trim().length > 0) {
			wordmark.text = text.trim().slice(0, 16);
		}
		const color = safeColor((spec.wordmark as any).color);
		const outline = safeColor((spec.wordmark as any).outline);
		if (color !== undefined) {
			wordmark.color = color;
		}
		if (outline !== undefined) {
			wordmark.outline = outline;
		}
		if (Object.keys(wordmark).length > 0) {
			out.wordmark = wordmark;
		}
	}

	if (typeof spec.number === "object" && spec.number !== null) {
		const number: NonNullable<UniformSpec["number"]> = {};
		const color = safeColor((spec.number as any).color);
		const outline = safeColor((spec.number as any).outline);
		if (color !== undefined) {
			number.color = color;
		}
		if (outline !== undefined) {
			number.outline = outline;
		}
		if (Object.keys(number).length > 0) {
			out.number = number;
		}
	}

	return out;
};

// undefined means "not a custom uniform" - a preset id, or garbage that should
// fall back to the preset path.
export const parseUniform = (
	jersey: string | undefined,
): UniformSpec | undefined => {
	if (!isUniformJersey(jersey)) {
		return undefined;
	}
	let raw;
	try {
		raw = JSON.parse(jersey!.slice(UNIFORM_JERSEY_PREFIX.length));
	} catch {
		return undefined;
	}
	return cleanUniformSpec(raw);
};

export const serializeUniform = (spec: UniformSpec): string =>
	UNIFORM_JERSEY_PREFIX + JSON.stringify(cleanUniformSpec(spec));

// The five basketball presets, as specs, so the editor can start from what the
// team already wears. Approximate where a preset uses offset paths instead of
// stacked strokes (jersey4/jersey5) - these are starting points for editing,
// not pixel-faithful conversions.
export const presetToSpec = (
	jersey: string | undefined,
	colors: [string, string, string],
): UniformSpec => {
	const black = "#000000";
	const c0 = safeColor(colors[0], "#666666")!;
	const c1 = safeColor(colors[1], "#cccccc")!;
	const c2 = safeColor(colors[2], "#333333")!;
	switch (jersey) {
		case "jersey2":
			return {
				collar: [
					{ color: black, width: 16 },
					{ color: c2, width: 12 },
					{ color: c1, width: 6 },
				],
				arm: [
					{ color: black, width: 16 },
					{ color: c2, width: 12 },
					{ color: c1, width: 6 },
				],
			};
		case "jersey3":
			return {
				band: c1,
				collar: [
					{ color: black, width: 16 },
					{ color: c0, width: 12 },
					{ color: c2, width: 6 },
				],
				arm: [
					{ color: black, width: 16 },
					{ color: c0, width: 12 },
					{ color: c2, width: 6 },
				],
			};
		case "jersey4":
			return {
				collar: [
					{ color: black, width: 16 },
					{ color: c2, width: 8 },
					{ color: c1, width: 4 },
				],
				arm: [
					{ color: black, width: 16 },
					{ color: c2, width: 8 },
					{ color: c1, width: 4 },
				],
			};
		case "jersey5":
			return {
				pinstripes: { color: c2, gap: 10, width: 2 },
				collar: [
					{ color: black, width: 16 },
					{ color: c2, width: 8 },
					{ color: c1, width: 4 },
				],
				arm: [
					{ color: black, width: 16 },
					{ color: c2, width: 8 },
					{ color: c1, width: 4 },
				],
			};
		default:
			// "jersey" (Plain), or anything unrecognized.
			return {};
	}
};

// ----- Geometry -----
//
// The faces.js jersey lives at the bottom of its 400x600 canvas. The
// silhouette's top edge dips along the neckline, so clipping to it notches the
// collar automatically. Everything here is symmetric about x=200 on purpose:
// faces.js scales the jersey horizontally about its bounding box center for
// body size, and symmetric geometry keeps that center at 200 no matter what a
// user adds.

export const JERSEY_SILHOUETTE =
	"M80 610s10-30 10-90l20-10s10 80 90 80 90-80 90-80l20 10c0 60 10 90 10 90z";
export const JERSEY_NECK = "M110 510s10 80 90 80 90-80 90-80";
export const JERSEY_ARMS = "M90 520c0 60-10 90-10 90M310 520c0 60 10 90 10 90";

const XML_ESCAPES: Record<string, string> = {
	"&": "&amp;",
	"<": "&lt;",
	">": "&gt;",
	'"': "&quot;",
	"'": "&apos;",
};

export const escapeXml = (s: string): string =>
	s.replaceAll(/["&'<>]/g, (c) => XML_ESCAPES[c]!);

const trimStrokes = (trims: UniformTrim[] | undefined, d: string): string => {
	if (!trims) {
		return "";
	}
	let out = "";
	for (const { color, width } of trims) {
		out += `<path fill="none" stroke="${color}" stroke-width="${width}" d="${d}"/>`;
	}
	return out;
};

// The jersey as one SVG string, ready for the faces.js table. idBase keys the
// clip path and pattern ids - the caller derives it from the content, so two
// identical jerseys on one page share identical defs and different jerseys
// never collide.
export const buildJerseySvg = (
	specRaw: UniformSpec,
	teamColors: [string, string, string],
	idBase: string,
): string => {
	const spec = cleanUniformSpec(specRaw);
	const base = spec.base ?? safeColor(teamColors[0], "#888888")!;

	let s = `<path fill="${base}" stroke="#000" stroke-width="6" d="${JERSEY_SILHOUETTE}"/>`;

	const needsClip =
		spec.yoke !== undefined ||
		spec.band !== undefined ||
		spec.pinstripes !== undefined;
	if (needsClip) {
		s += `<clipPath id="${idBase}c"><path d="${JERSEY_SILHOUETTE}"/></clipPath>`;
	}
	const clip = `clip-path="url(#${idBase}c)"`;

	if (spec.yoke !== undefined) {
		s += `<rect x="60" y="480" width="280" height="58" fill="${spec.yoke}" ${clip}/>`;
	}

	if (spec.pinstripes) {
		const { color, gap = 14, width = 3 } = spec.pinstripes;
		let d = "";
		// Symmetric about 200: walk outward from the center line.
		for (let off = 0; 200 - off >= 84; off += gap) {
			d += `M${200 - off} 505v110`;
			if (off > 0) {
				d += `M${200 + off} 505v110`;
			}
		}
		s += `<path fill="none" stroke="${color}" stroke-width="${width}" d="${d}" ${clip}/>`;
	}

	if (spec.band !== undefined) {
		s += `<rect x="76" y="572" width="248" height="30" fill="${spec.band}" ${clip}/>`;
	}

	if (spec.image) {
		const { url, scale = 1, dx = 0, dy = 0, opacity = 1, stretch } = spec.image;
		// A pattern fill instead of a clipped <image>, for two reasons: the clip
		// happens for free, and the jersey group's bounding box stays that of the
		// silhouette - faces.js centers its body-size scaling on that box, and a
		// stray <image> extent would pull the whole jersey off center.
		s +=
			`<pattern id="${idBase}p" patternUnits="userSpaceOnUse" x="0" y="0" width="400" height="700" patternTransform="translate(${dx} ${dy})">` +
			`<g transform="translate(200 556) scale(${scale}) translate(-200 -556)">` +
			`<image href="${escapeXml(url)}" x="78" y="499" width="244" height="112" preserveAspectRatio="${stretch ? "none" : "xMidYMid slice"}" opacity="${opacity}"/>` +
			`</g></pattern>` +
			`<path d="${JERSEY_SILHOUETTE}" fill="url(#${idBase}p)"/>`;
	}

	s += trimStrokes(spec.collar, JERSEY_NECK);
	s += trimStrokes(spec.arm, JERSEY_ARMS);

	return s;
};
