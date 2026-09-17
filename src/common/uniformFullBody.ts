// The player below the neck. faces.js stops at the shoulders - its canvas is
// 400x600 and the jersey is a strip at the bottom - so the full-body view
// draws everything from there down as its own SVG and slots the faces.js face
// on top. Coordinates share the face's x axis one-to-one; the seam sits at
// y=600, and every shape here starts a few units above it so the face's own
// opaque body/jersey fill hides the join.
//
// Same string-building rules as the jersey: everything embedded is sanitized
// (hex colors, escaped text), because this ends up in the DOM and league data
// is shared between people.

import {
	cleanUniformSpec,
	escapeXml,
	safeColor,
	type UniformSpec,
	type UniformTrim,
} from "./uniform.ts";

export const FULL_BODY_VIEWBOX = "0 0 400 1300";
export const FULL_BODY_HEIGHT = 1300;

// Rough perceived luminance, for picking a readable default text color on the
// jersey. Good enough for #rrggbb; #rgb is expanded first.
const luminance = (hex: string): number => {
	let h = hex.slice(1);
	if (h.length === 3 || h.length === 4) {
		h = [...h].map((c) => c + c).join("");
	}
	const r = Number.parseInt(h.slice(0, 2), 16);
	const g = Number.parseInt(h.slice(2, 4), 16);
	const b = Number.parseInt(h.slice(4, 6), 16);
	return (0.299 * r + 0.587 * g + 0.114 * b) / 255;
};

export const contrastColor = (hex: string): string =>
	luminance(hex) > 0.55 ? "#000000" : "#ffffff";

const OUTLINE = 'stroke="#000" stroke-width="6" stroke-linejoin="round"';

// ----- The fixed anatomy, symmetric about x=200 -----
//
// Torso: continues the jersey from the seam down to the waist.
const TORSO = "M80 592Q84 726 92 840L308 840Q316 726 320 592Z";
// Arms: continue the face's bare shoulders (its body edge hits the seam at
// x=10/390) down to the wrists, hanging straight in the faces.js pose.
const ARM_LEFT = "M10 592Q18 736 34 872L72 872Q78 736 80 592Z";
const ARM_RIGHT = "M390 592Q382 736 366 872L328 872Q322 736 320 592Z";
const HAND_LEFT = "M32 870q-8 54 20 58t24 -58z";
const HAND_RIGHT = "M368 870q8 54 -20 58t-24 -58z";
// Shorts: waistband, then two baggy legs meeting at an inverted V.
const WAISTBAND = "M88 840L312 840L315 872L85 872Z";
const SHORTS =
	"M85 872L315 872Q327 946 331 1022L212 1022Q205 992 200 972Q195 992 188 1022L69 1022Q73 946 85 872Z";
// The bottom edges of the shorts legs, for trim bands.
const SHORTS_HEM = "M69 1022L188 1022M212 1022L331 1022";
// The outer side seams, for a side stripe.
const SHORTS_SIDE = "M85 872Q73 946 69 1022M315 872Q327 946 331 1022";
// Legs: from inside the shorts down to the ankles.
const LEG_LEFT = "M96 1010Q100 1120 110 1190L150 1190Q158 1120 160 1010Z";
const LEG_RIGHT = "M304 1010Q300 1120 290 1190L250 1190Q242 1120 240 1010Z";
// Socks: a band across each leg's taper.
const SOCK_LEFT = "M103 1140Q106 1170 110 1192L150 1192Q154 1170 156 1140Z";
const SOCK_RIGHT = "M297 1140Q294 1170 290 1192L250 1192Q246 1170 244 1140Z";
// Shoes: front-facing sneakers, slightly splayed.
const SHOE_LEFT =
	"M106 1188q-26 10 -28 42q-2 26 30 26l40 0q18 0 18 -22l0 -46q-30 10 -60 0z";
const SHOE_RIGHT =
	"M294 1188q26 10 28 42q2 26 -30 26l-40 0q-18 0 -18 -22l0 -46q30 10 60 0z";
// A thin dark line where the sole meets the floor shadow.
const SHOE_SOLE = "M80 1244q46 16 84 6M320 1244q-46 16 -84 6";

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

export const buildFullBodySvg = ({
	spec: specRaw,
	teamColors,
	skinColor,
	bodySize = 1,
	hgt,
	jerseyNumber,
	idBase,
}: {
	spec: UniformSpec;
	teamColors: [string, string, string];
	skinColor: string;
	bodySize?: number;
	// Height in inches, when known - taller men get visibly longer legs.
	hgt?: number;
	jerseyNumber?: string;
	idBase: string;
}): string => {
	const spec = cleanUniformSpec(specRaw);
	const base = spec.base ?? safeColor(teamColors[0], "#888888")!;
	const skin = safeColor(skinColor, "#a67358")!;
	const shortsBase = spec.shorts?.base ?? base;
	const belt = spec.shorts?.belt ?? shortsBase;

	let s = "";

	// LEGS AND FEET first, so the shorts overlap their tops.
	const legs =
		`<path fill="${skin}" ${OUTLINE} d="${LEG_LEFT}"/>` +
		`<path fill="${skin}" ${OUTLINE} d="${LEG_RIGHT}"/>` +
		`<path fill="#ffffff" ${OUTLINE} d="${SOCK_LEFT}"/>` +
		`<path fill="#ffffff" ${OUTLINE} d="${SOCK_RIGHT}"/>` +
		`<path fill="#ffffff" ${OUTLINE} d="${SHOE_LEFT}"/>` +
		`<path fill="#ffffff" ${OUTLINE} d="${SHOE_RIGHT}"/>` +
		`<path fill="none" stroke="#000" stroke-width="4" d="${SHOE_SOLE}"/>`;
	// Leg length follows height: 6'10" stretches the legs, 5'10" shortens
	// them, anchored at the top of the legs so the seam under the shorts
	// stays put.
	const legStretch =
		hgt !== undefined && Number.isFinite(hgt)
			? Math.min(1.12, Math.max(0.88, 1 + (hgt - 78) * 0.012))
			: 1;
	if (legStretch !== 1) {
		s += `<g transform="translate(0 ${1010 - 1010 * legStretch}) scale(1 ${legStretch})">${legs}</g>`;
	} else {
		s += legs;
	}

	// ARMS behind the torso.
	s +=
		`<path fill="${skin}" ${OUTLINE} d="${ARM_LEFT}"/>` +
		`<path fill="${skin}" ${OUTLINE} d="${ARM_RIGHT}"/>` +
		`<path fill="${skin}" ${OUTLINE} d="${HAND_LEFT}"/>` +
		`<path fill="${skin}" ${OUTLINE} d="${HAND_RIGHT}"/>` +
		// Elbow creases, faces.js-style detail lines.
		`<path fill="none" stroke="#000" stroke-width="3" d="M38 742q10 8 22 6M362 742q-10 8 -22 6"/>`;

	// TORSO.
	s += `<path fill="${base}" ${OUTLINE} d="${TORSO}"/>`;

	const needsClip = spec.pinstripes !== undefined || spec.image !== undefined;
	if (needsClip) {
		s += `<clipPath id="${idBase}t"><path d="${TORSO}"/></clipPath>`;
	}

	if (spec.pinstripes) {
		const { color, gap = 14, width = 3 } = spec.pinstripes;
		let d = "";
		for (let off = 0; 200 - off >= 84; off += gap) {
			d += `M${200 - off} 588v260`;
			if (off > 0) {
				d += `M${200 + off} 588v260`;
			}
		}
		s += `<path fill="none" stroke="${color}" stroke-width="${width}" d="${d}" clip-path="url(#${idBase}t)"/>`;
	}

	if (spec.image) {
		const { url, scale = 1, dx = 0, dy = 0, opacity = 1, stretch } = spec.image;
		// The same pattern the jersey strip uses, with the same 400x700 tile and
		// the same anchor - so the print continues seamlessly across the seam.
		s +=
			`<pattern id="${idBase}p" patternUnits="userSpaceOnUse" x="0" y="0" width="400" height="700" patternTransform="translate(${dx} ${dy})">` +
			`<g transform="translate(200 556) scale(${scale}) translate(-200 -556)">` +
			`<image href="${escapeXml(url)}" x="78" y="499" width="244" height="112" preserveAspectRatio="${stretch ? "none" : "xMidYMid slice"}" opacity="${opacity}"/>` +
			`</g></pattern>` +
			`<path d="${TORSO}" fill="url(#${idBase}p)"/>`;
	}

	// Side-seam trim down the jersey, echoing the armhole's innermost band so
	// the trim doesn't dead-end at the armpit.
	const lastArm = spec.arm?.at(-1);
	if (lastArm) {
		s += `<path fill="none" stroke="${lastArm.color}" stroke-width="${Math.min(lastArm.width, 8)}" d="M84 606Q86 726 93 836M316 606Q314 726 307 836"/>`;
	}

	// WORDMARK and NUMBER.
	const textColor = spec.wordmark?.color ?? contrastColor(base);
	const textOutline = spec.wordmark?.outline;
	const numberColor = spec.number?.color ?? textColor;
	const numberOutline = spec.number?.outline ?? textOutline;
	const textAttrs = (fill: string, outline: string | undefined) =>
		`fill="${fill}"${outline ? ` stroke="${outline}" stroke-width="2" paint-order="stroke"` : ""}`;
	if (spec.wordmark?.text) {
		const text = escapeXml(spec.wordmark.text.toUpperCase());
		// textLength pins the rendered width, so a long city name squeezes onto
		// the chest instead of running over the arms.
		const length = Math.min(196, spec.wordmark.text.length * 30);
		s += `<text x="200" y="668" text-anchor="middle" font-family="'Arial Black',Arial,sans-serif" font-weight="900" font-size="38" textLength="${length}" lengthAdjust="spacingAndGlyphs" ${textAttrs(textColor, textOutline)}>${text}</text>`;
	}
	if (jerseyNumber !== undefined) {
		const num = escapeXml(String(jerseyNumber).slice(0, 3));
		s += `<text x="200" y="790" text-anchor="middle" font-family="'Arial Black',Arial,sans-serif" font-weight="900" font-size="92" ${textAttrs(numberColor, numberOutline)}>${num}</text>`;
	}

	// SHORTS over the legs and torso hem.
	s += `<path fill="${belt}" ${OUTLINE} d="${WAISTBAND}"/>`;
	s += `<path fill="${shortsBase}" ${OUTLINE} d="${SHORTS}"/>`;

	if (spec.pinstripes) {
		const { color, gap = 14, width = 3 } = spec.pinstripes;
		s += `<clipPath id="${idBase}s"><path d="${SHORTS}"/></clipPath>`;
		let d = "";
		for (let off = 0; 200 - off >= 64; off += gap) {
			d += `M${200 - off} 870v156`;
			if (off > 0) {
				d += `M${200 + off} 870v156`;
			}
		}
		s += `<path fill="none" stroke="${color}" stroke-width="${width}" d="${d}" clip-path="url(#${idBase}s)"/>`;
	}
	if (spec.shorts?.side !== undefined) {
		s += `<path fill="none" stroke="${spec.shorts.side}" stroke-width="10" d="${SHORTS_SIDE}"/>`;
	}
	s += trimStrokes(spec.shorts?.trim, SHORTS_HEM);

	// The same horizontal body-size scaling faces.js applies to its body and
	// jersey, about the same center, so the torso continues the shoulders at
	// exactly the width the face drew them.
	const size = Math.min(1.2, Math.max(0.7, bodySize));
	if (size !== 1) {
		return `<g transform="translate(${200 * (1 - size)} 0) scale(${size} 1)">${s}</g>`;
	}
	return s;
};
