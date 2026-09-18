// The player below the neck. faces.js stops at the shoulders - its canvas is
// 400x600 and the jersey is a strip at the bottom - so the full-body view
// draws everything from there down as its own SVG and slots the faces.js face
// on top. Coordinates share the face's x axis one-to-one; the seam sits at
// y=600, and every shape here starts a few units above it so the face's own
// opaque body/jersey fill hides the join.
//
// MATCHING THE ART. faces.js draws everything with stroke-width 6 in black and
// nothing but smooth curves - its body path is one flowing bezier from wrist
// to wrist. So this file has two rules. First, no straight parallel edges: an
// arm drawn as a rectangle with a rounded corner reads as a plank next to a
// head made of curves, which is exactly how the first version of this looked.
// Every limb tapers and carries one soft bulge (deltoid, calf) the way the
// face's own shapes do. Second, no seam a reader can find: a hand drawn as its
// own closed shape puts a black line across the wrist, so the hand is part of
// the arm's single path and the wrist is a light crease line on top. Socks and
// soles are clipped to the limb they sit on and the limb's outline is redrawn
// over them, so the silhouette is one unbroken stroke from hip to floor.
//
// WHERE THE SEAM LANDS. The faces.js body path starts at (10,600) and its
// jersey spans x=80..320 there, so below the seam x=10..80 is bare arm and
// x=80..320 is jersey. Those three numbers anchor everything here.
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

// faces.js's own stroke, exactly: same width, same colour, same joins.
const OUTLINE = 'stroke="#000" stroke-width="6" stroke-linejoin="round"';
const RESTROKE = 'fill="none" stroke="#000" stroke-width="6"';
// The lighter weight faces.js uses for creases inside a shape.
const CREASE =
	'fill="none" stroke="#000" stroke-width="4" stroke-linecap="round"';

// Everything below is drawn for the LEFT half and mirrored about x=200, so the
// two sides cannot drift apart. A -1 x scale keeps stroke width intact.
const mirror = (inner: string) =>
	`<g transform="translate(400,0) scale(-1,1)">${inner}</g>`;
const both = (inner: string) => inner + mirror(inner);

// ----- The fixed anatomy -----
//
// OPEN PATHS AT THE SEAM. faces.js's own body is an open path: it is filled,
// but only the drawn segments are stroked, so nothing is outlined along the
// bottom where the canvas ends. Anything here that reaches the seam has to do
// the same. Stroking a closed shape put a black line across the top of the
// arm and the chest that the face could not quite cover - a notch poking out
// past the shoulder, and a bar across the top of the jersey. So the shapes
// that touch y=600 are drawn twice: filled with no stroke, then outlined with
// an open path that simply stops at the seam, leaving the face's own edge to
// carry on from it.
//
// TORSO. It starts at x=82.6, not 80. Every basketball jersey faces.js draws
// shares one silhouette whose side runs from (80,610) up to (90,520), so by
// the time the canvas cuts it at y=600 the edge has already reached 82.6;
// anchoring at 80 left the chest a couple of units proud of the shirt above
// it, a small ledge down both sides. From there it keeps the jersey's downward
// direction - the armhole is still opening out at the seam - swells gently
// through the chest and draws in to a waist. The old version ran straight down
// and read as a box with the number painted on it.
const TORSO_LINE =
	"M82.6 600C78 648 75 704 80 774C85 810 89 834 93 850L307 850C311 834 315 810 320 774C325 704 322 648 317.4 600";
const TORSO = `${TORSO_LINE}Z`;
// The two side edges of the torso on their own, for the jersey's side piping.
const SIDE_SEAM =
	"M82.6 600C78 648 75 704 80 774C85 810 89 834 93 850" +
	"M317.4 600C322 648 325 704 320 774C315 810 311 834 307 850";

// ARM AND HAND, one path. Out from the shoulder at (10,600) - the exact point
// the faces.js body path starts, so the two outlines are one line - with a soft
// deltoid swell, tapering through the forearm and on into the hand without a
// break. The inner edge rides up behind the jersey, so the arm emerges from
// under the armhole rather than floating beside it with a gap.
//
// The taper is unbroken on purpose, with no pinch at the wrist: narrowing the
// silhouette there needs the curvature to reverse, and at this stroke weight
// that reads as a kink - the arm looks snapped rather than slender. The crease
// below marks the wrist instead, which is the faces.js way round: shape for the
// silhouette, a light line for the detail.
const ARM_LINE =
	"M10 600C6 656 10 716 18 772C24 812 29 848 34 888C24 912 22 940 30 960C39 980 64 982 74 964C82 950 80 922 74 898C78 860 84 812 86 762C86 700 85 648 82.6 600";
const ARM = `${ARM_LINE}Z`;
// The wrist. Drawn wider than the arm and clipped to it, so it meets the
// outline on both sides instead of floating short of it - which is what an
// arc guessed at by eye did, and it read as a scratch rather than a crease.
const WRIST = "M10 890C34 904 60 904 90 890";

// SHORTS. Waistband picks up the torso's waist exactly - a band any wider
// stepped out past the jersey above it - then the legs flare to the thigh and
// part at an inseam notch shallow enough not to read as a spike.
const WAISTBAND = "M92 848L308 848L311 890L89 890Z";
const SHORTS =
	"M89 888C82 942 78 996 80 1048L172 1048C180 1020 192 1002 200 1000C208 1002 220 1020 228 1048L320 1048C322 996 318 942 311 888Z";
// The bottom edges of the shorts legs, for trim bands.
const SHORTS_HEM = "M80 1048L172 1048M228 1048L320 1048";
// One outer side seam, for a side stripe (mirrored for the other).
const SHORTS_SIDE_HALF = "M89 888C82 942 78 996 80 1048";

// LEG, hip to ankle, with the calf as the one bulge and a real taper into the
// ankle. Starts above the shorts hem so the shorts overlap its top.
const LEG =
	"M104 1030C98 1096 104 1152 112 1200L154 1200C162 1152 166 1096 162 1030Z";
// Where the sock starts, as a cuff that dips slightly at the front. Drawn
// wider than the leg and clipped to it, same as the wrist, so its round caps
// cannot poke out past the silhouette.
const SOCK_TOP = "M92 1142C120 1158 146 1158 174 1142";
const SOCK_FROM = 1148;

// SHOE. A sneaker seen head on: heel under the ankle, instep rising to the
// toe box, splayed a little outwards the way a stance does.
const SHOE =
	"M110 1194C93 1204 84 1224 84 1244C84 1260 94 1268 110 1268L168 1268C180 1268 186 1258 186 1242C186 1218 180 1200 166 1192Z";
// The midsole, a band across the bottom of the shoe.
const SOLE_FROM = 1246;

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

	// Clip paths for the sock and the midsole, so each sits exactly inside the
	// limb it belongs to and the limb's own outline can be redrawn over it.
	// Referenced from inside the mirrored group too, where they mirror with it.
	s +=
		`<clipPath id="${idBase}l"><path d="${LEG}"/></clipPath>` +
		`<clipPath id="${idBase}f"><path d="${SHOE}"/></clipPath>` +
		`<clipPath id="${idBase}a"><path d="${ARM}"/></clipPath>`;

	// LEGS AND FEET first, so the shorts overlap their tops. One side, then
	// mirrored: leg, sock inside it, outline redrawn, then the shoe with its
	// midsole done the same way.
	const legSide =
		`<path fill="${skin}" ${OUTLINE} d="${LEG}"/>` +
		`<rect x="0" y="${SOCK_FROM}" width="400" height="120" fill="#ffffff" clip-path="url(#${idBase}l)"/>` +
		`<path ${RESTROKE} d="${LEG}"/>` +
		`<path ${CREASE} d="${SOCK_TOP}" clip-path="url(#${idBase}l)"/>` +
		`<path fill="#ffffff" ${OUTLINE} d="${SHOE}"/>` +
		`<rect x="0" y="${SOLE_FROM}" width="400" height="60" fill="#2f3337" clip-path="url(#${idBase}f)"/>` +
		`<path ${RESTROKE} d="${SHOE}"/>`;
	const legs = both(legSide);
	// Leg length follows height: 6'10" stretches the legs, 5'10" shortens
	// them, anchored at the top of the legs so the seam under the shorts
	// stays put.
	const legStretch =
		hgt !== undefined && Number.isFinite(hgt)
			? Math.min(1.12, Math.max(0.88, 1 + (hgt - 78) * 0.012))
			: 1;
	if (legStretch !== 1) {
		s += `<g transform="translate(0 ${1030 - 1030 * legStretch}) scale(1 ${legStretch})">${legs}</g>`;
	} else {
		s += legs;
	}

	// ARMS behind the torso, so the inner edge disappears under the armhole.
	// Filled closed, outlined open - see the note on the seam above.
	s += both(
		`<path fill="${skin}" d="${ARM}"/>` +
			`<path ${RESTROKE} stroke-linejoin="round" d="${ARM_LINE}"/>` +
			`<path ${CREASE} d="${WRIST}" clip-path="url(#${idBase}a)"/>`,
	);

	// TORSO, the same way.
	s +=
		`<path fill="${base}" d="${TORSO}"/>` +
		`<path ${RESTROKE} stroke-linejoin="round" d="${TORSO_LINE}"/>`;

	const needsClip = spec.pinstripes !== undefined || spec.image !== undefined;
	if (needsClip) {
		s += `<clipPath id="${idBase}t"><path d="${TORSO}"/></clipPath>`;
	}

	if (spec.pinstripes) {
		const { color, gap = 14, width = 3 } = spec.pinstripes;
		let d = "";
		for (let off = 0; 200 - off >= 72; off += gap) {
			d += `M${200 - off} 588v272`;
			if (off > 0) {
				d += `M${200 + off} 588v272`;
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

	// The armhole binding, carried on down the side seam. faces.js finishes the
	// armhole with a stack of bands (16 black, 12 primary, 6 accent) that
	// arrives at the seam 16 units wide; a plain 6-wide outline picking up from
	// it left a step at both armpits. Drawing the WHOLE stack down the side -
	// widest first, exactly the order faces.js paints them - continues it
	// without a join, and reads as the side piping a real jersey has.
	s += trimStrokes(spec.arm, SIDE_SEAM);

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
		const length = Math.min(190, spec.wordmark.text.length * 30);
		s += `<text x="200" y="676" text-anchor="middle" font-family="'Arial Black',Arial,sans-serif" font-weight="900" font-size="38" textLength="${length}" lengthAdjust="spacingAndGlyphs" ${textAttrs(textColor, textOutline)}>${text}</text>`;
	}
	if (jerseyNumber !== undefined) {
		const num = escapeXml(String(jerseyNumber).slice(0, 3));
		s += `<text x="200" y="796" text-anchor="middle" font-family="'Arial Black',Arial,sans-serif" font-weight="900" font-size="90" ${textAttrs(numberColor, numberOutline)}>${num}</text>`;
	}

	// SHORTS over the legs and torso hem.
	s += `<path fill="${belt}" ${OUTLINE} d="${WAISTBAND}"/>`;
	s += `<path fill="${shortsBase}" ${OUTLINE} d="${SHORTS}"/>`;

	if (spec.pinstripes) {
		const { color, gap = 14, width = 3 } = spec.pinstripes;
		s += `<clipPath id="${idBase}s"><path d="${SHORTS}"/></clipPath>`;
		let d = "";
		for (let off = 0; 200 - off >= 76; off += gap) {
			d += `M${200 - off} 886v168`;
			if (off > 0) {
				d += `M${200 + off} 886v168`;
			}
		}
		s += `<path fill="none" stroke="${color}" stroke-width="${width}" d="${d}" clip-path="url(#${idBase}s)"/>`;
	}
	if (spec.shorts?.side !== undefined) {
		s += both(
			`<path fill="none" stroke="${spec.shorts.side}" stroke-width="10" d="${SHORTS_SIDE_HALF}"/>`,
		);
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
