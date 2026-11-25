import type { Camera } from "./camera.ts";

// THE BOWL OF THE BUILDING, BUILT IN BLENDER (see tools/court3d-arena/
// bowl.py, EXPORT): the lower bowl's rows, its aisles and tunnels, the two
// rings of suites with a ribbon board along each, the upper deck, the
// catwalk and the roof - the far side and both ends, as the game's cameras
// see them - each point of it with the light it gets on a game night baked
// in (Cycles), and every seat in it with the light a fan sat there gets.
// Drawn on the graphics card (see bowlGl.ts), the fans in its seats.
//
// The file, little-endian:
//   header: "BOWL", version, points, opaque indices, see-through indices,
//     seats (u32 each); feet per unit of position, of uv, the brightest
//     light (f32 each); bytes an index (u32)
//   anchors: how many (u32), then each (f32) - see ANCHORS
//   points: x, y, z (i16 each, in units), padded to 4 bytes
//           u, v (u16 each)
//           light r, g, b (u8 each: the square root of its share of the
//             brightest) and its material (u8: see BOWL_MATERIALS)
//   indices: triangles, the opaque first then the see-through, padded to 4
//   seats: x, y, z of the pan (i16 each, in units) and the way it faces
//          (i16: its angle, -32767..32767 for -pi..pi)
//          light r, g, b (u8 each, as the points') and its deck (u8: see
//          SEAT_DECKS)

// The materials, by number (as bowl.py's GAME_MATS has them).
export const BOWL_MATERIALS = [
	"concrete",
	"tread",
	"riser",
	"black",
	"fascia",
	"seat",
	"suiteWall",
	"suiteFloor",
	"suiteCeil",
	"suiteLight",
	"mullion",
	"glass",
	"rail",
	"floor",
	"apron",
	"stepLight",
	"tunnelLight",
	"concourse",
	"tunnelWall",
	"roof",
	"steel",
	"fixture",
	"portal",
	"banner0",
	"banner1",
	"ribbon",
	"wall",
	"tunnelFloor",
] as const;
export type BowlMaterial = (typeof BOWL_MATERIALS)[number];
export const SEAT_DECKS = ["lower", "balcony", "upper"] as const;

// Where things are in it (feet), as bowl.py writes them.
const ANCHORS = [
	// The front of the stands all round: a rectangle round the floor,
	// rounded at the corners.
	"x0",
	"x1",
	"y0",
	"y1",
	"corner",
	// How far back from it the balconies' fronts are - the ribbon boards
	// along them, the bottom and top of each.
	"balcony",
	"ribbon1Bottom",
	"ribbon1Top",
	"ribbon2Bottom",
	"ribbon2Top",
	// The upper deck's front and top row, how far back its top row is, the
	// roof, the catwalk.
	"upperFront",
	"upperTop",
	"upperBack",
	"roof",
	"catwalk",
] as const;
export type BowlAnchors = Record<(typeof ANCHORS)[number], number>;

export type Bowl = {
	points: number;
	// Feet a unit of position.
	unit: number;
	pos: Int16Array;
	uv: Uint16Array;
	// Light and material, four bytes a point.
	lightMat: Uint8Array;
	index: Uint16Array | Uint32Array;
	opaque: number;
	clear: number;
	seats: number;
	// x, y, z, angle a seat.
	seat: Int16Array;
	// Light and deck, four bytes a seat.
	seatLight: Uint8Array;
	// The light a byte of 255 stands for.
	brightest: number;
	anchors: BowlAnchors;
};

export const parseBowl = (buf: ArrayBuffer): Bowl => {
	const dv = new DataView(buf);
	const magic = String.fromCharCode(
		dv.getUint8(0),
		dv.getUint8(1),
		dv.getUint8(2),
		dv.getUint8(3),
	);
	if (magic !== "BOWL" || dv.getUint32(4, true) !== 1) {
		throw new Error("not a bowl");
	}
	const points = dv.getUint32(8, true);
	const opaque = dv.getUint32(12, true);
	const clear = dv.getUint32(16, true);
	const seats = dv.getUint32(20, true);
	const unit = dv.getFloat32(24, true);
	const brightest = dv.getFloat32(32, true);
	const indexBytes = dv.getUint32(36, true);
	let at = 40;
	const nAnchors = dv.getUint32(at, true);
	at += 4;
	const anchors = {} as BowlAnchors;
	ANCHORS.forEach((name, i) => {
		anchors[name] = i < nAnchors ? dv.getFloat32(at + i * 4, true) : 0;
	});
	at += nAnchors * 4;
	const pad4 = (n: number) => Math.ceil(n / 4) * 4;
	// (Copied out: an Int16Array has to start on an even byte, and the rest
	// on a fourth.)
	const pos = new Int16Array(buf.slice(at, at + points * 6));
	at += pad4(points * 6);
	const uv = new Uint16Array(buf.slice(at, at + points * 4));
	at += points * 4;
	const lightMat = new Uint8Array(buf.slice(at, at + points * 4));
	at += points * 4;
	const nIdx = opaque + clear;
	const index =
		indexBytes === 4
			? new Uint32Array(buf.slice(at, at + nIdx * 4))
			: new Uint16Array(buf.slice(at, at + nIdx * 2));
	at += pad4(nIdx * indexBytes);
	const seat = new Int16Array(buf.slice(at, at + seats * 8));
	at += seats * 8;
	const seatLight = new Uint8Array(buf.slice(at, at + seats * 4));
	return {
		points,
		unit,
		pos,
		uv,
		lightMat,
		index,
		opaque,
		clear,
		seats,
		seat,
		seatLight,
		brightest,
		anchors,
	};
};

// Fetched once a visit (bump the v when bowl.py's export is run again: the
// browser keeps the old one otherwise).
const URL = "/img/arena/bowl.bin?v=1";
export const TONE_URL = "/img/arena/bowl-tone.png?v=1";
let loading: Promise<Bowl | undefined> | undefined;
let loaded: Bowl | undefined;
export const loadBowl = (): Promise<Bowl | undefined> =>
	(loading ??= fetch(URL)
		.then((r) => (r.ok ? r.arrayBuffer() : Promise.reject(new Error("none"))))
		.then((buf) => (loaded = parseBowl(buf)))
		.catch(() => undefined));
export const bowlLoaded = (): Bowl | undefined => loaded;

// THE CAMERA AS THE CARD TAKES IT: a 4 x 4 matrix (column by column) from
// the world's feet to the card's clip space, the same picture project
// (camera.ts) makes - upright or not - with how far off as its depth, so
// what is nearer covers what is behind it, and what is behind the camera
// is cut off rather than flung across the picture.
export const BOWL_NEAR = 1;
export const BOWL_FAR = 3000;
export const bowlMatrix = (cam: Camera): Float32Array => {
	const { pos, right, up, fwd, f, cx, cy, viewW: W, viewH: H, upright } = cam;
	// Each of the camera's sums as a row: [x, y, z, 1] -> a number. Upright,
	// what is measured is the spot on the floor under the point - and its
	// height is added straight up the picture (see project).
	const row = (v: { x: number; y: number; z: number }, z: number) => [
		v.x,
		v.y,
		z,
		-(v.x * pos.x + v.y * pos.y + v.z * pos.z),
	];
	const w = row(fwd, upright ? 0 : fwd.z);
	const r = row(right, upright ? 0 : right.z);
	const u = row(up, upright ? 1 : up.z);
	const A = (BOWL_FAR + BOWL_NEAR) / (BOWL_FAR - BOWL_NEAR);
	const B = (-2 * BOWL_FAR * BOWL_NEAR) / (BOWL_FAR - BOWL_NEAR);
	const ax = 2 * (cx / W) - 1;
	const ay = 1 - 2 * (cy / H);
	const m = new Float32Array(16);
	for (let k = 0; k < 4; k++) {
		const clipX = ax * w[k]! + ((2 * f) / W) * r[k]!;
		const clipY = ay * w[k]! + ((2 * f) / H) * u[k]!;
		const clipZ = A * w[k]! + (k === 3 ? B : 0);
		m[k * 4] = clipX;
		m[k * 4 + 1] = clipY;
		m[k * 4 + 2] = clipZ;
		m[k * 4 + 3] = w[k]!;
	}
	return m;
};

// Where a point comes out on the picture by that matrix (for the tests):
// x, y in the picture's px, and how far off.
export const bowlProject = (
	m: Float32Array,
	cam: Camera,
	p: { x: number; y: number; z: number },
) => {
	const c = [0, 1, 2, 3].map(
		(i) => m[i]! * p.x + m[4 + i]! * p.y + m[8 + i]! * p.z + m[12 + i]!,
	);
	const w = c[3]!;
	return {
		x: ((c[0]! / w + 1) / 2) * cam.viewW,
		y: ((1 - c[1]! / w) / 2) * cam.viewH,
		depth: w,
	};
};
