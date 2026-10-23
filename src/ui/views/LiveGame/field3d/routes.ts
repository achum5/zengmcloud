// THE ROUTE TREE: where a receiver goes on a pass play - the routes every
// offense runs, each a stem straight up the field and a break, and which a
// play gives each man: the one the ball goes to, made to come open where the
// sim says it was caught, and the others run off it the way a called concept
// would.
//
// In the offense's own frame, in feet: `d` downfield of the line of
// scrimmage, `a` across the field to the offense's right of the ball.

export type OPt = { d: number; a: number };

export type RouteName =
	| "go"
	| "fade"
	| "seam"
	| "post"
	| "corner"
	| "dig"
	| "out"
	| "slant"
	| "hitch"
	| "curl"
	| "comeback"
	| "drag"
	| "flat"
	| "swing"
	| "screen"
	| "checkdown"
	| "wheel";

export type Route = {
	name: RouteName;
	// From where he lines up, one point after another.
	path: OPt[];
	// Where a ball thrown to him on time meets him: a point on the path.
	catchAt: OPt;
	// How far along the path (feet) that is.
	catchAlong: number;
	// He works back to the ball and sits down at the end of it - turned to
	// face the quarterback - rather than running on.
	settles: boolean;
};

// What kind of receiver: split out, in the slot, a tight end, a back.
export type Spot = "wide" | "slot" | "te" | "back";

// The field across, in the offense's frame: its sidelines, left and right.
export type Bounds = { left: number; right: number };

const RAD = Math.PI / 180;

const lengthOf = (path: OPt[], upTo = path.length - 1) => {
	let s = 0;
	for (let k = 1; k <= upTo; k++) {
		s += Math.hypot(path[k]!.d - path[k - 1]!.d, path[k]!.a - path[k - 1]!.a);
	}
	return s;
};

// A route laid out: its path, and the point on it where the catch is (the
// k'th point).
const laid = (
	name: RouteName,
	path: OPt[],
	k: number,
	settles = false,
): Route => ({
	name,
	path,
	catchAt: path[k]!,
	catchAlong: lengthOf(path, k),
	settles,
});

// A route for a man lined up at `start`, the middle of the field toward
// `inSign` (+1 to the offense's right) - `depth` (feet downfield of the
// line) where it is caught, or where it breaks if it is run as a decoy -
// kept a few feet inside the sidelines. `u` and `v` (0 to 1) pick its
// details.
export const layRoute = (
	name: RouteName,
	start: OPt,
	inSign: 1 | -1,
	depth: number,
	bounds: Bounds,
	u: number,
	v: number,
): Route => {
	const lo = bounds.left + 5;
	const hi = bounds.right - 5;
	const inside = (a: number) => Math.min(hi, Math.max(lo, a));
	const out = -inSign;
	const s = start;
	const D = depth;
	const at = (d: number, a: number): OPt => ({ d, a: inside(a) });
	switch (name) {
		case "go": {
			// A step outside off the line and straight up the field.
			const path = [
				s,
				at(Math.min(D, 15), s.a + out * 3),
				at(D, s.a + out * 4),
				at(D + 45, s.a + out * 5),
			];
			return laid(name, path, 2);
		}
		case "fade": {
			// Up the field, drifting to the sideline.
			const path = [
				s,
				at(D * 0.4, s.a + out * 4),
				at(D, s.a + out * 12),
				at(D + 30, s.a + out * 15),
			];
			return laid(name, path, 2);
		}
		case "seam": {
			const path = [
				s,
				at(Math.min(D, 12), s.a + inSign),
				at(D, s.a + inSign * 2),
				at(D + 40, s.a + inSign * 3),
			];
			return laid(name, path, 2);
		}
		case "post":
		case "corner": {
			// Up to the break, then 40 degrees in toward the goalposts (or
			// out toward the corner of the end zone).
			const stem = Math.min(D - 6, 30 + 12 * u);
			const side = name === "post" ? inSign : out;
			const across = (D - stem) * Math.tan(40 * RAD);
			const path = [
				s,
				at(stem, s.a),
				at(D, s.a + side * across),
				at(D + 30, s.a + side * (across + 25)),
			];
			return laid(name, path, 2);
		}
		case "dig": {
			// Up to his depth and square across the middle.
			const w = 14 + 10 * u;
			const path = [
				s,
				at(D, s.a),
				at(D, s.a + inSign * w),
				at(D, s.a + inSign * (w + 40)),
			];
			return laid(name, path, 2);
		}
		case "out": {
			// Up to his depth and square to the sideline, a step back down.
			const w = Math.max(
				6,
				Math.min(12 + 6 * u, Math.abs(inside(s.a + out * 30) - s.a)),
			);
			const path = [s, at(D + 1, s.a), at(D - 1, s.a + out * w)];
			return laid(name, path, 2, true);
		}
		case "slant": {
			// Three steps up and in at 50 degrees.
			const stem = Math.min(D, 3 + 3 * u);
			const across = (D - stem) * Math.tan(50 * RAD) + 3;
			const path = [
				s,
				at(stem, s.a),
				at(D, s.a + inSign * across),
				at(D + 18, s.a + inSign * (across + 20)),
			];
			return laid(name, path, 2);
		}
		case "hitch": {
			// Up a yard past it and back to it, turned around.
			const path = [s, at(D + 3, s.a), at(D, s.a + inSign)];
			return laid(name, path, 2, true);
		}
		case "curl": {
			// Up two yards past it and back in under it.
			const path = [s, at(D + 6, s.a), at(D, s.a + inSign * (4 + 3 * u))];
			return laid(name, path, 2, true);
		}
		case "comeback": {
			// Up three yards past it, then back down to the sideline.
			const path = [s, at(D + 9, s.a), at(D, s.a + out * (6 + 4 * u))];
			return laid(name, path, 2, true);
		}
		case "drag": {
			// A step up and across under the linebackers, past the ball.
			const cross = inSign * (6 + 18 * u);
			const path = [
				s,
				at(Math.min(D, 4), s.a + inSign * 4),
				at(D, cross),
				at(D, cross + inSign * 40),
			];
			return laid(name, path, 2);
		}
		case "flat": {
			// Out to the flat, past the end of the line.
			const side = start.a === 0 ? (v < 0.5 ? -1 : 1) : Math.sign(start.a);
			const wide = side * (Math.max(Math.abs(s.a), 14) + 12 + 10 * u);
			const path = [
				s,
				at(Math.min(D, 2), s.a + side * 5),
				at(D, wide),
				at(D + 6, wide + side * 15),
			];
			return laid(name, path, 2);
		}
		case "swing": {
			// A back looping out behind the line.
			const side = start.a === 0 ? (v < 0.5 ? -1 : 1) : Math.sign(start.a);
			const path = [
				s,
				at(Math.min(s.d + 3, -6), s.a + side * 10),
				at(Math.max(D, -4), side * (28 + 8 * u)),
				at(Math.max(D, -4) + 10, side * (36 + 8 * u)),
			];
			return laid(name, path, 2);
		}
		case "screen": {
			// A receiver back off the line a step for the ball, his
			// blockers out in front of him.
			const path = [s, at(Math.min(D, s.d - 3), s.a + out * (3 + 3 * u))];
			return laid(name, path, 1, true);
		}
		case "checkdown": {
			// A back out of the backfield to an open spot short, sitting
			// down in it.
			const side = start.a === 0 ? (v < 0.5 ? -1 : 1) : Math.sign(start.a);
			const path = [
				s,
				at(Math.min(D, 2), s.a + side * 5),
				at(D, side * (6 + 10 * u)),
			];
			return laid(name, path, 2, true);
		}
		case "wheel": {
			// Out to the flat and up the sideline.
			const side = start.a === 0 ? (v < 0.5 ? -1 : 1) : Math.sign(start.a);
			const wide = side * (34 + 10 * u);
			const path = [
				s,
				at(1, wide - side * 8),
				at(10, wide),
				at(D, wide),
				at(D + 30, wide),
			];
			return laid(name, path, 3);
		}
	}
};

// The ways a ball caught this far downfield (yards, off the line) gets
// there, for each kind of receiver - each with how often.
type Choice = [RouteName, number][];
const TARGET_ROUTES: { upTo: number; by: Record<Spot, Choice> }[] = [
	{
		upTo: -1,
		by: {
			wide: [["screen", 1]],
			slot: [["screen", 1]],
			te: [["flat", 1]],
			back: [["swing", 1]],
		},
	},
	{
		upTo: 3,
		by: {
			wide: [
				["hitch", 3],
				["slant", 3],
				["out", 2],
			],
			slot: [
				["drag", 3],
				["slant", 2],
				["hitch", 2],
				["flat", 1],
			],
			te: [
				["flat", 3],
				["drag", 2],
			],
			back: [
				["flat", 3],
				["checkdown", 3],
				["swing", 1],
			],
		},
	},
	{
		upTo: 8,
		by: {
			wide: [
				["slant", 3],
				["out", 3],
				["hitch", 2],
				["curl", 1],
			],
			slot: [
				["slant", 2],
				["out", 2],
				["drag", 2],
				["curl", 1],
			],
			te: [
				["curl", 2],
				["flat", 2],
				["drag", 2],
				["out", 1],
			],
			back: [
				["checkdown", 3],
				["flat", 2],
			],
		},
	},
	{
		upTo: 15,
		by: {
			wide: [
				["curl", 3],
				["dig", 3],
				["out", 2],
				["comeback", 2],
			],
			slot: [
				["dig", 3],
				["curl", 2],
				["out", 2],
				["seam", 1],
			],
			te: [
				["curl", 3],
				["dig", 2],
				["seam", 2],
			],
			back: [
				["wheel", 1],
				["checkdown", 1],
			],
		},
	},
	{
		upTo: 25,
		by: {
			wide: [
				["dig", 2],
				["post", 3],
				["corner", 2],
				["comeback", 1],
			],
			slot: [
				["seam", 3],
				["post", 2],
				["corner", 2],
				["dig", 1],
			],
			te: [
				["seam", 3],
				["post", 1],
				["dig", 1],
			],
			back: [["wheel", 1]],
		},
	},
	{
		upTo: Infinity,
		by: {
			wide: [
				["go", 4],
				["post", 2],
				["fade", 2],
			],
			slot: [
				["seam", 3],
				["post", 2],
				["go", 1],
			],
			te: [
				["seam", 3],
				["post", 1],
			],
			back: [["wheel", 1]],
		},
	},
];

// And the rest, running theirs off his: anything from the whole tree, at a
// depth of its own.
const DECOY_ROUTES: Record<Spot, Choice> = {
	wide: [
		["go", 4],
		["post", 1],
		["dig", 2],
		["out", 2],
		["curl", 2],
		["comeback", 1],
		["slant", 1],
	],
	slot: [
		["seam", 2],
		["slant", 2],
		["out", 2],
		["dig", 2],
		["drag", 2],
		["curl", 2],
		["corner", 1],
	],
	te: [
		["seam", 2],
		["flat", 3],
		["curl", 3],
		["drag", 2],
		["dig", 1],
	],
	back: [
		["checkdown", 4],
		["flat", 3],
		["swing", 1],
		["wheel", 1],
	],
};

// How deep each is run as a decoy (yards off the line, shortest and
// longest).
const DECOY_DEPTH: Record<RouteName, [number, number]> = {
	go: [25, 30],
	fade: [20, 25],
	seam: [18, 25],
	post: [16, 22],
	corner: [16, 22],
	dig: [10, 15],
	out: [5, 12],
	slant: [5, 8],
	hitch: [5, 6],
	curl: [8, 12],
	comeback: [12, 15],
	drag: [2, 5],
	flat: [1, 4],
	swing: [-1, 0],
	screen: [-1, -1],
	checkdown: [3, 5],
	wheel: [15, 22],
};

const pick = (choices: Choice, u: number): RouteName => {
	const total = choices.reduce((s, [, w]) => s + w, 0);
	let x = u * total;
	for (const [name, w] of choices) {
		x -= w;
		if (x < 0) {
			return name;
		}
	}
	return choices.at(-1)![0];
};

// The routes that take him toward the sideline.
const OUTSIDE = new Set<RouteName>([
	"go",
	"fade",
	"corner",
	"out",
	"comeback",
	"flat",
	"swing",
	"screen",
	"wheel",
]);

// The route the ball is thrown to him on, caught `yds` yards downfield -
// one of those out toward the sideline, for a man who goes out of bounds
// after the catch.
export const targetRoute = (
	spot: Spot,
	yds: number,
	u: number,
	outside = false,
): RouteName => {
	const all = TARGET_ROUTES.find((t) => yds <= t.upTo)!.by[spot];
	if (!outside) {
		return pick(all, u);
	}
	const out = all.filter(([name]) => OUTSIDE.has(name));
	return out.length > 0
		? pick(out, u)
		: yds <= 3
			? "flat"
			: yds <= 15
				? "out"
				: "corner";
};

// A route to run as a decoy, and how deep (feet downfield of the line).
export const decoyRoute = (
	spot: Spot,
	u: number,
	v: number,
): { name: RouteName; depth: number } => {
	const name = pick(DECOY_ROUTES[spot], u);
	const [lo, hi] = DECOY_DEPTH[name];
	return { name, depth: (lo + (hi - lo) * v) * 3 };
};

// Where he is `along` feet down his path (past its end: still at the end -
// or, for a route run on, carrying on the way the last stretch goes).
export const alongRoute = (route: Route, along: number): OPt => {
	const p = route.path;
	let left = Math.max(0, along);
	for (let k = 1; k < p.length; k++) {
		const a = p[k - 1]!;
		const b = p[k]!;
		const len = Math.hypot(b.d - a.d, b.a - a.a);
		if (left <= len || k === p.length - 1) {
			const u =
				len > 0 ? (route.settles ? Math.min(1, left / len) : left / len) : 0;
			return { d: a.d + (b.d - a.d) * u, a: a.a + (b.a - a.a) * u };
		}
		left -= len;
	}
	return p[0]!;
};
