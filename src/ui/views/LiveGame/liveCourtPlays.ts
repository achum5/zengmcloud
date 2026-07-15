// ---------------------------------------------------------------------------
// The live-game court's basketball "play" engine. The sim only tells us a shot's
// coarse zone (at rim / low post / mid-range / three) and who was involved; this
// module turns that into a genuine-looking basketball possession: it synthesizes
// a real shot LOCATION with a proper spread across the zone (threes fan across
// the whole arc AND both corners, not three fixed spots), then GENERATES a play
// - an iso drive, a pull-up, a catch-and-shoot off a cross-court swing, a pick-
// and-roll, a dribble hand-off, a backdoor cut, a step-back, a transition
// finish, a post-up - whose choreography flows into that exact spot. Each play
// is a set of smooth keyframed paths (Catmull-Rom) for the handful of players it
// involves, plus a ball script (held, passed, shot). The court binds the real
// players onto the play's roles, mirrors it for the attacking team, and animates
// the whole thing through to the shot the engine resolves (make / miss / block).
//
// Because plays are built AROUND a freshly-synthesized spot rather than read off
// a fixed template, the same play type yields infinitely many looks and the shot
// chart comes out balanced - the way real basketball is spread across the floor.
//
// CANONICAL FRAME: everything here is authored for the offense attacking the
// RIGHT rim (rim center x = 88.75, y = 25) with play time t running 0 -> 1. The
// court is 94 (length) x 50 (width). Placement mirrors x for a team attacking
// left. `side` (+1 = the spot is in the bottom half, -1 = top) orients each play
// toward the correct sideline so it develops from a natural angle.
// ---------------------------------------------------------------------------

export type V = { x: number; y: number };

// A path waypoint: at play-fraction `t` the player is at (x, y) in canonical
// coordinates. Positions are interpolated smoothly (Catmull-Rom) between nodes.
export type PathNode = { t: number; x: number; y: number };

export type PlayRole =
	| "scorer"
	| "passer"
	| "screen"
	| "def1" // guards the scorer
	| "def2" // guards the passer / second offensive player
	| "victim"
	| "stealer"
	| "fouler"
	| "jumper2";

export type Track = {
	role: PlayRole;
	// Is this an inferred/named defender (opposing team colors)?
	def: boolean;
	nodes: PathNode[];
};

// The ball's script through the play. Roles resolve to the live position of the
// bound player each frame, so the ball stays in a moving handler's hands.
export type BallSeg =
	| { kind: "hold"; role: PlayRole; t0: number; t1: number }
	| { kind: "pass"; from: PlayRole; to: PlayRole; t0: number; t1: number }
	// A shot RELEASE: the court engine takes over here for the flight to the rim
	// and the make/miss/block outcome (from the real event), so a play works for a
	// make, a miss, or a block alike.
	| { kind: "shot"; role: PlayRole; t: number }
	// A loose ball (rebound off the rim, a steal popping free): travels from a
	// fixed spot / role to another over [t0, t1].
	| {
			kind: "loose";
			t0: number;
			t1: number;
			from?: V;
			fromRole?: PlayRole;
			to?: V;
			toRole?: PlayRole;
	  }
	// The ball disappears (a turnover with no clear new handler) at t.
	| { kind: "vanish"; t: number };

// The shot category (coarse zone) or a non-shot action.
export type ShotCat = "rim" | "post" | "mid" | "three" | "ft";
export type NonShotCat = "steal" | "tov" | "orb" | "drb" | "foul" | "jump";
export type PlayCat = ShotCat | NonShotCat;

// A ready-to-place play: the paths its players run plus the ball script. Both are
// in canonical coordinates; placement mirrors/jitters them onto the live court.
export type BuiltPlay = { tracks: Track[]; ball: BallSeg[] };

// ---- Canonical geometry (attacking the RIGHT rim) -------------------------
const RIM_X = 88.75;
const RIM_Y = 25;
const RIM: V = { x: RIM_X, y: RIM_Y };
const R3 = 23.75; // three-point radius from the rim
const FT_X = 75; // free-throw line depth (canonical x)

const clamp = (v: number, lo: number, hi: number) =>
	Math.max(lo, Math.min(hi, v));
const rand = (lo: number, hi: number) => lo + Math.random() * (hi - lo);
const lerp = (a: number, b: number, u: number) => a + (b - a) * u;
const deg = (d: number) => (d * Math.PI) / 180;

// Keep a spot a few feet off every edge so the face + name tag never clip the
// sideline/baseline.
const clampSpot = (p: V): V => ({
	x: clamp(p.x, 5, 90),
	y: clamp(p.y, 4, 46),
});

// A point at distance r from the rim, on a bearing a measured from the "straight
// out toward half court" axis, positive rotating toward the BOTTOM (+y).
const polar = (r: number, aDeg: number): V => {
	const a = deg(aDeg);
	return { x: RIM_X - r * Math.cos(a), y: RIM_Y + r * Math.sin(a) };
};

const unit = (dx: number, dy: number): V => {
	const d = Math.hypot(dx, dy) || 1;
	return { x: dx / d, y: dy / d };
};
// Away from the rim (roughly toward half court) at a spot.
const outward = (p: V): V => unit(p.x - RIM_X, p.y - RIM_Y);
// A perpendicular ("across the floor") direction.
const lateral = (u: V): V => ({ x: -u.y, y: u.x });
// Move from p by k feet along unit vector u.
const along = (p: V, u: V, k: number): V => ({
	x: p.x + u.x * k,
	y: p.y + u.y * k,
});
const A = (t: number, p: V): PathNode => ({ t, x: p.x, y: p.y });
const track = (role: PlayRole, def: boolean, nodes: PathNode[]): Track => ({
	role,
	def,
	nodes,
});

// ---- Shot-spot synthesis --------------------------------------------------
// The heart of the "balanced shot chart" fix: a plausible release spot for a
// zone, spread properly across the floor instead of clustering at a few points.
export const synthCanonicalSpot = (cat: ShotCat): V => {
	if (cat === "ft") {
		return { x: FT_X, y: RIM_Y + rand(-0.4, 0.4) };
	}
	if (cat === "three") {
		const u = Math.random();
		if (u < 0.14) {
			// Bottom corner: behind the corner line, a short way up from the baseline.
			return { x: rand(80, 88), y: rand(47.1, 48.4) };
		}
		if (u > 0.86) {
			// Top corner.
			return { x: rand(80, 88), y: rand(1.6, 2.9) };
		}
		// Around the arc, wing to wing.
		const a = lerp(-64, 64, (u - 0.14) / 0.72);
		return clampSpot(polar(R3 + rand(0.4, 2.4), a));
	}
	if (cat === "mid") {
		return clampSpot(polar(rand(10.5, 21), rand(-80, 80)));
	}
	if (cat === "post") {
		const s = Math.random() < 0.5 ? 1 : -1;
		return clampSpot(polar(rand(5, 10), s * rand(34, 74)));
	}
	// rim
	return clampSpot(polar(rand(1.5, 4.8), rand(-105, 105)));
};

// A spread spot for a non-shot action (turnover / steal / foul), in the
// offense's frontcourt but out of the immediate paint.
const synthActionSpot = (): V => ({ x: rand(58, 80), y: rand(9, 41) });

export const sideOf = (S: V): 1 | -1 => (S.y >= RIM_Y ? 1 : -1);

// A defender who sags into help then CLOSES OUT, arriving a step inside the
// shooter as the shot goes up (a real, slightly-late contest).
const closeout = (end: V): PathNode[] => {
	const help: V = { x: (end.x + RIM_X) / 2, y: (end.y + RIM_Y) / 2 };
	const contest = along(end, outward(end), -2.2);
	return [
		A(0, help),
		A(0.6, { x: (help.x + contest.x) / 2, y: (help.y + contest.y) / 2 }),
		A(1, contest),
	];
};

// A defender a step off the man toward the basket (between man and rim),
// trailing his path.
const trail = (nodes: PathNode[], off = 3): PathNode[] =>
	nodes.map((p) => {
		const u = unit(RIM_X - p.x, RIM_Y - p.y);
		return { t: p.t, x: p.x + u.x * off, y: p.y + u.y * off };
	});

// ---- Shot-play generators -------------------------------------------------
// Each takes the synthesized spot S (+ its side) and returns the play. `passer`
// generators depict a real assist (a pass immediately before the shot); the
// others are self-created shots. The final ball segment is always the shot from
// the scorer, which the court resolves to a make / miss / block.

type ShotOpts = { S: V; side: 1 | -1 };
type ShotGen = {
	id: string;
	needsPasser: boolean;
	build: (o: ShotOpts) => BuiltPlay;
};

// Catch-and-shoot off a cross-court swing: the passer is across the floor and
// swings it to the shooter, who lifts into the spot as the defender closes out.
const catchAndShoot: ShotGen = {
	id: "catch-shoot",
	needsPasser: true,
	build: ({ S, side }) => {
		const out = outward(S);
		const lat = lateral(out);
		const s0 = along(along(S, lat, side * 8), out, -1);
		const s1 = along(S, lat, side * 3);
		const sc = [A(0, s0), A(0.55, s1), A(1, S)];
		const pPos = clampSpot(along(along(S, lat, -side * 20), out, 3));
		const pp = [A(0, pPos), A(1, along(pPos, lat, side * 1.5))];
		return {
			tracks: [
				track("passer", false, pp),
				track("scorer", false, sc),
				track("def1", true, closeout(S)),
				track("def2", true, trail(pp, 3)),
			],
			ball: [
				{ kind: "hold", role: "passer", t0: 0, t1: 0.5 },
				{ kind: "pass", from: "passer", to: "scorer", t0: 0.5, t1: 0.72 },
				{ kind: "shot", role: "scorer", t: 0.9 },
			],
		};
	},
};

// Dribble hand-off: the handler dribbles up, the shooter curls tight off him and
// takes the handoff into the shot.
const handoff: ShotGen = {
	id: "dho",
	needsPasser: true,
	build: ({ S, side }) => {
		const out = outward(S);
		const lat = lateral(out);
		const top = along(S, out, 4);
		const pp = [A(0, along(top, out, 3)), A(0.5, top), A(1, along(top, lat, side * 2.5))];
		const sc = [
			A(0, along(along(S, lat, side * 12), out, 1)),
			A(0.5, along(top, lat, side * 4)),
			A(1, S),
		];
		return {
			tracks: [
				track("passer", false, pp),
				track("scorer", false, sc),
				track("def1", true, trail(sc, 2.6)),
				track("def2", true, trail(pp, 3)),
			],
			ball: [
				{ kind: "hold", role: "passer", t0: 0, t1: 0.52 },
				{ kind: "pass", from: "passer", to: "scorer", t0: 0.52, t1: 0.66 },
				{ kind: "shot", role: "scorer", t: 0.9 },
			],
		};
	},
};

// Pull-up off the dribble: the shooter dribbles in from beyond the spot at an
// angle and rises, the defender trailing.
const pullUp: ShotGen = {
	id: "pull-up",
	needsPasser: false,
	build: ({ S, side }) => {
		const out = outward(S);
		const lat = lateral(out);
		const s0 = along(along(S, out, 9), lat, side * 5);
		const sc = [A(0, s0), A(0.5, along(S, out, 3.5)), A(1, S)];
		return {
			tracks: [
				track("scorer", false, sc),
				track("def1", true, trail(sc, 3.2)),
			],
			ball: [
				{ kind: "hold", role: "scorer", t0: 0, t1: 0.85 },
				{ kind: "shot", role: "scorer", t: 0.85 },
			],
		};
	},
};

// Step-back: the shooter drives toward the rim, then steps back to the spot,
// leaving the over-committing defender inside.
const stepBack: ShotGen = {
	id: "step-back",
	needsPasser: false,
	build: ({ S, side }) => {
		const out = outward(S);
		const lat = lateral(out);
		const s0 = along(along(S, out, 6), lat, side * 3);
		const inside = along(S, out, -2.5);
		const sc = [A(0, s0), A(0.5, inside), A(1, S)];
		const d1 = [
			A(0, along(s0, out, -2)),
			A(0.5, along(inside, out, -1.5)),
			A(1, along(inside, out, -0.5)),
		];
		return {
			tracks: [track("scorer", false, sc), track("def1", true, d1)],
			ball: [
				{ kind: "hold", role: "scorer", t0: 0, t1: 0.85 },
				{ kind: "shot", role: "scorer", t: 0.85 },
			],
		};
	},
};

// Iso drive: the scorer attacks from the wing on the spot's side and finishes,
// the defender trailing on his hip.
const driveIso: ShotGen = {
	id: "drive-iso",
	needsPasser: false,
	build: ({ S, side }) => {
		const w = polar(23, side * 40);
		const sc = [A(0, w), A(0.5, polar(12, side * 30)), A(1, S)];
		return {
			tracks: [
				track("scorer", false, sc),
				track("def1", true, trail(sc, 3)),
			],
			ball: [
				{ kind: "hold", role: "scorer", t0: 0, t1: 0.9 },
				{ kind: "shot", role: "scorer", t: 0.9 },
			],
		};
	},
};

// Pick-and-roll, ball-handler keeper: the handler (scorer) uses a teammate's
// screen and turns the corner to finish; the defender gets hung on the screen
// and recovers late. No pass needed - works for any self-created shot.
const pnrHandler: ShotGen = {
	id: "pnr-handler",
	needsPasser: false,
	build: ({ S, side }) => {
		const out = outward(S);
		const top = polar(22, side * 8);
		const useAt = polar(15, side * 20);
		const sc = [A(0, top), A(0.45, useAt), A(1, S)];
		const scr = [
			A(0, polar(14, side * 24)),
			A(0.5, along(useAt, out, -1)),
			A(1, polar(9, side * 30)),
		];
		const d1 = [
			A(0, along(top, out, -2.5)),
			A(0.5, polar(16, side * 26)),
			A(1, along(S, out, -2)),
		];
		return {
			tracks: [
				track("scorer", false, sc),
				track("screen", false, scr),
				track("def1", true, d1),
			],
			ball: [
				{ kind: "hold", role: "scorer", t0: 0, t1: 0.9 },
				{ kind: "shot", role: "scorer", t: 0.9 },
			],
		};
	},
};

// Pick-and-roll, roll man: the handler (passer) comes off the scorer's screen and
// feeds the scorer rolling hard to the rim. A real assist.
const pnrRoll: ShotGen = {
	id: "pnr-roll",
	needsPasser: true,
	build: ({ S, side }) => {
		const out = outward(S);
		const top = polar(22, side * 8);
		const pp = [A(0, top), A(0.5, polar(19, side * 4)), A(1, polar(20, side * 2))];
		const setAt = polar(15, side * 20);
		const sc = [A(0, setAt), A(0.4, polar(14, side * 14)), A(1, S)];
		const d2 = [
			A(0, along(top, out, -2.5)),
			A(0.5, polar(16, side * 20)),
			A(1, along(pp.at(-1)!, out, -2)),
		];
		return {
			tracks: [
				track("passer", false, pp),
				track("scorer", false, sc),
				track("def1", true, trail(sc, 3)),
				track("def2", true, d2),
			],
			ball: [
				{ kind: "hold", role: "passer", t0: 0, t1: 0.55 },
				{ kind: "pass", from: "passer", to: "scorer", t0: 0.55, t1: 0.75 },
				{ kind: "shot", role: "scorer", t: 0.9 },
			],
		};
	},
};

// Backdoor cut: the passer surveys up top and slips a pass to the scorer cutting
// backdoor to the rim behind an over-playing defender.
const backdoor: ShotGen = {
	id: "backdoor",
	needsPasser: true,
	build: ({ S, side }) => {
		const lat = lateral(outward(S));
		const top = polar(21, side * 6);
		const pp = [A(0, top), A(1, along(top, lat, -side * 1.5))];
		const w = polar(22, side * 38);
		const sc = [A(0, w), A(0.45, polar(15, side * 34)), A(1, S)];
		const d1 = [
			A(0, along(w, outward(w), -2.5)),
			A(0.5, polar(15, side * 30)),
			A(1, along(S, outward(S), 2)),
		];
		return {
			tracks: [
				track("passer", false, pp),
				track("scorer", false, sc),
				track("def1", true, d1),
				track("def2", true, trail(pp, 3)),
			],
			ball: [
				{ kind: "hold", role: "passer", t0: 0, t1: 0.5 },
				{ kind: "pass", from: "passer", to: "scorer", t0: 0.5, t1: 0.7 },
				{ kind: "shot", role: "scorer", t: 0.88 },
			],
		};
	},
};

// Transition, self-created: the scorer pushes the ball up the floor in the open
// court and finishes, a defender chasing from behind.
const transitionDrive: ShotGen = {
	id: "transition-drive",
	needsPasser: false,
	build: ({ S, side }) => {
		const start: V = { x: rand(30, 43), y: RIM_Y + side * rand(3, 11) };
		const sc = [A(0, start), A(0.55, polar(16, side * 18)), A(1, S)];
		return {
			tracks: [
				track("scorer", false, sc),
				track("def1", true, [
					A(0, { x: start.x - 5, y: start.y + side * 2 }),
					A(0.55, along(polar(16, side * 18), outward(polar(16, side * 18)), 2)),
					A(1, along(S, outward(S), 2.5)),
				]),
			],
			ball: [
				{ kind: "hold", role: "scorer", t0: 0, t1: 0.9 },
				{ kind: "shot", role: "scorer", t: 0.9 },
			],
		};
	},
};

// Transition, outlet: an outlet passer hits the scorer streaking ahead - a long
// cross-court/court-length pass leading to the finish.
const transitionOutlet: ShotGen = {
	id: "transition-outlet",
	needsPasser: true,
	build: ({ S, side }) => {
		const start: V = { x: rand(34, 46), y: RIM_Y + side * rand(4, 12) };
		const sc = [A(0, start), A(0.5, polar(15, side * 16)), A(1, S)];
		const pPos: V = { x: rand(22, 34), y: RIM_Y - side * rand(6, 14) };
		const pp = [A(0, pPos), A(1, along(pPos, { x: 1, y: 0 }, 3))];
		return {
			tracks: [
				track("passer", false, pp),
				track("scorer", false, sc),
				track("def1", true, [
					A(0, { x: start.x - 4, y: start.y + side * 2 }),
					A(1, along(S, outward(S), 2.5)),
				]),
			],
			ball: [
				{ kind: "hold", role: "passer", t0: 0, t1: 0.32 },
				{ kind: "pass", from: "passer", to: "scorer", t0: 0.32, t1: 0.6 },
				{ kind: "shot", role: "scorer", t: 0.9 },
			],
		};
	},
};

// Put-back: the scorer crashes the glass and goes right back up. The ball comes
// loose off the rim to him first.
const putback: ShotGen = {
	id: "putback",
	needsPasser: false,
	build: ({ S, side }) => {
		const sc = [
			A(0, polar(7, side * 32)),
			A(0.5, polar(3.5, side * 16)),
			A(1, S),
		];
		return {
			tracks: [
				track("scorer", false, sc),
				track("def1", true, trail(sc, 2.4)),
			],
			ball: [
				{ kind: "loose", t0: 0, t1: 0.45, from: RIM, toRole: "scorer" },
				{ kind: "hold", role: "scorer", t0: 0.45, t1: 0.6 },
				{ kind: "shot", role: "scorer", t: 0.6 },
			],
		};
	},
};

// Post-up, solo: the scorer works on the block and finishes over a defender.
const postSolo: ShotGen = {
	id: "post-solo",
	needsPasser: false,
	build: ({ S, side }) => {
		const out = outward(S);
		const lat = lateral(out);
		const sc = [
			A(0, along(S, out, 2)),
			A(0.55, along(S, lat, side * 1.5)),
			A(1, S),
		];
		return {
			tracks: [
				track("scorer", false, sc),
				track("def1", true, [
					A(0, along(S, out, -1.5)),
					A(1, along(S, out, -1)),
				]),
			],
			ball: [
				{ kind: "hold", role: "scorer", t0: 0, t1: 0.86 },
				{ kind: "shot", role: "scorer", t: 0.86 },
			],
		};
	},
};

// Post-up entry: a wing feeds the post, who finishes.
const postEntry: ShotGen = {
	id: "post-entry",
	needsPasser: true,
	build: ({ S, side }) => {
		const out = outward(S);
		const pp = [A(0, polar(20, side * 30)), A(1, polar(18, side * 34))];
		const sc = [A(0, along(S, out, 1.5)), A(0.6, S), A(1, along(S, out, -0.5))];
		return {
			tracks: [
				track("passer", false, pp),
				track("scorer", false, sc),
				track("def1", true, [
					A(0, along(S, out, -1.5)),
					A(1, along(S, out, -0.8)),
				]),
			],
			ball: [
				{ kind: "hold", role: "passer", t0: 0, t1: 0.4 },
				{ kind: "pass", from: "passer", to: "scorer", t0: 0.4, t1: 0.6 },
				{ kind: "shot", role: "scorer", t: 0.9 },
			],
		};
	},
};

// Face-up: from the mid-post, jab and rise into a short jumper.
const faceUp: ShotGen = {
	id: "face-up",
	needsPasser: false,
	build: ({ S, side }) => {
		const out = outward(S);
		const lat = lateral(out);
		const sc = [
			A(0, along(S, out, 2.5)),
			A(0.5, along(S, lat, side * 1.5)),
			A(1, S),
		];
		return {
			tracks: [
				track("scorer", false, sc),
				track("def1", true, trail(sc, 2.8)),
			],
			ball: [
				{ kind: "hold", role: "scorer", t0: 0, t1: 0.84 },
				{ kind: "shot", role: "scorer", t: 0.84 },
			],
		};
	},
};

// The free-throw set: shooter at the line, two players lined along the lane.
const freeThrow: ShotGen = {
	id: "ft",
	needsPasser: false,
	build: ({ S }) => ({
		tracks: [
			track("scorer", false, [A(0, S), A(1, S)]),
			track("def1", true, [A(0, { x: 84, y: 19 }), A(1, { x: 84, y: 19 })]),
			track("def2", true, [A(0, { x: 84, y: 31 }), A(1, { x: 84, y: 31 })]),
		],
		ball: [
			{ kind: "hold", role: "scorer", t0: 0, t1: 0.5 },
			{ kind: "shot", role: "scorer", t: 0.5 },
		],
	}),
};

// The generator pools per zone. Assisted shots (a passer is available) draw from
// the passer plays so a pass genuinely shows; self-created shots draw from the
// rest. Every zone has both kinds so makes, misses and blocks all animate.
const POOLS: Record<ShotCat, ShotGen[]> = {
	three: [catchAndShoot, handoff, pullUp, stepBack, pnrHandler, transitionDrive, transitionOutlet],
	mid: [catchAndShoot, handoff, pullUp, stepBack, pnrHandler, faceUp],
	rim: [
		driveIso,
		pnrHandler,
		putback,
		transitionDrive,
		pnrRoll,
		backdoor,
		transitionOutlet,
	],
	post: [postSolo, faceUp, postEntry],
	ft: [freeThrow],
};

// Build a shot play for a zone. When a passer is available we pick a play that
// shows a pass; otherwise a self-created one. Returns the built play plus the
// synthesized release spot (canonical) - the caller feeds that spot to placePlay
// so the shot flight and the shot-chart dot land on the spread location.
export const buildShotPlay = (
	cat: ShotCat,
	passer: boolean,
): { built: BuiltPlay; spot: V } => {
	const pool = POOLS[cat] ?? POOLS.mid;
	const want = pool.filter((g) => (passer ? g.needsPasser : !g.needsPasser));
	const list = want.length > 0 ? want : pool;
	const gen = list[Math.floor(Math.random() * list.length)]!;
	const spot = synthCanonicalSpot(cat);
	return { built: gen.build({ S: spot, side: sideOf(spot) }), spot };
};

// ---- Non-shot plays -------------------------------------------------------
export const buildNonShotPlay = (cat: NonShotCat): BuiltPlay => {
	if (cat === "steal") {
		const a = synthActionSpot();
		return {
			tracks: [
				track("victim", false, [
					A(0, along(a, { x: -1, y: 0 }, 4)),
					A(0.7, a),
					A(1, along(a, { x: 1, y: 0.4 }, 1)),
				]),
				track("stealer", true, [
					A(0, along(a, { x: 1, y: -1 }, 6)),
					A(0.7, along(a, { x: 0, y: -0.5 }, 1)),
					A(1, along(a, { x: 1, y: -1 }, 3)),
				]),
			],
			ball: [
				{ kind: "hold", role: "victim", t0: 0, t1: 0.6 },
				{ kind: "loose", t0: 0.6, t1: 0.82, fromRole: "victim", toRole: "stealer" },
				{ kind: "hold", role: "stealer", t0: 0.82, t1: 1 },
			],
		};
	}
	if (cat === "tov") {
		const a = synthActionSpot();
		return {
			tracks: [
				track("scorer", false, [
					A(0, along(a, { x: -1, y: -0.5 }, 5)),
					A(0.6, a),
					A(1, along(a, { x: 1, y: 0.6 }, 2)),
				]),
				track("def1", true, [
					A(0, along(a, { x: 1, y: -0.6 }, 5)),
					A(0.6, along(a, { x: 0.5, y: 0 }, 1)),
					A(1, along(a, { x: 1, y: 0.4 }, 3)),
				]),
			],
			ball: [
				{ kind: "hold", role: "scorer", t0: 0, t1: 0.6 },
				{ kind: "vanish", t: 0.72 },
			],
		};
	}
	if (cat === "orb" || cat === "drb") {
		const def = cat === "drb";
		const s: V = { x: rand(81, 86), y: RIM_Y + rand(-8, 8) };
		return {
			tracks: [
				track("scorer", def, [
					A(0, along(s, outward(s), 3)),
					A(0.55, along(s, outward(s), -1)),
					A(1, s),
				]),
			],
			ball: [
				{ kind: "loose", t0: 0, t1: 0.55, from: RIM, toRole: "scorer" },
				{ kind: "hold", role: "scorer", t0: 0.55, t1: 1 },
			],
		};
	}
	if (cat === "foul") {
		const a = synthActionSpot();
		return {
			tracks: [
				track("victim", false, [A(0, a), A(1, along(a, { x: 1, y: 0 }, 1))]),
				track("fouler", true, [
					A(0, along(a, { x: 1, y: 0.4 }, 5)),
					A(0.6, along(a, { x: 1, y: 0 }, 2)),
					A(1, along(a, { x: 1, y: 0 }, 3)),
				]),
			],
			ball: [{ kind: "hold", role: "victim", t0: 0, t1: 1 }],
		};
	}
	// jump ball
	return {
		tracks: [
			track("scorer", false, [
				A(0, { x: 45, y: 25 }),
				A(0.5, { x: 46.5, y: 24 }),
				A(1, { x: 44, y: 23 }),
			]),
			track("jumper2", true, [
				A(0, { x: 49, y: 25 }),
				A(0.5, { x: 47.5, y: 26 }),
				A(1, { x: 50, y: 27 }),
			]),
		],
		ball: [{ kind: "loose", t0: 0, t1: 0.6, from: { x: 47, y: 24 }, to: { x: 40, y: 22 } }],
	};
};

// The distinct roles a built play uses (for player binding).
export const playRoles = (built: BuiltPlay): PlayRole[] =>
	built.tracks.map((tr) => tr.role);

// ---- Bound, placed play (what the court actually animates) ----------------
export type LivePlayer = {
	pid: number;
	name: string;
	colorT: 0 | 1; // display team for coloring (0 = away, 1 = home)
	nodes: PathNode[]; // absolute court coordinates
};

export type LiveBall =
	| { kind: "hold"; pid: number; t0: number; t1: number }
	| { kind: "pass"; fromPid: number; toPid: number; t0: number; t1: number }
	| {
			kind: "shot";
			pid: number;
			spot: V;
			rimX: number;
			made: boolean;
			blocked: boolean;
			t: number;
	  }
	| {
			kind: "loose";
			t0: number;
			t1: number;
			from?: V;
			fromPid?: number;
			to?: V;
			toPid?: number;
	  }
	| { kind: "vanish"; t: number };

export type PlayInstance = {
	key: number;
	players: LivePlayer[];
	ball: LiveBall[];
	pulse?: "red" | "amber";
};

const COURT_W = 94;
const COURT_H = 50;
const RIM_INSET = 5.25;
const rimXForTeam = (t: 0 | 1): number =>
	t === 0 ? RIM_INSET : COURT_W - RIM_INSET;

// Which real player fills each role (team color is derived from the track).
export type Binding = Map<PlayRole, { pid: number; name: string }>;

export type PlaceOpts = {
	key: number;
	attackT: 0 | 1; // display team on offense (which rim they attack)
	flipY: boolean; // mirror top<->bottom (non-shot plays only; shots are spread)
	jitterX: number;
	jitterY: number;
	made: boolean;
	blocked: boolean;
	pulse?: "red" | "amber";
	// The canonical shot spot to release from (so the dot + flight use the
	// synthesized, spread location rather than a path endpoint). Optional.
	shotSpot?: V;
};

// Transform a canonical point (offense attacking RIGHT) onto the live court for
// the team actually attacking, with an optional top/bottom flip and jitter.
const placePoint = (p: V, o: PlaceOpts): V => ({
	x: (o.attackT === 0 ? COURT_W - p.x : p.x) + o.jitterX,
	y: (o.flipY ? COURT_H - p.y : p.y) + o.jitterY,
});

// Bind + place a built play into a ready-to-animate PlayInstance. Roles with no
// bound player are dropped (an optional screener / second defender that a given
// play doesn't have); ball segments referencing a missing role are skipped.
export const placePlay = (
	built: BuiltPlay,
	binding: Binding,
	o: PlaceOpts,
): PlayInstance => {
	const players: LivePlayer[] = [];
	const rolePid = new Map<PlayRole, number>();
	for (const tr of built.tracks) {
		const bound = binding.get(tr.role);
		if (!bound) {
			continue;
		}
		rolePid.set(tr.role, bound.pid);
		players.push({
			pid: bound.pid,
			name: bound.name,
			colorT: tr.def ? ((o.attackT === 0 ? 1 : 0) as 0 | 1) : o.attackT,
			nodes: tr.nodes.map((nd) => {
				const q = placePoint({ x: nd.x, y: nd.y }, o);
				return { t: nd.t, x: q.x, y: q.y };
			}),
		});
	}

	const rimX = rimXForTeam(o.attackT);
	const ball: LiveBall[] = [];
	for (const seg of built.ball) {
		if (seg.kind === "hold") {
			const pid = rolePid.get(seg.role);
			if (pid !== undefined) {
				ball.push({ kind: "hold", pid, t0: seg.t0, t1: seg.t1 });
			}
		} else if (seg.kind === "pass") {
			const fromPid = rolePid.get(seg.from);
			const toPid = rolePid.get(seg.to);
			if (fromPid !== undefined && toPid !== undefined) {
				ball.push({ kind: "pass", fromPid, toPid, t0: seg.t0, t1: seg.t1 });
			}
		} else if (seg.kind === "shot") {
			const pid = rolePid.get(seg.role);
			const shooter = players.find((p) => p.pid === pid);
			if (pid !== undefined && shooter) {
				const last = shooter.nodes.at(-1) ?? { x: rimX, y: 25 };
				const spot = o.shotSpot
					? placePoint(o.shotSpot, o)
					: { x: last.x, y: last.y };
				ball.push({
					kind: "shot",
					pid,
					spot,
					rimX,
					made: o.made,
					blocked: o.blocked,
					t: seg.t,
				});
			}
		} else if (seg.kind === "loose") {
			ball.push({
				kind: "loose",
				t0: seg.t0,
				t1: seg.t1,
				from: seg.from ? placePoint(seg.from, o) : undefined,
				fromPid: seg.fromRole ? rolePid.get(seg.fromRole) : undefined,
				to: seg.to ? placePoint(seg.to, o) : undefined,
				toPid: seg.toRole ? rolePid.get(seg.toRole) : undefined,
			});
		} else if (seg.kind === "vanish") {
			ball.push({ kind: "vanish", t: seg.t });
		}
	}

	return { key: o.key, players, ball, pulse: o.pulse };
};

// Sample a path smoothly (Catmull-Rom) at global play-fraction g in [0,1].
export const samplePath = (nodes: PathNode[], g: number): V => {
	if (nodes.length === 0) {
		return { x: COURT_W / 2, y: COURT_H / 2 };
	}
	if (nodes.length === 1 || g <= nodes[0]!.t) {
		return { x: nodes[0]!.x, y: nodes[0]!.y };
	}
	const lastNode = nodes.at(-1)!;
	if (g >= lastNode.t) {
		return { x: lastNode.x, y: lastNode.y };
	}
	let i = 0;
	while (i < nodes.length - 1 && !(g >= nodes[i]!.t && g < nodes[i + 1]!.t)) {
		i++;
	}
	const p1 = nodes[i]!;
	const p2 = nodes[i + 1]!;
	const p0 = nodes[i - 1] ?? p1;
	const p3 = nodes[i + 2] ?? p2;
	const span = p2.t - p1.t;
	const u = span > 0 ? (g - p1.t) / span : 0;
	const u2 = u * u;
	const u3 = u2 * u;
	// Catmull-Rom basis.
	const cr = (a: number, b: number, c: number, d: number) =>
		0.5 *
		(2 * b +
			(-a + c) * u +
			(2 * a - 5 * b + 4 * c - d) * u2 +
			(-a + 3 * b - 3 * c + d) * u3);
	return { x: cr(p0.x, p1.x, p2.x, p3.x), y: cr(p0.y, p1.y, p2.y, p3.y) };
};
