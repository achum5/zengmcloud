// ---------------------------------------------------------------------------
// A library of hand-authored basketball "plays" for the live-game court. Each
// template is a set of smooth keyframed paths (for the few players a play
// involves) plus a ball script (held, passed, shot). The live court picks one
// that matches the category of the play-by-play line about to happen, binds the
// real players (shooter, assister, inferred defenders) onto the template's
// roles, mirrors/flips it for variety, and plays the whole thing through to the
// action. Authored motion (curved drives, cuts, kick-outs) reads far more like
// real basketball than procedurally-computed straight-line glides.
//
// CANONICAL FRAME: every template is authored for the offense attacking the
// RIGHT rim (x = 88.75, y = 25), with play time t running 0 -> 1. The court is
// 94 (length) x 50 (width). Placement mirrors x for a team attacking left, and
// optionally flips y (top/bottom) so the same template yields many looks.
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
	// and the make/miss/block outcome (from the real event), so a template works
	// for a make, a miss, or a block alike.
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

// The bucket a play falls in. Shot categories carry the shot's release; the
// non-shot ones are their own actions.
export type ShotCat = "rim" | "post" | "mid" | "three" | "ft";
export type PlayCat =
	| ShotCat
	| "steal"
	| "tov"
	| "orb"
	| "drb"
	| "foul"
	| "jump";

export type PlayTemplate = {
	id: string;
	cat: PlayCat;
	// Requires a real passer (an assist / entry feed) to be shown.
	needsPasser: boolean;
	// Ends in a shot the engine resolves (flight + make/miss/block).
	hasShot: boolean;
	// May this template be flipped top<->bottom for variety? (Off for plays that
	// belong at a specific spot, e.g. free throws / jump ball.)
	flippable: boolean;
	tracks: Track[];
	ball: BallSeg[];
};

// ---- Canonical landmarks (attacking the RIGHT rim) ------------------------
const RIM: V = { x: 87.5, y: 25 };
// Handy authoring points.
const TOP: V = { x: 66, y: 25 };
const WING_T: V = { x: 70, y: 11 }; // "top" wing (small y)
const WING_B: V = { x: 70, y: 39 };
const CORNER_B: V = { x: 69, y: 46 };
const ELBOW_B: V = { x: 76, y: 32 };
const BLOCK_T: V = { x: 84, y: 19 };
const BLOCK_B: V = { x: 84, y: 31 };
const FT: V = { x: 75, y: 25 };

const n = (t: number, p: V, dx = 0, dy = 0): PathNode => ({
	t,
	x: p.x + dx,
	y: p.y + dy,
});
const track = (role: PlayRole, def: boolean, nodes: PathNode[]): Track => ({
	role,
	def,
	nodes,
});

// A defender path a step off an offensive path, toward the rim (between man and
// basket). Given the man's nodes, shift each toward the rim.
const shadow = (nodes: PathNode[], off = 3): PathNode[] =>
	nodes.map((p) => {
		const dx = RIM.x - p.x;
		const dy = RIM.y - p.y;
		const d = Math.hypot(dx, dy) || 1;
		return { t: p.t, x: p.x + (dx / d) * off, y: p.y + (dy / d) * off };
	});

// ---- The library ----------------------------------------------------------
export const PLAY_TEMPLATES: PlayTemplate[] = [
	// ===== RIM =====
	{
		id: "rim-iso-wing",
		cat: "rim",
		needsPasser: false,
		hasShot: true,
		flippable: true,
		tracks: (() => {
			const s = [n(0, WING_B), n(0.5, { x: 79, y: 33 }), n(1, RIM, -1.5, 1)];
			return [track("scorer", false, s), track("def1", true, shadow(s, 3.2))];
		})(),
		ball: [
			{ kind: "hold", role: "scorer", t0: 0, t1: 0.9 },
			{ kind: "shot", role: "scorer", t: 0.9 },
		],
	},
	{
		id: "rim-cut-backdoor",
		cat: "rim",
		needsPasser: true,
		hasShot: true,
		flippable: true,
		tracks: (() => {
			const p = [n(0, TOP), n(1, TOP, -2, 0)];
			const s = [n(0, WING_T, 0, 1), n(0.55, { x: 80, y: 18 }), n(1, RIM, -2, -1)];
			return [
				track("passer", false, p),
				track("scorer", false, s),
				track("def1", true, shadow(s, 3)),
				track("def2", true, shadow(p, 3)),
			];
		})(),
		ball: [
			{ kind: "hold", role: "passer", t0: 0, t1: 0.52 },
			{ kind: "pass", from: "passer", to: "scorer", t0: 0.52, t1: 0.72 },
			{ kind: "shot", role: "scorer", t: 0.9 },
		],
	},
	{
		id: "rim-pnr-roll",
		cat: "rim",
		needsPasser: true,
		hasShot: true,
		flippable: true,
		tracks: (() => {
			const p = [n(0, TOP, 2, 0), n(0.5, { x: 71, y: 22 }), n(1, { x: 68, y: 20 })];
			const s = [n(0, ELBOW_B, 2, 0), n(0.55, { x: 82, y: 30 }), n(1, RIM, -1, 1)];
			const scr = [n(0, { x: 72, y: 24 }), n(0.5, { x: 73, y: 24 }), n(1, { x: 74, y: 22 })];
			return [
				track("passer", false, p),
				track("scorer", false, s),
				track("screen", false, scr),
				track("def1", true, shadow(s, 3)),
			];
		})(),
		ball: [
			{ kind: "hold", role: "passer", t0: 0, t1: 0.6 },
			{ kind: "pass", from: "passer", to: "scorer", t0: 0.6, t1: 0.78 },
			{ kind: "shot", role: "scorer", t: 0.92 },
		],
	},
	{
		id: "rim-putback",
		cat: "rim",
		needsPasser: false,
		hasShot: true,
		flippable: true,
		tracks: (() => {
			const s = [n(0, BLOCK_B, 1, 0), n(0.6, RIM, -2, 1), n(1, RIM, -1, 0)];
			return [track("scorer", false, s), track("def1", true, shadow(s, 2.5))];
		})(),
		ball: [
			{ kind: "hold", role: "scorer", t0: 0, t1: 0.55 },
			{ kind: "shot", role: "scorer", t: 0.55 },
		],
	},

	// ===== THREE =====
	{
		id: "three-catch-corner",
		cat: "three",
		needsPasser: true,
		hasShot: true,
		flippable: true,
		tracks: (() => {
			const p = [n(0, TOP), n(0.5, TOP, -1, -2), n(1, TOP, -2, -3)];
			const s = [n(0, { x: 74, y: 42 }), n(0.55, CORNER_B, 1, -1), n(1, CORNER_B)];
			return [
				track("passer", false, p),
				track("scorer", false, s),
				track("def2", true, shadow(p, 3)),
				track("def1", true, shadow(s, 2.6)),
			];
		})(),
		ball: [
			{ kind: "hold", role: "passer", t0: 0, t1: 0.48 },
			{ kind: "pass", from: "passer", to: "scorer", t0: 0.48, t1: 0.7 },
			{ kind: "shot", role: "scorer", t: 0.86 },
		],
	},
	{
		id: "three-catch-wing",
		cat: "three",
		needsPasser: true,
		hasShot: true,
		flippable: true,
		tracks: (() => {
			const p = [n(0, ELBOW_B, 0, 2), n(1, { x: 72, y: 30 })];
			const s = [n(0, TOP, 2, -4), n(0.5, WING_T, 1, 1), n(1, WING_T)];
			return [
				track("passer", false, p),
				track("scorer", false, s),
				track("def1", true, shadow(s, 2.6)),
			];
		})(),
		ball: [
			{ kind: "hold", role: "passer", t0: 0, t1: 0.46 },
			{ kind: "pass", from: "passer", to: "scorer", t0: 0.46, t1: 0.68 },
			{ kind: "shot", role: "scorer", t: 0.85 },
		],
	},
	{
		id: "three-pullup-top",
		cat: "three",
		needsPasser: false,
		hasShot: true,
		flippable: true,
		tracks: (() => {
			const s = [n(0, { x: 58, y: 22 }), n(0.6, { x: 64, y: 24 }), n(1, TOP, -1, 0)];
			return [track("scorer", false, s), track("def1", true, shadow(s, 3))];
		})(),
		ball: [
			{ kind: "hold", role: "scorer", t0: 0, t1: 0.86 },
			{ kind: "shot", role: "scorer", t: 0.86 },
		],
	},
	{
		id: "three-stepback-wing",
		cat: "three",
		needsPasser: false,
		hasShot: true,
		flippable: true,
		tracks: (() => {
			const s = [n(0, { x: 76, y: 36 }), n(0.6, WING_B, 1, 1), n(1, WING_B, -2, 2)];
			const d = [n(0, { x: 79, y: 34 }), n(0.6, { x: 74, y: 37 }), n(1, { x: 73, y: 37 })];
			return [track("scorer", false, s), track("def1", true, d)];
		})(),
		ball: [
			{ kind: "hold", role: "scorer", t0: 0, t1: 0.85 },
			{ kind: "shot", role: "scorer", t: 0.85 },
		],
	},

	// ===== MID =====
	{
		id: "mid-pullup-elbow",
		cat: "mid",
		needsPasser: false,
		hasShot: true,
		flippable: true,
		tracks: (() => {
			const s = [n(0, { x: 69, y: 33 }), n(0.6, ELBOW_B, 0, 1), n(1, ELBOW_B)];
			return [track("scorer", false, s), track("def1", true, shadow(s, 3))];
		})(),
		ball: [
			{ kind: "hold", role: "scorer", t0: 0, t1: 0.82 },
			{ kind: "shot", role: "scorer", t: 0.82 },
		],
	},
	{
		id: "mid-floater",
		cat: "mid",
		needsPasser: false,
		hasShot: true,
		flippable: true,
		tracks: (() => {
			const s = [n(0, { x: 68, y: 22 }), n(0.6, { x: 77, y: 23 }), n(1, { x: 80, y: 24 })];
			return [track("scorer", false, s), track("def1", true, shadow(s, 2.8))];
		})(),
		ball: [
			{ kind: "hold", role: "scorer", t0: 0, t1: 0.84 },
			{ kind: "shot", role: "scorer", t: 0.84 },
		],
	},
	{
		id: "mid-catch-elbow",
		cat: "mid",
		needsPasser: true,
		hasShot: true,
		flippable: true,
		tracks: (() => {
			const p = [n(0, WING_B), n(1, WING_B, -1, -1)];
			const s = [n(0, TOP, 4, 4), n(0.5, ELBOW_B, -1, 0), n(1, ELBOW_B)];
			return [
				track("passer", false, p),
				track("scorer", false, s),
				track("def1", true, shadow(s, 2.8)),
			];
		})(),
		ball: [
			{ kind: "hold", role: "passer", t0: 0, t1: 0.46 },
			{ kind: "pass", from: "passer", to: "scorer", t0: 0.46, t1: 0.66 },
			{ kind: "shot", role: "scorer", t: 0.84 },
		],
	},

	// ===== POST =====
	{
		id: "post-up-solo",
		cat: "post",
		needsPasser: false,
		hasShot: true,
		flippable: true,
		tracks: (() => {
			const s = [n(0, BLOCK_B, 1, 1), n(0.6, BLOCK_B, 1, -1), n(1, { x: 85, y: 29 })];
			const d = [n(0, BLOCK_B, 3, 1), n(1, { x: 87, y: 28 })];
			return [track("scorer", false, s), track("def1", true, d)];
		})(),
		ball: [
			{ kind: "hold", role: "scorer", t0: 0, t1: 0.86 },
			{ kind: "shot", role: "scorer", t: 0.86 },
		],
	},
	{
		id: "post-entry",
		cat: "post",
		needsPasser: true,
		hasShot: true,
		flippable: true,
		tracks: (() => {
			const p = [n(0, WING_B), n(1, { x: 72, y: 38 })];
			const s = [n(0, BLOCK_B, 0, 1), n(0.6, BLOCK_B), n(1, { x: 85, y: 30 })];
			return [
				track("passer", false, p),
				track("scorer", false, s),
				track("def1", true, [n(0, BLOCK_B, 3, 0), n(1, { x: 87, y: 29 })]),
			];
		})(),
		ball: [
			{ kind: "hold", role: "passer", t0: 0, t1: 0.4 },
			{ kind: "pass", from: "passer", to: "scorer", t0: 0.4, t1: 0.6 },
			{ kind: "shot", role: "scorer", t: 0.88 },
		],
	},

	// ===== FREE THROW =====
	{
		id: "ft",
		cat: "ft",
		needsPasser: false,
		hasShot: true,
		flippable: false,
		tracks: [
			track("scorer", false, [n(0, FT), n(1, FT)]),
			track("def1", true, [n(0, BLOCK_T, 1, 0), n(1, BLOCK_T, 1, 0)]),
			track("def2", true, [n(0, BLOCK_B, 1, 0), n(1, BLOCK_B, 1, 0)]),
		],
		ball: [
			{ kind: "hold", role: "scorer", t0: 0, t1: 0.5 },
			{ kind: "shot", role: "scorer", t: 0.5 },
		],
	},

	// ===== STEAL =====
	{
		id: "steal",
		cat: "steal",
		needsPasser: false,
		hasShot: false,
		flippable: true,
		tracks: [
			track("victim", false, [n(0, { x: 68, y: 25 }), n(0.7, { x: 73, y: 25 }), n(1, { x: 74, y: 26 })]),
			track("stealer", true, [n(0, { x: 79, y: 20 }), n(0.7, { x: 74, y: 24 }), n(1, { x: 76, y: 21 })]),
		],
		ball: [
			{ kind: "hold", role: "victim", t0: 0, t1: 0.62 },
			{ kind: "loose", t0: 0.62, t1: 0.82, fromRole: "victim", toRole: "stealer" },
			{ kind: "hold", role: "stealer", t0: 0.82, t1: 1 },
		],
	},

	// ===== TURNOVER =====
	{
		id: "tov",
		cat: "tov",
		needsPasser: false,
		hasShot: false,
		flippable: true,
		tracks: [
			track("scorer", false, [n(0, { x: 66, y: 22 }), n(0.6, { x: 72, y: 26 }), n(1, { x: 74, y: 28 })]),
			track("def1", true, [n(0, { x: 74, y: 20 }), n(0.6, { x: 75, y: 25 }), n(1, { x: 76, y: 27 })]),
		],
		ball: [
			{ kind: "hold", role: "scorer", t0: 0, t1: 0.62 },
			{ kind: "vanish", t: 0.72 },
		],
	},

	// ===== OFFENSIVE / DEFENSIVE REBOUND =====
	{
		id: "orb",
		cat: "orb",
		needsPasser: false,
		hasShot: false,
		flippable: true,
		tracks: [
			track("scorer", false, [n(0, { x: 80, y: 18 }), n(0.55, { x: 85, y: 23 }), n(1, { x: 84, y: 24 })]),
		],
		ball: [{ kind: "loose", t0: 0, t1: 0.55, from: RIM, toRole: "scorer" }, { kind: "hold", role: "scorer", t0: 0.55, t1: 1 }],
	},
	{
		id: "drb",
		cat: "drb",
		needsPasser: false,
		hasShot: false,
		flippable: true,
		tracks: [
			track("scorer", true, [n(0, { x: 83, y: 30 }), n(0.55, { x: 86, y: 25 }), n(1, { x: 83, y: 24 })]),
		],
		ball: [{ kind: "loose", t0: 0, t1: 0.55, from: RIM, toRole: "scorer" }, { kind: "hold", role: "scorer", t0: 0.55, t1: 1 }],
	},

	// ===== FOUL =====
	{
		id: "foul",
		cat: "foul",
		needsPasser: false,
		hasShot: false,
		flippable: true,
		tracks: [
			track("victim", false, [n(0, { x: 76, y: 25 }), n(1, { x: 77, y: 25 })]),
			track("fouler", true, [n(0, { x: 81, y: 26 }), n(0.6, { x: 78, y: 25 }), n(1, { x: 79, y: 25 })]),
		],
		ball: [{ kind: "hold", role: "victim", t0: 0, t1: 1 }],
	},

	// ===== JUMP BALL =====
	{
		id: "jump",
		cat: "jump",
		needsPasser: false,
		hasShot: false,
		flippable: false,
		tracks: [
			track("scorer", false, [n(0, { x: 45, y: 25 }), n(0.5, { x: 46.5, y: 24 }), n(1, { x: 44, y: 23 })]),
			track("jumper2", true, [n(0, { x: 49, y: 25 }), n(0.5, { x: 47.5, y: 26 }), n(1, { x: 50, y: 27 })]),
		],
		ball: [{ kind: "loose", t0: 0, t1: 0.6, from: { x: 47, y: 24 }, to: { x: 40, y: 22 } }],
	},
];

// ---- Bound, placed play (what the court actually animates) ----------------
// A template is authored in canonical coordinates with abstract roles; binding
// fills each role with a real player and placement transforms the paths onto the
// live court (mirrored for the attacking team, optionally flipped top/bottom).

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
	flipY: boolean; // mirror top<->bottom for variety
	jitterX: number;
	jitterY: number;
	made: boolean;
	blocked: boolean;
	pulse?: "red" | "amber";
};

// Transform a canonical point (offense attacking RIGHT) onto the live court for
// the team actually attacking, with an optional top/bottom flip and jitter.
const placePoint = (p: V, o: PlaceOpts): V => ({
	x: (o.attackT === 0 ? COURT_W - p.x : p.x) + o.jitterX,
	y: (o.flipY ? COURT_H - p.y : p.y) + o.jitterY,
});

// Bind + place a template into a ready-to-animate PlayInstance. Roles with no
// bound player are dropped (an optional screener / second defender that a given
// play doesn't have); the ball segments referencing a missing role are skipped.
export const placePlay = (
	tpl: PlayTemplate,
	binding: Binding,
	o: PlaceOpts,
): PlayInstance => {
	const players: LivePlayer[] = [];
	const rolePid = new Map<PlayRole, number>();
	for (const tr of tpl.tracks) {
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
	for (const seg of tpl.ball) {
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
				ball.push({
					kind: "shot",
					pid,
					spot: { x: last.x, y: last.y },
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

// Pick a template for a category. Respects whether a real passer is available
// (templates that need one are excluded when there isn't). `rnd` in [0,1) picks
// among the matches; falls back to any template in the category.
export const pickTemplate = (
	cat: PlayCat,
	hasPasser: boolean,
	rnd: number,
): PlayTemplate | undefined => {
	const all = PLAY_TEMPLATES.filter((p) => p.cat === cat);
	if (all.length === 0) {
		return undefined;
	}
	const ok = all.filter((p) => !p.needsPasser || hasPasser);
	const pool = ok.length > 0 ? ok : all.filter((p) => !p.needsPasser);
	const list = pool.length > 0 ? pool : all;
	return list[Math.min(list.length - 1, Math.floor(rnd * list.length))];
};

// The distinct offensive/defensive roles a template uses (for player binding).
export const templateRoles = (tpl: PlayTemplate): PlayRole[] =>
	tpl.tracks.map((tr) => tr.role);
