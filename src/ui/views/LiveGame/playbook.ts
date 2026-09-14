import { courtRandom } from "./courtRng.ts";
import {
	clampY,
	dirAcross,
	type Dir,
	type FieldActor,
	type FieldPoint,
	type Slot,
	toField,
} from "./fieldSpots.ts";

// A PLAYBOOK.
//
// The field could already show where a play started and where it ended. What it
// could not show was the PLAY - eleven men with eleven jobs, which is the thing
// anyone who watches football is actually watching. A completion for fourteen
// yards was a ball going from one dot to another; it should be a receiver
// running a dig while the back checks down and the line holds up a four-man
// rush.
//
// So: a route tree, the concepts that combine those routes, and the run schemes
// a back is actually following. These are the common property of the sport -
// every one of them is drawn in any coaching manual - and they are written here
// from the football, not lifted from anyone's game. What the sim reports (down,
// distance, how far the ball travelled, whether the quarterback went down)
// chooses which of them gets called, so the play you watch is always consistent
// with the play that happened.

// A route in the receiver's own terms: how far DOWNFIELD each waypoint is, and
// how far to the side, where positive is toward the sideline he is nearest and
// negative is back toward the middle of the field. Mirroring for the other side
// of the formation - and for the other direction of play - happens once, in
// routePath, so every route below can be written the way a coach draws it.
type RoutePoint = { d: number; a: number };

export type RouteName =
	| "go"
	| "seam"
	| "post"
	| "corner"
	| "out"
	| "dig"
	| "curl"
	| "comeback"
	| "slant"
	| "drag"
	| "flat"
	| "wheel"
	| "screen"
	| "checkdown"
	| "block";

export const ROUTES: Record<RouteName, RoutePoint[]> = {
	// Straight up the sideline, leaning out as he stacks the corner.
	go: [
		{ d: 6, a: 0.3 },
		{ d: 15, a: 0.8 },
		{ d: 25, a: 0.6 },
		{ d: 35, a: 0.4 },
	],
	// The same speed, run up the hashes instead - the inside version.
	seam: [
		{ d: 6, a: -0.8 },
		{ d: 15, a: -1.4 },
		{ d: 25, a: -1.2 },
		{ d: 33, a: -1 },
	],
	// Stem upfield, then break hard for the goalpost.
	post: [
		{ d: 6, a: 0 },
		{ d: 12, a: 0.4 },
		{ d: 18, a: -4 },
		{ d: 25, a: -9.5 },
		{ d: 32, a: -15 },
	],
	// The mirror of a post: break away from the middle, toward the pylon.
	corner: [
		{ d: 6, a: 0 },
		{ d: 12, a: -0.4 },
		{ d: 18, a: 4 },
		{ d: 24, a: 9 },
		{ d: 29, a: 13 },
	],
	out: [
		{ d: 4, a: 0 },
		{ d: 9, a: 0 },
		{ d: 10.5, a: 4 },
		{ d: 11, a: 9.5 },
	],
	// A dig: same stem, broken back across the field instead.
	dig: [
		{ d: 4, a: 0 },
		{ d: 10, a: 0 },
		{ d: 12, a: -5 },
		{ d: 13, a: -12 },
	],
	// Up, sit down, and work back toward the throw.
	curl: [
		{ d: 5, a: 0 },
		{ d: 11, a: 0 },
		{ d: 12, a: -0.5 },
		{ d: 10, a: -1.5 },
	],
	comeback: [
		{ d: 6, a: 0 },
		{ d: 13, a: 0 },
		{ d: 15, a: 0.5 },
		{ d: 12, a: 3 },
	],
	slant: [
		{ d: 1.5, a: 0.3 },
		{ d: 4, a: -1.8 },
		{ d: 7, a: -4 },
		{ d: 10.5, a: -6.5 },
	],
	// Underneath, all the way across the formation.
	drag: [
		{ d: 2, a: 0 },
		{ d: 4, a: -5 },
		{ d: 5, a: -11 },
		{ d: 6, a: -18 },
	],
	flat: [
		{ d: 1, a: 1 },
		{ d: 2.5, a: 5 },
		{ d: 3.5, a: 10 },
		{ d: 4, a: 14 },
	],
	// Out of the backfield, into the flat, then straight up the sideline.
	wheel: [
		{ d: 1, a: 3 },
		{ d: 3, a: 7 },
		{ d: 8, a: 9 },
		{ d: 16, a: 8.5 },
		{ d: 25, a: 8 },
	],
	// Behind the line, wait for the wall, then up.
	screen: [
		{ d: -2, a: 1 },
		{ d: -2.5, a: 5 },
		{ d: -1, a: 8 },
		{ d: 3, a: 9 },
	],
	checkdown: [
		{ d: 1, a: 1.5 },
		{ d: 3, a: 3.5 },
		{ d: 4.5, a: 5 },
	],
	// He isn't going anywhere: he has somebody to block.
	block: [],
};

// THE SLOTS A CONCEPT TALKS ABOUT. The offensive formations are written in one
// fixed order (see fieldSpots), so a concept can name a job by its slot and
// know it reaches the right man whichever formation is on the field.
export const SLOT_C = 0;
export const SLOT_LG = 1;
export const SLOT_RG = 2;
export const SLOT_LT = 3;
export const SLOT_RT = 4;
export const SLOT_TE = 5;
export const SLOT_QB = 6;
export const SLOT_RB = 7;
export const SLOT_WR_SLOT = 8;
export const SLOT_WR_L = 9;
export const SLOT_WR_R = 10;

// The five men who can catch it, which is what a concept assigns.
export const ELIGIBLE = [
	SLOT_TE,
	SLOT_RB,
	SLOT_WR_SLOT,
	SLOT_WR_L,
	SLOT_WR_R,
];

export type PassConcept = {
	name: string;
	routes: Partial<Record<number, RouteName>>;
	// Roughly how far downfield the concept is designed to be caught. Used to
	// pick a concept that matches the throw the sim actually reported, so a
	// four-yard completion is never staged as four verticals.
	depth: "short" | "medium" | "deep";
	// The quarterback holds it - play action, or a screen setting up.
	hold?: number;
};

// Concepts every level of football runs, from the quick game out to the shots.
export const PASS_CONCEPTS: PassConcept[] = [
	{
		name: "Slant–Flat",
		depth: "short",
		routes: {
			[SLOT_WR_L]: "slant",
			[SLOT_WR_SLOT]: "flat",
			[SLOT_WR_R]: "slant",
			[SLOT_TE]: "block",
			[SLOT_RB]: "block",
		},
	},
	{
		name: "Stick",
		depth: "short",
		routes: {
			[SLOT_WR_SLOT]: "out",
			[SLOT_TE]: "curl",
			[SLOT_WR_L]: "slant",
			[SLOT_WR_R]: "go",
			[SLOT_RB]: "flat",
		},
	},
	{
		name: "Mesh",
		depth: "short",
		routes: {
			[SLOT_WR_SLOT]: "drag",
			[SLOT_TE]: "drag",
			[SLOT_WR_R]: "curl",
			[SLOT_WR_L]: "comeback",
			[SLOT_RB]: "checkdown",
		},
	},
	{
		name: "Running Back Screen",
		depth: "short",
		hold: 0.34,
		routes: {
			[SLOT_RB]: "screen",
			[SLOT_WR_L]: "go",
			[SLOT_WR_R]: "go",
			[SLOT_WR_SLOT]: "drag",
			[SLOT_TE]: "block",
		},
	},
	{
		name: "Curl–Flat",
		depth: "medium",
		routes: {
			[SLOT_WR_L]: "curl",
			[SLOT_WR_R]: "curl",
			[SLOT_WR_SLOT]: "flat",
			[SLOT_TE]: "drag",
			[SLOT_RB]: "block",
		},
	},
	{
		name: "Dagger",
		depth: "medium",
		routes: {
			[SLOT_WR_SLOT]: "seam",
			[SLOT_WR_L]: "dig",
			[SLOT_WR_R]: "comeback",
			[SLOT_TE]: "drag",
			[SLOT_RB]: "block",
		},
	},
	{
		name: "Smash",
		depth: "medium",
		routes: {
			[SLOT_WR_R]: "curl",
			[SLOT_WR_SLOT]: "corner",
			[SLOT_WR_L]: "out",
			[SLOT_TE]: "drag",
			[SLOT_RB]: "flat",
		},
	},
	{
		name: "Flood",
		depth: "medium",
		routes: {
			[SLOT_WR_R]: "go",
			[SLOT_WR_SLOT]: "corner",
			[SLOT_TE]: "out",
			[SLOT_RB]: "flat",
			[SLOT_WR_L]: "comeback",
		},
	},
	{
		name: "Four Verticals",
		depth: "deep",
		routes: {
			[SLOT_WR_L]: "go",
			[SLOT_WR_R]: "go",
			[SLOT_WR_SLOT]: "seam",
			[SLOT_TE]: "seam",
			[SLOT_RB]: "checkdown",
		},
	},
	{
		name: "Post–Wheel",
		depth: "deep",
		routes: {
			[SLOT_WR_L]: "post",
			[SLOT_RB]: "wheel",
			[SLOT_WR_R]: "go",
			[SLOT_WR_SLOT]: "dig",
			[SLOT_TE]: "block",
		},
	},
	{
		name: "Play Action Shot",
		depth: "deep",
		hold: 0.28,
		routes: {
			[SLOT_WR_R]: "post",
			[SLOT_WR_L]: "go",
			[SLOT_WR_SLOT]: "drag",
			[SLOT_TE]: "block",
			[SLOT_RB]: "block",
		},
	},
];

export type RunScheme = {
	name: string;
	// Where the back is aiming as he crosses the line, in yards off the ball.
	// Negative is to one side, positive the other; a toss is way outside, a
	// sneak is straight ahead.
	aim: number;
	// A false step, a ride, or a draw's pause - how much of the play he spends
	// before he starts downhill.
	hold: number;
	// The back presses one way and cuts back the other: this is how far the
	// press goes before the cut.
	press?: number;
};

export const RUN_SCHEMES: RunScheme[] = [
	{ name: "Inside Zone", aim: 1.5, hold: 0.16, press: 3 },
	{ name: "Outside Zone", aim: 7, hold: 0.14, press: 9 },
	{ name: "Power", aim: 3.5, hold: 0.18 },
	{ name: "Counter", aim: -3, hold: 0.22, press: -5 },
	{ name: "Toss", aim: 12, hold: 0.1, press: 14 },
	{ name: "Draw", aim: 0.5, hold: 0.34 },
	{ name: "Trap", aim: -1.5, hold: 0.16 },
	{ name: "Sweep", aim: 9, hold: 0.16, press: 11 },
];

export const QB_SNEAK: RunScheme = { name: "QB Sneak", aim: 0, hold: 0.08 };
export const QB_SCRAMBLE: RunScheme = {
	name: "Scramble",
	aim: 6,
	hold: 0.42,
	press: -4,
};
export const KNEEL_DOWN: RunScheme = { name: "Victory", aim: 0, hold: 0.2 };

// WHICH PLAY WAS THAT? The sim never says, but it says enough: how far the ball
// travelled in the air, how far the runner got, the down and the distance. A
// concept is drawn from the ones whose design depth matches the throw, so the
// play on screen is never contradicted by the play in the text.
export const callPass = ({
	airYards,
	toGo,
	sacked,
}: {
	// How far the ball actually travelled, when that is known. On the dropback
	// it is NOT known - the throw has not happened - and passing the
	// quarterback's drop depth in its place used to call a screen on every
	// third down, because a five-yard drop looks exactly like a throw behind
	// the line. Undefined means "call it from the situation", which is what a
	// coach does anyway: the call is made before the snap.
	airYards: number | undefined;
	toGo: number;
	sacked: boolean;
}): PassConcept => {
	const depth: PassConcept["depth"] =
		airYards === undefined || sacked
			? toGo >= 12
				? "deep"
				: toGo >= 6
					? "medium"
					: "short"
			: airYards >= 17
				? "deep"
				: airYards >= 7
					? "medium"
					: "short";
	let pool = PASS_CONCEPTS.filter((c) => c.depth === depth);
	// A screen is a specific thing - a throw at or behind the line - so it is
	// only ever called when the ball really did go there.
	if (airYards !== undefined && airYards <= 1 && !sacked) {
		const screen = PASS_CONCEPTS.find((c) => c.name.includes("Screen"));
		if (screen) {
			return screen;
		}
	}
	// A screen is NEVER drawn at random - it is a short concept, so it sat in
	// the short pool and got called on throws that went nowhere near the line.
	// It is only ever the deliberate choice above.
	pool = pool.filter((c) => !c.name.includes("Screen"));
	if (pool.length === 0) {
		pool = PASS_CONCEPTS;
	}
	return pool[Math.floor(courtRandom() * pool.length)]!;
};

export const callRun = ({
	yards,
	down,
	toGo,
	byQuarterback,
	kneel,
}: {
	yards: number;
	down: number;
	toGo: number;
	byQuarterback: boolean;
	kneel: boolean;
}): RunScheme => {
	if (kneel) {
		return KNEEL_DOWN;
	}
	if (byQuarterback) {
		// A yard on fourth-and-inches is a sneak; anything that actually gained
		// ground was the quarterback leaving the pocket.
		return toGo <= 2 && yards <= 3 ? QB_SNEAK : QB_SCRAMBLE;
	}
	// Third and long that the offense ran anyway is a draw; short yardage is
	// downhill; everything else is drawn from the rest.
	if (down >= 3 && toGo >= 7) {
		return RUN_SCHEMES.find((s) => s.name === "Draw")!;
	}
	if (toGo <= 2) {
		const pool = RUN_SCHEMES.filter((s) =>
			["Inside Zone", "Power", "Trap"].includes(s.name),
		);
		return pool[Math.floor(courtRandom() * pool.length)]!;
	}
	// A long gain came from a play designed to get outside far more often than
	// from a dive, and vice versa.
	const pool = RUN_SCHEMES.filter((s) =>
		yards >= 9 ? s.aim >= 3 : s.name !== "Draw",
	);
	const from = pool.length > 0 ? pool : RUN_SCHEMES;
	return from[Math.floor(courtRandom() * from.length)]!;
};

// THE ROUTE ON THE FIELD. A route is written from where the man lines up, so it
// is placed by the same two mirrors the formation is: which side of the ball he
// is on (so "out" means his own sideline), and which way the offense is moving.
export const routePath = ({
	slot,
	losX,
	dir,
	ballAcross,
	route,
}: {
	slot: Slot;
	losX: number;
	dir: Dir;
	ballAcross: number;
	route: RouteName;
}): FieldPoint[] => {
	const points = ROUTES[route];
	if (points.length === 0) {
		return [];
	}
	// Which way is "toward his sideline". A man lined up on the ball (the back,
	// the quarterback) has no side of his own, so he works to the right.
	const side = slot.across < -0.5 ? -1 : 1;
	const mirror = dirAcross(dir);
	return [{ d: 0, a: 0 }, ...points].map(({ d, a }) =>
		toField(
			losX,
			dir,
			slot.depth - d,
			ballAcross + (slot.across + a * side) * mirror,
		),
	);
};

// THE BACK'S PATH. Unlike a route this one has a fact to honour at the end: the
// sim says how far he got, so the scheme decides only how he got there - the
// aiming point he hits at the line, the press before the cut, and the ride or
// the pause before he starts. Everything is pulled back inbounds, because a
// toss aiming twelve yards outside a ball already on the hash is a run into the
// bench.
export const runPath = ({
	start,
	end,
	scheme,
	losX,
	dir,
}: {
	start: FieldPoint;
	end: FieldPoint;
	scheme: RunScheme;
	losX: number;
	dir: Dir;
}): FieldPoint[] => {
	const mirror = dirAcross(dir);
	const aimY = clampY(start.y + scheme.aim * mirror);
	const pressY =
		scheme.press === undefined
			? undefined
			: clampY(start.y + scheme.press * mirror);
	// The press: he attacks one gap before the cut, which is the whole point of
	// a zone play and the whole point of a counter. Then he crosses the line at
	// the aiming point, and from there he is running at the end point the sim
	// gave us, with one cut on the way if there is room for one.
	const remaining = (end.x - losX) * dir;
	return [
		start,
		...(pressY === undefined
			? []
			: [
					{ x: start.x - dir * 1.2, y: (start.y + pressY) / 2 },
					{ x: losX + dir * 0.4, y: pressY },
				]),
		// He only crosses the line if the play got that far: a handoff shown on
		// its own beat, or a run that lost yardage, never gets there.
		...(remaining > 0 ? [{ x: losX + dir * 1.2, y: aimY }] : []),
		...(remaining > 7
			? [
					{
						x: losX + dir * remaining * 0.55,
						y: clampY((aimY + end.y) / 2 + (end.y > aimY ? -2.5 : 2.5)),
					},
				]
			: []),
		end,
	];
};

// ============================================================================
// ELEVEN JOBS, HANDED OUT.
// ============================================================================

type Geom = {
	losX: number;
	dir: Dir;
	ballAcross: number;
};

// THE OFFENSE. The five eligible men get the concept's routes; the line sets
// and holds. A route is only ever a path - where a man ENDS is still whatever
// the play actually did to him, so a receiver who caught it finishes at the
// catch and the rest finish where their route ran out.
export const assignRoutes = ({
	actors,
	slots,
	concept,
	geom,
	protectDepth,
}: {
	actors: FieldActor[];
	slots: Slot[];
	concept: PassConcept;
	geom: Geom;
	// How deep the quarterback set up, so the line can form a pocket in front
	// of him rather than standing on the ball.
	protectDepth: number;
}): FieldActor[] =>
	actors.map((actor) => {
		const i = actor.slotIndex;
		if (i === undefined) {
			return actor;
		}
		const slot = slots[i];
		if (!slot) {
			return actor;
		}

		// The line: a short set back into the pocket, opening as it goes. Guards
		// and tackles give more ground than the centre, which is what a pocket
		// looks like from above.
		if (i <= SLOT_RT) {
			const spread = 1 + Math.abs(slot.across) * 0.14;
			const set = toField(
				geom.losX,
				geom.dir,
				slot.depth + Math.min(protectDepth - 1.5, 1.6 + spread * 0.5),
				geom.ballAcross + slot.across * dirAcross(geom.dir) * 1.12,
			);
			return {
				...actor,
				x: set.x,
				y: set.y,
				path: [{ x: actor.x, y: actor.y }, set],
				delay: 0.02,
			};
		}

		// The quarterback: back to his launch point and then still, which is what
		// makes the routes in front of him read as routes.
		if (i === SLOT_QB) {
			const drop = toField(
				geom.losX,
				geom.dir,
				protectDepth,
				geom.ballAcross + slot.across * dirAcross(geom.dir),
			);
			return {
				...actor,
				x: drop.x,
				y: drop.y,
				path: [{ x: actor.x, y: actor.y }, drop],
			};
		}

		const route = concept.routes[i];
		if (!route || route === "block") {
			return actor;
		}
		const path = routePath({ slot, route, ...geom });
		if (path.length === 0) {
			return actor;
		}
		const end = path.at(-1)!;
		return {
			...actor,
			x: end.x,
			y: end.y,
			path,
			delay: concept.hold ?? 0,
		};
	});

// THE HANDOFF AND EVERYTHING BEHIND IT. On a running play the line fires off
// low into the front instead of setting, which is the difference a viewer reads
// first - a run looks like a run before the back has gone anywhere.
export const assignRunBlocking = ({
	actors,
	slots,
	scheme,
	geom,
}: {
	actors: FieldActor[];
	slots: Slot[];
	scheme: RunScheme;
	geom: Geom;
}): FieldActor[] =>
	actors.map((actor) => {
		const i = actor.slotIndex;
		if (i === undefined || i > SLOT_RT) {
			return actor;
		}
		const slot = slots[i]!;
		// Everybody steps toward the play - the whole line moving as one in the
		// direction of the aiming point is what zone blocking looks like.
		const drive = toField(
			geom.losX,
			geom.dir,
			slot.depth - 1.4,
			geom.ballAcross +
				(slot.across + scheme.aim * 0.22) * dirAcross(geom.dir),
		);
		return {
			...actor,
			x: drive.x,
			y: drive.y,
			path: [{ x: actor.x, y: actor.y }, drive],
			delay: 0.02,
		};
	});

// THE DEFENSE, IN A BASE FRONT. Four rush, the linebackers and corners take the
// men in front of them, and the safeties get depth. It is not a coverage
// install - it is the shape of one, which is all a graphic needs to stop
// looking like eleven statues.
const COVER_ASSIGNMENTS: Record<number, number> = {
	4: SLOT_WR_SLOT,
	5: SLOT_RB,
	6: SLOT_TE,
	7: SLOT_WR_L,
	8: SLOT_WR_R,
};

export const assignPassDefense = ({
	defenders,
	defSlots,
	receivers,
	target,
	geom,
	reachTarget,
}: {
	defenders: FieldActor[];
	defSlots: Slot[];
	// The offense's actors, so a cover man can follow the route his man is
	// actually running.
	receivers: FieldActor[];
	// Where the quarterback set up.
	target: FieldPoint;
	geom: Geom;
	// True on a sack: the rush gets home instead of stopping short.
	reachTarget: boolean;
}): FieldActor[] =>
	defenders.map((actor) => {
		const i = actor.slotIndex;
		if (i === undefined) {
			return actor;
		}
		const start = { x: actor.x, y: actor.y };

		// The front four, coming off the edge and up the middle.
		if (i <= 3) {
			const stop = reachTarget ? 0.4 : 2.2 + courtRandom() * 1.6;
			const dx = target.x - start.x;
			const dy = target.y - start.y;
			const dist = Math.max(0.1, Math.hypot(dx, dy));
			const f = Math.max(0, (dist - stop) / dist);
			// An edge rusher loops; an interior man goes straight through.
			const arc = Math.abs(defSlots[i]?.across ?? 0) > 4 ? 2.6 : 0.6;
			const end = { x: start.x + dx * f, y: clampY(start.y + dy * f) };
			return {
				...actor,
				x: end.x,
				y: end.y,
				path: [
					start,
					{
						x: start.x + dx * f * 0.5,
						y: clampY(start.y + dy * f * 0.5 + arc * (dy >= 0 ? -1 : 1)),
					},
					end,
				],
			};
		}

		// The two safeties, getting to their depth.
		if (i >= 9) {
			const slot = defSlots[i]!;
			const deep = toField(
				geom.losX,
				geom.dir,
				slot.depth - 9,
				geom.ballAcross + slot.across * 1.25 * dirAcross(geom.dir),
			);
			return {
				...actor,
				x: deep.x,
				y: deep.y,
				path: [start, deep],
			};
		}

		// Everybody else has a man. He trails him - a step behind and a step to
		// the inside, which is where a defender in coverage actually is.
		const mySlot = COVER_ASSIGNMENTS[i];
		const man = receivers.find((r) => r.slotIndex === mySlot);
		if (!man?.path || man.path.length < 2) {
			return actor;
		}
		const inside = geom.ballAcross > man.y ? 1.4 : -1.4;
		const trail = man.path.slice(1).map((p) => ({
			x: p.x - geom.dir * 1.6,
			y: clampY(p.y + inside),
		}));
		const end = trail.at(-1)!;
		return {
			...actor,
			x: end.x,
			y: end.y,
			path: [start, ...trail],
			delay: 0.04,
		};
	});

// A RUN, FROM THE OTHER SIDE. Everyone flows to the ball: the front seven get
// there, the secondary closes from depth. The lag is what makes it read as
// pursuit rather than as a magnet.
export const assignRunPursuit = ({
	defenders,
	ballEnd,
	geom,
}: {
	defenders: FieldActor[];
	ballEnd: FieldPoint;
	geom: Geom;
}): FieldActor[] =>
	defenders.map((actor) => {
		const i = actor.slotIndex;
		if (i === undefined) {
			return actor;
		}
		const start = { x: actor.x, y: actor.y };
		// How much of the way he actually gets: the front seven arrive, the deep
		// men are still closing when the whistle goes.
		const closes = i <= 3 ? 0.88 : i <= 6 ? 0.82 : i <= 8 ? 0.7 : 0.55;
		const spread = ((i % 5) - 2) * 1.3;
		const end = {
			x: start.x + (ballEnd.x - start.x) * closes - geom.dir * 0.8,
			y: clampY(start.y + (ballEnd.y - start.y) * closes + spread),
		};
		return {
			...actor,
			x: end.x,
			y: end.y,
			path: [
				start,
				{
					x: start.x + (end.x - start.x) * 0.45,
					y: clampY(start.y + (end.y - start.y) * 0.55),
				},
				end,
			],
			delay: 0.05 + (i > 6 ? 0.08 : 0),
		};
	});
