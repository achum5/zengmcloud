import { courtRandom } from "./courtRng.ts";
import type { CoverageShell } from "./coverages.ts";
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

// READING IT. A route is a plan, and the whole craft of playing receiver is
// changing the plan once you see what the defense is doing. Until now the
// offense ran its concept into whatever the defense happened to be playing and
// the two never touched each other.
//
// These are the conversions every receiver in football is taught:
//
//   Two men deep      you cannot run past both of them, so the deep routes
//                     come back to the ball and the intermediate stuff stays.
//   One man deep      the middle of the field is closed and the outside is
//                     one-on-one, so routes break AWAY from the free safety
//                     and a comeback turns into a go.
//   Man coverage      break away from the man trailing you: a curl becomes a
//                     comeback, because he has no help to pass you off to.
//   Nobody deep       because everybody is coming. There is no time for any of
//                     it, so every route gets HOT and the ball comes out now.
const VS_TWO_HIGH: Partial<Record<RouteName, RouteName>> = {
	go: "comeback",
	wheel: "out",
	corner: "out",
};

const VS_SINGLE_HIGH: Partial<Record<RouteName, RouteName>> = {
	post: "corner",
	seam: "corner",
	comeback: "go",
};

const VS_MAN: Partial<Record<RouteName, RouteName>> = {
	curl: "comeback",
	seam: "go",
};

const VS_BLITZ: Partial<Record<RouteName, RouteName>> = {
	go: "slant",
	seam: "slant",
	post: "slant",
	corner: "out",
	dig: "drag",
	comeback: "out",
	curl: "out",
	wheel: "flat",
};

export const adjustRoute = (
	route: RouteName,
	shell: CoverageShell | undefined,
): RouteName => {
	if (route === "block" || route === "screen" || shell === undefined) {
		return route;
	}
	const table =
		shell === "twoHigh"
			? VS_TWO_HIGH
			: shell === "singleHigh"
				? VS_SINGLE_HIGH
				: shell === "man"
					? VS_MAN
					: VS_BLITZ;
	return table[route] ?? route;
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
export const ELIGIBLE = [SLOT_TE, SLOT_RB, SLOT_WR_SLOT, SLOT_WR_L, SLOT_WR_R];

export type PassConcept = {
	name: string;
	routes: Partial<Record<number, RouteName>>;
	// Roughly how far downfield the concept is designed to be caught. Used to
	// pick a concept that matches the throw the sim actually reported, so a
	// four-yard completion is never staged as four verticals.
	depth: "short" | "medium" | "deep";
	// The quarterback holds it - play action, or a screen setting up.
	hold?: number;
	// The line releases downfield to build a wall in front of the catch.
	screen?: boolean;
	// There is a fake in it: the back and the quarterback both sell the run
	// before anybody looks downfield.
	playAction?: boolean;
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
		screen: true,
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
		playAction: true,
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
	// Somebody leaves the line and leads the play: a guard on power, a guard
	// and a tackle on counter, the play-side guard out in front on a sweep.
	// It is the single clearest tell of what a running play IS.
	pull?: "backsideGuard" | "guardAndTackle" | "playsideGuard";
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
	// It began as a pass. The line is protecting, the receivers are running
	// routes, and the man carrying it is the quarterback leaving a pocket that
	// did not hold - which looks nothing like a designed run, and used to be
	// drawn as one.
	dropback?: boolean;
};

export const RUN_SCHEMES: RunScheme[] = [
	{ name: "Inside Zone", aim: 1.5, hold: 0.16, press: 3 },
	{ name: "Outside Zone", aim: 7, hold: 0.14, press: 9 },
	{ name: "Power", aim: 3.5, hold: 0.18, pull: "backsideGuard" },
	{ name: "Counter", aim: -3, hold: 0.22, press: -5, pull: "guardAndTackle" },
	{ name: "Toss", aim: 12, hold: 0.1, press: 14, pull: "playsideGuard" },
	{ name: "Draw", aim: 0.5, hold: 0.34 },
	{ name: "Trap", aim: -1.5, hold: 0.16, pull: "backsideGuard" },
	{ name: "Sweep", aim: 9, hold: 0.16, press: 11, pull: "playsideGuard" },
];

export const QB_SNEAK: RunScheme = { name: "QB Sneak", aim: 0, hold: 0.08 };
export const QB_SCRAMBLE: RunScheme = {
	name: "Scramble",
	aim: 6,
	hold: 0.42,
	press: -4,
	dropback: true,
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
	// Yards to run the whole route deeper than it is written. Used for the one
	// thing that needs it: two men crossing each other (see assignRoutes).
	deeper = 0,
}: {
	slot: Slot;
	losX: number;
	dir: Dir;
	ballAcross: number;
	route: RouteName;
	deeper?: number;
}): FieldPoint[] => {
	const points = ROUTES[route];
	if (points.length === 0) {
		return [];
	}
	// Which way is "toward his sideline". A man lined up on the ball (the back,
	// the quarterback) has no side of his own, so he works to the right.
	const side = slot.across < -0.5 ? -1 : 1;
	const mirror = dirAcross(dir);
	return [{ d: 0, a: 0 }, ...points].map(({ d, a }, i) =>
		toField(
			losX,
			dir,
			slot.depth - d - (i === 0 ? 0 : deeper),
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
	empty,
	motion,
	shell,
}: {
	actors: FieldActor[];
	slots: Slot[];
	concept: PassConcept;
	geom: Geom;
	// How deep the quarterback set up, so the line can form a pocket in front
	// of him rather than standing on the ball.
	protectDepth: number;
	// Nobody stayed in to protect, so an assignment to block is not one.
	empty?: boolean;
	// Somebody goes in motion before the snap. It is the most recognisable
	// thing an offense does before the ball moves, and a formation that never
	// moves before the snap reads as a diagram rather than a play.
	motion?: boolean;
	// What the defense is showing, so the receivers can read it.
	shell?: CoverageShell;
}): FieldActor[] => {
	const mirror = dirAcross(geom.dir);
	// Which side the screen is going, so the wall is built on that side.
	const screenSide = concept.screen ? 1 : 0;

	// THE MESH POINT. Two men running the same crossing route at each other from
	// opposite sides meet in the middle of the field at exactly the same depth,
	// which drew them one on top of the other and is also the one thing the
	// route is coached NOT to do: on a mesh they pass one OVER and one UNDER,
	// close enough to rub off the men covering them and not close enough to run
	// into each other. So the second man to be given a crossing route runs it a
	// yard and a half deeper.
	const CROSSING = new Set<RouteName>(["drag", "slant"]);
	const crossers: { slot: number; depth: number }[] = [];
	for (const actor of actors) {
		const i = actor.slotIndex;
		if (i === undefined || i <= SLOT_RT) {
			continue;
		}
		const route = concept.routes[i];
		const slot = slots[i];
		if (route && slot && CROSSING.has(route)) {
			// How far downfield the route finishes for THIS man: the route's own
			// depth less how far behind the line he lines up.
			const last = ROUTES[route].at(-1);
			crossers.push({ slot: i, depth: (last?.d ?? 0) - slot.depth });
		}
	}
	// Only when there really are two of them. The man already running the
	// deeper of the two goes deeper still, so the gap always OPENS - pushing
	// the shallower one down instead would slide him past the other and put
	// both of them back on the same blade of grass.
	crossers.sort((a, b) => b.depth - a.depth);
	const goesOver = crossers.length >= 2 ? crossers[0]!.slot : undefined;

	return actors.map((actor) => {
		const i = actor.slotIndex;
		if (i === undefined) {
			return actor;
		}
		const slot = slots[i];
		if (!slot) {
			return actor;
		}
		const from = { x: actor.x, y: actor.y };

		// THE LINE.
		if (i <= SLOT_RT) {
			// A SCREEN is the one pass where the line does not stay: the interior
			// three sell the rush for a beat and then get out in front of the
			// catch. That wall forming downfield is the whole play, and without it
			// a screen is just a short throw.
			if (concept.screen && i <= SLOT_RG) {
				const lead = toField(
					geom.losX,
					geom.dir,
					-(3 + i * 2.5),
					geom.ballAcross + (8 + i * 3) * screenSide * mirror,
				);
				return {
					...actor,
					x: lead.x,
					y: lead.y,
					job: "release",
					path: [
						from,
						{ x: geom.losX, y: clampY((from.y + lead.y) / 2) },
						lead,
					],
					delay: 0.16,
				};
			}
			const spread = 1 + Math.abs(slot.across) * 0.14;
			const set = toField(
				geom.losX,
				geom.dir,
				slot.depth + Math.min(protectDepth - 1.5, 1.6 + spread * 0.5),
				geom.ballAcross + slot.across * mirror * 1.12,
			);
			return {
				...actor,
				x: set.x,
				y: set.y,
				job: "block",
				path: [from, set],
				delay: 0.02,
			};
		}

		// THE QUARTERBACK. On play action he rides the fake first, which is what
		// puts him deeper and later than an ordinary drop.
		if (i === SLOT_QB) {
			const drop = toField(
				geom.losX,
				geom.dir,
				protectDepth + (concept.playAction ? 1.5 : 0),
				geom.ballAcross + slot.across * mirror,
			);
			if (concept.playAction) {
				const ride = toField(
					geom.losX,
					geom.dir,
					protectDepth - 2.5,
					geom.ballAcross - 2 * mirror,
				);
				return { ...actor, x: drop.x, y: drop.y, path: [from, ride, drop] };
			}
			return { ...actor, x: drop.x, y: drop.y, path: [from, drop] };
		}

		const assigned = concept.routes[i];
		// In empty there is nobody to protect with, so an assignment to block is
		// not an assignment at all - the man is split out and has to run
		// something.
		const called: RouteName | undefined =
			assigned === "block" && empty ? "flat" : assigned;
		// And then he reads the coverage and changes it.
		const route = called === undefined ? undefined : adjustRoute(called, shell);

		// THE BACK ON PLAY ACTION: he takes the fake INTO the line and comes back
		// out, which is the half of the fake the defense actually reacts to.
		if (i === SLOT_RB && concept.playAction) {
			const mesh = toField(
				geom.losX,
				geom.dir,
				2,
				geom.ballAcross + 1.5 * mirror,
			);
			const out = toField(
				geom.losX,
				geom.dir,
				protectDepth - 0.5,
				geom.ballAcross - 5 * mirror,
			);
			return { ...actor, x: out.x, y: out.y, path: [from, mesh, out] };
		}

		if (!route || route === "block") {
			return actor;
		}
		const path = routePath({
			slot,
			route,
			...geom,
			deeper: i === goesOver ? 1.5 : 0,
		});
		if (path.length === 0) {
			return actor;
		}
		// THE MAN IN MOTION starts the play somewhere else and arrives at his
		// spot as the ball is snapped, so his path simply begins further across
		// the formation.
		const full =
			motion && i === SLOT_WR_SLOT
				? [
						toField(
							geom.losX,
							geom.dir,
							slot.depth,
							geom.ballAcross + (slot.across + 13) * mirror,
						),
						...path,
					]
				: path;
		const end = full.at(-1)!;
		return {
			...actor,
			x: end.x,
			y: end.y,
			job: "route",
			path: full,
			delay: concept.hold ?? 0,
		};
	});
};

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
}): FieldActor[] => {
	const mirror = dirAcross(geom.dir);
	const playSide = scheme.aim >= 0 ? 1 : -1;
	// WHO PULLS. Power sends the backside guard across the formation; counter
	// sends him and a tackle behind him; a sweep or a toss puts the play-side
	// guard out in front. Which man leaves the line is the clearest tell of what
	// a running play is, and a line where nobody ever leaves reads as one play
	// run over and over.
	const pullers = new Set<number>();
	if (scheme.pull === "backsideGuard" || scheme.pull === "guardAndTackle") {
		pullers.add(playSide > 0 ? SLOT_LG : SLOT_RG);
	}
	if (scheme.pull === "guardAndTackle") {
		pullers.add(playSide > 0 ? SLOT_LT : SLOT_RT);
	}
	if (scheme.pull === "playsideGuard") {
		pullers.add(playSide > 0 ? SLOT_RG : SLOT_LG);
	}

	return actors.map((actor) => {
		const i = actor.slotIndex;
		if (i === undefined || i > SLOT_RT) {
			return actor;
		}
		const slot = slots[i]!;
		const from = { x: actor.x, y: actor.y };

		if (pullers.has(i)) {
			// Out of his stance, back off the line, across the formation behind
			// everybody, and up through the hole.
			const behind = toField(
				geom.losX,
				geom.dir,
				slot.depth + 2.2,
				geom.ballAcross + slot.across * mirror,
			);
			const across = toField(
				geom.losX,
				geom.dir,
				slot.depth + 2,
				geom.ballAcross + scheme.aim * 0.6 * mirror,
			);
			const through = toField(
				geom.losX,
				geom.dir,
				-2.5,
				geom.ballAcross + (scheme.aim + 1.5 * playSide) * mirror,
			);
			return {
				...actor,
				x: through.x,
				y: through.y,
				job: "pull",
				path: [from, behind, across, through],
			};
		}

		// Everybody else steps toward the play - the whole line moving as one in
		// the direction of the aiming point is what zone blocking looks like.
		const drive = toField(
			geom.losX,
			geom.dir,
			slot.depth - 1.4,
			geom.ballAcross + (slot.across + scheme.aim * 0.22) * mirror,
		);
		return {
			...actor,
			x: drive.x,
			y: drive.y,
			job: "block",
			path: [from, drive],
			delay: 0.02,
		};
	});
};

// THE OTHER SIX. A running play is eleven men working, and only five of them
// are linemen - but the field was drawing a run as five linemen firing out
// while the quarterback, the second back, the tight end and both receivers
// stood perfectly still for the whole play. Five statues out of eleven is the
// single most lifeless thing a football graphic can do, and it is also a lie
// about the play: on a real run the receivers are the reason a six-yard gain
// becomes a twenty, and the quarterback's fake is the reason the backside
// defenders are late.
//
// So everybody who is not carrying the ball and not on the line gets the job he
// would really have:
//
//   the tight end   seals the edge on his side, or works back across on the
//                   backside, which is the block the hole actually depends on
//   the quarterback carries out the fake AWAY from the run, which is what
//                   holds the backside
//   the second back leads through the hole ahead of the carrier
//   the receivers   stalk - release at the man covering them, then square up
//                   and wall him off downfield; the backside one takes a flat
//                   cut-off angle instead, because he is never getting there
//                   straight
//
// The receivers' job is "stalk" rather than "block" on purpose: the trenches
// pair blockers with rushers by job, and a receiver twenty yards outside has no
// business being paired with a defensive end.
//
// WHICH JOB A MAN GETS COMES FROM HIS SLOT'S POSITION, NOT ITS NUMBER. The slot
// ORDER is fixed across formations, but what stands in a slot is not: slot 8 is
// a third receiver in shotgun, the FULLBACK in an I-formation and a second
// TIGHT END in a heavy set. Keying off the number sent an I-formation fullback
// on a twelve-yard stalk block down the seam instead of through the hole.
export const assignRunSupport = ({
	actors,
	slots,
	scheme,
	geom,
	carrierPid,
}: {
	actors: FieldActor[];
	slots: Slot[];
	scheme: RunScheme;
	geom: Geom;
	// The man with the ball. He runs the play, not a job, so he is left alone -
	// and must be, because a carrier labelled a blocker would be dragged into
	// the line pairing.
	carrierPid: number | undefined;
}): FieldActor[] => {
	const mirror = dirAcross(geom.dir);
	const playSide = scheme.aim >= 0 ? 1 : -1;

	const spot = (depth: number, across: number) =>
		toField(geom.losX, geom.dir, depth, geom.ballAcross + across * mirror);

	return actors.map((actor) => {
		const i = actor.slotIndex;
		if (
			i === undefined ||
			i <= SLOT_RT ||
			actor.role === "main" ||
			(carrierPid !== undefined && actor.pid === carrierPid)
		) {
			return actor;
		}
		const slot = slots[i]!;
		const from = { x: actor.x, y: actor.y };
		// Whether he is lined up on the side the ball is going. A man at the
		// midline counts as play side - he has the shorter trip either way.
		const onPlaySide = slot.across * playSide >= -1;

		// What he PLAYS, not which slot number he happens to occupy.
		const pos = slot.pos;
		// A man split out is a receiver for this play whatever his position
		// says - he is in no position to lead through anything. A tight end
		// lines up ATTACHED, just outside the tackle, so he is only "split" once
		// he is flexed well beyond that; a back out there is split at once.
		const wide = Math.abs(slot.across) > (pos === "TE" ? 14 : 8);

		if (pos === "TE" && !wide) {
			// Play side he seals the edge, a yard or two past the line and further
			// outside; backside he hinges and works back across the formation,
			// which is a block made at the line rather than beyond it.
			const to = onPlaySide
				? spot(-2, slot.across + 2.4 * playSide)
				: spot(-0.4, slot.across + 2.6 * playSide);
			return {
				...actor,
				x: to.x,
				y: to.y,
				job: "block",
				path: [from, to],
				delay: 0.02,
			};
		}

		if (pos === "QB") {
			// The fake: open away from the run, hide the empty hands, and keep
			// going for a few yards. Three points so it reads as a turn rather
			// than a slide.
			const open = spot(slot.depth + 1.4, slot.across - 2.2 * playSide);
			const boot = spot(slot.depth + 2.6, slot.across - 7 * playSide);
			return {
				...actor,
				x: boot.x,
				y: boot.y,
				job: "fake",
				path: [from, open, boot],
			};
		}

		if (pos === "RB" && !wide) {
			// A second back in the game is a lead blocker: through the hole ahead
			// of the carrier and a shade to the play side of it.
			const through = spot(-1.6, scheme.aim + 1 * playSide);
			return {
				...actor,
				x: through.x,
				y: through.y,
				job: "block",
				path: [from, through],
				delay: 0.03,
			};
		}

		// Receivers. Play side: release, then square up seven or eight yards
		// downfield with a little inside leverage, which is where the man
		// covering him has to be. Backside: a flat angle across, cutting off the
		// pursuit rather than chasing it.
		const stalkDepth = -7.4 - courtRandom() * 1.6;
		const release = onPlaySide
			? spot(-3, slot.across + 0.6 * playSide)
			: spot(-2, slot.across + 3.5 * playSide);
		const to = onPlaySide
			? spot(stalkDepth, slot.across + 1.8 * playSide)
			: spot(-3.6, slot.across + 9 * playSide);
		return {
			...actor,
			x: to.x,
			y: to.y,
			job: "stalk",
			path: [from, release, to],
			delay: 0.02,
		};
	});
};

// A RUN, FROM THE OTHER SIDE.
//
// Pursuit used to be a magnet with a fudge factor: every defender ran a
// straight line at the ball and was then nudged sideways by his slot number
// ("spread = ((i % 5) - 2) * 1.3"). Because the nudge came from the slot and
// not from the field, eleven men who started all over the place finished evenly
// spaced on one vertical line - a picket fence, which is the one shape a pile
// of tacklers never makes.
//
// What actually spreads a pursuit is that everybody arrives from somewhere
// different. So nobody is sent to the ball: each man is sent to a point a
// STANDOFF short of it ALONG HIS OWN LINE OF APPROACH. Converging from eleven
// directions and stopping short by different amounts puts a ring round the
// carrier for free, and the ring is his - the man who came from the sideline
// finishes outside him, the man who came from the middle finishes inside him.
//
// Three things then decide how close each man gets:
//
//   how deep he started   a lineman is on top of it, a safety is still closing
//   whether he was beaten  the ball getting past him downfield means he is
//                         chasing, and chasers finish behind the play
//   how wide he was       a corner keeps his leverage rather than running
//                         through the tackle
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
		if (actor.slotIndex === undefined) {
			return actor;
		}
		const start = { x: actor.x, y: actor.y };
		const toBallX = ballEnd.x - start.x;
		const toBallY = ballEnd.y - start.y;
		const dist = Math.hypot(toBallX, toBallY);
		if (dist < 0.4) {
			return actor;
		}
		const ux = toBallX / dist;
		const uy = toBallY / dist;

		// How far off the line he lined up, in yards - which is the honest
		// version of "front seven or secondary", and keeps working when the
		// front is a dime or a goal-line set.
		const depth = (start.x - geom.losX) * geom.dir;
		// The ball finished downfield of where he started: it went past him, and
		// he spends the play running it down from behind.
		const beaten = toBallX * geom.dir > 1.5;
		// How wide of the ball he started. A man from the sideline is taking an
		// angle, not a straight line.
		const width = Math.abs(start.y - ballEnd.y);

		// The gap he ends up leaving. A yard is contact; five yards is a man who
		// never got there.
		let standoff = 0.9 + depth * 0.1 + width * 0.06;
		if (beaten) {
			standoff += 1.6 + depth * 0.12;
		}
		standoff = Math.min(standoff, dist * 0.75);
		// Nobody arrives in a perfect circle.
		standoff = Math.max(0.6, standoff + (courtRandom() - 0.5) * 0.9);

		// AND HOW LONG THE PLAY LASTED. Sending every man to his standoff makes
		// a two-yard gain look like a forty: a corner on the far numbers would
		// cover twenty yards to arrive at a stuff in the backfield, because
		// nothing in the geometry knows the whistle went. A run's duration is
		// roughly its length, and everybody is running for that same duration -
		// so nobody travels further than the play was long, plus the few yards
		// of it that pass before the back is through the line.
		const gain = (ballEnd.x - geom.losX) * geom.dir;
		const reach = 6 + Math.max(0, gain) * 1.25;
		const travel = Math.min(Math.max(0, dist - standoff), reach);

		const end = {
			x: start.x + ux * travel,
			y: clampY(start.y + uy * travel),
		};

		// The middle of the run is where the angle shows. A defender does not
		// aim at the tackle, he aims at where he thinks it will be, so his path
		// bows toward his own side of the field before it closes.
		const bow = (start.y - ballEnd.y) * 0.18;
		return {
			...actor,
			x: end.x,
			y: end.y,
			path: [
				start,
				{
					x: start.x + (end.x - start.x) * 0.45,
					y: clampY(start.y + (end.y - start.y) * 0.5 + bow),
				},
				end,
			],
			// Depth is a head start for the offense: the deeper he was, the later
			// he is, and a man being run away from is later still.
			delay: 0.04 + Math.min(0.12, depth * 0.011) + (beaten ? 0.05 : 0),
		};
	});

// THE TRENCHES.
//
// Blockers and rushers were being sent to spots worked out independently of
// each other, so they slid through one another like two teams playing on
// different fields. Football's line play is PAIRS: a blocker finds the man
// across from him and the two of them fight over one piece of ground.
//
// So after both sides have their jobs, each blocker is paired with the nearest
// unclaimed rusher and the two are brought to a point BETWEEN them - the
// blocker giving a little ground, the rusher winning a little, which is what a
// pass rush looks like from above. A rusher nobody picks up is free, and gets
// to the quarterback: that is how a sack should read.
export const engageLine = ({
	blockers,
	rushers,
	// How far the rusher wins ON AVERAGE: 0 is a standstill, 1 is right through
	// him. Every pair is rolled around it, because a line where all five men
	// give exactly the same ground is a line nobody is fighting on.
	push = 0.42,
}: {
	blockers: FieldActor[];
	rushers: FieldActor[];
	push?: number;
}): { blockers: FieldActor[]; rushers: FieldActor[] } => {
	const meeting = new Map<number, FieldPoint>();
	const rusherMeeting = new Map<number, FieldPoint>();

	// Nearest first, so the men actually across from each other pair up rather
	// than the first blocker in the list grabbing somebody on the other side of
	// the formation.
	const pairs: { b: number; r: number; d: number }[] = [];
	for (const [bi, b] of blockers.entries()) {
		for (const [ri, r] of rushers.entries()) {
			pairs.push({ b: bi, r: ri, d: Math.hypot(b.x - r.x, b.y - r.y) });
		}
	}
	pairs.sort((a, b) => a.d - b.d);
	const usedB = new Set<number>();
	const usedR = new Set<number>();
	for (const pair of pairs) {
		if (usedB.has(pair.b) || usedR.has(pair.r) || pair.d > 9) {
			continue;
		}
		usedB.add(pair.b);
		usedR.add(pair.r);
		const b = blockers[pair.b]!;
		const r = rushers[pair.r]!;
		// Some of them hold and some of them get driven backwards.
		const won = Math.max(
			0.05,
			Math.min(0.95, push + (courtRandom() - 0.5) * 0.5),
		);
		const at = {
			x: b.x + (r.x - b.x) * won,
			y: b.y + (r.y - b.y) * won,
		};
		// THEY ARE NOT THE SAME MAN. Both of them were being sent to the meeting
		// point itself, so the two chips drew exactly on top of each other and
		// five one-on-one fights read as five lone players. Back each of them off
		// it by half a body along the axis they are fighting on: still locked up,
		// still visibly two men.
		const len = Math.hypot(r.x - b.x, r.y - b.y) || 1;
		const sep = { x: ((r.x - b.x) / len) * 0.5, y: ((r.y - b.y) / len) * 0.5 };
		meeting.set(pair.b, { x: at.x - sep.x, y: at.y - sep.y });
		rusherMeeting.set(pair.r, { x: at.x + sep.x, y: at.y + sep.y });
	}

	return {
		blockers: blockers.map((actor, i) => {
			const at = meeting.get(i);
			if (!at) {
				return actor;
			}
			const from = actor.path?.[0] ?? { x: actor.x, y: actor.y };
			return { ...actor, x: at.x, y: at.y, path: [from, at] };
		}),
		rushers: rushers.map((actor, i) => {
			// A rusher nobody blocked keeps the path he was given, and that path
			// goes to the quarterback.
			const at = rusherMeeting.get(i);
			if (!at) {
				return actor;
			}
			const from = actor.path?.[0] ?? { x: actor.x, y: actor.y };
			return { ...actor, x: at.x, y: at.y, path: [from, at] };
		}),
	};
};
