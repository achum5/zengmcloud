import type { ReactNode } from "react";
import { courtRandom } from "./courtRng.ts";
import type { FormationKind } from "./formations.ts";
import type { BallFlight } from "./fieldAnimation.ts";

// Field geometry and the twenty-two positions a play is staged from, kept apart
// from the SVG so it can be reasoned about (and tested) on its own. LiveField
// owns the drawing; this owns where everybody stands.
//
// Unlike the basketball court, the football sim is generous with coordinates:
// every play carries the line of scrimmage and the yards gained, so the two
// points that matter - where the ball starts and where it ends up - are FACTS,
// not inventions. What has to be invented is the third dimension of a football
// play: which side of the hashes it happened on, how the twenty-two were
// aligned, and the path the carrier ran between those two facts. Those come
// from the seeded stream (courtRng), so two people watching the same broadcast
// see the same play.

// A regulation field in yards: 100 of playing field plus a 10-yard end zone at
// each end. x runs the LENGTH (0 = back of the left end zone, 10 = left goal
// line, 60 = midfield, 110 = right goal line, 120 = back of the right end
// zone). y runs ACROSS (0 = the near sideline, 53 1/3 = the far one).
export const FIELD_LEN = 120;
export const FIELD_W = 160 / 3;
export const ENDZONE = 10;
export const MID_Y = FIELD_W / 2;

// The room outside the lines the viewBox still draws: a ball that squirts out
// of bounds has somewhere to die, and the numbers have a margin to sit in.
export const SIDELINE = 3.4;

// NFL hash marks: 70'9" in from each sideline. They are the single detail that
// makes a green rectangle read as a football field rather than a lawn, and
// they're also where the ball is actually spotted, so plays line up on them.
export const HASH_NEAR = 70.75 / 3;
export const HASH_FAR = FIELD_W - HASH_NEAR;

// The yard numbers sit 12 yards in from each sideline (bottom of the number),
// and they're 2 yards tall.
export const NUMBER_Y_NEAR = 13;
export const NUMBER_Y_FAR = FIELD_W - NUMBER_Y_NEAR;

export type FieldPoint = { x: number; y: number };

// Which way the offense is moving: +1 is left-to-right. THE HOME TEAM (display
// team 1) ALWAYS ATTACKS RIGHT and the away team left - fixed for readability,
// and the same trade the basketball court makes (real teams change ends; a
// graphic that changes ends every quarter is unreadable).
//
// Home going left to right is the way a broadcast frames its own team, and it
// is what makes the rest of the picture make sense: the field is the home
// team's ground, so both end zones carry the home team's colours and name, and
// "driving right" is the home team driving.
export type Dir = 1 | -1;

export const dirFor = (t: 0 | 1): Dir => (t === 1 ? 1 : -1);

export const rand = (lo: number, hi: number) => lo + courtRandom() * (hi - lo);

// The sim's scrimmage is yards from the offense's OWN goal line (0-100). Turn
// that into a position on the drawn field.
export const fieldX = (scrimmage: number, dir: Dir): number =>
	dir === 1 ? ENDZONE + scrimmage : FIELD_LEN - ENDZONE - scrimmage;

// Nothing is ever drawn off the back of an end zone or past a sideline: a
// 60-yard gain from midfield ends in the end zone, not in the parking lot.
export const clampX = (x: number): number =>
	Math.min(FIELD_LEN - 0.6, Math.max(0.6, x));

export const clampY = (y: number): number =>
	Math.min(FIELD_W - 0.8, Math.max(0.8, y));

// A spot expressed the way a coach would say it - so many yards BEHIND the line
// of scrimmage (negative is across it, into the defense), so far ACROSS the
// field - placed on the drawn field for an offense moving `dir`.
export const toField = (
	losX: number,
	dir: Dir,
	depth: number,
	across: number,
): FieldPoint => ({
	x: clampX(losX - dir * depth),
	y: clampY(across),
});

// WHERE THE BALL IS SPOTTED. Between the hashes on most plays, on one of them
// when the last play died near a sideline. The offense lines up around it, so
// this one number decides whether the play looks like it's happening in the
// middle of the field or squeezed against a boundary.
export const snapAcross = (): number =>
	courtRandom() < 0.42
		? courtRandom() < 0.5
			? HASH_NEAR
			: HASH_FAR
		: rand(HASH_NEAR + 1, HASH_FAR - 1);

// A formation slot: where one player stands, and the position group he'd have
// to play to stand there. The group is what lets a real roster be sorted into
// the formation - the quarterback gets the quarterback's spot rather than
// whoever happens to be first in the list.
export type Slot = {
	depth: number;
	across: number;
	pos: string;
};

// Mirroring across as well as along keeps a formation's handedness: the tight
// end lines up on the same physical side of the field whichever way the offense
// is moving, so the two teams' sets are reflections of each other rather than
// copies.
export const dirAcross = (dir: Dir): number => (dir === 1 ? 1 : -1);

// Place a formation on the field: slots are written around a ball spotted in
// the middle, so they slide across to wherever it actually is, and any receiver
// who would end up outside the numbers is pulled back inbounds (a ball on the
// far hash would otherwise put a split end in the stands).
export const placeFormation = (
	slots: Slot[],
	losX: number,
	dir: Dir,
	ballAcross: number,
): FieldPoint[] =>
	slots.map((slot) =>
		toField(losX, dir, slot.depth, ballAcross + slot.across * dirAcross(dir)),
	);

// WHERE THE PLAY ENDS UP. The sim gives the yards, so this is only ever a
// question of how far off the middle of the field the carrier was when he was
// brought down - a run to the boundary and a run up the gut both gain four.
export const synthEndPoint = (
	losX: number,
	dir: Dir,
	yards: number,
	ballAcross: number,
	spread: number,
): FieldPoint => toField(losX, dir, -yards, ballAcross + rand(-spread, spread));

// THE PATH BETWEEN THOSE TWO FACTS. A carrier does not travel in a straight
// line - he presses one way and cuts back - and a graphic that moves him along
// the hypotenuse looks like a chess piece sliding. Two control points, both
// pulled off the straight line in the same direction he eventually breaks, make
// the run read as a run. Returned as a cubic Bezier's control points.
export const runControlPoints = (
	from: FieldPoint,
	to: FieldPoint,
): [FieldPoint, FieldPoint] => {
	const dx = to.x - from.x;
	const dy = to.y - from.y;
	// How much room there is to weave: a two-yard plunge should barely wiggle, a
	// forty-yard run can cross the field twice.
	const sway = Math.min(7, Math.abs(dx) * 0.3);
	const bias = dy >= 0 ? 1 : -1;
	return [
		{
			x: from.x + dx * 0.3,
			y: clampY(from.y + dy * 0.15 - bias * sway * rand(0.3, 1)),
		},
		{
			x: from.x + dx * 0.68,
			y: clampY(from.y + dy * 0.7 + bias * sway * rand(0.2, 0.9)),
		},
	];
};

// A point along that cubic, so the ball and the carrier travel the same curve.
export const bezierAt = (
	p0: FieldPoint,
	p1: FieldPoint,
	p2: FieldPoint,
	p3: FieldPoint,
	t: number,
): FieldPoint => {
	const u = 1 - t;
	const a = u * u * u;
	const b = 3 * u * u * t;
	const c = 3 * u * t * t;
	const d = t * t * t;
	return {
		x: a * p0.x + b * p1.x + c * p2.x + d * p3.x,
		y: a * p0.y + b * p1.y + c * p2.y + d * p3.y,
	};
};

// A LOOSE BALL. An incompletion dies a yard or two past the receiver, a fumble
// bounces somewhere nobody meant it to go - both need a spot that is clearly
// NOT a player's spot, or the play reads as completed.
export const synthLooseBall = (
	near: FieldPoint,
	dir: Dir,
	scatter: number,
): FieldPoint => ({
	x: clampX(near.x + dir * rand(0.5, scatter)),
	y: clampY(near.y + rand(-scatter, scatter)),
});

// The uprights: 10 yards behind the goal line (the back of the end zone) and
// 18'6" apart, centered. A kick that goes through has to be drawn going through
// something.
export const UPRIGHT_HALF_W = 18.5 / 6;

export const goalpostX = (dir: Dir): number =>
	dir === 1 ? FIELD_LEN - ENDZONE : ENDZONE;

// ============================================================================
// WHAT A SCENE IS, and who is standing where in it.
//
// These live beside the geometry rather than in LiveField.tsx because that
// module reaches the worker through its face rendering: importing it from a
// test spins up a Worker and fails before a single assertion runs. Everything
// here is plain data and arithmetic, so the scene builder - and its tests -
// can use it without a browser.
// ============================================================================

export type FieldActor = {
	pid: number;
	name: string;
	// Where he ENDS the play. A pathed actor walks his path to get here; an
	// unpathed one simply glides.
	x: number;
	y: number;
	// The job he was given, so the play can be watched rather than inferred:
	// waypoints in field coordinates, starting where he lined up. `delay` is how
	// much of the play passes before he moves (a ride, a draw's pause, a
	// blocker's first step), as a fraction of the scene.
	path?: FieldPoint[];
	delay?: number;
	// Which formation slot he filled, so a concept can give the tight end the
	// tight end's route.
	slotIndex?: number;
	// What he was actually asked to do. Set by the assignment functions and read
	// by anything that needs to pair men up - the line engaging, most of all.
	job?:
		| "block"
		| "pull"
		| "release"
		| "route"
		| "rush"
		| "man"
		| "zone"
		| "spy"
		// A receiver's run block, out in space rather than in the trenches - so
		// the line pairing never mistakes him for a man in it.
		| "stalk"
		// Carrying out a fake with nothing in his hands.
		| "fake";
	// The position he plays, which is most of where his speed comes from.
	pos?: string;
	// And his skill badges, which are the rest of it - the one genuinely rare
	// athlete on the field should pull away from the men covering him.
	skills?: string[];
	// "main" is the man with the ball, "passer" whoever threw or kicked it,
	// "defender" whoever did something about it, "onField" everybody else.
	role: "main" | "defender" | "passer" | "onField";
	// Display team (0 = away/attacks left, 1 = home/attacks right). Set on
	// background players so they're colored by their own team.
	t?: 0 | 1;
};

export type FieldSceneKind =
	// A textless beat: the twenty-two break the huddle and line up. This is what
	// stops a drive from teleporting - the set gets its own moment before the
	// snap, exactly like the court's "advance".
	| "set"
	| "run"
	| "pass"
	| "incomplete"
	| "sack"
	| "kick"
	| "punt"
	| "kickoff"
	| "return"
	| "fumble"
	| "interception"
	| "penalty"
	| "injury"
	// Play stopped: a timeout, the end of a quarter, the final whistle.
	| "dead"
	| "other";

export type FieldScene = {
	key: number; // increments per scene, retriggers animations
	// Deterministic seed for this play, so the ball's jitter is identical on
	// every device and on every replay rather than re-rolled per viewer.
	seed?: string;
	kind: FieldSceneKind;
	t: 0 | 1; // display team on offense
	dir: Dir;
	losX: number;
	// Where a first down would be, when there is one to draw (never on a kickoff
	// or an extra point).
	firstDownX?: number;
	actors: FieldActor[];
	text: ReactNode;
	// The running score line, shown under the play text when a play scores.
	score?: ReactNode;
	// Down and distance, drawn in the corner the way a scorebug does it.
	down?: string;
	ball?: {
		flight: BallFlight;
		from: FieldPoint;
		to: FieldPoint;
		// A carrier's weave, as a cubic's two control points.
		curve?: [FieldPoint, FieldPoint];
		// A ball being CARRIED travels the carrier's own path, not a curve of
		// its own - otherwise the two drift apart over a long run and the ball
		// ends up alongside the man supposedly holding it.
		path?: FieldPoint[];
	};
	impact?: { kind: "tackle" | "score"; at: FieldPoint };
	// Where a flag landed. A penalty that says so only in the text is the one
	// thing on a football field everybody spots before they hear about it.
	flag?: FieldPoint;
	// Where the drive's earlier plays ended, drawn as faint marks on the field so
	// the drive's shape is visible without a separate chart.
	driveMarks?: number[];
	// "Drive: 4 plays, 31 yards", shown in the corner opposite the down.
	drive?: string;
	// The play that was called - "Shotgun · Four Verticals" - so a viewer can
	// see WHAT he is watching and not only what happened.
	playName?: string;
	// And what the defense answered with: "Nickel · Cover 3".
	defenseName?: string;
};

export type FieldTeam = {
	tid: number;
	abbrev?: string;
	region?: string;
	name?: string;
	colors?: [string, string, string];
	imgURL?: string;
	imgURLSmall?: string;
};

// A player on the field, ready to be placed into a formation slot. Passed in
// from the live game view, which is the only place that knows the rosters.
export type FieldPlayer = {
	pid: number;
	name: string;
	pos?: string;
	// His skill badges, straight off the box score. The field reads them for
	// one thing only: how fast he moves (see speedFor).
	skills?: string[];
};

// THE TWENTY-TWO WHO AREN'T IN THE PLAY. The play's actors are placed by the
// scene; everybody else fills the formation around them, so the field always
// shows two teams rather than the three men the sim happened to name.
//
// Slot assignment is by position group first (the quarterback gets the
// quarterback's spot) and stable within a group, so a lineman keeps his slot
// from play to play and the line doesn't shuffle itself every snap.
export const buildFormationActors = ({
	players,
	slots,
	losX,
	dir,
	ballAcross,
	t,
	skipPids,
	preferPids,
}: {
	players: FieldPlayer[];
	slots: Slot[];
	losX: number;
	dir: Dir;
	ballAcross: number;
	t: 0 | 1;
	skipPids: Set<number>;
	// The men the play named. They go to the FRONT of the pool so each wins the
	// slot his position plays - which is what lets the scene find him in the
	// formation afterwards and give him his face, his route and his end point.
	// Without it a featured receiver loses the receiver slots to his backups and
	// has to be bolted on beside the eleven, which is how a team ends up with
	// twelve men on the field.
	preferPids?: Set<number>;
}): FieldActor[] => {
	const points = placeFormation(slots, losX, dir, ballAcross);
	const first = preferPids ?? skipPids;
	const pool = [...players].sort(
		(a, b) => (first.has(b.pid) ? 1 : 0) - (first.has(a.pid) ? 1 : 0),
	);
	const actors: FieldActor[] = [];
	for (const [i, slot] of slots.entries()) {
		// Prefer a player who actually plays this slot's position; fall back to
		// whoever is left, so a team missing a tight end still fields eleven.
		let idx = pool.findIndex((p) => p.pos === slot.pos);
		if (idx === -1) {
			idx = pool.findIndex((p) => POS_GROUP[p.pos ?? ""] === slot.pos);
		}
		if (idx === -1) {
			idx = 0;
		}
		const p = pool[idx];
		if (!p) {
			break;
		}
		pool.splice(idx, 1);
		if (skipPids.has(p.pid)) {
			continue;
		}
		const point = points[i]!;
		actors.push({
			pid: p.pid,
			name: p.name,
			x: point.x,
			y: point.y,
			role: "onField",
			slotIndex: i,
			pos: p.pos,
			skills: p.skills,
			t,
		});
	}

	// A named man can still miss out: a wide receiver returning a kick has no
	// slot to win in a formation written entirely of backs, so the position
	// match above hands every one of them to a real back. He TAKES a slot
	// instead of being added beside the eleven - which is the difference
	// between a man being shown where the play put him and a team having
	// twelve men on the field.
	const placed = new Set(actors.map((a) => a.pid));
	for (const pid of preferPids ?? []) {
		if (placed.has(pid) || actors.length === 0) {
			continue;
		}
		const p = players.find((x) => x.pid === pid);
		if (!p) {
			continue;
		}
		const group = POS_GROUP[p.pos ?? ""] ?? p.pos;
		const at =
			actors.findLastIndex((a) => {
				const slot = slots[a.slotIndex ?? -1];
				return slot !== undefined && slot.pos === group;
			}) ?? -1;
		const i = at >= 0 ? at : actors.length - 1;
		actors[i] = {
			...actors[i]!,
			pid: p.pid,
			name: p.name,
			pos: p.pos,
			skills: p.skills,
		};
		placed.add(pid);
	}
	return actors;
};

// Football positions collapsed into the groups the formations are written in.
const POS_GROUP: Record<string, string> = {
	QB: "QB",
	RB: "RB",
	FB: "RB",
	WR: "WR",
	TE: "TE",
	OL: "OL",
	C: "OL",
	G: "OL",
	T: "OL",
	DL: "DL",
	DE: "DL",
	DT: "DL",
	LB: "LB",
	CB: "CB",
	S: "S",
	K: "K",
	P: "P",
	KR: "RB",
	PR: "RB",
};

export const formationFor = (kind: FieldSceneKind): FormationKind => {
	switch (kind) {
		case "run":
		case "fumble":
			return "run";
		case "punt":
			return "punt";
		case "kick":
			return "kick";
		case "kickoff":
			return "kickoff";
		case "return":
			return "kickoffReturn";
		default:
			return "pass";
	}
};
