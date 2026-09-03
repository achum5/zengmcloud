import { courtRandom } from "./courtRng.ts";

// Court geometry and the invented spots plays happen at, kept apart from the
// SVG so it can be reasoned about (and tested) on its own. LiveCourt owns the
// drawing; this owns where things are.
//
// The sim knows only coarse shot zones, never coordinates, so every position
// here is synthesized - but synthesized from a seeded stream (courtRng), which
// is what makes two people watching the same broadcast see the same floor.

// Full court in feet: 94 x 50. The rendered viewBox extends past it - rails
// behind each baseline, an apron outside each sideline - which is the room a
// ball has to travel into when it goes out of bounds.
export const COURT_W = 94;
export const COURT_H = 50;
export const APRON = 2.5;

export const degToRad = (deg: number) => (deg * Math.PI) / 180;

export const rand = (lo: number, hi: number) => lo + courtRandom() * (hi - lo);

// Convert a spot expressed as (depth from the attacked baseline, position
// across the court) into full-court coordinates for team t. Away attacks the
// LEFT rim, home the RIGHT.
export const toCourt = (t: 0 | 1, depth: number, across: number) => ({
	x: t === 0 ? depth : COURT_W - depth,
	y: across,
});

// The widest window in which a three can be staged as a heave. Past this there
// is time for a real shot, so it gets a real shot's spot no matter what the sim
// flagged - the sim's "desperation" also covers a forced three with ten seconds
// left, which is a normal look, not a launch.
export const HEAVE_MAX_SECONDS = 1.5;

// A last-second heave. How far out it comes from is set by how much clock is
// left, because that is what decides it in real life: with a second and a half
// a player can catch, turn, and get to the logo; with two tenths he throws it
// from wherever the inbound reached him. So 1.5s puts him a few steps behind
// the arc and a tenth of a second puts him past half court.
//
// This ramp matters for more than looks. The court used to draw EVERY three
// launched inside 1.5s from beyond half court, so ordinary end-of-quarter
// threes - which go in at an ordinary three's rate - were being staged as
// full-court heaves. Half-court heaves appeared to drop constantly because
// most of them were never heaves at all.
export const synthHeaveSpot = (
	t: 0 | 1,
	secondsLeft: number,
): { x: number; y: number } => {
	// 0 = no time at all, 1 = the full window.
	const room = Math.min(1, Math.max(0, secondsLeft / HEAVE_MAX_SECONDS));
	const near = 46 - 19 * room; // 46ft out -> 27ft (a deep three)
	const far = 58 - 25 * room; // 58ft (past half court) -> 33ft
	// From half court the launch can come from anywhere across the floor; from
	// thirty feet he is much more likely to be somewhere in front of the rim.
	const spread = 9 + 5 * (1 - room);
	return toCourt(t, rand(near, far), 25 + rand(-spread, spread));
};

// A ball knocked out of bounds: it leaves a spot in the offense's frontcourt
// and crosses the NEARER sideline, dying a couple of feet past the line. A path
// rather than a point because the travel IS the play - where the ball ends up
// is the only thing that reads as "out of bounds".
export const synthOutOfBoundsPath = (
	t: 0 | 1,
): { from: { x: number; y: number }; to: { x: number; y: number } } => {
	const from = toCourt(t, rand(8, 40), rand(6, 44));
	const towardTop = from.y < COURT_H / 2;
	const to = {
		// It carries a few feet up or down the floor on the way out.
		x: Math.min(COURT_W - 2, Math.max(2, from.x + rand(-9, 9))),
		// Just past the line, but inside the apron the viewBox actually draws.
		y: towardTop ? -APRON + rand(0.4, 1.4) : COURT_H + APRON - rand(0.4, 1.4),
	};
	return { from, to };
};

// The rim each display team attacks (away left, home right). Kept here rather
// than with the SVG because every formation below is measured from it.
export const RIM_INSET = 5.25;

export const rimXFor = (t: 0 | 1): number =>
	t === 0 ? RIM_INSET : COURT_W - RIM_INSET;

// Deterministic half-court formation slots (depth from the attacked baseline,
// across the width), ordered guard→wing→big. Both clusters live in the
// offense's frontcourt - the end the ball is at - so all ten faces read as one
// 5-on-5 set. Slot assignment is stable (full lineup sorted by position, THEN
// play actors skipped) so a player keeps his slot from scene to scene and just
// glides; the defense man-marks by shadowing its counterpart's slot a step
// closer to the rim.
export const OFFENSE_SPOTS: { depth: number; across: number }[] = [
	{ depth: 28, across: 25 }, // point, top of the key
	{ depth: 22, across: 42 }, // wing
	{ depth: 22, across: 8 }, // wing
	{ depth: 10, across: 39 }, // corner / short corner
	{ depth: 7, across: 20 }, // low block
];

// THE OFFENSE DOES NOT STAND STILL WHILE THE BALL GOES AROUND.
//
// A possession used to be one frozen set: the five arrived at OFFENSE_SPOTS and
// held their ground until the shot went up, however long the sim said the
// possession lasted. Real basketball spends those seconds moving - the ball is
// reversed, the weak side lifts, a man cuts through, the big flashes high - and
// on a top-down graphic that movement IS the difference between a diagram and a
// game.
//
// So a possession advances through MOTION PHASES, one per swing of the ball
// (see possessionBeats): phase 0 is the set the offense walks into, and each
// later phase is where those same five have moved to by the next reversal.
// Slot order is preserved (0 = the guard ... 4 = the big), so nobody ends up
// somewhere his position would never be - the point does not post up and the
// center does not stand in the corner.
//
// The defense needs no phases of its own: it is placed by shadowing whatever
// slot its man occupies, so it follows the motion for free.
export const MOTION_OFFENSE_SPOTS: { depth: number; across: number }[][] = [
	OFFENSE_SPOTS,
	// The ball has been reversed once: the point gave it up and slid to the
	// wing, the weak-side wing lifted to the top to receive, the other wing
	// dropped to the corner, and the big flashed up to the elbow.
	[
		{ depth: 24, across: 36 },
		{ depth: 26, across: 16 },
		{ depth: 14, across: 6 },
		{ depth: 9, across: 43 },
		{ depth: 14, across: 26 },
	],
	// Late clock: the ball has come all the way back the other way, the big has
	// stepped out to set a screen, and the corner man has cut to the rim.
	[
		{ depth: 21, across: 20 },
		{ depth: 12, across: 44 },
		{ depth: 25, across: 34 },
		{ depth: 6, across: 29 },
		{ depth: 23, across: 24 },
	],
];

// Which slot has the ball in each motion phase - the man the previous swing
// found. Phase 0 is the point who brought it up; after that the ball is with
// whoever lifted into it, which is what makes the reversal cross the floor
// instead of rattling between two spots.
export const MOTION_HANDLER_SLOT = [0, 1, 2];

// A FAST-BREAK set: the offense is strung out toward the rim in wide lanes -
// a lead attacker at the basket, both wings filling the lanes, a trailer, and a
// deep safety - instead of a settled half-court spread. Ordered guard→big (see
// posRank), so the guard leads the break and the big trails, the way it runs in
// real life. Used for a possession that starts off a steal or a defensive board
// (see buildLineupActors' `transition`), which is what makes a turnover into a
// coast-to-coast break rather than another static set popping onto the floor.
export const TRANSITION_OFFENSE_SPOTS: { depth: number; across: number }[] = [
	{ depth: 6, across: 25 }, // lead attacker, at the rim
	{ depth: 13, across: 6 }, // left lane runner
	{ depth: 13, across: 44 }, // right lane runner
	{ depth: 23, across: 31 }, // trailer
	{ depth: 31, across: 19 }, // deep safety / late trailer
];

// Where the ball sits in a given motion phase: in the hands of that phase's
// handler.
export const motionHandlerSpot = (
	offenseT: 0 | 1,
	motion: number,
): { x: number; y: number } => {
	const phase =
		MOTION_OFFENSE_SPOTS[
			Math.min(MOTION_OFFENSE_SPOTS.length - 1, Math.max(0, motion))
		]!;
	const slot =
		MOTION_HANDLER_SLOT[
			Math.min(MOTION_HANDLER_SLOT.length - 1, Math.max(0, motion))
		]!;
	const spot = phase[slot]!;
	return toCourt(offenseT, spot.depth, spot.across);
};

// The ball's path for a setup beat: brought up from the FAR end (where the
// possession was just won) to the ball-handler's spot in the set the team is
// about to run - the lead attacker on a break, the top of the key in a
// half-court set. Matches OFFENSE_SPOTS[0] / TRANSITION_OFFENSE_SPOTS[0] so the
// ball settles right where a real handler stands.
//
// When the possession never changed ends (an offensive rebound
// resetting the offense), it starts at THIS team's own rim instead, because the
// ball never went anywhere near the other one.
export const setupBallPath = (
	offenseT: 0 | 1,
	transition: boolean,
	sameEnd = false,
): { ballFrom: { x: number; y: number }; ballTo: { x: number; y: number } } => {
	const handler = transition ? TRANSITION_OFFENSE_SPOTS[0]! : OFFENSE_SPOTS[0]!;
	return {
		ballFrom: sameEnd
			? toCourt(offenseT, 8, 25)
			: { x: rimXFor(offenseT === 0 ? 1 : 0), y: 25 },
		ballTo: toCourt(offenseT, handler.depth, handler.across),
	};
};

// HOW LONG THE POSSESSION TOOK, AND HOW MANY BEATS THAT IS WORTH.
//
// The sim never says "this possession lasted eleven seconds", but it says it
// anyway: every play-by-play event carries the game clock, so the clock burned
// between the last play and the shot IS the possession. Measured over ten
// engine-simmed games that runs from about 2 seconds to 24, mean 12, with the
// middle half between 9 and 15 - a fourfold spread the court used to throw away
// entirely, since a half-court possession got no beat at all (see the gate in
// processToNextPause: only a fast break ever reached one).
//
// So: a quick hitter gets one beat, an ordinary possession two, a grind three -
// 20/54/26 percent of them over that same measurement. A putback follows its
// own miss by a fraction of a second and gets none, because nobody brought the
// ball anywhere. A fast break is one push by definition, however long the clock
// says.
export const possessionBeats = (
	elapsedSeconds: number | undefined,
	transition: boolean,
): number => {
	if (elapsedSeconds === undefined || !Number.isFinite(elapsedSeconds)) {
		return 1;
	}
	if (elapsedSeconds < 2) {
		return 0;
	}
	if (transition) {
		return 1;
	}
	if (elapsedSeconds < 8) {
		return 1;
	}
	if (elapsedSeconds < 15) {
		return 2;
	}
	return 3;
};

// ...WITHOUT CHARGING THREE BEATS FOR IT.
//
// Three full beats a possession would stretch a live game by half, which is a
// worse experience, not a better one. So the beats of ONE possession share a
// single budget: roughly four tenths of an ordinary beat for a quick hitter,
// about one and a quarter for a grind, averaging a bit over three quarters.
// Reversing the ball twice therefore costs barely more than bringing it up
// once, and the extra realism is paid for in movement rather than in the
// viewer's time.
//
// It is not free, though, and the comment should not pretend otherwise: a
// possession that used to be shown as nothing at all now gets a beat, which
// works out at about 0.8 of a beat per possession - a live game at the default
// speed goes from roughly 11.7 minutes to 14.5. That IS the feature (the
// possession is being played instead of skipped) and the speed control is right
// there, but it is a real cost and worth knowing before widening the budget.
//
// Floored so a beat is never so short that the glide it triggers cannot finish.
export const setupBeatMs = (
	sceneMs: number,
	elapsedSeconds: number | undefined,
	beats: number,
): number => {
	if (beats <= 0) {
		return sceneMs;
	}
	const elapsed =
		elapsedSeconds !== undefined && Number.isFinite(elapsedSeconds)
			? elapsedSeconds
			: 12;
	const budget = sceneMs * Math.min(1.25, Math.max(0.45, elapsed / 15));
	return Math.max(240, budget / beats);
};

// Both benches sit on the SAME sideline in a real arena, either side of the
// scorer's table: away to the left of center, home to the right. On a stoppage
// the five on the floor collapse into a horseshoe in front of their own bench,
// facing the coach - which is what a timeout looks like from above.
//
// Kept a few feet in from the sideline rather than right on it, so a face
// centered on one of these spots stays on the floor.
export const benchHuddle = (
	t: 0 | 1,
	n: number,
): { x: number; y: number }[] => {
	const cx = t === 0 ? COURT_W * 0.29 : COURT_W * 0.71;
	const cy = 9;
	const out: { x: number; y: number }[] = [];
	for (let i = 0; i < n; i++) {
		// An arc opening back toward the floor, so nobody stands on the coach.
		const ang = degToRad(20 + (i * 140) / Math.max(1, n - 1));
		out.push({
			x: cx - 6.2 * Math.cos(ang),
			y: cy + 4.6 * Math.sin(ang),
		});
	}
	return out;
};
