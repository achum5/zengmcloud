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
