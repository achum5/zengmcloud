// WATCHING SOMEONE ELSE'S SIM, SMOOTHLY.
//
// A device following a league-mate's live game has the whole game already
// (the sim finished before playback started); what it learns from the device
// in charge of simming is only how far along it is, a report every few
// hundred milliseconds that arrives late and unevenly. Running to each
// reported play and stopping there played the game in bursts - a stop at
// every line while the next report was on its way, then a sprint to catch up.
//
// So the follower keeps its own estimate of where the other court is - it
// runs on at the same speed between reports, and never past the play the
// other device hasn't reached yet - and plays a little behind that estimate,
// easing its speed up or down to hold the gap instead of stopping dead.

// How far behind the other court to play (ms of game timeline): enough that
// a late report doesn't run the picture into a wall.
export const FOLLOW_BEHIND = 800;

// The other court's position, run on by dt (ms) at its rate, but never past
// the next play it hasn't shown yet.
export const advanceLead = (
	lead: number,
	target: number,
	dt: number,
	rate: number,
): number => Math.min(target, lead + dt * rate);

// How much faster or slower than the other court to run, from how far
// behind it this one is: steady at FOLLOW_BEHIND, easing toward it.
export const followRate = (lag: number): number =>
	Math.min(2.5, Math.max(0.5, 1 + (lag - FOLLOW_BEHIND) / 1500));
