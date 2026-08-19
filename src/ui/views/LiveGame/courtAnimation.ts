// THE BALL AND THE IRON: the two pure rules behind the live court's animation.
//
// Kept out of LiveCourt.tsx on purpose - that module reaches the worker through
// its face rendering, so importing it from a test spins up a Worker and fails
// before a single assertion runs. These are plain arithmetic over numbers, so
// they belong beside courtSpots.ts and courtRng.ts, where the geometry already
// lives and can be reasoned about without a browser.

// HOW THE BALL TURNS. Spin is accumulated from distance travelled rather than
// passed in per animation, so a dribble, a pass, a shot and a ball skipping out
// of bounds all get it, all at the same rate - which is what makes it read as
// one physical object instead of an effect applied to some plays. Direction of
// travel decides which way, so a ball coming back the other way does not keep
// turning the same way.
//
// Degrees per foot is tuned to what a basketball does, not to a rolling ball: a
// jump shot carries 2-3 revolutions a second at ~35 ft/s, which is about 25
// degrees a foot. A true roll rate (~95) works out to nine revolutions a second
// - at 60fps that is 57 degrees a frame, fast enough that the seams alias into a
// flicker and the ball reads as vibrating.
export const SPIN_DEG_PER_FT = 26;

export const nextSpinDeg = (
	deg: number,
	from: { x: number; y: number },
	to: { x: number; y: number },
): number => {
	const dist = Math.hypot(to.x - from.x, to.y - from.y);
	const dir = to.x >= from.x ? 1 : -1;
	return (deg + dir * dist * SPIN_DEG_PER_FT) % 360;
};

// WHAT THE IRON DOES ABOUT IT.
//
// A make and a miss used to look nearly the same on the floor - the ball faded
// either way and a ring pulsed. The rim is the one thing on a basketball court
// that KNOWS what happened, so it answers: a make FLARES (the rings kick
// outward, bright, and settle), a miss RATTLES sideways and stays dim. Seen
// from overhead that is the honest reading of both - you cannot watch a net
// billow from the ceiling, but you can see the rim jump.
//
// The two must never be confusable at a glance, which is what the tests pin:
// only a make ever scales up, and a miss is always the dimmer of the two.
export const rimReaction = (
	made: boolean,
	p: number,
): { opacity: number; scale: number; dx: number; dy: number } => {
	if (!(p >= 0) || p >= 1) {
		return { opacity: 0, scale: 1, dx: 0, dy: 0 };
	}
	if (made) {
		// A snap: hard flare that overshoots once, then settles.
		const snap = Math.sin(Math.PI * Math.min(1, p * 1.35));
		const wobble = Math.sin(p * Math.PI * 3) * (1 - p) * 0.16;
		return {
			// Fades to nothing rather than to 0.14 and then cutting - a linear
			// ramp left the flare popping off at the end of the beat. The
			// fractional exponent keeps it bright through the early frames, where
			// the flare is doing its job, and still lands on zero.
			opacity: 0.95 * (1 - p) ** 0.8,
			scale: 1 + snap * 0.5 + wobble,
			dx: 0,
			dy: 0,
		};
	}
	// A clang. The squared damping is what makes it read as iron rather than a
	// wobble: most of the movement is over in the first third.
	const damp = (1 - p) * (1 - p);
	const shake = Math.sin(p * Math.PI * 9) * damp * 1.4;
	return {
		opacity: 0.5 * damp,
		scale: 0.92,
		dx: shake,
		dy: shake * 0.35,
	};
};
