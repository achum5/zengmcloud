// THE BALL IN THE AIR, and what the ground does about it: the pure rules behind
// the live field's animation.
//
// Kept out of LiveField.tsx for the same reason courtAnimation.ts is kept out
// of LiveCourt.tsx - that module reaches the worker through its face rendering,
// so importing it from a test spins up a Worker and fails before a single
// assertion runs. These are plain arithmetic over numbers.

export type BallFlight =
	| "snap"
	| "pitch"
	| "pass"
	| "punt"
	| "kick"
	| "loose"
	// Tucked under an arm: it travels with a man, so it never leaves the ground.
	| "carry";

// HOW HIGH IT GETS. A football graphic drawn from overhead has no vertical
// axis, so height has to be spent on the two things a camera directly above
// would actually see: the ball getting BIGGER as it rises, and its shadow
// sliding out from under it. Both are driven from one number, so they can never
// disagree.
//
// The peaks are in yards and are what each kind of kick or throw really does. A
// punt is the extreme - a good one hangs forty-five feet up, which is why it
// looks so different from a thrown ball travelling the same distance.
const PEAK: Record<BallFlight, (dist: number) => number> = {
	snap: () => 0.7,
	pitch: () => 1.2,
	pass: (dist) => Math.min(7, 1.4 + dist * 0.11),
	punt: (dist) => Math.min(17, 9 + dist * 0.16),
	kick: (dist) => Math.min(14, 6 + dist * 0.13),
	loose: () => 1.6,
	carry: () => 0,
};

// A parabola, which is what a ball in the air is. Clamped at the ends so a
// flight that is still being interpolated never reports a negative height.
export const ballHeight = (
	flight: BallFlight,
	dist: number,
	p: number,
): number => {
	if (!(p > 0) || p >= 1) {
		return 0;
	}
	return PEAK[flight](dist) * 4 * p * (1 - p);
};

// The ball itself grows a little as it climbs (it is nearer the camera) and its
// shadow shrinks and fades - the two cues that read as altitude from above. A
// yard of height is worth much less scale than it is shadow separation, which
// is why they use different divisors.
export const ballLift = (
	height: number,
): { scale: number; shadowScale: number; shadowOpacity: number } => ({
	// Gentle: a punt already peaks at seventeen yards, and anything steeper than
	// this drew a football bigger than the men throwing it.
	scale: 1 + Math.min(0.5, height * 0.042),
	shadowScale: Math.max(0.35, 1 - height * 0.045),
	shadowOpacity: Math.max(0.12, 0.4 - height * 0.022),
});

// HOW IT TURNS. A football is the one ball whose rotation tells you what
// happened to it: a thrown ball SPIRALS, so from above its nose simply points
// where it's going and it never appears to tumble; a kicked ball turns
// END OVER END, which from above is the long axis sweeping round; a loose ball
// tumbles fast and unpredictably, which is exactly why it's frightening.
//
// Returned in degrees for the SVG transform. `travelDeg` is the direction of
// travel, which a spiral locks to.
export const ballAngle = (
	flight: BallFlight,
	travelDeg: number,
	spinDeg: number,
): number =>
	flight === "pass" || flight === "snap" || flight === "carry"
		? travelDeg
		: spinDeg;

// Degrees a tumbling ball turns per yard travelled. A punt turns about three
// times over forty yards, which is 27 degrees a yard - fast enough to read,
// slow enough not to strobe at 60fps.
export const TUMBLE_DEG_PER_YD = 27;

export const nextTumble = (
	deg: number,
	from: { x: number; y: number },
	to: { x: number; y: number },
	flight: BallFlight,
): number => {
	const dist = Math.hypot(to.x - from.x, to.y - from.y);
	const rate = flight === "loose" ? TUMBLE_DEG_PER_YD * 2.4 : TUMBLE_DEG_PER_YD;
	return (deg + dist * rate) % 360;
};

// WHAT THE GROUND DOES ABOUT IT. Two moments on a football field are worth an
// impact, and they must never look alike: a TACKLE (the play is over, energy
// goes into the turf) and a play breaking the plane for a SCORE.
//
// A tackle is a single hard thud that damps out fast - squared damping, most of
// it gone in the first third, like the court's rim clang. A score is a flare
// that overshoots and settles bright, because it is the one thing on a football
// field everyone in the stadium reacts to at once.
export const impactReaction = (
	kind: "tackle" | "score",
	p: number,
): { opacity: number; scale: number } => {
	if (!(p >= 0) || p >= 1) {
		return { opacity: 0, scale: 1 };
	}
	if (kind === "score") {
		const snap = Math.sin(Math.PI * Math.min(1, p * 1.3));
		return {
			opacity: 0.9 * (1 - p) ** 0.75,
			scale: 1 + snap * 1.9,
		};
	}
	const damp = (1 - p) * (1 - p);
	return {
		opacity: 0.45 * damp,
		scale: 0.85 + p * 0.7,
	};
};

// Glide duration (seconds) for a body covering `dist` yards, capped so it
// always lands before the scene it belongs to is replaced. Same contract as the
// court's glideSeconds, but paced to a football field: a kick return crosses
// eighty yards where a basketball possession crosses ninety FEET, so the
// per-unit rate is much lower or every return would take a full second longer
// than it has.
export const fieldGlideSeconds = (
	dist: number,
	sceneMs: number | undefined,
): number => {
	const cap = Math.min(0.95, ((sceneMs ?? 1100) / 1000) * 0.84);
	return Math.min(cap, 0.28 + dist * 0.011);
};
