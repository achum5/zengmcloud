// Deterministic randomness for the live court.
//
// The sim emits no coordinates, so every position on the court is invented by
// the viewer. With Math.random that meant two people watching the SAME
// broadcast saw the same three taken from different spots on the floor, and a
// saved replay re-rolled itself every time you watched it.
//
// Nothing needs to be sent over the wire to fix that. Every device already
// loads the identical play-by-play array, and the broadcast cursor is already
// an index into it (`initialEventCount - events.length`, the number the simmer
// publishes and followers step to). Seeding from (gid, that index) makes every
// device invent the SAME fiction from the same inputs - no payload, no
// latency, and it works for an offline replay too.

// mulberry32 over an FNV-1a hash of the seed string. Small, fast, and stable
// across engines - which matters, since two devices must agree exactly.
export const makeCourtRng = (seed: string): (() => number) => {
	let h = 2166136261;
	for (let i = 0; i < seed.length; i += 1) {
		h ^= seed.charCodeAt(i);
		h = Math.imul(h, 16777619);
	}
	let a = h >>> 0;
	return () => {
		a |= 0;
		a = (a + 0x6d2b79f5) | 0;
		let t = Math.imul(a ^ (a >>> 15), 1 | a);
		t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
		return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
	};
};

// The stream the synth*Spot helpers draw from. It is module-level rather than
// threaded through every helper because those are called from a dozen places
// inside one synchronous event handler; re-seeding once at the top of that
// handler covers all of them without changing a signature.
//
// Left unseeded it falls back to Math.random, so callers that genuinely want
// variety (the court editor's preview) keep it.
let current: (() => number) | undefined;

// Start a fresh deterministic stream for one play. Called once per play-by-play
// event, before any spot for that play is invented.
export const seedCourtRng = (seed: string) => {
	current = makeCourtRng(seed);
};

export const clearCourtRng = () => {
	current = undefined;
};

export const courtRandom = (): number => (current ? current() : Math.random());
