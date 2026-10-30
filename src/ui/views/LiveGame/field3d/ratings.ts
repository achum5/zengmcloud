// HOW GOOD HE IS AT IT - football's (the 3D court's: see court3d/ratings.ts).
// The ratings the 3D field plays a man by, as the live box score carries
// them (see courtRatings.ts in worker/util): how fast he is next to the
// others in the game who play where he does, and how hard he is to get a
// hand on. A game without them - an old replay, the tests - plays
// everybody the way it always did.
export type FieldRatings = Partial<
	Record<
		| "ovr"
		| "hgt"
		| "stre"
		| "spd"
		| "endu"
		| "elu"
		| "bsc"
		| "rtr"
		| "hnd"
		| "thv"
		| "thp"
		| "tha"
		| "rbk"
		| "pbk"
		| "pcv"
		| "tck"
		| "prs"
		| "rns"
		| "kpw"
		| "kac"
		| "ppw"
		| "pac",
		number
	>
>;

// Flat out, each man's speed against the others in the game at his
// position: this much quicker (or slower) a point of speed over theirs, and
// no more either way than this.
const SPEED_PER = 0.004;
const SPEED_MOST = 0.07;

export const speedsOf = (
	players: { pid: number; pos: string; ratings?: FieldRatings }[],
): Map<number, number> => {
	const byPos = new Map<string, number[]>();
	for (const p of players) {
		const v = p.ratings?.spd;
		if (typeof v === "number") {
			byPos.set(p.pos, [...(byPos.get(p.pos) ?? []), v]);
		}
	}
	const out = new Map<number, number>();
	for (const p of players) {
		const v = p.ratings?.spd;
		const all = byPos.get(p.pos);
		if (typeof v !== "number" || !all || all.length < 2) {
			continue;
		}
		const mean = all.reduce((a, b) => a + b, 0) / all.length;
		const by = Math.max(
			-SPEED_MOST,
			Math.min(SPEED_MOST, (v - mean) * SPEED_PER),
		);
		out.set(p.pid, 1 + by);
	}
	return out;
};

// How hard he is to get a hand on (0-1): next to none, a lineman's; as good
// as they come, the slipperiest back in the league - or nothing to go by.
const ELU_LOW = 20;
const ELU_HIGH = 65;
export const elusiveness = (r: FieldRatings | undefined): number | undefined =>
	typeof r?.elu === "number"
		? Math.min(1, Math.max(0, (r.elu - ELU_LOW) / (ELU_HIGH - ELU_LOW)))
		: undefined;
