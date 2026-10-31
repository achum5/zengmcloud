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

// HOW HE STANDS OUT from the others in the game who play where he does, at
// what he is rated in (-1 well short of them, 1 well beyond them): this far
// from the middle of them is a whole step, however spread out they are.
const STEP_LEAST = 6;
const standOut = (
	players: { pid: number; pos: string; ratings?: FieldRatings }[],
	key: keyof FieldRatings,
): Map<number, number> => {
	const byPos = new Map<string, number[]>();
	for (const p of players) {
		const v = p.ratings?.[key];
		if (typeof v === "number") {
			byPos.set(p.pos, [...(byPos.get(p.pos) ?? []), v]);
		}
	}
	const out = new Map<number, number>();
	for (const p of players) {
		const v = p.ratings?.[key];
		const all = byPos.get(p.pos);
		if (typeof v !== "number" || !all || all.length < 2) {
			continue;
		}
		const mean = all.reduce((a, b) => a + b, 0) / all.length;
		const sd = Math.sqrt(
			all.reduce((a, b) => a + (b - mean) ** 2, 0) / all.length,
		);
		out.set(
			p.pid,
			Math.max(-1, Math.min(1, (v - mean) / Math.max(STEP_LEAST, sd * 2))),
		);
	}
	return out;
};

// Where the stars show it: a quarterback's arm (how hard he throws it), a
// receiver's hands (up for it, or laid out for it), a back's strength (an
// arm out into a man), a pass rusher's hands and a blocker's feet against
// each other (see passRush), a defensive back's coverage (how tight he
// sits on his man) - each against the others at his position.
export type Flair = {
	arm?: number;
	hands?: number;
	power?: number;
	rush?: number;
	block?: number;
	cover?: number;
};
export const flairOf = (
	players: { pid: number; pos: string; ratings?: FieldRatings }[],
): Map<number, Flair> => {
	const arm = standOut(players, "thp");
	const hands = standOut(players, "hnd");
	const power = standOut(players, "stre");
	const rush = standOut(players, "prs");
	const block = standOut(players, "pbk");
	const cover = standOut(players, "pcv");
	const out = new Map<number, Flair>();
	for (const p of players) {
		const f: Flair = {
			...(arm.has(p.pid) ? { arm: arm.get(p.pid) } : {}),
			...(hands.has(p.pid) ? { hands: hands.get(p.pid) } : {}),
			...(power.has(p.pid) ? { power: power.get(p.pid) } : {}),
			...(rush.has(p.pid) ? { rush: rush.get(p.pid) } : {}),
			...(block.has(p.pid) ? { block: block.get(p.pid) } : {}),
			...(cover.has(p.pid) ? { cover: cover.get(p.pid) } : {}),
		};
		if (Object.keys(f).length > 0) {
			out.set(p.pid, f);
		}
	}
	return out;
};
