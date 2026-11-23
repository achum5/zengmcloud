import { type CourtRatings, levelOf, type RatingKey } from "../ratings.ts";
import {
	fits,
	MOVE_TYPES,
	type MoveType,
	packageOf,
	packagesFor,
	type Package,
} from "./packages.ts";

// HIS SIGNATURE: which package (see packages.ts) he has for each kind of
// move. One saved for him is his; otherwise it is picked from his ratings,
// with a draw that is his own (by his player id) - the same every game, on
// every device, with nothing in the league to change.
export type Signature = Record<MoveType, string>;

// A number from 0 to 1 that is his, for this kind of move: worked out in
// whole-number arithmetic, which every device does alike.
const drawOf = (pid: number, salt: number): number => {
	let h = Math.imul(pid | 0, 0x9e3779b1) ^ Math.imul(salt + 1, 0x85ebca77);
	h ^= h >>> 16;
	h = Math.imul(h, 0x7feb352d);
	h ^= h >>> 15;
	h = Math.imul(h, 0x846ca68b);
	h ^= h >>> 16;
	return (h >>> 0) / 4294967296;
};

// How strongly his ratings lean him toward a package that suits them: a
// man as good as they come at what it prefers is this many times likelier
// to have it as one with next to none of it.
const LEAN = 6;
// (Each rating it prefers moves its chances by up to this, either way -
// times as much for the best, a share of it for the worst.)
const weightOf = (p: Package, r: CourtRatings | undefined): number => {
	let w = p.fit.share;
	for (const [k, by] of Object.entries(p.fit.prefer)) {
		const level = levelOf(r, [k as RatingKey]);
		if (level !== undefined && by !== undefined) {
			// (From 1/sqrt(LEAN) to sqrt(LEAN), on a curve with no logs or
			// powers in it.)
			const x = (level - 0.5) * 2 * by;
			const half = Math.sqrt(LEAN);
			w *= x >= 0 ? 1 + (half - 1) * x : 1 / (1 + (half - 1) * -x);
		}
	}
	return w;
};

export const pickFor = (
	type: MoveType,
	pid: number,
	r: CourtRatings | undefined,
): Package => {
	const all = packagesFor(type);
	const open = all.filter((p) => fits(p, r));
	const from = open.length > 0 ? open : all;
	const weights = from.map((p) => weightOf(p, r));
	const total = weights.reduce((a, b) => a + b, 0);
	let u = drawOf(pid, MOVE_TYPES.indexOf(type)) * total;
	for (let i = 0; i < from.length; i++) {
		u -= weights[i]!;
		if (u < 0) {
			return from[i]!;
		}
	}
	return from.at(-1)!;
};

// His signature: what is saved for him where it names a package of that
// kind, the rest picked.
export const signatureOf = (
	pid: number,
	r: CourtRatings | undefined,
	saved?: Partial<Signature>,
): Signature => {
	const out = {} as Signature;
	for (const type of MOVE_TYPES) {
		const own = saved?.[type];
		out[type] =
			own !== undefined && packageOf(own)?.type === type
				? own
				: pickFor(type, pid, r).id;
	}
	return out;
};
