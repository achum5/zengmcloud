// THE COVERAGE BEHIND IT: what the defense plays on a pass, called from the
// look it showed before the snap - two safeties deep or one, the corners
// pressed or off - and the down and the distance; and in a zone, the spot
// on the field each man drops to.
//
// Two deep, it plays Cover 2 (the safeties a deep half each, the corners
// sat in the flats - now and then Tampa 2, the middle linebacker running
// the deep middle between them), Cover 4 (quarters: the corners and the
// safeties a deep quarter each), or 2-man (man under the two halves) - or
// rotates at the snap, a safety down, into Cover 3 or Cover 1. One deep:
// Cover 3 (the corners and the free safety a deep third each, four under)
// or Cover 1 (man, the free safety deep in the middle). Pressed, man.
//
// In the offense's own frame, in feet: `d` downfield of the line of
// scrimmage, `a` across the field to the offense's right of the ball.

import type { OPt } from "./routes.ts";

export type Coverage = "c1" | "c2" | "tampa2" | "c2man" | "c3" | "c4";

export const isZone = (c: Coverage): boolean =>
	c === "c2" || c === "tampa2" || c === "c3" || c === "c4";
// The deep men a man coverage keeps off its receivers: the free safety
// alone over Cover 1, both safeties over 2-man.
export const deepInMan = (c: Coverage, role: string): boolean =>
	role === "FS" || (c === "c2man" && role === "SS");

type Weights = [Coverage, number][];
const pick = (ws: Weights, u: number): Coverage => {
	const total = ws.reduce((s, [, w]) => s + w, 0);
	let x = u * total;
	for (const [c, w] of ws) {
		x -= w;
		if (x < 0) {
			return c;
		}
	}
	return ws.at(-1)![0];
};

// The call: off what it showed (`twoHigh`, `pressed`), and the down and the
// distance - third and long, the deep zones; short yardage and in close,
// man.
export const callCoverage = ({
	twoHigh,
	pressed,
	down,
	toGo,
	toGoal,
	u,
}: {
	twoHigh: boolean;
	pressed: boolean;
	down: number | undefined;
	toGo: number | undefined;
	// Yards to the goal line.
	toGoal: number;
	u: number;
}): Coverage => {
	if (pressed) {
		return twoHigh ? "c2man" : "c1";
	}
	const long = (down ?? 1) >= 3 && (toGo ?? 10) >= 7;
	const short = (toGo ?? 10) <= 2;
	const close = toGoal <= 20;
	if (twoHigh) {
		return pick(
			long
				? [
						["c4", 4],
						["c2", 2.5],
						["tampa2", 1.5],
						["c2man", 1],
						["c3", 0.7],
						["c1", 0.3],
					]
				: close || short
					? [
							["c2man", 2],
							["c1", 2],
							["c4", 3],
							["c2", 1.5],
							["c3", 1.5],
						]
					: [
							["c4", 3.5],
							["c2", 2],
							["tampa2", 1],
							["c2man", 1],
							["c3", 1.7],
							["c1", 0.8],
						],
			u,
		);
	}
	return pick(
		long
			? [
					["c3", 6.5],
					["c1", 3.5],
				]
			: close || short
				? [
						["c1", 5.5],
						["c3", 4.5],
					]
				: [
						["c3", 6],
						["c1", 4],
					],
		u,
	);
};

// A ZONE'S SPOTS: where each of its men drops to - deep zones for the
// safeties and corners, the rest under them: a flat out by the sideline, a
// curl-to-flat by the numbers, a hook inside them - each `deep` or not, and
// best suited to whom. (`mid` the middle of the field across, `half` from
// there to a sideline; `u` the play's random numbers, for how deep.)
export type ZoneSpot = { at: OPt; deep: boolean; by: string[] };
export const zoneSpots = (
	c: Coverage,
	mid: number,
	half: number,
	u: () => number,
): ZoneSpot[] => {
	const at = (d: [number, number], a: number): OPt => ({
		d: d[0] + (d[1] - d[0]) * u(),
		a: mid + a,
	});
	const deep = (d: [number, number], a: number, by: string[]): ZoneSpot => ({
		at: at(d, a),
		deep: true,
		by,
	});
	const under = (d: [number, number], a: number, by: string[]): ZoneSpot => ({
		at: at(d, a),
		deep: false,
		by,
	});
	// (Feet: a flat's depth, a curl's, a hook's - off the line.)
	const FLAT: [number, number] = [14, 20];
	const CURL: [number, number] = [20, 26];
	const HOOK: [number, number] = [26, 34];
	const UNDER = ["SAM", "MIKE", "WILL", "BUCK", "NB", "SS", "CB", "FS"];
	const BACKERS = ["MIKE", "WILL", "SAM", "BUCK", "NB", "SS"];
	switch (c) {
		case "c2":
		case "tampa2":
			return [
				deep([38, 48], -half * 0.45, ["FS", "SS"]),
				deep([38, 48], half * 0.45, ["SS", "FS"]),
				under(FLAT, -half * 0.7, ["CB", "NB", ...UNDER]),
				under(FLAT, half * 0.7, ["CB", "NB", ...UNDER]),
				c === "tampa2"
					? deep([42, 54], 0, ["MIKE", "BUCK", ...BACKERS])
					: under(HOOK, 0, BACKERS),
				under(HOOK, -half * 0.3, BACKERS),
				under(HOOK, half * 0.3, BACKERS),
			];
		case "c3":
			return [
				deep([42, 54], 0, ["FS", "SS"]),
				deep([36, 46], -half * 0.65, ["CB", "FS", "SS"]),
				deep([36, 46], half * 0.65, ["CB", "FS", "SS"]),
				under(CURL, -half * 0.5, ["SS", "NB", "SAM", "WILL", ...UNDER]),
				under(CURL, half * 0.5, ["SS", "NB", "SAM", "WILL", ...UNDER]),
				under(HOOK, -half * 0.12, BACKERS),
				under(HOOK, half * 0.12, BACKERS),
			];
		case "c4":
			return [
				deep([32, 42], -half * 0.75, ["CB", "SS", "FS"]),
				deep([32, 42], half * 0.75, ["CB", "SS", "FS"]),
				deep([34, 44], -half * 0.25, ["FS", "SS", "CB"]),
				deep([34, 44], half * 0.25, ["SS", "FS", "CB"]),
				under(CURL, -half * 0.5, ["NB", "SAM", "WILL", ...UNDER]),
				under(CURL, half * 0.5, ["NB", "SAM", "WILL", ...UNDER]),
				under(HOOK, -half * 0.15, BACKERS),
				under(HOOK, half * 0.15, BACKERS),
			];
		default:
			return [];
	}
};

// Each man in the zone his spot: the deep ones first, to the men best
// suited, nearest where they lined up; then the rest under them. Any left
// over a hook in the middle between the others; spots left over, nobody's.
export const dropTo = <M>(
	men: { m: M; role: string; at: OPt }[],
	spots: ZoneSpot[],
	mid: number,
): Map<M, OPt> => {
	const out = new Map<M, OPt>();
	const left = [...men];
	const fill = (deep: boolean) => {
		for (const s of spots.filter((x) => x.deep === deep)) {
			let best = -1;
			let bestScore = Infinity;
			for (const [k, x] of left.entries()) {
				const rank = s.by.indexOf(x.role);
				if (rank < 0) {
					continue;
				}
				const score =
					rank * 40 +
					Math.abs(x.at.a - s.at.a) +
					Math.abs(x.at.d - s.at.d) * 0.3;
				if (score < bestScore) {
					bestScore = score;
					best = k;
				}
			}
			if (best >= 0) {
				out.set(left[best]!.m, s.at);
				left.splice(best, 1);
			}
		}
	};
	fill(true);
	fill(false);
	for (const x of left) {
		out.set(x.m, {
			d: 24 + (x.at.d > 24 ? 4 : 0),
			a: mid + (x.at.a - mid) * 0.3,
		});
	}
	return out;
};
