import type { CourtTimeline, Fx } from "../director.ts";
import { seatsAt, tensionAt } from "../evaluate.ts";
import type { Side } from "../geometry.ts";

// THE CROWD: how loud the building is and what it lets out, from what the
// game already knows - how tense it is late (the timeline's tension), how
// many are still in their seats, who scored and how, the calls, a run - and
// nothing it decides. Two crowds: the home fans, nearly all of it, and the
// road fans in a corner. The same for the device playing it (see
// engine.ts) and a film made of it.

// The home side, as the 3D court lays the teams out.
export const HOME: Side = 1;

export type ReactionKind =
	| "cheer"
	| "roar"
	| "groan"
	| "ooh"
	| "boo"
	| "applause";

export type Reaction = {
	t: number;
	kind: ReactionKind;
	// The home fans, or the road fans' few.
	home: boolean;
	// How big (0 to 1).
	size: number;
};

// The building's night: a playoff game is louder from the tip; a building
// half empty thinner.
export type CrowdNight = { playoffs: boolean; full: number };

// A whistle against the home side (the ball to the road team) the home fans
// let the officials hear about; one their way, they like.
const FOULS = new Set<Fx["call"]>([
	"foul",
	"shootingFoul",
	"andOne",
	"offensive",
	"travel",
]);

// A shot up in the air: from when he lets it go to when it is decided at
// the rim, and whose. (A pass into a play at the rim - see physics.ts.)
export type Flight = { t0: number; t1: number; team: Side; made: boolean };

// Which side has the ball at t.
export const possAt = (tl: CourtTimeline, t: number): Side | undefined => {
	const p = tl.poss;
	let lo = 0;
	let hi = p.length - 1;
	let k = -1;
	while (lo <= hi) {
		const mid = (lo + hi) >> 1;
		if (p[mid]![0] <= t) {
			k = mid;
			lo = mid + 1;
		} else {
			hi = mid - 1;
		}
	}
	return k >= 0 ? p[k]![1] : undefined;
};

// A basket's cheer comes this soon after the ball is decided at the rim.
const MADE_WITHIN = 1500;

const flightsOf = (tl: CourtTimeline): Flight[] => {
	const out: Flight[] = [];
	const cheers = tl.fx.filter((f) => f.kind === "cheer");
	for (let i = 0; i + 1 < tl.ball.length; i++) {
		const a = tl.ball[i]!;
		const b = tl.ball[i + 1]!;
		if (a.kind !== "fly" || b.kind !== "path" || b.t0 !== a.t1) {
			continue;
		}
		const team = possAt(tl, a.t0);
		if (team === undefined) {
			continue;
		}
		const made = cheers.some(
			(c) => c.team === team && c.t >= a.t0 && c.t <= b.t1 + MADE_WITHIN,
		);
		out.push({ t0: a.t0, t1: b.t1, team, made });
	}
	return out;
};

// When the game is over, and who won it.
const gameOverOf = (tl: CourtTimeline): number | undefined =>
	tl.beats.findLast((b) => b.type === "gameOver")?.actionStart;

const reactionsOf = (tl: CourtTimeline, flights: Flight[]): Reaction[] => {
	const out: Reaction[] = [];
	const over = gameOverOf(tl);
	const tense = (t: number) => tensionAt(tl, t);
	for (const f of tl.fx) {
		const home = f.team === HOME;
		const late = tense(f.t);
		switch (f.kind) {
			case "cheer": {
				if (over !== undefined && f.t >= over) {
					// The final horn: the building up for a home win, the
					// road fans for theirs while the rest sit stunned.
					if (home) {
						out.push(
							{ t: f.t, kind: "roar", home: true, size: 1 },
							{ t: f.t + 1500, kind: "applause", home: true, size: 0.8 },
						);
					} else {
						out.push(
							{ t: f.t, kind: "cheer", home: false, size: 0.7 },
							{ t: f.t, kind: "groan", home: true, size: 0.5 },
						);
					}
					break;
				}
				// A big one (a roar at the same moment) is the roar's.
				const big = tl.fx.some(
					(r) =>
						r.kind === "roar" && r.team === f.team && Math.abs(r.t - f.t) < 400,
				);
				if (big) {
					break;
				}
				out.push(
					home
						? { t: f.t, kind: "cheer", home: true, size: 0.45 + 0.35 * late }
						: { t: f.t, kind: "cheer", home: false, size: 0.3 + 0.3 * late },
				);
				break;
			}
			case "roar": {
				if (over !== undefined && f.t >= over) {
					break;
				}
				const dunk = f.what === "dunk";
				out.push(
					home
						? {
								t: f.t,
								kind: "roar",
								home: true,
								size: Math.min(1, 0.7 + (dunk ? 0.1 : 0) + 0.25 * late),
							}
						: { t: f.t, kind: "cheer", home: false, size: 0.5 + 0.3 * late },
				);
				// A big one from the road team late: the home fans let out a
				// groan - then the building goes quiet (see levelAt).
				if (!home && late > 0) {
					out.push({
						t: f.t + 150,
						kind: "groan",
						home: true,
						size: 0.3 + 0.3 * late,
					});
				}
				break;
			}
			case "block":
				out.push({ t: f.t, kind: "ooh", home: true, size: home ? 0.7 : 0.45 });
				if (home) {
					out.push({
						t: f.t + 250,
						kind: "cheer",
						home: true,
						size: 0.5 + 0.3 * late,
					});
				}
				break;
			case "whistle":
				if (f.call && FOULS.has(f.call) && f.team !== undefined) {
					if (f.team !== HOME) {
						out.push({
							t: f.t + 300,
							kind: "boo",
							home: true,
							size: 0.3 + 0.5 * late,
						});
					} else if (f.call === "offensive") {
						out.push({
							t: f.t + 200,
							kind: "cheer",
							home: true,
							size: 0.5 + 0.3 * late,
						});
					}
				}
				break;
			case "clinch":
			case "confetti":
				out.push({ t: f.t, kind: "roar", home: f.team === HOME, size: 1 });
				break;
			default:
				break;
		}
	}
	// A miss late in a close one: a groan for the home side's, a cheer of
	// relief for the road team's.
	for (const fl of flights) {
		if (fl.made) {
			continue;
		}
		const late = tense(fl.t1);
		if (late <= 0) {
			continue;
		}
		out.push(
			fl.team === HOME
				? { t: fl.t1, kind: "groan", home: true, size: 0.25 + 0.35 * late }
				: { t: fl.t1, kind: "cheer", home: true, size: 0.3 + 0.35 * late },
		);
	}
	out.sort((a, b) => a.t - b.t);
	return out;
};

// THE HOME FANS' LEVEL over the night (0 to 1): what sets the crowd sound's
// layer, from a murmur to the building shaking (see engine.ts).
//
// From the footage: a big home basket brings the place up within a fifth
// of a second, keeps it up about two seconds, and it falls back over the
// next few.
const UP_MS = 200;
const HOLD_MS = 1800;
const DOWN_MS = 3500;
const envelope = (e: number): number =>
	e < 0
		? 0
		: e < UP_MS
			? e / UP_MS
			: e < HOLD_MS
				? 1
				: Math.exp(-(e - HOLD_MS) / DOWN_MS);
// (Long enough after that it no longer counts.)
const REACH_MS = HOLD_MS + DOWN_MS * 4;

// How much each kind lifts the building (a groan or a boo is noise too).
const LIFT: Record<ReactionKind, number> = {
	cheer: 0.35,
	roar: 0.55,
	groan: 0.1,
	ooh: 0.25,
	boo: 0.2,
	applause: 0.2,
};

export type Crowd = {
	reactions: Reaction[];
	flights: Flight[];
	// The home side's baskets in a row, by when each was.
	run: { t: number; n: number }[];
	// The road team's big ones late: the building goes quiet.
	hush: number[];
	// The breaks (timeouts, between periods): the crowd sits back.
	breaks: [number, number][];
};

// (Worked out once a timeline.)
const made = new WeakMap<CourtTimeline, Crowd>();
export const crowdOf = (tl: CourtTimeline): Crowd => {
	let c = made.get(tl);
	if (!c) {
		const flights = flightsOf(tl);
		const run: { t: number; n: number }[] = [];
		let n = 0;
		const hush: number[] = [];
		for (const f of tl.fx) {
			if (f.kind !== "cheer" || f.team === undefined) {
				continue;
			}
			n = f.team === HOME ? n + 1 : 0;
			run.push({ t: f.t, n });
			if (f.team !== HOME && tensionAt(tl, f.t) > 0) {
				hush.push(f.t);
			}
		}
		const breaks: [number, number][] = (tl.huddles ?? []).map((h) => [
			h.called,
			h.t1,
		]);
		c = { reactions: reactionsOf(tl, flights), flights, run, hush, breaks };
		made.set(tl, c);
	}
	return c;
};

// The last of a sorted list at or before t.
const lastAt = <T>(list: T[], t: number, key: (x: T) => number): number => {
	let lo = 0;
	let hi = list.length - 1;
	let k = -1;
	while (lo <= hi) {
		const mid = (lo + hi) >> 1;
		if (key(list[mid]!) <= t) {
			k = mid;
			lo = mid + 1;
		} else {
			hi = mid - 1;
		}
	}
	return k;
};

// A run of home baskets lifts the building a little more with each.
const RUN_STEP = 0.05;
const RUN_MOST = 0.2;
// The shot in the air: up as it goes, the same whether it goes in or not.
const FLIGHT_LIFT = 0.18;

export const levelAt = (
	tl: CourtTimeline,
	t: number,
	night: CrowdNight,
): number => {
	const c = crowdOf(tl);
	const late = tensionAt(tl, t);
	let base = 0.3 + 0.25 * late + (night.playoffs ? 0.12 : 0);
	// Sat back over a break.
	for (const [a, b] of c.breaks) {
		if (t >= a && t <= b) {
			base = Math.min(base, 0.16 + 0.1 * late);
			break;
		}
	}
	// A run going.
	const r = lastAt(c.run, t, (x) => x.t);
	if (r >= 0) {
		base += Math.min(RUN_MOST, RUN_STEP * Math.max(0, c.run[r]!.n - 1));
	}
	// A home shot up late, or any home three, in the air.
	const fl = lastAt(c.flights, t, (x) => x.t0);
	if (fl >= 0) {
		const f = c.flights[fl]!;
		if (f.team === HOME && t <= f.t1 && f.t1 > f.t0) {
			const u = (t - f.t0) / (f.t1 - f.t0);
			base += FLIGHT_LIFT * (0.4 + 0.6 * late) * u;
		}
	}
	// The road team's big one late: quiet for a few seconds.
	const h = lastAt(c.hush, t, (x) => x);
	if (h >= 0) {
		base -= 0.25 * envelope(t - c.hush[h]! - 400);
	}
	// What just happened.
	let lift = 0;
	for (let k = lastAt(c.reactions, t, (x) => x.t); k >= 0; k--) {
		const x = c.reactions[k]!;
		if (t - x.t > REACH_MS) {
			break;
		}
		if (x.home) {
			lift += LIFT[x.kind] * x.size * envelope(t - x.t);
		}
	}
	// Fewer in the seats: thinner.
	const there = Math.min(seatsAt(tl, t), Math.max(0.2, night.full));
	const level = (base + lift) * (0.45 + 0.55 * there);
	return Math.min(1, Math.max(0, level));
};
