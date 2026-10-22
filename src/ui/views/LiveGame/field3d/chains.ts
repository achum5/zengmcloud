import { makeCourtRng } from "../courtRng.ts";
import type { CrewPart } from "./crew.ts";
import type { Leg, Part } from "./director.ts";
import { BORDER, FIELD_W, YD, type Dir, type Pt } from "./geometry.ts";
import type { Move } from "./poses.ts";

// THE CHAIN CREW: on one sideline - the far one in the first half; they
// change sides at the half - just off the white border, the down box on the
// line of scrimmage showing the down, and the two rods on their ten yards of
// chain, the rear one where the series started and the front one on the
// line to gain; and over on the other sideline, a second box on the line.
// Each moves between plays, as the ball is spotted: the box up to the new
// line, and on a first down the chain, its rear rod up to the box and its
// front ten yards on. On a kick or a try, or with the goal to go, the rods
// are laid down and their men stand back; and a play coming at them out of
// bounds, they back off out of its way.

export const CHAIN = ["box", "rear", "front", "aux"] as const;
export type ChainMan = (typeof CHAIN)[number];
export const chainId = (c: ChainMan): number => -8 - CHAIN.indexOf(c);
export const chainManOf = (id: number): ChainMan | undefined =>
	id <= -8 ? CHAIN[-8 - id] : undefined;

// The chain crew working a game: each man's build, the same every time the
// game is watched.
export const chainCrewFor = (gid: number) => {
	const r = makeCourtRng(`chains|${gid}`);
	return CHAIN.map((c) => ({
		c,
		id: chainId(c),
		hgt: 67 + Math.floor(r() * 8),
		weight: 170 + Math.floor(r() * 70),
	}));
};

// Which sideline the chain is on (0 the far one), in a quarter.
const chainSide = (quarter: number): 0 | 1 => (quarter <= 2 ? 0 : 1);

// Where each is at the end of a play: which sideline the chain was on,
// whether the rods were up and the boxes - and the down they showed.
export type ChainSpots = {
	quarter: number;
	side: 0 | 1;
	at: Record<ChainMan, Pt>;
	up: boolean;
	boxUp: boolean;
	down: number;
};

export type ChainInput = {
	quarter: number;
	dir: Dir;
	losX: number;
	// A down from scrimmage (not a kick, not a try), its down, and its line
	// to gain - unset with the goal to go.
	scrimmage: boolean;
	down: number;
	gainX: number | undefined;
	t0: number;
	snap: number;
	whistle: number;
	t1: number;
	// The men in the play, for the chain crew to keep out of their way.
	parts: Part[];
	before: ChainSpots | undefined;
};

// Off the sideline: the poles' line (the box's and the rods'), where a man
// stands behind his - and stood back, out of the way.
export const POLE_OFF = BORDER + 1.5;
const MAN_OFF = POLE_OFF + 1;
const BACK_OFF = MAN_OFF + 7;
// Off from the snap this long in, one after another; there before it (or
// as far on as they can get - the picture takes them up partway there).
const LEAVE_MS: Record<ChainMan, number> = {
	box: 300,
	rear: 450,
	front: 700,
	aux: 400,
};
const THERE_BY = 500;
const JOG = 9;
const WALK = 4.6;
const WALK_ONLY = 12;

const yOf = (side: 0 | 1, off: number) => (side === 0 ? -off : FIELD_W + off);

// Out of the way of a play coming at him: anyone headed within this far of
// him (feet), he backs off from it, his pole with him, this far - in time,
// by his reckoning - and back to his spot once it is over.
const CLEAR = 9;
const BACK_AWAY = 8;
const SEES_MS = 900;
const BACK_FPS = 7;
const BACK_AFTER = 1400;

export const stageChains = (
	input: ChainInput,
): { parts: CrewPart[]; spots: ChainSpots } => {
	const { dir, losX, t0, t1 } = input;
	const side = chainSide(input.quarter);
	const other = side === 0 ? 1 : 0;
	// (Over a quarter's end, they had all the break to get to where the ball
	// is at the other end.)
	const was =
		input.before?.quarter === input.quarter ? input.before : undefined;
	const boxUp = input.scrimmage;
	const up = input.scrimmage && input.gainX !== undefined;
	const boxX = boxUp ? losX : (was?.at.box.x ?? losX);
	// (With the goal to go, the rods are laid down behind the box.)
	const rearX =
		input.gainX !== undefined && up
			? input.gainX - dir * 10 * YD
			: boxUp
				? boxX - dir * 10
				: (was?.at.rear.x ?? losX - dir * 10);
	const frontX =
		input.gainX !== undefined && up
			? input.gainX
			: boxUp
				? boxX - dir * 19
				: (was?.at.front.x ?? losX - dir * 19);
	const to: Record<ChainMan, Pt> = {
		box: { x: boxX, y: yOf(side, boxUp ? MAN_OFF : BACK_OFF) },
		rear: { x: rearX, y: yOf(side, up ? MAN_OFF : BACK_OFF) },
		front: { x: frontX, y: yOf(side, up ? MAN_OFF : BACK_OFF) },
		aux: { x: boxX, y: yOf(other, boxUp ? MAN_OFF : BACK_OFF) },
	};
	const sideOf = (c: ChainMan) => (c === "aux" ? other : side);
	const parts = CHAIN.map((c): CrewPart => {
		const isBox = c === "box" || c === "aux";
		const holding = isBox ? boxUp : up;
		const held: Move = holding ? "holdPole" : "stand";
		const target = to[c];
		const yaw = sideOf(c) === 0 ? Math.PI / 2 : -Math.PI / 2;
		const legs: Leg[] = [];
		let from = was?.at[c];
		let there = t0;
		if (from) {
			const d = Math.hypot(target.x - from.x, target.y - from.y);
			const leave = t0 + LEAVE_MS[c];
			const fps = d > WALK_ONLY && holding ? JOG : WALK;
			const reach = Math.max(0, ((input.snap - THERE_BY - leave) / 1000) * fps);
			if (d > reach) {
				const u = reach / d;
				from = {
					x: target.x + (from.x - target.x) * u,
					y: target.y + (from.y - target.y) * u,
				};
			}
			const go = Math.min(d, reach);
			if (go >= 0.5) {
				// (He has his pole up as he goes - picked up off the turf, or
				// carried until he lays it down.)
				const had = (isBox ? was?.boxUp : was?.up) === true;
				there = leave + (go / fps) * 1000;
				legs.push(
					{
						t0,
						t1: leave,
						from,
						to: from,
						move: had ? "holdPole" : "stand",
						yaw,
					},
					{
						t0: leave,
						t1: there,
						from,
						to: target,
						move: fps === JOG ? "jog" : "walk",
						arms: "holdPole",
					},
				);
			}
		}
		const coming = comingAt(input, target);
		const back =
			coming === undefined
				? undefined
				: Math.max(there, input.snap, coming - SEES_MS);
		if (back === undefined) {
			legs.push({ t0: there, t1, from: target, to: target, move: held, yaw });
		} else {
			const away = {
				x: target.x,
				y: target.y + (sideOf(c) === 0 ? -BACK_AWAY : BACK_AWAY),
			};
			const gone = back + (BACK_AWAY / BACK_FPS) * 1000;
			const again = Math.max(gone, input.whistle + BACK_AFTER);
			const home = again + (BACK_AWAY / WALK) * 1000;
			const pole = holding ? { arms: "holdPole" as const } : {};
			legs.push(
				{ t0: there, t1: back, from: target, to: target, move: held, yaw },
				{
					t0: back,
					t1: gone,
					from: target,
					to: away,
					move: "back",
					yaw,
					...pole,
				},
			);
			if (home < t1) {
				legs.push(
					{ t0: gone, t1: again, from: away, to: away, move: held, yaw },
					{
						t0: again,
						t1: home,
						from: away,
						to: target,
						move: "walk",
						...pole,
					},
					{ t0: home, t1, from: target, to: target, move: held, yaw },
				);
			} else {
				legs.push({ t0: gone, t1, from: away, to: away, move: held, yaw });
			}
		}
		return { pid: chainId(c), team: 0, legs };
	});
	const at = Object.fromEntries(
		CHAIN.map((c, i) => [c, parts[i]!.legs.at(-1)!.to]),
	) as Record<ChainMan, Pt>;
	return {
		parts,
		spots: {
			quarter: input.quarter,
			side,
			at,
			up,
			boxUp,
			down: input.scrimmage ? input.down : (input.before?.down ?? 1),
		},
	};
};

// When the first man in the play is headed to within CLEAR of the spot,
// if anyone is: along each leg he runs, from the snap until a moment after
// the whistle, the nearest he comes.
const comingAt = (input: ChainInput, spot: Pt): number | undefined => {
	let first: number | undefined;
	for (const p of input.parts) {
		for (const l of p.legs) {
			if (l.t1 < input.snap || l.t0 > input.whistle + 600) {
				continue;
			}
			const dx = l.to.x - l.from.x;
			const dy = l.to.y - l.from.y;
			const len2 = dx * dx + dy * dy;
			const u =
				len2 > 0
					? Math.min(
							1,
							Math.max(
								0,
								((spot.x - l.from.x) * dx + (spot.y - l.from.y) * dy) / len2,
							),
						)
					: 0;
			const x = l.from.x + dx * u;
			const y = l.from.y + dy * u;
			if (Math.hypot(x - spot.x, y - spot.y) < CLEAR) {
				const t = l.t0 + (l.t1 - l.t0) * u;
				if (first === undefined || t < first) {
					first = t;
				}
				break;
			}
		}
	}
	return first;
};
