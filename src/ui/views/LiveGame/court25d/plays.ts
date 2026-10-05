import { PLAYBOOK, SPOTS, type PlaySource } from "./playbook.ts";

// THE PLAYBOOK, READ: each set parsed into steps a director can run, and the
// choice of which set a possession ran.
//
// The sim says who shot, from which zone, and who (if anyone) passed it to
// him. Those are the facts a set has to end in. So the court looks for sets
// with a scoring option that matches - a corner three off a kickout, a roll to
// the rim off a lob - and casts the five on the floor into its roles so that
// the real shooter and the real passer land in the right ones and everybody
// else plays a part his position can play. Then it picks among the ones that
// fit, weighted by how often each is run.

// A role in a set, 0 for "1" (usually the point guard) to 4 for "5".
export type Role = 0 | 1 | 2 | 3 | 4;
export type PlayZone = "rim" | "post" | "mid" | "three";
export type PlayCategory = PlaySource["cat"];
export type TurnoverKind =
	| "screen"
	| "lost"
	| "pass"
	| "charge"
	| "fiveSeconds"
	| "travel"
	| "shotClock";

export type PlayAction =
	| {
			type: "screen";
			who: Role[];
			for: Role;
			at: string[];
			kind: string;
			double: boolean;
	  }
	| { type: "dribble"; who: Role; to: string; kind: string }
	| { type: "move"; who: Role; to: string; style: string }
	| { type: "pass"; who: Role; to: Role; kind: string }
	| { type: "handoff"; who: Role; to: Role; get: boolean }
	| { type: "post"; who: Role; at: string; move: string };

export type PlayOption = {
	shooter: Role;
	zone: PlayZone;
	kind: string;
	at: string;
	// The step after which it is open.
	after: number;
	assist?: Role;
	// How the last pass is thrown, if it still has to be.
	pass?: string;
	playType: string;
	weight: number;
	// The read's own moves, after that step and before the shot.
	branch: PlayAction[];
};

export type PlayRisk = { step: number; who: Role; kind: TurnoverKind };

export type Play = {
	id: string;
	cat: PlayCategory;
	use: number;
	start: string[];
	ball: Role;
	needs: string[][];
	steps: PlayAction[][];
	options: PlayOption[];
	risks: PlayRisk[];
};

const role = (s: string | undefined): Role => {
	const n = Number(s) - 1;
	if (!Number.isInteger(n) || n < 0 || n > 4) {
		throw new Error(`Not a role: ${s}`);
	}
	return n as Role;
};

const spotName = (s: string | undefined): string => {
	if (s === undefined || !(s in SPOTS)) {
		throw new Error(`Not a spot: ${s}`);
	}
	return s;
};

export const parseAction = (text: string): PlayAction => {
	const [who, verb, a, b, c, d] = text.trim().split(/\s+/);
	switch (verb) {
		case "screen":
			return {
				type: "screen",
				who: who!.split("+").map(role),
				for: role(a),
				at: b!.split("+").map(spotName),
				kind: c ?? "ball",
				double: d === "double",
			};
		case "dribble":
			return {
				type: "dribble",
				who: role(who),
				to: spotName(a),
				kind: b ?? "",
			};
		case "move":
			return { type: "move", who: role(who), to: spotName(a), style: b ?? "" };
		case "pass":
			return { type: "pass", who: role(who), to: role(a), kind: b ?? "" };
		case "handoff":
			return {
				type: "handoff",
				who: role(who),
				to: role(a),
				get: b !== "fake",
			};
		case "post":
			return { type: "post", who: role(who), at: spotName(a), move: b ?? "" };
		default:
			throw new Error(`Not an action: ${text}`);
	}
};

const parseActions = (text: string): PlayAction[] =>
	text
		.split(";")
		.map((s) => s.trim())
		.filter((s) => s.length > 0)
		.map(parseAction);

const ZONES = new Set<string>(["rim", "post", "mid", "three"]);

const parseOption = (text: string): PlayOption => {
	const [head, branch] = text.split("|");
	const [shooter, zone, kind, at, after, assist, pass, playType, weight] = head!
		.trim()
		.split(/\s+/);
	if (!ZONES.has(zone!)) {
		throw new Error(`Not a zone: ${text}`);
	}
	return {
		shooter: role(shooter),
		zone: zone as PlayZone,
		kind: kind!,
		at: spotName(at),
		after: Number(after),
		...(assist === "-" ? {} : { assist: role(assist) }),
		...(pass === "-" || pass === undefined ? {} : { pass }),
		playType: playType!,
		weight: Number(weight),
		branch: branch ? parseActions(branch) : [],
	};
};

const parseRisk = (text: string): PlayRisk => {
	const [step, who, kind] = text.trim().split(/\s+/);
	return { step: Number(step), who: role(who), kind: kind as TurnoverKind };
};

export const parsePlay = (src: PlaySource): Play => {
	const start = src.start.trim().split(/\s+/);
	if (start.length !== 5) {
		throw new Error(`${src.id}: five spots to start`);
	}
	const ball = start.findIndex((s) => s.endsWith("*"));
	return {
		id: src.id,
		cat: src.cat,
		use: src.use,
		start: start.map((s) => spotName(s.replace("*", ""))),
		ball: role(String(ball + 1)),
		needs: src.needs.split("|").map((s) =>
			s
				.trim()
				.split(/\s+/)
				.filter((n) => n !== "-"),
		),
		steps: src.steps.map(parseActions),
		options: src.options.map(parseOption),
		risks: src.risks.map(parseRisk),
	};
};

export const PLAYS: Play[] = PLAYBOOK.map(parsePlay);

// ---- casting ---------------------------------------------------------------

// One of the five on the floor: who, and his position as a rank from 0 (a
// point guard) to 8 (a center).
export type Cast = { pid: number; rank: number };

// The position each role is drawn for: 1 the point, 5 the center.
const ROLE_RANK = [0, 2, 4, 6, 8];
const BIG_NEEDS = new Set([
	"rollMan",
	"popBig",
	"rimRunner",
	"lobThreat",
	"postScorer",
	"handoffBig",
	"big",
	"stretchBig",
	"rebounder",
]);

// How badly a player of this rank fits a role: a center bringing it up, a
// point guard rolling to the rim.
const roleCost = (play: Play, r: Role, rank: number): number => {
	const needs = play.needs[r] ?? [];
	let cost = Math.abs(rank - ROLE_RANK[r]!) * 0.3;
	if (needs.includes("ballHandler")) {
		cost += Math.max(0, rank - 3) * 0.55;
	}
	if (needs.some((n) => BIG_NEEDS.has(n))) {
		cost += Math.max(0, 5 - rank) * 0.5;
	}
	return cost;
};

const PERMS: Role[][] = (() => {
	const out: Role[][] = [];
	const walk = (left: Role[], acc: Role[]) => {
		if (left.length === 0) {
			out.push(acc);
		}
		for (const r of left) {
			walk(
				left.filter((x) => x !== r),
				[...acc, r],
			);
		}
	};
	walk([0, 1, 2, 3, 4], []);
	return out;
})();

// The cheapest way to put the five in the play's roles with some of them
// pinned to a role already. Returns the pid in each role.
export const castPlay = (
	play: Play,
	five: Cast[],
	pinned: Map<number, Role>,
): { roles: number[]; cost: number } | undefined => {
	if (five.length !== 5) {
		return undefined;
	}
	let best: { roles: number[]; cost: number } | undefined;
	for (const perm of PERMS) {
		let cost = 0;
		let ok = true;
		for (let k = 0; k < 5; k++) {
			const p = five[k]!;
			const r = perm[k]!;
			const pin = pinned.get(p.pid);
			if (pin !== undefined && pin !== r) {
				ok = false;
				break;
			}
			cost += roleCost(play, r, p.rank);
		}
		if (ok && (!best || cost < best.cost)) {
			const roles: number[] = [];
			for (let k = 0; k < 5; k++) {
				roles[perm[k]!] = five[k]!.pid;
			}
			best = { roles, cost };
		}
	}
	return best;
};

// ---- choosing ----------------------------------------------------------------

export type Called<X> = {
	play: Play;
	pick: X;
	// The pid in each role.
	roles: number[];
	// 1 as drawn, -1 mirrored side to side.
	mirror: 1 | -1;
};

const choose = <X>(
	rng: () => number,
	cands: { play: Play; pick: X; roles: number[]; w: number }[],
): Called<X> | undefined => {
	const total = cands.reduce((s, c) => s + c.w, 0);
	if (!(total > 0)) {
		return undefined;
	}
	let r = rng() * total;
	for (const c of cands) {
		r -= c.w;
		if (r <= 0) {
			return { ...c, mirror: rng() < 0.5 ? 1 : -1 };
		}
	}
	const c = cands.at(-1)!;
	return { ...c, mirror: rng() < 0.5 ? 1 : -1 };
};

// Fit is worth a lot: a set that needs a stretch big is a poor choice for a
// lineup without one.
const fitWeight = (cost: number) => Math.exp(-1.1 * cost);

export type ShotCall = {
	// Categories to look in, each with how much it is preferred.
	cats: Partial<Record<PlayCategory, number>>;
	zone: PlayZone;
	shooter: number;
	// The passer the sim credited, if it did.
	assist?: number;
	// A make the sim credited to nobody: he got it himself.
	unassisted?: boolean;
	// Whoever has the ball, when the set must start from him (a break).
	holder?: number;
	five: Cast[];
};

export const callShot = (
	rng: () => number,
	call: ShotCall,
	plays: Play[] = PLAYS,
): Called<PlayOption> | undefined => {
	const cands: {
		play: Play;
		pick: PlayOption;
		roles: number[];
		w: number;
	}[] = [];
	for (const play of plays) {
		const catW = call.cats[play.cat] ?? 0;
		if (catW <= 0) {
			continue;
		}
		const cache = new Map<string, ReturnType<typeof castPlay>>();
		for (const o of play.options) {
			if (o.zone !== call.zone) {
				continue;
			}
			if (call.assist !== undefined && o.assist === undefined) {
				continue;
			}
			if (call.unassisted && o.assist !== undefined) {
				continue;
			}
			const pinned = new Map<number, Role>([[call.shooter, o.shooter]]);
			if (call.assist !== undefined && o.assist !== undefined) {
				if (o.assist === o.shooter || call.assist === call.shooter) {
					continue;
				}
				pinned.set(call.assist, o.assist);
			}
			if (call.holder !== undefined) {
				const prior = pinned.get(call.holder);
				if (prior !== undefined && prior !== play.ball) {
					continue;
				}
				pinned.set(call.holder, play.ball);
			}
			const key = [...pinned].map(([p, r]) => `${p}:${r}`).join(",");
			let cast = cache.get(key);
			if (!cache.has(key)) {
				cast = castPlay(play, call.five, pinned);
				cache.set(key, cast);
			}
			if (!cast) {
				continue;
			}
			cands.push({
				play,
				pick: o,
				roles: cast.roles,
				w: catW * play.use * o.weight * fitWeight(cast.cost),
			});
		}
	}
	return choose(rng, cands);
};

export type TurnoverCall = {
	cats: Partial<Record<PlayCategory, number>>;
	victim: number;
	// How likely each way of losing it is.
	kinds: Partial<Record<TurnoverKind, number>>;
	holder?: number;
	five: Cast[];
};

export const callTurnover = (
	rng: () => number,
	call: TurnoverCall,
	plays: Play[] = PLAYS,
): Called<PlayRisk> | undefined => {
	const cands: { play: Play; pick: PlayRisk; roles: number[]; w: number }[] =
		[];
	for (const play of plays) {
		const catW = call.cats[play.cat] ?? 0;
		if (catW <= 0) {
			continue;
		}
		for (const risk of play.risks) {
			const kindW = call.kinds[risk.kind] ?? 0;
			if (kindW <= 0) {
				continue;
			}
			const pinned = new Map<number, Role>([[call.victim, risk.who]]);
			if (call.holder !== undefined && call.holder !== call.victim) {
				pinned.set(call.holder, play.ball);
			} else if (call.holder === call.victim && risk.who !== play.ball) {
				continue;
			}
			const cast = castPlay(play, call.five, pinned);
			if (!cast) {
				continue;
			}
			cands.push({
				play,
				pick: risk,
				roles: cast.roles,
				w: catW * play.use * fitWeight(cast.cost),
			});
		}
	}
	// How it goes wrong first, at the rate it goes wrong in the league - not
	// at the rate the playbook happens to list it - then a set it can go wrong
	// in that way.
	const kinds = [...new Set(cands.map((c) => c.pick.kind))];
	const kind = choose(
		rng,
		kinds.map((k) => ({
			play: cands[0]!.play,
			pick: k,
			roles: [],
			w: call.kinds[k] ?? 0,
		})),
	)?.pick;
	return choose(
		rng,
		cands.filter((c) => c.pick.kind === kind),
	);
};

// Any set at all, by how often it is run - for a possession that ends in a
// foul away from the shot.
export const callAny = (
	rng: () => number,
	call: {
		cats: Partial<Record<PlayCategory, number>>;
		five: Cast[];
		holder?: number;
	},
	plays: Play[] = PLAYS,
): Called<undefined> | undefined => {
	const cands: { play: Play; pick: undefined; roles: number[]; w: number }[] =
		[];
	for (const play of plays) {
		const catW = call.cats[play.cat] ?? 0;
		if (catW <= 0) {
			continue;
		}
		const pinned = new Map<number, Role>();
		if (call.holder !== undefined) {
			pinned.set(call.holder, play.ball);
		}
		const cast = castPlay(play, call.five, pinned);
		if (cast) {
			cands.push({
				play,
				pick: undefined,
				roles: cast.roles,
				w: catW * play.use * fitWeight(cast.cost),
			});
		}
	}
	return choose(rng, cands);
};

// ---- following a set on paper ---------------------------------------------------

// Who has the ball after an action, given who had it before.
export const holderAfter = (a: PlayAction, holder: Role): Role => {
	if (a.type === "pass" && a.who === holder) {
		return a.to;
	}
	if (a.type === "handoff" && a.get && a.who === holder) {
		return a.to;
	}
	return holder;
};

// Where everybody stands, and who has the ball, after the first `n` steps and
// then some extra actions - what the court will act out, worked on paper.
export const walkPlay = (
	play: Play,
	n: number,
	extra: PlayAction[] = [],
): { at: string[]; holder: Role } => {
	const at = [...play.start];
	let holder = play.ball;
	const apply = (a: PlayAction) => {
		if (a.type === "dribble" || a.type === "move") {
			at[a.who] = a.to;
		} else if (a.type === "post") {
			at[a.who] = a.at;
		} else if (a.type === "screen") {
			a.who.forEach((w, k) => {
				at[w] = a.at[k] ?? a.at[0]!;
			});
		} else if (a.type === "handoff") {
			if (a.get) {
				at[a.to] = at[a.who]!;
			}
		}
		holder = holderAfter(a, holder);
	};
	for (const step of play.steps.slice(0, n)) {
		for (const a of step) {
			apply(a);
		}
	}
	for (const a of extra) {
		apply(a);
	}
	return { at, holder };
};

export const spotXY = (name: string): readonly [number, number] =>
	SPOTS[name] ?? [0, 20];
