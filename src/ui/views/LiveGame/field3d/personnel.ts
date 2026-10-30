import type { FieldRatings } from "./ratings.ts";
import type { Side } from "./geometry.ts";

// WHO IS ON THE FIELD FOR A SNAP. The sim picks its eleven off each depth
// chart, by the groups its formation calls for (five OL, three WR, a TE and
// a back; nine OL with a kicker and a holder for a kick; ...), and says so
// only in passing: who blocked, who threw, who was thrown to, who made the
// tackle. So the men a play's stats name are on for it, in the groups they
// played it in, and the rest of each group is filled the way the sim fills
// it - down its depth chart past anyone hurt.

// The sim's groups (its depth chart's positions).
export const GROUPS = [
	"QB",
	"RB",
	"WR",
	"TE",
	"OL",
	"DL",
	"LB",
	"CB",
	"S",
	"K",
	"P",
	"KR",
	"PR",
] as const;
export type Group = (typeof GROUPS)[number];
export type DepthChart = Partial<Record<Group, number[]>>;

export type RosterMan = {
	pid: number;
	team: Side;
	pos: string;
	name?: string;
	// His ratings, where the game has them (see ratings.ts).
	ratings?: FieldRatings;
};

const isGroup = (s: unknown): s is Group =>
	typeof s === "string" && (GROUPS as readonly string[]).includes(s);

// The two depth charts, by display side (0 the visitors), off the
// play-by-play's first event - the sim's own two teams, raw team 0 the home
// team - or, without them, each group read off the box score's positions.
export const depthCharts = (
	init: { type: string; boxScore?: unknown } | undefined,
	roster: RosterMan[],
): [DepthChart, DepthChart] => {
	const out: [DepthChart, DepthChart] = [{}, {}];
	const raw = init?.boxScore as
		| { [k: number]: { depth?: Record<string, unknown> } | undefined }
		| undefined;
	for (const side of [0, 1] as const) {
		const depth = raw?.[1 - side]?.depth;
		if (!depth || typeof depth !== "object") {
			continue;
		}
		for (const [group, list] of Object.entries(depth)) {
			if (!isGroup(group) || !Array.isArray(list)) {
				continue;
			}
			out[side][group] = list
				.map((p: unknown) =>
					typeof p === "number" ? p : (p as { id?: unknown } | null)?.id,
				)
				.filter((pid): pid is number => typeof pid === "number");
		}
	}
	for (const side of [0, 1] as const) {
		for (const group of GROUPS) {
			if (out[side][group]?.length) {
				continue;
			}
			const by = roster.filter((p) => p.team === side && p.pos === group);
			out[side][group] = by.map((p) => p.pid);
		}
	}
	return out;
};

// Who can stand in for a group the depth chart has run out of: the nearest
// kind of player.
const STAND_INS: Record<Group, Group[]> = {
	QB: ["RB", "WR"],
	RB: ["WR", "TE", "CB"],
	WR: ["RB", "TE", "CB"],
	TE: ["OL", "WR", "DL"],
	OL: ["DL", "TE"],
	DL: ["LB", "OL"],
	LB: ["DL", "S"],
	CB: ["S", "WR"],
	S: ["CB", "LB"],
	K: ["P"],
	P: ["K", "QB"],
	KR: ["RB", "WR", "CB"],
	PR: ["WR", "RB", "CB"],
};

// A slot to fill: the group the sim would fill it from.
export type Want = { group: Group };

// The eleven of one side for one snap: each slot's man, in the slots' order.
// `named` the men the play's stats say were on, with the groups they could
// have been playing in (unsaid: any); `out` anyone hurt and gone; `roster`
// the rest of the side's men, for a group nobody on the chart is left for.
export const fillSlots = (
	slots: Want[],
	depth: DepthChart,
	named: { pid: number; groups?: Group[] }[],
	out: Set<number>,
	roster: number[],
): (number | undefined)[] => {
	const got: (number | undefined)[] = slots.map(() => undefined);
	const used = new Set<number>();
	const rank = (pid: number, g: Group) => {
		const i = depth[g]?.indexOf(pid) ?? -1;
		return i < 0 ? Infinity : i;
	};
	const place = (pid: number, i: number) => {
		got[i] = pid;
		used.add(pid);
	};
	const any = [...new Set(slots.map((s) => s.group))];
	for (const n of named) {
		if (used.has(n.pid)) {
			continue;
		}
		// Into a slot of the group he played, the one he is highest on the
		// chart for first - or, if the formation has none of those free, of
		// a group he could stand in for.
		const own = n.groups ?? any;
		let best = -1;
		let bestRank = Infinity;
		for (const [pass, groups] of [
			own,
			own.flatMap((g) => STAND_INS[g]),
		].entries()) {
			for (const [i, s] of slots.entries()) {
				if (got[i] !== undefined || !groups.includes(s.group)) {
					continue;
				}
				const r = rank(n.pid, s.group) + pass * 1000;
				if (best < 0 || r < bestRank) {
					best = i;
					bestRank = r;
				}
			}
			if (best >= 0) {
				break;
			}
		}
		if (best < 0) {
			best = got.indexOf(undefined);
		}
		if (best >= 0) {
			place(n.pid, best);
		}
	}
	const free = (pid: number) => !used.has(pid) && !out.has(pid);
	for (const [i, s] of slots.entries()) {
		if (got[i] !== undefined) {
			continue;
		}
		const pick =
			depth[s.group]?.find(free) ??
			STAND_INS[s.group]
				.map((g) => depth[g]?.find(free))
				.find((pid) => pid !== undefined) ??
			roster.find(free) ??
			// (Short-handed past even that: someone hurt, sooner than nobody.)
			roster.find((pid) => !used.has(pid));
		if (pick !== undefined) {
			place(pick, i);
		}
	}
	return got;
};
