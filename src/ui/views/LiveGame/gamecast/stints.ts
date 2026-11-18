import { helpers } from "../../../util/helpers.ts";
import processLiveGameEventsBasketball from "../../../util/processLiveGameEvents.basketball.tsx";

// WHO WAS ON THE FLOOR, AND HOW IT WENT. The game's plays run through again
// (as the replay runs them), cut wherever either team's five changes: each
// piece its stretch of the clock, the two fives on the floor through it, and
// what each team did in it. From these, a man's stints (rotations), every
// group of his team that played together (lineups), and how his team did
// with him on the floor and off it (on/off). All the sim's own plays: the
// subs, the shots, the points.

const KEYS = [
	"pts",
	"fg",
	"fga",
	"tp",
	"tpa",
	"ft",
	"fta",
	"orb",
	"drb",
	"ast",
	"tov",
	"stl",
	"blk",
] as const;
export type Tot = Record<(typeof KEYS)[number], number>;
export type Seg = {
	period: number;
	// Game seconds from the opening tip.
	start: number;
	end: number;
	on: [number[], number[]];
	d: [Tot, Tot];
};
export type Stints = {
	// The tids of the two sides, as on/d have them.
	tids: [number, number];
	segs: Seg[];
	// Each period's start and end in game seconds (0 the first period).
	periods: [number, number][];
	end: number;
};

const secsOf = (time: string | undefined) => {
	if (!time) {
		return 0;
	}
	const [m, s] = time.split(":");
	return s === undefined ? Number(m) || 0 : Number(m) * 60 + (Number(s) || 0);
};
const zero = (): Tot => Object.fromEntries(KEYS.map((k) => [k, 0])) as Tot;
const totOf = (t: any): Tot =>
	Object.fromEntries(KEYS.map((k) => [k, t[k] ?? 0])) as Tot;
const minus = (a: Tot, b: Tot): Tot =>
	Object.fromEntries(KEYS.map((k) => [k, a[k] - b[k]])) as Tot;
export const add = (a: Tot, b: Tot): Tot =>
	Object.fromEntries(KEYS.map((k) => [k, a[k] + b[k]])) as Tot;
export const sumTots = (list: Tot[]) => list.reduce(add, zero());
export const poss = (d: Tot) => d.fga + 0.44 * d.fta - d.orb + d.tov;

export const stintsFromEvents = (
	events: any[],
	initialBoxScore: any,
	// A game still going (the events so far): the fives on the floor now
	// stop at the last play, not at the end of the period.
	live?: boolean,
): Stints => {
	const box = helpers.deepCopy(initialBoxScore);
	const queue = events.slice();
	let overtimes = 0;
	let quarters: string[] = [];
	const segs: Seg[] = [];
	const periods: [number, number][] = [];
	let base = 0;
	let len = 0;
	let lastPeriod = 0;
	let cur:
		| {
				period: number;
				start: number;
				on: [number[], number[]];
				tot: [Tot, Tot];
		  }
		| undefined;
	const onOf = (i: 0 | 1) =>
		(box.teams[i].players as any[])
			.filter((p) => p.inGame)
			.map((p) => p.pid as number)
			.sort((a, b) => a - b);
	const close = (end: number) => {
		if (cur && end > cur.start) {
			segs.push({
				period: cur.period,
				start: cur.start,
				end,
				on: cur.on,
				d: [
					minus(totOf(box.teams[0]), cur.tot[0]),
					minus(totOf(box.teams[1]), cur.tot[1]),
				],
			});
		}
		cur = undefined;
	};
	let t = 0;
	while (queue.length > 0) {
		const out = processLiveGameEventsBasketball({
			events: queue,
			boxScore: box,
			overtimes,
			quarters,
		});
		overtimes = out.overtimes;
		quarters = out.quarters;
		const period: number = box.teams[0].ptsQtrs?.length ?? 0;
		if (period === 0) {
			continue;
		}
		const left = secsOf(box.time);
		if (period !== lastPeriod) {
			if (lastPeriod > 0) {
				close(base + len);
				periods.push([base, base + len]);
				base += len;
			}
			// (The period's length: its clock as it starts, to the minute.)
			len = Math.max(60, Math.ceil(left / 60) * 60);
			lastPeriod = period;
		}
		t = base + Math.max(0, len - left);
		const on: [number[], number[]] = [onOf(0), onOf(1)];
		if (
			!cur ||
			on[0].join() !== cur.on[0].join() ||
			on[1].join() !== cur.on[1].join()
		) {
			close(t);
			cur = {
				period,
				start: t,
				on,
				tot: [totOf(box.teams[0]), totOf(box.teams[1])],
			};
		}
	}
	if (lastPeriod > 0) {
		close(live ? t : base + len);
		periods.push([base, base + len]);
	}
	return {
		tids: [box.teams[0].tid, box.teams[1].tid],
		segs,
		periods,
		end: base + len,
	};
};

// Every group of k of a team's men on the floor together: their time, their
// possessions (the average of both teams'), points for and against.
export type Lineup = {
	pids: number[];
	secs: number;
	poss: number;
	ptsFor: number;
	ptsAgainst: number;
	possFor: number;
	possAgainst: number;
};
const combos = (list: number[], k: number): number[][] => {
	if (k === 0) {
		return [[]];
	}
	const out: number[][] = [];
	list.forEach((x, i) => {
		for (const rest of combos(list.slice(i + 1), k - 1)) {
			out.push([x, ...rest]);
		}
	});
	return out;
};
export const lineupsOf = (stints: Stints, team: 0 | 1, k: number) => {
	const by = new Map<string, Lineup>();
	for (const s of stints.segs) {
		const on = s.on[team];
		if (on.length < k) {
			continue;
		}
		for (const pids of combos(on, k)) {
			const key = pids.join(",");
			const l = by.get(key) ?? {
				pids,
				secs: 0,
				poss: 0,
				ptsFor: 0,
				ptsAgainst: 0,
				possFor: 0,
				possAgainst: 0,
			};
			const own = s.d[team];
			const opp = s.d[team === 0 ? 1 : 0];
			l.secs += s.end - s.start;
			l.ptsFor += own.pts;
			l.ptsAgainst += opp.pts;
			l.possFor += poss(own);
			l.possAgainst += poss(opp);
			l.poss = (l.possFor + l.possAgainst) / 2;
			by.set(key, l);
		}
	}
	return [...by.values()].sort((a, b) => b.secs - a.secs);
};

// A man's team with him on the floor and off it: the time, and what his team
// and theirs did meanwhile.
export const onOffOf = (stints: Stints, team: 0 | 1, pid: number) => {
	const on = { secs: 0, own: zero(), opp: zero() };
	const off = { secs: 0, own: zero(), opp: zero() };
	for (const s of stints.segs) {
		const side = s.on[team].includes(pid) ? on : off;
		side.secs += s.end - s.start;
		side.own = add(side.own, s.d[team]);
		side.opp = add(side.opp, s.d[team === 0 ? 1 : 0]);
	}
	return { on, off };
};

// A man's stretches on the floor, start to end in game seconds, each with
// his team's margin over it.
export const stintsOf = (stints: Stints, team: 0 | 1, pid: number) => {
	const out: { period: number; start: number; end: number; margin: number }[] =
		[];
	for (const s of stints.segs) {
		if (!s.on[team].includes(pid)) {
			continue;
		}
		const margin = s.d[team].pts - s.d[team === 0 ? 1 : 0].pts;
		const last = out.at(-1);
		if (
			last &&
			last.period === s.period &&
			Math.abs(last.end - s.start) < 0.5
		) {
			last.end = s.end;
			last.margin += margin;
		} else {
			out.push({ period: s.period, start: s.start, end: s.end, margin });
		}
	}
	return out;
};
