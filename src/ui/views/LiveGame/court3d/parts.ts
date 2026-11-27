import type { CheckIn, CourtTimeline, Track } from "./director.ts";
import type { Beat } from "./timeline.ts";

// THE GAME A PERIOD AT A TIME. A whole game took a phone minutes to stage -
// the new possession sim three and more - with the pregame show up all that
// while, and the work held hundreds of megabytes at once: enough, with the
// page's own, for a phone to put the whole tab down. So a live game is
// staged in parts, a period each (see partsOf): the first is played as soon
// as it is there, the rest staged while it plays, each put on the end of the
// game so far (mergeCourt) - a part a quarter of the work, and of the
// memory.
//
// Each part's director goes through the play-by-play from the start, the
// same way it would for the whole game (it only follows the lines: that is
// quick), and so stands exactly where the whole game's would at the part's
// start: the same men on, the same score, the same moment. What it put down
// before then is cut away (trimBefore) - all but where each man is, the
// ball, who has it - and only the part's own is finished (staged a step at a
// time, kept apart, and the rest - the director's `finish`, the most of the
// work by far). A part starts once a break between periods is over: everyone
// stood in the huddles or on the bench, the ball with whoever took it there,
// the next line the period's first.

// The lines a part stages: events `from` to `to` (inclusive).
export type CourtPart = { from: number; to: number };

type Ev = { type?: unknown; clipGap?: unknown; clipStart?: unknown };

// A part a period: each through the break that ends it - the line that
// starts the next period, the huddles - the next from the line after that.
// (A reel of highlights is staged whole: its clips are cut together.)
export const partsOf = (events: readonly (Ev | undefined)[]): CourtPart[] => {
	const last = events.length - 1;
	if (events.some((e) => e?.clipStart === true)) {
		return [{ from: 0, to: last }];
	}
	const parts: CourtPart[] = [];
	let from = 0;
	events.forEach((e, i) => {
		if (
			(e?.type === "period" || e?.type === "overtime") &&
			e.clipGap !== true &&
			i < last
		) {
			parts.push({ from, to: i });
			from = i + 1;
		}
	});
	parts.push({ from, to: last });
	return parts;
};

// WHAT A PART'S DIRECTOR KEEPS FROM BEFORE IT STARTS, AND WHAT IT CUTS. What
// stands as of now - who is where, the score, the clock, whose ball, the
// lines to come - is kept as it is; everything put down along the way is cut
// back to what goes on from `at` on (and, for what says how something stands
// from a moment on, what stood at `at`). A field nobody has said which it is
// is taken for a list of what happened when, and cut by its times (see
// keeps) - see parts.test.ts, which says when one turns up.
export const KEPT = new Set([
	"T",
	"rng",
	"pos",
	"face",
	"free",
	"makesInARow",
	"team",
	"rank",
	"skills",
	"rated",
	"dunkTurns",
	"deepTurns",
	"seat",
	"lineup",
	"replaced",
	"periodNo",
	"gameSecs",
	"clockSeen",
	"stint",
	"overtime",
	"introKind",
	"stakes",
	"introOrder",
	"lineupsFrom",
	"holder",
	"ballHand",
	"ballHandOf",
	"ballAt",
	"official",
	"backFrom",
	"offense",
	"phase",
	"inboundAt",
	"motion",
	"motionTeam",
	"spotFit",
	"lastClock",
	"postUps",
	"quickIn",
	"pending",
	"score",
	"guarding",
	"marking",
	"spacing",
	"detours",
	"directed",
	"backingDown",
	"jostles",
	"flyBy",
	"events",
	"gid",
	"gender",
	"step",
	"possession",
	"made",
	"lastFtShooter",
	"outOffMiss",
	"interview",
]);
// Stretches, from [0] to [1].
const SPANS = new Set([
	"fast",
	"liveFast",
	"jumps",
	"planned",
	"meets",
	"meeting",
]);
// Cut away whole.
const DROPPED = new Set(["intro"]);
// Known lists of what happened when (as against something new: see
// parts.test.ts).
export const CUT = new Set([
	"tracks",
	"ball",
	"poss",
	"tension",
	"seats",
	...SPANS,
	...DROPPED,
	"fx",
	"beats",
	"cuts",
	"clips",
	"breaks",
	"checkIns",
	"sitDowns",
	"huddles",
	"clears",
	"shots",
	"officialStands",
	"turns",
	"conceded",
	"fakes",
	"missed",
	"closeMissed",
	"walls",
	"closeUps",
	"hurts",
	"screens",
	"shotFouls",
	"guards",
]);

type Timed = Record<string, unknown>;
const num = (v: unknown): number | undefined =>
	typeof v === "number" ? v : undefined;
// When something starts, and when it is over (its start, if it is a moment).
const startOf = (x: unknown): number | undefined => {
	if (typeof x === "number") {
		return x;
	}
	if (Array.isArray(x)) {
		return num(x[0]);
	}
	if (x && typeof x === "object") {
		const o = x as Timed;
		return (
			num(o.t0) ??
			num(o.t) ??
			num(o.preStart) ??
			num(o.release) ??
			num(o.called)
		);
	}
	return undefined;
};
const endOf = (x: unknown, span: boolean): number | undefined => {
	if (Array.isArray(x)) {
		return span ? (num(x[1]) ?? num(x[0])) : num(x[0]);
	}
	if (x && typeof x === "object") {
		const o = x as Timed;
		return num(o.t1) ?? num(o.end) ?? num(o.off) ?? num(o.landed) ?? startOf(x);
	}
	return startOf(x);
};
// Still going on at `at`, or after it - or the last put down before it
// (`last`): what the director looks back at as it goes on (the beat before,
// a man's last run, who has the ball) is there as it was. (Nothing to go
// by: kept.)
const keeps = (
	x: unknown,
	at: number,
	span: boolean,
	last: boolean,
): boolean => {
	if (last) {
		return true;
	}
	const s = startOf(x);
	if (s === undefined) {
		return true;
	}
	return s >= at || (endOf(x, span) ?? s) >= at;
};
const cutList = <T>(list: T[], at: number, span = false): T[] =>
	list.filter((x, i) => keeps(x, at, span, i === list.length - 1));

const cutTrack = (tr: Track, at: number, where: unknown): void => {
	tr.moves = cutList(tr.moves, at);
	tr.acts = cutList(tr.acts, at);
	tr.arms = cutList(tr.arms, at);
	tr.faces = cutList(tr.faces, at);
	tr.looks = cutList(tr.looks, at);
	tr.shown = cutList(tr.shown, at);
	if (tr.nudges) {
		tr.nudges = cutList(tr.nudges, at);
	}
	if (tr.sep) {
		tr.sep = cutList(tr.sep, at);
	}
	// (Before the first of his runs kept, where it starts from.)
	const first = tr.moves[0]?.from;
	const p = (first ?? where) as { x: number; y: number } | undefined;
	if (p && typeof p.x === "number" && typeof p.y === "number") {
		tr.start = { x: p.x, y: p.y };
	}
};

const BREAK = /^(endOfPeriod|period|overtime)$/;

// The fields met that neither list names (see parts.test.ts).
export const unsorted = new Set<string>();

// A director that went through the lines up to a part's start (at `at`, the
// moment it stood at then) cut back to what the part takes up. Said back,
// for each man: when what it kept of his runs is over (at `at` at the
// latest) - from when on his are the part's own (see mergeCourt).
export const trimBefore = (
	director: object,
	at: number,
): Map<number, number> => {
	const own = new Map<number, number>();
	const d = director as Record<string, unknown>;
	// (No further back than the break the part starts out of: from the end
	// of the last play before it.)
	let brk = at;
	const beats = Array.isArray(d.beats) ? (d.beats as Beat[]) : [];
	for (let k = beats.length - 1; k >= 0; k--) {
		const b = beats[k]!;
		if (!BREAK.test(b.type)) {
			brk = Math.min(at, b.end);
			break;
		}
	}
	for (const key of Object.keys(d)) {
		if (KEPT.has(key)) {
			continue;
		}
		if (!CUT.has(key)) {
			unsorted.add(key);
		}
		const v = d[key];
		if (DROPPED.has(key)) {
			d[key] = undefined;
		} else if (key === "tracks" && v instanceof Map) {
			const pos = d.pos instanceof Map ? d.pos : undefined;
			for (const [pid, tr] of v as Map<number, Track>) {
				cutTrack(tr, at, pos?.get(pid));
				let over = -Infinity;
				for (const m of tr.moves) {
					over = Math.max(over, m.t1);
				}
				own.set(pid, Math.max(brk, Math.min(at, over)));
			}
		} else if (Array.isArray(v)) {
			d[key] = cutList(v, at, SPANS.has(key));
		} else if (v instanceof Map) {
			for (const [k, list] of v) {
				if (Array.isArray(list)) {
					v.set(k, cutList(list, at, SPANS.has(key)));
				}
			}
		}
	}
	return own;
};

// THE GAME SO FAR, AND THE NEXT PART ON THE END OF IT (`next` staged from
// `next.from` on). Whatever either has the other side of the join is left
// out: up to the join the game so far, from it on the part.
export type Staged = CourtTimeline & {
	// The moment this part takes over (none: from the start).
	from?: number;
	// How many of the game's events it is staged through (none: all).
	upTo?: number;
	// Where the first of a game's parts ends (none: staged whole) - all two
	// courts staging the same game have both had, however far on either is
	// (see follow.ts's stagedAs).
	opening?: number;
	// Which staging it is (see stageCourt.ts): the same, each part on, for
	// the game so far.
	staging?: number;
	// A part: from when on each man's is its own (see trimBefore) - what it
	// lays for its own first lines can go back into the break before it (a
	// sub's walk to the table, the men up out of the huddles), what came
	// before then the game so far's.
	ownFrom?: Map<number, number>;
	// The game so far: where each part went on (`at`), and from when what
	// it brought differs from what was there before it (see Court3D's
	// going on into the next part).
	seams?: { at: number; since: number }[];
};

// The same game staged, further on - or as far (see stageCourt.ts).
export const goesOn = (a: Staged, b: Staged): boolean =>
	a === b || (a.staging !== undefined && a.staging === b.staging);

const before = <T>(list: T[] | undefined, at: number): T[] =>
	(list ?? []).filter((x) => (startOf(x) ?? -Infinity) < at);
const from = <T>(list: T[] | undefined, at: number): T[] =>
	(list ?? []).filter((x) => (startOf(x) ?? Infinity) >= at);
const joined = <T>(a: T[] | undefined, b: T[] | undefined, at: number): T[] => [
	...before(a, at),
	...from(b, at),
];
// (A stretch either side of the join: cut at it.)
const joinedSpans = (
	a: [number, number][] | undefined,
	b: [number, number][] | undefined,
	at: number,
): [number, number][] => [
	...(a ?? [])
		.filter(([s]) => s < at)
		.map(([s, e]): [number, number] => [s, Math.min(e, at)]),
	...(b ?? [])
		.filter(([, e]) => e > at)
		.map(([s, e]): [number, number] => [Math.max(s, at), e]),
];
// How things stood, on from the join: what stood as the part began, as of
// the join.
const joinedStanding = <T>(
	a: [number, T][] | undefined,
	b: [number, T][] | undefined,
	at: number,
): [number, T][] => {
	const out = before(a, at);
	const rest = b ?? [];
	const k = rest.findIndex(([t]) => t >= at);
	const first = k < 0 ? rest.at(-1) : k > 0 ? rest[k - 1] : undefined;
	if (first) {
		out.push([at, first[1]]);
	}
	return [...out, ...(k < 0 ? [] : rest.slice(k))];
};

// A man's runs and the rest: up to `own` (his own in the part from then on:
// see trimBefore - the join at the latest) the game so far's, the part's
// from then on. Said back with it: the first moment that differs from the
// game so far (none: Infinity).
const joinTrack = (
	a: Track | undefined,
	b: Track | undefined,
	own: number,
	at: number,
): { tr: Track; since: number } => {
	if (!a || !b) {
		const t = (a ?? b)!;
		return {
			tr: {
				...t,
				moves: [...t.moves],
				acts: [...t.acts],
				arms: [...t.arms],
				faces: [...t.faces],
				looks: [...t.looks],
				shown: [...t.shown],
			},
			since: Infinity,
		};
	}
	const c = Math.min(own, at);
	const tr: Track = {
		pid: a.pid,
		team: a.team,
		start: a.start,
		moves: joined(a.moves, b.moves, c),
		acts: joined(a.acts, b.acts, c),
		arms: joined(a.arms, b.arms, c),
		faces: joinedStanding(a.faces, b.faces, c),
		looks: joinedStanding(a.looks, b.looks, c),
		shown: joinedStanding(a.shown, b.shown, c),
	};
	if (a.nudges || b.nudges) {
		tr.nudges = joined(a.nudges, b.nudges, c);
	}
	if (a.sep || b.sep) {
		tr.sep = joined(a.sep, b.sep, c);
	}
	// (Between the two, what is the part's - or was the game so far's, and
	// is not now.)
	let since = Infinity;
	if (c < at) {
		const lists: (keyof Track)[] = [
			"moves",
			"acts",
			"arms",
			"faces",
			"looks",
			"shown",
			"nudges",
			"sep",
		];
		for (const key of lists) {
			for (const side of [a, b]) {
				for (const x of (side[key] as unknown[] | undefined) ?? []) {
					const t = startOf(x);
					if (t !== undefined && t >= c && t < at) {
						since = Math.min(since, t);
					}
				}
			}
		}
	}
	return { tr, since };
};

// Who checks in in the break before it, the part has (see ownFrom): the game
// so far's, and any of the part's it hasn't.
const joinedCheckIns = (
	a: CheckIn[],
	b: CheckIn[],
	at: number,
): { list: CheckIn[]; since: number } => {
	const had = new Set(a.map((c) => `${c.pid}|${Math.round(c.t0)}`));
	const early = b.filter(
		(c) => c.t0 < at && !had.has(`${c.pid}|${Math.round(c.t0)}`),
	);
	return {
		list: [...before(a, at), ...early, ...from(b, at)],
		since: Math.min(Infinity, ...early.map((c) => c.t0)),
	};
};

export const mergeCourt = (a: Staged, b: Staged): Staged => {
	const at = b.from ?? a.end;
	const tracks = new Map<number, Track>();
	let since = Infinity;
	for (const pid of new Set([...a.tracks.keys(), ...b.tracks.keys()])) {
		const j = joinTrack(
			a.tracks.get(pid),
			b.tracks.get(pid),
			b.ownFrom?.get(pid) ?? at,
			at,
		);
		tracks.set(pid, j.tr);
		since = Math.min(since, j.since);
	}
	const checkIns = joinedCheckIns(a.checkIns ?? [], b.checkIns ?? [], at);
	since = Math.min(since, checkIns.since, at);
	const out: Staged = {
		...a,
		tracks,
		...(a.dribbleLow || b.dribbleLow
			? {
					dribbleLow: new Map([
						...(b.dribbleLow ?? []),
						...(a.dribbleLow ?? []),
					]),
				}
			: {}),
		...(a.passEyes || b.passEyes
			? { passEyes: new Map([...(b.passEyes ?? []), ...(a.passEyes ?? [])]) }
			: {}),
		...(a.gaits || b.gaits
			? { gaits: new Map([...(b.gaits ?? []), ...(a.gaits ?? [])]) }
			: {}),
		ball: joined(a.ball, b.ball, at),
		fx: joined(a.fx, b.fx, at),
		beats: joined(a.beats, b.beats, at),
		poss: joined(a.poss, b.poss, at),
		cuts: joined(a.cuts, b.cuts, at),
		clips: joined(a.clips, b.clips, at),
		jumps: joinedSpans(a.jumps, b.jumps, at),
		shots: joined(a.shots, b.shots, at),
		tension: joinedStanding(a.tension, b.tension, at),
		seats: joinedStanding(a.seats, b.seats, at),
		fast: joinedSpans(a.fast, b.fast, at),
		calm: joinedSpans(a.calm, b.calm, at),
		checkIns: checkIns.list,
		sitDowns: joined(a.sitDowns, b.sitDowns, at),
		huddles: joined(a.huddles, b.huddles, at),
		closeUps: joined(a.closeUps, b.closeUps, at),
		hurts: joined(a.hurts, b.hurts, at),
		screens: joined(a.screens, b.screens, at),
		end: Math.max(at, b.end),
	};
	if (b.interview) {
		out.interview = b.interview;
	}
	out.seams = [...(a.seams ?? []), { at, since }];
	delete out.ownFrom;
	if (b.upTo === undefined) {
		delete out.upTo;
	} else {
		out.upTo = b.upTo;
	}
	delete out.from;
	return out;
};
