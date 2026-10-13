import { safeLocalStorage } from "../../../util/safeLocalStorage.ts";
import type { Beat, CourtTimeline } from "./director.ts";
import { fastAt } from "./evaluate.ts";
import { momentsOf } from "../winProb.ts";

// KEY PLAYS. A game watched as its big moments: a reel of them - the dunks,
// the threes, the blocks, the lead changing hands - in order, the score and
// the clock moving on between them, to a length the viewer picks. And if it
// is close at the end, every play of the finish, the way it happened.
//
// Saved in this browser only, like the choice of picture (see setting.ts).

export type KeyPlaySettings = {
	on: boolean;
	// About how long (minutes) a whole game takes to watch, at the speed it
	// plays at.
	minutes: number;
	// Every play once it is within this many points with this many minutes
	// left in the fourth, or in overtime (either 0: never).
	closePoints: number;
	closeMinutes: number;
	// What makes the reel.
	dunks: boolean;
	threes: boolean;
	blocks: boolean;
	steals: boolean;
	andOnes: boolean;
	leadChanges: boolean;
	buzzer: boolean;
	baskets: boolean;
	// The plays that swing who is likely to win the most (see winProb.ts).
	swings: boolean;
	// Your own team's plays before anybody else's.
	mine: boolean;
};

export const KEY_PLAY_DEFAULTS: KeyPlaySettings = {
	on: false,
	minutes: 3,
	closePoints: 8,
	closeMinutes: 3,
	dunks: true,
	threes: true,
	blocks: true,
	steals: true,
	andOnes: true,
	leadChanges: true,
	buzzer: true,
	baskets: true,
	swings: true,
	mine: true,
};

// The lengths (minutes) offered to pick from.
export const KEY_PLAY_PRESETS = [2, 3, 5, 10, 15] as const;

export const KEY_PLAY_LIMITS = {
	minutes: [1, 30],
	closePoints: [0, 30],
	closeMinutes: [0, 12],
} as const;

const KEY = "bbgmKeyPlays";

const clampTo = (
	v: unknown,
	[lo, hi]: readonly [number, number],
	or: number,
) => {
	const n = typeof v === "number" ? v : Number(v);
	return Number.isFinite(n) ? Math.min(hi, Math.max(lo, n)) : or;
};

export const parseKeyPlays = (value: unknown): KeyPlaySettings => {
	const d = KEY_PLAY_DEFAULTS;
	const o =
		value && typeof value === "object"
			? (value as Record<string, unknown>)
			: {};
	const flag = (k: keyof KeyPlaySettings) =>
		typeof o[k] === "boolean" ? (o[k] as boolean) : (d[k] as boolean);
	return {
		on: flag("on"),
		minutes: clampTo(o.minutes, KEY_PLAY_LIMITS.minutes, d.minutes),
		closePoints: Math.round(
			clampTo(o.closePoints, KEY_PLAY_LIMITS.closePoints, d.closePoints),
		),
		closeMinutes: clampTo(
			o.closeMinutes,
			KEY_PLAY_LIMITS.closeMinutes,
			d.closeMinutes,
		),
		dunks: flag("dunks"),
		threes: flag("threes"),
		blocks: flag("blocks"),
		steals: flag("steals"),
		andOnes: flag("andOnes"),
		leadChanges: flag("leadChanges"),
		buzzer: flag("buzzer"),
		baskets: flag("baskets"),
		swings: flag("swings"),
		mine: flag("mine"),
	};
};

export const getKeyPlays = (): KeyPlaySettings => {
	try {
		const raw = safeLocalStorage.getItem(KEY);
		return parseKeyPlays(raw ? JSON.parse(raw) : undefined);
	} catch {
		return { ...KEY_PLAY_DEFAULTS };
	}
};

export const setKeyPlays = (settings: KeyPlaySettings) => {
	safeLocalStorage.setItem(KEY, JSON.stringify(parseKeyPlays(settings)));
};

// A stretch of the game shown: from t0 to t1 on the timeline, the play-by-play
// caught up to `cursor` (events) as it starts. `close`: part of a close
// finish, shown play by play.
export type KeySegment = {
	t0: number;
	t1: number;
	cursor: number;
	close: boolean;
};

// Into each play from this long (ms) before it happens, and on for this long
// after it is over.
const LEAD_MS = 4500;
const TAIL_MS = 900;
// A play that moves the chance of winning by this much (0-1) makes the reel
// on that alone, weighed at this much a whole game's worth.
const SWING_MIN = 0.04;
const SWING_W = 40;
// Two stretches this near each other (ms) run on as one.
const JOIN_MS = 1500;

const MADE = /^(fg|tp)/;
const isMade = (type: string) =>
	MADE.test(type) && !type.startsWith("fga") && !type.startsWith("fgaTp");

// How long a stretch takes to watch at 1x (ms): the dead time in it runs
// fast (see fastAt).
const costOf = (tl: CourtTimeline, t0: number, t1: number): number => {
	let ms = 0;
	for (let t = t0; t < t1; t += 50) {
		ms += Math.min(50, t1 - t) / fastAt(tl, t);
	}
	return ms;
};

export const planKeyPlays = (
	tl: CourtTimeline,
	events: any[],
	settings: KeyPlaySettings,
	{
		rate,
		numPeriods,
		mine,
		edge,
	}: {
		rate: number;
		numPeriods: number;
		// The raw box score team (0, 1) of the viewer's own team, if playing.
		mine?: 0 | 1;
		// The home team's edge (points over a game; 0 on a neutral floor).
		edge?: number;
	},
): KeySegment[] => {
	const beats = tl.beats;
	if (beats.length === 0) {
		return [{ t0: 0, t1: tl.end, cursor: 0, close: false }];
	}
	const before = momentsOf(events, {
		numPeriods,
		...(edge === undefined ? {} : { edge }),
	});
	// How it stands once the play and what it brings are done: just before
	// the next line.
	const after = (k: number) =>
		before[beats[k + 1]?.i ?? events.length] ?? before[beats[k]!.i]!;
	const lead = (s: [number, number]) => Math.sign(s[0] - s[1]);

	const close = (b: Beat): boolean => {
		const st = before[b.i];
		return (
			!!st &&
			settings.closePoints > 0 &&
			settings.closeMinutes > 0 &&
			st.period >= numPeriods &&
			st.clock <= settings.closeMinutes * 60 &&
			Math.abs(st.score[0] - st.score[1]) <= settings.closePoints
		);
	};

	const picked: { t0: number; t1: number; close: boolean }[] = [];
	// The tip, and the end of the game.
	const first = beats[0]!;
	picked.push({ t0: 0, t1: first.end + TAIL_MS, close: false });
	const last = beats.at(-1)!;
	picked.push({
		t0: Math.max(last.preStart, last.actionStart - LEAD_MS),
		t1: tl.end,
		close: false,
	});
	// Every play of a close finish: from the end of the play before it.
	beats.forEach((b, k) => {
		if (close(b)) {
			picked.push({
				t0: Math.min(b.preStart, beats[k - 1]?.end ?? b.preStart),
				t1: b.end + TAIL_MS,
				close: true,
			});
		}
	});

	// The rest of the time, the best of the big plays.
	const fxIn = (b: Beat, kind: string) =>
		tl.fx.find(
			(f) => f.kind === kind && f.t >= b.preStart - 1 && f.t <= b.end + 1,
		);
	const candidates: { k: number; w: number; t0: number; t1: number }[] = [];
	// (And, should there be time for more than the big plays, the rest - the
	// ones that matter most first.)
	const rest: { k: number; w: number; t0: number; t1: number }[] = [];
	beats.forEach((b, k) => {
		if (close(b)) {
			return;
		}
		const e = events[b.i];
		const type: string = typeof e?.type === "string" ? e.type : b.type;
		const st = before[b.i];
		if (!st) {
			return;
		}
		let w = 0;
		const made = isMade(type);
		const dunk = made ? fxIn(b, "dunk") : undefined;
		if (settings.dunks && dunk) {
			w += dunk.big ? 5 : 4;
		}
		if (settings.threes && (type === "tp" || type === "tpAndOne")) {
			w += 3;
		}
		if (settings.andOnes && type.endsWith("AndOne")) {
			w += 3;
		}
		if (settings.blocks && type.startsWith("blk")) {
			w += 3;
		}
		if (settings.steals && type === "stl") {
			w += 2;
		}
		if (made) {
			const was = lead(st.score);
			const now = lead(after(k).score);
			if (settings.leadChanges && was !== 0 && now === -was) {
				w += 3;
			} else if (settings.leadChanges && now === 0) {
				w += 1;
			}
			if (settings.buzzer && st.clock <= 2) {
				w += 5;
			}
			if (settings.baskets && w === 0) {
				w += 1;
			}
		}
		// However big a play it is otherwise, how much it changes who is
		// likely to win - a few points early in the game, little; the shot
		// that wins it, everything.
		if (settings.swings) {
			const swing = Math.abs(after(k).p0 - st.p0);
			if (swing >= SWING_MIN) {
				w += swing * SWING_W;
			}
		}
		const t0 = Math.max(b.preStart, b.actionStart - LEAD_MS);
		const t1 = Math.min(b.end + TAIL_MS, beats[k + 1]?.actionStart ?? tl.end);
		if (w === 0) {
			rest.push({ k, w: Math.abs(after(k).p0 - st.p0), t0, t1 });
			return;
		}
		// Later and closer, more.
		if (st.period >= numPeriods) {
			w += 1;
		}
		if (Math.abs(st.score[0] - st.score[1]) <= 6) {
			w += 0.5;
		}
		if (settings.mine && mine !== undefined && e?.t === mine) {
			w += 1.5;
		}
		candidates.push({ k, w, t0, t1 });
	});

	const budget = settings.minutes * 60_000 * Math.max(0.25, rate);
	let spent = 0;
	for (const p of merge(picked)) {
		spent += costOf(tl, p.t0, p.t1);
	}
	// (Among equals, spread through the game rather than bunched.)
	candidates.sort(
		(a, b) => b.w - a.w || ((a.k * 7919) % 101) - ((b.k * 7919) % 101),
	);
	rest.sort((a, b) => b.w - a.w || a.k - b.k);
	for (const c of [...candidates, ...rest]) {
		const cost = costOf(tl, c.t0, c.t1);
		if (spent + cost > budget) {
			continue;
		}
		spent += cost;
		picked.push({ t0: c.t0, t1: c.t1, close: false });
	}

	return merge(picked).map((p) => ({
		...p,
		t1: Math.min(p.t1, tl.end),
		cursor: beats.find((b) => b.actionStart >= p.t0)?.i ?? events.length,
	}));
};

const merge = <T extends { t0: number; t1: number; close: boolean }>(
	list: T[],
): { t0: number; t1: number; close: boolean }[] => {
	const out: { t0: number; t1: number; close: boolean }[] = [];
	for (const p of [...list].sort((a, b) => a.t0 - b.t0)) {
		const prev = out.at(-1);
		if (prev && p.t0 <= prev.t1 + JOIN_MS) {
			prev.t1 = Math.max(prev.t1, p.t1);
			prev.close ||= p.close;
		} else {
			out.push({ t0: p.t0, t1: p.t1, close: p.close });
		}
	}
	return out;
};

// The stretch shown at t, if any, and the next one after it.
export const segmentAt = (
	plan: KeySegment[],
	t: number,
): { at?: KeySegment; next?: KeySegment } => {
	for (const s of plan) {
		if (t < s.t0) {
			return { next: s };
		}
		if (t < s.t1) {
			return { at: s };
		}
	}
	return {};
};
