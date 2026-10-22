import { safeLocalStorage } from "../../../util/safeLocalStorage.ts";
import type { Beat, CourtTimeline } from "./director.ts";
import { fastAt } from "./evaluate.ts";
import { momentsOf } from "../winProb.ts";

// KEY PLAYS. A game watched as its big moments: a reel of them - the dunks,
// the threes, the blocks, the lead changing hands - in order, the score and
// the clock moving on between them, to a length the viewer picks. And if it
// is close at the end, every play of the finish, the way it happened - out
// of that length, or on top of it (so a close finish takes nothing from the
// rest of the game, and how quickly the early going passes never gives one
// away).
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
	// A close finish shown on top of the length asked, not out of it.
	closeOnTop: boolean;
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
	closeOnTop: false,
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
		closeOnTop: flag("closeOnTop"),
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
// after it is over - but, at least, this long after a basket goes down
// through the net.
const LEAD_MS = 4500;
const TAIL_MS = 900;
const NET_MS = 700;
// A play that moves the chance of winning by this much (0-1) makes the reel
// on that alone, weighed at this much a whole game's worth.
const SWING_MIN = 0.04;
const SWING_W = 40;
// Each play of a kind already in the reel counts for this much less than the
// last; one from a period with none in it yet, this much more.
const SAME_KIND = 0.82;
const NEW_PERIOD = 1.35;
// Two stretches this near each other (ms) run on as one.
const JOIN_MS = 1500;

const MADE = /^(fg|tp)/;
const isMade = (type: string) =>
	MADE.test(type) && !type.startsWith("fga") && !type.startsWith("fgaTp");
// Every shot comes as two lines: the attempt (the play that gets it up, and
// the ball on its way) and what came of it - in, blocked, the rebound.
const isAttempt = (type: string) => type.startsWith("fga");
// What leads into a play, in the run of it: the shot it came of, the miss
// or the block, the offensive board - all as much part of it as it is.
const LEADS_IN = /^(fga|miss|blk|orb)/;
// (Straight on from it: within this long, ms.)
const CHAIN_MS = 300;
// What a reel is made of: baskets, blocks, steals - not a free throw, a sub
// or a timeout.
const isPlay = (type: string) =>
	isMade(type) || type.startsWith("blk") || type === "stl";
// How long after the ball is in the air at the end of a clip it may yet take
// to come down to somebody (ms): no longer than this.
const SETTLE_MS = 4000;

// The ball through a stretch of the game: where it is at t, and when it is
// next in somebody's hands, or still.
const ballOf = (tl: CourtTimeline) => {
	const ball = tl.ball;
	const at = (t: number): number => {
		let lo = 0;
		let hi = ball.length - 1;
		let k = -1;
		while (lo <= hi) {
			const mid = (lo + hi) >> 1;
			if (ball[mid]!.t0 <= t) {
				k = mid;
				lo = mid + 1;
			} else {
				hi = mid - 1;
			}
		}
		return k;
	};
	// Up in the air or loose: shot, off the rim, bouncing.
	const inAir = (k: number) => {
		const s = ball[k];
		return (
			!!s &&
			(s.kind === "path" ||
				s.kind === "bounce" ||
				(s.kind === "fly" && !("pid" in s.to)))
		);
	};
	return {
		// When the ball, up at t, comes down: in somebody's hands, or still -
		// and when it last went down through the net on the way, if it did.
		settle: (t: number): { t: number; net?: number } => {
			let k = at(t);
			let net: number | undefined;
			while (inAir(k) && ball[k + 1] && ball[k + 1]!.t0 - t < SETTLE_MS) {
				const s = ball[k]!;
				if (s.kind === "path") {
					net = s.t1;
				}
				k++;
			}
			return { t: inAir(k) ? t : Math.max(t, ball[k]?.t0 ?? t), net };
		},
		// The ball up on its way from a shot at or after t, if any.
		shotFrom: (t: number): number | undefined => {
			for (let k = Math.max(0, at(t)); k < ball.length; k++) {
				const s = ball[k]!;
				if (s.t0 >= t - 1 && inAir(k)) {
					return s.t0;
				}
				if (s.t0 > t + 6000) {
					break;
				}
			}
			return undefined;
		},
		inAir: (t: number) => inAir(at(t)),
	};
};

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
		spread,
		edge,
	}: {
		rate: number;
		numPeriods: number;
		// The raw box score team (0, 1) of the viewer's own team, if playing.
		mine?: 0 | 1;
		// The pregame spread, the home team's (see Game.spread) - or, failing
		// it, home court's edge alone (points over a game; 0 on a neutral
		// floor).
		spread?: number;
		edge?: number;
	},
): KeySegment[] => {
	const beats = tl.beats;
	if (beats.length === 0) {
		return [{ t0: 0, t1: tl.end, cursor: 0, close: false }];
	}
	const typeOf = (b: Beat): string => {
		const e = events[b.i];
		return typeof e?.type === "string" ? e.type : b.type;
	};
	const ball = ballOf(tl);
	// A play's clip: from a few seconds before it - before the shot it came of,
	// if it is what came of one - to when the ball is down: through the net
	// and picked up to be taken out (and not on into that), a block run down.
	const clipOf = (k: number): { t0: number; t1: number } => {
		const b = beats[k]!;
		// (A putback: from the miss, the board and all.)
		let j = k;
		while (
			j > 0 &&
			k - j < 4 &&
			beats[j]!.preStart - beats[j - 1]!.end < CHAIN_MS &&
			LEADS_IN.test(typeOf(beats[j - 1]!)) &&
			!isAttempt(typeOf(beats[j]!))
		) {
			j--;
		}
		const from = beats[j]!;
		const t0 = Math.max(from.preStart, from.actionStart - LEAD_MS);
		let t1 = b.end + TAIL_MS;
		// (The shot that went in: off the attempt just before it - or, a
		// putback, straight up off the board.)
		const prev = beats[k - 1];
		const up = ball.shotFrom(
			prev && isAttempt(typeOf(prev)) ? prev.actionStart : b.preStart,
		);
		if (isMade(typeOf(b)) && up !== undefined) {
			const down = ball.settle(up);
			t1 = Math.max(Math.min(t1, down.t), (down.net ?? up) + NET_MS);
		} else {
			const down = ball.settle(b.actionStart);
			t1 = Math.max(t1, Math.min(down.t + 400, b.end + SETTLE_MS));
		}
		return { t0, t1 };
	};
	const before = momentsOf(events, {
		numPeriods,
		...(spread === undefined ? {} : { spread }),
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
	const candidates: {
		k: number;
		w: number;
		t0: number;
		t1: number;
		kind: string;
		period: number;
	}[] = [];
	// (And, should there be time for more than the big plays, the rest - the
	// ones that matter most first.)
	const rest: { k: number; w: number; t0: number; t1: number }[] = [];
	beats.forEach((b, k) => {
		if (close(b)) {
			return;
		}
		const e = events[b.i];
		const type = typeOf(b);
		const st = before[b.i];
		if (!st || !isPlay(type)) {
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
		const { t0, t1 } = clipOf(k);
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
		const kind = dunk
			? "dunk"
			: type === "tp" || type === "tpAndOne"
				? "three"
				: type.startsWith("blk")
					? "block"
					: type === "stl"
						? "steal"
						: "basket";
		candidates.push({ k, w, t0, t1, kind, period: st.period });
	});

	const budget = settings.minutes * 60_000 * Math.max(0.25, rate);
	let spent = 0;
	for (const p of merge(picked)) {
		spent += costOf(tl, p.t0, p.t1);
	}
	// (On top of it, a close finish: what it takes, the rest of the game
	// still has.)
	if (settings.closeOnTop) {
		for (const p of merge(picked.filter((x) => x.close))) {
			spent -= costOf(tl, p.t0, p.t1);
		}
	}
	// (Among equals, spread through the game rather than bunched.)
	candidates.sort(
		(a, b) => b.w - a.w || ((a.k * 7919) % 101) - ((b.k * 7919) % 101),
	);
	// The best of them one at a time: the more of a kind already in it, the
	// less the next of that kind counts, and a play from a period not yet
	// shown counts for more - the whole game in it, not five dunks from the
	// second quarter.
	const pool = candidates.map((c) => ({ ...c, cost: costOf(tl, c.t0, c.t1) }));
	const kinds = new Map<string, number>();
	const periods = new Set<number>();
	for (;;) {
		let best = -1;
		let value = -Infinity;
		pool.forEach((c, i) => {
			if (spent + c.cost > budget) {
				return;
			}
			const v =
				c.w *
				SAME_KIND ** (kinds.get(c.kind) ?? 0) *
				(periods.has(c.period) ? 1 : NEW_PERIOD);
			if (v > value) {
				value = v;
				best = i;
			}
		});
		if (best < 0) {
			break;
		}
		const c = pool.splice(best, 1)[0]!;
		spent += c.cost;
		picked.push({ t0: c.t0, t1: c.t1, close: false });
		kinds.set(c.kind, (kinds.get(c.kind) ?? 0) + 1);
		periods.add(c.period);
	}
	rest.sort((a, b) => b.w - a.w || a.k - b.k);
	for (const c of rest) {
		const cost = costOf(tl, c.t0, c.t1);
		if (spent + cost > budget) {
			continue;
		}
		spent += cost;
		picked.push({ t0: c.t0, t1: c.t1, close: false });
	}

	return merge(
		// (Never left with the ball in the air: on till it comes down.)
		merge(picked).map((p) => ({
			...p,
			t1: ball.inAir(p.t1) ? ball.settle(p.t1).t + 300 : p.t1,
		})),
	).map((p) => {
		// The page caught up to the last line before the stretch - and no
		// further: the stats after that line go in with the next one, the
		// key play's own, which would be up on the page before it happens.
		const k = beats.findIndex((b) => b.actionStart >= p.t0);
		return {
			...p,
			t1: Math.min(p.t1, tl.end),
			cursor: k < 0 ? events.length : k === 0 ? 0 : beats[k - 1]!.i + 1,
		};
	});
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
