import { assert, beforeAll, describe, test } from "vitest";
import { KEY_PLAY_DEFAULTS, type KeySegment } from "../court3d/keyPlays.ts";
import {
	addReport,
	makeFollow,
	SYNC_BEHIND,
	type Follow,
	type FollowReport,
} from "../court3d/follow.ts";
import {
	compileField,
	isLineItem,
	targetForCursor,
	type FieldTimeline,
} from "./director.ts";
import { planFieldKeyPlays } from "./keyPlays.ts";
import {
	fieldStagedAs,
	makeFieldClock,
	onFieldCursor,
	stepField,
	type FieldClock,
} from "./playback.ts";
import { simGames, type SimGame } from "./testGames.ts";

// Two devices playing the same football game: one in charge of simming, the
// other watching it in 3D, told where the first one's field is once a
// second, each word reaching it 80-700ms later, stamped by a clock 40s off
// its own - the 3D court's (see court3d/playback.test.ts), for the field.

let game: SimGame;
let tl: FieldTimeline;

beforeAll(async () => {
	[game] = (await simGames([21])) as [SimGame];
	tl = compileField({ events: game.events, players: game.roster, gid: 1 });
}, 120_000);

type Device = {
	s: FieldClock;
	f: Follow;
	cursor: number;
	paused: boolean;
	skips: number;
	// The page's cursor once it has drawn again (it reaches the field a frame
	// later, as with React).
	next: number;
	along: boolean;
};

const device = (along: boolean): Device => ({
	s: makeFieldClock(false, 0),
	f: makeFollow(),
	cursor: 0,
	paused: false,
	skips: 0,
	next: 0,
	along,
});

const watch = ({
	rate = 1,
	minutes,
	joinAt = 0,
	pauseAt,
	skipAt,
	seconds,
	otherBuild,
	unsaid,
	silent,
}: {
	rate?: number;
	minutes?: number;
	joinAt?: number;
	pauseAt?: number;
	skipAt?: number;
	seconds?: number;
	// The watching device stages the game otherwise (another build there).
	otherBuild?: boolean;
	// The other device on a build that doesn't say what it staged - or,
	// older still, nothing of where its field is at all.
	unsaid?: boolean;
	silent?: boolean;
}) => {
	const { events } = game;
	const ftl = otherBuild
		? compileField({ events, players: game.roster, gid: 2 })
		: tl;
	const planOf = (tlx: FieldTimeline) =>
		minutes
			? planFieldKeyPlays(
					tlx,
					events,
					{ ...KEY_PLAY_DEFAULTS, on: true, minutes },
					{ rate, numPeriods: 4 },
				)
			: undefined;
	const plan: KeySegment[] | undefined = planOf(tl);
	const fPlan = ftl === tl ? plan : planOf(ftl);
	const n = events.length;
	// As the page does: on through the next line, with whatever came in
	// before it.
	const lineEnd = (c: number) => {
		let j = c;
		while (j < n && !isLineItem(events[j])) {
			j += 1;
		}
		return Math.min(n, j + 1);
	};
	// The watching device plays along until it finds it can't (see
	// Field3D's onLost) - then the page has it stepped line by line.
	let stepped = !!silent;
	let steppedAt: number | undefined = silent ? 0 : undefined;
	const frame = (d: Device, tlx: FieldTimeline, now: number, wall: number) => {
		const st = d.along && stepped;
		stepField(
			d.s,
			{
				cursor: d.cursor,
				paused: d.paused,
				rate,
				follower: st,
				along: d.along && !st,
				keyPlan: d.along ? (st ? undefined : fPlan) : plan,
				eventsLength: n,
				onReady: () => {
					if (!st) {
						d.next = lineEnd(d.cursor);
					}
				},
				onJump: (c) => {
					if (!st) {
						let x = Math.max(d.next, d.cursor);
						while (x < Math.min(n, c)) {
							x = lineEnd(x);
						}
						d.next = x;
					}
				},
			},
			tlx,
			now,
			d.f,
			wall,
		);
	};
	const render = (d: Device) => {
		if (
			d.next !== d.cursor ||
			d.s.prevPaused !== d.paused ||
			d.s.prevSkips !== d.skips
		) {
			d.cursor = d.next;
			onFieldCursor(d.s, d.along ? ftl : tl, {
				cursor: d.cursor,
				paused: d.paused,
				skips: d.skips,
				follower: d.along && stepped,
			});
		}
	};
	const b = device(false);
	const f = device(true);
	onFieldCursor(b.s, tl, { cursor: 0, paused: false, skips: 0 });
	onFieldCursor(f.s, ftl, { cursor: 0, paused: false, skips: 0 });

	// Where the other field was, show ms to timeline ms (and replays).
	const shown: { w: number; t: number; rp: number }[] = [];
	const inbox: { due: number; r: FollowReport }[] = [];
	let last:
		| { cursor: number; paused: boolean; w: number; at: number }
		| undefined;
	let sends = 0;
	const out: {
		now: number;
		dt: number;
		dw: number;
		gap: number;
		paused: boolean;
		t: number;
		w: number;
		rp: number;
		done: boolean;
		line: boolean;
	}[] = [];
	const end = (seconds ?? 3600) * 1000;
	// Each line's moment, when each field played through it (ms) - not cut
	// past, nor in a replay.
	const seenB = new Map<number, number>();
	const seenF = new Map<number, number>();
	const seen = (
		m: Map<number, number>,
		tlx: FieldTimeline,
		t0: number,
		s: FieldClock,
		now: number,
	) => {
		if (s.replay || s.t <= t0 || s.t - t0 > 2000) {
			return;
		}
		for (const bt of tlx.beats) {
			if (bt.actionStart > t0 && bt.actionStart <= s.t && !m.has(bt.i)) {
				m.set(bt.i, now);
			}
		}
	};
	let bNext = 0;
	let fNext = joinAt * 1000;
	let beat = 0;
	let k = 0;
	const SKEW = 40_000;
	while (Math.min(bNext, fNext) < end) {
		const now = Math.min(bNext, fNext, beat);
		if (now === beat) {
			// The heartbeat, as the page sends it (see LiveGame).
			const cursor = b.cursor;
			const changed =
				!last ||
				last.paused !== b.paused ||
				((last.cursor !== cursor || last.w !== b.s.w) && now - last.at >= 1000);
			if (changed || now - last!.at > 4000) {
				last = { cursor, paused: b.paused, w: b.s.w, at: now };
				sends += 1;
				const due = Math.max(
					inbox.at(-1)?.due ?? 0,
					now + 80 + ((sends * 7919) % 620),
				);
				inbox.push({
					due,
					r: {
						t: b.s.t,
						w: b.s.w,
						rp: b.s.replay?.at ?? -1,
						cursor,
						paused: b.paused,
						at: now + SKEW,
						recv: 0,
						...(unsaid ? {} : { fp: fieldStagedAs(tl) }),
					},
				});
			}
			beat += 200;
			continue;
		}
		while (inbox.length > 0 && inbox[0]!.due <= now) {
			const { r, due } = inbox.shift()!;
			if (due >= joinAt * 1000) {
				if (!silent) {
					addReport(f.f, { ...r, recv: due });
				}
				if (!stepped && r.fp !== undefined && r.fp !== fieldStagedAs(ftl)) {
					stepped = true;
					steppedAt = due;
				}
				if (stepped) {
					f.next = Math.max(f.next, r.cursor);
				}
			}
		}
		k += 1;
		const jitter = ((k * 2654435761) % 7) - 3;
		if (now === bNext) {
			b.paused =
				pauseAt !== undefined &&
				now >= pauseAt * 1000 &&
				now < (pauseAt + 8) * 1000;
			// "Next play" there, three times over.
			if (
				skipAt !== undefined &&
				b.skips < 3 &&
				now >= (skipAt + b.skips * 0.3) * 1000
			) {
				b.skips += 1;
				b.next = Math.min(n, lineEnd(b.cursor));
			}
			render(b);
			const bt = b.s.t;
			frame(b, tl, now, now + SKEW);
			// (The camera takes a cut as it draws the frame - see Field3D.)
			b.s.snapCam = false;
			seen(seenB, tl, bt, b.s, now);
			shown.push({ w: b.s.w, t: b.s.t, rp: b.s.replay?.at ?? -1 });
			render(b);
			bNext = now + 1000 / 60 + jitter;
		} else {
			const w0 = f.s.w;
			const dt = f.s.last === undefined ? 0 : Math.min(100, now - f.s.last);
			// (Waiting on its own page for the next line, as the other field
			// does there too.)
			const line = f.s.t >= targetForCursor(ftl, f.cursor);
			const ft = f.s.t;
			frame(f, ftl, now, now);
			f.s.snapCam = false;
			seen(seenF, ftl, ft, f.s, now);
			if (f.s.lost && !stepped) {
				stepped = true;
				steppedAt = now;
			}
			render(f);
			out.push({
				now,
				dt,
				dw: f.s.w - w0,
				gap: b.s.w - f.s.w,
				paused: b.paused,
				t: f.s.t,
				w: f.s.w,
				rp: f.s.replay?.at ?? -1,
				done: f.cursor >= n,
				line,
			});
			fNext = now + 1000 / 60 - jitter;
		}
		if (b.cursor >= n && f.cursor >= n && f.s.t >= tl.end - 1) {
			break;
		}
	}
	// What the other field showed at show ms `w`: its frames either side.
	const theirs = (w: number) => {
		let lo = 0;
		let hi = shown.length - 1;
		while (lo < hi) {
			const mid = (lo + hi + 1) >> 1;
			if (shown[mid]!.w <= w) {
				lo = mid;
			} else {
				hi = mid - 1;
			}
		}
		return [shown[lo]!, shown[lo + 1] ?? shown[lo]!];
	};
	return { out, theirs, seenB, seenF, steppedAt };
};

// How it looked: the frames after it had settled, where the picture stood
// still while the other one played on, any cut, and the gap.
const judge = (run: ReturnType<typeof watch>, from: number) => {
	const frames = run.out.filter((x) => x.now >= from && !x.done);
	let still = 0;
	let longest = 0;
	let held = 0;
	let cuts = 0;
	let off = 0;
	for (const x of frames) {
		if (x.dw <= 0 && !x.paused && !x.line && x.dt > 0) {
			still += 1;
			held += x.dt;
			longest = Math.max(longest, held);
		} else {
			held = 0;
		}
		if (x.dw < -1 || x.dw > 3 * Math.max(x.dt, 16)) {
			cuts += 1;
		}
		// Showing what the other field showed then.
		const same = run
			.theirs(x.w)
			.some(
				(them) =>
					them.rp >= 0 === x.rp >= 0 &&
					(x.rp >= 0 || Math.abs(them.t - x.t) <= 400 * 8),
			);
		if (!same) {
			off += 1;
		}
	}
	const gaps = frames.filter((x) => !x.paused).map((x) => x.gap);
	return {
		frames: frames.length,
		still,
		longest,
		cuts,
		off,
		minGap: Math.min(...gaps),
		maxGap: Math.max(...gaps),
	};
};

// What the watching field showed of what the other one did: each line's
// moment it played through - how many it never showed, and how long after
// the other field it showed the rest (ms).
const moments = (run: ReturnType<typeof watch>) => {
	let missing = 0;
	const lags: number[] = [];
	for (const [i, at] of run.seenB) {
		const f = run.seenF.get(i);
		if (f === undefined) {
			missing += 1;
		} else {
			lags.push(f - at);
		}
	}
	return {
		shown: run.seenB.size,
		missing,
		maxLag: Math.max(...lags),
	};
};

// The picture stood still while the other one played on (ms each time),
// longest first.
const freezes = (run: ReturnType<typeof watch>, from: number) => {
	const held: number[] = [];
	let h = 0;
	for (const x of run.out) {
		if (x.now < from || x.done) {
			continue;
		}
		if (x.dw <= 0 && !x.paused && x.dt > 0) {
			h += x.dt;
		} else {
			if (h > 0) {
				held.push(h);
			}
			h = 0;
		}
	}
	return held.sort((a, b) => b - a);
};

describe("watching another device's 3D football sim", () => {
	test("the game: never a stop, never a cut, a moment behind", () => {
		const run = watch({ seconds: 300 });
		const j = judge(run, 5000);
		assert.isAbove(j.frames, 15_000);
		assert.strictEqual(j.cuts, 0, JSON.stringify(j));
		assert.strictEqual(j.longest, 0, JSON.stringify(j));
		assert.isBelow(j.still / j.frames, 0.02, JSON.stringify(j));
		assert.isBelow(j.off / j.frames, 0.01, JSON.stringify(j));
		assert.isAbove(j.minGap, 0);
		assert.isBelow(j.maxGap, SYNC_BEHIND + 1500);
	}, 120_000);

	test("the key plays: on to each with it, not a burst at a time", () => {
		for (const [rate, minutes] of [
			[1, 2],
			[2, 5],
		] as const) {
			const run = watch({ rate, minutes });
			const j = judge(run, 5000);
			assert.isUndefined(run.steppedAt);
			assert.isAbove(j.frames, 3000);
			assert.strictEqual(j.cuts, 0, JSON.stringify(j));
			assert.strictEqual(j.longest, 0, JSON.stringify(j));
			assert.isBelow(j.off / j.frames, 0.01, JSON.stringify(j));
			assert.isBelow(j.maxGap, SYNC_BEHIND + 1500, JSON.stringify(j));
		}
	}, 120_000);

	test("it pauses: held on the same picture, and on with it after", () => {
		const run = watch({ seconds: 150, pauseAt: 60 });
		const j = judge(run, 5000);
		assert.strictEqual(j.cuts, 0, JSON.stringify(j));
		const held = run.out.filter((x) => x.now > 66_000 && x.now < 68_000);
		assert.isTrue(held.every((x) => x.dw === 0 && x.gap < 1));
		assert.isAtMost(j.longest, 2500, JSON.stringify(j));
	}, 120_000);

	test("joining late, or after a skip there: one cut, then with it", () => {
		const late = judge(watch({ seconds: 150, joinAt: 90 }), 95_000);
		assert.strictEqual(late.cuts, 0, JSON.stringify(late));
		assert.isBelow(late.off / late.frames, 0.01, JSON.stringify(late));
		const skip = judge(watch({ seconds: 150, skipAt: 60 }), 5000);
		assert.strictEqual(skip.cuts, 1, JSON.stringify(skip));
		assert.isBelow(skip.off / skip.frames, 0.01, JSON.stringify(skip));
	}, 120_000);

	test("a field says what it staged: the same game the same, another not", () => {
		const again = compileField({
			events: game.events,
			players: game.roster,
			gid: 1,
		});
		const other = compileField({
			events: game.events,
			players: game.roster,
			gid: 2,
		});
		assert.strictEqual(fieldStagedAs(again), fieldStagedAs(tl));
		assert.notStrictEqual(fieldStagedAs(other), fieldStagedAs(tl));
	}, 60_000);

	test("on another build: stepped line by line, never round the same stretch again", () => {
		for (const unsaid of [false, true]) {
			const run = watch({ seconds: 240, otherBuild: true, unsaid });
			const label = unsaid ? "unsaid" : "said";
			const at = run.steppedAt ?? Infinity;
			const back = (after: boolean) =>
				run.out.filter(
					(x, k) =>
						k > 0 && x.now > at === after && x.t < run.out[k - 1]!.t - 50,
				).length;
			assert.strictEqual(back(true), 0, label);
			assert.isAtMost(back(false), unsaid ? 3 : 0, label);
			if (!unsaid) {
				assert.isBelow(at, 3000, label);
			}
			const m = moments(run);
			assert.isAbove(m.shown, 20, label);
			assert.isBelow(m.maxLag, 5000, `${label} ${JSON.stringify(m)}`);
		}
	}, 240_000);

	// The device in charge of simming on a build that says nothing of where
	// its field is: stepped line by line from the start, on through each
	// line as the word of it comes - all of them, a moment behind, never
	// back over one, and not in fits and starts.
	test("an older build there: stepped, every line, never back", () => {
		const run = watch({ seconds: 240, silent: true });
		const back = run.out.filter(
			(x, k) => k > 0 && x.now > 0 && x.t < run.out[k - 1]!.t - 50,
		).length;
		assert.strictEqual(back, 0);
		const m = moments(run);
		assert.isAbove(m.shown, 20);
		assert.isAtMost(m.missing, 2, JSON.stringify(m));
		assert.isBelow(m.maxLag, 3000, JSON.stringify(m));
		const held = freezes(run, 5000);
		assert.isBelow(held[0] ?? 0, 2500, `${held.slice(0, 4)}`);
	}, 120_000);
});
