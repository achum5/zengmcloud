import { assert, describe, test } from "vitest";
import { compile } from "./testGame.ts";
import {
	KEY_PLAY_DEFAULTS,
	planKeyPlays,
	type KeySegment,
} from "./keyPlays.ts";
import {
	addReport,
	makeFollow,
	SYNC_BEHIND,
	type Follow,
	type FollowReport,
} from "./follow.ts";
import { onCursor, stepPlay, type PlayClock } from "./playback.ts";
import { targetForCursor, type CourtTimeline } from "./director.ts";

// Two devices playing the same game: one in charge of simming, the other
// watching it in 3D, told where the first one's court is once a second,
// each word reaching it 80-700ms later, stamped by a clock 40s off its own.

type Device = {
	s: PlayClock;
	f: Follow;
	cursor: number;
	paused: boolean;
	skips: number;
	// The page's cursor once it has drawn again (it reaches the court a frame
	// later, as with React).
	next: number;
	along: boolean;
};

const device = (along: boolean): Device => ({
	s: {
		t: 0,
		w: 0,
		pace: 1,
		paceAt: -1,
		lead: 0,
		last: undefined,
		replay: undefined,
		nextDunk: 0,
		snapCam: true,
		jumpTo: -1,
		keyDip: -Infinity,
		keyHold: -Infinity,
		readyFor: -1,
		stepping: false,
		prevCursor: -1,
		prevPaused: false,
		prevSkips: 0,
	},
	f: makeFollow(),
	cursor: 0,
	paused: false,
	skips: 0,
	next: 0,
	along,
});

const watch = ({
	seed,
	possessions = 140,
	rate = 1,
	minutes,
	joinAt = 0,
	pauseAt,
	skipAt,
	seconds,
}: {
	seed: string;
	possessions?: number;
	rate?: number;
	minutes?: number;
	joinAt?: number;
	pauseAt?: number;
	skipAt?: number;
	seconds?: number;
}) => {
	const { tl, events } = compile(seed, possessions);
	const plan: KeySegment[] | undefined = minutes
		? planKeyPlays(
				tl,
				events,
				{ ...KEY_PLAY_DEFAULTS, on: true, minutes },
				{ rate, numPeriods: 4 },
			)
		: undefined;
	const n = events.length;
	const frame = (d: Device, tlx: CourtTimeline, now: number, wall: number) => {
		stepPlay(
			d.s,
			{
				cursor: d.cursor,
				paused: d.paused,
				rate,
				follower: false,
				along: d.along,
				keyPlan: plan,
				eventsLength: n,
				onReady: () => {
					d.next = Math.min(n, d.cursor + 1);
				},
				onJump: (c) => {
					d.next = Math.max(d.next, Math.min(n, c));
				},
			},
			d.f,
			tlx,
			false,
			now,
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
			onCursor(d.s, tl, {
				cursor: d.cursor,
				paused: d.paused,
				skips: d.skips,
				follower: false,
			});
		}
	};
	const b = device(false);
	const f = device(true);
	onCursor(b.s, tl, { cursor: 0, paused: false, skips: 0, follower: false });
	onCursor(f.s, tl, { cursor: 0, paused: false, skips: 0, follower: false });

	// Where the other court was, show ms to timeline ms (and replays).
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
					},
				});
			}
			beat += 200;
			continue;
		}
		while (inbox.length > 0 && inbox[0]!.due <= now) {
			const { r, due } = inbox.shift()!;
			if (due >= joinAt * 1000) {
				addReport(f.f, { ...r, recv: due });
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
				b.next = Math.min(n, b.cursor + 1);
			}
			render(b);
			frame(b, tl, now, now + SKEW);
			shown.push({ w: b.s.w, t: b.s.t, rp: b.s.replay?.at ?? -1 });
			render(b);
			bNext = now + 1000 / 60 + jitter;
		} else {
			const w0 = f.s.w;
			const dt = f.s.last === undefined ? 0 : Math.min(100, now - f.s.last);
			// (Waiting on its own page for the next line, as the other court
			// does there too - the opening tip's dozen lines, say.)
			const line = f.s.t >= targetForCursor(tl, f.cursor);
			frame(f, tl, now, now);
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
	// What the other court showed at show ms `w`: its frames either side.
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
	return { out, theirs, tl, plan };
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
		// Showing what the other court showed then.
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

describe("watching another device's 3D sim", () => {
	test("the whole game: never a stop, never a cut, a second behind", () => {
		const run = watch({ seed: "along", seconds: 600 });
		const j = judge(run, 5000);
		assert.isAbove(j.frames, 20_000);
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
			const run = watch({ seed: "along", rate, minutes });
			const j = judge(run, 5000);
			assert.isAbove(j.frames, 3000);
			assert.strictEqual(j.cuts, 0, JSON.stringify(j));
			assert.strictEqual(j.longest, 0, JSON.stringify(j));
			assert.isBelow(j.off / j.frames, 0.01, JSON.stringify(j));
			assert.isBelow(j.maxGap, SYNC_BEHIND + 1500, JSON.stringify(j));
		}
	}, 120_000);

	test("it pauses: held on the same picture, and on with it after", () => {
		const run = watch({ seed: "along", seconds: 150, pauseAt: 60 });
		const j = judge(run, 5000);
		assert.strictEqual(j.cuts, 0, JSON.stringify(j));
		// Stopped on the very picture it stopped on.
		const held = run.out.filter((x) => x.now > 66_000 && x.now < 68_000);
		assert.isTrue(held.every((x) => x.dw === 0 && x.gap < 1));
		assert.isAtMost(j.longest, 2500, JSON.stringify(j));
	}, 120_000);

	test("joining late, or after a skip there: one cut, then with it", () => {
		const late = judge(
			watch({ seed: "along", seconds: 150, joinAt: 90 }),
			95_000,
		);
		assert.strictEqual(late.cuts, 0, JSON.stringify(late));
		assert.isBelow(late.off / late.frames, 0.01, JSON.stringify(late));
		const run = watch({ seed: "along", seconds: 150, skipAt: 60 });
		const skip = judge(run, 5000);
		assert.strictEqual(skip.cuts, 1, JSON.stringify(skip));
		assert.isBelow(skip.off / skip.frames, 0.01, JSON.stringify(skip));
	}, 120_000);
});
