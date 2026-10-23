import { assert, describe, test } from "vitest";
import { compile, gidOf } from "./testGame.ts";
import {
	KEY_PLAY_DEFAULTS,
	planKeyPlays,
	type KeySegment,
} from "./keyPlays.ts";
import {
	addReport,
	makeFollow,
	stagedAs,
	SYNC_BEHIND,
	type Follow,
	type FollowReport,
} from "./follow.ts";
import {
	makeWipe,
	onCursor,
	replayed,
	STING_IN,
	stepPlay,
	wipeAt,
	type PlayClock,
} from "./playback.ts";
import {
	compileCourt,
	isLineItem,
	targetForCursor,
	type CourtPlayer,
	type CourtTimeline,
	type RawEvent,
} from "./director.ts";

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
	stagedBy,
	unsaid,
	silent,
}: {
	seed: string;
	possessions?: number;
	rate?: number;
	minutes?: number;
	joinAt?: number;
	pauseAt?: number;
	skipAt?: number;
	seconds?: number;
	// How the watching device stages the game, if not as the other one does
	// (an older build there, say).
	stagedBy?: (events: RawEvent[], players: CourtPlayer[]) => CourtTimeline;
	// The other device on a build that doesn't say what it staged - or,
	// older still, nothing of where its court is at all.
	unsaid?: boolean;
	silent?: boolean;
}) => {
	const { tl, events, players } = compile(seed, possessions);
	const ftl = stagedBy ? stagedBy(events, players) : tl;
	const planOf = (tlx: CourtTimeline) =>
		minutes
			? planKeyPlays(
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
	// Court3D's onLost) - then the page has it stepped line by line.
	let stepped = !!silent;
	let steppedAt: number | undefined = silent ? 0 : undefined;
	const frame = (d: Device, tlx: CourtTimeline, now: number, wall: number) => {
		const st = d.along && stepped;
		stepPlay(
			d.s,
			{
				cursor: d.cursor,
				paused: d.paused,
				rate,
				follower: st,
				along: d.along && !st,
				keyPlan: d.along ? (silent ? undefined : fPlan) : plan,
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
			onCursor(d.s, d.along ? ftl : tl, {
				cursor: d.cursor,
				paused: d.paused,
				skips: d.skips,
				follower: d.along && stepped,
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
	// Each line's moment, when each court played through it (ms) - not cut
	// past, nor in a replay.
	const seenB = new Map<number, number>();
	const seenF = new Map<number, number>();
	const seen = (
		m: Map<number, number>,
		tlx: CourtTimeline,
		t0: number,
		s: PlayClock,
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
						...(unsaid ? {} : { fp: stagedAs(tl) }),
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
				if (!stepped && r.fp !== undefined && r.fp !== stagedAs(ftl)) {
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
				b.next = Math.min(n, b.cursor + 1);
			}
			render(b);
			const bt = b.s.t;
			frame(b, tl, now, now + SKEW);
			seen(seenB, tl, bt, b.s, now);
			shown.push({ w: b.s.w, t: b.s.t, rp: b.s.replay?.at ?? -1 });
			render(b);
			bNext = now + 1000 / 60 + jitter;
		} else {
			const w0 = f.s.w;
			const dt = f.s.last === undefined ? 0 : Math.min(100, now - f.s.last);
			// (Waiting on its own page for the next line, as the other court
			// does there too - the opening tip's dozen lines, say.)
			const line = f.s.t >= targetForCursor(ftl, f.cursor);
			const ft = f.s.t;
			frame(f, ftl, now, now);
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
	return { out, theirs, tl, plan, seenB, seenF, steppedAt };
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

// What the watching court showed of what the other one did: each line's
// moment it played through - how many it never showed, and how long after
// the other court it showed the rest (ms).
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
		minLag: Math.min(...lags),
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

// Staged by another build: the same game, one man's skills read otherwise
// (so other sets get run).
const otherBuild = (events: RawEvent[], players: CourtPlayer[]) =>
	compileCourt({
		events,
		players: players.map((q, k) =>
			k === 2 ? { ...q, skills: ["3", "B"] } : q,
		),
		gid: gidOf("along"),
	});

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
			assert.isUndefined(run.steppedAt);
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

	// Both devices stage the game the same way, and say so.
	test("a court says what it staged: the same game the same, another not", () => {
		const a = compile("along", 140);
		const b = compile("along", 140);
		assert.strictEqual(stagedAs(a.tl), stagedAs(b.tl));
		assert.notStrictEqual(
			stagedAs(a.tl),
			stagedAs(otherBuild(a.events, a.players)),
		);
	}, 60_000);

	// The field report: watched from a device on another build, which stages
	// the game differently, playing along cut back to where the other court
	// was second after second - the same few seconds over and over until it
	// moved on. Told what the other court staged, it is stepped line by line
	// at once instead (failing that, after a couple of cuts back): every
	// play the other one shows, a moment behind it, never ahead, and never
	// the same stretch twice.
	test("on another build: stepped line by line, never round the same stretch again", () => {
		for (const minutes of [3, undefined]) {
			for (const unsaid of [false, true]) {
				const run = watch({
					seed: "along",
					minutes,
					seconds: 300,
					stagedBy: otherBuild,
					unsaid,
				});
				const label = `${minutes ?? "all"} ${unsaid ? "unsaid" : "said"}`;
				// Back to a moment it had already shown: never once it is
				// stepped; before that, told what the other court staged,
				// never - and not told, only the couple it takes to tell.
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
				// Every play the other one shows, and not long after (staged
				// otherwise here, some a moment sooner) - and never stood still
				// for long, waiting on the other device's word of the next play.
				const m = moments(run);
				assert.isAbove(m.shown, 40, label);
				// (Not told, it plays along with the other court until it can
				// tell - and a cut to where that one was, staged otherwise, can
				// go past a play or two here: only so many, and from a couple
				// of seconds after it is stepped, all but the usual.)
				const told = at + 2000;
				const missed = [...run.seenB].filter(([i]) => !run.seenF.has(i));
				const late = missed.filter(([, when]) => when > told).length;
				assert.isAtMost(late, 2, `${label} ${JSON.stringify(m)}`);
				assert.isAtMost(
					missed.length - late,
					unsaid ? 3 : 0,
					`${label} ${JSON.stringify(m)}`,
				);
				assert.isBelow(m.maxLag, 4000, `${label} ${JSON.stringify(m)}`);
				const held = freezes(run, 5000);
				assert.isBelow(held[0] ?? 0, 4000, `${label} ${held.slice(0, 4)}`);
				assert.isAtMost(
					held.filter((x) => x > 1000).length,
					2,
					`${label} ${held.slice(0, 4)}`,
				);
			}
		}
	}, 240_000);

	// A device in charge of simming on a build that says nothing of where
	// its court is: stepped line by line from the start, on through each
	// play as the word of it comes - all of them, a moment behind, never
	// back over one.
	test("an older build there: stepped, every play, never back", () => {
		const run = watch({ seed: "along", seconds: 300, silent: true });
		const back = run.out.filter(
			(x, k) => k > 0 && x.t < run.out[k - 1]!.t - 50,
		).length;
		assert.strictEqual(back, 0);
		const m = moments(run);
		assert.isAbove(m.shown, 60);
		assert.isAtMost(m.missing, 2, JSON.stringify(m));
		assert.isBelow(m.maxLag, 3000, JSON.stringify(m));
		const held = freezes(run, 5000);
		assert.isBelow(held[0] ?? 0, 2500, `${held.slice(0, 4)}`);
	}, 120_000);
});

describe("onto the game from the warm-ups", () => {
	// The warm-ups run on their own clock while the game is staged; the game
	// arrives, and the picture has a frame or two of it before the page's
	// cursor reaches the clock (see Court3D).
	test("the first line goes up at the tip, not on the game's first frame", () => {
		const { tl, events } = compile("warm", 20);
		const s = device(false).s;
		s.t = 25_000;
		let asked = 0;
		const f = makeFollow();
		for (let now = 16; now < 100; now += 16) {
			stepPlay(
				s,
				{
					cursor: 0,
					paused: false,
					rate: 1,
					follower: false,
					along: false,
					keyPlan: undefined,
					eventsLength: events.length,
					onReady: () => {
						asked += 1;
					},
				},
				f,
				tl,
				false,
				now,
				now,
			);
		}
		assert.strictEqual(asked, 0);
		assert.isBelow(s.t, targetForCursor(tl, 0));
	});
});

describe("the wipe into a replay and out of it", () => {
	// A dunk shown again: the picture goes into the replay and back out of it
	// behind a wipe - coming in over the picture to cover all of it at the
	// cut, never over the dunk itself, and off it again as quickly; held
	// over it while the picture on the other side is not ready.
	test("in to cover the cut, never over the dunk, held while need be", () => {
		const { tl, events } = compile("b", 150);
		const dunks = tl.fx.filter((f) => replayed(f));
		assert.isAtLeast(dunks.length, 1);
		for (const [k, dunk] of dunks.slice(0, 3).entries()) {
			const s = device(false).s;
			s.t = dunk.t - 4000;
			s.prevCursor = events.length;
			s.nextDunk = tl.fx.indexOf(dunk);
			const w = makeWipe();
			const f = makeFollow();
			const frames: {
				now: number;
				t: number;
				replay: boolean;
				u: number | undefined;
				covered: boolean;
			}[] = [];
			// The second dunk's: the picture on the other side of each cut
			// not ready for six frames - the wipe held over it.
			const holds = k === 1 ? 6 : 0;
			let held = 0;
			for (let now = 16; now < 14_000; now += 1000 / 60) {
				const { rate } = stepPlay(
					s,
					{
						cursor: events.length,
						paused: false,
						rate: 1,
						follower: false,
						along: false,
						keyPlan: undefined,
						eventsLength: events.length,
						onReady: () => {},
					},
					f,
					tl,
					false,
					now,
					now,
				);
				const was = frames.at(-1);
				if (holds && w.covered && was && was.replay !== !!s.replay) {
					held = holds;
				}
				if (held > 0 && w.covered) {
					w.hold = now;
					held -= 1;
				}
				const u = wipeAt(w, s, now, 1, rate);
				frames.push({ now, t: s.t, replay: !!s.replay, u, covered: w.covered });
			}
			const label = `${k} @${Math.round(dunk.t)}`;
			const cuts = frames
				.map((x, i) => i)
				.filter((i) => i > 0 && frames[i]!.replay !== frames[i - 1]!.replay);
			assert.strictEqual(cuts.length, 2, label);
			for (const i of cuts) {
				// All of the picture covered at the cut - come in over it,
				// not popped up.
				assert.isTrue(frames[i]!.covered, label);
				assert.isAtLeast(frames[i - 1]!.u ?? 0, 0.35, label);
				// On the picture only about the cut.
				const on = frames.filter(
					(x) => x.u !== undefined && Math.abs(x.now - frames[i]!.now) < 2000,
				);
				assert.isBelow(
					on.length,
					((2 * STING_IN) / 1000) * 60 + holds + 4,
					label,
				);
				// Held while the other side was not ready, then off.
				if (holds) {
					assert.isTrue(
						frames.slice(i, i + holds).every((x) => x.covered),
						label,
					);
				}
			}
			// Never over the dunk.
			for (const x of frames) {
				if (!x.replay && x.t > dunk.t - 1500 && x.t < dunk.t + 400) {
					assert.isUndefined(x.u, `${label} t=${Math.round(x.t)}`);
				}
			}
		}
	}, 60_000);
});
