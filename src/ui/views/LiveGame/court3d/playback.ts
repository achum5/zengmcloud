import {
	linesBetween,
	snapForCursor,
	targetForCursor,
	type CourtTimeline,
	type Fx,
} from "./director.ts";
import { cameraCuts, fastAt } from "./evaluate.ts";
import {
	advanceLead,
	agrees,
	cutTo,
	dueChecks,
	followRate,
	followStep,
	noteShown,
	type Follow,
	type FollowReport,
	type Shown,
} from "./follow.ts";
import { segmentAt, type KeySegment } from "./keyPlays.ts";
import { WARMUP_MS } from "./pregame.ts";

// THE 3D COURT'S CLOCK, frame by frame: how far along the timeline the
// picture is, run on toward the page's cursor (see Court3D) - fast through
// the dead stretches, a big dunk shown again, the plays a reel of key plays
// leaves out cut past - and, watching along with another device's court,
// kept a steady moment behind it (see follow.ts).

// The timeline is in real time: played at 1x, a second of it is a second
// (bar the dead time it runs through fast - see fastAt). Watched quicker
// than this, a dunk is not shown again.
const REPLAY_RATE_MAX = 2;
// A dunk's replay: from just before he takes off to just after, in slow
// motion, once its own play is over. (A big three's from his gather -
// longer before it goes in.)
const REPLAY_FROM = 1700;
const REPLAY_FROM_THREE = 2300;
const REPLAY_TO = 450;
export const REPLAY_AFTER = 1150;
const REPLAY_SPEED = 0.42;
// Which plays are shown again: a big dunk, or a big three (see Fx).
export const replayed = (f: Fx) =>
	!!f.big && (f.kind === "dunk" || f.kind === "swish");
const replayOf = (f: Fx) => {
	const from = f.t - (f.kind === "swish" ? REPLAY_FROM_THREE : REPLAY_FROM);
	return { at: from, from, to: f.t + REPLAY_TO };
};

// The cut nearest to t, by binary search.
const nearestCut = (cuts: number[], t: number): number | undefined => {
	let lo = 0;
	let hi = cuts.length - 1;
	while (lo < hi) {
		const mid = (lo + hi) >> 1;
		if (cuts[mid]! < t) {
			lo = mid + 1;
		} else {
			hi = mid;
		}
	}
	const a = cuts[lo];
	const b = cuts[lo - 1];
	if (a === undefined) {
		return b;
	}
	return b !== undefined && t - b < a - t ? b : a;
};

// The show and the game run on together the same way on any device: in
// steps of STEP_MS of the show, the game at the pace it was going at each
// step's start - so where a frame happens to end makes no difference to
// where in the game a point in the show is, and two devices playing it
// agree on that through every quickening and slowing (see fastAt).
const STEP_MS = 8;

// On through `show` ms of the show, no further than `limit` (timeline ms).
const runOn = (
	s: PlayClock,
	tl: CourtTimeline,
	base: number,
	show: number,
	limit: number,
) => {
	let left = show;
	for (let n = 0; n < 1000 && left > 1e-9 && s.t < limit; n++) {
		const k = Math.floor(s.w / STEP_MS + 1e-9);
		if (k !== s.paceAt) {
			s.pace = base * fastAt(tl, s.t);
			s.paceAt = k;
		}
		const there = (limit - s.t) / s.pace;
		const step = Math.min(left, Math.max(1e-9, (k + 1) * STEP_MS - s.w), there);
		// (Exactly there, not a rounding short of it.)
		s.t = step === there ? limit : Math.min(limit, s.t + step * s.pace);
		s.w += step;
		left -= step;
	}
};

export type PlayClock = {
	// Where the picture is on the timeline (ms).
	t: number;
	// How much of the show has been played (ms): the game played through at
	// its own pace, and the replays, but none of what is cut past. Two
	// devices playing the same game the same way agree on it, so it is how
	// one plays along with the other (see follow.ts).
	w: number;
	// How many game ms a show ms is through the step of the show it is in
	// (see runOn), and which step that is (-1: work it out again).
	pace: number;
	paceAt: number;
	// Following another device's sim the old way: where its court is
	// estimated to be (see follow.ts).
	lead: number;
	// The last frame's clock (ms).
	last: number | undefined;
	// The dunk replay showing now, and the next dunk that would get one.
	replay: { at: number; from: number; to: number } | undefined;
	nextDunk: number;
	snapCam: boolean;
	// Key plays: the cursor the page is catching up to (-1 none), and when
	// the picture last came up out of black onto the next one (ms).
	jumpTo: number;
	keyDip: number;
	keyHold: number;
	readyFor: number;
	stepping: boolean;
	prevCursor: number;
	prevPaused: boolean;
	prevSkips: number;
};

// What the page says, this frame.
export type PlayInput = {
	cursor: number;
	paused: boolean;
	// How many times real time it plays at (1 is real time).
	rate: number;
	// Stepped by the device in charge of simming (see Court3D's props).
	follower: boolean;
	// Playing along with it instead (see follow.ts).
	along: boolean;
	keyPlan: KeySegment[] | undefined;
	eventsLength: number;
	onReady: () => void;
	onJump?: (cursor: number) => void;
	report?: { current: Shown | undefined };
};

// Follow the page's cursor: run on to the next line normally; cut straight
// there on a rewind or a big jump ahead (fast-forward, joining late).
export const onCursor = (
	s: PlayClock,
	timeline: CourtTimeline,
	{
		cursor,
		paused,
		skips,
		follower,
	}: { cursor: number; paused: boolean; skips: number; follower: boolean },
) => {
	const target = targetForCursor(timeline, cursor);
	// "Next play": past what is left of the play now shown, straight on to
	// the next one - never back.
	const skipped =
		s.prevCursor >= 0 && skips !== s.prevSkips && cursor > s.prevCursor;
	// (Caught up to a key play it went on to: it is there already.)
	const expected = s.jumpTo >= 0 && cursor <= s.jumpTo;
	const snap =
		!expected &&
		(s.prevCursor < 0 ||
			cursor < s.prevCursor ||
			target < s.t - 1 ||
			skipped ||
			(linesBetween(timeline, s.prevCursor, cursor) > 2 &&
				target - s.t > 9000));
	if (snap) {
		const at = snapForCursor(timeline, cursor);
		s.t = skipped ? Math.max(s.t, at) : at;
		s.snapCam = true;
		s.paceAt = -1;
	}
	if (follower) {
		// Where the other court is: just past the play it last reported
		// (it reports one the moment it gets there) - or, after a cut,
		// where this one now is.
		s.lead = snap
			? s.t
			: Math.max(s.lead, s.t, targetForCursor(timeline, s.prevCursor));
	}
	if (paused && s.prevCursor >= 0 && cursor > s.prevCursor && !skipped) {
		// "Next play" while paused: show that one play, then hold.
		s.stepping = true;
	}
	if (s.prevPaused && !paused) {
		s.readyFor = -1;
	}
	if (s.snapCam) {
		s.replay = undefined;
		s.nextDunk = timeline.fx.findIndex(
			(f) => replayed(f) && f.t + REPLAY_AFTER > s.t,
		);
	}
	s.prevCursor = cursor;
	s.prevPaused = paused;
	s.prevSkips = skips;
};

// Playing along: a cut to where the other court was (see follow.ts) - up
// out of black when it is a long way, as onto the next key play.
const alignTo = (
	s: PlayClock,
	p: PlayInput,
	f: Follow,
	tl: CourtTimeline,
	r: FollowReport,
	now: number,
	dip: boolean,
) => {
	cutTo(f, r);
	s.t = r.t;
	s.w = r.w;
	s.paceAt = -1;
	const dunk =
		r.rp >= 0
			? tl.fx.find((x) => replayed(x) && Math.abs(x.t + REPLAY_AFTER - r.t) < 1)
			: undefined;
	s.replay = dunk ? { ...replayOf(dunk), at: r.rp } : undefined;
	s.nextDunk = tl.fx.findIndex((x) => replayed(x) && x.t + REPLAY_AFTER > s.t);
	s.snapCam = true;
	if (dip) {
		s.keyDip = now;
		s.keyHold = -Infinity;
	}
	if (r.cursor > p.cursor && p.onJump) {
		s.jumpTo = r.cursor;
		p.onJump(r.cursor);
	}
};

// One frame: the clock run on to `now` (ms; `wall` is Date.now()). How long
// the frame was, how fast the timeline ran through it, and the camera cut
// nearest where it got to.
export const stepPlay = (
	s: PlayClock,
	p: PlayInput,
	f: Follow,
	tl: CourtTimeline,
	staging: boolean,
	now: number,
	wall: number,
): { dt: number; rate: number; cut: number | undefined } => {
	const dt = s.last === undefined ? 0 : Math.min(100, now - s.last);
	s.last = now;
	// Playing along with another device's court: as much of the show as
	// holds a steady gap behind it - or a cut to where it was.
	let show = dt;
	if (p.along && !staging) {
		const step = followStep(f, s.w, wall, dt);
		show = step.ms;
		if (step.snap) {
			alignTo(s, p, f, tl, step.snap, now, true);
		}
	}
	const target = staging
		? Math.min(WARMUP_MS, s.t + dt)
		: targetForCursor(tl, p.cursor);
	const base = p.rate;
	// Through the dead stretches, fast.
	let rate = base * fastAt(tl, s.t);
	if (p.follower) {
		// Behind the device in charge of simming: hold a steady gap to
		// where its court is, rather than running to the last play it
		// reported and stopping there.
		if (!p.paused) {
			s.lead = advanceLead(
				Math.max(s.lead, s.t),
				target,
				dt,
				base * fastAt(tl, s.lead),
			);
		}
		const k = followRate(s.lead - s.t);
		rate *= k;
		show *= k;
	}
	const before = s.t;
	// The next dunk that would be shown again, and whether it would be -
	// unless this device is only following, stepping play by play, or
	// watching on fast.
	const fx = tl.fx;
	while (
		s.nextDunk >= 0 &&
		s.nextDunk < fx.length &&
		(!replayed(fx[s.nextDunk]!) || fx[s.nextDunk]!.t + REPLAY_AFTER <= before)
	) {
		s.nextDunk += 1;
	}
	const dunk = fx[s.nextDunk];
	const again =
		!!dunk &&
		!p.follower &&
		!p.paused &&
		!s.stepping &&
		p.rate <= REPLAY_RATE_MAX &&
		// (A reel of the key plays moves on to the next - but for a close
		// finish, shown the way it happened.)
		(!p.keyPlan || segmentAt(p.keyPlan, dunk.t).at?.close === true);
	const plan = p.keyPlan;
	const keyOn = !!plan && !p.follower && !p.paused;
	// A replay holds the live clock while it plays.
	const r = s.replay;
	if (staging) {
		if (s.t < target) {
			s.t = Math.min(target, s.t + show * rate);
		}
	} else if (r) {
		if (!p.paused) {
			const at = r.at + show * base * REPLAY_SPEED;
			s.w += (Math.min(at, r.to) - r.at) / (base * REPLAY_SPEED);
			r.at = at;
		}
		if (r.at >= r.to) {
			s.replay = undefined;
			s.snapCam = true;
			s.paceAt = -1;
		}
	} else if ((!p.paused || s.stepping) && s.t < target) {
		// On to the next line - stopping on the moment to show a dunk again,
		// or at the end of the key play shown, exactly there.
		const seg = keyOn ? segmentAt(plan, s.t).at : undefined;
		runOn(
			s,
			tl,
			base,
			show,
			Math.min(
				target,
				again ? dunk.t + REPLAY_AFTER : Infinity,
				seg ? seg.t1 : Infinity,
			),
		);
	}
	// A dunk just finished: show it again.
	if (
		!s.replay &&
		dunk &&
		dunk.t + REPLAY_AFTER > before &&
		dunk.t + REPLAY_AFTER <= s.t
	) {
		s.nextDunk += 1;
		if (again) {
			s.t = dunk.t + REPLAY_AFTER;
			s.replay = replayOf(dunk);
			s.snapCam = true;
			s.paceAt = -1;
		}
	}
	// Through a cut: the camera starts fresh on the other side.
	const cut = nearestCut(cameraCuts(tl), s.t);
	if (cut !== undefined && cut > before && cut <= s.t) {
		s.snapCam = true;
	}
	// Key plays only: past what is left out, straight on to the next
	// stretch shown - the page catching the score and the lines up to
	// it on the way.
	if (keyOn && !s.replay && s.jumpTo < 0) {
		const { at, next } = segmentAt(plan, s.t);
		if (!at && next) {
			if (p.along) {
				noteShown(f, { w: s.w, t: s.t, rp: -1 });
			}
			s.t = next.t0;
			s.paceAt = -1;
			s.snapCam = true;
			s.keyDip = now;
			if (next.cursor > p.cursor && p.onJump) {
				s.jumpTo = next.cursor;
				p.onJump(next.cursor);
			}
		}
	}
	if (s.jumpTo >= 0 && p.cursor >= s.jumpTo) {
		s.jumpTo = -1;
	}
	// Playing along: still where the other court was at each point it
	// said - or, if not (it skipped on), a cut to there.
	if (p.along && !staging) {
		noteShown(f, { w: s.w, t: s.t, rp: s.replay?.at ?? -1 });
		const per = (t: number, rp: number) =>
			rp >= 0 ? base * REPLAY_SPEED : base * fastAt(tl, t);
		for (const due of dueChecks(f, s.w)) {
			if (!agrees(f, due, per)) {
				alignTo(s, p, f, tl, due, now, false);
				break;
			}
		}
	}
	if (s.t >= target && !s.replay && !staging) {
		s.stepping = false;
		if (
			!p.paused &&
			!p.follower &&
			s.jumpTo < 0 &&
			p.cursor < p.eventsLength &&
			s.readyFor !== p.cursor
		) {
			s.readyFor = p.cursor;
			p.onReady();
		}
	}
	if (p.report && !staging) {
		p.report.current = { w: s.w, t: s.t, rp: s.replay?.at ?? -1 };
	}
	return { dt, rate, cut };
};
