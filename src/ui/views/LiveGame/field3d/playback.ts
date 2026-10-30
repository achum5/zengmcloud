import {
	linesBetween,
	snapForCursor,
	targetForCursor,
	type FieldTimeline,
} from "./director.ts";
import { segmentAt, type KeySegment } from "../court3d/keyPlays.ts";
import type { FieldReplay } from "./replays.ts";

// THE 3D FIELD'S CLOCK, frame by frame - the 3D court's, for football (see
// court3d/playback.ts): how far along the timeline the picture is, run on
// toward the page's cursor, and at each line it gets to, asking the page for
// it - and a big play, once it is over, shown again. Or, the key plays only
// (see keyPlays.ts), on from each one shown straight to the next.

// Watched quicker than this, a big play is not shown again.
export const REPLAY_RATE_MAX = 2;
export const REPLAY_SPEED = 0.55;

// A replay showing: where in it the picture is (`at`, timeline ms).
export type FieldReplayShowing = FieldReplay & { at: number };

export type FieldClock = {
	t: number;
	last: number | undefined;
	// The cursor it last asked the page to move on from (-1 none).
	readyFor: number;
	// "Next play" while paused: show that one play, then hold.
	stepping: boolean;
	prevCursor: number;
	prevPaused: boolean;
	prevSkips: number;
	// Cut the camera straight to its shot, next frame.
	snapCam: boolean;
	// The replay showing now; the live clock held while it does.
	replay: FieldReplayShowing | undefined;
	// Key plays: the cursor it has asked the page to go on to (-1 none), and
	// when (frame time) it went on to the next one.
	jumpTo: number;
	keyDip: number;
};

export const makeFieldClock = (paused: boolean, skips: number): FieldClock => ({
	t: 0,
	last: undefined,
	readyFor: -1,
	stepping: false,
	prevCursor: -1,
	prevPaused: paused,
	prevSkips: skips,
	snapCam: true,
	replay: undefined,
	jumpTo: -1,
	keyDip: -Infinity,
});

// Follow the page's cursor: run on to the next line normally; cut straight
// there on a rewind or a big jump ahead (fast-forward, joining late).
export const onFieldCursor = (
	s: FieldClock,
	tl: FieldTimeline,
	{ cursor, paused, skips }: { cursor: number; paused: boolean; skips: number },
) => {
	const target = targetForCursor(tl, cursor);
	// "Next play": past what is left of the play now shown, straight on to
	// the next one - never back.
	const skipped =
		s.prevCursor >= 0 && skips !== s.prevSkips && cursor > s.prevCursor;
	// (Caught up to the key play it went on to: it is there already - or a
	// line on, the page getting there a line at a time.)
	const expected =
		s.jumpTo >= 0 &&
		(cursor <= s.jumpTo || linesBetween(tl, s.jumpTo, cursor) <= 1);
	const snap =
		!expected &&
		(s.prevCursor < 0 ||
			cursor < s.prevCursor ||
			target < s.t - 1 ||
			skipped ||
			(linesBetween(tl, s.prevCursor, cursor) > 2 && target - s.t > 9000));
	if (snap) {
		const at = snapForCursor(tl, cursor);
		s.t = skipped ? Math.max(s.t, at) : at;
		s.snapCam = true;
		s.replay = undefined;
	}
	if (paused && s.prevCursor >= 0 && cursor > s.prevCursor && !skipped) {
		s.stepping = true;
	}
	if (s.prevPaused && !paused) {
		s.readyFor = -1;
		s.jumpTo = -1;
	}
	s.prevCursor = cursor;
	s.prevPaused = paused;
	s.prevSkips = skips;
};

export type FieldPlayInput = {
	cursor: number;
	paused: boolean;
	rate: number;
	// Stepped by the device in charge of simming: never asks for a line.
	follower: boolean;
	keyPlan?: KeySegment[];
	eventsLength: number;
	onReady: () => void;
	onJump?: (cursor: number) => void;
};

// Whether a big play coming up is shown again: not stepping through play
// by play, not watching on fast - and, a reel of the key plays, only in a
// close finish, shown the way it happened.
export const showsAgain = (
	s: FieldClock,
	p: Pick<FieldPlayInput, "rate" | "keyPlan">,
	r: FieldReplay | undefined,
): r is FieldReplay =>
	!!r &&
	!s.stepping &&
	p.rate <= REPLAY_RATE_MAX &&
	(!p.keyPlan || segmentAt(p.keyPlan, r.cue).at?.close === true);

// One frame: the clock run on to `now` (ms). How long the frame was, and
// whether it went through a cut.
export const stepField = (
	s: FieldClock,
	p: FieldPlayInput,
	tl: FieldTimeline,
	now: number,
): { dt: number; cut: boolean } => {
	const dt = s.last === undefined ? 0 : Math.min(100, now - s.last);
	s.last = now;
	const target = targetForCursor(tl, p.cursor);
	const before = s.t;
	const plan = p.keyPlan;
	const keyOn = !!plan && !p.paused && !p.follower;
	const r = s.replay;
	if (r) {
		if (!p.paused) {
			r.at += dt * p.rate * REPLAY_SPEED;
		}
		if (r.at >= r.to) {
			s.replay = undefined;
			s.snapCam = true;
		}
	} else if ((!p.paused || s.stepping) && s.t < target) {
		// On to the next line - stopping on the moment to show a big play
		// again, or at the end of the key play shown, exactly there.
		const next = nextReplay(tl, before);
		const again = showsAgain(s, p, next);
		const seg = keyOn ? segmentAt(plan, s.t).at : undefined;
		s.t = Math.min(
			target,
			again ? next.cue : Infinity,
			seg ? seg.t1 : Infinity,
			s.t + dt * p.rate,
		);
		if (again && s.t >= next.cue) {
			s.replay = { ...next, at: next.from };
			s.snapCam = true;
		}
	}
	let cut = tl.cuts.some((c) => c > before && c <= s.t);
	// Key plays only: past what is left out, straight on to the next
	// stretch shown - the page catching the score and the lines up to it on
	// the way.
	if (keyOn && !s.replay && s.jumpTo < 0) {
		const { at, next } = segmentAt(plan, s.t);
		if (!at && next) {
			s.t = next.t0;
			s.keyDip = now;
			cut = true;
			if (next.cursor > p.cursor && p.onJump) {
				s.jumpTo = next.cursor;
				p.onJump(next.cursor);
			}
		}
	}
	if (s.jumpTo >= 0 && p.cursor >= s.jumpTo) {
		s.jumpTo = -1;
	}
	if (cut) {
		s.snapCam = true;
	}
	if (s.t >= target && !s.replay) {
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
	return { dt, cut };
};

// The next replay to come after t, if any.
export const nextReplay = (
	tl: FieldTimeline,
	t: number,
): FieldReplay | undefined => {
	for (const play of tl.plays) {
		if (play.replay && play.replay.cue > t) {
			return play.replay;
		}
	}
	return undefined;
};

// THE WIPE: a replay gone into, and come out of, behind a band across the
// picture in the colors of the team whose play it is - the 3D court's (see
// court3d/playback.ts), the cut made while it covers all of it. How far
// across it is, 0 to 1 (0.5: all of the picture covered): in over the last
// STING_IN ms (real time) before a replay cuts in, or before it is over, to
// the cut; then on off the picture in as long.
export const STING_IN = 280;
// (All of the picture covered while it is this close to halfway across.)
export const STING_COVERS = 0.03;
export type FieldWipe = {
	// Whether a replay was showing last frame, and when it last cut in or
	// out (frame time, ms).
	was: boolean;
	cut: number | undefined;
	covered: boolean;
};
export const makeFieldWipe = (): FieldWipe => ({
	was: false,
	cut: undefined,
	covered: false,
});
// Before the cut: how far in it has come.
const stingIn = (
	s: FieldClock,
	p: Pick<FieldPlayInput, "rate" | "keyPlan">,
	tl: FieldTimeline,
): number | undefined => {
	const r = s.replay;
	const rate = Math.max(1e-6, p.rate);
	if (r) {
		const left = (r.to - r.at) / (rate * REPLAY_SPEED);
		return left < STING_IN ? 0.5 * (1 - left / STING_IN) : undefined;
	}
	const next = nextReplay(tl, s.t);
	if (!showsAgain(s, p, next)) {
		return undefined;
	}
	const lead = STING_IN * rate;
	const d = next.cue - s.t;
	return d < lead ? 0.5 * (1 - Math.max(0, d) / lead) : undefined;
};
// The wipe this frame (after the clock's step - see stepField), and the
// replay it is for.
export const wipeAt = (
	w: FieldWipe,
	s: FieldClock,
	p: Pick<FieldPlayInput, "rate" | "keyPlan">,
	tl: FieldTimeline,
	now: number,
): number | undefined => {
	if (!!s.replay !== w.was) {
		w.was = !!s.replay;
		w.cut = now;
	}
	let u: number | undefined;
	if (w.cut !== undefined) {
		const e = now - w.cut;
		if (e < STING_IN) {
			u = 0.5 + (0.5 * e) / STING_IN;
		} else {
			w.cut = undefined;
		}
	}
	u ??= stingIn(s, p, tl);
	w.covered = u !== undefined && Math.abs(u - 0.5) <= STING_COVERS;
	return u;
};
