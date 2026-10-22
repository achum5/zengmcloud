import {
	linesBetween,
	snapForCursor,
	targetForCursor,
	type FieldTimeline,
} from "./director.ts";
import type { FieldReplay } from "./replays.ts";

// THE 3D FIELD'S CLOCK, frame by frame - the 3D court's, for football (see
// court3d/playback.ts): how far along the timeline the picture is, run on
// toward the page's cursor, and at each line it gets to, asking the page for
// it - and a big play, once it is over, shown again.

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
	// The replay showing now; the live clock held while it does - and when
	// (frame time, ms) the last one ended.
	replay: FieldReplayShowing | undefined;
	backAt: number;
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
	backAt: -Infinity,
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
	const snap =
		s.prevCursor < 0 ||
		cursor < s.prevCursor ||
		target < s.t - 1 ||
		skipped ||
		(linesBetween(tl, s.prevCursor, cursor) > 2 && target - s.t > 9000);
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
	eventsLength: number;
	onReady: () => void;
};

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
	const r = s.replay;
	if (r) {
		if (!p.paused) {
			r.at += dt * p.rate * REPLAY_SPEED;
		}
		if (r.at >= r.to) {
			s.replay = undefined;
			s.snapCam = true;
			s.backAt = now;
		}
	} else if ((!p.paused || s.stepping) && s.t < target) {
		// On to the next line - stopping on the moment to show a big play
		// again, unless stepping play by play or watching on fast.
		const next = nextReplay(tl, before);
		const again = !!next && !s.stepping && p.rate <= REPLAY_RATE_MAX;
		s.t = Math.min(target, again ? next.cue : Infinity, s.t + dt * p.rate);
		if (again && s.t >= next.cue) {
			s.replay = { ...next, at: next.from };
			s.snapCam = true;
		}
	}
	const cut = tl.cuts.some((c) => c > before && c <= s.t);
	if (cut) {
		s.snapCam = true;
	}
	if (s.t >= target && !s.replay) {
		s.stepping = false;
		if (
			!p.paused &&
			!p.follower &&
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
