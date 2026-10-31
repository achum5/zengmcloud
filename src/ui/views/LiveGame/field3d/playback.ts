import {
	linesBetween,
	snapForCursor,
	targetForCursor,
	type FieldTimeline,
} from "./director.ts";
import { segmentAt, type KeySegment } from "../court3d/keyPlays.ts";
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
} from "../court3d/follow.ts";
import type { FieldReplay } from "./replays.ts";

// THE 3D FIELD'S CLOCK, frame by frame - the 3D court's, for football (see
// court3d/playback.ts): how far along the timeline the picture is, run on
// toward the page's cursor, and at each line it gets to, asking the page for
// it - and a big play, once it is over, shown again. Or, the key plays only
// (see keyPlays.ts), on from each one shown straight to the next. And,
// watching along with another device's field, kept a steady moment behind
// it (see court3d/follow.ts).

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
	// How much of the show has been played (ms): the game played through,
	// and the replays, but none of what is cut past. Two devices playing the
	// same game the same way agree on it, so it is how one plays along with
	// the other (see court3d/follow.ts).
	w: number;
	// Stepped by another device: where its field is taken to be.
	lead: number;
	// Playing along: when (ms) it last had to cut to where the other field
	// was - and, cut time after time, that it is not the same game over
	// there (see LOST_SLIPS).
	slips?: number[];
	lost?: boolean;
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
	w: 0,
	lead: 0,
});

// Playing along, this many cuts to where the other field was within this
// long (ms) - when, with it, there is one on joining late or after a skip
// there: the two are not showing the same game (an older build over there,
// say), and it is stepped line by line instead (see Field3D).
const LOST_SLIPS = 3;
const LOST_WITHIN = 20_000;
// (Or, a reel of the key plays, the other field somewhere this one's reel
// leaves out - more than this far (ms) from any stretch of it.)
const OFF_REEL_MS = 1000;
const offReel = (plan: KeySegment[], t: number) =>
	!plan.some((x) => t > x.t0 - OFF_REEL_MS && t < x.t1 + OFF_REEL_MS);

// THE SAME GAME, OR NOT (see court3d/follow.ts stagedAs): what this field
// staged - its lines' moments to a tenth of a second - said along with
// where it is, so one that staged something else is stepped instead.
export const fieldStagedAs = (tl: FieldTimeline): string => {
	let h = 0x811c9dc5;
	const add = (x: number) => {
		h ^= Math.round(x) | 0;
		h = Math.imul(h, 0x01000193);
	};
	add(tl.beats.length);
	add(tl.end / 100);
	for (const b of tl.beats) {
		add(b.i);
		add(b.actionStart / 100);
		add(b.end / 100);
	}
	for (const play of tl.plays) {
		add((play.replay?.cue ?? -1) / 100);
	}
	return (h >>> 0).toString(36);
};

// How far on the picture may run with the page's cursor where it is: to the
// moment of the next line. Stepped by another device, through to the end of
// that line: the word of it comes late and unevenly (see court3d's).
const stepTarget = (tl: FieldTimeline, cursor: number, follower: boolean) => {
	if (!follower) {
		return targetForCursor(tl, cursor);
	}
	const beat = tl.beats.find((b) => b.i >= cursor);
	return beat ? beat.end : tl.end;
};

// Follow the page's cursor: run on to the next line normally; cut straight
// there on a rewind or a big jump ahead (fast-forward, joining late).
export const onFieldCursor = (
	s: FieldClock,
	tl: FieldTimeline,
	{
		cursor,
		paused,
		skips,
		follower = false,
	}: { cursor: number; paused: boolean; skips: number; follower?: boolean },
) => {
	const target = stepTarget(tl, cursor, follower);
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
	if (follower) {
		// Where the other field is: just past the line it last reported (it
		// reports one the moment it gets there) - or, after a cut, where
		// this one now is.
		s.lead = snap
			? s.t
			: Math.max(s.lead, s.t, targetForCursor(tl, s.prevCursor));
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
	// Playing along with another device's field instead (see
	// court3d/follow.ts).
	along?: boolean;
	keyPlan?: KeySegment[];
	eventsLength: number;
	onReady: () => void;
	onJump?: (cursor: number) => void;
	// Where this field is, for anyone watching along - and what it staged.
	report?: { current: Shown | undefined };
	fp?: string;
};

// Playing along: a cut to where the other field was (see court3d/follow.ts)
// - up out of black when it is a long way, as onto the next key play.
const alignTo = (
	s: FieldClock,
	p: FieldPlayInput,
	f: Follow,
	tl: FieldTimeline,
	r: FollowReport,
	now: number,
	dip: boolean,
) => {
	cutTo(f, r);
	s.t = r.t;
	s.w = r.w;
	const replay =
		r.rp >= 0
			? tl.plays.find((x) => x.replay && Math.abs(x.replay.cue - r.t) < 1)
					?.replay
			: undefined;
	s.replay = replay ? { ...replay, at: r.rp } : undefined;
	s.snapCam = true;
	if (dip) {
		s.keyDip = now;
	}
	const slips = (s.slips ?? []).filter((x) => now - x < LOST_WITHIN);
	slips.push(now);
	s.slips = slips;
	if (slips.length >= LOST_SLIPS) {
		s.lost = true;
	}
	if (r.cursor > p.cursor && p.onJump) {
		s.jumpTo = r.cursor;
		p.onJump(r.cursor);
	}
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

// One frame: the clock run on to `now` (ms; `wall`, Date.now() - for
// playing along, `f`). How long the frame was, and whether it went through
// a cut.
export const stepField = (
	s: FieldClock,
	p: FieldPlayInput,
	tl: FieldTimeline,
	now: number,
	f?: Follow,
	wall = now,
): { dt: number; cut: boolean } => {
	const dt = s.last === undefined ? 0 : Math.min(100, now - s.last);
	s.last = now;
	const rate = Math.max(1e-6, p.rate);
	// Playing along with another device's field: as much of the show as
	// holds a steady gap behind it - or a cut to where it was.
	const along = p.along && f ? f : undefined;
	let show = dt;
	let cut = false;
	if (along) {
		const step = followStep(along, s.w, wall, dt);
		show = step.ms;
		if (step.snap) {
			alignTo(s, p, along, tl, step.snap, now, true);
			cut = true;
		}
		const said = along.reports.at(-1);
		if (
			p.keyPlan &&
			said &&
			said.rp < 0 &&
			!said.paused &&
			offReel(p.keyPlan, said.t)
		) {
			s.lost = true;
		}
	}
	const target = stepTarget(tl, p.cursor, p.follower);
	if (p.follower) {
		// Stepped by the device in charge of simming: hold a steady gap to
		// where its field is, rather than running to the last line it
		// reported and stopping there.
		if (!p.paused && !s.replay) {
			s.lead = advanceLead(Math.max(s.lead, s.t), target, dt, rate);
		}
		show *= followRate(s.lead - s.t);
	}
	const before = s.t;
	const plan = p.keyPlan;
	const keyOn = !!plan && !p.paused && !p.follower;
	const r = s.replay;
	if (r) {
		if (!p.paused) {
			const at = r.at + show * rate * REPLAY_SPEED;
			s.w += (Math.min(at, r.to) - r.at) / (rate * REPLAY_SPEED);
			r.at = at;
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
			s.t + show * rate,
		);
		s.w += (s.t - before) / rate;
		if (again && s.t >= next.cue) {
			s.replay = { ...next, at: next.from };
			s.snapCam = true;
		}
	}
	cut ||= tl.cuts.some((c) => c > before && c <= s.t);
	// Key plays only: past what is left out, straight on to the next
	// stretch shown - the page catching the score and the lines up to it on
	// the way.
	if (keyOn && !s.replay && s.jumpTo < 0) {
		const { at, next } = segmentAt(plan, s.t);
		if (!at && next) {
			if (along) {
				noteShown(along, { w: s.w, t: s.t, rp: -1 });
			}
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
	// Playing along: still where the other field was at each point it said
	// - or, if not (it skipped on), a cut to there.
	if (along) {
		noteShown(along, { w: s.w, t: s.t, rp: s.replay?.at ?? -1 });
		const per = (_t: number, rp: number) =>
			rp >= 0 ? rate * REPLAY_SPEED : rate;
		for (const due of dueChecks(along, s.w)) {
			if (!agrees(along, due, per)) {
				alignTo(s, p, along, tl, due, now, false);
				cut = true;
				break;
			}
		}
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
	if (p.report) {
		p.report.current = {
			w: s.w,
			t: s.t,
			rp: s.replay?.at ?? -1,
			...(p.fp === undefined ? {} : { fp: p.fp }),
		};
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
