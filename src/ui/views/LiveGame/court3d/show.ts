import {
	advanceLead,
	agrees,
	FOLLOW_BEHIND,
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
import {
	linesBetween,
	snapForCursor,
	targetForCursor,
	type Beats,
} from "./timeline.ts";

// THE SHOW'S CLOCK, frame by frame, for any sport (see court3d/playback.ts,
// field3d/playback.ts): how far along the timeline the picture is, run on
// toward the page's cursor - fast through the dead stretches, a big play
// shown again, the plays a reel of key plays leaves out cut past - and,
// watching along with another device's picture, kept a steady moment behind
// it (see follow.ts).

// What a sport's timeline says about how it is shown (see courtShow,
// fieldShow).
export type Show<R extends Replay = Replay> = {
	// How many times real time the game runs at t (1: real time; more
	// through the dead stretches - see the court's fastAt).
	fastAt: (t: number) => number;
	// The moments the picture cuts (ascending): the camera starts fresh on
	// the other side.
	cuts: number[];
	// The plays shown again (ascending by `cue`): the moment the game stops
	// for it, and the replay.
	replays: { cue: number; replay: () => R }[];
	// How many times real time a replay plays at.
	replaySpeed: number;
	// The wipe into a replay coming never starts further back than this
	// (game ms) before it - not back over the play itself.
	stingMost?: number;
};

// (What the wipe needs of it.)
export type Replaying = Pick<Show, "replaySpeed" | "stingMost">;

// The timeline is in real time: played at 1x, a second of it is a second
// (bar the dead time it runs through fast). Watched quicker than this, a
// big play is not shown again.
export const REPLAY_RATE_MAX = 2;

// A replay showing: where it is (`at`), from and to (timeline ms) - and,
// asked for from the page (a highlight), it plays even with the game paused.
export type Replay = {
	at: number;
	from: number;
	to: number;
	asked?: boolean;
};

// THE WIPE: a replay is gone into, and come out of, behind a wipe across
// the picture in the colors of the team whose play it is, the way a
// broadcast does it - the cut made while it covers all of it. How far
// across it is, 0 to 1 (0.5: all of the picture covered): in over the last
// STING_IN ms (real time) before a replay cuts in, or before it is over, to
// the cut; then held over the picture while the other side of the cut is
// made ready to be drawn (`hold`), and on off it in as long.
export const STING_IN = 280;
// (All of the picture covered while it is this close to halfway across.)
export const STING_COVERS = 0.03;
export type Wipe = {
	// Whether a replay was showing last frame; when the picture last cut
	// behind the wipe, and when it was last held over all of the picture
	// (ms).
	was: boolean;
	cut: number | undefined;
	hold: number;
	// How far across it is (undefined: off the picture), and whether that
	// covers all of it.
	u: number | undefined;
	covered: boolean;
};
export const makeWipe = (): Wipe => ({
	was: false,
	cut: undefined,
	hold: -Infinity,
	u: undefined,
	covered: false,
});

export type PlayClock<R extends Replay = Replay> = {
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
	// Following another device's sim the old way: where its picture is
	// estimated to be (see follow.ts).
	lead: number;
	// The last frame's clock (ms).
	last: number | undefined;
	// The replay showing now, and the next play that would get one (its
	// index in the show's replays) - and, one coming, the moment of the game
	// it cuts in at (for the wipe into it: see stingIn).
	replay: R | undefined;
	nextReplay: number;
	replayDue?: number;
	snapCam: boolean;
	// Key plays: the cursor the page is catching up to (-1 none), and when
	// the picture last came up out of black onto the next one (ms) - and
	// when it was held black for the picture to be ready.
	jumpTo: number;
	keyDip: number;
	keyHold: number;
	// The cursor it last asked the page to move on from (-1 none).
	readyFor: number;
	// "Next play" while paused: show that one play, then hold.
	stepping: boolean;
	prevCursor: number;
	prevPaused: boolean;
	prevSkips: number;
	// Playing along: when (ms) it last had to cut to where the other picture
	// was - and, cut time after time, that it is not the same game over
	// there (see LOST_SLIPS).
	slips?: number[];
	lost?: boolean;
};

export const makePlayClock = <R extends Replay>(
	paused: boolean,
	skips: number,
): PlayClock<R> => ({
	t: 0,
	w: 0,
	pace: 1,
	paceAt: -1,
	lead: 0,
	last: undefined,
	replay: undefined,
	nextReplay: 0,
	snapCam: true,
	jumpTo: -1,
	keyDip: -Infinity,
	keyHold: -Infinity,
	readyFor: -1,
	stepping: false,
	prevCursor: -1,
	prevPaused: paused,
	prevSkips: skips,
});

// Before the cut: how far in it has come. Played at `base` times real
// time, the game going `rate` times as fast.
export const stingIn = (
	s: PlayClock,
	show: Replaying,
	base: number,
	rate: number,
): number | undefined => {
	const r = s.replay;
	if (r) {
		const left = (r.to - r.at) / (base * show.replaySpeed);
		return left < STING_IN ? 0.5 * (1 - left / STING_IN) : undefined;
	}
	if (s.replayDue === undefined) {
		return undefined;
	}
	// (In game time.)
	const lead = Math.min(STING_IN * rate, show.stingMost ?? Infinity);
	const d = s.replayDue - s.t;
	return d < lead ? 0.5 * (1 - Math.max(0, d) / lead) : undefined;
};
// The wipe this frame (after the clock's step - see stepShow).
export const wipeAt = (
	w: Wipe,
	s: PlayClock,
	show: Replaying,
	now: number,
	base: number,
	rate: number,
): number | undefined => {
	if (!!s.replay !== w.was) {
		w.was = !!s.replay;
		w.cut = now;
	}
	let u: number | undefined;
	if (w.cut !== undefined) {
		const e = now - Math.max(w.cut, w.hold);
		if (e < STING_IN) {
			u = 0.5 + (0.5 * e) / STING_IN;
		} else {
			w.cut = undefined;
		}
	}
	u ??= stingIn(s, show, base, rate);
	w.u = u;
	w.covered = u !== undefined && Math.abs(u - 0.5) <= STING_COVERS;
	return u;
};

// The cut nearest to t (for the dip to black across it), by binary search.
export const nearestCut = (cuts: number[], t: number): number | undefined => {
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

// Whether the picture went through a cut between `before` (not) and `t`.
const cutIn = (cuts: number[], before: number, t: number): boolean => {
	let lo = 0;
	let hi = cuts.length;
	while (lo < hi) {
		const mid = (lo + hi) >> 1;
		if (cuts[mid]! <= before) {
			lo = mid + 1;
		} else {
			hi = mid;
		}
	}
	return lo < cuts.length && cuts[lo]! <= t;
};

// The first of the show's replays not yet over by t (its cue after it).
const replayAfter = (show: Show, t: number) =>
	show.replays.findIndex((x) => x.cue > t);

// The show and the game run on together the same way on any device: in
// steps of STEP_MS of the show, the game at the pace it was going at each
// step's start - so where a frame happens to end makes no difference to
// where in the game a point in the show is, and two devices playing it
// agree on that through every quickening and slowing (see fastAt).
const STEP_MS = 8;

// On through `ms` of the show, no further than `limit` (timeline ms).
const runOn = (
	s: PlayClock,
	show: Show,
	base: number,
	ms: number,
	limit: number,
) => {
	let left = ms;
	for (let n = 0; n < 1000 && left > 1e-9 && s.t < limit; n++) {
		const k = Math.floor(s.w / STEP_MS + 1e-9);
		if (k !== s.paceAt) {
			s.pace = base * show.fastAt(s.t);
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

// Playing along, this many cuts to where the other picture was within this
// long (ms) - when, with it, there is one on joining late or after a skip
// there: the two are not showing the same game (an older build over there,
// say), and playing along only plays the same few seconds over and over -
// it is stepped line by line instead.
const LOST_SLIPS = 3;
const LOST_WITHIN = 20_000;
// (Or, a reel of the key plays, the other picture somewhere this one's reel
// leaves out - more than this far (ms) from any stretch of it: its reel is
// another, so the game it staged is.)
const OFF_REEL_MS = 1000;
const offReel = (plan: KeySegment[], t: number) =>
	!plan.some((x) => t > x.t0 - OFF_REEL_MS && t < x.t1 + OFF_REEL_MS);

// What the page says, this frame.
export type PlayInput = {
	cursor: number;
	paused: boolean;
	// How many times real time it plays at (1 is real time).
	rate: number;
	// Stepped by the device in charge of simming: never asks for a line.
	follower: boolean;
	// Playing along with it instead (see follow.ts).
	along: boolean;
	keyPlan: KeySegment[] | undefined;
	eventsLength: number;
	onReady: () => void;
	onJump?: (cursor: number) => void;
	report?: { current: Shown | undefined };
	// What this picture staged (see stagedAs), said along with where it is.
	fp?: string;
	// A clip (a highlight played on its own): no further than this
	// (timeline ms).
	stopAt?: number;
};

// How far on the picture may run with the page's cursor where it is: to the
// moment of the next line, its line going up as it gets there. Stepped by
// another device, through to the end of that play: the word of it comes
// late and unevenly, and a picture that stages a play quicker than the
// other one (another build there) would otherwise stand on it, seconds at a
// time, waiting for it.
const stepTarget = (tl: Beats, cursor: number, follower: boolean) => {
	if (!follower) {
		return targetForCursor(tl, cursor);
	}
	const beat = tl.beats.find((b) => b.i >= cursor);
	return beat ? beat.end : tl.end;
};

// Follow the page's cursor: run on to the next line normally; cut straight
// there on a rewind or a big jump ahead (fast-forward, joining late).
export const onCursor = (
	s: PlayClock,
	tl: Beats,
	show: Show,
	{
		cursor,
		paused,
		skips,
		follower,
	}: { cursor: number; paused: boolean; skips: number; follower: boolean },
) => {
	const target = stepTarget(tl, cursor, follower);
	// "Next play": past what is left of the play now shown, straight on to
	// the next one - never back.
	const skipped =
		s.prevCursor >= 0 && skips !== s.prevSkips && cursor > s.prevCursor;
	// (Caught up to a key play it went on to, or to the other picture: it is
	// there already. The page gets there a line at a time, so it can land
	// through the end of the line the jump fell in - one line on, not a jump
	// of its own. Cut to that line's end, it was past where the other picture
	// was, and cut back a second later.)
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
		s.paceAt = -1;
	}
	if (follower) {
		// Where the other picture is: just past the play it last reported
		// (it reports one the moment it gets there) - or, after a cut, where
		// this one now is.
		s.lead = snap
			? s.t
			: Math.max(s.lead, s.t, targetForCursor(tl, s.prevCursor));
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
		s.nextReplay = replayAfter(show, s.t);
	}
	s.prevCursor = cursor;
	s.prevPaused = paused;
	s.prevSkips = skips;
};

// Playing along: a cut to where the other picture was (see follow.ts) - up
// out of black when it is a long way, as onto the next key play.
const alignTo = <R extends Replay>(
	s: PlayClock<R>,
	p: PlayInput,
	f: Follow,
	show: Show<R>,
	r: FollowReport,
	now: number,
	dip: boolean,
) => {
	cutTo(f, r);
	s.t = r.t;
	s.w = r.w;
	s.paceAt = -1;
	const again =
		r.rp >= 0 ? show.replays.find((x) => Math.abs(x.cue - r.t) < 1) : undefined;
	s.replay = again ? { ...again.replay(), at: r.rp } : undefined;
	s.nextReplay = replayAfter(show, s.t);
	s.snapCam = true;
	if (dip) {
		s.keyDip = now;
		s.keyHold = -Infinity;
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

// One frame: the clock run on to `now` (ms; `wall` is Date.now()). How long
// the frame was, how fast the timeline ran through it, whether the picture
// cut - and where it ran on to (`ran`), before any jump on past what a reel
// leaves out, or to where another device's picture is. `staging`: the game
// not ready yet, the picture on something to show meanwhile (the court's
// warm-ups) - up to `staging` ms of it.
export const stepShow = <R extends Replay>(
	s: PlayClock<R>,
	p: PlayInput,
	f: Follow,
	tl: Beats,
	show: Show<R>,
	staging: number | undefined,
	now: number,
	wall: number,
): { dt: number; rate: number; cut: boolean; ran: number } => {
	const dt = s.last === undefined ? 0 : Math.min(100, now - s.last);
	s.last = now;
	// Onto the game from what was shown while it was staged: first to where
	// the page is. The page's cursor reaches the clock after the picture has
	// had a frame or two of the game - and run on from the other clock,
	// seconds in, it was past the first line.
	if (staging === undefined && s.prevCursor < 0) {
		onCursor(s, tl, show, {
			cursor: p.cursor,
			paused: p.paused,
			skips: s.prevSkips,
			follower: p.follower,
		});
	}
	// Playing along with another device's picture: as much of the show as
	// holds a steady gap behind it - or a cut to where it was.
	let ms = dt;
	if (p.along && staging === undefined) {
		const step = followStep(f, s.w, wall, dt);
		ms = step.ms;
		if (step.snap) {
			alignTo(s, p, f, show, step.snap, now, true);
		}
		const said = f.reports.at(-1);
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
	const target =
		staging !== undefined
			? Math.min(staging, s.t + dt)
			: Math.min(stepTarget(tl, p.cursor, p.follower), p.stopAt ?? Infinity);
	const base = p.rate;
	// Through the dead stretches, fast.
	let rate = base * show.fastAt(s.t);
	if (p.follower) {
		// Behind the device in charge of simming: hold a steady gap to where
		// its picture is, rather than running to the last play it reported
		// and stopping there. (Showing a play again, as it does too: its
		// clock held for it, as this one's is.)
		if (!p.paused && !s.replay) {
			s.lead = advanceLead(
				Math.max(s.lead, s.t),
				target,
				dt,
				base * show.fastAt(s.lead),
			);
		}
		const k = followRate(s.lead - s.t);
		rate *= k;
		ms *= k;
	}
	const before = s.t;
	// The next play that would be shown again, and whether it would be -
	// unless stepping play by play, or watching on fast.
	const replays = show.replays;
	while (
		s.nextReplay >= 0 &&
		s.nextReplay < replays.length &&
		replays[s.nextReplay]!.cue <= before
	) {
		s.nextReplay += 1;
	}
	const next = replays[s.nextReplay];
	const again =
		!!next &&
		!p.paused &&
		!s.stepping &&
		p.rate <= REPLAY_RATE_MAX &&
		// (A reel of the key plays moves on to the next - but for a close
		// finish, shown the way it happened.)
		(!p.keyPlan || segmentAt(p.keyPlan, next.cue).at?.close === true);
	const plan = p.keyPlan;
	const keyOn = !!plan && !p.paused;
	// A replay holds the live clock while it plays.
	const r = s.replay;
	if (staging !== undefined) {
		if (s.t < target) {
			s.t = Math.min(target, s.t + ms * rate);
		}
	} else if (r) {
		if (!p.paused || r.asked) {
			const at = r.at + ms * base * show.replaySpeed;
			s.w += (Math.min(at, r.to) - r.at) / (base * show.replaySpeed);
			r.at = at;
		}
		if (r.at >= r.to) {
			// Back to the game behind the wipe (see stingIn) - held over the
			// picture till everybody in it can be drawn at its size again.
			s.replay = undefined;
			s.snapCam = true;
			s.paceAt = -1;
		}
	} else if ((!p.paused || s.stepping) && s.t < target) {
		// On to the next line - stopping on the moment to show a play again,
		// or at the end of the key play shown, exactly there.
		// (Stepped by another device: on past the end of the play shown, if
		// that one isn't on to the next yet - it is still showing something
		// in between, its own reel being cut otherwise.)
		let seg = keyOn ? segmentAt(plan, s.t).at : undefined;
		if (seg && plan && p.follower) {
			const after = plan[plan.indexOf(seg) + 1];
			if (!after || p.cursor < after.cursor) {
				seg = undefined;
			}
		}
		runOn(
			s,
			show,
			base,
			ms,
			Math.min(target, again ? next.cue : Infinity, seg ? seg.t1 : Infinity),
		);
	}
	// A play just over that is shown again: show it.
	if (!s.replay && next && next.cue > before && next.cue <= s.t) {
		s.nextReplay += 1;
		if (again) {
			s.t = next.cue;
			s.replay = next.replay();
			s.snapCam = true;
			s.paceAt = -1;
		}
	}
	s.replayDue = again && !s.replay && next.cue > s.t ? next.cue : undefined;
	// Through a cut: the camera starts fresh on the other side.
	const ran = s.t;
	let cut = cutIn(show.cuts, before, s.t);
	// Key plays only: past what is left out, straight on to the next
	// stretch shown - the page catching the score and the lines up to
	// it on the way.
	if (keyOn && !s.replay && s.jumpTo < 0) {
		const { at, next: on } = segmentAt(plan, s.t);
		// (Stepped by another device, not before its page is there too.)
		if (!at && on && (!p.follower || p.cursor >= on.cursor)) {
			if (p.along) {
				noteShown(f, { w: s.w, t: s.t, rp: -1 });
			}
			s.t = on.t0;
			s.paceAt = -1;
			s.keyDip = now;
			cut = true;
			// (Stepped: the other picture went on to it first, and is on into
			// it by now.)
			if (p.follower) {
				s.lead = Math.max(s.lead, s.t + FOLLOW_BEHIND);
			}
			if (on.cursor > p.cursor && p.onJump) {
				s.jumpTo = on.cursor;
				p.onJump(on.cursor);
			}
		}
	}
	if (s.jumpTo >= 0 && p.cursor >= s.jumpTo) {
		s.jumpTo = -1;
	}
	// Playing along: still where the other picture was at each point it
	// said - or, if not (it skipped on), a cut to there.
	if (p.along && staging === undefined) {
		noteShown(f, { w: s.w, t: s.t, rp: s.replay?.at ?? -1 });
		const per = (t: number, rp: number) =>
			rp >= 0 ? base * show.replaySpeed : base * show.fastAt(t);
		for (const due of dueChecks(f, s.w)) {
			if (!agrees(f, due, per)) {
				alignTo(s, p, f, show, due, now, false);
				cut = true;
				break;
			}
		}
	}
	if (cut) {
		s.snapCam = true;
	}
	if (s.t >= target && !s.replay && staging === undefined) {
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
	if (p.report && staging === undefined) {
		p.report.current = {
			w: s.w,
			t: s.t,
			rp: s.replay?.at ?? -1,
			...(p.fp === undefined ? {} : { fp: p.fp }),
		};
	}
	return { dt, rate, cut, ran };
};
