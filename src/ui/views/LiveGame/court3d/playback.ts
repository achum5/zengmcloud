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
import { WARMUP_MS } from "./pregame.ts";
import type { Side } from "./geometry.ts";

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
// Which plays are shown again: every dunk, the way a broadcast shows them
// all again - not only a big one (on somebody, off a lob, through contact)
// - and a big three (see Fx).
export const replayed = (f: Fx) =>
	f.kind === "dunk" || (!!f.big && f.kind === "swish");

// THE WIPE: a replay is gone into, and come out of, behind a wipe across
// the picture in the home team's colors, the way a broadcast does it - the
// cut made while it covers all of it (see Court3D). How far across it is,
// 0 to 1 (0.5: all of the picture covered): in over the last STING_IN ms
// (real time) before a replay cuts in, or before it is over, to the cut;
// then held over the picture while the other side of the cut is made
// ready to be drawn (`hold`, see Court3D), and on off it in as long.
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
// Before the cut: how far in it has come. Played at `base` times real
// time, the game going `rate` times as fast.
export const stingIn = (
	s: PlayClock,
	base: number,
	rate: number,
): number | undefined => {
	const r = s.replay;
	if (r) {
		const left = (r.to - r.at) / (base * REPLAY_SPEED);
		return left < STING_IN ? 0.5 * (1 - left / STING_IN) : undefined;
	}
	if (s.replayDue === undefined) {
		return undefined;
	}
	// (In game time - and never back over the dunk itself.)
	const lead = Math.min(STING_IN * rate, REPLAY_AFTER - 400);
	const d = s.replayDue - s.t;
	return d < lead ? 0.5 * (1 - Math.max(0, d) / lead) : undefined;
};
// The wipe this frame (after the clock's step - see stepPlay).
export const wipeAt = (
	w: Wipe,
	s: PlayClock,
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
	u ??= stingIn(s, base, rate);
	w.u = u;
	w.covered = u !== undefined && Math.abs(u - 0.5) <= STING_COVERS;
	return u;
};

// A replay showing: where it is (`at`), from and to (timeline ms) - and who
// it is of, the man who put the ball in, and the rim he put it in, for the
// camera to stay on (see replayAim).
export type Replay = {
	at: number;
	from: number;
	to: number;
	pid?: number;
	rim?: Side;
};

// Who had the ball last at or before t: the man who dunked it (it leaves his
// hands at the rim) or shot it.
export const lastHolder = (
	tl: CourtTimeline,
	t: number,
): number | undefined => {
	const ball = tl.ball;
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
	for (; k >= 0; k--) {
		const s = ball[k]!;
		if (s.kind === "hold") {
			return s.pid;
		}
		if (s.kind === "fly" && "pid" in s.from) {
			return s.from.pid;
		}
	}
	return undefined;
};

export const replayOf = (tl: CourtTimeline, f: Fx): Replay => {
	const from = f.t - (f.kind === "swish" ? REPLAY_FROM_THREE : REPLAY_FROM);
	const pid = lastHolder(tl, f.t);
	return {
		at: from,
		from,
		to: f.t + REPLAY_TO,
		...(pid === undefined ? {} : { pid }),
		...(f.rim === undefined ? {} : { rim: f.rim }),
	};
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
	// The dunk replay showing now, and the next dunk that would get one -
	// and, one coming, the moment of the game it cuts in at (for the wipe
	// into it: see stingIn).
	replay: Replay | undefined;
	nextDunk: number;
	replayDue?: number;
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
	// Playing along: when (ms) it last had to cut to where the other court
	// was - and, cut time after time, that it is not the same game over
	// there (see LOST_SLIPS).
	slips?: number[];
	lost?: boolean;
};

// Playing along, this many cuts to where the other court was within this
// long (ms) - when, with it, there is one on joining late or after a skip
// there: the two courts are not showing the same game (an older build over
// there, say), and playing along only plays the same few seconds over and
// over - it is stepped line by line instead (see Court3D).
const LOST_SLIPS = 3;
const LOST_WITHIN = 20_000;
// (Or, a reel of the key plays, the other court somewhere this one's reel
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
	// Stepped by the device in charge of simming (see Court3D's props).
	follower: boolean;
	// Playing along with it instead (see follow.ts).
	along: boolean;
	keyPlan: KeySegment[] | undefined;
	eventsLength: number;
	onReady: () => void;
	onJump?: (cursor: number) => void;
	report?: { current: Shown | undefined };
	// What this court staged (see stagedAs), said along with where it is.
	fp?: string;
};

// How far on the picture may run with the page's cursor where it is: to the
// moment of the next line, its line going up as it gets there. Stepped by
// another device, through to the end of that play: the word of it comes
// late and unevenly, and a court that stages a possession quicker than the
// other one (another build there) would otherwise stand on the shot going
// up, seconds at a time, waiting for it.
const stepTarget = (tl: CourtTimeline, cursor: number, follower: boolean) => {
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
	timeline: CourtTimeline,
	{
		cursor,
		paused,
		skips,
		follower,
	}: { cursor: number; paused: boolean; skips: number; follower: boolean },
) => {
	const target = stepTarget(timeline, cursor, follower);
	// "Next play": past what is left of the play now shown, straight on to
	// the next one - never back.
	const skipped =
		s.prevCursor >= 0 && skips !== s.prevSkips && cursor > s.prevCursor;
	// (Caught up to a key play it went on to, or to the other court: it is
	// there already. The page gets there a line at a time, so it can land
	// through the end of the line the jump fell in - one line on, not a jump
	// of its own. Cut to that line's end, it was past where the other court
	// was, and cut back a second later.)
	const expected =
		s.jumpTo >= 0 &&
		(cursor <= s.jumpTo || linesBetween(timeline, s.jumpTo, cursor) <= 1);
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
	s.replay = dunk ? { ...replayOf(tl, dunk), at: r.rp } : undefined;
	s.nextDunk = tl.fx.findIndex((x) => replayed(x) && x.t + REPLAY_AFTER > s.t);
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
	// Onto the game from the warm-ups: first to where the page is. The
	// page's cursor reaches the clock after the picture has had a frame or
	// two of the game (see Court3D) - and run on from the warm-ups' own
	// clock, seconds in, it was past the tip, so the first line (who won
	// the jump ball) went up, and the game clock on the score bug started,
	// over the look round the building before the ball was thrown up.
	if (!staging && s.prevCursor < 0) {
		onCursor(s, tl, {
			cursor: p.cursor,
			paused: p.paused,
			skips: s.prevSkips,
			follower: p.follower,
		});
	}
	// Playing along with another device's court: as much of the show as
	// holds a steady gap behind it - or a cut to where it was.
	let show = dt;
	if (p.along && !staging) {
		const step = followStep(f, s.w, wall, dt);
		show = step.ms;
		if (step.snap) {
			alignTo(s, p, f, tl, step.snap, now, true);
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
	const target = staging
		? Math.min(WARMUP_MS, s.t + dt)
		: stepTarget(tl, p.cursor, p.follower);
	const base = p.rate;
	// Through the dead stretches, fast.
	let rate = base * fastAt(tl, s.t);
	if (p.follower) {
		// Behind the device in charge of simming: hold a steady gap to
		// where its court is, rather than running to the last play it
		// reported and stopping there. (Showing a dunk again, as it does
		// too: its clock held for it, as this one's is.)
		if (!p.paused && !s.replay) {
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
	// unless stepping play by play, or watching on fast.
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
		!p.paused &&
		!s.stepping &&
		p.rate <= REPLAY_RATE_MAX &&
		// (A reel of the key plays moves on to the next - but for a close
		// finish, shown the way it happened.)
		(!p.keyPlan || segmentAt(p.keyPlan, dunk.t).at?.close === true);
	const plan = p.keyPlan;
	const keyOn = !!plan && !p.paused;
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
			// Back to the game behind the wipe (see stingIn) - held over the
			// picture till everybody in it can be drawn at its size again.
			s.replay = undefined;
			s.snapCam = true;
			s.paceAt = -1;
		}
	} else if ((!p.paused || s.stepping) && s.t < target) {
		// On to the next line - stopping on the moment to show a dunk again,
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
			s.replay = replayOf(tl, dunk);
			s.snapCam = true;
			s.paceAt = -1;
		}
	}
	s.replayDue =
		again && dunk && !s.replay && dunk.t + REPLAY_AFTER > s.t
			? dunk.t + REPLAY_AFTER
			: undefined;
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
		// (Stepped by another device, not before its page is there too.)
		if (!at && next && (!p.follower || p.cursor >= next.cursor)) {
			if (p.along) {
				noteShown(f, { w: s.w, t: s.t, rp: -1 });
			}
			s.t = next.t0;
			s.paceAt = -1;
			s.snapCam = true;
			s.keyDip = now;
			// (Stepped: the other court went on to it first, and is on into
			// it by now.)
			if (p.follower) {
				s.lead = Math.max(s.lead, s.t + FOLLOW_BEHIND);
			}
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
		p.report.current = {
			w: s.w,
			t: s.t,
			rp: s.replay?.at ?? -1,
			...(p.fp === undefined ? {} : { fp: p.fp }),
		};
	}
	return { dt, rate, cut };
};
