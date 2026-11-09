import type { CourtTimeline, Fx } from "./director.ts";
import { cameraCuts, fastAt } from "./evaluate.ts";
import type { Follow } from "./follow.ts";
import { WARMUP_MS } from "./pregame.ts";
import type { Side } from "./geometry.ts";
import {
	onCursor as onShowCursor,
	stepShow,
	wipeAt as showWipeAt,
	type PlayClock as ShowClock,
	type PlayInput,
	type Replay as ShowReplay,
	nearestCut,
	type Show,
	type Wipe,
} from "./show.ts";

export { makeWipe, STING_COVERS, STING_IN } from "./show.ts";
export type { PlayInput, Wipe } from "./show.ts";

// THE 3D COURT'S CLOCK, frame by frame (the engine's: see show.ts): how far
// along the timeline the picture is, run on toward the page's cursor (see
// Court3D) - fast through the dead stretches, a big dunk shown again, the
// plays a reel of key plays leaves out cut past - and, watching along with
// another device's court, kept a steady moment behind it (see follow.ts).

// A dunk's replay: from just before he takes off to just after, in slow
// motion, once its own play is over. (A big basket's from his gather -
// longer before it goes in.)
const REPLAY_FROM = 1700;
const REPLAY_FROM_THREE = 2300;
const REPLAY_TO = 450;
// A block's: from as he goes up with it, on past it as the ball goes away.
const REPLAY_FROM_BLOCK = 1500;
const REPLAY_TO_BLOCK = 800;
export const REPLAY_AFTER = 1150;
const REPLAY_SPEED = 0.42;
// Which plays are shown again: every dunk, the way a broadcast shows them
// all again - not only a big one (on somebody, off a lob, through contact)
// - and a big basket: a three in a tight finish, anything at the buzzer,
// an and-one; and a big block, at the rim, a dunk sent back (see Fx).
export const replayed = (f: Fx) =>
	f.kind === "dunk" || (!!f.big && (f.kind === "swish" || f.kind === "block"));

// A replay showing (see show.ts) - and who it is of, the man who put the
// ball in, and the rim he put it in, for the camera to stay on (see
// replayAim); and, a dunk, when he threw it down (`slam`), for the camera
// to go round under the rim for it (see replayRig). (Asked for from the
// page - a highlight, see askedReplay - it plays even with the game paused.)
export type Replay = ShowReplay & {
	pid?: number;
	rim?: Side;
	slam?: number;
	// And the man who sent it back, a block's - kept in the picture too.
	also?: number;
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
	const block = f.kind === "block";
	const from =
		f.t -
		(f.kind === "swish"
			? REPLAY_FROM_THREE
			: block
				? REPLAY_FROM_BLOCK
				: REPLAY_FROM);
	const pid = lastHolder(tl, f.t);
	// (A block's rim: the one the shot went at - the other side's than the
	// blocker's.)
	const rim = block && f.team !== undefined ? (f.team === 0 ? 1 : 0) : f.rim;
	return {
		at: from,
		from,
		to: f.t + (block ? REPLAY_TO_BLOCK : REPLAY_TO),
		...(pid === undefined ? {} : { pid }),
		...(rim === undefined ? {} : { rim }),
		...(f.kind === "dunk" ? { slam: f.t } : {}),
		...(block && f.pid !== undefined ? { also: f.pid } : {}),
	};
};

// A PLAY SHOWN AGAIN BECAUSE THE PAGE ASKED (a highlight tapped): the line
// that ended at `cursor` - from the beat before it (the shot, for the line
// that says how it went) to the end of its own - shown the way a dunk is
// shown again, off the moment the court marked in it (the basket, the
// block), and then back to the game where it was.
export const askedReplay = (
	tl: CourtTimeline,
	cursor: number,
): Replay | undefined => {
	const i = cursor - 1;
	let k = -1;
	for (let j = 0; j < tl.beats.length; j++) {
		if (tl.beats[j]!.i <= i) {
			k = j;
		} else {
			break;
		}
	}
	const beat = tl.beats[k];
	if (!beat) {
		return undefined;
	}
	const from = tl.beats[k - 1]?.preStart ?? beat.preStart;
	const marked = tl.fx.find(
		(f) =>
			(f.kind === "dunk" || f.kind === "swish" || f.kind === "block") &&
			f.t >= from &&
			f.t <= beat.end + 300,
	);
	if (marked) {
		return { ...replayOf(tl, marked), asked: true };
	}
	const pid = lastHolder(tl, beat.actionStart);
	const start = Math.max(from, beat.end - REPLAY_FROM);
	return {
		at: start,
		from: start,
		to: beat.end + REPLAY_TO,
		...(pid === undefined ? {} : { pid }),
		asked: true,
	};
};

// How the court is shown (see show.ts): fast through the dead stretches
// (see fastAt), the camera's cuts (see cameraCuts), and the plays shown
// again - their replays from REPLAY_AFTER past each, slowed right down, the
// wipe in never back over the play itself.
const REPLAYS = { replaySpeed: REPLAY_SPEED, stingMost: REPLAY_AFTER - 400 };
const shows = new WeakMap<CourtTimeline, Show<Replay>>();
export const courtShow = (tl: CourtTimeline): Show<Replay> => {
	let show = shows.get(tl);
	if (!show) {
		show = {
			...REPLAYS,
			fastAt: (t) => fastAt(tl, t),
			cuts: cameraCuts(tl),
			replays: tl.fx.filter(replayed).map((f) => ({
				cue: f.t + REPLAY_AFTER,
				replay: () => replayOf(tl, f),
			})),
		};
		shows.set(tl, show);
	}
	return show;
};

export type PlayClock = ShowClock<Replay>;

// A CLIP'S END (a highlight played on its own - see Court3D's `until`): a
// moment past the end of the last line before `cursor` - and if it is a dunk
// shown again, past that.
const CLIP_TAIL = 1800;
export const clipStopAt = (tl: CourtTimeline, cursor: number): number => {
	const beat = tl.beats.findLast((b) => b.i < cursor);
	if (!beat) {
		return tl.end;
	}
	let stop = beat.end + CLIP_TAIL;
	for (const f of tl.fx) {
		if (replayed(f) && f.t >= beat.preStart && f.t <= beat.end + 300) {
			stop = Math.max(stop, f.t + REPLAY_AFTER + 100);
		}
	}
	return Math.min(tl.end, stop);
};

// Follow the page's cursor: run on to the next line normally; cut straight
// there on a rewind or a big jump ahead (fast-forward, joining late).
export const onCursor = (
	s: PlayClock,
	timeline: CourtTimeline,
	input: { cursor: number; paused: boolean; skips: number; follower: boolean },
) => {
	onShowCursor(s, timeline, courtShow(timeline), input);
};

// One frame: the clock run on to `now` (ms; `wall` is Date.now()) - the
// warm-ups' while `staging` (see pregame.ts). How long the frame was, how
// fast the timeline ran through it, and the camera cut nearest where it got
// to (for the dip to black across it: see Court3D).
export const stepPlay = (
	s: PlayClock,
	p: PlayInput,
	f: Follow,
	tl: CourtTimeline,
	staging: boolean,
	now: number,
	wall: number,
): { dt: number; rate: number; cut: number | undefined } => {
	const show = courtShow(tl);
	const { dt, rate, ran } = stepShow(
		s,
		p,
		f,
		tl,
		show,
		staging ? WARMUP_MS : undefined,
		now,
		wall,
	);
	return { dt, rate, cut: nearestCut(show.cuts, ran) };
};

// The wipe this frame (after the clock's step - see stepPlay). Played at
// `base` times real time, the game going `rate` times as fast.
export const wipeAt = (
	w: Wipe,
	s: PlayClock,
	now: number,
	base: number,
	rate: number,
): number | undefined => showWipeAt(w, s, REPLAYS, now, base, rate);
