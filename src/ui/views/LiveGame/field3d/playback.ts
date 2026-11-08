import type { FieldTimeline } from "./director.ts";
import type { KeySegment } from "../court3d/keyPlays.ts";
import { makeFollow, type Follow, type Shown } from "../court3d/follow.ts";
import {
	makePlayClock,
	makeWipe,
	onCursor,
	stepShow,
	wipeAt as showWipeAt,
	type PlayClock,
	type Show,
	type Wipe,
} from "../court3d/show.ts";
import type { FieldReplay } from "./replays.ts";

// THE 3D FIELD'S CLOCK, frame by frame - the engine's (see court3d/show.ts):
// how far along the timeline the picture is, run on toward the page's
// cursor, and at each line it gets to, asking the page for it - and a big
// play, once it is over, shown again (see replays.ts). Or, the key plays
// only (see keyPlays.ts), on from each one shown straight to the next. And,
// watching along with another device's field, kept a steady moment behind
// it (see court3d/follow.ts).

export const REPLAY_SPEED = 0.55;

// A replay showing: where in it the picture is (`at`, timeline ms).
export type FieldReplayShowing = FieldReplay & { at: number };

export type FieldClock = PlayClock<FieldReplayShowing>;
export const makeFieldClock = (paused: boolean, skips: number): FieldClock =>
	makePlayClock(paused, skips);

// How a game of football is shown (see court3d/show.ts): at the pace it was
// staged at, cut where the director cut it, and its big plays shown again
// (see replays.ts).
const REPLAYS = { replaySpeed: REPLAY_SPEED };
const shows = new WeakMap<FieldTimeline, Show<FieldReplayShowing>>();
const fieldShow = (tl: FieldTimeline): Show<FieldReplayShowing> => {
	let show = shows.get(tl);
	if (!show) {
		show = {
			...REPLAYS,
			fastAt: () => 1,
			cuts: tl.cuts,
			replays: tl.plays.flatMap((play) => {
				const r = play.replay;
				return r ? [{ cue: r.cue, replay: () => ({ ...r, at: r.from }) }] : [];
			}),
		};
		shows.set(tl, show);
	}
	return show;
};

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
	onCursor(s, tl, fieldShow(tl), { cursor, paused, skips, follower });
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

// (Not playing along with anybody.)
const ALONE = makeFollow();

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
	const { dt, cut } = stepShow(
		s,
		{
			...p,
			rate: Math.max(1e-6, p.rate),
			along: !!(p.along && f),
			keyPlan: p.keyPlan,
		},
		f ?? ALONE,
		tl,
		fieldShow(tl),
		undefined,
		now,
		wall,
	);
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

// THE WIPE into a replay and out of it (see court3d/show.ts).
export const makeFieldWipe = (): Wipe => makeWipe();
// The wipe this frame (after the clock's step - see stepField).
export const wipeAt = (
	w: Wipe,
	s: FieldClock,
	p: Pick<FieldPlayInput, "rate">,
	now: number,
): number | undefined => {
	const rate = Math.max(1e-6, p.rate);
	return showWipeAt(w, s, REPLAYS, now, rate, rate);
};
