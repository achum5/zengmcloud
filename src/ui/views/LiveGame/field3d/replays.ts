import type { Shot } from "./camera.ts";
import type { Beat, BallSeg, PlayKind, RawEvent } from "./director.ts";
import type { FieldMoment } from "./evaluate.ts";
import { dist, MID_Y, type Pt3 } from "./geometry.ts";

// A BIG PLAY SHOWN AGAIN: a score, a takeaway, a long gain - slowed down,
// from down on the sideline, once the play is over and before the next one
// (the 3D court's dunk replay, for football: see court3d/playback.ts).

export type FieldReplay = {
	// When the live picture cuts to it, and what it shows (timeline ms).
	cue: number;
	from: number;
	to: number;
	// Who it is of: the man with the ball at the moment it is shown for.
	pid?: number;
};

// How long after the whistle it cuts to it - after the signals, a moment
// of whoever made the play.
const REPLAY_AFTER = 2300;
// From this long before the moment it is shown for, to this long after.
const REPLAY_BEFORE = 3800;
const REPLAY_TO = 900;
// The most of a play it shows (timeline ms): the end of it, if it is long.
const REPLAY_MOST = 6000;
// Long enough a gain, or a return, to be shown again (yards).
const BIG_GAIN = 25;
const BIG_RETURN = 35;

const GAINS = new Set(["run", "passComplete"]);
const RETURNS = new Set(["kickoffReturn", "puntReturn", "interceptionReturn"]);
const SCORES = new Set([
	"run",
	"passComplete",
	"interceptionReturn",
	"kickoffReturn",
	"puntReturn",
	"fumbleRecovery",
]);

const holderAt = (ball: BallSeg[], t: number): number | undefined => {
	let pid: number | undefined;
	for (const s of ball) {
		if (s.t0 > t) {
			break;
		}
		if (s.kind === "held") {
			pid = s.pid;
		}
	}
	return pid;
};

export const replayFor = (input: {
	kind: PlayKind;
	events: RawEvent[];
	beats: Beat[];
	t0: number;
	snap: number;
	whistle: number;
	t1: number;
	ball: BallSeg[];
	// A flag that stands: what it was is shown instead.
	fouled: boolean;
}): FieldReplay | undefined => {
	const { events, beats, whistle } = input;
	if (input.fouled || input.kind === "kneel" || input.kind === "none") {
		return undefined;
	}
	// The moment it is shown for: a score above all, then the ball taken
	// away, then the long gain.
	let key: number | undefined;
	let to: number | undefined;
	let fumbleAt: number | undefined;
	let lost = false;
	for (const b of beats) {
		const e = events[b.i]!;
		const yds = typeof e.yds === "number" ? e.yds : 0;
		if (e.td === true && SCORES.has(e.type)) {
			key = Math.max(b.actionStart, whistle);
			to = undefined;
			break;
		}
		if (e.type === "fumble") {
			fumbleAt ??= b.actionStart;
		} else if (e.type === "interception") {
			key ??= b.actionStart;
		} else if (e.type === "fumbleRecovery") {
			// From the ball coming out, through to whoever has it at the last
			// (loose again, maybe, after it was had).
			if (key === undefined && e.lost === true) {
				key = fumbleAt ?? b.actionStart;
				lost = true;
			}
			if (lost) {
				to = b.actionStart + REPLAY_TO;
			}
		} else if (
			(GAINS.has(e.type) && yds >= BIG_GAIN) ||
			(RETURNS.has(e.type) && yds >= BIG_RETURN)
		) {
			key ??= whistle;
		}
	}
	if (key === undefined) {
		return undefined;
	}
	const cue = Math.min(input.t1 - 700, whistle + REPLAY_AFTER);
	const end = Math.min(cue, to ?? key + REPLAY_TO);
	const from = Math.max(
		input.t0,
		input.snap - 300,
		key - REPLAY_BEFORE,
		end - REPLAY_MOST,
	);
	if (end - from < 1000 || cue <= whistle) {
		return undefined;
	}
	const pid = holderAt(input.ball, key);
	return { cue, from, to: end, ...(pid === undefined ? {} : { pid }) };
};

// Where the replay's camera looks: the ball, and the man it is of while
// he is anywhere near it - tight, opening up only as far as it takes to
// have them both, and up after a ball in the air.
const TIGHT = 58;
const LOOSE = 140;
const ROOM = 34;
const NEAR = 70;

export const replayAim = (m: FieldMoment, r: FieldReplay): Shot => {
	const pts: Pt3[] = [];
	if (m.focus) {
		pts.push(m.focus);
	}
	const him =
		r.pid === undefined ? undefined : m.players.find((p) => p.id === r.pid);
	if (him && (!m.focus || dist(him, m.focus) < NEAR)) {
		pts.push({ x: him.x, y: him.y, z: 3.5 });
	}
	if (pts.length === 0) {
		return { x: m.play?.losX ?? 180, width: 90, y: MID_Y };
	}
	let lo = Infinity;
	let hi = -Infinity;
	let ys = 0;
	let high = 0;
	for (const p of pts) {
		lo = Math.min(lo, p.x);
		hi = Math.max(hi, p.x);
		ys += p.y;
		high = Math.max(high, p.z);
	}
	const up = Math.max(0, high - 12);
	return {
		x: (lo + hi) / 2,
		width: Math.min(LOOSE, Math.max(TIGHT, hi - lo + ROOM)) + up * 0.9,
		y: ys / pts.length,
		z: 4 + up * 0.6,
	};
};
