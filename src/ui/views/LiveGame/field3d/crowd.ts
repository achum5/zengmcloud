import type { FieldTimeline, Play } from "./director.ts";
import { playAt } from "./evaluate.ts";
import { MID_X, type Dir, type Side } from "./geometry.ts";
import type { BoardScreen, Shout } from "./stadium.ts";

// THE CROWD ON ITS FEET: up for a big play of the home team's (and for the
// team out of its tunnel before the game), arms going -
// the road team's fans, a few of them - and sitting back down (the 3D
// court's, for football: see court3d/scene.ts crowdUp).

// Up over this long (ms), held, and back down; held longer for a score, a
// takeaway.
const RISE = 300;
const HOLD = 2200;
const HOLD_BIG = 3800;
const FALL = 1400;
// How much of the stadium stands for the road team - at a neutral site,
// half of it for each.
const ROAD = 0.3;
// (There, the defense's half up and loud on a third or fourth down, this
// much of what a home crowd would be.)
const NEUTRAL_LOUD = 0.6;

// The big play the crowd is up for at t, if any - its own play's, or still
// up from the one before: for which side, how far up (0 to 1), and how long
// since it came.
export const cheerAt = (
	tl: FieldTimeline,
	t: number,
):
	| { side: Side; big: boolean; level: number; since: number; what?: Shout }
	| undefined => {
	const play = playAt(tl, t);
	const k = play ? tl.plays.indexOf(play) : -1;
	let out: ReturnType<typeof cheerAt>;
	// (The home team out of its tunnel too: see tunnel.ts.)
	for (const c of [
		tl.plays[k]?.cheer,
		tl.plays[k]?.runOut?.cheer,
		tl.plays[k - 1]?.cheer,
	]) {
		if (!c || c.t > t) {
			continue;
		}
		const e = t - c.t;
		const hold = c.hold ?? (c.big ? HOLD_BIG : HOLD);
		const level =
			e < RISE ? e / RISE : e < hold ? 1 : Math.max(0, 1 - (e - hold) / FALL);
		if (level > 0 && (!out || level > out.level)) {
			out = {
				side: c.side,
				big: c.big,
				level,
				since: e,
				...(c.what ? { what: c.what } : {}),
			};
		}
	}
	return out;
};

// The stretches the men on the sidelines are up for their side's big plays
// round t (see sideline.ts): from when its cheer gets this far up to when it
// falls back under it - the play's at t, and still the one before's.
export const upSpansAt = (
	tl: FieldTimeline,
	t: number,
	at: number,
): { side: Side; from: number; to: number }[] => {
	const play = playAt(tl, t);
	const k = play ? tl.plays.indexOf(play) : -1;
	const out: { side: Side; from: number; to: number }[] = [];
	for (const c of [
		tl.plays[k - 1]?.cheer,
		tl.plays[k]?.cheer,
		tl.plays[k]?.runOut?.cheer,
	]) {
		if (c) {
			const hold = c.hold ?? (c.big ? HOLD_BIG : HOLD);
			out.push({
				side: c.side,
				from: c.t + RISE * at,
				to: c.t + hold + (1 - at) * FALL,
			});
		}
	}
	return out;
};

// The stands at t: how much of the crowd is on its feet (0 to 1), arms
// waving rather than up, and how full they are (see director.ts seatsOf) -
// and the stands behind either end zone, where they are up on their own.
type Stand = { up: number; wave: boolean };
export type Crowd = Stand & {
	full: number;
	ends?: [Stand | undefined, Stand | undefined];
	// The wave, where it is along the field and the way it goes (see
	// rollingAt).
	rolling?: { x: number; dir: Dir };
};

// THE WAVE, over a quarter break (see director.ts): from the look round the
// stadium on, a band of the crowd across the field up out of their seats,
// arms high, rolling along the lower bowl at this pace (feet a second) -
// this far short of the middle of it as the picture comes to it.
const WAVE_FPS = 55;
const WAVE_FROM = 120;
export const rollingAt = (
	play: Play | undefined,
	t: number,
): { x: number; dir: Dir } | undefined => {
	const w = play?.wave;
	// (Until the picture goes to the huddles, if it does.)
	if (!play || !w || t < w.t || t > (play.timeout?.cut ?? play.t1)) {
		return undefined;
	}
	return {
		x: MID_X + w.dir * ((WAVE_FPS * (t - w.t)) / 1000 - WAVE_FROM),
		dir: w.dir,
	};
};

// At the final whistle (the 3D court's final horn: see court3d/scene.ts
// buildingAt), a home win brings the whole stadium up - arms going, then on
// its feet applauding as the teams shake hands; a road win sits it down
// (but for the road fans' few seconds, the way they take a big play).
const HORN_UP = 260;
const HORN_WAVE = 5000;
const HORN_SETTLE = 4000;
const HORN_STAY = 0.55;

// Third down or fourth, the visitors with the ball: the home crowd up and
// loud to put them off (the 3D court's fans behind the basket at the
// visitors' free throws, for football: see court3d/scene.ts endsAt) - up as
// the huddle breaks, down once the ball is dead.
const LOUD_UP = 900;
const LOUD_FROM = new Set(["pass", "run", "none"]);
// (A try for two, as loud as a fourth down.)
const loudOf = (situation: string): number =>
	situation.startsWith("3rd")
		? 0.7
		: situation.startsWith("4th") || situation === "2-Pt Try"
			? 0.85
			: 0;

const loudAt = (play: Play, t: number, neutral: boolean): number => {
	const level = loudOf(play.situation) * (neutral ? NEUTRAL_LOUD : 1);
	if (
		(play.offense !== 0 && !neutral) ||
		level === 0 ||
		!LOUD_FROM.has(play.kind)
	) {
		return 0;
	}
	return (
		level *
		Math.max(
			0,
			Math.min(1, (t - play.t0) / LOUD_UP, 1 - (t - play.whistle) / FALL),
		)
	);
};

// THE FANS BEHIND THE POSTS (the 3D court's behind the basket at the
// visitors' free throws: see court3d/scene.ts endsAt): the visitors kicking
// at them - a field goal, an extra point - the home fans in the stands
// behind the end line it goes to are up, arms going, to put the kicker off:
// from the kicking team out of its huddle until the ball is through (or
// short), then sat again. (A home kicker they leave be.)
const POSTS_WAVE_FROM = 600;
const POSTS_WAVE_UP_MS = 300;
const POSTS_WAVE_DOWN_MS = 600;
const POSTS_WAVE_EVERY = 90;
const KICKS = new Set(["fieldGoal", "extraPoint"]);
const postsAt = (
	play: Play,
	t: number,
	now: number,
): Crowd["ends"] | undefined => {
	if (!KICKS.has(play.kind) || play.offense !== 0) {
		return undefined;
	}
	// (The kick: the second flight from the snap, after the snap back to the
	// holder.)
	const kick = play.ball.filter(
		(b) => b.kind === "fly" && b.t0 >= play.snap,
	)[1];
	const over = kick?.kind === "fly" ? kick.t1 : play.whistle;
	const up = Math.max(
		0,
		Math.min(
			1,
			(t - play.t0 - POSTS_WAVE_FROM) / POSTS_WAVE_UP_MS,
			1 - (t - over) / POSTS_WAVE_DOWN_MS,
		),
	);
	if (up <= 0) {
		return undefined;
	}
	const end = { up, wave: Math.floor(now / POSTS_WAVE_EVERY) % 2 === 0 };
	return play.dir === 1 ? [undefined, end] : [end, undefined];
};

// THE KICKOFF OF A HALF (and of overtime): the stands on their feet for
// it, from a moment before the kick till the return is over - and back
// down in their seats.
const KICK_QUARTERS = [1, 3, 5];
const KICK_STAND_FROM = 4500;
const KICK_STAND_UP_MS = 700;
const KICK_STAND_AFTER = 600;
const KICK_SIT_MS = 2200;
const KICK_STAND = 0.85;
const halfKicks = new WeakMap<FieldTimeline, { n: number; plays: Play[] }>();
const halfKickoff = (tl: FieldTimeline, t: number): number => {
	let had = halfKicks.get(tl);
	if (!had || had.n !== tl.plays.length) {
		const plays = KICK_QUARTERS.flatMap((q) => {
			const first = tl.plays.find((p) => p.quarter === q && p.kind !== "none");
			return first?.kind === "kickoff" ? [first] : [];
		});
		had = { n: tl.plays.length, plays };
		halfKicks.set(tl, had);
	}
	let up = 0;
	for (const p of had.plays) {
		const from = p.snap - KICK_STAND_FROM;
		const sit = p.whistle + KICK_STAND_AFTER;
		if (t >= from && t <= sit + KICK_SIT_MS) {
			up = Math.max(
				up,
				KICK_STAND *
					Math.min(
						1,
						(t - from) / KICK_STAND_UP_MS,
						1 - (t - sit) / KICK_SIT_MS,
					),
			);
		}
	}
	return up;
};

export const crowdAt = (
	tl: FieldTimeline,
	t: number,
	now: number,
	// At a neutral site: as many fans for the one team as the other.
	neutral = false,
): Crowd => {
	const play = playAt(tl, t);
	const full = play?.seats ?? 1;
	const fast = Math.sin(now / 130) > 0;
	const slow = Math.sin(now / 520) > 0.55;
	const c = cheerAt(tl, t);
	const roar = Math.max(
		c ? c.level * (c.side === 1 || neutral ? 1 : ROAD) * (c.big ? 1 : 0.75) : 0,
		shotUp(play, t),
	);
	const post = play?.postgame;
	if (post && t >= post.from) {
		if (post.winner !== 1 && !neutral) {
			// (Whatever the finish had it doing.)
			return { up: roar, wave: roar > 0.3 && fast, full };
		}
		const e = t - post.from;
		const level =
			e < HORN_UP
				? e / HORN_UP
				: e < HORN_WAVE
					? 1
					: Math.max(
							HORN_STAY,
							1 - ((1 - HORN_STAY) * (e - HORN_WAVE)) / HORN_SETTLE,
						);
		return {
			up: Math.max(level, roar),
			wave: e < HORN_WAVE || roar > 0.3 ? fast : slow,
			full,
		};
	}
	// In crunch time, on its feet the whole way, arms going up now and then
	// - but for a moment after a big play of the visitors', stunned.
	const tense = play?.tense ?? 0;
	const loud = play ? loudAt(play, t, neutral) : 0;
	const hush = c && c.side === 0 && c.big && !neutral ? 1 - c.level : 1;
	const ends = play && !neutral ? postsAt(play, t, now) : undefined;
	const rolling = rollingAt(play, t);
	return {
		up: Math.max(
			roar,
			hush * Math.max(tense >= 1 ? 0.9 : tense * 0.7, loud),
			halfKickoff(tl, t),
		),
		wave: roar > 0.3 ? fast : tense >= 1 && slow,
		full,
		...(ends ? { ends } : {}),
		...(rolling ? { rolling } : {}),
	};
};

// THE PICTURE ON THE CROWD (the 3D court's look up at the crowd over a
// break: see court3d/scene.ts crowdShotAt): now and then, for the end of a
// timeout the cheerleaders are not on, cut away to the stands across the
// field from down low on the near side - panning along them, the crowd up
// and waving at it as it sees itself on the board - and from them to the
// next play (see aim.ts). From when, and where along them it starts.
export type CrowdShot = { cut: number; x: number };
const SHOT_SHARE = 0.5;
const SHOT_MS = 2800;
const SHOT_INTO = 1500;
const SHOT_UP = 350;

export const crowdShotFor = (
	cut: number,
	t1: number,
	r: () => number,
): CrowdShot | undefined => {
	const from = t1 - SHOT_MS;
	if (r() >= SHOT_SHARE || from < cut + SHOT_INTO) {
		return undefined;
	}
	return { cut: from, x: 70 + r() * 190 };
};

// How far up for the picture on it (0 to 1).
const shotUp = (play: Play | undefined, t: number): number => {
	const shot = play?.timeout?.crowd;
	return shot && t >= shot.cut ? Math.min(1, (t - shot.cut) / SHOT_UP) : 0;
};

// WHAT THE BOARDS SHOW (the 3D court's: see court3d/scene.ts boardAt): a
// shout for the home side's big play, DEFENSE (blinking) while the visitors
// have the ball from scrimmage, LET'S GO and MAKE SOME NOISE in turn with
// the home team's ball in crunch time; otherwise the team and the chants in
// turn - and once the game is over, the team alone. At a neutral site, a
// shout for either side's, and nobody's chants.
const SHOUT_MS = 3200;
const BLINK_MS = 420;
const CHANT_MS = 2600;
const AMBIENT: BoardScreen[] = ["name", "letsGo", "name", "noise"];
const AMBIENT_MS = 8000;
const SCRIMMAGE = new Set(["pass", "run", "none", "kneel"]);

export const boardAt = (
	tl: FieldTimeline,
	t: number,
	neutral = false,
): BoardScreen => {
	const c = cheerAt(tl, t);
	if ((c?.side === 1 || neutral) && c?.what && c.since < SHOUT_MS) {
		return c.what;
	}
	const play = playAt(tl, t);
	if (neutral || (play?.postgame && t >= play.postgame.from)) {
		return "name";
	}
	if (play && t < play.whistle && SCRIMMAGE.has(play.kind)) {
		if (play.offense === 0) {
			return Math.floor(t / BLINK_MS) % 2 ? "defense2" : "defense";
		}
		if ((play.tense ?? 0) >= 0.5) {
			return Math.floor(t / CHANT_MS) % 2 ? "letsGo" : "noise";
		}
	}
	return AMBIENT[Math.floor(t / AMBIENT_MS) % AMBIENT.length]!;
};
