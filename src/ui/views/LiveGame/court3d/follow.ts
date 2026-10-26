// WATCHING SOMEONE ELSE'S SIM, SMOOTHLY.
//
// A device following a league-mate's live game has the whole game already
// (the sim finished before playback started); what it learns from the device
// in charge of simming is only how far along it is, a report every few
// hundred milliseconds that arrives late and unevenly. Running to each
// reported play and stopping there played the game in bursts - a stop at
// every line while the next report was on its way, then a sprint to catch up.
//
// So the follower keeps its own estimate of where the other court is - it
// runs on at the same speed between reports, and never past the play the
// other device hasn't reached yet - and plays a little behind that estimate,
// easing its speed up or down to hold the gap instead of stopping dead.
//
// That is how a device in charge of simming on an older build is followed.
// One that says where its court is gets played along with instead (below).

import type { CourtTimeline } from "./director.ts";

// How far behind the other court to play (ms of game timeline): enough that
// a late report doesn't run the picture into a wall.
export const FOLLOW_BEHIND = 800;

// The other court's position, run on by dt (ms) at its rate, but never past
// the next play it hasn't shown yet.
export const advanceLead = (
	lead: number,
	target: number,
	dt: number,
	rate: number,
): number => Math.min(target, lead + dt * rate);

// How much faster or slower than the other court to run, from how far
// behind it this one is: steady at FOLLOW_BEHIND, easing toward it.
export const followRate = (lag: number): number =>
	Math.min(2.5, Math.max(0.5, 1 + (lag - FOLLOW_BEHIND) / 1500));

// PLAYING ALONG. The device in charge of simming says more than how far
// along its page is: where its court is in the show (`w`, ms of picture
// played - the same on any device playing the same game at the same speed,
// see Court3D's play.w), and in the game (`t`). So this device plays the
// game itself, the same way - the same speed, the same key plays, the same
// replays - a second behind the other court. Between reports the other
// court is taken to play on as it was; this one eases a touch faster or
// slower to hold the gap, stands still only when it has come up too close
// behind (the other device paused), and cuts only when far behind (joining
// late) or when the two pictures disagree about where the game is (a skip
// on the other device).

// One report from the device in charge of simming: where its court was in
// the show, in the game and in a replay (`rp`, -1 none); its page's cursor;
// whether it was paused; and when, by its clock (`at`) - and when it got
// here, by this one's (`recv`).
export type FollowReport = {
	t: number;
	w: number;
	rp: number;
	cursor: number;
	paused: boolean;
	at: number;
	recv: number;
	// What its court staged (see stagedAs) - absent from older builds.
	fp?: string;
};

// Where this court was, a moment at a time (and, said to whoever plays
// along, what it staged).
export type Shown = { w: number; t: number; rp: number; fp?: string };

// THE SAME GAME, OR NOT. Both courts stage the game from the same
// play-by-play - but a device still on an older build of the app (a tab not
// reloaded since an update) stages it differently, and then no point in one
// show is that point in the other's game: every check disagrees, and the cut
// back to where the other court was comes round again a second later, the
// same few seconds over and over. So a court says what it staged - this, its
// plays' moments to a tenth of a second - and one that staged something else
// is stepped line by line instead of playing along (see Court3D).
export const stagedAs = (tl: CourtTimeline): string => {
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
	for (const [a, b] of tl.fast) {
		add(a / 100);
		add(b / 100);
	}
	return (h >>> 0).toString(36);
};

export type Follow = {
	// The latest reports, oldest first.
	reports: FollowReport[];
	// The ones not yet checked against what this court showed then.
	checks: FollowReport[];
	shown: Shown[];
	// How far behind where it should be this court is (show ms; ahead is
	// negative), smoothed.
	gap: number;
	holding: boolean;
	// The other court started over (its page reloaded): cut to it.
	restart: boolean;
};

// How far behind the other court to play (show ms): watching together, the
// two screens should look like one - only enough slack that a report a
// little late doesn't run this one into the other's picture.
export const SYNC_BEHIND = 400;
// How long past its last report the other court is taken to keep playing -
// past that, it has stopped (its tab put away, say) until it says so.
const RUN_ON = 3000;
// Within this of the gap, dead on the other court's speed; further off, a
// little faster or slower, but never more than this.
const EASE = 200;
const EASE_GAIN = 2500;
const SLOWEST = 0.8;
const FASTEST = 1.3;
// This much too close behind: stand still until the other court is clear.
const HOLD_AT = 600;
// This far behind: cut to where the other court was.
const SNAP_AT = 3000;
// (How quickly the gap's measure follows each frame's, ms.)
const SMOOTH_MS = 600;
// This far apart about where the game was, at the same point in the show
// (show ms): cut to where the other court was.
export const SYNC_SLIP = 400;
// A report is checked once this court is this far past it, so a cut just
// there (a key play gone on to) is in what it showed either way.
const CHECK_AFTER = 300;
const KEEP_SHOWN = 4000;

export const makeFollow = (): Follow => ({
	reports: [],
	checks: [],
	shown: [],
	gap: 0,
	// (Until it has a gap behind the other court, standing still.)
	holding: true,
	restart: false,
});

export const addReport = (f: Follow, r: FollowReport) => {
	const last = f.reports.at(-1);
	if (last && r.at === last.at) {
		return;
	}
	if (last && r.w < last.w - 1) {
		f.reports = [];
		f.checks = [];
		f.restart = true;
	}
	f.reports.push(r);
	if (f.reports.length > 20) {
		f.reports.shift();
	}
	f.checks.push(r);
	if (f.checks.length > 60) {
		f.checks.shift();
	}
};

// Where the other court is in the show now (this device's clock, ms): on
// from its last report by the time since, less the quickest a report has
// come (which also takes out the difference between the two clocks).
export const otherAt = (f: Follow, now: number): number | undefined => {
	const r = f.reports.at(-1);
	if (!r) {
		return undefined;
	}
	if (r.paused) {
		return r.w;
	}
	let lag = Infinity;
	for (const x of f.reports) {
		lag = Math.min(lag, x.recv - x.at);
	}
	return r.w + Math.min(RUN_ON, Math.max(0, now - r.at - lag));
};

// This frame: how much of the show to play (ms - `dt` at the other court's
// pace), or a cut to where it was.
export const followStep = (
	f: Follow,
	w: number,
	now: number,
	dt: number,
): { ms: number; snap?: FollowReport } => {
	const r = f.reports.at(-1);
	const other = otherAt(f, now);
	if (!r || other === undefined) {
		return { ms: 0 };
	}
	// Paused, it stops on a picture: on to that one, then hold there.
	const goal = r.paused ? other : other - SYNC_BEHIND;
	const off = goal - w;
	if (f.restart || off > SNAP_AT) {
		f.restart = false;
		f.gap = 0;
		f.holding = true;
		// The latest place it was, at or before where this one should be.
		const to = f.reports.findLast((x) => x.w <= goal) ?? r;
		f.checks = f.checks.filter((x) => x.w > to.w);
		f.shown = [];
		return { ms: 0, snap: to };
	}
	f.gap += (off - f.gap) * Math.min(1, dt / SMOOTH_MS);
	let k: number;
	if (r.paused) {
		k = off > 0 ? 1 : 0;
		f.holding = off <= 0;
	} else {
		// (Standing still, it goes again the moment the gap is back - not
		// slowly, from a measure still catching up.)
		if (f.holding ? off > -EASE : f.gap < -HOLD_AT) {
			f.holding = !f.holding;
		}
		if (f.holding) {
			f.gap = off;
		}
		const over = Math.max(0, Math.abs(f.gap) - EASE) / EASE_GAIN;
		k = f.holding
			? 0
			: Math.min(FASTEST, Math.max(SLOWEST, 1 + Math.sign(f.gap) * over));
	}
	// Never past the other court.
	return { ms: Math.min(dt * k, Math.max(0, other - w)) };
};

// What this court showed (each frame, and both sides of a cut).
export const noteShown = (f: Follow, at: Shown) => {
	f.shown.push(at);
	while (f.shown.length > 2 && f.shown[1]!.w < at.w - KEEP_SHOWN) {
		f.shown.shift();
	}
};

// The reports to check now this court has played to `w`.
export const dueChecks = (f: Follow, w: number): FollowReport[] => {
	const due: FollowReport[] = [];
	while (
		f.checks.length > 0 &&
		f.checks[0]!.w <= (f.checks[0]!.paused ? w + 1 : w - CHECK_AFTER)
	) {
		due.push(f.checks.shift()!);
	}
	return due;
};

// Whether this court, at that point in the show, was where the other court
// was - `per` being how many game (or replay) ms a show ms is there. A
// report from before what this court remembers can't be checked: it agrees.
export const agrees = (
	f: Follow,
	r: FollowReport,
	per: (t: number, rp: number) => number,
): boolean => {
	const near = (x: Shown) => {
		if (x.rp >= 0 !== r.rp >= 0) {
			return false;
		}
		const scale = per(r.t, r.rp);
		const dw = r.w - x.w;
		return r.rp >= 0
			? Math.abs(x.t - r.t) < 1 &&
					Math.abs(x.rp + dw * scale - r.rp) <= SYNC_SLIP * scale
			: Math.abs(x.t + dw * scale - r.t) <= SYNC_SLIP * scale;
	};
	const { shown } = f;
	if (shown.length === 0 || shown[0]!.w > r.w) {
		return true;
	}
	// The moments either side of it (and every moment at it, both sides of
	// any cut there - or of a replay ending).
	for (let i = 0; i < shown.length; i++) {
		const x = shown[i]!;
		const next = shown[i + 1];
		if (
			(Math.abs(x.w - r.w) <= 1 || (x.w <= r.w && (!next || next.w >= r.w))) &&
			(near(x) || (next !== undefined && near(next)))
		) {
			return true;
		}
	}
	return false;
};

// Cut to where the other court was.
export const cutTo = (f: Follow, r: FollowReport) => {
	f.checks = f.checks.filter((x) => x.w > r.w);
	f.shown = [];
	f.gap = 0;
	f.holding = true;
};
