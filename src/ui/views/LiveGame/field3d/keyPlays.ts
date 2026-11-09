import {
	fillReel,
	reelCursors,
	SWING_MIN,
	SWING_W,
	type KeyPlaySettings,
	type KeySegment,
	type ReelCandidate,
} from "../court3d/keyPlays.ts";
import { winProb } from "../winProb.ts";
import type { FieldTimeline, Play, RawEvent } from "./director.ts";

// KEY PLAYS, FOR FOOTBALL: the game watched as its big moments - the 3D
// court's reel (see court3d/keyPlays.ts), of the touchdowns, the takeaways,
// the long gains and the kicks that count, in order, the score and the clock
// moving on between them, to a length the viewer picks. And if it is close
// at the end, every play of the finish, the way it happened, on top of that
// length. The same settings as the court's, saved in this browser.

// Into each play from this long (ms) before the snap - the line set, the
// count - and on for this long after the whistle (a score: through the
// scorer's moment, the end zone and all).
const LEAD_MS = 3000;
const TAIL_MS = 1500;
// A play from scrimmage with this little left in a quarter (seconds): the
// last of a half, or of the game - shown, whatever it comes to.
const LAST_PLAY_S = 5;
// Long enough a gain, or a return, to be a big play (yards).
const BIG_GAIN = 20;
const BIG_RETURN = 35;

const GAINS = new Set(["run", "passComplete"]);
const RETURNS = new Set(["kickoffReturn", "puntReturn", "interceptionReturn"]);
const FROM_SCRIMMAGE = new Set(["pass", "run", "fieldGoal"]);

// How it stood before each event: the quarter, the clock (seconds left in
// it), the score (box score teams), and the chance team 0, the home team,
// wins - and once it is all over.
type Standing = {
	period: number;
	clock: number;
	score: [number, number];
	p0: number;
};

const standingsOf = (
	events: RawEvent[],
	numPeriods: number,
	spread: number,
): Standing[] => {
	// (How long a quarter is: as long as its clock starts at.)
	let period = 1;
	let reg = 0;
	let ot = 0;
	for (const e of events) {
		if (e?.type === "quarter" && typeof e.quarter === "number") {
			period = e.quarter;
		} else if (e?.type === "overtime") {
			period = Math.max(period, numPeriods) + 1;
		}
		if (typeof e?.clock === "number") {
			if (period <= numPeriods) {
				reg = Math.max(reg, e.clock);
			} else {
				ot = Math.max(ot, e.clock);
			}
		}
	}
	const regS = reg > 0 ? Math.ceil(reg) * 60 : 15 * 60;
	const otS = ot > 0 ? Math.ceil(ot) * 60 : 10 * 60;
	const total = regS * numPeriods;
	const out: Standing[] = [];
	const score: [number, number] = [0, 0];
	let clock = regS;
	let ball: 0 | 1 | undefined;
	period = 1;
	for (const e of events) {
		if (e?.type === "quarter" && typeof e.quarter === "number") {
			period = e.quarter;
			clock = regS;
		} else if (e?.type === "overtime") {
			period = Math.max(period, numPeriods) + 1;
			clock = otS;
		}
		if (typeof e?.clock === "number") {
			clock = e.clock * 60;
		}
		if (e?.type === "clock" && (e.t === 0 || e.t === 1)) {
			ball = e.t;
		}
		const left =
			(period <= numPeriods ? clock + (numPeriods - period) * regS : clock) /
			total;
		out.push({
			period,
			clock,
			score: [score[0], score[1]],
			p0: winProb({
				margin: score[0] - score[1],
				left,
				spread,
				ball: ball === undefined ? 0 : ball === 0 ? 1 : -1,
			}),
		});
		if (
			e?.type === "stat" &&
			e.s === "pts" &&
			(e.t === 0 || e.t === 1) &&
			typeof e.amt === "number"
		) {
			score[e.t as 0 | 1] += e.amt;
		}
	}
	const decided = score[0] > score[1] ? 1 : score[0] < score[1] ? 0 : 0.5;
	out.push({ period, clock: 0, score: [score[0], score[1]], p0: decided });
	return out;
};

export const planFieldKeyPlays = (
	tl: FieldTimeline,
	events: RawEvent[],
	settings: KeyPlaySettings,
	{
		rate,
		numPeriods,
		mine,
		spread,
	}: {
		rate: number;
		numPeriods: number;
		// The box score team (0 home, 1 away) of the viewer's own team, if
		// playing.
		mine?: 0 | 1;
		// The pregame spread, the home team's (see Game.spread).
		spread?: number;
	},
): KeySegment[] => {
	const { plays, beats } = tl;
	if (plays.length === 0 || beats.length === 0) {
		return [{ t0: 0, t1: tl.end, cursor: 0, close: false }];
	}
	const before = standingsOf(events, numPeriods, spread ?? 0);
	const endOf = (k: number) => plays[k + 1]?.i ?? events.length;
	const at = (k: number) => before[plays[k]!.i]!;
	// How it stands once the play and all it brings are done: just before
	// the next one.
	const after = (k: number) => before[endOf(k)] ?? at(k);
	const lead = (s: [number, number]) => Math.sign(s[0] - s[1]);
	// A play's clip: from the line set to the ball dead - and, a big play,
	// through whoever made it being mobbed.
	const clipOf = (p: Play): { t0: number; t1: number } => ({
		t0: Math.max(p.t0, p.snap - LEAD_MS),
		t1: Math.min(p.t1, Math.max(p.whistle + TAIL_MS, p.party?.until ?? 0)),
	});
	const close = (k: number): boolean => {
		const st = at(k);
		return (
			settings.closePoints > 0 &&
			settings.closeMinutes > 0 &&
			Math.abs(st.score[0] - st.score[1]) <= settings.closePoints &&
			st.period >= numPeriods &&
			st.clock <= settings.closeMinutes * 60
		);
	};

	const picked: { t0: number; t1: number; close: boolean }[] = [];
	// The opening kickoff, and the end of the game.
	picked.push({ t0: 0, t1: clipOf(plays[0]!).t1, close: false });
	const last = plays.at(-1)!;
	picked.push({ t0: clipOf(last).t0, t1: tl.end, close: false });
	plays.forEach((p, k) => {
		// Every play of a close finish, as it happened.
		if (close(k)) {
			picked.push({ t0: p.t0, t1: p.t1, close: true });
			return;
		}
		// The last play of a half, a shot at the end zone or a kick at the
		// gun.
		if (FROM_SCRIMMAGE.has(p.kind) && at(k).clock <= LAST_PLAY_S) {
			picked.push({ ...clipOf(p), close: false });
		}
	});

	// The rest of the time, the best of the big plays.
	const candidates: ReelCandidate[] = [];
	// (And, should there be time for more than the big plays, the rest - the
	// ones that matter most first.)
	const rest: { k: number; w: number; t0: number; t1: number }[] = [];
	plays.forEach((p, k) => {
		if (close(k) || p.kind === "none" || p.kind === "kneel") {
			return;
		}
		const st = at(k);
		const now = after(k);
		const lines = events.slice(p.i, endOf(k));
		// (A flag that stands: what it was is not what happened.)
		const fouled = lines.some(
			(e) =>
				e.type === "penalty" &&
				e.decision === "accept" &&
				e.offsetStatus !== "offset",
		);
		const scored = Math.max(
			now.score[0] - st.score[0],
			now.score[1] - st.score[1],
		);
		let w = 0;
		let kind = "gain";
		if (scored >= 6) {
			w += 6;
			kind = "touchdown";
		} else if (scored >= 2) {
			w += 3;
			kind = "kick";
		}
		for (const e of fouled ? [] : lines) {
			const yds = typeof e.yds === "number" ? e.yds : 0;
			if (
				e.type === "interception" ||
				(e.type === "fumbleRecovery" && e.lost === true) ||
				(e.type === "onsideKickRecovery" && e.success === true)
			) {
				w += 5;
				kind = "takeaway";
			} else if (e.type === "turnoverOnDowns") {
				w += 3;
				kind = "stop";
			} else if (e.type === "sack") {
				w += 2;
				kind = "sack";
			} else if (e.type === "fieldGoal" && e.made !== true) {
				w += 1.5;
				kind = "kick";
			} else if (e.type === "fieldGoal" && yds >= 50) {
				w += 1;
			} else if (GAINS.has(e.type) && yds >= BIG_GAIN) {
				w += 2 + Math.min(3, (yds - BIG_GAIN) / 15);
			} else if (RETURNS.has(e.type) && yds >= BIG_RETURN) {
				w += 3;
				if (kind === "gain") {
					kind = "return";
				}
			}
		}
		if (settings.leadChanges && scored > 0) {
			const was = lead(st.score);
			const is = lead(now.score);
			if (was !== 0 && is === -was) {
				w += 3;
			} else if (is === 0) {
				w += 1;
			}
		}
		// However big a play it is otherwise, how much it changes who is
		// likely to win.
		const swing = Math.abs(now.p0 - st.p0);
		if (settings.swings && swing >= SWING_MIN) {
			w += swing * SWING_W;
		}
		const { t0, t1 } = clipOf(p);
		if (w === 0) {
			rest.push({ k, w: swing, t0, t1 });
			return;
		}
		// Later and closer, more.
		if (st.period >= numPeriods) {
			w += 1;
		}
		if (Math.abs(st.score[0] - st.score[1]) <= 8) {
			w += 0.5;
		}
		// (Whose play it was: the side the crowd is up for.)
		const side = p.cheer?.side ?? p.offense;
		if (settings.mine && mine !== undefined && 1 - side === mine) {
			w += 1.5;
		}
		candidates.push({ k, w, t0, t1, kind, period: st.period });
	});

	// (Every stretch watched at the pace it was staged at.)
	return reelCursors(
		fillReel({
			picked,
			candidates,
			rest,
			costOf: (t0, t1) => t1 - t0,
			budget: settings.minutes * 60_000 * Math.max(0.25, rate),
			closeOnTop: settings.closeOnTop,
		}),
		tl,
		events.length,
	);
};
