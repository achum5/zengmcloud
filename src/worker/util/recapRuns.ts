// HOW A GAME WAS WON, STRETCH BY STRETCH.
//
// A wire story of a basketball game is built on its stretches: "Denver
// closed on a 20-1 run", "a 14-2 run early in the third put Indiana ahead
// for good", "New York went 5:12 without a field goal". The game's flow
// summary (common/gameFlow.ts) keeps one of them - the longest UNANSWERED run
// - which is the rarest kind: a team that scores 20 of 21 points never had
// "20 straight". Broadcasters count a run the way it looked, both numbers.
//
// Every game since the box score's flow chart keeps its whole score log
// (`scoring`: when, who, how many) and where each period ended, and since
// this file, who scored each one (`scorers`). This reads that log back into
// the stretches a writer needs: the runs, counted the real way, and each
// side's longest dry spell. Pure, so a test can hand it a log.

import type { Game } from "../../common/types.ts";
import { decodeScorer, decodeScoring } from "./gameTimeline.ts";

export type Side = 0 | 1;

// One score, as the log has it: seconds of game played, whose, how many, and
// - when the game kept it - the man, and whether it was a basket.
export type LogScore = {
	t: number;
	side: Side;
	pts: number;
	pid?: number;
	fg?: boolean;
};

// A point in the game: when (seconds played, and the period and the time
// left in it) and the score then, home first.
export type Moment = {
	t: number;
	period: number;
	clock: number;
	score: [number, number];
};

export type Run = {
	side: Side;
	pts: number;
	// The other side's points inside it: 0 for "14 straight".
	opp: number;
	// Its first score's time, and the score just BEFORE it.
	from: Moment;
	// Its last score's time, and the score after it.
	to: Moment;
	// Who scored most of it, when the log says who scored.
	top?: { pid: number; pts: number };
	// The game's first points.
	opened?: true;
	// Nothing was scored after it: the game ended on it.
	closed?: true;
	// The points scored after it, [its side, the other], when it came in the
	// game's last period.
	after?: [number, number];
};

export type Drought = {
	side: Side;
	// Without a point, or without a basket (free throws still went in).
	kind: "pts" | "fg";
	secs: number;
	// Where it began (the side's last score before it, or the tip) and where
	// it ended (the score that ended it, or the final buzzer).
	from: Moment;
	to: Moment;
	// It ran to the final buzzer.
	toEnd?: true;
	// The other side's points while it lasted.
	oppPts: number;
	// Who ended it.
	endedBy?: number;
};

export type RecapRuns = {
	// The game's runs worth a sentence, in the order they happened. None
	// overlap.
	runs: Run[];
	// Each side's longest dry spell, when one was long enough to tell.
	droughts: Drought[];
	// Seconds of game played, and where each period ended.
	total: number;
	periodEnds: number[];
	// The log names who scored.
	named: boolean;
};

// A run worth calling one: at least this many points more than the other
// side, scoring at least three times what it gave up, inside this much game
// time.
export const RUN_MIN_NET = 8;
const RUN_MAX_RATIO = 1 / 3;
const RUN_MAX_SECS = 8 * 60;
const RUNS_KEPT = 6;

// Dry spells worth a sentence.
export const DROUGHT_MIN_PTS = 4 * 60;
export const DROUGHT_MIN_FG = 5 * 60;

// The period a moment falls in, and the time left in it. A score exactly at a
// period's end is that period's, at the buzzer.
const placeOf = (
	t: number,
	periodEnds: readonly number[],
): { period: number; clock: number } => {
	for (let i = 0; i < periodEnds.length; i++) {
		if (t <= periodEnds[i]!) {
			return { period: i + 1, clock: periodEnds[i]! - t };
		}
	}
	return { period: periodEnds.length, clock: 0 };
};

const momentOf = (
	t: number,
	score: [number, number],
	periodEnds: readonly number[],
): Moment => ({ t, ...placeOf(t, periodEnds), score: [score[0], score[1]] });

// The game's runs: for each side, every stretch from one of its scores to a
// later one that counts as a run; then the biggest (most net points, then
// fewest given up, then quickest), and the next biggest that does not touch
// it, and so on.
export const findRuns = (
	log: readonly LogScore[],
	periodEnds: readonly number[],
): Run[] => {
	const n = log.length;
	if (n === 0) {
		return [];
	}
	// The score before each event.
	const before: [number, number][] = [];
	const score: [number, number] = [0, 0];
	for (const e of log) {
		before.push([score[0], score[1]]);
		score[e.side] += e.pts;
	}
	const lastPeriod = periodEnds.length;

	type Cand = { i: number; j: number; pts: number; opp: number; secs: number };
	const cands: Cand[] = [];
	for (let i = 0; i < n; i++) {
		const side = log[i]!.side;
		let pts = 0;
		let opp = 0;
		for (let j = i; j < n; j++) {
			const e = log[j]!;
			if (e.t - log[i]!.t > RUN_MAX_SECS) {
				break;
			}
			if (e.side === side) {
				pts += e.pts;
			} else {
				opp += e.pts;
				// Past the ratio, it can only get back under it by scoring
				// more - keep going, but a run never ends on the other side.
				continue;
			}
			if (pts - opp >= RUN_MIN_NET && opp <= pts * RUN_MAX_RATIO) {
				cands.push({ i, j, pts, opp, secs: e.t - log[i]!.t });
			}
		}
	}
	// An unanswered run counts a little bigger: "15 straight points" says
	// more than the 18-2 around it.
	const size = (c: Cand) => c.pts - c.opp + (c.opp === 0 ? 2 : 0);
	cands.sort((a, b) => size(b) - size(a) || a.opp - b.opp || a.secs - b.secs);
	const taken: Cand[] = [];
	for (const c of cands) {
		if (taken.some((x) => c.i <= x.j && x.i <= c.j)) {
			continue;
		}
		taken.push(c);
		if (taken.length >= RUNS_KEPT) {
			break;
		}
	}
	taken.sort((a, b) => a.i - b.i);

	return taken.map((c) => {
		const side = log[c.i]!.side;
		const byPid = new Map<number, number>();
		for (let k = c.i; k <= c.j; k++) {
			const e = log[k]!;
			if (e.side === side && e.pid !== undefined) {
				byPid.set(e.pid, (byPid.get(e.pid) ?? 0) + e.pts);
			}
		}
		let top: Run["top"];
		for (const [pid, pts] of byPid) {
			if (!top || pts > top.pts) {
				top = { pid, pts };
			}
		}
		const after: [number, number] = [0, 0];
		for (let k = c.j + 1; k < n; k++) {
			const e = log[k]!;
			after[e.side === side ? 0 : 1] += e.pts;
		}
		const endScore: [number, number] = [
			before[c.j]![0] + (log[c.j]!.side === 0 ? log[c.j]!.pts : 0),
			before[c.j]![1] + (log[c.j]!.side === 1 ? log[c.j]!.pts : 0),
		];
		const to = momentOf(log[c.j]!.t, endScore, periodEnds);
		const run: Run = {
			side,
			pts: c.pts,
			opp: c.opp,
			from: momentOf(log[c.i]!.t, before[c.i]!, periodEnds),
			to,
			...(top ? { top } : {}),
		};
		if (c.i === 0) {
			run.opened = true;
		}
		if (c.j === n - 1) {
			run.closed = true;
		}
		if (to.period >= lastPeriod) {
			run.after = after;
		}
		return run;
	});
};

// Each side's longest spell without a point, and without a basket - from the
// tip or its last one to the next or the final buzzer.
export const findDroughts = (
	log: readonly LogScore[],
	periodEnds: readonly number[],
	named: boolean,
): Drought[] => {
	const total = periodEnds.at(-1) ?? log.at(-1)?.t ?? 0;
	const out: Drought[] = [];
	for (const side of [0, 1] as const) {
		for (const kind of named ? (["pts", "fg"] as const) : (["pts"] as const)) {
			const min = kind === "pts" ? DROUGHT_MIN_PTS : DROUGHT_MIN_FG;
			let best: Drought | undefined;
			let lastT = 0;
			let lastScore: [number, number] = [0, 0];
			let oppPts = 0;
			const score: [number, number] = [0, 0];
			const consider = (
				endT: number,
				endScore: [number, number],
				endedBy: number | undefined,
				toEnd: boolean,
			) => {
				const secs = endT - lastT;
				if (secs >= min && (!best || secs > best.secs)) {
					best = {
						side,
						kind,
						secs,
						from: momentOf(lastT, lastScore, periodEnds),
						to: momentOf(endT, endScore, periodEnds),
						oppPts,
						...(toEnd ? { toEnd: true as const } : {}),
						...(endedBy !== undefined ? { endedBy } : {}),
					};
				}
			};
			for (const e of log) {
				score[e.side] += e.pts;
				const counts = e.side === side && (kind === "pts" || e.fg === true);
				if (counts) {
					consider(e.t, [score[0], score[1]], e.pid, false);
					lastT = e.t;
					lastScore = [score[0], score[1]];
					oppPts = 0;
				} else if (e.side !== side) {
					oppPts += e.pts;
				}
			}
			consider(total, [score[0], score[1]], undefined, true);
			if (best) {
				out.push(best);
			}
		}
	}
	return out;
};

export const analyzeRuns = (
	log: readonly LogScore[],
	periodEnds: readonly number[],
): RecapRuns | undefined => {
	if (log.length === 0 || periodEnds.length === 0) {
		return undefined;
	}
	const named = log.some((e) => e.pid !== undefined);
	return {
		runs: findRuns(log, periodEnds),
		droughts: findDroughts(log, periodEnds, named),
		total: periodEnds.at(-1)!,
		periodEnds: [...periodEnds],
		named,
	};
};

// The score log a stored game kept, read back - with who scored, when the
// game kept that too.
export const scoreLogOf = (
	game: Pick<Game, "scoring" | "scorers">,
): LogScore[] => {
	const rows = decodeScoring(game.scoring);
	const scorers = game.scorers;
	const known = Array.isArray(scorers) && scorers.length === rows.length;
	return rows.map((row, i) => {
		const who = known ? decodeScorer(scorers[i]) : {};
		return {
			t: row.t,
			side: row.side,
			pts: row.pts,
			...(who.pid !== undefined ? { pid: who.pid } : {}),
			...(who.fg !== undefined ? { fg: who.fg } : {}),
		};
	});
};

// A stored game's stretches, or nothing for a game that kept no log.
export const runsOf = (
	game: Pick<Game, "scoring" | "scorers" | "periodEnds">,
): RecapRuns | undefined => {
	if (!Array.isArray(game.scoring) || !Array.isArray(game.periodEnds)) {
		return undefined;
	}
	return analyzeRuns(scoreLogOf(game), game.periodEnds);
};
