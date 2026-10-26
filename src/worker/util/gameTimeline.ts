// THE BOX SCORE'S PICTURES OF THE GAME: PACKED TO KEEP, UNPACKED TO DRAW.
//
// What the sim logged about when things happened (see timelineLog.ts) lives
// on the game row as flat runs of whole seconds, like the lineup scoring next
// to it (see gameShifts.ts) - field names would cost more than the numbers.
//
//   stints:     per player, `pid, n,` then n pairs of `on, off`
//   scoring:    per score, `t, side, pts`
//   periodEnds: the moment each period ended
//   shiftSecs:  per row of `shifts`, the seconds that ten played together
//
// Unpacked, it is what the box score draws: the score over the game, who was
// on the floor when, and how each five did - the same ten-man rows, folded
// into fives, with time.

import type { Game } from "../../common/types.ts";
import type { GameShift } from "../core/GameSim.basketball/shiftLog.ts";
import type { GameTimelineRaw } from "../core/GameSim.basketball/timelineLog.ts";
import { decodeShifts } from "./gameShifts.ts";
import type { LineupPic } from "./lineupPictures.ts";

export const encodeTimeline = (timeline: GameTimelineRaw) => {
	const stints: number[] = [];
	for (const { pid, spans } of timeline.stints) {
		const rounded = spans
			.map(([a, b]) => [Math.round(a), Math.round(b)] as const)
			.filter(([a, b]) => b > a);
		if (rounded.length === 0) {
			continue;
		}
		stints.push(pid, rounded.length);
		for (const [a, b] of rounded) {
			stints.push(a, b);
		}
	}
	const scoring: number[] = [];
	for (const { t, side, pts } of timeline.scoring) {
		scoring.push(Math.round(t), side, pts);
	}
	return {
		stints,
		scoring,
		periodEnds: timeline.periodEnds.map((t) => Math.round(t)),
	};
};

// The seconds column for `shifts`, row for row: the same rows encodeShifts
// keeps, in the same order.
export const encodeShiftSecs = (
	shifts: readonly GameShift[],
	numPlayersOnCourt: number,
): number[] =>
	shifts
		.filter(
			(shift) =>
				shift.lineups[0].length === numPlayersOnCourt &&
				shift.lineups[1].length === numPlayersOnCourt,
		)
		.map((shift) => Math.round(shift.sec ?? 0));

export type Stint = { pid: number; spans: [number, number][] };

export const decodeStints = (flat: readonly number[] | undefined): Stint[] => {
	const out: Stint[] = [];
	if (!flat) {
		return out;
	}
	let i = 0;
	while (i + 1 < flat.length) {
		const pid = flat[i]!;
		const n = flat[i + 1]!;
		if (i + 2 + 2 * n > flat.length) {
			break;
		}
		const spans: [number, number][] = [];
		for (let k = 0; k < n; k++) {
			spans.push([flat[i + 2 + 2 * k]!, flat[i + 3 + 2 * k]!]);
		}
		out.push({ pid, spans });
		i += 2 + 2 * n;
	}
	return out;
};

export type Score = { t: number; side: 0 | 1; pts: number };

export const decodeScoring = (flat: readonly number[] | undefined): Score[] => {
	const out: Score[] = [];
	if (!flat) {
		return out;
	}
	for (let i = 0; i + 2 < flat.length; i += 3) {
		out.push({
			t: flat[i]!,
			side: flat[i + 1] === 1 ? 1 : 0,
			pts: flat[i + 2]!,
		});
	}
	return out;
};

// How a five did while it was out there: its own possessions and points, and
// the other team's against it.
export type LineupLine = {
	pids: number[];
	sec: number;
	poss: number;
	possOpp: number;
	pts: number;
	ptsOpp: number;
};

export type OnCourt = Omit<LineupLine, "pids">;

export type BoxScoreFlow = {
	// Seconds of game, and where each period ended.
	total: number;
	periodEnds: number[];
	// The score after each score, from 0-0: when, team 0's, team 1's.
	scores: [number, number, number][];
	// Per team (box score order): who was on the floor when.
	rotation: [Stint[], Stint[]];
	// Per team: each five, most minutes first. Absent where the game kept no
	// lineup scoring (older games).
	lineups?: [LineupLine[], LineupLine[]];
	// Per team: the team as a whole, and each man's share of it on the floor.
	onCourt?: [
		{ team: OnCourt; byPid: Record<number, OnCourt> },
		{ team: OnCourt; byPid: Record<number, OnCourt> },
	];
	// Faces for the fives shown (see util/lineupPictures).
	pics?: Record<number, LineupPic>;
};

const blank = (): OnCourt => ({
	sec: 0,
	poss: 0,
	possOpp: 0,
	pts: 0,
	ptsOpp: 0,
});

const add = (a: OnCourt, b: OnCourt) => {
	a.sec += b.sec;
	a.poss += b.poss;
	a.possOpp += b.possOpp;
	a.pts += b.pts;
	a.ptsOpp += b.ptsOpp;
};

// Fold ten-man rows into each side's fives.
export const foldLineups = (
	game: Pick<Game, "shifts" | "shiftSecs" | "numPlayersOnCourt">,
): [LineupLine[], LineupLine[]] | undefined => {
	const rows = decodeShifts(game);
	if (rows.length === 0) {
		return undefined;
	}
	const secs = game.shiftSecs;
	const out: [Map<string, LineupLine>, Map<string, LineupLine>] = [
		new Map(),
		new Map(),
	];
	rows.forEach((row, k) => {
		for (const side of [0, 1] as const) {
			const other = side === 0 ? 1 : 0;
			const pids = row.lineups[side];
			const key = pids.join(",");
			let line = out[side].get(key);
			if (!line) {
				line = { pids, ...blank() };
				out[side].set(key, line);
			}
			add(line, {
				sec: secs?.[k] ?? 0,
				poss: row.poss[side],
				possOpp: row.poss[other],
				pts: row.pts[side],
				ptsOpp: row.pts[other],
			});
		}
	});
	const sorted = (m: Map<string, LineupLine>) =>
		[...m.values()].sort(
			(a, b) => b.sec - a.sec || b.poss + b.possOpp - (a.poss + a.possOpp),
		);
	return [sorted(out[0]), sorted(out[1])];
};

const onCourtOf = (lines: LineupLine[]) => {
	const team = blank();
	const byPid: Record<number, OnCourt> = {};
	for (const line of lines) {
		add(team, line);
		for (const pid of line.pids) {
			byPid[pid] ??= blank();
			add(byPid[pid], line);
		}
	}
	return { team, byPid };
};

export const getBoxScoreFlow = (
	game: Pick<
		Game,
		| "stints"
		| "scoring"
		| "periodEnds"
		| "shifts"
		| "shiftSecs"
		| "numPlayersOnCourt"
	> & { teams: { players: { pid: number }[] }[] },
): BoxScoreFlow | undefined => {
	const stints = decodeStints(game.stints);
	const periodEnds = game.periodEnds ?? [];
	const total = periodEnds.at(-1) ?? 0;
	if (stints.length === 0 || total <= 0) {
		return undefined;
	}

	const sideOf = new Map<number, 0 | 1>();
	for (const side of [0, 1] as const) {
		for (const p of game.teams[side]!.players) {
			sideOf.set(p.pid, side);
		}
	}
	const rotation: [Stint[], Stint[]] = [[], []];
	for (const stint of stints) {
		const side = sideOf.get(stint.pid);
		if (side !== undefined) {
			rotation[side].push(stint);
		}
	}

	const scores: [number, number, number][] = [[0, 0, 0]];
	const now: [number, number] = [0, 0];
	for (const { t, side, pts } of decodeScoring(game.scoring)) {
		now[side] += pts;
		scores.push([t, now[0], now[1]]);
	}

	const lineups = foldLineups(game);
	return {
		total,
		periodEnds,
		scores,
		rotation,
		...(lineups
			? {
					lineups,
					onCourt: [onCourtOf(lineups[0]), onCourtOf(lineups[1])],
				}
			: {}),
	};
};

// A team's fives over a stretch of games: the same rows, added up across them,
// with how many games each five saw the floor in.
export type SeasonLineup = LineupLine & { gp: number };

export const seasonLineups = (
	games: readonly (Pick<Game, "shifts" | "shiftSecs" | "numPlayersOnCourt"> & {
		teams: { tid: number; players: { pid: number; name?: string }[] }[];
	})[],
	tid: number,
) => {
	const byKey = new Map<string, SeasonLineup>();
	const names: Record<number, string> = {};
	for (const game of games) {
		const side =
			game.teams[0]!.tid === tid ? 0 : game.teams[1]!.tid === tid ? 1 : -1;
		if (side < 0) {
			continue;
		}
		for (const p of game.teams[side]!.players) {
			if (p.name) {
				names[p.pid] = p.name;
			}
		}
		const folded = foldLineups(game);
		if (!folded) {
			continue;
		}
		for (const line of folded[side as 0 | 1]) {
			const key = line.pids.join(",");
			let total = byKey.get(key);
			if (!total) {
				total = { pids: line.pids, gp: 0, ...blank() };
				byKey.set(key, total);
			}
			total.gp += 1;
			add(total, line);
		}
	}
	return {
		names,
		lineups: [...byKey.values()].sort((a, b) => b.sec - a.sec),
	};
};
