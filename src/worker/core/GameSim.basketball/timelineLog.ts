// WHEN THINGS HAPPENED, FOR THE BOX SCORE'S PICTURES OF THE GAME.
//
// A box score adds a game up. This keeps just enough of the order it happened
// in to draw it: who was on the floor from when to when (the rotation map),
// every score and when (the lead, over the game), and where each period ended.
//
// Times are seconds of game played, on the same clock minutes are counted on -
// a possession's time goes to whoever is on the floor when it ends - so a
// man's stints add up to his minutes.

import type { TeamNum } from "../../../common/types.ts";

export type GameTimelineRaw = {
	periodEnds: number[];
	stints: { pid: number; spans: [number, number][] }[];
	scoring: { t: number; side: TeamNum; pts: number }[];
};

export class TimelineLog {
	// Who is on the floor, and since when.
	private since = new Map<number, number>();
	private spans = new Map<number, [number, number][]>();
	private scoring: GameTimelineRaw["scoring"] = [];
	private periodEnds: number[] = [];

	private close(pid: number, t0: number, t1: number) {
		let spans = this.spans.get(pid);
		if (!spans) {
			spans = [];
			this.spans.set(pid, spans);
		}
		const last = spans.at(-1);
		// Off and straight back on at the same dead ball is one stint.
		if (last && last[1] === t0) {
			last[1] = t1;
		} else if (t1 > t0) {
			spans.push([t0, t1]);
		}
	}

	// Everyone on the floor now, both teams.
	setLineups(onFloor: readonly number[], now: number) {
		const next = new Set(onFloor);
		for (const [pid, t0] of this.since) {
			if (!next.has(pid)) {
				this.close(pid, t0, now);
				this.since.delete(pid);
			}
		}
		for (const pid of next) {
			if (!this.since.has(pid)) {
				this.since.set(pid, now);
			}
		}
	}

	addPoints(side: TeamNum, pts: number, t: number) {
		const last = this.scoring.at(-1);
		// A basket and its and-one, or a trip to the line: one score.
		if (last && last.side === side && last.t === t) {
			last.pts += pts;
		} else {
			this.scoring.push({ t, side, pts });
		}
	}

	endPeriod(now: number) {
		if (this.periodEnds.at(-1) !== now) {
			this.periodEnds.push(now);
		}
	}

	finish(now: number): GameTimelineRaw {
		for (const [pid, t0] of this.since) {
			this.close(pid, t0, now);
		}
		this.since.clear();
		this.endPeriod(now);
		return {
			periodEnds: this.periodEnds,
			stints: [...this.spans].map(([pid, spans]) => ({ pid, spans })),
			scoring: this.scoring,
		};
	}
}
