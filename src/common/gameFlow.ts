// HOW A GAME UNFOLDED, IN A FEW NUMBERS.
//
// A stored game keeps quarter scores and a clutch-play log, and nothing else
// about the order things happened in. That is enough to say a game was close
// and not enough to say how: who led with two minutes left, when the winner
// took the lead for good, how many times it changed hands, the run that
// turned it, the lead the loser let go. Every one of those is the sentence a
// real recap of a close game is built on, and none could be written.
//
// So the sim keeps a log of every score as it happens and boils it down to
// this before the game is stored. The summary is a few dozen bytes; the log
// it came from is thrown away. Absent on games played before it existed, and
// on sports that do not record it, and everything that reads it says nothing
// when it is not there.

export type GameFlowSide = 0 | 1;

export type GameFlow = {
	// Times the lead changed hands. A tie in between is not a change.
	leadChanges: number;
	// Times the score was level, not counting 0-0.
	ties: number;
	// The biggest lead each side held: teams[0] (home), teams[1] (away).
	maxLead: [number, number];
	// The last time the lead changed hands - from here the side that took it
	// never trailed again. The opening score of a game nobody ever caught.
	lastLead?: {
		side: GameFlowSide;
		pid?: number;
		period: number;
		clock: number;
		pts: [number, number];
		// Points in the play that took it: 1 is a free throw.
		by?: number;
	};
	// The last time it was level.
	lastTie?: { period: number; clock: number; pts: number };
	// The longest unanswered run, and where it began.
	run?: { side: GameFlowSide; pts: number; period: number; clock: number };
	// The score with five minutes and with two minutes left in regulation.
	late?: { clock: number; pts: [number, number] }[];
};

type Event = {
	side: GameFlowSide;
	pid?: number;
	period: number;
	clock: number;
	pts: number;
};

// Clock marks, in seconds left in the final period, that the late score is
// taken at.
export const LATE_MARKS = [300, 120] as const;

export class FlowLog {
	private events: Event[] = [];
	// One score can arrive as several stat calls - a three is two and then
	// one, an and-one is a basket and a free throw - all at the same instant.
	// Held back until the next instant, so a three that erases a two-point
	// deficit reads as a lead change and not as a tie followed by a lead.
	private pending?: Event;

	addPoints(
		side: GameFlowSide,
		pts: number,
		period: number,
		clock: number,
		pid?: number,
	) {
		const p = this.pending;
		if (
			p &&
			p.side === side &&
			p.pid === pid &&
			p.period === period &&
			p.clock === clock
		) {
			p.pts += pts;
			return;
		}
		this.flush();
		this.pending = { side, pid, period, clock, pts };
	}

	private flush() {
		if (this.pending) {
			this.events.push(this.pending);
			this.pending = undefined;
		}
	}

	summary(numPeriods: number): GameFlow {
		this.flush();
		const score: [number, number] = [0, 0];
		const out: GameFlow = { leadChanges: 0, ties: 0, maxLead: [0, 0] };
		let leader: GameFlowSide | undefined;

		let runSide: GameFlowSide | undefined;
		let runPts = 0;
		let runStart = { period: 1, clock: 0 };
		let bestRun = 0;

		const late = new Map<number, [number, number]>();

		for (const e of this.events) {
			score[e.side] += e.pts;

			if (runSide === e.side) {
				runPts += e.pts;
			} else {
				runSide = e.side;
				runPts = e.pts;
				runStart = { period: e.period, clock: e.clock };
			}
			if (runPts > bestRun) {
				bestRun = runPts;
				out.run = { side: e.side, pts: runPts, ...runStart };
			}

			const diff = score[0] - score[1];
			if (diff === 0) {
				out.ties += 1;
				out.lastTie = { period: e.period, clock: e.clock, pts: score[0] };
			} else {
				const now: GameFlowSide = diff > 0 ? 0 : 1;
				if (leader !== undefined && now !== leader) {
					out.leadChanges += 1;
				}
				if (now !== leader) {
					out.lastLead = {
						side: now,
						pid: e.pid,
						period: e.period,
						clock: e.clock,
						pts: [score[0], score[1]],
						by: e.pts,
					};
				}
				leader = now;
				out.maxLead[now] = Math.max(out.maxLead[now], Math.abs(diff));
			}

			// The score at each mark is the score after the last event before
			// it - which may be from an earlier period, if the final one opened
			// quietly. A clock of Infinity is an Elam ending, which has no marks.
			for (const mark of LATE_MARKS) {
				if (
					e.period < numPeriods ||
					(e.period === numPeriods &&
						Number.isFinite(e.clock) &&
						e.clock >= mark)
				) {
					late.set(mark, [score[0], score[1]]);
				}
			}
		}

		// Only when the game actually reached the mark with a clock.
		const reached = this.events.some(
			(e) => e.period === numPeriods && Number.isFinite(e.clock),
		);
		if (reached && late.size > 0) {
			out.late = LATE_MARKS.filter((mark) => late.has(mark)).map((mark) => ({
				clock: mark,
				pts: late.get(mark)!,
			}));
		}
		return out;
	}
}

// "3:41", or "9.9 seconds" inside the last minute, or nothing for a clock
// that means nothing (an Elam ending).
export const clockLeft = (clock: number): string | undefined => {
	if (!Number.isFinite(clock) || clock < 0) {
		return undefined;
	}
	if (clock >= 60) {
		const m = Math.floor(clock / 60);
		const s = Math.floor(clock - m * 60);
		return `${m}:${String(s).padStart(2, "0")}`;
	}
	const s = Math.round(clock * 10) / 10;
	return `${s} second${s === 1 ? "" : "s"}`;
};
