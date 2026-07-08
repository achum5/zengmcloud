import {
	nextFireForRule,
	type AutoPlayAmount,
	type ScheduleRule,
} from "./scheduleTime.ts";

// One scheduled league day and what's on it (from the worker's season calendar).
export type PreviewDay = {
	day: number;
	numGames: number;
	tradeDeadline?: boolean;
	allStar?: boolean;
};

// What getAutoPlayPreview returns from the worker.
export type AutoPlayPreviewData = {
	phase: number;
	upcomingDays: PreviewDay[];
	amountDays: Record<AutoPlayAmount, number>;
	phaseEndNote?: string;
};

// A scheduled fire on the real clock, with the league days it will cover.
export type ProjectedFire = {
	at: number;
	amount: AutoPlayAmount;
	fromDay: number;
	toDay: number;
	numDays: number;
	numGames: number;
	events: string[];
	// True when this fire consumes the last known scheduled day (the current
	// phase's schedule runs out here).
	endsPhase: boolean;
};

// The next `max` fire times across all enabled rules, in chronological order.
// Walks the clock forward past each fire and re-asks every rule for its next
// fire, so overlapping rules interleave correctly.
export const nextFires = (
	rules: ScheduleRule[],
	from: Date,
	max: number,
): { at: number; amount: AutoPlayAmount }[] => {
	const enabled = rules.filter((rule) => rule.enabled);
	const out: { at: number; amount: AutoPlayAmount }[] = [];
	let cursor = from.getTime();
	for (let i = 0; i < max; i++) {
		let best: { at: number; amount: AutoPlayAmount } | undefined;
		for (const rule of enabled) {
			const at = nextFireForRule(rule, new Date(cursor));
			if (at !== undefined && (best === undefined || at < best.at)) {
				best = { at, amount: rule.amount };
			}
		}
		if (!best) {
			break;
		}
		out.push(best);
		// Step just past this fire so the next iteration finds the following one.
		cursor = best.at + 1;
	}
	return out;
};

// Map each real-clock fire onto the league days it will advance, walking a cursor
// through the scheduled days. A fire advances up to amountDays[amount] scheduled
// days, but no further than the schedule reaches (BBGM caps a sim at the days
// left in the phase). Stops once the schedule is exhausted.
export const projectFires = (
	fires: { at: number; amount: AutoPlayAmount }[],
	upcomingDays: PreviewDay[],
	amountDays: Record<AutoPlayAmount, number>,
	phaseEndNote: string | undefined,
): ProjectedFire[] => {
	const out: ProjectedFire[] = [];
	let cursor = 0;
	for (const fire of fires) {
		if (cursor >= upcomingDays.length) {
			break;
		}
		const step = Math.max(1, amountDays[fire.amount] ?? 1);
		const end = Math.min(cursor + step, upcomingDays.length);
		const slice = upcomingDays.slice(cursor, end);
		const numGames = slice.reduce((sum, day) => sum + day.numGames, 0);

		const events: string[] = [];
		for (const day of slice) {
			if (day.tradeDeadline) {
				events.push("Trade deadline");
			}
			if (day.allStar) {
				events.push("All-Star Game");
			}
		}
		const endsPhase = end >= upcomingDays.length;
		if (endsPhase && phaseEndNote) {
			events.push(phaseEndNote);
		}

		out.push({
			at: fire.at,
			amount: fire.amount,
			fromDay: slice[0]!.day,
			toDay: slice[slice.length - 1]!.day,
			numDays: slice.length,
			numGames,
			events,
			endsPhase,
		});
		cursor = end;
	}
	return out;
};
