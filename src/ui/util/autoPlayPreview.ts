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

// How many league days each Play Menu preset advances. "days" isn't here - it
// carries its own count on the rule.
export type AmountDays = Record<"day" | "week" | "month", number>;

// What getAutoPlayPreview returns from the worker.
export type AutoPlayPreviewData = {
	phase: number;
	season: number;
	upcomingDays: PreviewDay[];
	amountDays: AmountDays;
	phaseEndNote?: string;
};

// One fire, resolved to the number of league days it will try to advance.
export type Fire = {
	at: number;
	amount: AutoPlayAmount;
	// Only meaningful when amount is "days".
	numDays: number;
	ruleId: string;
	label?: string;
};

// A scheduled fire on the real clock, with the league days it will cover.
export type ProjectedFire = Fire & {
	fromDay: number;
	toDay: number;
	numLeagueDays: number;
	numGames: number;
	events: string[];
	// True when this fire consumes the last known scheduled day (the current
	// phase's schedule runs out here).
	endsPhase: boolean;
};

// League days one fire advances. A preset asks the worker how long it is; a
// custom "N days" rule already knows.
export const stepDays = (
	fire: { amount: AutoPlayAmount; numDays: number },
	amountDays: AmountDays,
): number =>
	fire.amount === "days"
		? Math.max(1, Math.round(fire.numDays))
		: Math.max(1, amountDays[fire.amount] ?? 1);

// Has the league been simmed THROUGH a "stop after" target? True once the season
// has moved past the target's season, or (same season) the next day to play is
// past the target day - including when the schedule has run dry (nextDay
// undefined). Used to auto-stop after the selected sim.
export const hasPassedStop = (
	target: { season: number; day: number },
	curSeason: number,
	nextDay: number | undefined,
): boolean =>
	curSeason > target.season ||
	(curSeason === target.season &&
		(nextDay === undefined || nextDay > target.day));

// The next `max` fire times across all enabled rules, in chronological order.
// Walks the clock forward past each fire and re-asks every rule for its next
// fire, so overlapping rules interleave correctly.
export const nextFires = (
	rules: ScheduleRule[],
	from: Date,
	max: number,
): Fire[] => {
	const enabled = rules.filter((rule) => rule.enabled);
	const out: Fire[] = [];
	let cursor = from.getTime();
	for (let i = 0; i < max; i++) {
		let best: Fire | undefined;
		for (const rule of enabled) {
			const at = nextFireForRule(rule, new Date(cursor));
			if (at !== undefined && (best === undefined || at < best.at)) {
				best = {
					at,
					amount: rule.amount,
					numDays: rule.numDays,
					ruleId: rule.id,
					label: rule.label,
				};
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
// through the scheduled days. A fire advances up to its own step, but no further
// than the schedule reaches (BBGM caps a sim at the days left in the phase).
// Stops once the schedule is exhausted.
export const projectFires = (
	fires: Fire[],
	upcomingDays: PreviewDay[],
	amountDays: AmountDays,
	phaseEndNote: string | undefined,
): ProjectedFire[] => {
	const out: ProjectedFire[] = [];
	let cursor = 0;
	for (const fire of fires) {
		if (cursor >= upcomingDays.length) {
			break;
		}
		const step = stepDays(fire, amountDays);
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
			...fire,
			fromDay: slice[0]!.day,
			toDay: slice.at(-1)!.day,
			numLeagueDays: slice.length,
			numGames,
			events,
			endsPhase,
		});
		cursor = end;
	}
	return out;
};
