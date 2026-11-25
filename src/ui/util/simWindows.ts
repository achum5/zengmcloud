import {
	nextFires,
	projectFires,
	type AutoPlayPreviewData,
} from "./autoPlayPreview.ts";
import { prevFireForRule, type ScheduleRule } from "./scheduleTime.ts";

// WHEN EACH LEAGUE DAY IS ON, IN REAL TIME, under the room's auto play
// schedule: from the sim before it to the sim that plays it. "1:15 – 1:30 PM"
// is a day that comes up when the 1:15 sim plays the one before it, and is
// played by the 1:30 sim. A sim that plays a week gives all seven days the
// same window. Pure, so it can be tested without a worker.

export type SimWindow = {
	// The sim before it, or undefined if the schedule has none in the last week.
	start: number | undefined;
	// The sim that plays it.
	end: number;
};

// What stops the schedule on its own (see AutoPlayStopConditions), so no day
// past it gets a time.
export type SimWindowStops = {
	stopAfter?: { season: number; day: number };
	stopAfterSims?: number;
	stopAt?: number;
};

// The schedule's last sim at or before `now`.
export const prevFire = (
	rules: ScheduleRule[],
	now: Date,
): number | undefined => {
	let best: number | undefined;
	for (const rule of rules) {
		const at = prevFireForRule(rule, now);
		if (at !== undefined && (best === undefined || at > best)) {
			best = at;
		}
	}
	return best;
};

// Far enough for a whole regular season at one sim a night.
const MAX_FIRES = 400;

// Each league day the schedule will reach, with its window. Days from a stop
// the room still has to ready up for are left out: the timer can't play them
// until it does, so their times aren't known.
export const simWindows = ({
	now,
	preview,
	rules,
	stops = {},
}: {
	now: Date;
	preview: AutoPlayPreviewData;
	rules: ScheduleRule[];
	stops?: SimWindowStops;
}): Map<number, SimWindow> => {
	const out = new Map<number, SimWindow>();

	const firstStop = preview.upcomingDays.findIndex((day) => day.simStop);
	const days =
		firstStop === -1
			? preview.upcomingDays
			: preview.upcomingDays.slice(0, firstStop);
	if (days.length === 0) {
		return out;
	}
	if (
		stops.stopAfter !== undefined &&
		stops.stopAfter.season < preview.season
	) {
		return out;
	}

	let fires = nextFires(rules, now, Math.min(MAX_FIRES, days.length));
	if (stops.stopAt !== undefined) {
		fires = fires.filter((fire) => fire.at < stops.stopAt!);
	}
	if (stops.stopAfterSims !== undefined) {
		fires = fires.slice(0, Math.max(0, stops.stopAfterSims));
	}

	let start = prevFire(rules, now);
	for (const fire of projectFires(fires, days, preview.amountDays, undefined)) {
		// (A sim that starts past the stop never happens; the one that reaches
		// it plays all its days.)
		if (
			stops.stopAfter !== undefined &&
			stops.stopAfter.season === preview.season &&
			fire.fromDay > stops.stopAfter.day
		) {
			break;
		}
		for (const day of days) {
			if (day.day >= fire.fromDay && day.day <= fire.toDay) {
				out.set(day.day, { start, end: fire.at });
			}
		}
		start = fire.at;
	}
	return out;
};

// The same, by night: a date's window runs from its first day's start to its
// last day's end (the trade deadline is a day of its own on a game night).
export const simWindowsByDate = (
	windows: Map<number, SimWindow>,
	preview: AutoPlayPreviewData,
): Map<string, SimWindow> => {
	const out = new Map<string, SimWindow>();
	for (const day of preview.upcomingDays) {
		const window = windows.get(day.day);
		if (day.date === undefined || window === undefined) {
			continue;
		}
		const have = out.get(day.date);
		out.set(
			day.date,
			have
				? {
						start: have.start,
						end: Math.max(have.end, window.end),
					}
				: window,
		);
	}
	return out;
};

const MS_PER_DAY = 24 * 60 * 60 * 1000;

// Whole local days from `now`'s date to `ms`'s (DST-proof: by calendar date).
const daysFrom = (now: number, ms: number) => {
	const a = new Date(now);
	const b = new Date(ms);
	const utcA = Date.UTC(a.getFullYear(), a.getMonth(), a.getDate());
	const utcB = Date.UTC(b.getFullYear(), b.getMonth(), b.getDate());
	return Math.round((utcB - utcA) / MS_PER_DAY);
};

const clock = (ms: number) =>
	new Date(ms).toLocaleTimeString([], { hour: "numeric", minute: "2-digit" });

const weekday = (ms: number) =>
	new Date(ms).toLocaleDateString([], { weekday: "short" });

// "Today", "Tomorrow", "Yesterday", "Fri" in the week ahead, else "Fri 12/5".
const dayName = (now: number, ms: number) => {
	const n = daysFrom(now, ms);
	if (n === 0) {
		return "Today";
	}
	if (n === 1) {
		return "Tomorrow";
	}
	if (n === -1) {
		return "Yesterday";
	}
	const date = new Date(ms).toLocaleDateString([], {
		month: "numeric",
		day: "numeric",
	});
	return n >= 2 && n <= 6 ? weekday(ms) : `${weekday(ms)} ${date}`;
};

// "1:15 – 1:30 PM" (with the AM/PM said once, the device's way, where it can).
const clockRange = (start: number, end: number) => {
	try {
		const format = new Intl.DateTimeFormat([], {
			hour: "numeric",
			minute: "2-digit",
		}) as Intl.DateTimeFormat & {
			formatRange?: (a: number, b: number) => string;
		};
		if (format.formatRange) {
			return format.formatRange(start, end);
		}
	} catch {}
	return `${clock(start)} – ${clock(end)}`;
};

// A window in the device's own time: "1:15 – 1:30 PM" today, "Tomorrow 1:15 –
// 1:30 PM", "Fri 1:15 – 1:30 PM", and across midnight "Today 11:00 PM –
// Tomorrow 9:00 AM".
export const formatSimWindow = (
	window: SimWindow,
	now: number = Date.now(),
): string => {
	const { start, end } = window;
	const prefix = daysFrom(now, end) === 0 ? "" : `${dayName(now, end)} `;
	if (start === undefined) {
		return `${prefix}${clock(end)}`;
	}
	if (daysFrom(start, end) !== 0) {
		return `${dayName(now, start)} ${clock(start)} – ${dayName(now, end)} ${clock(end)}`;
	}
	return `${prefix}${clockRange(start, end)}`;
};
