// Pure scheduling model + next-fire computation for the auto-simmer. Kept free
// of any worker/DOM imports so it can be unit-tested in isolation (the scheduler
// itself pulls in toWorker, which instantiates a real Worker).

// "days" carries its own count in `numDays`, so a rule can sim any number of
// league days rather than only the three Play Menu presets.
export type AutoPlayAmount = "day" | "week" | "month" | "days";

export type ScheduleRule = {
	id: string;
	enabled: boolean;
	// Optional name, so a list of rules is readable at a glance.
	label?: string;
	// Days of week this rule runs, 0=Sun … 6=Sat. Empty = every day.
	days: number[];
	mode: "at" | "every";
	// mode "at": fire once at each of these "HH:MM" times.
	times: string[];
	// mode "every": fire every `everyMinutes` within the [start, end] window. An
	// end EARLIER than the start means the window runs overnight into the next
	// day, which is the normal shape for an unattended sim.
	start: string; // "HH:MM"
	end: string; // "HH:MM"
	everyMinutes: number;
	amount: AutoPlayAmount;
	// Only meaningful when amount is "days".
	numDays: number;
};

const makeId = (): string => {
	if (typeof crypto !== "undefined" && crypto.randomUUID) {
		return crypto.randomUUID();
	}
	return `${Date.now()}-${Math.floor(Math.random() * 1e9)}`;
};

export const newRule = (): ScheduleRule => ({
	id: makeId(),
	enabled: true,
	days: [],
	mode: "every",
	times: ["20:00"],
	start: "00:00",
	end: "23:59",
	everyMinutes: 30,
	amount: "day",
	numDays: 3,
});

export const DAY_NAMES = ["Su", "Mo", "Tu", "We", "Th", "Fr", "Sa"];

// "09:00" → "9:00 AM"
export const to12h = (hhmm: string): string => {
	const [hStr, m = "00"] = (hhmm ?? "").split(":");
	const h = Number(hStr);
	if (Number.isNaN(h)) {
		return hhmm;
	}
	const ampm = h < 12 ? "AM" : "PM";
	const h12 = h % 12 === 0 ? 12 : h % 12;
	return `${h12}:${m} ${ampm}`;
};

// What one fire of this rule advances, in words.
export const describeAmount = (rule: {
	amount: AutoPlayAmount;
	numDays: number;
}): string => {
	if (rule.amount === "days") {
		const n = Math.max(1, Math.round(rule.numDays));
		return n === 1 ? "1 day" : `${n} days`;
	}
	return rule.amount;
};

// One human-readable line describing a rule. Shown on the rule itself and
// broadcast to the other devices in the room, so both read the same way.
export const summarizeRule = (rule: ScheduleRule): string => {
	const days =
		rule.days.length === 0 || rule.days.length === 7
			? "every day"
			: [...rule.days]
					.sort((a, b) => a - b)
					.map((d) => DAY_NAMES[d])
					.join(",");
	const amount = `sim ${describeAmount(rule)}`;
	if (rule.mode === "at") {
		return `${days} at ${rule.times.map(to12h).join(", ")} — ${amount}`;
	}
	const overnight = crossesMidnight(rule.start, rule.end) ? " (overnight)" : "";
	return `${days}, every ${rule.everyMinutes} min ${to12h(rule.start)}–${to12h(rule.end)}${overnight} — ${amount}`;
};

const parseHHMM = (s: string): { h: number; m: number } | undefined => {
	const match = /^(\d{1,2}):(\d{2})$/.exec((s ?? "").trim());
	if (!match) {
		return undefined;
	}
	const h = Number(match[1]);
	const m = Number(match[2]);
	if (h < 0 || h > 23 || m < 0 || m > 59) {
		return undefined;
	}
	return { h, m };
};

// An end time earlier than the start means the window runs past midnight.
export const crossesMidnight = (start: string, end: string): boolean => {
	const s = parseHHMM(start);
	const e = parseHHMM(end);
	if (!s || !e) {
		return false;
	}
	return e.h * 60 + e.m < s.h * 60 + s.m;
};

const atOffset = (
	now: Date,
	dayOffset: number,
	h: number,
	m: number,
): number => {
	const d = new Date(now);
	d.setDate(d.getDate() + dayOffset);
	d.setHours(h, m, 0, 0);
	return d.getTime();
};

const dayMatches = (days: number[], dow: number): boolean =>
	days.length === 0 || days.includes(dow);

// The next time a rule should fire, strictly after `now`, or undefined if it
// never will (e.g. no valid times). Looks up to a week ahead.
export const nextFireForRule = (
	rule: ScheduleRule,
	now: Date,
): number | undefined => {
	if (!rule.enabled) {
		return undefined;
	}
	const nowMs = now.getTime();

	if (rule.mode === "at") {
		const times = rule.times
			.map(parseHHMM)
			.filter((t): t is { h: number; m: number } => t !== undefined);
		if (times.length === 0) {
			return undefined;
		}
		for (let offset = 0; offset <= 7; offset++) {
			const dow = new Date(atOffset(now, offset, 0, 0)).getDay();
			if (!dayMatches(rule.days, dow)) {
				continue;
			}
			let best: number | undefined;
			for (const t of times) {
				const fire = atOffset(now, offset, t.h, t.m);
				if (fire > nowMs && (best === undefined || fire < best)) {
					best = fire;
				}
			}
			if (best !== undefined) {
				return best;
			}
		}
		return undefined;
	}

	// mode "every"
	const start = parseHHMM(rule.start);
	const end = parseHHMM(rule.end);
	if (!start || !end) {
		return undefined;
	}
	const overnight = end.h * 60 + end.m < start.h * 60 + start.m;
	const everyMs = Math.max(1, rule.everyMinutes) * 60_000;

	// Start at -1 so an overnight window opened YESTERDAY and still running is
	// found. The day-of-week test is against the day the window OPENS.
	for (let offset = -1; offset <= 7; offset++) {
		const dayStart = atOffset(now, offset, start.h, start.m);
		const dayEnd = overnight
			? atOffset(now, offset + 1, end.h, end.m)
			: atOffset(now, offset, end.h, end.m);
		const dow = new Date(dayStart).getDay();
		if (!dayMatches(rule.days, dow)) {
			continue;
		}
		if (nowMs < dayStart) {
			return dayStart;
		}
		if (nowMs <= dayEnd) {
			const slots = Math.floor((nowMs - dayStart) / everyMs) + 1;
			const next = dayStart + slots * everyMs;
			if (next <= dayEnd) {
				return next;
			}
			// No more slots in this window; fall through to the next matching day.
		}
	}
	return undefined;
};
