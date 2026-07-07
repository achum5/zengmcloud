// Pure scheduling model + next-fire computation for the auto-simmer. Kept free
// of any worker/DOM imports so it can be unit-tested in isolation (the scheduler
// itself pulls in toWorker, which instantiates a real Worker).

export type AutoPlayAmount = "day" | "week" | "month";

export type ScheduleRule = {
	id: string;
	enabled: boolean;
	// Days of week this rule runs, 0=Sun … 6=Sat. Empty = every day.
	days: number[];
	mode: "at" | "every";
	// mode "at": fire once at each of these "HH:MM" times.
	times: string[];
	// mode "every": fire every `everyMinutes` within the [start, end] window.
	start: string; // "HH:MM"
	end: string; // "HH:MM"
	everyMinutes: number;
	amount: AutoPlayAmount;
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
});

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
	const everyMs = Math.max(1, rule.everyMinutes) * 60_000;
	for (let offset = 0; offset <= 7; offset++) {
		const dayStart = atOffset(now, offset, start.h, start.m);
		const dayEnd = atOffset(now, offset, end.h, end.m);
		if (dayEnd < dayStart) {
			// We don't support windows that cross midnight; skip.
			continue;
		}
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
			// No more slots today; fall through to the next matching day.
		}
	}
	return undefined;
};
