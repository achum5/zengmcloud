// WHERE THE SIM STOPS ON PURPOSE.
//
// A room of humans needs the league to pause at the moments worth pausing at.
// The trade deadline was hardcoded as the only one and it stopped whether the
// league wanted it or not; this makes it a list, and makes the deadline one
// entry in that list rather than a special case.
//
// Stored as a plain comma-separated string so it can be typed into League
// Settings and read back at a glance ("15, 41, deadline"), and so it syncs as
// one small game attribute rather than a structure that has to be merged.
//
// A day entry means: stop when day N is the NEXT thing to be played, before it
// is played. That is how the deadline already behaves - you stop on arrival,
// deal, and then cross - so a number and the word "deadline" mean the same
// kind of thing.

export const SIM_STOP_DEADLINE = "deadline";

export type SimStopDays = {
	// Stop when the trade deadline sentinel is next.
	deadline: boolean;
	// Regular-season day numbers to stop before, ascending and deduplicated.
	days: number[];
};

export const EMPTY_SIM_STOP_DAYS: SimStopDays = { deadline: false, days: [] };

// Lenient on the way in - people type "15,deadline" and "Day 15, 41" - and
// strict about what it will accept as a day, since a typo that silently became
// day 0 would stop the league somewhere it can never leave.
export const parseSimStopDays = (raw: string | undefined): SimStopDays => {
	if (typeof raw !== "string" || raw.trim() === "") {
		return EMPTY_SIM_STOP_DAYS;
	}

	let deadline = false;
	const days = new Set<number>();
	for (const part of raw.split(",")) {
		const token = part
			.trim()
			.toLowerCase()
			.replace(/^day\s+/, "");
		if (token === "") {
			continue;
		}
		if (token === SIM_STOP_DEADLINE) {
			deadline = true;
			continue;
		}
		const day = Number(token);
		if (Number.isInteger(day) && day > 0) {
			days.add(day);
		}
	}

	return { deadline, days: [...days].sort((a, b) => a - b) };
};

// What the settings field shows. Round-trips through parseSimStopDays.
export const formatSimStopDays = ({ deadline, days }: SimStopDays): string =>
	[...days.map(String), ...(deadline ? [SIM_STOP_DEADLINE] : [])].join(", ");

// Whether every token is something parseSimStopDays would keep, so the settings
// form can reject a typo instead of quietly dropping it.
export const invalidSimStopDayToken = (
	raw: string | undefined,
): string | undefined => {
	if (typeof raw !== "string") {
		return undefined;
	}
	for (const part of raw.split(",")) {
		const token = part
			.trim()
			.toLowerCase()
			.replace(/^day\s+/, "");
		if (token === "" || token === SIM_STOP_DEADLINE) {
			continue;
		}
		const day = Number(token);
		if (!Number.isInteger(day) || day <= 0) {
			return part.trim();
		}
	}
	return undefined;
};

export const stopsOnDay = (
	stops: SimStopDays,
	day: number | undefined,
): boolean => typeof day === "number" && stops.days.includes(day);
