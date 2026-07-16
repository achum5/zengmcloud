// Session-scoped memory (survives in-app navigation, reset on a full refresh) of
// the Daily Schedule's transient UI state, so leaving the page and coming back
// restores which game-note recaps were open and where you were scrolled.

const expandedNotes = new Set<number>();

export const isGameNoteExpanded = (gid: number): boolean =>
	expandedNotes.has(gid);

export const setGameNoteExpanded = (gid: number, value: boolean): void => {
	if (value) {
		expandedNotes.add(gid);
	} else {
		expandedNotes.delete(gid);
	}
};

// Same, for the whole-day "Day in the League" recap, keyed by (season, day).
const expandedDayNotes = new Set<string>();
const dayKey = (season: number, day: number) => `${season}-${day}`;

export const isDayNoteExpanded = (season: number, day: number): boolean =>
	expandedDayNotes.has(dayKey(season, day));

export const setDayNoteExpanded = (
	season: number,
	day: number,
	value: boolean,
): void => {
	if (value) {
		expandedDayNotes.add(dayKey(season, day));
	} else {
		expandedDayNotes.delete(dayKey(season, day));
	}
};

// Scroll position keyed by which day is showing (season + day), so each day
// remembers its own spot.
const scrollByKey = new Map<string, number>();

export const getDailyScheduleScroll = (key: string): number | undefined =>
	scrollByKey.get(key);

export const setDailyScheduleScroll = (key: string, y: number): void => {
	scrollByKey.set(key, y);
};
