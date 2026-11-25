import { create } from "zustand";

// Each league day's real-time window under the room's auto play schedule,
// formatted in this device's time ("1:15 – 1:30 PM"), for the calendars to
// show in a bubble over a day: on hover, or press and hold on a phone (see
// simTimeTip.ts). Kept up by SimTimesWatcher; empty whenever auto play isn't
// running. Only days still to be played that the schedule will reach are in
// it.

type SimTimesState = {
	season: number | undefined;
	byDay: Map<number, string>;
	byDate: Map<string, string>;
};

export const useSimTimesStore = create<SimTimesState>(() => ({
	season: undefined,
	byDay: new Map(),
	byDate: new Map(),
}));

export type SimTimes = {
	// A night on the NBA calendar.
	date: (date: string | undefined) => string | undefined;
	// A day of a season (classic leagues, or a day of the schedule).
	day: (season: number, day: number | undefined) => string | undefined;
};

const lookup = (state: SimTimesState): SimTimes => ({
	date: (date) => (date === undefined ? undefined : state.byDate.get(date)),
	day: (season, day) =>
		day === undefined || season !== state.season
			? undefined
			: state.byDay.get(day),
});

let lastState: SimTimesState | undefined;
let lastLookup: SimTimes | undefined;
const select = (state: SimTimesState) => {
	if (state !== lastState || !lastLookup) {
		lastState = state;
		lastLookup = lookup(state);
	}
	return lastLookup;
};

export const useSimTimes = () => useSimTimesStore(select);
