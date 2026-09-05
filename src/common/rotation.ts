// A ROTATION PLAN: WHO THE COACH MEANS TO HAVE ON THE FLOOR, AND WHEN.
//
// The sim never chooses a lineup. It scores every player at each dead ball and
// plays the top five, subject to the rules of the game - which is why a plan
// can be a guide rather than a script. The plan says who is meant to be out
// there at a given moment; the sim gives those men a decisive edge in the
// scoring and lets everything else it already knows about fouls, injuries and
// blowouts overrule it when it must.
//
// Time is stored as a fraction of a period, never a minute. Leagues run
// periods of twelve, fifteen and twenty minutes, and a plan drawn under one
// length should still mean the same thing under another. A stint from 0 to 0.5
// of period 2 is the first half of the third quarter, whatever a quarter is.
//
// Shared between the worker, which follows it, and the UI, which draws it.

export type RotationStint = {
	pid: number;
	// Zero-based. Anything at or beyond the number of regulation periods is
	// overtime, which no plan covers.
	period: number;
	// Fractions of the period, with start < end <= 1.
	start: number;
	end: number;
};

export type TeamRotation = {
	// Let the coach handle it. The plan is kept, so switching back does not
	// lose it, but the sim ignores it.
	auto: boolean;
	stints: RotationStint[];
};

// A fraction that lands exactly on a stint boundary belongs to the stint that
// starts there, not the one that ends there, so a swap planned for the six
// minute mark happens at the six minute mark.
const EPSILON = 1e-9;

// Everybody planned to be on the floor at one moment of the game.
export const lineupAt = (
	stints: readonly RotationStint[],
	period: number,
	fraction: number,
): Set<number> => {
	const pids = new Set<number>();
	for (const stint of stints) {
		if (
			stint.period === period &&
			stint.start - EPSILON <= fraction &&
			fraction < stint.end - EPSILON
		) {
			pids.add(stint.pid);
		}
	}
	return pids;
};

// A player's planned minutes for the game.
export const plannedMinutes = (
	stints: readonly RotationStint[],
	pid: number,
	periodLength: number,
) => {
	let total = 0;
	for (const stint of stints) {
		if (stint.pid === pid) {
			total += (stint.end - stint.start) * periodLength;
		}
	}
	return total;
};

// Whatever the UI or a league file hands over, reduced to something the sim
// can follow: only players on the roster, only periods that exist, stints
// clipped to their period, empty and inverted ones dropped, and one player's
// overlapping stints merged so he is never counted twice at the same moment.
export const sanitizeRotation = (
	rotation: TeamRotation | undefined,
	rosterPids: ReadonlySet<number>,
	numPeriods: number,
): TeamRotation | undefined => {
	if (!rotation) {
		return undefined;
	}

	const byPid = new Map<number, RotationStint[]>();
	for (const raw of rotation.stints ?? []) {
		if (
			typeof raw?.pid !== "number" ||
			!rosterPids.has(raw.pid) ||
			!Number.isInteger(raw.period) ||
			raw.period < 0 ||
			raw.period >= numPeriods
		) {
			continue;
		}
		const start = Math.max(0, Math.min(1, Number(raw.start)));
		const end = Math.max(0, Math.min(1, Number(raw.end)));
		if (!(end > start + EPSILON)) {
			continue;
		}
		const list = byPid.get(raw.pid) ?? [];
		list.push({ pid: raw.pid, period: raw.period, start, end });
		byPid.set(raw.pid, list);
	}

	const stints: RotationStint[] = [];
	for (const list of byPid.values()) {
		list.sort((a, b) => a.period - b.period || a.start - b.start);
		let current: RotationStint | undefined;
		for (const stint of list) {
			if (
				current &&
				current.period === stint.period &&
				stint.start <= current.end + EPSILON
			) {
				current.end = Math.max(current.end, stint.end);
			} else {
				current = { ...stint };
				stints.push(current);
			}
		}
	}

	stints.sort(
		(a, b) => a.period - b.period || a.start - b.start || a.pid - b.pid,
	);

	return { auto: rotation.auto !== false, stints };
};

// THE EDITOR'S VIEW OF THE SAME PLAN: A GRID OF MINUTES.
//
// People plan in minutes, so the page shows one cell per minute of each
// period and the plan converts to and from that grid at the edges. A cell is
// on when the player is planned for that whole minute.

export type RotationGrid = Map<number, boolean[][]>; // pid -> [period][minute]

export const rotationToGrid = (
	stints: readonly RotationStint[],
	pids: readonly number[],
	numPeriods: number,
	periodLength: number,
): RotationGrid => {
	const grid: RotationGrid = new Map();
	for (const pid of pids) {
		grid.set(
			pid,
			Array.from({ length: numPeriods }, () =>
				Array.from({ length: periodLength }, () => false),
			),
		);
	}

	for (const stint of stints) {
		const row = grid.get(stint.pid);
		if (!row || stint.period >= numPeriods) {
			continue;
		}
		// A minute counts when the stint covers most of it, so a plan drawn
		// under one period length still reads sensibly under another.
		for (let minute = 0; minute < periodLength; minute++) {
			const mid = (minute + 0.5) / periodLength;
			if (stint.start - EPSILON <= mid && mid < stint.end - EPSILON) {
				row[stint.period]![minute] = true;
			}
		}
	}

	return grid;
};

export const gridToRotation = (
	grid: RotationGrid,
	periodLength: number,
): RotationStint[] => {
	const stints: RotationStint[] = [];
	for (const [pid, periods] of grid) {
		for (const [period, minutes] of periods.entries()) {
			let start: number | undefined;
			for (let minute = 0; minute <= minutes.length; minute++) {
				const on = minute < minutes.length && minutes[minute];
				if (on && start === undefined) {
					start = minute;
				} else if (!on && start !== undefined) {
					stints.push({
						pid,
						period,
						start: start / periodLength,
						end: minute / periodLength,
					});
					start = undefined;
				}
			}
		}
	}
	stints.sort(
		(a, b) => a.period - b.period || a.start - b.start || a.pid - b.pid,
	);
	return stints;
};

// How many are planned for each minute, which is what the editor colors: a
// minute with the wrong count is the one thing a plan can get wrong that the
// sim cannot quietly fix in the way the planner meant.
export const playersPerMinute = (
	grid: RotationGrid,
	numPeriods: number,
	periodLength: number,
): number[][] => {
	const counts = Array.from({ length: numPeriods }, () =>
		Array.from({ length: periodLength }, () => 0),
	);
	for (const periods of grid.values()) {
		for (const [period, minutes] of periods.entries()) {
			for (const [minute, on] of minutes.entries()) {
				if (on) {
					counts[period]![minute]! += 1;
				}
			}
		}
	}
	return counts;
};
