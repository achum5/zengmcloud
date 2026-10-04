// Class years for college leagues. A player's freshman season is stored as
// collegeYear0; a redshirt year doesn't use up eligibility.

export const COLLEGE_CLASSES = ["FR", "SO", "JR", "SR"] as const;

export const collegeYear = (
	p: { collegeYear0?: number; redshirt?: number },
	season: number,
) => {
	if (p.collegeYear0 === undefined) {
		return undefined;
	}
	let year = season - p.collegeYear0 + 1;
	if (p.redshirt !== undefined && p.redshirt < season) {
		year -= 1;
	}
	return year;
};

export const collegeClassLabel = (
	p: { collegeYear0?: number; redshirt?: number },
	season: number,
) => {
	const year = collegeYear(p, season);
	if (year === undefined) {
		return "";
	}
	if (year < 1) {
		return "HS";
	}
	const label = COLLEGE_CLASSES[Math.min(year, 4) - 1]!;
	return p.redshirt !== undefined && p.redshirt < season
		? `RS ${label}`
		: label;
};

// Last season he can play: four seasons from his freshman year, five with a
// redshirt.
export const collegeFinalSeason = (p: {
	collegeYear0?: number;
	redshirt?: number;
}) =>
	p.collegeYear0 === undefined
		? undefined
		: p.collegeYear0 + 3 + (p.redshirt !== undefined ? 1 : 0);

// Conference tournament progress, kept in game attributes while they run.
export type CollegeConfTourney = {
	season: number;
	// Seed order: alive[cid][0] is the top remaining seed.
	alive: Record<number, number[]>;
	champs: Record<number, number>;
	// [home, away, cid] for games scheduled but not yet resolved.
	pending: [number, number, number][];
};

// NCAA seeds: the field is seeded 1-64 overall, four teams per seed line.
export const collegeSeedLine = (overallSeed: number) =>
	Math.ceil(overallSeed / 4);
