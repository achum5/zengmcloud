// Letter grades for team category ratings, used when team ratings are hidden.
//
// The point is to keep Power Rankings readable - "this team can shoot, it can't
// rebound" - without handing back the number the setting exists to hide. So the
// grade is RELATIVE: it says where a team sits in this league this season, not
// what its rating is. A 55 shooting team is an A in a weak league and a D in a
// strong one, which is the useful thing to know anyway.
//
// Graded on an actual curve rather than fixed quintiles, so the spread of the
// league shows through. A season where one team is miles ahead produces one A;
// a season where everyone is bunched produces mostly Cs. Fixed quintiles would
// force exactly six of each and hide that.

export const TEAM_RATING_GRADES = ["A", "B", "C", "D", "F"] as const;

export type TeamRatingGrade = (typeof TEAM_RATING_GRADES)[number];

// Standard deviations from the league mean at which each grade starts. Chosen
// so a normal distribution lands roughly 16/21/26/21/16 percent per grade.
const CUTOFFS: [number, TeamRatingGrade][] = [
	[1, "A"],
	[1 / 3, "B"],
	[-1 / 3, "C"],
	[-1, "D"],
];

// A curve on its own is scale-invariant, which is wrong at the bottom end: a
// league where every team is within a rating point of every other would still
// hand out As and Fs, dressing up noise as a difference. Ratings are integers
// on a 0-100 scale, so anything under a couple of points of league-wide spread
// is nothing. Dividing by at least this much collapses such a league toward C
// without special-casing it.
const MIN_SPREAD = 2;

// The grade for one value against a league already summarized. Split out from
// gradeTeamRatings so a table can summarize once and grade each row as it goes.
export const gradeAgainst = (
	value: number,
	{ mean, stdDev }: { mean: number; stdDev: number },
): TeamRatingGrade => {
	const z = (value - mean) / Math.max(stdDev, MIN_SPREAD);
	for (const [cutoff, grade] of CUTOFFS) {
		if (z >= cutoff) {
			return grade;
		}
	}
	return "F";
};

export const summarizeTeamRatings = (values: number[]) => {
	if (values.length === 0) {
		return { mean: 0, stdDev: 0 };
	}
	const mean = values.reduce((sum, x) => sum + x, 0) / values.length;
	const variance =
		values.reduce((sum, x) => sum + (x - mean) ** 2, 0) / values.length;
	return { mean, stdDev: Math.sqrt(variance) };
};

// Grade a whole league's worth of one category at once, in the same order.
export const gradeTeamRatings = (values: number[]): TeamRatingGrade[] => {
	const summary = summarizeTeamRatings(values);
	return values.map((value) => gradeAgainst(value, summary));
};
