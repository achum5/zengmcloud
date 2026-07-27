// Letter grades for the Power Rankings category columns, used when team ratings
// are hidden.
//
// The point is to keep the table readable - "this team can shoot, it can't
// rebound" - without handing back the number the setting exists to hide. So the
// grade is RELATIVE: it says where a team sits in this league this season.
//
// The input is the team's RANK in that category, which is what the view already
// computes from the real (hidden) values - see otherToRanks in
// worker/views/powerRankings. Rank 1 is best. A rank is exactly a percentile
// position, so grading straight off it can never disagree with the ordering the
// column sorts by.

export const TEAM_RATING_GRADES = ["A", "B", "C", "D", "F"] as const;

export type TeamRatingGrade = (typeof TEAM_RATING_GRADES)[number];

// Equal fifths of the league: in a 30-team league ranks 1-6 are A, 7-12 B,
// 13-18 C, 19-24 D, 25-30 F.
//
// Dividing by numTeams - 1 rather than numTeams is what makes the best team
// always an A and the worst always an F, at any league size. Dividing by
// numTeams leaves the bottom of a small league short of the F band.
export const gradeFromRank = (
	rank: number,
	numTeams: number,
): TeamRatingGrade => {
	// Nothing to rank against, or a rank the view couldn't compute. Average is
	// the honest answer; the bug this replaced defaulted to F and shipped a table
	// that was almost entirely Fs.
	if (!Number.isFinite(rank) || numTeams < 2) {
		return "C";
	}
	const percentile = (rank - 1) / (numTeams - 1);
	const index = Math.min(
		TEAM_RATING_GRADES.length - 1,
		Math.max(0, Math.floor(percentile * TEAM_RATING_GRADES.length)),
	);
	return TEAM_RATING_GRADES[index]!;
};
