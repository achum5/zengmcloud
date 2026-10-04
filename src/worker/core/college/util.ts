// Recruiting stars by rank in a class, scaled so a full D1 class has about
// 30 five-stars and 300 four-stars.
export const starsForRank = (rank: number, classSize: number) => {
	const scale = classSize / 1460;
	if (rank <= 30 * scale) {
		return 5;
	}
	if (rank <= 300 * scale) {
		return 4;
	}
	if (rank <= 1000 * scale) {
		return 3;
	}
	if (rank <= 1300 * scale) {
		return 2;
	}
	return 1;
};

// NIL money (thousands of dollars per year) a player asks for. Five-stars draw
// seven figures, rotation players tens of thousands, walk-on types next to
// nothing.
export const askForRank = (stars: number, rank: number) => {
	if (stars === 5) {
		return Math.max(700, 1500 - (rank - 1) * 25);
	}
	if (stars === 4) {
		return Math.max(150, Math.round(500 - (rank - 31) * 1.2));
	}
	if (stars === 3) {
		return Math.max(25, Math.round(120 - (rank - 301) * 0.13));
	}
	return stars === 2 ? 15 : 5;
};

// The same scale for a player already in college, by where he ranks (0 = the
// best) among a group of players: the top 2% are paid like five-star
// recruits, and so on.
export const collegeStarsForPercentile = (pct: number) =>
	starsForRank(Math.max(1, Math.round(pct * 1460)), 1460);

export const collegeNilForPercentile = (pct: number) => {
	const rank = Math.max(1, Math.round(pct * 1460));
	return askForRank(starsForRank(rank, 1460), rank);
};

// High school seniors generated each year: more than there are scholarships,
// so the bottom of every class goes unsigned.
export const recruitClassSize = (numTeams: number) => Math.round(numTeams * 4);
