import { helpers } from "../../util/index.ts";

// NIL money (thousands of dollars per year) a player of this value commands.
// Stars draw seven figures, rotation players tens of thousands, walk-on types
// next to nothing.
export const collegeNilForValue = (value: number) => {
	const amount = 10 * 1.13 ** (value - 40);
	return helpers.bound(Math.round(amount / 5) * 5, 5, 5000);
};

// High school seniors generated each year: more than there are scholarships,
// so the bottom of every class goes unsigned.
export const recruitClassSize = (numTeams: number) => Math.round(numTeams * 4);
