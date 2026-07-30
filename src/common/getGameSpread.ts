import { bySport } from "./sportFunctions.ts";
import { defaultGameAttributes } from "./defaultGameAttributes.ts";

// Every spread SHOWN outside the sportsbook lands on a whole or half point.
// (The book's own lines go through toHalfPointLine instead, which forces the
// half so a bet can't push.)
export const roundHalf = (x: number) => Math.round(x * 2) / 2;

// The pregame point spread from the HOME team's (teams[0]) perspective:
//   > 0  home favored by that many points
//   < 0  away favored by |that| points
//   0    pick'em
// This mirrors exactly what ScoreBox shows next to each game (favorite listed
// with the negative number). Returns undefined when the team OVRs aren't
// available (legacy games), so callers can omit the spread. `neutralSite` should
// already account for finals/playoff neutral courts (home-court advantage is
// dropped when true).
export const getGameSpread = ({
	ovr0,
	ovr1,
	homeCourtAdvantage,
	neutralSite,
	numPeriods,
	quarterLength,
}: {
	ovr0: number | undefined;
	ovr1: number | undefined;
	homeCourtAdvantage: number;
	neutralSite: boolean;
	numPeriods: number;
	quarterLength: number;
}): number | undefined => {
	if (ovr0 === undefined || ovr1 === undefined) {
		return undefined;
	}

	// From @nicidob https://github.com/nicidob/bbgm/blob/master/team_win_testing.ipynb
	// Default homeCourtAdvantage is 1.
	const actualHomeCourtAdvantage = neutralSite
		? 0
		: bySport({
				baseball: 1,
				basketball: 3.3504,
				football: 3,
				hockey: 0.25,
			}) * homeCourtAdvantage;

	let spread = bySport({
		baseball: () => (1 / 10) * (ovr0 - ovr1) + actualHomeCourtAdvantage,
		basketball: () => (15 / 50) * (ovr0 - ovr1) + actualHomeCourtAdvantage,
		football: () => (3 / 10) * (ovr0 - ovr1) + actualHomeCourtAdvantage,
		hockey: () => (1.8 / 100) * (ovr0 - ovr1) + actualHomeCourtAdvantage,
	})();

	// Adjust for game length.
	spread *=
		(numPeriods * quarterLength) /
		(defaultGameAttributes.numPeriods * defaultGameAttributes.quarterLength);

	return roundHalf(spread);
};
