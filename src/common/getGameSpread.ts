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
// Basketball, with the synergy difference known. Measured against the engine
// itself - every pairing of two real leagues' rosters, 60 sims a pairing,
// 104,400 games - the two coefficient sets came out (0.175, 8.43) and (0.165,
// 8.84), and each league's coefficients priced the OTHER league within 0.02
// points of its own best fit. These are the averages.
//
// Why the ovr slope drops from 0.3 when synergy is present: on natural rosters
// synergy and talent run together (r ~ 0.8), so the 0.3 slope was carrying
// synergy implicitly. Naming it splits the credit - and cuts the model's error
// against the engine from 2.8 points to 2.0 on a 1.3-point noise floor. The
// straight line in ovr was never wrong (a refit moved it 0.03); what it could
// not see was that two teams the same ovr apart can be a different SHAPE apart.
const BASKETBALL_SYNERGY_OVR_SLOPE = 0.17;
const BASKETBALL_SYNERGY_COEF = 8.6;

export const getGameSpread = ({
	ovr0,
	ovr1,
	homeCourtAdvantage,
	neutralSite,
	numPeriods,
	quarterLength,
	synergyDiff,
}: {
	ovr0: number | undefined;
	ovr1: number | undefined;
	homeCourtAdvantage: number;
	neutralSite: boolean;
	numPeriods: number;
	quarterLength: number;
	// Home minus away pregame lineup synergy (synergyTotal units - see
	// pregameLineupSynergy in GameSim.basketball/synergy.ts). Basketball only;
	// when undefined the ovr-only model is used, so legacy callers and finished
	// games with no stored synergy keep their old numbers.
	synergyDiff?: number;
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
		basketball: () =>
			synergyDiff !== undefined && Number.isFinite(synergyDiff)
				? BASKETBALL_SYNERGY_OVR_SLOPE * (ovr0 - ovr1) +
					BASKETBALL_SYNERGY_COEF * synergyDiff +
					actualHomeCourtAdvantage
				: (15 / 50) * (ovr0 - ovr1) + actualHomeCourtAdvantage,
		football: () => (3 / 10) * (ovr0 - ovr1) + actualHomeCourtAdvantage,
		hockey: () => (1.8 / 100) * (ovr0 - ovr1) + actualHomeCourtAdvantage,
	})();

	// Adjust for game length.
	spread *=
		(numPeriods * quarterLength) /
		(defaultGameAttributes.numPeriods * defaultGameAttributes.quarterLength);

	return roundHalf(spread);
};
