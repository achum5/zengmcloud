import { bySport, isSport } from "./sportFunctions.ts";
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
export const BASKETBALL_SYNERGY_OVR_SLOPE = 0.17;
export const BASKETBALL_SYNERGY_COEF = 8.6;

// PLAYOFF games are a different margin model: the engine plays them with
// synergy 2.5x as important and fatigue cut nearly in half (see
// GameSim.basketball), and the same measurement run with those parameters
// (spreadCalibration.test.ts, SPREAD_CALIBRATION_PLAYOFFS=1, both real
// leagues) came back with the synergy coefficient roughly DOUBLED, part of
// the ovr slope's credit moving with it, and home court worth 4.91 points
// instead of 3.35 - the two leagues' refits were (0.135, 16.7, 4.910) and
// (0.081, 18.8, 4.908). The coefficient split wobbles between leagues
// (synergy and talent are collinear on real rosters) but the combined effect
// transfers; these are the averages.
export const BASKETBALL_PLAYOFF_SYNERGY_OVR_SLOPE = 0.108;
export const BASKETBALL_PLAYOFF_SYNERGY_COEF = 17.7;
// The ovr-only fallback's playoff slope, from the same runs' combined effect
// (a roster too thin to read a synergy still plays playoff games).
export const BASKETBALL_PLAYOFF_OVR_SLOPE = 0.37;
// Playoff home court / regular home court, basketball (4.909 / 3.3504).
export const BASKETBALL_PLAYOFF_HCA_FACTOR = 1.465;
// The engine scores about 6.6% fewer points under playoff parameters (mean
// total 208.7 -> 194.3 and 208.6 -> 195.5 on the same rosters), so a playoff
// total built from regular-season scoring rates has to come down by this.
export const BASKETBALL_PLAYOFF_TOTAL_FACTOR = 0.934;

// Home-court advantage in points at a given league setting (default setting is
// 1), before game-length scaling. The basketball number is measured against the
// engine on real rosters (the same 104,400-sim runs as the coefficients above);
// note it is roster-dependent - synthetic random-player leagues measure ~1.8 -
// so it describes leagues people actually play.
export const homeCourtAdvantagePoints = (homeCourtAdvantage: number): number =>
	bySport({
		baseball: 1,
		basketball: 3.3504,
		football: 3,
		hockey: 0.25,
	}) * homeCourtAdvantage;

// How margins scale with game length relative to the sport's default. Expected
// margins scale linearly with minutes played; per-game noise scales with its
// square root (more possessions average it out), so sigma consumers multiply by
// Math.sqrt of this.
export const gameLengthFactor = (
	numPeriods: number,
	quarterLength: number,
): number =>
	(numPeriods * quarterLength) /
	(defaultGameAttributes.numPeriods * defaultGameAttributes.quarterLength);

export const getGameSpread = ({
	ovr0,
	ovr1,
	homeCourtAdvantage,
	neutralSite,
	numPeriods,
	quarterLength,
	synergyDiff,
	playoffs,
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
	// A playoff game (basketball): the engine plays those under different
	// parameters, so the measured playoff coefficients and the bigger playoff
	// home-court advantage apply - see the constants above.
	playoffs?: boolean;
}): number | undefined => {
	if (ovr0 === undefined || ovr1 === undefined) {
		return undefined;
	}

	const basketballPlayoffs = playoffs === true && isSport("basketball");

	// From @nicidob https://github.com/nicidob/bbgm/blob/master/team_win_testing.ipynb
	// Default homeCourtAdvantage is 1.
	const actualHomeCourtAdvantage = neutralSite
		? 0
		: homeCourtAdvantagePoints(homeCourtAdvantage) *
			(basketballPlayoffs ? BASKETBALL_PLAYOFF_HCA_FACTOR : 1);

	let spread = bySport({
		baseball: () => (1 / 10) * (ovr0 - ovr1) + actualHomeCourtAdvantage,
		basketball: () =>
			synergyDiff !== undefined && Number.isFinite(synergyDiff)
				? (basketballPlayoffs
						? BASKETBALL_PLAYOFF_SYNERGY_OVR_SLOPE
						: BASKETBALL_SYNERGY_OVR_SLOPE) *
						(ovr0 - ovr1) +
					(basketballPlayoffs
						? BASKETBALL_PLAYOFF_SYNERGY_COEF
						: BASKETBALL_SYNERGY_COEF) *
						synergyDiff +
					actualHomeCourtAdvantage
				: (basketballPlayoffs ? BASKETBALL_PLAYOFF_OVR_SLOPE : 15 / 50) *
						(ovr0 - ovr1) +
					actualHomeCourtAdvantage,
		football: () => (3 / 10) * (ovr0 - ovr1) + actualHomeCourtAdvantage,
		hockey: () => (1.8 / 100) * (ovr0 - ovr1) + actualHomeCourtAdvantage,
	})();

	// Adjust for game length.
	spread *= gameLengthFactor(numPeriods, quarterLength);

	return roundHalf(spread);
};
