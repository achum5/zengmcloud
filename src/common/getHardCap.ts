// The hard cap is a second, higher salary ceiling that sits above the normal
// soft `salaryCap`, applied to specific teams (or all of them). It returns the
// ceiling for a team in the same units as salaryCap (thousands of dollars), or
// Infinity when the team isn't bound or the feature is off.
//
// `hardCapTids` empty means "all teams" (whenever `hardCapAmount` is set), so a
// stored team list never goes stale when teams are added or removed.
export const hardCapForTid = (
	tid: number,
	hardCapAmount: number,
	hardCapTids: number[],
): number => {
	if (!hardCapAmount || hardCapAmount <= 0) {
		return Infinity;
	}
	if (hardCapTids.length === 0 || hardCapTids.includes(tid)) {
		return hardCapAmount;
	}
	return Infinity;
};
