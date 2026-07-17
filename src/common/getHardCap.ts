// The hard cap is a second, higher salary ceiling that sits above the normal
// soft `salaryCap`, applied to specific teams (or all of them). It returns the
// ceiling for a team in the same units as salaryCap (thousands of dollars), or
// Infinity when the team isn't bound or the feature is off.
//
// `hardCapTids` empty means "all teams" (whenever the cap is active), so a
// stored team list never goes stale when teams are added or removed. When
// `hardCapUseLuxuryTax` is set, the ceiling tracks the current luxury-tax line
// (`luxuryPayroll`) instead of the fixed `hardCapAmount`, so it auto-scales as
// the cap inflates season to season.
export const hardCapForTid = (
	tid: number,
	{
		hardCapAmount,
		hardCapTids,
		hardCapUseLuxuryTax = false,
		luxuryPayroll = 0,
	}: {
		hardCapAmount: number;
		hardCapTids: number[];
		hardCapUseLuxuryTax?: boolean;
		luxuryPayroll?: number;
	},
): number => {
	const amount = hardCapUseLuxuryTax ? luxuryPayroll : hardCapAmount;
	if (!amount || amount <= 0) {
		return Infinity;
	}
	if (hardCapTids.length === 0 || hardCapTids.includes(tid)) {
		return amount;
	}
	return Infinity;
};
