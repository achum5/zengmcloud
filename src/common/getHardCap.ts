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

// Is a stored `hardCapAmount` a problem worth refusing a save over?
//
// Only when something actually READS it. hardCapForTid above ignores the
// amount entirely while `hardCapUseLuxuryTax` is on - the ceiling is
// luxuryPayroll then - so validating it in that state rejects a save because
// of a number nothing uses. That is not hypothetical: a league running
// "hard cap = luxury tax line" with a leftover amount below the salary cap
// could not save ANY setting, on any page, because the settings form runs
// every validator on every save. The error named a field the user had not
// touched and that had no effect on their league.
//
// Returns the message to show, or undefined when the value is fine.
export const hardCapAmountProblem = ({
	hardCapAmount,
	salaryCap,
	hardCapUseLuxuryTax = false,
}: {
	hardCapAmount: number;
	salaryCap: number | undefined;
	hardCapUseLuxuryTax?: boolean;
}): string | undefined => {
	if (hardCapAmount < 0) {
		return "Must be 0 (off) or a positive number";
	}
	if (hardCapUseLuxuryTax) {
		// Inert: the luxury tax line is the ceiling, whatever this says.
		return undefined;
	}
	if (
		hardCapAmount > 0 &&
		typeof salaryCap === "number" &&
		hardCapAmount < salaryCap
	) {
		return "Hard cap must be at least the salary cap";
	}
	return undefined;
};
