import { helpers } from "./helpers.ts";

// One Contract Value cell, shared by the three tables that show it.
//
// Signed and coloured because the sign IS the reading: the number is what the
// team saved (or wasted) against what the production was worth, so "+$8.1M" and
// "-$8.1M" are opposite verdicts on an identically sized contract, and a bare
// "8.1" would hide which one you were looking at.
export const contractValueCell = (surplus: number | undefined) => {
	if (surplus === undefined) {
		return null;
	}

	// Rounded before comparing, so a contract that displays as $0.0M isn't
	// coloured as though it were a bargain.
	const rounded = Math.round(surplus * 10) / 10;
	const formatted = `${rounded > 0 ? "+" : ""}${helpers.formatCurrency(rounded, "M", 1)}`;

	return {
		value: (
			<span
				className={
					rounded > 0 ? "text-success" : rounded < 0 ? "text-danger" : undefined
				}
			>
				{formatted}
			</span>
		),
		// The raw number, so sorting is by actual value rather than by the
		// formatted string (where "-$9.0M" would outrank "+$10.0M").
		sortValue: surplus,
		searchValue: formatted,
	};
};
