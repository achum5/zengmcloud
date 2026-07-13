// Shared play-money sportsbook helpers, used by the worker odds engine and the
// UI. This is a purely-for-fun side feature with its own virtual "$" currency,
// completely separate from the real game economy.

// Virtual $ granted to every team each preseason (rolls over year to year).
export const SPORTSBOOK_PRESEASON_GRANT = 1_000_000;

// House edge baked into displayed odds, so a market's implied probabilities sum
// to a bit over 100% like a real book (the "vig"/"juice"). Applied by inflating
// each outcome's probability before converting to a price.
export const SPORTSBOOK_VIG = 0.045;

// Clamp a probability away from 0/1 so odds stay finite. The floor is deep
// (0.2%) so genuine long shots differentiate (+2000 vs +20000) instead of
// every non-favorite pancaking into one max price.
const clampProb = (p: number): number => Math.min(0.99, Math.max(0.002, p));

// Convert a true win probability to American odds, with the house vig applied.
// Favorites come out negative (e.g. -150), underdogs positive (e.g. +130).
export const probToAmerican = (probTrue: number): number => {
	const p = clampProb(probTrue * (1 + SPORTSBOOK_VIG));
	const pClamped = Math.min(0.99, p);
	const american =
		pClamped >= 0.5
			? -(pClamped / (1 - pClamped)) * 100
			: ((1 - pClamped) / pClamped) * 100;
	// Round to the nearest 5 for a clean, book-like look.
	const rounded = Math.round(american / 5) * 5;
	// Keep favorites <= -100 and underdogs >= +100 after rounding.
	if (rounded > -100 && rounded < 100) {
		return american <= 0 ? -100 : 100;
	}
	return rounded;
};

// American odds → decimal multiplier (total return per $1 staked, incl. stake).
export const americanToDecimal = (american: number): number => {
	if (american === 0) {
		return 1;
	}
	return american > 0 ? american / 100 + 1 : 100 / -american + 1;
};

// American odds → the true(ish) implied probability, i.e. WITHOUT stripping the
// vig back out. Handy for display ("implied 62%").
export const americanToImpliedProb = (american: number): number => {
	if (american === 0) {
		return 1;
	}
	return american > 0 ? 100 / (american + 100) : -american / (-american + 100);
};

// Format American odds for display: "+130", "-150", "EVEN".
export const formatAmerican = (american: number): string => {
	if (american === 0) {
		return "EVEN";
	}
	return american > 0 ? `+${american}` : `${american}`;
};

// Full virtual-$ amount with separators: 1500000 → "$1,500,000".
export const formatSportsbookMoneyFull = (amount: number): string =>
	`${amount < 0 ? "-" : ""}$${Math.round(Math.abs(amount)).toLocaleString()}`;

// Format a virtual-$ amount compactly: 1500000 → "$1.5M", 12500 → "$12.5K".
export const formatSportsbookMoney = (amount: number): string => {
	const sign = amount < 0 ? "-" : "";
	const abs = Math.abs(amount);
	if (abs >= 1_000_000) {
		return `${sign}$${(abs / 1_000_000).toLocaleString(undefined, { maximumFractionDigits: 2 })}M`;
	}
	if (abs >= 1_000) {
		return `${sign}$${(abs / 1_000).toLocaleString(undefined, { maximumFractionDigits: 1 })}K`;
	}
	return `${sign}$${Math.round(abs).toLocaleString()}`;
};
