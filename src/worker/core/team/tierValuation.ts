import type { TradeTier } from "../trade/tradePosture.ts";

// ---------------------------------------------------------------------------
// WHAT A TRADE IS WORTH TO THIS TEAM
//
// The AI priced every offer off BBGM's own `strategy` flag, which has two
// values and is refreshed once a year in the offseason. So:
//
//   - A TEAM COULD PLAN ONE THING AND PRICE ANOTHER. Free agency, re-signing,
//     the draft and the AI's own trade proposals all run off the five-tier
//     posture. Valuation did not, so a team midway through a teardown would
//     still price your offer as a contender - refusing the picks it was
//     supposed to be collecting, and handing back the veterans it was supposed
//     to be shopping.
//   - THE FLAG LAGS BY UP TO A YEAR. A team that collapses in January still
//     reads "contending" until the following June, which is the entire stretch
//     during which anyone actually trades with it.
//   - TWO VALUES CANNOT SAY FIVE THINGS. A fringe team and a title favourite
//     both read "contending" and discounted a 19-year-old identically. They
//     want completely different things.
//
// So the multipliers are a function of the tier. The middle of the range is
// deliberately calibrated to reproduce the old numbers: `seller` is the old
// "rebuilding" and `buyer` is the old "contending", to the decimal. What is new
// is the two ends - a teardown that wants youth and picks MORE than the old
// rebuilding flag ever did, and an all-in team that wants them less - and
// `fringe`, which is the honest answer for a .500 team that the old flag had no
// way to express and had to round to one extreme or the other.
//
// Pure - no database - so the whole table is unit-testable.
// ---------------------------------------------------------------------------

// How much a future draft pick is worth, relative to face value.
export const PICK_MULTIPLIER: Record<TradeTier, number> = {
	teardown: 1.2,
	seller: 1.1, // the old "rebuilding"
	fringe: 1,
	buyer: 0.825, // the old "contending"
	allIn: 0.7,
};

// How much a player of a given age is worth, relative to face value. An age with
// no entry is worth face value: the adjustment exists for the young and the old,
// not for a player in his prime.
const AGE_MULTIPLIERS: Record<TradeTier, Partial<Record<number, number>>> = {
	// Everything is about the future, so youth is worth a real premium and
	// anyone in his prime is a trade chip rather than a player.
	teardown: {
		19: 1.15,
		20: 1.1,
		21: 1.075,
		22: 1.05,
		23: 1.025,
		26: 0.95,
		27: 0.9,
		28: 0.85,
		29: 0.8,
	},
	// The old "rebuilding" numbers, unchanged.
	seller: {
		19: 1.075,
		20: 1.05,
		21: 1.0375,
		22: 1.025,
		23: 1.0125,
		27: 0.975,
		28: 0.95,
		29: 0.9,
	},
	// A .500 team is not choosing yet, which is the whole point of the tier.
	fringe: {},
	// The old "contending" numbers, unchanged.
	buyer: {
		19: 0.8,
		20: 0.825,
		21: 0.85,
		22: 0.875,
		23: 0.925,
		24: 0.95,
	},
	// A player who cannot help this season is nearly worthless to this team,
	// which is exactly why it is the one that trades its young players away.
	allIn: {
		19: 0.7,
		20: 0.725,
		21: 0.75,
		22: 0.8,
		23: 0.875,
		24: 0.925,
	},
};

// WHICH DIRECTION EACH TABLE EXTENDS, and it is not the same one.
//
// A selling team's table is about AGE: its last entry is a penalty for being
// old, so it has to keep applying past the last row - a 35-year-old is at least
// as unwanted as a 29-year-old. The old "rebuilding" branch ended in
// `age >= 29` for exactly this reason.
//
// A buying team's table is about YOUTH: its last entry is where the discount on
// an unfinished player runs out, and everyone past it is a prime player worth
// face value. The old "contending" branch ended in an exact `age === 24`, and
// extending it would have a contender quietly discounting a 33-year-old, which
// is the opposite of what that team wants.
const OLDEST_AGE_EXTENDS: Record<TradeTier, boolean> = {
	teardown: true,
	seller: true,
	fringe: false,
	buyer: false,
	allIn: false,
};

// The young end always extends: a 17-year-old import is at least as much of a
// project as a 19-year-old, which is what the old `age <= 19` did.
export const ageMultiplier = (tier: TradeTier, age: number): number => {
	const table = AGE_MULTIPLIERS[tier];
	const ages = Object.keys(table).map(Number);
	if (ages.length === 0 || !Number.isFinite(age)) {
		return 1;
	}
	const rounded = Math.round(age);
	const youngest = Math.min(...ages);
	const oldest = Math.max(...ages);
	if (rounded <= youngest) {
		return table[youngest] ?? 1;
	}
	if (rounded > oldest) {
		return OLDEST_AGE_EXTENDS[tier] ? (table[oldest] ?? 1) : 1;
	}
	return table[rounded] ?? 1;
};

// How heavily a contract's own value counts. A team with no plans for this
// season cares a great deal about what it is committed to; a team going all-in
// has already decided it does not.
export const CONTRACT_FACTOR: Record<TradeTier, number> = {
	teardown: 2.5,
	seller: 2, // the old "rebuilding"
	fringe: 1.25,
	buyer: 0.5, // the old "contending"
	allIn: 0.35,
};

// The tier a league without the smart front office behaves as, so turning the
// setting off gives back exactly the numbers BBGM always used.
export const tierForLegacyStrategy = (strategy: string): TradeTier =>
	strategy === "rebuilding" ? "seller" : "buyer";
