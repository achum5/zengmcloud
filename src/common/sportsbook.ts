import type { SportsbookMarket } from "./types.ts";

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

// Decimal multiplier → American odds, rounded book-like to the nearest 5. Used
// to show a parlay's combined price (the product of its legs' decimal odds).
export const decimalToAmerican = (decimal: number): number => {
	if (decimal <= 1) {
		return 0;
	}
	const american = decimal >= 2 ? (decimal - 1) * 100 : -100 / (decimal - 1);
	const rounded = Math.round(american / 5) * 5;
	if (rounded > -100 && rounded < 100) {
		return american <= 0 ? -100 : 100;
	}
	return rounded;
};

// A parlay's combined decimal multiplier is the product of its legs'. All legs
// must win; the payout compounds.
export const combinedDecimalOdds = (americanOddsList: number[]): number =>
	americanOddsList.reduce((d, a) => d * americanToDecimal(a), 1);

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

// The game a market is about, or undefined for a futures/award market (which
// has no single game and so no box score to link to).
export const marketGid = (market: SportsbookMarket): number | undefined => {
	switch (market.type) {
		case "gameMoneyline":
		case "gameSpread":
		case "gameTotal":
		case "playerProp":
		case "playerMilestone":
		case "teamGameProp":
		case "gameProp":
			return market.gid;
		default:
			return undefined;
	}
};

// Which team a market picks to win the game, if it's a game-outcome market.
// Used to catch a parlay that bets on both teams of the same game.
const marketGamePickTid = (market: SportsbookMarket): number | undefined =>
	market.type === "gameMoneyline" || market.type === "gameSpread"
		? market.pickTid
		: undefined;

// Reject parlays whose legs contradict each other (you'd be guaranteed to lose
// at least one leg no matter what happens, so it's never a real parlay). Returns
// a user-facing reason for the first conflict found, or undefined if the legs
// can all win together. Correlated same-game legs that CAN all hit (a player
// over + his team's total over, say) are allowed - only genuine contradictions
// are blocked.
export const parlayConflict = (
	markets: SportsbookMarket[],
	opts?: {
		// How many players make the All-Star team this season (both rosters), so
		// parlaying more than that many "makes the All-Star team" legs is caught as
		// impossible. Omitted (e.g. on a page with no futures) = don't cap it here.
		allStarRosterSize?: number;
	},
): string | undefined => {
	// Exact duplicate legs.
	const seen = new Set<string>();
	for (const m of markets) {
		const k = JSON.stringify(m);
		if (seen.has(k)) {
			return "You can't put the same pick in a parlay twice.";
		}
		seen.add(k);
	}

	// --- Futures / awards: mutually-exclusive outcomes ---------------------
	// Single-winner markets: only one team/player can actually win, so two
	// different picks for the same title can never both hit.
	const countByKey = (keyOf: (m: SportsbookMarket) => string | undefined) => {
		const counts = new Map<string, number>();
		for (const m of markets) {
			const k = keyOf(m);
			if (k !== undefined) {
				counts.set(k, (counts.get(k) ?? 0) + 1);
			}
		}
		return counts;
	};

	const anyOver = (
		counts: Map<string, number>,
		cap: number,
		message: string,
	) => {
		for (const n of counts.values()) {
			if (n > cap) {
				return message;
			}
		}
		return undefined;
	};

	// Exactly one champion / conference winner / division winner / award winner.
	const single: [Map<string, number>, string][] = [
		[
			countByKey((m) => (m.type === "champion" ? `${m.season}` : undefined)),
			"A parlay can't back two different teams to win the title.",
		],
		[
			countByKey((m) =>
				m.type === "conf" ? `${m.season}:${m.cid}` : undefined,
			),
			"A parlay can't back two different teams to win the same conference.",
		],
		[
			countByKey((m) => (m.type === "div" ? `${m.season}:${m.did}` : undefined)),
			"A parlay can't back two different teams to win the same division.",
		],
		[
			countByKey((m) =>
				m.type === "award" ? `${m.season}:${m.award}` : undefined,
			),
			"A parlay can't back two different players to win the same award.",
		],
	];
	for (const [counts, message] of single) {
		const conflict = anyOver(counts, 1, message);
		if (conflict) {
			return conflict;
		}
	}

	// Over and under of the same team's win total.
	const winTotalSides = new Map<string, Set<string>>();
	for (const m of markets) {
		if (m.type === "winTotal") {
			const k = `${m.season}:${m.pickTid}`;
			const set = winTotalSides.get(k) ?? new Set();
			set.add(m.side);
			winTotalSides.set(k, set);
		}
	}
	for (const set of winTotalSides.values()) {
		if (set.size > 1) {
			return "A parlay can't take over and under on the same team's win total.";
		}
	}

	// Award TEAMS have a fixed number of spots, so you can't parlay more players
	// making one than actually fit (e.g. 6 different players all making the
	// 5-man All-League First Team is impossible).
	const teamCap = (
		m: SportsbookMarket,
	): { key: string; cap: number } | undefined => {
		if (m.type === "allLeagueTeam") {
			return { key: `all-league:${m.season}:${m.tier}`, cap: 5 };
		}
		if (m.type === "allDefensiveTeam") {
			return { key: `all-defensive:${m.season}:${m.tier}`, cap: 5 };
		}
		if (m.type === "allRookieTeam") {
			return { key: `all-rookie:${m.season}`, cap: 5 };
		}
		if (m.type === "allStarTeam") {
			return {
				key: `all-star:${m.season}`,
				cap: opts?.allStarRosterSize ?? Number.POSITIVE_INFINITY,
			};
		}
		return undefined;
	};
	const teamCounts = new Map<string, { n: number; cap: number }>();
	for (const m of markets) {
		const tc = teamCap(m);
		if (tc) {
			const cur = teamCounts.get(tc.key) ?? { n: 0, cap: tc.cap };
			cur.n += 1;
			teamCounts.set(tc.key, cur);
		}
	}
	for (const { n, cap } of teamCounts.values()) {
		if (n > cap) {
			return `A parlay can't have more than ${cap} players making the same team — only ${cap} spots exist.`;
		}
	}

	// Everything else only conflicts within a single game.
	const byGid = new Map<number, SportsbookMarket[]>();
	for (const m of markets) {
		const gid = marketGid(m);
		if (gid === undefined) {
			continue;
		}
		const arr = byGid.get(gid);
		if (arr) {
			arr.push(m);
		} else {
			byGid.set(gid, [m]);
		}
	}

	for (const legs of byGid.values()) {
		// Betting the game outcome for two different teams (e.g. underdog
		// moneyline + the other team's spread) can never both hit.
		const pickTids = new Set<number>();
		for (const m of legs) {
			const tid = marketGamePickTid(m);
			if (tid !== undefined) {
				pickTids.add(tid);
			}
		}
		if (pickTids.size > 1) {
			return "A parlay can't bet on both teams to win the same game.";
		}

		// Over and under of the same game total.
		const totalSides = new Set(
			legs
				.filter((m) => m.type === "gameTotal")
				.map((m) => (m as { side: string }).side),
		);
		if (totalSides.size > 1) {
			return "A parlay can't take both over and under on the same total.";
		}

		// Over and under of the same player prop (same player + stat).
		const playerSides = new Map<string, Set<string>>();
		for (const m of legs) {
			if (m.type === "playerProp") {
				const k = `${m.pid}:${m.stat}`;
				const set = playerSides.get(k) ?? new Set();
				set.add(m.side);
				playerSides.set(k, set);
			}
		}
		for (const set of playerSides.values()) {
			if (set.size > 1) {
				return "A parlay can't take both over and under on the same player prop.";
			}
		}

		// Over and under of the same team prop (same team + stat).
		const teamSides = new Map<string, Set<string>>();
		for (const m of legs) {
			if (m.type === "teamGameProp") {
				const k = `${m.tid}:${m.stat}`;
				const set = teamSides.get(k) ?? new Set();
				set.add(m.side);
				teamSides.set(k, set);
			}
		}
		for (const set of teamSides.values()) {
			if (set.size > 1) {
				return "A parlay can't take both over and under on the same team prop.";
			}
		}
	}

	return undefined;
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
