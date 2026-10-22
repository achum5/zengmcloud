import { PHASE } from "../../common/constants.ts";
import { roundContract } from "../../common/roundContract.ts";
import type { GameAttributesLeague } from "../../common/types.ts";

// THE CAP MOVES, AND A FUTURE COLUMN HAS TO BE JUDGED AGAINST ITS OWN.
//
// In a real-players league a scheduled event steps the salary cap, the luxury
// tax line and everything derived from them up at the draft lottery of every
// season, so measuring a payroll five years out against today's numbers is
// wrong by tens of millions by the time you get there.
//
// This walks the pending events forward to give the caps each season will
// actually be played under.
export type SeasonCaps = {
	salaryCap: number;
	luxuryPayroll: number;
	minPayroll: number;
	minContract: number;
	maxContract: number;
	hardCapAmount: number;
	hardCapTids: number[];
	hardCapUseLuxuryTax: boolean;
};

export const capsForSeasons = ({
	seasons,
	current,
	events,
	season,
	phase,
}: {
	// The seasons to project, ascending.
	seasons: number[];
	// What the league is playing under right now.
	current: SeasonCaps;
	// Every gameAttributes scheduled event the league has - READ FROM THE
	// DATABASE, not from idb.cache. The cache only ever holds the CURRENT
	// season's events (Cache.ts loads them by an exact season index), so a walk
	// over it sees at most this season's own pending change and nothing beyond
	// it: every column from the second onward then inherits that one value.
	// That is the bug this function exists to fix - a 2014 league showed 76.85
	// for 2014 through 2018, when only 2015 was ever going to be 76.85.
	events: {
		season: number;
		phase: number;
		info: Partial<GameAttributesLeague>;
	}[];
	season: number;
	phase: number;
}): SeasonCaps[] => {
	// ONLY EVENTS THAT HAVE NOT FIRED YET. `current` already carries everything
	// in the past, and reading the database rather than the cache means a stale
	// event can turn up - one from a season the league was created past, which
	// nothing ever processed or deleted. Applying it would overwrite today's
	// cap with an older, smaller one.
	const pending = events
		.filter(
			(event) =>
				event.season > season ||
				(event.season === season && event.phase > phase),
		)
		.sort((a, b) => a.season - b.season || a.phase - b.phase);

	const running = { ...current };
	let i = 0;

	return seasons.map((yr) => {
		// A cap change fires at the START of its phase, so what a season is
		// played under is every event through that season's regular season. The
		// real-data events land at the draft lottery, which comes at the END of
		// a season - so the one dated 2014 governs 2015, and the cap for the
		// season about to be played is the one already in force today.
		while (
			i < pending.length &&
			(pending[i]!.season < yr ||
				(pending[i]!.season === yr &&
					pending[i]!.phase <= PHASE.REGULAR_SEASON))
		) {
			const { info } = pending[i]!;
			if (info.salaryCap !== undefined) {
				running.salaryCap = info.salaryCap;
			}
			if (info.luxuryPayroll !== undefined) {
				running.luxuryPayroll = info.luxuryPayroll;
			}
			if (info.minPayroll !== undefined) {
				running.minPayroll = info.minPayroll;
			}
			if (info.minContract !== undefined) {
				running.minContract = info.minContract;
			}
			if (info.maxContract !== undefined) {
				running.maxContract = info.maxContract;
			}
			if (info.hardCapAmount !== undefined) {
				running.hardCapAmount = info.hardCapAmount;
			}
			if (info.hardCapTids !== undefined) {
				running.hardCapTids = info.hardCapTids;
			}
			if (info.hardCapUseLuxuryTax !== undefined) {
				running.hardCapUseLuxuryTax = info.hardCapUseLuxuryTax;
			}
			i += 1;
		}

		return { ...running };
	});
};

// A salary priced in one season's money, in another's - a projected contract
// worked out against today's cap, for the season it would start in. A max deal
// is that season's max and a minimum deal its minimum; anything in between
// moves with the cap.
export const salaryInSeason = (
	amount: number,
	now: SeasonCaps,
	then: SeasonCaps,
): number => {
	if (amount >= now.maxContract) {
		return then.maxContract;
	}
	if (amount <= now.minContract) {
		return then.minContract;
	}
	const scaled = roundContract(
		(amount * then.salaryCap) / now.salaryCap,
		then.minContract,
	);
	return Math.min(then.maxContract, Math.max(then.minContract, scaled));
};
