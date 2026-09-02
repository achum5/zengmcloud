// GRUDGES, WITHOUT REMEMBERING ANYTHING.
//
// A feed where accounts carry history is far better than one where every night
// starts from nothing, and history is normally a thing you store. This one
// cannot: the whole feed is derived, so there is no place to write down that
// these two have been at each other since November.
//
// The way out is that a league ALREADY RECORDS ITS OWN GRUDGES. Two teams that
// have played four times, split the season series, and swapped a player at the
// deadline have a documented history, and it is sitting in the schedule and
// the transaction log. So friction is recomputed from that rather than
// remembered, which has the pleasant side effect of making it true: the feud
// gets hotter over a season because the teams actually keep meeting.
//
// The one relationship the league does not record is the one inside a
// fanbase. A homer and a doomer supporting the SAME team is the most reliable
// argument in any comment section, and it needs no history at all - the two
// accounts disagree by construction. That gets its own branch.

export type RivalryContext = {
	// Times these two teams have met this season, so far.
	meetings: number;
	// How many of those the first account's team won. Used for how CONTESTED
	// the series is, not who is winning it: a 3-3 season series is a rivalry
	// and a 6-0 one is a chore.
	firstWins: number;
	// Either account names the other's team as a rival.
	declared: boolean;
	// A player changed hands between these two teams this season. Nothing
	// generates bad blood in a fanbase faster.
	swapped: boolean;
};

const clamp01 = (n: number) => Math.max(0, Math.min(1, n));

export const NO_RIVALRY: RivalryContext = {
	meetings: 0,
	firstWins: 0,
	declared: false,
	swapped: false,
};

// How much history two accounts have, 0 to 1. Pure, and symmetric in the sense
// that matters: swapping the two accounts must give the same heat, because a
// feud that only one side feels is not a feud.
export const feudHeat = ({
	firstTid,
	secondTid,
	firstOptimism,
	secondOptimism,
	rivalry,
}: {
	// The teams the two accounts follow, if any.
	firstTid: number | undefined;
	secondTid: number | undefined;
	// Their outlooks, which is what separates two fans of the same team.
	firstOptimism: number;
	secondOptimism: number;
	rivalry: RivalryContext;
}): number => {
	// Neither has a side to defend. Nothing here is a rivalry; whatever
	// friction they have comes from temperament, which replyAppetite already
	// handles on its own.
	if (firstTid === undefined || secondTid === undefined) {
		return 0;
	}

	// THE CIVIL WAR. Same colours, opposite wiring. No shared history is
	// needed, because the disagreement is about the same evidence.
	if (firstTid === secondTid) {
		const split = Math.abs(firstOptimism - secondOptimism) / 2;
		// A gentle floor so two like-minded fans of one team are not enemies,
		// and a ceiling short of 1 so a genuine rivalry always outranks it.
		return split < 0.4 ? 0 : clamp01(0.25 + split * 0.5);
	}

	let heat = 0;

	if (rivalry.declared) {
		heat += 0.55;
	}

	// Familiarity. Every meeting adds, with diminishing returns, because the
	// fourth game against the same team is what turns an opponent into a
	// nemesis and the tenth adds nothing new.
	heat += clamp01(rivalry.meetings / 4) * 0.3;

	// How contested it is. Measured as closeness to an even split, so a
	// back-and-forth series burns and a sweep does not.
	if (rivalry.meetings >= 2) {
		const share = rivalry.firstWins / rivalry.meetings;
		const contested = 1 - Math.abs(share - 0.5) * 2;
		heat += contested * 0.2;
	}

	if (rivalry.swapped) {
		heat += 0.25;
	}

	return clamp01(heat);
};

// Build the context for two teams out of the season's games and moves. Kept
// here rather than in the worker so the rule is testable without a database,
// and so both sides of a pair are guaranteed to be counted the same way.
export const rivalryFrom = ({
	firstTid,
	secondTid,
	games,
	swappedPairs,
	declaredRivals,
}: {
	firstTid: number;
	secondTid: number;
	// Every game played this season, as team pairs plus who won.
	games: readonly { tids: readonly number[]; winnerTid: number }[];
	// Team pairs that have exchanged a player this season, in either order.
	swappedPairs: readonly (readonly [number, number])[];
	// Teams the first account has declared as rivals.
	declaredRivals: readonly number[];
}): RivalryContext => {
	let meetings = 0;
	let firstWins = 0;
	for (const game of games) {
		if (game.tids.includes(firstTid) && game.tids.includes(secondTid)) {
			meetings += 1;
			if (game.winnerTid === firstTid) {
				firstWins += 1;
			}
		}
	}
	const swapped = swappedPairs.some(
		([a, b]) =>
			(a === firstTid && b === secondTid) ||
			(a === secondTid && b === firstTid),
	);
	return {
		meetings,
		firstWins,
		declared: declaredRivals.includes(secondTid),
		swapped,
	};
};
