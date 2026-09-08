import { helpers } from "./helpers.ts";
import { bySport } from "./sportFunctions.ts";
import type {
	Award,
	AwardInfoIndividual,
	AwardInfoTeam,
	PlayerAwardBuiltIn,
} from "./types.ts";

// HOW DEEP AN INDIVIDUAL AWARD'S BALLOT GOES.
//
// One number, because it governs two things that have to agree: how many
// players the Award Races page ranks, and how many of them are written down
// when the award is decided. When they disagreed, the page would tell you a
// player finished sixth in MVP voting and his own page could never say so -
// the ballot stopped at five, so there was no sixth place to show.
export const NUM_PLAYERS_PER_INDIVIDUAL_AWARD = 10;

// WHAT AN AWARD IS CALLED, ACCORDING TO THE SETTINGS, RIGHT NOW.
//
// A season writes down the label its awards were given at the time, and a
// player writes down his own copy of it. Renaming an award rewrites both (see
// core/awards/renameAwards), but a copy can always be left behind: a sweep
// interrupted, a season synced from a device that had already relabeled its
// own, a league renamed before any of that existed. One stale copy is enough
// for a player's page to keep saying All-League forever, because his page
// reads his copy.
//
// So the label is also resolved as it is read. The abbrev is what identifies
// an award - the settings enforce that it is unique - so an award still
// answering to an abbrev the settings use takes whatever the settings call it
// now. An abbrev the settings no longer use belongs to an award that was
// deleted or renamed abbrev and all; its history is not ours to rewrite, and
// it is left exactly as stored.
//
// Nothing is mutated: an award that already agrees comes back as it was.
export const relabelAwardsFromSettings = <
	T extends {
		type?: string;
		name?: string;
		shortName?: string;
		numTeams?: number;
	},
>(
	awards: readonly T[] | undefined,
	settings:
		| readonly { name: string; shortName: string; numTeams?: number }[]
		| undefined,
): T[] | undefined => {
	if (!awards || !settings || settings.length === 0) {
		return awards as T[] | undefined;
	}

	const byShortName = new Map(
		settings.map((award) => [award.shortName, award]),
	);

	let changed = false;
	const out = awards.map((award) => {
		// A legacy award is just a string; it answers to no abbrev.
		if (award.type !== undefined || award.shortName === undefined) {
			return award;
		}

		const setting = byShortName.get(award.shortName);
		if (
			!setting ||
			// A team award and an individual one are different things under the
			// same abbrev.
			(setting.numTeams !== undefined) !== (award.numTeams !== undefined) ||
			setting.name === award.name
		) {
			return award;
		}

		changed = true;
		return { ...award, name: setting.name };
	});

	return changed ? out : (awards as T[]);
};

export const formatTeamNumber = (rank: number) =>
	`${helpers.ordinal(rank)} Team`;

export const formatPlayerAwardName = (
	// This is like PlayerAward but with only the required field specified so it can be used elsewhere easily
	award:
		| {
				type: string;
		  }
		| Pick<PlayerAwardBuiltIn, "name" | "numTeams" | "rank" | "type">,
	{
		groupPrefix,
		hideTeamName,
	}: {
		groupPrefix?: string; // Like for conf awards, prefix with conf abbrev
		hideTeamName?: boolean;
	} = {},
) => {
	if (award.type === undefined) {
		const prefixWithSpace = groupPrefix !== undefined ? `${groupPrefix} ` : "";
		if (award.numTeams === undefined) {
			return `${prefixWithSpace}${award.name}`;
		}

		if (award.numTeams === 1) {
			if (hideTeamName && groupPrefix !== undefined) {
				return groupPrefix;
			}
			return `${prefixWithSpace}${award.name} Team`;
		}

		const prefixAndRank = `${prefixWithSpace}${formatTeamNumber(award.rank)}`;
		if (hideTeamName) {
			return prefixAndRank;
		}

		return `${prefixAndRank} ${award.name}`;
	}

	// For either manually added team awards, or old ones in a league without corresponding awards objects (such as a real players league without all historical data)
	if (award.type.startsWith("First ")) {
		return `1st ${award.type.replace("First ", "")}`;
	}
	if (award.type.startsWith("Second ")) {
		return `2nd ${award.type.replace("Second ", "")}`;
	}
	if (award.type.startsWith("Third ")) {
		return `3rd ${award.type.replace("Third ", "")}`;
	}

	return award.type;
};

export const showStatsByType: Partial<Record<Award["showStats"], string[]>> =
	bySport({
		baseball: {
			// keyStats formats W-L and slash line nicely
			overall: ["keyStats"],
			sp: ["keyStats"],
			rp: ["sv", "era", "ip"],
			offense: ["keyStats"],
			defense: ["keyStats"], // Showing actualy defensive stats would be annoying because arrays
		},
		basketball: {
			offense: ["pts", "trb", "ast"],
			defense: ["trb", "blk", "stl"],
		},
		football: {
			overall: ["keyStats"],
			defense: ["keyStats"],
			blocking: ["keyStats"],
		},
		hockey: {
			overall: ["keyStats", "ps"],
			defense: ["tk", "hit", "dps"],
			goalkeeping: ["gpGoalie", "gaa", "svPct", "gps"],
		},
	});

export const leaderAwardCategories = bySport({
	baseball: [
		{
			name: "League HR Leader",
			stat: "hr",
		},
		{
			name: "League BA Leader",
			stat: "ba",
		},
		{
			name: "League OPS Leader",
			stat: "ops",
		},
		{
			name: "League RBI Leader",
			stat: "rbi",
		},
		{
			name: "League Runs Leader",
			stat: "r",
		},
		{
			name: "League Stolen Bases Leader",
			stat: "sb",
		},
		{
			name: "League Walks Leader",
			stat: "bb",
		},
		{
			name: "League Wins Leader",
			stat: "w",
		},
		{
			name: "League Strikeouts Leader",
			stat: "soPit",
		},
		{
			name: "League ERA Leader",
			stat: "era",
		},
		{
			name: "League Saves Leader",
			stat: "sv",
		},
		{
			name: "League WAR Leader",
			stat: "war",
		},
	],
	basketball: [
		{
			name: "League Scoring Leader",
			stat: "pts",
		},
		{
			name: "League Rebounding Leader",
			stat: "trb",
		},
		{
			name: "League Assists Leader",
			stat: "ast",
		},
		{
			name: "League Steals Leader",
			stat: "stl",
		},
		{
			name: "League Blocks Leader",
			stat: "blk",
		},
	],
	football: [
		{
			name: "League Passing Leader",
			stat: "pssYds",
		},
		{
			name: "League Rushing Leader",
			stat: "rusYds",
		},
		{
			name: "League Receiving Leader",
			stat: "recYds",
		},
		{
			name: "League Scrimmage Yards Leader",
			stat: "ydsFromScrimmage",
		},
		{
			name: "League Interceptions Leader",
			stat: "defInt",
		},
		{
			name: "League Sacks Leader",
			stat: "defSk",
		},
		{
			name: "League TD Leader",
			stat: "totTD",
		},
	],
	hockey: [
		{
			name: "League Points Leader",
			stat: "pts",
		},
		{
			name: "League Goals Leader",
			stat: "g",
		},
		{
			name: "League Assists Leader",
			stat: "a",
		},
	],
});

export const pruneEmptyWinners = (
	awards: (AwardInfoIndividual | AwardInfoTeam)[],
) => {
	return awards.map((award) => {
		if (award.numTeams === undefined) {
			const winner = [...award.winner];

			while (winner.length > 0 && winner.at(-1)?.pid === undefined) {
				winner.pop();
			}

			return {
				...award,
				winner,
			};
		} else {
			const winner = award.winner.map((teamTemp) => {
				const team = [...teamTemp];
				while (team.length > 0 && team.at(-1)?.pid === undefined) {
					team.pop();
				}
				return team;
			});

			return {
				...award,
				winner,
			};
		}
	});
};
