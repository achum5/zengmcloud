import { assert, describe, test } from "vitest";
import {
	delayedTeamOvrNote,
	hideTeamOvr,
	powerRankingIsJustTeamOvr,
	teamOvrDeltaBandLabel,
	teamOvrDeltaSymbols,
	showTeamOvr,
	teamOvrDisplay,
	teamOvrDisplayForSeason,
	teamOvrVisibleForSeason,
} from "./teamRatings.ts";

// Two settings hide a team's overall, and honouring only one of them is the
// actual bug this replaced: Frivolities > Team Seasons checked
// challengeNoRatings alone and kept printing team overalls in a league running
// "No Visible Team Ratings". Every screen that shows a team overall now asks
// this, so half-honouring it isn't expressible.
describe("when a team overall is hidden", () => {
	const cases = [
		{ challengeNoRatings: false, hideTeamRatings: false, hidden: false },
		// The one that was getting through.
		{ challengeNoRatings: false, hideTeamRatings: true, hidden: true },
		{ challengeNoRatings: true, hideTeamRatings: false, hidden: true },
		{ challengeNoRatings: true, hideTeamRatings: true, hidden: true },
	];

	for (const { hidden, ...settings } of cases) {
		test(`challengeNoRatings=${settings.challengeNoRatings} hideTeamRatings=${settings.hideTeamRatings}`, () => {
			assert.strictEqual(hideTeamOvr(settings), hidden);
			assert.strictEqual(showTeamOvr(settings), !hidden);
		});
	}
});

describe("teamOvrDisplay", () => {
	const base = {
		challengeNoRatings: false,
		hideTeamRatings: true,
		season: 2007,
	};

	test("a delay of 5 in 2007 shows 2002", () => {
		assert.deepStrictEqual(
			teamOvrDisplay({ ...base, teamRatingsDelaySeasons: 5 }),
			{ type: "delayed", season: 2002 },
		);
	});

	test("nothing hidden means nothing delayed - the delay only softens hiding", () => {
		assert.deepStrictEqual(
			teamOvrDisplay({
				challengeNoRatings: false,
				hideTeamRatings: false,
				teamRatingsDelaySeasons: 5,
				season: 2007,
			}),
			{ type: "current" },
		);
	});

	test("no delay set leaves the old hide-everything behaviour untouched", () => {
		assert.deepStrictEqual(teamOvrDisplay(base), { type: "hidden" });
		assert.deepStrictEqual(
			teamOvrDisplay({ ...base, teamRatingsDelaySeasons: 0 }),
			{ type: "hidden" },
		);
	});

	test("the delay also softens No Visible Player Ratings", () => {
		// An old team rating leaks nothing about today's roster, so there is no
		// reason for this setting to be the one that overrides it.
		assert.deepStrictEqual(
			teamOvrDisplay({
				challengeNoRatings: true,
				hideTeamRatings: false,
				teamRatingsDelaySeasons: 3,
				season: 2007,
			}),
			{ type: "delayed", season: 2004 },
		);
	});

	test("a delay of 1 is the shortest real delay - last season", () => {
		assert.deepStrictEqual(
			teamOvrDisplay({ ...base, teamRatingsDelaySeasons: 1 }),
			{ type: "delayed", season: 2006 },
		);
	});

	test("junk values hide rather than pointing at a season that cannot exist", () => {
		for (const teamRatingsDelaySeasons of [
			-1,
			0.4,
			Number.NaN,
			Number.POSITIVE_INFINITY,
		]) {
			assert.deepStrictEqual(
				teamOvrDisplay({ ...base, teamRatingsDelaySeasons }),
				{ type: "hidden" },
				`delay ${teamRatingsDelaySeasons}`,
			);
		}
	});

	test("a fractional delay floors rather than landing between seasons", () => {
		assert.deepStrictEqual(
			teamOvrDisplay({ ...base, teamRatingsDelaySeasons: 5.9 }),
			{ type: "delayed", season: 2002 },
		);
	});
});

describe("teamOvrDisplayForSeason", () => {
	const settings = {
		challengeNoRatings: false,
		hideTeamRatings: true,
		teamRatingsDelaySeasons: 5,
		season: 2007,
	};

	test("the current season's page falls back to the newest knowable rating", () => {
		assert.deepStrictEqual(teamOvrDisplayForSeason(settings, 2007), {
			type: "delayed",
			season: 2002,
		});
	});

	test("a season old enough to be unlocked shows its own rating outright", () => {
		assert.deepStrictEqual(teamOvrDisplayForSeason(settings, 2002), {
			type: "current",
		});
		assert.deepStrictEqual(teamOvrDisplayForSeason(settings, 1998), {
			type: "current",
		});
	});

	test("a season in between shows nothing rather than another season's number", () => {
		assert.deepStrictEqual(teamOvrDisplayForSeason(settings, 2003), {
			type: "hidden",
		});
		assert.deepStrictEqual(teamOvrDisplayForSeason(settings, 2006), {
			type: "hidden",
		});
	});

	test("with the delay off, every page behaves exactly as it did before", () => {
		const off = { ...settings, teamRatingsDelaySeasons: 0 };
		for (const pageSeason of [1998, 2003, 2007]) {
			assert.deepStrictEqual(teamOvrDisplayForSeason(off, pageSeason), {
				type: "hidden",
			});
		}
		const nothingHidden = { ...off, hideTeamRatings: false };
		for (const pageSeason of [1998, 2003, 2007]) {
			assert.deepStrictEqual(
				teamOvrDisplayForSeason(nothingHidden, pageSeason),
				{ type: "current" },
			);
		}
	});
});

describe("teamOvrVisibleForSeason", () => {
	const settings = {
		challengeNoRatings: false,
		hideTeamRatings: true,
		teamRatingsDelaySeasons: 5,
		season: 2007,
	};

	test("in 2007 with a delay of 5, history is open through 2002 and covered after", () => {
		assert.strictEqual(teamOvrVisibleForSeason(settings, 1999), true);
		assert.strictEqual(teamOvrVisibleForSeason(settings, 2002), true);
		assert.strictEqual(teamOvrVisibleForSeason(settings, 2003), false);
		assert.strictEqual(teamOvrVisibleForSeason(settings, 2007), false);
	});

	test("with nothing hidden every row shows", () => {
		assert.strictEqual(
			teamOvrVisibleForSeason({ ...settings, hideTeamRatings: false }, 2007),
			true,
		);
	});

	test("with no delay no row shows", () => {
		assert.strictEqual(
			teamOvrVisibleForSeason(
				{ ...settings, teamRatingsDelaySeasons: 0 },
				1999,
			),
			false,
		);
	});
});

describe("delayedTeamOvrNote", () => {
	test("names the season, because an unlabelled old number reads as the current one", () => {
		assert.strictEqual(delayedTeamOvrNote(2002), "2002 rating");
	});
});

// THE PRESEASON LEAK. A power ranking is performance plus margin of victory
// plus team rating, so before a game is played it is only the last of those -
// the hidden ratings, sorted and numbered. The Power Rankings page had always
// closed itself in that state, but the Draft Picks table printed the same rank
// in a column, so a 2011 preseason league with team ratings off could read the
// whole league's pecking order off it (reported from a screenshot: every
// original team at rank 5, ATL 24, DEN 28, CHI 18).
describe("powerRankingIsJustTeamOvr", () => {
	test("ratings hidden and no games played closes it", () => {
		assert.strictEqual(
			powerRankingIsJustTeamOvr({
				display: { type: "hidden" },
				noGamesYet: true,
			}),
			true,
		);
	});

	// Once there are results, the ranking is made of something other than the
	// ratings, and the Power Rankings page shows it - so withholding it here
	// would only hide what is one click away.
	test("once games have been played the ranking stands on its own", () => {
		assert.strictEqual(
			powerRankingIsJustTeamOvr({
				display: { type: "hidden" },
				noGamesYet: false,
			}),
			false,
		);
	});

	test("a league that shows ratings has nothing to protect", () => {
		assert.strictEqual(
			powerRankingIsJustTeamOvr({
				display: { type: "current" },
				noGamesYet: true,
			}),
			false,
		);
	});

	// The delay is an opt-in to scouting with old information, and the page that
	// serves it says outright that the rankings still use today's rosters.
	test("a delayed league keeps its ranking", () => {
		assert.strictEqual(
			powerRankingIsJustTeamOvr({
				display: { type: "delayed", season: 2006 },
				noGamesYet: true,
			}),
			false,
		);
	});
});

// THE MEASURING INSTRUMENT. With team ratings hidden the trade screens showed
// the exact change, which a league used as an oracle: offer one player, read
// the delta, solve for his rating. The numbers below are theirs - a guard worth
// +13, another +11, a third +5 - and the whole point of the bands is that the
// first two now read identically.
describe("teamOvrDeltaSymbols", () => {
	test("the reported exploit no longer separates two players in the same band", () => {
		assert.strictEqual(teamOvrDeltaSymbols(13), "+++");
		assert.strictEqual(teamOvrDeltaSymbols(11), "+++");
		assert.strictEqual(teamOvrDeltaSymbols(15), "+++");
		assert.strictEqual(teamOvrDeltaSymbols(5), "+");
	});

	test("every band boundary", () => {
		const cases: [number, string][] = [
			[1, "+"],
			[5, "+"],
			[6, "++"],
			[10, "++"],
			[11, "+++"],
			[15, "+++"],
			[16, "++++"],
			[20, "++++"],
			[21, "+++++"],
			[999, "+++++"],
		];
		for (const [diff, expected] of cases) {
			assert.strictEqual(teamOvrDeltaSymbols(diff), expected, `+${diff}`);
		}
	});

	test("negatives mirror exactly", () => {
		for (const diff of [1, 5, 6, 10, 11, 15, 16, 20, 21, 999]) {
			assert.strictEqual(
				teamOvrDeltaSymbols(-diff),
				teamOvrDeltaSymbols(diff).replaceAll("+", "-"),
				`-${diff}`,
			);
		}
	});

	test("no change reads as zero, not as an empty cell", () => {
		assert.strictEqual(teamOvrDeltaSymbols(0), "0");
		assert.strictEqual(teamOvrDeltaSymbols(-0), "0");
	});

	// team.ovr is rounded already, so this is about imported leagues and God
	// Mode. Rounding first is what stops a half point becoming a whole band.
	test("a fraction is rounded before it is banded", () => {
		assert.strictEqual(teamOvrDeltaSymbols(0.4), "0");
		assert.strictEqual(teamOvrDeltaSymbols(5.4), "+");
		assert.strictEqual(teamOvrDeltaSymbols(5.6), "++");
	});

	test("an unusable number claims nothing", () => {
		assert.strictEqual(teamOvrDeltaSymbols(Number.NaN), "0");
		assert.strictEqual(teamOvrDeltaSymbols(Number.POSITIVE_INFINITY), "0");
	});

	// The property that matters: within a band the symbols are identical, so no
	// sequence of one-player offers can price a player more finely than five.
	test("nothing inside a band is distinguishable", () => {
		for (let low = 1; low <= 16; low += 5) {
			const symbols = teamOvrDeltaSymbols(low);
			for (let diff = low; diff < low + 5; diff += 1) {
				assert.strictEqual(teamOvrDeltaSymbols(diff), symbols, `+${diff}`);
			}
		}
	});
});

describe("teamOvrDeltaBandLabel", () => {
	test("names the band the symbols stand for", () => {
		assert.strictEqual(teamOvrDeltaBandLabel(3), "+1 to +5");
		assert.strictEqual(teamOvrDeltaBandLabel(13), "+11 to +15");
		assert.strictEqual(teamOvrDeltaBandLabel(-8), "-6 to -10");
	});

	test("the top band is open-ended", () => {
		assert.strictEqual(teamOvrDeltaBandLabel(21), "+21 or more");
		assert.strictEqual(teamOvrDeltaBandLabel(-60), "-21 or more");
	});

	test("no change", () => {
		assert.strictEqual(teamOvrDeltaBandLabel(0), "No change");
	});
});
