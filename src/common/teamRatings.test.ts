import { assert, describe, test } from "vitest";
import {
	delayedTeamOvrNote,
	hideTeamOvr,
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
