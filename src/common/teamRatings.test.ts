import { assert, describe, test } from "vitest";
import { hideTeamOvr, showTeamOvr } from "./teamRatings.ts";

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
