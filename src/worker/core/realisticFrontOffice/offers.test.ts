import { assert, beforeEach, test } from "vitest";
import { PHASE } from "../../../common/constants.ts";
import { resetG } from "../../../test/helpers.ts";
import { g } from "../../util/index.ts";
import {
	askFor,
	choiceOdds,
	offerSettings,
	offerUtility,
	rolePoints,
	wantedYears,
} from "./offers.ts";

beforeEach(() => {
	resetG();
	g.setWithoutSavingToDB("phase", PHASE.FREE_AGENCY);
	g.setWithoutSavingToDB("season", 2030);
	g.setWithoutSavingToDB("playerMoodTraits", true);
});

const wantsThreeYears = {
	contract: { amount: 10000, exp: 2033 },
	ratings: [{ ovr: 60 }],
} as any;

test("the odds add up to 1, and more money is more likely to be chosen", () => {
	const ask = 10000;
	const odds = choiceOdds(
		[
			offerUtility({ amount: 11000, ask, appeal: 0 }),
			offerUtility({ amount: 10000, ask, appeal: 0 }),
		],
		0,
	);
	assert.closeTo(
		odds.reduce((a, x) => a + x, 0),
		1,
		1e-12,
	);
	assert.isAbove(odds[0]!, odds[1]!);
	// His ask from a team he is neutral on is worth the same as waiting.
	assert.closeTo(odds[1]!, odds[2]!, 1e-12);
});

test("an awful team with a low offer has next to no chance", () => {
	const ask = 10000;
	const [awful] = choiceOdds(
		[
			offerUtility({ amount: 8000, ask, appeal: -8 }),
			offerUtility({ amount: 10000, ask, appeal: 3 }),
		],
		0,
	);
	assert.isBelow(awful!, 0.002);
});

test("a point of mood is worth about what the multiplayer board gave it", () => {
	const [liked, neutral] = choiceOdds([
		offerUtility({ amount: 10000, ask: 10000, appeal: 1 }),
		offerUtility({ amount: 10000, ask: 10000, appeal: 0 }),
	]);
	const ratio = liked! / neutral!;
	assert.isAbove(ratio, 1.3);
	assert.isBelow(ratio, 1.7);
	assert.strictEqual(offerSettings.temperature > 0, true);
});

test("he asks most for the length he wants least", () => {
	assert.strictEqual(wantedYears(wantsThreeYears), 3);
	assert.strictEqual(askFor(wantsThreeYears, 3), 10000);
	assert.isAbove(askFor(wantsThreeYears, 1), askFor(wantsThreeYears, 2));
	assert.closeTo(askFor(wantsThreeYears, 2), askFor(wantsThreeYears, 4), 1e-9);
});

test("a starter's place pleases him and the end of the bench does not", () => {
	const p = { valueNoPot: 60, moodTraits: [] } as any;
	const worse = Array.from({ length: 12 }, () => ({ valueNoPot: 40 }));
	const better = Array.from({ length: 12 }, () => ({ valueNoPot: 70 }));
	// Usually the third man on a team.
	assert.isAbove(rolePoints(p, worse, 2), 0);
	assert.isBelow(rolePoints(p, better, 2), 0);
	// Fame doubles down on it, winning cares less.
	const fame = rolePoints({ ...p, moodTraits: ["F"] }, better, 2);
	const winning = rolePoints({ ...p, moodTraits: ["W"] }, better, 2);
	assert.isBelow(fame, winning);
});
