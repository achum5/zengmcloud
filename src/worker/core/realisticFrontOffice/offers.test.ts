import { assert, beforeEach, test } from "vitest";
import { PHASE } from "../../../common/constants.ts";
import { resetG } from "../../../test/helpers.ts";
import { g } from "../../util/index.ts";
import {
	askFor,
	choiceOdds,
	effectiveAmount,
	moneyUtility,
	offerSettings,
	offerUtility,
	rolePoints,
	waitUtility,
	wantedYears,
} from "./offers.ts";
import { atCamp, stageSettings, stageWait } from "./stages.ts";

beforeEach(() => {
	resetG();
	g.setWithoutSavingToDB("phase", PHASE.FREE_AGENCY);
	g.setWithoutSavingToDB("season", 2030);
	g.setWithoutSavingToDB("playerMoodTraits", true);
});

const wantsThreeYears = {
	tid: -1,
	contractDemand: { amount: 10000, exp: 2033 },
	contracts: [],
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

test("money past his ask buys less than money short of it costs", () => {
	const ask = 10000;
	const at = (amount: number) =>
		choiceOdds([offerUtility({ amount, ask, appeal: 0 })], 0)[0]!;
	assert.closeTo(at(ask), 0.5, 1e-12);
	const up = at(11000) - at(ask);
	const down = at(ask) - at(ask / 1.1);
	assert.isAbove(up, 0);
	assert.isAbove(down, 2 * up);
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

test("a team option costs him far more than a player option is worth to him", () => {
	const plain = effectiveAmount({ amount: 10_000 }, 3);
	const po = effectiveAmount({ amount: 10_000, option: "player" }, 3);
	const to = effectiveAmount({ amount: 10_000, option: "team" }, 3);
	assert.closeTo(po, 10_500, 1e-6);
	assert.closeTo(to, 8_500, 1e-6);
	assert.isAbove(plain - to, 2 * (po - plain));
});

test("at camp he takes an offer well under his ask rather than wait into the season, where only the minimum is left", () => {
	const ask = 10000;
	const half = offerUtility({ amount: ask / 2, ask, appeal: 0 });
	const season = moneyUtility(g.get("minContract"), ask);
	const takes = () =>
		choiceOdds(
			[half],
			stageWait(waitUtility(), season),
			stageSettings.decisionTemperature,
		)[0]!;

	// Training camp, the last stage.
	g.setWithoutSavingToDB("daysLeft", 1);
	assert.isTrue(atCamp());
	assert.isAbove(takes(), 0.95);

	// With the market still open, there is more to wait for.
	g.setWithoutSavingToDB("daysLeft", 2);
	assert.isFalse(atCamp());
	assert.isBelow(takes(), 0.05);
});
