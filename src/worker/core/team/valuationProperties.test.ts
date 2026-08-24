import { assert, beforeEach, describe, test } from "vitest";
import { g } from "../../util/index.ts";
import { changeTracker } from "../../db/changeTracker.ts";
import { ValueChangeCalculator } from "./ValueChangeCalculator.ts";
import {
	AI_TID,
	buildValuationLeague,
	type Spec,
	USER_TID,
} from "../../../test/fixtures/valuationLeague.ts";

// ---------------------------------------------------------------------------
// THINGS THE ARITHMETIC MUST NEVER DO.
//
// The two real defects found in this calculator were the same shape, and
// neither was findable by asking whether a particular number looked right:
//
//   - an injury made a below-average player worth MORE, because the discount
//     was a multiplication on a z-score and half the league is negative;
//   - a third second-round pick made every second-round pick in the deal
//     CHEAPER, because the premium counted first rounders by mistake.
//
// Both are monotonicity violations. Neither shows up in a single comparison -
// any one pairing lands on one side of the line and tells you nothing - and
// both show up at once if you sweep the input and watch the direction. So
// rather than keep finding these one at a time, this sweeps each dimension the
// valuation takes and asserts only the direction, which is the part that is
// not a matter of taste.
//
// WHAT EACH PROPERTY IS PROVEN AGAINST, because a passing test that cannot
// fail is decoration and this file would be the easiest place in the codebase
// to accumulate some. Every one below was checked by breaking the code under
// it:
//
//   better player offered / asked for  - flattening zscore
//   injury never worth more            - THE REAL DEFECT, and value zeroed
//   age never worth more               - value zeroed
//   bigger salary never an improvement - CONTRACT_FACTOR zeroed
//   giving up one more good player     - CONTRACT_FACTOR zeroed
//   picks: total and price-per-pick    - THE REAL DEFECT, and zscore flattened
//
// The exception is "receiving one more good player is never worse", which no
// mutation here breaks: there is no pile adjustment on the player side, so it
// is arithmetically additive and cannot currently misbehave. It is kept as a
// guard against somebody adding one - which is exactly what went wrong on the
// pick side - and not as evidence about the code as it stands.
//
// Two traps, both of which produced a confident wrong answer first time round:
//
//   THE FIXTURE'S TIER IS FRINGE, not buyer. Tier decides the multipliers, so a
//   mutation aimed at the wrong row of a table changes nothing and reads as
//   "the property cannot fail".
//
//   A BUYER'S AGE TABLE STOPS AT 24 and does not extend, so on a mid-table team
//   an ageing player never touches it and an age sweep quietly measures BBGM's
//   base value instead. The age property runs against a seller for that reason.

// ---------------------------------------------------------------------------

// The AI's side of every swap: a real rotation player, deliberately under
// coreValue so the untouchable guard stays out of the arithmetic.
const TARGET: Spec = { ovr: 52, age: 29 };

const dvFor = async ({
	give = [],
	get = [],
	aiPicks,
	givePicks = false,
	aiWon,
}: {
	give?: Spec[];
	get?: Spec[];
	aiPicks?: number[];
	givePicks?: boolean;
	aiWon?: number;
}) => {
	const { userExtra, aiExtra, dpids } = await buildValuationLeague({
		user: give,
		ai: get,
		aiPicks,
		aiWon,
	});
	return new ValueChangeCalculator().evaluate({
		tid: AI_TID,
		pidsAdd: userExtra.map((p) => p.pid),
		pidsRemove: aiExtra.map((p) => p.pid),
		dpidsAdd: [],
		dpidsRemove: givePicks ? dpids : [],
		tradingPartnerTid: USER_TID,
	});
};

// Sweeping one input and checking the sign of every step. `expect` is the
// direction the series must move; equal steps are allowed, because plenty of
// these adjustments legitimately saturate.
const assertMonotonic = (
	values: number[],
	expect: "up" | "down",
	what: string,
) => {
	const shown = values.map((v) => v.toPrecision(4)).join(" -> ");
	for (let i = 1; i < values.length; i++) {
		if (expect === "up") {
			assert.isAtLeast(values[i]!, values[i - 1]! - 1e-9, `${what}: ${shown}`);
		} else {
			assert.isAtMost(values[i]!, values[i - 1]! + 1e-9, `${what}: ${shown}`);
		}
	}
	// A flat series satisfies the loop above and proves nothing, so the sweep
	// also has to actually move somewhere.
	assert.notStrictEqual(
		values[0],
		values.at(-1),
		`${what}: nothing moved at all across the sweep (${shown})`,
	);
};

describe("what the trade valuation must never do", () => {
	beforeEach(() => {
		changeTracker.disable();
		changeTracker.reset();
	});

	test("a better player offered is never worth less", async () => {
		const dvs = [];
		for (const ovr of [44, 48, 52, 56, 60, 64]) {
			dvs.push(await dvFor({ give: [{ ovr, age: 28 }], get: [TARGET] }));
		}
		assertMonotonic(dvs, "up", "a better player was worth less");
	});

	test("a better player asked for never costs less", async () => {
		const dvs = [];
		for (const ovr of [44, 48, 52, 56, 60]) {
			dvs.push(
				await dvFor({ give: [{ ovr: 52, age: 28 }], get: [{ ovr, age: 29 }] }),
			);
		}
		assertMonotonic(dvs, "down", "asking for more cost less");
	});

	// The first of the two real defects. Below average is where it inverted, so
	// the sweep runs there as well as at the top.
	test("an injury never makes a player worth more, at any level of ability", async () => {
		for (const ovr of [46, 54, 62, 70]) {
			const dvs = [];
			for (const injuredGames of [0, 15, 35, 60, 75]) {
				dvs.push(
					await dvFor({
						give: [{ ovr, age: 28, injuredGames }],
						get: [TARGET],
					}),
				);
			}
			assertMonotonic(dvs, "down", `${ovr} ovr got better by being hurt`);
		}
	});

	// Same shape, the other adjustment that multiplies a signed value.
	//
	// This has to run against a SELLER. The tiers do not share an age table and
	// a buyer's stops at 24 without extending, so on a mid-table team an ageing
	// player never touches the multiplier at all and the sweep quietly measures
	// BBGM's base value instead - passing whatever the code does. A seller's
	// table extends, which is what puts the adjustment under the sweep.
	test("age never makes a player worth more, at any level of ability", async () => {
		for (const ovr of [46, 54, 62]) {
			const dvs = [];
			for (const age of [26, 29, 32, 35, 38]) {
				dvs.push(
					await dvFor({ give: [{ ovr, age }], get: [TARGET], aiWon: 14 }),
				);
			}
			assertMonotonic(dvs, "down", `${ovr} ovr got better by getting older`);
		}
	});

	test("a bigger salary on the same player is never an improvement", async () => {
		const salaryCap = g.get("salaryCap");
		const dvs = [];
		for (const share of [0.03, 0.08, 0.15, 0.22, 0.3]) {
			dvs.push(
				await dvFor({
					give: [
						{
							ovr: 54,
							age: 28,
							amount: Math.round(salaryCap * share),
							exp: g.get("season") + 4,
						},
					],
					get: [TARGET],
				}),
			);
		}
		assertMonotonic(dvs, "down", "a fatter contract made him more attractive");
	});

	// DOMINANCE, which is the general form of the pick bug and would have caught
	// it in a different guise: whatever else changes, being handed one more GOOD
	// player cannot leave a team worse off.
	//
	// "Good" is load-bearing, and was got wrong first time. A player at the
	// league's median rating is worth about nothing once his roster spot and his
	// contract are counted, so piling those on genuinely does make a deal worse -
	// a sweep over them measures the wrong thing and reads as a bug. These sit
	// well clear of replacement level.
	test("receiving one more good player is never worse", async () => {
		const dvs = [];
		const pile: Spec[] = [];
		for (let i = 0; i < 4; i++) {
			pile.push({ ovr: 62 - i, age: 27 });
			dvs.push(await dvFor({ give: [...pile], get: [TARGET] }));
		}
		assertMonotonic(dvs, "up", "an extra good player made the deal worse");
	});

	test("giving up one more good player is never better", async () => {
		const dvs = [];
		const pile: Spec[] = [];
		for (let i = 0; i < 4; i++) {
			pile.push({ ovr: 62 - i, age: 27 });
			dvs.push(await dvFor({ give: [{ ovr: 72, age: 28 }], get: [...pile] }));
		}
		assertMonotonic(
			dvs,
			"down",
			"giving up an extra good player made the deal better",
		);
	});

	// The second real defect, as a property rather than a single case: the pile
	// premium exists so that gutting a draft is not three small decisions, and
	// whichever round it is in, more picks must cost more.
	test("giving up more picks always costs more, in every round", async () => {
		for (const round of [1, 2]) {
			const dvs = [];
			for (const count of [1, 2, 3, 4, 5]) {
				dvs.push(
					await dvFor({
						get: [TARGET],
						aiPicks: Array.from({ length: count }, () => round),
						givePicks: true,
					}),
				);
			}
			assertMonotonic(dvs, "down", `round ${round}: a bigger pile cost less`);
		}
	});

	// And the per-pick form, which is the one that actually caught it. A total
	// that rises proves nothing: three picks cost more than two however badly
	// each is priced.
	test("and the price per pick rises with the size of the pile", async () => {
		for (const round of [1, 2]) {
			const perPick = [];
			for (const count of [1, 2, 3, 4]) {
				const dv = await dvFor({
					get: [TARGET],
					aiPicks: Array.from({ length: count }, () => round),
					givePicks: true,
				});
				perPick.push(-dv / count);
			}
			assertMonotonic(perPick, "up", `round ${round}: each pick got cheaper`);
		}
	});
});
