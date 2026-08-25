import { MAX_LEVEL } from "../../../common/budgetLevels.ts";
import { helpers } from "../../util/index.ts";
import type { TradeTier } from "../trade/tradePosture.ts";

// ---------------------------------------------------------------------------
// WHERE A FRONT OFFICE PUTS ITS MONEY
//
// AI budgets were a coin flip: each preseason, each department had a 50%
// chance of being reset to a level implied by market size, with no notion of
// what the franchise was trying to do. But the four departments serve
// different plans - coaching develops the young roster a rebuild lives on,
// the health staff protects the aging legs a contender depends on (injury
// duration scales with age), facilities drive the gate and sway free agents,
// and scouting matters most to the team whose future arrives through the
// draft. An all-in team that has traded its picks famously guts its scouting
// department; a teardown does the opposite.
//
// So a smart team's departments are tilted around the SAME market-size base
// vanilla would spend - each tilt row sums to zero, so this reallocates the
// payroll a team was going to spend anyway rather than conjuring money.
// ---------------------------------------------------------------------------

export type BudgetKey = "scouting" | "coaching" | "health" | "facilities";

export const BUDGET_TILT: Record<TradeTier, Record<BudgetKey, number>> = {
	// The future arrives through the draft and the practice gym; nobody is
	// buying tickets this year and the veterans' legs are not the point.
	teardown: { scouting: 6, coaching: 10, health: -8, facilities: -8 },
	seller: { scouting: 4, coaching: 6, health: -5, facilities: -5 },
	fringe: { scouting: 0, coaching: 0, health: 0, facilities: 0 },
	// Win-now teams protect the legs they have and the building they sell out,
	// paid for out of a scouting department with fewer picks to scout for.
	buyer: { scouting: -6, coaching: 0, health: 4, facilities: 2 },
	allIn: { scouting: -12, coaching: 0, health: 8, facilities: 4 },
};

// AND THE CLAMP MUST NOT TURN THE PLAN INTO A RAISE.
//
// The row summing to zero is only half of "spends what it was going to spend".
// Levels live on a 1..100 scale and a small-market team starts near the BOTTOM
// of it: defaultBudgetLevel hands the poorest club in a thirty-team league a
// base of about 2. A teardown's -8 to health and facilities cannot be taken
// from a department at 2, so both clamp at 1 while scouting and coaching take
// their +6 and +10 in full - and the club ends up spending nearly three times
// what vanilla would have it spend, funded out of nothing. About a third of
// the league sits low enough for this to bite, every season.
//
// It runs the other way at the top: a big-market allIn team clamps its
// increases against 100 while its -12 to scouting comes off in full, so the
// plan quietly UNDERspends. Same defect, opposite sign, and the tilt is
// supposed to be a reallocation in both cases.
//
// So whatever the clamp eats is handed back to the departments that still have
// room, taken from (or given to) the ones that have strayed furthest from the
// base - the plan keeps its shape and gives up only as much of it as the scale
// will not fund. A boost that can only be paid for by cutting a department
// already at the floor is not a plan, it is a wish.
const settleResidual = (
	levels: Record<BudgetKey, number>,
	keys: BudgetKey[],
	base: number,
) => {
	const total = () => keys.reduce((sum, key) => sum + levels[key], 0);
	// Positive: the row came out UNDER what the team was going to spend.
	let residual = keys.length * base - total();
	// Each pass moves exactly one level, so this cannot run longer than the
	// residual it is closing. The guard is for a bound nobody can reach.
	let guard = keys.length * MAX_LEVEL;
	while (residual !== 0 && guard > 0) {
		guard -= 1;
		const step = residual > 0 ? 1 : -1;
		let pick: BudgetKey | undefined;
		let bestSlack = -Infinity;
		for (const key of keys) {
			const room = step > 0 ? levels[key] < MAX_LEVEL : levels[key] > 1;
			if (!room) {
				continue;
			}
			// Furthest from the base in the direction being corrected, so the
			// departments the plan cares least about pay first.
			const slack = step * (base - levels[key]);
			if (slack > bestSlack) {
				bestSlack = slack;
				pick = key;
			}
		}
		if (pick === undefined) {
			// The whole row is against a bound; there is nowhere left to put it.
			return;
		}
		levels[pick] += step;
		residual -= step;
	}
};

// The department levels for a team with this plan, spread around the given
// market-size base level.
export const smartBudgetLevels = ({
	tier,
	baseLevel,
}: {
	tier: TradeTier;
	baseLevel: number;
}): Record<BudgetKey, number> => {
	const tilt = BUDGET_TILT[tier];
	const keys = Object.keys(tilt) as BudgetKey[];
	const base = helpers.bound(Math.round(baseLevel), 1, MAX_LEVEL);

	const out = {} as Record<BudgetKey, number>;
	for (const key of keys) {
		out[key] = helpers.bound(base + tilt[key], 1, MAX_LEVEL);
	}
	settleResidual(out, keys, base);
	return out;
};
