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
	const out = {} as Record<BudgetKey, number>;
	for (const key of Object.keys(tilt) as BudgetKey[]) {
		out[key] = helpers.bound(Math.round(baseLevel + tilt[key]), 1, MAX_LEVEL);
	}
	return out;
};
