import { g } from "../../util/index.ts";
import { getHardCap } from "../../util/getHardCap.ts";
import { checkTradeSalary } from "../realisticFrontOffice/cba.ts";
import { stagesOn } from "../realisticFrontOffice/stages.ts";

// THE TRADE RULES ON SALARY, AS THE GM KNOWS THEM: whether a team with this
// payroll may take in `incoming` salary and send out `outgoing` - the checks
// trade/summary.ts makes for one side, without reading the league. A real
// offer is one the rules allow (2026-11-14: offers built without them broke
// the rules 84% of the time, half sending the seller more salary than it may
// take, half the buyer too little back). The trade summary still has the
// last word.
export const salaryRulesAllow = (
	tid: number,
	// The team's payroll as the trade summary counts it (team.getPayroll).
	payroll: number,
	incoming: number,
	outgoing: number,
) => {
	const after = payroll + incoming - outgoing;
	if (after > g.get("salaryCap")) {
		const capType = g.get("salaryCapType");
		if (capType === "soft") {
			const ratio =
				outgoing > 0
					? Math.floor((100 * incoming) / outgoing)
					: incoming > 0
						? Infinity
						: 100;
			if (ratio > g.get("softCapTradeSalaryMatch")) {
				return false;
			}
		} else if (capType === "hard" && incoming > outgoing) {
			return false;
		}
	}
	const ceiling = getHardCap(tid);
	if (Number.isFinite(ceiling) && after > ceiling && incoming > outgoing) {
		return false;
	}
	if (
		stagesOn() &&
		!checkTradeSalary({ tid, payrollAfter: after, incoming, outgoing }).ok
	) {
		return false;
	}
	return true;
};
