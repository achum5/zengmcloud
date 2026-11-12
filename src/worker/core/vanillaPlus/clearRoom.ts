import type { Asset, RoomPlan } from "../realisticFrontOffice/clearRoom.ts";
import type { ValueChangeCalculator } from "../team/ValueChangeCalculator.ts";

// CLEARING ROOM FOR A FREE AGENT on vanilla+'s values. The old engine finds
// the trades (realisticFrontOffice/clearRoom.ts) and prices them its own way:
// a 51-31 team gave two firsts and two young players to clear $5.5M, and a
// team taking salary took picks it would have done the deal without. With
// vanilla+ on, the team taking the players must want them as it would in any
// trade, with as few of the picks as that takes; and the team clearing the
// room must value the free agent above everything it gives for the room.

const pidsOf = (give: readonly Asset[]) =>
	give.flatMap((a) => (a.kind === "player" ? [a.p.pid] : []));
const dpidsOf = (give: readonly Asset[]) =>
	give.flatMap((a) => (a.kind === "pick" ? [a.dp.dpid] : []));

// Whether the team taking the players would make the trade.
export const partnerTakes = async (
	calc: ValueChangeCalculator,
	plan: Pick<RoomPlan, "tid" | "partner">,
	give: readonly Asset[],
) =>
	(await calc.evaluate({
		tid: plan.partner,
		pidsAdd: pidsOf(give),
		pidsRemove: [],
		dpidsAdd: dpidsOf(give),
		dpidsRemove: [],
		tradingPartnerTid: plan.tid,
	})) > 0;

// What the team clearing the room sends, on vanilla+'s values: the plan's
// players with the fewest of its picks the other team takes them with - if
// the free agent (pid, at his asking contract) is worth more to it than all
// of that. Undefined if not.
export const roomOnValues = async (
	calc: ValueChangeCalculator,
	plan: RoomPlan,
	pid: number,
): Promise<Asset[] | undefined> => {
	const players = plan.give.filter((a) => a.kind === "player");
	const picks = plan.give.filter((a) => a.kind === "pick");
	const options: Asset[][] = [[], ...picks.map((a) => [a])];
	if (picks.length > 1) {
		options.push(picks);
	}
	// Cheapest to the clearing team first.
	const costs = new Map<Asset[], number>();
	for (const option of options) {
		costs.set(
			option,
			option.length === 0
				? 0
				: -(await calc.evaluate({
						tid: plan.tid,
						pidsAdd: [],
						pidsRemove: [],
						dpidsAdd: [],
						dpidsRemove: dpidsOf(option),
						tradingPartnerTid: plan.partner,
						raw: true,
					})),
		);
	}
	options.sort((a, b) => costs.get(a)! - costs.get(b)!);
	for (const option of options) {
		const give = [...players, ...option];
		if (!(await partnerTakes(calc, plan, give))) {
			continue;
		}
		const own = await calc.evaluate({
			tid: plan.tid,
			pidsAdd: [pid],
			pidsRemove: pidsOf(players),
			dpidsAdd: [],
			dpidsRemove: dpidsOf(option),
			tradingPartnerTid: plan.partner,
		});
		return own > 0 ? give : undefined;
	}
	return undefined;
};
