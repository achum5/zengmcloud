import { PHASE } from "../../../common/constants.ts";
import type { DraftPick, Player } from "../../../common/types.ts";
import { g } from "../../util/index.ts";
import getRookieContractLength from "../draft/getRookieContractLength.ts";
import { currentOutlook, loadTeam } from "./market.ts";
import {
	buildClassBoard,
	ensureRetention,
	franchiseValue,
	teamStrengths,
} from "./outlook.ts";
import { pickSalaries } from "./payroll.ts";

// THE DRAFT (owner's calls). A CPU team takes the prospect worth the most to
// its franchise - true ratings, through the same projections and title odds
// as everything else, on the rookie deal of its pick - so talent rules and
// fit and timeline break near-ties. From the lottery on, the draft's picks
// are priced by who would go there (outlook.ts, the class board), kept up to
// date as players are taken.

// The prospect a CPU team takes with this pick.
export const bestProspect = async (
	dp: DraftPick,
	prospects: readonly Player[],
) => {
	const outlook = await currentOutlook();
	const t = await loadTeam(outlook, dp.tid);
	await ensureRetention(outlook, prospects, dp.tid);
	// The pick is used either way.
	const picks = t.picks.filter((x) => x.dpid !== dp.dpid);
	const slot = (dp.round - 1) * g.get("numActiveTeams") + dp.pick - 1;
	const amount = pickSalaries()({ slot, round: dp.round, years: 1, ovr: 0 });
	const exp = g.get("season") + getRookieContractLength(dp.round);
	let best: { p: Player; value: number } | undefined;
	for (const p of prospects) {
		const drafted = {
			...p,
			tid: dp.tid,
			contract: { amount, exp, rookie: true },
		} as Player;
		const value = franchiseValue(
			outlook,
			teamStrengths(outlook, dp.tid, [...t.players, drafted], picks),
		);
		if (!best || value > best.value) {
			best = { p, value };
		}
	}
	return best?.p;
};

// After a pick, the board the draft's other picks are priced off.
export const afterPick = async () => {
	if (g.get("phase") !== PHASE.DRAFT) {
		return;
	}
	const outlook = await currentOutlook();
	outlook.classBoard = await buildClassBoard(outlook);
};
