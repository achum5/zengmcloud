import { PLAYER } from "../../../common/constants.ts";
import type { DraftPick, Player } from "../../../common/types.ts";
import { g } from "../../util/index.ts";
import getRookieContractLength from "../draft/getRookieContractLength.ts";
import { currentOutlook } from "../realisticFrontOffice/market.ts";
import { ensureRetention } from "../realisticFrontOffice/outlook.ts";
import { pickSalaries } from "../realisticFrontOffice/payroll.ts";
import { prepareSignings, viewFor } from "./signing.ts";

// THE DRAFT BY THE GM'S WORTH (docs/smart-cpu.md, layer 2). A CPU team takes
// the prospect worth the most to it on the rookie deal of its pick: what he
// does for its goal, and at least what the market would pay for him (layer
// 1's floor) - so a bad team, to whom any prospect does little for years,
// takes the best asset, as a human drafting the best player available does.

// Prospects weighed for a pick, best first by value: the rest never come
// close (the market prices talent above any one team's needs).
export const DRAFT_CANDIDATES = 20;

// The prospect a CPU team takes with this pick, and what each it weighed is
// worth to it.
export const smartDraftChoice = async (
	dp: Pick<DraftPick, "dpid" | "tid" | "round" | "pick">,
	prospects: readonly Player[],
) => {
	const outlook = await currentOutlook();
	const tried = [...prospects]
		.sort((a, b) => b.value - a.value || a.pid - b.pid)
		.slice(0, DRAFT_CANDIDATES);
	await prepareSignings(outlook, tried);
	await ensureRetention(outlook, tried, dp.tid);
	const view = viewFor(outlook);
	const held = view.heldNow(dp.tid);
	// The pick is used either way.
	const base = {
		players: held.players,
		picks: held.picks.filter((x) => x.dpid !== dp.dpid),
	};
	const slot = (dp.round - 1) * g.get("numActiveTeams") + dp.pick - 1;
	const amount = pickSalaries()({ slot, round: dp.round, years: 1, ovr: 0 });
	const exp = g.get("season") + getRookieContractLength(dp.round);
	const worths: { p: Player; worth: number }[] = [];
	for (const p of tried) {
		const drafted = {
			...p,
			tid: dp.tid,
			contractDemand: { amount, exp, rookie: true },
		} as Player;
		worths.push({
			p,
			worth: view.dealWorthNow(
				dp.tid,
				PLAYER.UNDRAFTED,
				{ players: [], picks: [] },
				{ players: [drafted], picks: [] },
				base,
			),
		});
	}
	// Ties to the better prospect, then the lower pid, so every device in a
	// shared league drafts the same player.
	worths.sort(
		(a, b) => b.worth - a.worth || b.p.value - a.p.value || a.p.pid - b.p.pid,
	);
	return { choice: worths[0]?.p, worths };
};
