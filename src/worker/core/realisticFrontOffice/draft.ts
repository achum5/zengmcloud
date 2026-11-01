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
import { strengthSettings } from "./strength.ts";

// THE DRAFT (owner's calls). A CPU team takes the prospect worth the most to
// its franchise - true ratings, through the same projections and title odds
// as everything else, on the rookie deal of its pick - so talent rules and
// fit and timeline break near-ties. From the lottery on, the draft's picks
// are priced by who would go there (outlook.ts, the class board), kept up to
// date as players are taken.

// For the arena, to measure what a behaviour is worth by turning it off.
export const draftSettings = {
	// Off: CPU teams draft as in stock.
	franchiseBoard: true,
	// Off: the floor's positions count in full, as for a trade.
	talentFirst: true,
};

// TALENT FIRST (owner's call: talent rules, fit breaks near-ties). Prospects
// are ranked by what they are worth to the franchise with the floor's
// positions left out - a prospect plays for rosters years away, which the
// team will have reshaped by then - and positions only choose among those
// within this share of the best. Counted in full, a roster's guard shortage
// had CPU teams take a point guard over a better player in a third of the
// picks that passed one: the 5th best prospect on average, 3.7 points of
// overall three years on given up.
const NEAR_TIE = 0.05;

// The prospect a CPU team takes with this pick.
export const bestProspect = async (
	dp: DraftPick,
	prospects: readonly Player[],
) => {
	if (!draftSettings.franchiseBoard) {
		return undefined;
	}
	const outlook = await currentOutlook();
	const t = await loadTeam(outlook, dp.tid);
	await ensureRetention(outlook, prospects, dp.tid);
	// The pick is used either way.
	const picks = t.picks.filter((x) => x.dpid !== dp.dpid);
	const slot = (dp.round - 1) * g.get("numActiveTeams") + dp.pick - 1;
	const amount = pickSalaries()({ slot, round: dp.round, years: 1, ovr: 0 });
	const exp = g.get("season") + getRookieContractLength(dp.round);
	const valueOf = (p: Player) => {
		const drafted = {
			...p,
			tid: dp.tid,
			contract: { amount, exp, rookie: true },
		} as Player;
		return franchiseValue(
			outlook,
			teamStrengths(outlook, dp.tid, [...t.players, drafted], picks),
		);
	};
	if (!draftSettings.talentFirst) {
		let best: { p: Player; value: number } | undefined;
		for (const p of prospects) {
			const value = valueOf(p);
			if (!best || value > best.value) {
				best = { p, value };
			}
		}
		return best?.p;
	}

	// Talent: worth to the franchise with positions left out, against the
	// team without him the same way.
	const positions = strengthSettings.positions;
	let talent: { p: Player; value: number }[];
	let base: number;
	try {
		strengthSettings.positions = false;
		base = franchiseValue(
			outlook,
			teamStrengths(outlook, dp.tid, t.players, picks),
		);
		talent = prospects.map((p) => ({ p, value: valueOf(p) - base }));
	} finally {
		strengthSettings.positions = positions;
	}
	const top = Math.max(...talent.map((x) => x.value));
	let best: { p: Player; value: number } | undefined;
	for (const x of talent) {
		if (x.value < top - NEAR_TIE * Math.abs(top)) {
			continue;
		}
		const value = valueOf(x.p);
		if (!best || value > best.value) {
			best = { p: x.p, value };
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
