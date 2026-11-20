import { PLAYER } from "../../../common/constants.ts";
import type { DraftPick, Player } from "../../../common/types.ts";
import { g } from "../../util/index.ts";
import getRookieContractLength from "../draft/getRookieContractLength.ts";
import { currentOutlook } from "../realisticFrontOffice/market.ts";
import {
	ensureRetention,
	retentionKey,
} from "../realisticFrontOffice/outlook.ts";
import { pickSalaries } from "../realisticFrontOffice/payroll.ts";
import { timed } from "./clock.ts";
import { marketSettings } from "./market.ts";
import { prepareSignings, viewFor } from "./signing.ts";

// THE DRAFT BY THE GM'S WORTH (docs/smart-cpu.md, layer 2). A CPU team takes
// the prospect worth the most to it on the rookie deal of its pick: what he
// does for its goal - a bad team drafting for the seasons it may contend in,
// a contender for now, his chance of re-signing forecast for the end of that
// deal - and what the market would pay for him past that, at the share it can
// be sure of (SmartView.sureWorthNow, smartSettings.premiumRealized). Between
// prospects within its margin of error of each other, the board decides.

// Prospects weighed for a pick, best first by value: the rest never come
// close (the market prices talent above any one team's needs).
export const DRAFT_CANDIDATES = 20;

// Called with each pick's reckoning before it is made, the one it takes
// first, for harnesses that log why (none in the game).
export const smartDraftWatchers: ((
	dp: Pick<DraftPick, "dpid" | "tid" | "round" | "pick">,
	worths: readonly { p: Player; worth: number; drafted: Player }[],
	view: ReturnType<typeof viewFor>,
) => void)[] = [];

// Of prospects by what each is worth to the team, the one it takes: the best
// on the board (by value, then the lower pid) of those worth no less than its
// best by more than `margin`.
export const boardChoice = <
	T extends { worth: number; p: Pick<Player, "value" | "pid"> },
>(
	worths: readonly T[],
	margin: number,
) => {
	let best: T | undefined;
	for (const x of worths) {
		if (!best || x.worth > best.worth) {
			best = x;
		}
	}
	if (!best) {
		return;
	}
	const top = best.worth;
	return worths
		.filter((x) => x.worth >= top - margin)
		.sort((a, b) => b.p.value - a.p.value || a.p.pid - b.p.pid)[0];
};

// The prospect a CPU team takes with this pick, and what each it weighed is
// worth to it.
export const smartDraftChoice = (
	dp: Pick<DraftPick, "dpid" | "tid" | "round" | "pick">,
	prospects: readonly Player[],
) => timed("draft", () => draftChoiceNow(dp, prospects));

const draftChoiceNow = async (
	dp: Pick<DraftPick, "dpid" | "tid" | "round" | "pick">,
	prospects: readonly Player[],
) => {
	const outlook = await currentOutlook();
	const tried = [...prospects]
		.sort((a, b) => b.value - a.value || a.pid - b.pid)
		.slice(0, DRAFT_CANDIDATES);
	const view = viewFor(outlook);
	// A pick is priced as a trade is (dealWorthNow): the market's tables first.
	await view.prime();
	await prepareSignings(outlook, tried);
	const slot = (dp.round - 1) * g.get("numActiveTeams") + dp.pick - 1;
	const amount = pickSalaries()({ slot, round: dp.round, years: 1, ovr: 0 });
	const exp = g.get("season") + getRookieContractLength(dp.round);
	const drafts = tried.map(
		(p) =>
			({
				...p,
				tid: dp.tid,
				contractDemand: { amount, exp, rookie: true },
			}) as Player,
	);
	// His chance of re-signing after the rookie deal he signs here, not after
	// the contract he asks for as a prospect.
	for (const p of drafts) {
		outlook.retention.delete(retentionKey(p.pid, dp.tid));
	}
	await ensureRetention(outlook, drafts, dp.tid);
	const held = view.heldNow(dp.tid);
	// The pick is used either way.
	const base = {
		players: held.players,
		picks: held.picks.filter((x) => x.dpid !== dp.dpid),
	};
	const worths: { p: Player; worth: number; drafted: Player }[] = [];
	for (const [i, p] of tried.entries()) {
		const drafted = drafts[i]!;
		worths.push({
			p,
			drafted,
			worth: view.sureWorthNow(
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
	const byBoard = (a: (typeof worths)[number], b: (typeof worths)[number]) =>
		b.p.value - a.p.value || a.p.pid - b.p.pid;
	worths.sort((a, b) => b.worth - a.worth || byBoard(a, b));
	// The board between prospects the team can't tell apart: any worth less
	// to it than its best by no more than its margin of error on him
	// (marketSettings.margins, as a trade's sides must clear theirs) is as
	// good as him, and of those it takes the one highest on the board.
	// Far from contending, every prospect does a team little, and by its own
	// use alone the order among them was mostly each one's chance of
	// re-signing four seasons on: in a League 12 run Detroit took Bol Bol
	// (38/63, 20th on the board) at #8 for 0.065 of a title against Jaxson
	// Hayes (48/76, 3rd) at 0.057, and Hayes fell to #10.
	const best = worths[0];
	if (best && marketSettings.margins) {
		const margin = view.marginNow(
			dp.tid,
			{ players: [], picks: [] },
			{ players: [best.drafted], picks: [] },
		);
		const choice = boardChoice(worths, margin)!;
		worths.splice(worths.indexOf(choice), 1);
		worths.unshift(choice);
	}
	for (const watch of smartDraftWatchers) {
		watch(dp, worths, view);
	}
	return { choice: worths[0]?.p, worths };
};
