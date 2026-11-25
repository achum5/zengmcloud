import { PLAYER } from "../../../common/constants.ts";
import type { DraftPick, Player } from "../../../common/types.ts";
import { g, local } from "../../util/index.ts";
import value from "../player/value.ts";
import { projectRatings } from "../realisticFrontOffice/projection.ts";
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

// Prospects weighed for a pick, best first on the board (boardValue): the rest
// never come close (the market prices talent above any one team's needs).
export const DRAFT_CANDIDATES = 20;

// For the arena, to measure what a behaviour is worth by turning it off.
export const smartDraftSettings = {
	// The board a team weighs prospects from and breaks near-ties on is value
	// with potential scouted again over 200 careers (boardValue). Off, the
	// game's value.
	scoutedBoard: true,
};

// The game's potential is the 75th percentile of a player's best overall to
// age 29 over 20 simulated careers (monteCarloPot), and 0.7 of a 19-year-old's
// value, so a few lucky careers can put a prospect at the top of the board:
// each class's top prospect by value who slid 13-40 picks played out 22nd to
// 62nd best (400 careers each, four arena drafts, 2026-11-25) - teams were
// right to pass on him, but the board they break ties on and pick their 20
// candidates from was ordered by that luck. Scouted the same way over 200
// careers, lottery picks came within 0.54 of the best player left (his
// average best overall) against 0.85, and the four first rounds took players
// 0.26 better (with outlookSettings.youngCareers). Seeded by the player and
// season, so every device in a shared league reads him the same.
const SCOUTED_CAREERS = 200;
const boardValues = new Map<string, number>();
let boardValuesKey = "";
export const boardValue = (p: Player) => {
	if (!smartDraftSettings.scoutedBoard) {
		return p.value;
	}
	const season = g.get("season");
	const forKey = `${g.get("lid")}:${season}`;
	if (forKey !== boardValuesKey || boardValues.size > 2_000) {
		boardValues.clear();
		boardValuesKey = forKey;
	}
	const r = p.ratings.at(-1)!;
	const key = `${p.pid}:${r.season}:${r.ovr}:${r.pot}:${p.value}`;
	let v = boardValues.get(key);
	if (v === undefined) {
		const age = season - p.born.year;
		const pot = Math.max(
			r.ovr,
			Math.round(
				projectRatings({
					ratings: r,
					age,
					years: Math.max(0, 29 - age),
					samples: SCOUTED_CAREERS,
					seed: (p.pid * 2_654_435_761 + season * 40_507 + 1) % 2_147_483_647,
				}).pot,
			),
		);
		v = value(
			{ ...p, ratings: [{ ...r, pot }] },
			{ ovrMean: local.playerOvrMean, ovrStd: local.playerOvrStd },
		);
		boardValues.set(key, v);
	}
	return v;
};

// Called with each pick's reckoning before it is made, the one it takes
// first, for harnesses that log why (none in the game).
export const smartDraftWatchers: ((
	dp: Pick<DraftPick, "dpid" | "tid" | "round" | "pick">,
	worths: readonly { p: Player; worth: number; drafted: Player }[],
	view: ReturnType<typeof viewFor>,
) => void)[] = [];

// Of prospects by what each is worth to the team, the one it takes: the best
// on the board (`board`, the game's value unless given; then the lower pid) of
// those worth no less than its best by more than `margin`.
export const boardChoice = <
	T extends { worth: number; p: Pick<Player, "value" | "pid"> },
>(
	worths: readonly T[],
	margin: number,
	board: (p: T["p"]) => number = (p) => p.value,
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
		.sort((a, b) => board(b.p) - board(a.p) || a.p.pid - b.p.pid)[0];
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
		.sort((a, b) => boardValue(b) - boardValue(a) || a.pid - b.pid)
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
		boardValue(b.p) - boardValue(a.p) || a.p.pid - b.p.pid;
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
		const choice = boardChoice(worths, margin, boardValue)!;
		worths.splice(worths.indexOf(choice), 1);
		worths.unshift(choice);
	}
	for (const watch of smartDraftWatchers) {
		watch(dp, worths, view);
	}
	return { choice: worths[0]?.p, worths };
};
