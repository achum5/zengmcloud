import type {
	MinimalPlayerRatings,
	Player,
	TradeTeams,
} from "../../../common/types.ts";
import { g } from "../../util/index.ts";
import { getFuzz, applyFuzz } from "../player/fuzzRating.ts";
import type { LookingFor } from "../trade/makeItWork.ts";
import summary from "../trade/summary.ts";
import { assetsFor, judgeHumanOffer, type Side } from "./humanTrades.ts";
import { aiTids, loadTeam, type Asset } from "./market.ts";
import type { LeagueOutlook } from "./outlook.ts";
import { IMPACT_RATINGS } from "./playerImpact.ts";
import {
	frontOfficeValuer,
	lastTaken,
	makeItWorkSmart,
} from "./vanillaTrades.ts";

// CPU TEAMS' OFFERS TO HUMANS (owner's call, 2026-11-11: "the same identical
// mechanics as vanilla, it's just that teams are far far smarter"): Trade
// Proposals, "What would make this work" and the trading block are
// vanilla's, a team adding or asking for one asset at a time until the deal
// is only just good enough for it - its best offer - priced by its own read
// (vanillaTrades.ts). Before, the trading block held each bid to three
// assets sized by the stock trade values, and "What would make this work"
// added at most one of the team's own.

// A player as the human sees him: every rating with his scouting fuzz on it.
const publicRow = <R extends MinimalPlayerRatings>(r: R): R => {
	const fuzz = getFuzz(r.fuzz);
	if (fuzz === 0) {
		return r;
	}
	const out: Record<string, unknown> = { ...r, fuzz: 0 };
	for (const key of IMPACT_RATINGS) {
		out[key] = applyFuzz((r as any)[key], fuzz);
	}
	out.ovr = applyFuzz(r.ovr, fuzz);
	out.pot = applyFuzz(r.pot, fuzz);
	return out as R;
};

export const publicPlayer = (p: Player): Player => {
	if (getFuzz(p.ratings.at(-1)!.fuzz) === 0) {
		return p;
	}
	return {
		...p,
		ratings: p.ratings.map(publicRow) as Player["ratings"],
		value: p.valueFuzz,
		valueNoPot: p.valueNoPotFuzz,
	};
};

const empty = (tid: number) => ({
	tid,
	pids: [] as number[],
	dpids: [] as number[],
	pidsExcluded: [] as number[],
	dpidsExcluded: [] as number[],
});

// Whether the CPU team (the trade screen's second) takes the deal by the
// same judge as a human's offer: before the deadline, not if it would rather
// wait; `shop`, not if what it gives would fetch more from another team.
const takes = async (teams: TradeTeams, shop: boolean) =>
	(
		await judgeHumanOffer(
			{ tid: teams[1].tid, pids: teams[1].pids, dpids: teams[1].dpids },
			{ tid: teams[0].tid, pids: teams[0].pids, dpids: teams[0].dpids },
			{ shop },
		)
	).kind === "yes";

// TRADE PROPOSALS: the page picks one of the human's assets for a CPU team as
// vanilla does, and the team makes the deal work as vanilla's does, adding
// at most five assets either side. The trade screen's teams, user's side
// first; undefined if there is none it would take.
export const proposalTeams = async (
	teams: TradeTeams,
): Promise<TradeTeams | undefined> => {
	const valuer = await frontOfficeValuer();
	const states: TradeTeams[] = [];
	await makeItWorkSmart(teams, {
		holdUserConstant: false,
		maxAssetsToAdd: 5,
		valuer,
		states,
	});
	return lastTaken(
		states,
		async (x) =>
			x[1].pids.length + x[1].dpids.length > 0 &&
			!(await summary(x)).warning &&
			(await takes(x, true)),
	);
};

// Whether a Trade Proposals offer made earlier still stands: everything in it
// where it was, and the team would still take it (it was shopped when made).
export const proposalStands = async (teams: TradeTeams) =>
	(await judgeHumanOffer(teams[1], teams[0], { shop: false })).kind === "yes";

// "What would make this work" on the trade screen, as vanilla's: from a deal
// the team would not take, it asks for the human's assets until it would,
// then gives back what it can; from one it would, it adds its own until it
// only just would. The deal as a trade screen holds it, user's side first -
// undefined if there is none.
export const counterTeams = async (
	teams: TradeTeams,
): Promise<TradeTeams | undefined> => {
	const valuer = await frontOfficeValuer();
	const states: TradeTeams[] = [];
	await makeItWorkSmart(teams, {
		holdUserConstant: false,
		valuer,
		states,
	});
	return lastTaken(states, (x) => takes(x, true));
};

// The same, from what each side gives: what the CPU team would give and get.
export const counterOffer = async (
	outlook: LeagueOutlook,
	cpu: Side,
	human: Side,
): Promise<{ give: Asset[]; get: Asset[] } | undefined> => {
	const made = await counterTeams([
		{ ...empty(human.tid), pids: [...human.pids], dpids: [...human.dpids] },
		{ ...empty(cpu.tid), pids: [...cpu.pids], dpids: [...cpu.dpids] },
	]);
	if (!made) {
		return undefined;
	}
	const give = assetsFor(
		await loadTeam(outlook, cpu.tid),
		made[1].pids,
		made[1].dpids,
	);
	const get = assetsFor(
		await loadTeam(outlook, human.tid),
		made[0].pids,
		made[0].dpids,
	);
	return give && get ? { give, get } : undefined;
};

// THE TRADING BLOCK, as vanilla's: each CPU team adds its own assets to what
// the human puts up until the deal is only just good enough for it, at most
// four more than he puts up - picks first if he asks for picks, a player at
// the position first if he asks for one, by the sorts he asks for. Salaries
// left out, as vanilla's: he makes the money work after. The trade screens'
// teams, user's side first, of the teams that bid anything.
export const tradingBlockTeams = async (
	userTid: number,
	pids: number[],
	dpids: number[],
	lookingFor: LookingFor,
): Promise<TradeTeams[]> => {
	const valuer = await frontOfficeValuer();
	const teamInfo = g.get("teamInfoCache");
	const cpuTids = (await aiTids()).sort((a, b) =>
		`${teamInfo[a]?.region} ${teamInfo[a]?.name}`.localeCompare(
			`${teamInfo[b]?.region} ${teamInfo[b]?.name}`,
		),
	);
	const offers: TradeTeams[] = [];
	for (const tid of cpuTids) {
		const states: TradeTeams[] = [];
		await makeItWorkSmart([{ ...empty(userTid), pids, dpids }, empty(tid)], {
			holdUserConstant: true,
			maxAssetsToAdd: 4 + pids.length + dpids.length,
			lookingFor,
			valuer,
			states,
		});
		const bid = await lastTaken(
			states,
			async (x) =>
				x[1].pids.length + x[1].dpids.length > 0 && (await takes(x, false)),
		);
		if (bid) {
			offers.push(bid);
		}
	}
	return offers;
};
