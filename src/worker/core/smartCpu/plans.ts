import { PHASE } from "../../../common/constants.ts";
import type { Player } from "../../../common/types.ts";
import { g } from "../../util/index.ts";
import { contractOf } from "../realisticFrontOffice/contractOf.ts";
import { retentionOf } from "../realisticFrontOffice/outlook.ts";
import type { Holdings, SmartView } from "./worth.ts";

// TEAM PLANS (owner, 2026-11-21: "every team should be determining through
// the season what kind of situation they're in"). At each market moment
// every team reads where it stands from its own title chances as its trades
// count them - this season's on what it holds (a star hurt now back for the
// playoffs), and the next three seasons':
// - contending: a real shot at this season's title;
// - pushing: an outside shot this season;
// - building: no shot now, a real one within three seasons;
// - rebuilding: neither.
// A plan steers how a team trades, not what anything is worth to it (its
// title chances say that already): which players it must move before the
// deadline, and how much risk it takes as the deadline nears.
export type Plan = "contending" | "pushing" | "building" | "rebuilding";

export const planSettings = {
	// This season's title chance that makes a contender.
	contend: 0.05,
	// And an outside shot.
	push: 0.01,
	// Any of the next three seasons' that makes a team a builder.
	build: 0.05,
	// A player whose deal ends this season, on a team not contending, less
	// likely than this to re-sign with it: he must move before the deadline
	// or leave for nothing.
	mustMoveStay: 0.5,
	// The share of its margin of error a team still asks on the deadline's
	// last day, where doing nothing is the worse risk: a contender buying
	// for this season's title, a team selling a player who must move. All of
	// it as the season opens, then less as the deadline nears (owner,
	// 2026-11-21: "Yes, by situation").
	deadlineMargin: 0.5,
};

export const plansOf = (view: SmartView, tids: Iterable<number>) => {
	const plans = new Map<number, Plan>();
	for (const tid of tids) {
		const chances = view.titleChancesNow(tid);
		const now = chances[0] ?? 0;
		const soon = Math.max(0, ...chances.slice(1, 4));
		plans.set(
			tid,
			now >= planSettings.contend
				? "contending"
				: now >= planSettings.push
					? "pushing"
					: soon >= planSettings.build
						? "building"
						: "rebuilding",
		);
	}
	return plans;
};

// How near the trade deadline the season is: 0 as it opens, 1 on the
// deadline's day; 0 outside the season before it.
export const deadlineNearness = (view: SmartView) => {
	const deadline = view.outlook.deadline;
	if (
		g.get("phase") !== PHASE.REGULAR_SEASON ||
		!deadline ||
		deadline.daysLeft <= 0
	) {
		return 0;
	}
	const progress = view.outlook.progress;
	const atDeadline =
		progress + (1 - progress) * (deadline.daysToDeadline / deadline.daysLeft);
	return atDeadline > 0 ? Math.min(1, Math.max(0, progress / atDeadline)) : 1;
};

// A player his team must move before the deadline: in season, his deal
// ending, his team not contending, and unlikely to re-sign with it. (One
// it wouldn't pay to keep it sells for what it can get - SmartView.
// keptChanceNow - but isn't put first: counted among those that must move,
// in three random-players leagues they cut players leaving for nothing 16
// to 13 for 22% more trades, a season a fifth slower to play.)
export const mustMove = (
	view: SmartView,
	plans: ReadonlyMap<number, Plan>,
	p: Player,
	tid: number,
) =>
	g.get("phase") === PHASE.REGULAR_SEASON &&
	contractOf(p).exp <= g.get("season") &&
	plans.get(tid) !== "contending" &&
	retentionOf(view.outlook, p.pid, tid) < planSettings.mustMoveStay;

// The share of its margin of error a team asks of a deal (planSettings.
// deadlineMargin): less as the deadline nears for a contender buying
// players, and for a team selling one who must move.
export const marginShare = (
	view: SmartView,
	plans: ReadonlyMap<number, Plan>,
	tid: number,
	give: Holdings,
	get: Holdings,
) => {
	const urgent =
		(plans.get(tid) === "contending" && get.players.length > 0) ||
		give.players.some((p) => mustMove(view, plans, p, tid));
	return urgent
		? 1 - (1 - planSettings.deadlineMargin) * deadlineNearness(view)
		: 1;
};
