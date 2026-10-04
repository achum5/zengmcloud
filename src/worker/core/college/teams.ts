import {
	COLLEGE_PRIORITIES,
	collegeFinalSeason,
	collegeNilBudget,
	homeState,
	sameRegion,
	type CollegePriority,
	type CollegeProfile,
	type CollegeRecruiting,
} from "../../../common/college.ts";
import { idb } from "../../db/index.ts";
import { g, helpers } from "../../util/index.ts";
import type { Player, Team } from "../../../common/types.ts";
import { last } from "../../../common/utils.ts";
import { userCoachStability } from "./coach.ts";
import { isAiControlled } from "../../util/isAiControlled.ts";

// What a school offers a player, priority by priority, and the interest that
// adds up to. Shared by recruiting, the portal and retention.

export const posGroup = (pos: string) =>
	pos.includes("C") || pos === "PF" || pos === "FC"
		? "big"
		: pos.includes("G") && !pos.includes("F")
			? "guard"
			: "wing";

export type TeamCtx = {
	t: Team;
	tid: number;
	prestige: number;
	// Recent winning percentage, this season weighted most.
	winp: number;
	confStrength: number;
	proScore: number;
	coachScore: number;
	facilities: number;
	rep: number;
	// Scholarships still open for next season.
	open: number;
	// Next season's returning players, for playing time.
	returning: { group: string; ovr: number }[];
	nilBudget: number;
	nilCommitted: number;
	visitsUsed: number;
	auto: boolean;
	user: boolean;
};

const recentWinp = async (tid: number, season: number) => {
	let total = 0;
	let weight = 0;
	for (const [i, w] of [0.5, 0.3, 0.2].entries()) {
		const ts = await idb.cache.teamSeasons.indexGet("teamSeasonsByTidSeason", [
			tid,
			season - i,
		]);
		if (!ts) {
			continue;
		}
		const games = ts.won + ts.lost;
		if (games === 0) {
			continue;
		}
		// A few games into a season don't say much yet.
		const ww = w * Math.min(1, games / 10);
		total += (ts.won / games) * ww;
		weight += ww;
	}
	return weight > 0 ? total / weight : 0.5;
};

export const getTeamCtxs = async (recruits: Player[]) => {
	const season = g.get("season");
	const userTids = g.get("userTids");
	const auto = new Set(g.get("collegeAutoRecruit") ?? []);
	const rosterLimit = g.get("maxRosterSize");
	const nilScale = g.get("collegeNilScale");
	const teams = (await idb.cache.teams.getAll()).filter((t) => !t.disabled);

	const confPrestige = new Map<number, number[]>();
	for (const t of teams) {
		const list = confPrestige.get(t.cid) ?? [];
		list.push(t.prestige ?? 30);
		confPrestige.set(t.cid, list);
	}

	const ctxs = new Map<number, TeamCtx>();
	for (const t of teams) {
		const roster = await idb.cache.players.indexGetAll("playersByTid", t.tid);
		// Next season's roster: everyone who still has eligibility left.
		const returningPlayers = roster.filter((p) => {
			const final = collegeFinalSeason(p);
			return final === undefined || final > season;
		});
		let nilCommitted = 0;
		for (const p of returningPlayers) {
			nilCommitted += p.contract.amount;
		}
		const conf = confPrestige.get(t.cid) ?? [30];
		const pros = (t.collegePros ?? []).reduce((a, b) => a + b, 0);
		const prestige = t.prestige ?? 30;
		ctxs.set(t.tid, {
			t,
			tid: t.tid,
			prestige,
			winp: await recentWinp(t.tid, season),
			confStrength: helpers.bound(
				(conf.reduce((a, b) => a + b, 0) / conf.length - 20) / 60,
				0,
				1,
			),
			proScore: 0.6 * Math.min(1, pros / 5) + 0.4 * (prestige / 100),
			coachScore:
				(await userCoachStability(t.tid)) ??
				helpers.bound((t.collegeCoachYears ?? 3) / 10, 0.05, 1),
			facilities: (t.collegeFacilities ?? prestige) / 100,
			rep: t.collegePromiseRep ?? 0.8,
			open: Math.max(0, rosterLimit - returningPlayers.length),
			returning: returningPlayers.map((p) => {
				const r = last(p.ratings);
				return { group: posGroup(r.pos), ovr: r.ovr };
			}),
			nilBudget: collegeNilBudget(prestige, nilScale),
			nilCommitted,
			visitsUsed: 0,
			// Under auto play and in spectator mode the AI runs your school too.
			auto: auto.has(t.tid) || isAiControlled(t),
			user: userTids.includes(t.tid),
		});
	}

	// Commitments already made count against scholarships and NIL budgets.
	for (const p of recruits) {
		const rec = p.recruiting!;
		if (rec.committed !== undefined) {
			const ctx = ctxs.get(rec.committed);
			if (ctx) {
				ctx.open -= 1;
				ctx.nilCommitted += rec.offers[rec.committed] ?? 0;
			}
		}
		for (const tid of rec.visits) {
			const ctx = ctxs.get(tid);
			if (ctx) {
				ctx.visitsUsed += 1;
			}
		}
	}

	return ctxs;
};

// --- Fit ---------------------------------------------------------------------

// Home state, position group and ovr, looked up once per player (AI planning
// scores every player for every school).
const infoCache = new WeakMap<
	Player,
	{ state: string | undefined; group: string; ovr: number }
>();
const playerInfo = (p: Player) => {
	let info = infoCache.get(p);
	if (!info) {
		const r = last(p.ratings);
		info = {
			state: homeState(p.born.loc),
			group: posGroup(r.pos),
			ovr: r.ovr,
		};
		infoCache.set(p, info);
	}
	return info;
};

// 0-1 for each priority: how well this school delivers it for this player.
export const collegeFits = (
	p: Player,
	ctx: TeamCtx,
	ask: number,
): Record<CollegePriority, number> => {
	const { state, group, ovr } = playerInfo(p);

	let proximity = 0.35; // From abroad, everywhere is far.
	const teamState = ctx.t.state;
	if (state && teamState) {
		proximity =
			state === teamState ? 1 : sameRegion(state, teamState) ? 0.55 : 0.1;
	}

	// Playing time: returning players at his position as good as he'll be.
	// High schoolers improve a bit before they arrive.
	const projected = ovr + (p.tid < 0 && p.collegeYear0 !== undefined ? 3 : 0);
	let better = 0;
	let atPos = 0;
	for (const r of ctx.returning) {
		if (r.group === group) {
			atPos += 1;
			if (r.ovr >= projected - 2) {
				better += 1;
			}
		}
	}
	const playingTime = helpers.bound(
		1 - 0.28 * better - 0.05 * Math.max(0, atPos - 3),
		0,
		1,
	);

	const room = ctx.nilBudget - ctx.nilCommitted;

	return {
		prestige: ctx.prestige / 100,
		winning: helpers.bound((ctx.winp - 0.2) / 0.6, 0, 1),
		proximity,
		playingTime,
		proPotential: ctx.proScore,
		nil: helpers.bound(room / Math.max(1, ask) / 3, 0, 1),
		conference: ctx.confStrength,
		coachStability: ctx.coachScore,
		facilities: ctx.facilities,
	};
};

const fitScore = (
	p: Player,
	profile: CollegeProfile,
	ctx: TeamCtx,
	ask: number,
) => {
	const fits = collegeFits(p, ctx, ask);
	let score = 0;
	for (const key of COLLEGE_PRIORITIES) {
		score += profile.weights[key] * fits[key];
	}
	return score;
};

// Interest before anything a school does: just what it is.
export const baseInterest = (p: Player, ctx: TeamCtx, ask: number) => {
	const profile = p.collegeProfile;
	const score = profile ? fitScore(p, profile, ctx, ask) : 0.45;
	// A school known for breaking promises is a harder sell to everyone.
	return 20 + 65 * score - (1 - ctx.rep) * 8;
};

const offerBonus = (p: Player, rec: CollegeRecruiting, tid: number) => {
	const nil = rec.offers[tid];
	if (nil === undefined) {
		return 0;
	}
	const ratio = nil / Math.max(1, rec.ask);
	const nilWeight =
		(0.5 + 3 * (p.collegeProfile?.weights.nil ?? 0.11)) *
		g.get("collegeNilWeight");
	return 4 + nilWeight * helpers.bound((ratio - 1) * 25, -15, 12);
};

const promiseBonus = (p: Player, rec: CollegeRecruiting, ctx: TeamCtx) => {
	const promises = rec.promises[ctx.tid];
	if (!promises) {
		return 0;
	}
	const w = p.collegeProfile?.weights;
	const pt = w?.playingTime ?? 0.11;
	const nil = w?.nil ?? 0.11;
	let bonus = 0;
	for (const promise of promises) {
		if (promise.type === "starter") {
			bonus += 3 + 25 * pt;
		} else if (promise.type === "minutes") {
			bonus += 2 + 15 * pt;
		} else if (promise.type === "nilRaise") {
			bonus += 2 + 18 * nil;
		} else if (promise.type === "noPosition") {
			bonus += 2 + 12 * pt;
		}
	}
	// Only as good as the school's word.
	return bonus * ctx.rep;
};

export const EFFORT_CAP = 25;
export const VISIT_BONUS = 6;

export const interestFor = (p: Player, ctx: TeamCtx) => {
	const rec = p.recruiting!;
	return (
		baseInterest(p, ctx, rec.ask) +
		(rec.effort[ctx.tid] ?? 0) +
		(rec.visits.includes(ctx.tid) ? VISIT_BONUS : 0) +
		offerBonus(p, rec, ctx.tid) +
		promiseBonus(p, rec, ctx) -
		(rec.talks[ctx.tid]?.penalty ?? 0) +
		// His old school knows him and he knows it.
		(rec.portalFrom === ctx.tid ? 5 : 0)
	);
};
