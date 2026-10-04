import { PHASE, PLAYER } from "../../../common/constants.ts";
import {
	collegeFinalSeason,
	collegeNilBudget,
	homeState,
	nilReaction,
	RECRUITING_HOURS_PER_WEEK,
	RECRUITING_MAX_HOURS,
	RECRUITING_VISITS,
	sameRegion,
	type CollegeRecruiting,
} from "../../../common/college.ts";
import { idb } from "../../db/index.ts";
import { g, helpers, logEvent } from "../../util/index.ts";
import { league, player } from "../index.ts";
import { getNumPlayersPerTeam } from "../league/create/createRandomPlayers.ts";
import type { Phase, Player, Team } from "../../../common/types.ts";
import { last } from "../../../common/utils.ts";
import { askForRank, starsForRank } from "./util.ts";

// RECRUITING
//
// Every high school senior (and, after the season, every transfer portal
// player) carries a CollegeRecruiting record. Schools spend weekly recruiting
// hours on the players they want, offer scholarships with NIL money attached,
// and host official visits. A player's interest in a school is:
//
//   base fit (prestige - weighted by how good he is - plus close to home,
//            this season's record, and how much playing time he'd see)
//   + accumulated effort (diminishing returns, capped)
//   + an official visit
//   + the offer: a scholarship, and NIL relative to what he's asking for
//
// Once a school with an offer pulls clearly ahead he commits, sooner for lower
// rated players. AI schools (and user schools on auto) plan weekly: they pick
// targets they can realistically land, spread their hours, and make offers
// within their NIL budget. Signing day makes it official.

const EFFORT_CAP = 40;
const COMMIT_THRESHOLD = 52;
const COMMIT_MARGIN = 5;

// --- Class setup -------------------------------------------------------------

export const newRecruiting = (
	stars: number,
	rank: number,
	ask: number,
): CollegeRecruiting => ({
	stars,
	rank,
	ask,
	effort: {},
	interest: {},
	offers: {},
	visits: [],
	hours: {},
});

// Rank a high school class and give every player his recruiting record.
export const initRecruitClass = (players: Player[]) => {
	const sorted = [...players].sort((a, b) => b.value - a.value);
	for (const [i, p] of sorted.entries()) {
		const rank = i + 1;
		const stars = starsForRank(rank, sorted.length);
		p.recruiting = newRecruiting(stars, rank, askForRank(stars, rank));
	}
};

// --- Context -----------------------------------------------------------------

type TeamCtx = {
	t: Team;
	prestige: number;
	winp: number;
	open: number;
	// Players returning next season, by position group, for playing time.
	returningByPos: Record<string, number>;
	nilBudget: number;
	nilCommitted: number;
	visitsUsed: number;
	auto: boolean;
	user: boolean;
};

const posGroup = (pos: string) =>
	pos.includes("C") || pos === "PF" || pos === "FC"
		? "big"
		: pos.includes("G")
			? "guard"
			: "wing";

export const getRecruits = async () => {
	const season = g.get("season");
	const phase = g.get("phase");
	const recruits: Player[] = [];
	if (phase <= PHASE.RESIGN_PLAYERS) {
		for (const p of await idb.cache.players.indexGetAll(
			"playersByTid",
			PLAYER.UNDRAFTED,
		)) {
			if (p.draft.year === season && p.recruiting) {
				recruits.push(p);
			}
		}
	}
	if (phase >= PHASE.DRAFT_LOTTERY) {
		for (const p of await idb.cache.players.indexGetAll(
			"playersByTid",
			PLAYER.FREE_AGENT,
		)) {
			// The portal, plus high schoolers who went unsigned.
			if (p.recruiting) {
				recruits.push(p);
			}
		}
	}
	return recruits;
};

export const getTeamCtxs = async (recruits: Player[]) => {
	const season = g.get("season");
	const userTids = g.get("userTids");
	const auto = new Set(g.get("collegeAutoRecruit") ?? []);
	const scholarships = getNumPlayersPerTeam();
	const teamSeasons = await idb.cache.teamSeasons.indexGetAll(
		"teamSeasonsBySeasonTid",
		[[season], [season, "Z"]],
	);
	const winpByTid = new Map<number, number>();
	for (const ts of teamSeasons) {
		const games = ts.won + ts.lost;
		winpByTid.set(ts.tid, games > 0 ? ts.won / games : 0.5);
	}

	const ctxs = new Map<number, TeamCtx>();
	for (const t of await idb.cache.teams.getAll()) {
		if (t.disabled) {
			continue;
		}
		const roster = await idb.cache.players.indexGetAll("playersByTid", t.tid);
		// Next season's roster: everyone who still has eligibility left.
		const returning = roster.filter((p) => {
			const final = collegeFinalSeason(p);
			return final === undefined || final > season;
		});
		const returningByPos: Record<string, number> = {};
		let nilCommitted = 0;
		for (const p of returning) {
			const group = posGroup(last(p.ratings).pos);
			returningByPos[group] = (returningByPos[group] ?? 0) + 1;
			nilCommitted += p.contract.amount;
		}
		const ctx: TeamCtx = {
			t,
			prestige: t.prestige ?? 30,
			winp: winpByTid.get(t.tid) ?? 0.5,
			open: Math.max(0, scholarships - returning.length),
			returningByPos,
			nilBudget: collegeNilBudget(t.prestige ?? 30),
			nilCommitted,
			visitsUsed: 0,
			auto: auto.has(t.tid),
			user: userTids.includes(t.tid),
		};
		ctxs.set(t.tid, ctx);
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

// --- Interest ----------------------------------------------------------------

export const baseInterest = (p: Player, ctx: TeamCtx) => {
	const rec = p.recruiting!;
	const starWeight = 0.4 + 0.2 * rec.stars;
	let score = 40 + (ctx.prestige - 50) * 0.6 * starWeight;

	const state = homeState(p.born.loc);
	const teamState = ctx.t.state;
	if (state && teamState) {
		if (state === teamState) {
			score += 10;
		} else if (sameRegion(state, teamState)) {
			score += 4;
		}
	}

	score += (ctx.winp - 0.5) * 20;

	// Playing time: fewer returning players at his position is a selling point.
	const group = posGroup(last(p.ratings).pos);
	const depth = ctx.returningByPos[group] ?? 0;
	score += helpers.bound(8 - 2 * depth, -4, 8);

	return score;
};

const offerBonus = (rec: CollegeRecruiting, tid: number) => {
	const nil = rec.offers[tid];
	if (nil === undefined) {
		return 0;
	}
	const ratio = nil / Math.max(1, rec.ask);
	return 5 + helpers.bound((ratio - 1) * 25, -25, 12);
};

export const interestFor = (p: Player, ctx: TeamCtx) => {
	const rec = p.recruiting!;
	return (
		baseInterest(p, ctx) +
		(rec.effort[ctx.t.tid] ?? 0) +
		(rec.visits.includes(ctx.t.tid) ? 8 : 0) +
		offerBonus(rec, ctx.t.tid)
	);
};

// --- AI planning -------------------------------------------------------------

type Plan = Map<number, { hours: Map<number, number>; offers: number[] }>;
let planCache: { key: string; day: number; plan: Plan } | undefined;
let dayCounter = 0;

const planKey = () => `${g.get("lid")}-${g.get("season")}-${g.get("phase")}`;

// Each AI (or auto) school picks targets it can land - weighing how good a
// player is against how interested he could get - spreads its hours over
// them, and offers the ones it wants most.
const makePlan = (recruits: Player[], ctxs: Map<number, TeamCtx>) => {
	const plan: Plan = new Map();
	for (const ctx of ctxs.values()) {
		if (ctx.user && !ctx.auto) {
			continue;
		}
		if (ctx.open <= 0) {
			plan.set(ctx.t.tid, { hours: new Map(), offers: [] });
			continue;
		}

		const scored: { p: Player; score: number }[] = [];
		for (const p of recruits) {
			const rec = p.recruiting!;
			if (rec.committed !== undefined && rec.committed !== ctx.t.tid) {
				continue;
			}
			if (rec.portalFrom === ctx.t.tid) {
				continue;
			}
			const interest = interestFor(p, ctx);
			// Chance of landing him: how his interest here compares to his best.
			const best = Math.max(0, ...Object.values(rec.interest));
			const landing = helpers.sigmoid((interest - best + 10) / 8, 1, 0);
			scored.push({ p, score: (p.value ** 2 / 100) * landing });
		}
		scored.sort((a, b) => b.score - a.score);

		const numTargets = Math.min(scored.length, ctx.open * 3);
		const targets = scored.slice(0, numTargets);
		const hours = new Map<number, number>();
		const perTarget = Math.min(
			RECRUITING_MAX_HOURS,
			RECRUITING_HOURS_PER_WEEK / Math.max(1, numTargets),
		);
		for (const { p } of targets) {
			hours.set(p.pid, perTarget);
		}
		const offers = targets.slice(0, ctx.open * 2).map(({ p }) => p.pid);
		plan.set(ctx.t.tid, { hours, offers });
	}
	return plan;
};

// --- The daily tick ----------------------------------------------------------

// Daily chance of committing, once a school is ahead: higher the more he likes
// it and the bigger its lead, lower for the best players, who take their time.
const commitChance = (rec: CollegeRecruiting, top: number, lead: number) =>
	0.02 *
	(1 + (top - COMMIT_THRESHOLD) / 15) *
	(1 + Math.min(lead, 16) / 8) *
	((6 - rec.stars) / 3);

// One day of recruiting: effort, offers and visits from every school, then
// any commitments.
export const collegeRecruitingDay = async () => {
	const recruits = await getRecruits();
	if (recruits.length === 0) {
		return;
	}
	const ctxs = await getTeamCtxs(recruits);

	dayCounter += 1;
	if (
		!planCache ||
		planCache.key !== planKey() ||
		dayCounter - planCache.day >= 7
	) {
		planCache = {
			key: planKey(),
			day: dayCounter,
			plan: makePlan(recruits, ctxs),
		};
	}
	const plan = planCache.plan;
	const byPid = new Map(recruits.map((p) => [p.pid, p]));

	// AI offers and visits.
	for (const [tid, { hours, offers }] of plan) {
		const ctx = ctxs.get(tid)!;
		for (const pid of offers) {
			const p = byPid.get(pid);
			const rec = p?.recruiting;
			if (!p || !rec || rec.offers[tid] !== undefined) {
				continue;
			}
			const room = ctx.nilBudget - ctx.nilCommitted;
			const nil = Math.round(
				helpers.bound(rec.ask * (0.85 + Math.random() * 0.35), 5, room),
			);
			if (nil >= rec.ask * 0.6 || rec.ask <= 25) {
				rec.offers[tid] = Math.max(5, nil);
			}
		}
		// Official visits for the top targets, while visits remain.
		for (const pid of hours.keys()) {
			if (ctx.visitsUsed >= RECRUITING_VISITS) {
				break;
			}
			const rec = byPid.get(pid)?.recruiting;
			if (rec && rec.offers[tid] !== undefined && !rec.visits.includes(tid)) {
				if (Math.random() < 0.05) {
					rec.visits.push(tid);
					ctx.visitsUsed += 1;
				}
			}
		}
	}

	const changed: Player[] = [];
	for (const p of recruits) {
		const rec = p.recruiting!;

		// Effort: a seventh of the weekly hours each day, diminishing returns.
		const hoursByTid = new Map<number, number>();
		for (const [tid, hours] of Object.entries(rec.hours)) {
			const ctx = ctxs.get(Number(tid));
			if (ctx && ctx.user && !ctx.auto) {
				hoursByTid.set(Number(tid), hours);
			}
		}
		for (const [tid, { hours }] of plan) {
			const h = hours.get(p.pid);
			if (h !== undefined) {
				hoursByTid.set(tid, h);
			}
		}
		for (const [tid, hours] of hoursByTid) {
			const effort = rec.effort[tid] ?? 0;
			rec.effort[tid] =
				effort + (hours / 7) * 0.08 * Math.max(0, 1 - effort / EFFORT_CAP);
		}

		// Interest for every school that has engaged.
		const engaged = new Set([
			...Object.keys(rec.effort).map(Number),
			...Object.keys(rec.offers).map(Number),
		]);
		for (const tid of engaged) {
			const ctx = ctxs.get(tid);
			if (ctx) {
				rec.interest[tid] = Math.round(interestFor(p, ctx) * 10) / 10;
			}
		}

		// Commitment: a school with an offer clearly ahead of the rest, with
		// room for him on its roster and in its NIL budget.
		if (rec.committed === undefined) {
			const offered = Object.keys(rec.offers)
				.map(Number)
				.map((tid) => ({ tid, interest: rec.interest[tid] ?? 0 }))
				.sort((a, b) => b.interest - a.interest);
			const top = offered[0];
			const second = offered[1]?.interest ?? 0;
			const ctx = top ? ctxs.get(top.tid) : undefined;
			if (
				top &&
				ctx &&
				ctx.open > 0 &&
				ctx.nilCommitted + rec.offers[top.tid]! <= ctx.nilBudget &&
				top.interest >= COMMIT_THRESHOLD &&
				top.interest - second >= COMMIT_MARGIN &&
				Math.random() < commitChance(rec, top.interest, top.interest - second)
			) {
				rec.committed = top.tid;
				ctx.open -= 1;
				ctx.nilCommitted += rec.offers[top.tid]!;
				if (ctx.user) {
					logEvent({
						type: "freeAgent",
						text: `<a href="${helpers.leagueUrl(["player", p.pid])}">${p.firstName} ${p.lastName}</a> (${rec.stars}-star) committed to the ${ctx.t.region} ${ctx.t.name}.`,
						showNotification: true,
						pids: [p.pid],
						tids: [top.tid],
						score: 10,
					});
				}
			}
		}

		changed.push(p);
	}

	await idb.cache.players.putAll(changed);

	// In the offseason, commitments sign right away.
	const phase = g.get("phase");
	if (phase >= PHASE.DRAFT_LOTTERY) {
		const committed = recruits.filter(
			(p) =>
				p.tid === PLAYER.FREE_AGENT && p.recruiting?.committed !== undefined,
		);
		if (committed.length > 0) {
			await collegeSign(committed, ctxs, phase);
		}
	}
};

// --- Signing day -------------------------------------------------------------

// Commitments become signings; uncommitted players pick the best school that
// offered and still has a scholarship, then the best school with a spot at
// all. Anyone left over walks on somewhere later, or not at all.
export const collegeSign = async (
	recruits: Player[],
	ctxs: Map<number, TeamCtx>,
	phase: Phase,
) => {
	const season = g.get("season");
	const signOne = async (p: Player, tid: number) => {
		const rec = p.recruiting!;
		const ctx = ctxs.get(tid)!;
		const nil = rec.offers[tid] ?? Math.min(rec.ask, 5);
		ctx.open -= 1;
		ctx.nilCommitted += nil;
		if (p.collegeYear0 === undefined) {
			p.collegeYear0 = season + 1;
		}
		await player.sign(p, tid, { amount: nil, exp: season + 4 }, phase);
		delete p.recruiting;
		await idb.cache.players.put(p);
	};

	// Committed players first; their spot and money were counted already.
	for (const p of recruits) {
		const tid = p.recruiting!.committed;
		if (tid !== undefined) {
			const ctx = ctxs.get(tid);
			if (ctx) {
				ctx.open += 1;
				ctx.nilCommitted -= p.recruiting!.offers[tid] ?? 0;
				await signOne(p, tid);
			}
		}
	}

	const remaining = recruits
		.filter((p) => p.recruiting !== undefined)
		.sort((a, b) => b.value - a.value);
	for (const p of remaining) {
		const rec = p.recruiting!;
		const options = Object.keys(rec.offers)
			.map(Number)
			.filter((tid) => {
				const ctx = ctxs.get(tid);
				return ctx && ctx.open > 0;
			})
			.sort((a, b) => (rec.interest[b] ?? 0) - (rec.interest[a] ?? 0));
		const tid = options[0];
		if (tid !== undefined) {
			await signOne(p, tid);
		}
	}
};

// High school signing day, at the end of the season.
export const collegeSigningDay = async () => {
	const season = g.get("season");
	const recruits = (await getRecruits()).filter(
		(p) => p.tid === PLAYER.UNDRAFTED && p.draft.year === season,
	);
	const ctxs = await getTeamCtxs(recruits);
	await collegeSign(recruits, ctxs, PHASE.RESIGN_PLAYERS);
};

// --- User actions ------------------------------------------------------------

export type RecruitAction =
	| { type: "hours"; pid: number; hours: number }
	| { type: "offer"; pid: number; nil: number }
	| { type: "pull"; pid: number }
	| { type: "visit"; pid: number };

// Returns an error message, or undefined on success.
export const collegeRecruitAction = async (action: RecruitAction) => {
	const userTid = g.get("userTid");
	const recruits = await getRecruits();
	const p = recruits.find((p2) => p2.pid === action.pid);
	if (!p || !p.recruiting) {
		return "That player isn't being recruited.";
	}
	const rec = p.recruiting;
	const ctxs = await getTeamCtxs(recruits);
	const ctx = ctxs.get(userTid);
	if (!ctx) {
		return "No team.";
	}

	if (action.type === "hours") {
		const hours = helpers.bound(
			Math.round(action.hours),
			0,
			RECRUITING_MAX_HOURS,
		);
		let used = 0;
		for (const p2 of recruits) {
			if (p2.pid !== p.pid) {
				used += p2.recruiting!.hours[userTid] ?? 0;
			}
		}
		if (used + hours > RECRUITING_HOURS_PER_WEEK) {
			return `You only have ${RECRUITING_HOURS_PER_WEEK - used} hours left this week.`;
		}
		if (hours === 0) {
			delete rec.hours[userTid];
		} else {
			rec.hours[userTid] = hours;
		}
	} else if (action.type === "offer") {
		const nil = Math.max(0, Math.round(action.nil));
		const current = rec.offers[userTid] ?? 0;
		const committedHere = rec.committed === userTid ? current : 0;
		const room = ctx.nilBudget - (ctx.nilCommitted - committedHere);
		if (nil > room) {
			return `That's more than the ${helpers.formatCurrency(room / 1000, "M")} left in your NIL budget.`;
		}
		rec.offers[userTid] = nil;
		if (rec.committed === userTid && nil < current) {
			// Cutting a committed player's NIL is a gamble.
			if (nilReaction(rec, nil) === "insulted") {
				delete rec.committed;
			}
		}
	} else if (action.type === "pull") {
		delete rec.offers[userTid];
		if (rec.committed === userTid) {
			delete rec.committed;
		}
	} else if (action.type === "visit") {
		if (rec.visits.includes(userTid)) {
			return "He has already visited.";
		}
		if (rec.offers[userTid] === undefined) {
			return "Offer him a scholarship before inviting him on a visit.";
		}
		if (ctx.visitsUsed >= RECRUITING_VISITS) {
			return `You've used all ${RECRUITING_VISITS} official visits.`;
		}
		rec.visits.push(userTid);
	}

	// Refresh his interest in your school right away.
	rec.interest[userTid] = Math.round(interestFor(p, ctx) * 10) / 10;
	await idb.cache.players.put(p);
	return undefined;
};

export const collegeSetAutoRecruit = async (auto: boolean) => {
	const userTid = g.get("userTid");
	const current = new Set(g.get("collegeAutoRecruit") ?? []);
	if (auto) {
		current.add(userTid);
	} else {
		current.delete(userTid);
	}
	await league.setGameAttributes({ collegeAutoRecruit: [...current] });
	planCache = undefined;
};
