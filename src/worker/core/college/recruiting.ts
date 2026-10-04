import { PHASE, PLAYER } from "../../../common/constants.ts";
import {
	RECRUITING_HOURS_PER_WEEK,
	RECRUITING_MAX_HOURS,
	RECRUITING_VISITS,
	type CollegePromise,
	type CollegePromiseType,
	type CollegeRecruiting,
} from "../../../common/college.ts";
import { idb } from "../../db/index.ts";
import { g, helpers, logEvent } from "../../util/index.ts";
import { league, player } from "../index.ts";
import type { Phase, Player } from "../../../common/types.ts";
import { last } from "../../../common/utils.ts";
import { realGauss } from "../../../common/random.ts";
import { askForRank, starsForRank } from "./util.ts";
import { genCollegeProfile } from "./profile.ts";
import {
	nilRange,
	openTalks,
	respondToOffer,
	roundNil,
	type OfferOutcome,
} from "./negotiation.ts";
import {
	collegeFits,
	EFFORT_CAP,
	getTeamCtxs,
	interestFor,
	posGroup,
	type TeamCtx,
} from "./teams.ts";

// RECRUITING
//
// Every high school senior (and, in the offseason, every transfer portal
// player) carries a CollegeRecruiting record. Schools spend weekly recruiting
// hours on the players they want - which also scouts them - offer
// scholarships with NIL money (negotiated: see negotiation.ts) and promises
// attached, and host official visits.
//
// A player's interest in a school is how well it fits what he cares about
// (his own mix of priorities: see teams.ts), plus the effort it has put in,
// a visit, its offer and its promises. Once a school with an offer pulls
// clearly ahead he commits - the best players take longest - and a commit can
// still flip if another school gets well ahead before he signs.
//
// AI schools play by the same rules: same hours, visits, budgets, the same
// haggling with the same partial information. The difficulty setting decides
// how sharp they are about it.
//
// High schoolers sign on signing day, after the offseason recruiting weeks.
// Portal players sign as soon as they commit.

const COMMIT_MARGIN = 5;
const DECOMMIT_MARGIN = 10;

const difficulty = () => g.get("collegeRecruitingDifficulty");
const commitThreshold = () => 60 + 4 * (difficulty() - 1);

// --- Class setup -------------------------------------------------------------

export const newRecruiting = (
	stars: number,
	rank: number,
	ask: number,
): CollegeRecruiting => ({
	stars,
	rank,
	ask,
	askRange: nilRange(ask),
	fuzz: helpers.bound(realGauss(0, 4), -10, 10),
	scout: {},
	effort: {},
	interest: {},
	offers: {},
	talks: {},
	promises: {},
	visits: [],
	hours: {},
});

// Rank a high school class and give every player his recruiting record.
export const initRecruitClass = (players: Player[]) => {
	const nilScale = g.get("collegeNilScale");
	const sorted = [...players].sort((a, b) => b.value - a.value);
	for (const [i, p] of sorted.entries()) {
		const rank = i + 1;
		const stars = starsForRank(rank, sorted.length);
		p.collegeProfile ??= genCollegeProfile(stars);
		p.recruiting = newRecruiting(
			stars,
			rank,
			roundNil(askForRank(stars, rank) * nilScale),
		);
	}
};

// Everyone being recruited right now: this year's high school class, plus
// the transfer portal during the offseason recruiting weeks.
export const getRecruits = async () => {
	const season = g.get("season");
	const recruits: Player[] = [];
	for (const p of await idb.cache.players.indexGetAll(
		"playersByTid",
		PLAYER.UNDRAFTED,
	)) {
		if (p.draft.year === season && p.recruiting) {
			recruits.push(p);
		}
	}
	if (g.get("phase") === PHASE.FREE_AGENCY) {
		for (const p of await idb.cache.players.indexGetAll(
			"playersByTid",
			PLAYER.FREE_AGENT,
		)) {
			if (p.recruiting) {
				recruits.push(p);
			}
		}
	}
	return recruits;
};

// --- AI planning -------------------------------------------------------------

type Plan = Map<
	number,
	{ hours: Map<number, number>; offers: number[]; maxPay: Map<number, number> }
>;
let planCache: { key: string; day: number; plan: Plan } | undefined;
let dayCounter = 0;

const planKey = () => `${g.get("lid")}-${g.get("season")}-${g.get("phase")}`;

// Each AI (or auto) school picks targets it can land - weighing how good a
// player is against how it stacks up with the other schools that could want
// him - spreads its hours over them, and offers the ones it wants most.
const makePlan = (recruits: Player[], ctxs: Map<number, TeamCtx>) => {
	const plan: Plan = new Map();
	const shopping = [...ctxs.values()].filter((ctx) => ctx.open > 0);
	// Sloppier target picking on easier settings.
	const noise = Math.max(0, 0.5 * (1.5 - difficulty()));

	// For each player, the interest of the best three schools that could go
	// after him. A school well behind the second best of the others is
	// unlikely to land him, so the best players are fought over by the best
	// programs and everyone else looks further down the board.
	const rival = new Map<number, number[]>();
	for (const p of recruits) {
		const rec = p.recruiting!;
		if (rec.committed !== undefined) {
			continue;
		}
		const top3 = [-Infinity, -Infinity, -Infinity];
		for (const ctx of shopping) {
			if (rec.talks[ctx.tid]?.walked) {
				continue;
			}
			const x = interestFor(p, ctx);
			if (x > top3[2]!) {
				top3[2] = x;
				top3.sort((a, b) => b - a);
			}
		}
		rival.set(p.pid, top3);
	}

	for (const ctx of ctxs.values()) {
		if (ctx.user && !ctx.auto) {
			continue;
		}
		if (ctx.open <= 0) {
			plan.set(ctx.tid, { hours: new Map(), offers: [], maxPay: new Map() });
			continue;
		}

		const scored: { p: Player; score: number }[] = [];
		for (const p of recruits) {
			const rec = p.recruiting!;
			if (rec.committed !== undefined && rec.committed !== ctx.tid) {
				continue;
			}
			if (rec.talks[ctx.tid]?.walked) {
				continue;
			}
			let landing = 1;
			if (rec.committed === undefined) {
				const interest = interestFor(p, ctx);
				const top3 = rival.get(p.pid)!;
				const ref = interest >= top3[1]! ? top3[2]! : top3[1]!;
				landing = helpers.sigmoid((interest - ref + 4) / 4, 1, 0);
			}
			const jitter = noise > 0 ? Math.exp(realGauss(0, noise)) : 1;
			scored.push({ p, score: (p.value ** 2 / 100) * landing * jitter });
		}
		scored.sort((a, b) => b.score - a.score);

		const numTargets = Math.min(scored.length, ctx.open * 3);
		const targets = scored.slice(0, numTargets);
		const hours = new Map<number, number>();
		const perTarget = Math.min(
			RECRUITING_MAX_HOURS,
			RECRUITING_HOURS_PER_WEEK / Math.max(1, numTargets),
		);
		const maxPay = new Map<number, number>();
		for (const { p } of targets) {
			hours.set(p.pid, perTarget);
			// The most it will pay, judging by what it can see of his asking
			// range.
			maxPay.set(
				p.pid,
				roundNil(p.recruiting!.askRange[1] * (1 + 0.1 * difficulty())),
			);
		}
		const offers = targets.slice(0, ctx.open * 2).map(({ p }) => p.pid);
		plan.set(ctx.tid, { hours, offers, maxPay });
	}
	return plan;
};

// An AI school's promises: what it can honestly offer a player who cares
// about playing time.
const aiPromises = (p: Player, ctx: TeamCtx): CollegePromise[] => {
	const rec = p.recruiting!;
	const pt = p.collegeProfile?.weights.playingTime ?? 0;
	const fit = collegeFits(p, ctx, rec.ask).playingTime;
	if (pt >= 0.3 && fit >= 0.85) {
		return [{ type: "starter", tid: ctx.tid, season: 0, value: 60 }];
	}
	if (pt >= 0.2 && fit >= 0.7) {
		return [{ type: "minutes", tid: ctx.tid, season: 0, value: 15 }];
	}
	return [];
};

// Haggle like the user does: an opening offer judged from the range, then
// meet his counter if it's affordable.
const aiNegotiate = (p: Player, ctx: TeamCtx, maxPay: number) => {
	const rec = p.recruiting!;
	const room = ctx.nilBudget - ctx.nilCommitted;
	const cap = Math.min(maxPay, room);
	const talks = (rec.talks[ctx.tid] ??= openTalks(
		p.collegeProfile,
		interestFor(p, ctx),
		Object.keys(rec.offers).length,
	));
	const [lo, hi] = rec.askRange;
	if (cap < lo * 0.85) {
		return;
	}
	const mid = (lo + hi) / 2;
	// Sharper opening offers on harder settings.
	const sharpness = helpers.bound(difficulty(), 0.5, 1.5);
	let amount = roundNil(mid * (0.86 + 0.08 * sharpness + 0.06 * Math.random()));
	for (let round = 0; round < 3; round++) {
		amount = Math.min(amount, cap);
		const outcome = respondToOffer(talks, rec.ask, amount);
		if (outcome.type === "accepted") {
			rec.offers[ctx.tid] = outcome.amount;
			const promises = aiPromises(p, ctx);
			if (promises.length > 0) {
				rec.promises[ctx.tid] = promises;
			}
			return;
		}
		if (outcome.type === "walked") {
			return;
		}
		if (outcome.counter > cap) {
			return;
		}
		amount = outcome.counter;
	}
};

// --- The daily tick ----------------------------------------------------------

// Daily chance of committing, once a school is ahead: higher the more he likes
// it and the bigger its lead, lower for the best players, who take their time.
const commitChance = (rec: CollegeRecruiting, top: number, lead: number) =>
	(0.02 *
		(1 + (top - commitThreshold()) / 15) *
		(1 + Math.min(lead, 16) / 8) *
		((6 - rec.stars) / 3)) /
	difficulty();

const teamName = (ctx: TeamCtx) => `${ctx.t.region} ${ctx.t.name}`;

const playerLink = (p: Player) =>
	`<a href="${helpers.leagueUrl(["player", p.pid])}">${p.firstName} ${p.lastName}</a>`;

const canAfford = (ctx: TeamCtx, amount: number) =>
	ctx.open > 0 && ctx.nilCommitted + amount <= ctx.nilBudget;

// One day of recruiting: scouting and effort from every school, AI offers and
// visits, then commitments and flips.
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
	for (const [tid, { hours, offers, maxPay }] of plan) {
		const ctx = ctxs.get(tid)!;
		for (const pid of offers) {
			const p = byPid.get(pid);
			const rec = p?.recruiting;
			if (
				!p ||
				!rec ||
				rec.offers[tid] !== undefined ||
				rec.talks[tid]?.walked ||
				(rec.committed !== undefined && rec.committed !== tid)
			) {
				continue;
			}
			aiNegotiate(p, ctx, maxPay.get(pid) ?? 0);
		}
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

	for (const p of recruits) {
		const rec = p.recruiting!;

		// A seventh of the weekly hours each day: scouting, and effort with
		// diminishing returns.
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
			if (rec.talks[tid]?.walked) {
				continue;
			}
			rec.scout[tid] =
				Math.round(((rec.scout[tid] ?? 0) + hours / 7) * 10) / 10;
			const effort = rec.effort[tid] ?? 0;
			rec.effort[tid] =
				effort + (hours / 7) * 0.07 * Math.max(0, 1 - effort / EFFORT_CAP);
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

		const offered = Object.keys(rec.offers)
			.map(Number)
			.map((tid) => ({ tid, interest: rec.interest[tid] ?? 0 }))
			.sort((a, b) => b.interest - a.interest);

		if (rec.committed === undefined) {
			// Commitment: a school with an offer clearly ahead of the rest,
			// with room for him on its roster and in its NIL budget.
			const top = offered[0];
			const second = offered[1]?.interest ?? 0;
			const ctx = top ? ctxs.get(top.tid) : undefined;
			if (
				top &&
				ctx &&
				canAfford(ctx, rec.offers[top.tid]!) &&
				top.interest >= commitThreshold() &&
				top.interest - second >= COMMIT_MARGIN &&
				Math.random() < commitChance(rec, top.interest, top.interest - second)
			) {
				rec.committed = top.tid;
				ctx.open -= 1;
				ctx.nilCommitted += rec.offers[top.tid]!;
				if (ctx.user) {
					logEvent({
						type: "freeAgent",
						text: `${playerLink(p)} (${rec.stars}-star) committed to the ${teamName(ctx)}.`,
						showNotification: true,
						pids: [p.pid],
						tids: [top.tid],
						score: 10,
					});
				}
			}
		} else {
			// A flip: another school with an offer gets well ahead.
			const current = ctxs.get(rec.committed);
			const currentInterest = rec.interest[rec.committed] ?? 0;
			const challenger = offered.find((row) => row.tid !== rec.committed);
			const ctx = challenger ? ctxs.get(challenger.tid) : undefined;
			if (
				current &&
				challenger &&
				ctx &&
				challenger.interest >= currentInterest + DECOMMIT_MARGIN &&
				canAfford(ctx, rec.offers[challenger.tid]!) &&
				Math.random() < 0.03
			) {
				current.open += 1;
				current.nilCommitted -= rec.offers[current.tid] ?? 0;
				rec.committed = challenger.tid;
				ctx.open -= 1;
				ctx.nilCommitted += rec.offers[challenger.tid]!;
				if (current.user || ctx.user) {
					logEvent({
						type: "freeAgent",
						text: `${playerLink(p)} (${rec.stars}-star) flipped his commitment from the ${teamName(current)} to the ${teamName(ctx)}.`,
						showNotification: true,
						pids: [p.pid],
						tids: [current.user ? current.tid : ctx.tid],
						score: 10,
					});
				}
			}
		}
	}

	await idb.cache.players.putAll(recruits);

	// In the offseason, portal commitments sign right away.
	if (g.get("phase") === PHASE.FREE_AGENCY) {
		const committed = recruits.filter(
			(p) =>
				p.tid === PLAYER.FREE_AGENT && p.recruiting?.committed !== undefined,
		);
		if (committed.length > 0) {
			await collegeSign(committed, ctxs, PHASE.FREE_AGENCY);
		}
	}
};

// --- Signing -----------------------------------------------------------------

// Commitments become signings; uncommitted players pick the school they like
// best among those that offered and can still fit him. Anyone left over is a
// walk-on, if anyone has room for him later.
export const collegeSign = async (
	recruits: Player[],
	ctxs: Map<number, TeamCtx>,
	phase: Phase,
) => {
	const season = g.get("season");
	const signOne = async (p: Player, tid: number) => {
		const rec = p.recruiting!;
		const ctx = ctxs.get(tid)!;
		const nil = rec.offers[tid] ?? 0;
		ctx.open -= 1;
		ctx.nilCommitted += nil;
		if (p.collegeYear0 === undefined) {
			p.collegeYear0 = season + 1;
		}
		const promises = rec.promises[tid];
		if (promises) {
			p.collegePromises = [
				...(p.collegePromises ?? []),
				...promises.map((promise) => ({
					...promise,
					tid,
					season: season + 1,
					value: promise.type === "noPosition" ? p.draft.year : promise.value,
				})),
			];
		}
		await player.sign(p, tid, { amount: nil, exp: season + 4 }, phase);
		delete p.recruiting;
		await idb.cache.players.put(p);
		if (ctx.user && phase === PHASE.FREE_AGENCY) {
			logEvent({
				type: "freeAgent",
				text: `${playerLink(p)} transferred to the ${teamName(ctx)}.`,
				showNotification: false,
				pids: [p.pid],
				tids: [tid],
				score: 0,
			});
		}
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
				return ctx && canAfford(ctx, rec.offers[tid]!);
			})
			.sort((a, b) => (rec.interest[b] ?? 0) - (rec.interest[a] ?? 0));
		const tid = options[0];
		if (tid !== undefined) {
			await signOne(p, tid);
		}
	}
};

// A "won't sign anyone else at his position" promise is broken by signing
// someone else at his position in his class.
const checkNoPositionPromises = async (signed: Player[]) => {
	const byTid = Map.groupBy(signed, (p) => p.tid);
	for (const [, players] of byTid) {
		for (const p of players) {
			for (const promise of p.collegePromises ?? []) {
				if (promise.type !== "noPosition" || promise.status) {
					continue;
				}
				const group = posGroup(last(p.ratings).pos);
				const broken = players.some(
					(other) =>
						other !== p &&
						other.draft.year === promise.value &&
						posGroup(last(other.ratings).pos) === group,
				);
				promise.status = broken ? "broken" : "kept";
				await idb.cache.players.put(p);
				await applyPromiseResult(p, promise);
			}
		}
	}
};

// Reputation for keeping promises, and how a broken one sits with him.
export const applyPromiseResult = async (
	p: Player,
	promise: CollegePromise,
) => {
	const t = await idb.cache.teams.get(promise.tid);
	if (!t) {
		return;
	}
	const rep = t.collegePromiseRep ?? 0.8;
	t.collegePromiseRep =
		Math.round(
			helpers.bound(
				promise.status === "broken" ? rep - 0.1 : rep + 0.03,
				0.2,
				1,
			) * 1000,
		) / 1000;
	await idb.cache.teams.put(t);
	if (promise.status === "broken" && g.get("userTids").includes(t.tid)) {
		logEvent({
			type: "freeAgent",
			text: `You broke your promise to ${playerLink(p)} (${promiseText(promise)}).`,
			showNotification: true,
			pids: [p.pid],
			tids: [t.tid],
			score: 10,
		});
	}
};

export const promiseText = (promise: CollegePromise) =>
	promise.type === "starter"
		? "starter"
		: promise.type === "minutes"
			? `${promise.value ?? 15}+ minutes`
			: promise.type === "nilRaise"
				? "NIL raise"
				: "no one else at his position";

// High school signing day, at the end of the offseason recruiting weeks.
export const collegeSigningDay = async () => {
	const season = g.get("season");
	const recruits = (await getRecruits()).filter(
		(p) => p.tid === PLAYER.UNDRAFTED && p.draft.year === season,
	);
	const ctxs = await getTeamCtxs(recruits);
	await collegeSign(recruits, ctxs, PHASE.FREE_AGENCY);

	const signed = recruits.filter((p) => p.tid >= 0);
	await checkNoPositionPromises(signed);

	// The rest are walk-ons now.
	for (const p of recruits) {
		if (p.tid === PLAYER.UNDRAFTED) {
			p.tid = PLAYER.FREE_AGENT;
			p.contract = { amount: 0, exp: season + 1 };
			delete p.recruiting;
			await idb.cache.players.put(p);
		}
	}
};

// --- User actions ------------------------------------------------------------

export type RecruitAction =
	| { type: "hours"; pid: number; hours: number }
	| {
			type: "offer";
			pid: number;
			nil: number;
			promises: { type: CollegePromiseType; value?: number }[];
	  }
	| { type: "pull"; pid: number }
	| { type: "visit"; pid: number };

export type RecruitActionResult = {
	error?: string;
	outcome?: OfferOutcome;
};

export const collegeRecruitAction = async (
	action: RecruitAction,
): Promise<RecruitActionResult> => {
	const userTid = g.get("userTid");
	const recruits = await getRecruits();
	const p = recruits.find((p2) => p2.pid === action.pid);
	if (!p || !p.recruiting) {
		return { error: "That player isn't being recruited." };
	}
	const rec = p.recruiting;
	const ctxs = await getTeamCtxs(recruits);
	const ctx = ctxs.get(userTid);
	if (!ctx) {
		return { error: "No team." };
	}
	if (rec.talks[userTid]?.walked) {
		return { error: "He's done talking to you." };
	}

	let outcome: OfferOutcome | undefined;
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
			return {
				error: `You only have ${RECRUITING_HOURS_PER_WEEK - used} hours left this week.`,
			};
		}
		if (hours === 0) {
			delete rec.hours[userTid];
		} else {
			rec.hours[userTid] = hours;
		}
	} else if (action.type === "offer") {
		const nil = roundNil(action.nil);
		const current = rec.offers[userTid];
		const committedHere = rec.committed === userTid ? (current ?? 0) : 0;
		const room = ctx.nilBudget - (ctx.nilCommitted - committedHere);
		if (nil > room) {
			return {
				error: `That's more than the ${helpers.formatCurrency(room / 1000, "M")} left in your NIL budget.`,
			};
		}
		const promises = action.promises.map((promise) => ({
			type: promise.type,
			tid: userTid,
			season: 0,
			value: promise.type === "minutes" ? (promise.value ?? 15) : promise.value,
		}));

		if (current !== undefined && nil >= current) {
			// A raise on an offer he already accepted.
			rec.offers[userTid] = nil;
			outcome = { type: "accepted", amount: nil };
		} else if (current !== undefined) {
			// Cutting his money: he decommits, and doesn't forget it.
			const talks = (rec.talks[userTid] ??= openTalks(
				p.collegeProfile,
				interestFor(p, ctx),
				0,
			));
			talks.penalty += 5;
			if (rec.committed === userTid) {
				delete rec.committed;
			}
			delete rec.offers[userTid];
			outcome = respondToOffer(talks, rec.ask, nil);
			if (outcome.type === "accepted") {
				rec.offers[userTid] = outcome.amount;
			}
		} else {
			const talks = (rec.talks[userTid] ??= openTalks(
				p.collegeProfile,
				interestFor(p, ctx),
				Object.keys(rec.offers).length,
			));
			outcome = respondToOffer(talks, rec.ask, nil);
			if (outcome.type === "accepted") {
				rec.offers[userTid] = outcome.amount;
			} else if (outcome.type === "walked") {
				delete rec.hours[userTid];
			}
		}
		if (rec.offers[userTid] !== undefined) {
			if (promises.length > 0) {
				rec.promises[userTid] = promises;
			} else {
				delete rec.promises[userTid];
			}
		}
	} else if (action.type === "pull") {
		delete rec.offers[userTid];
		delete rec.promises[userTid];
		if (rec.committed === userTid) {
			delete rec.committed;
			const talks = (rec.talks[userTid] ??= openTalks(p.collegeProfile, 0, 0));
			talks.penalty += 5;
		}
	} else if (action.type === "visit") {
		if (rec.visits.includes(userTid)) {
			return { error: "He has already visited." };
		}
		if (rec.offers[userTid] === undefined) {
			return {
				error: "Offer him a scholarship before inviting him on a visit.",
			};
		}
		if (ctx.visitsUsed >= RECRUITING_VISITS) {
			return { error: `You've used all ${RECRUITING_VISITS} official visits.` };
		}
		rec.visits.push(userTid);
	}

	// Refresh his interest in your school right away.
	rec.interest[userTid] = Math.round(interestFor(p, ctx) * 10) / 10;
	await idb.cache.players.put(p);
	return { outcome };
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
