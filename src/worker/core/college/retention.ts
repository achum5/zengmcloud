import {
	collegeFinalSeason,
	type CollegeRetention,
	type CollegePromiseType,
} from "../../../common/college.ts";
import { idb } from "../../db/index.ts";
import { g, helpers } from "../../util/index.ts";
import type { Player } from "../../../common/types.ts";
import { collegeNilForPercentile } from "./util.ts";
import {
	nilRange,
	openTalks,
	respondToOffer,
	roundNil,
} from "./negotiation.ts";
import type { OfferOutcome } from "./negotiation.ts";
import { applyPromiseResult } from "./recruiting.ts";
import { getTeamCtxs } from "./teams.ts";

// THE RETENTION PERIOD
//
// After the season, before the transfer portal opens, every returning player
// takes stock. He renegotiates his NIL (players who improved want raises;
// players who were paid over their value expect raises anyway), and he
// decides how likely he is to leave: guys who barely played, guys whose NIL
// talks went badly, guys whose promises were broken, standouts at small
// programs, players on losing teams. Schools get one chance to change their
// minds - an NIL raise or a playing time promise - and then the portal opens.

// A new NIL deal for next season on: the contract and the salary rows ahead.
const setNil = (p: Player, amount: number) => {
	const season = g.get("season");
	p.contract.amount = amount;
	for (const row of p.salaries) {
		if (row.season > season) {
			row.amount = amount;
		}
	}
};

// Regular season line for a season.
export const seasonLine = (p: Player, season: number) => {
	let gp = 0;
	let gs = 0;
	let min = 0;
	for (const row of p.stats) {
		if (row.season === season && !row.playoffs && row.tid === p.tid) {
			gp += row.gp ?? 0;
			gs += row.gs ?? 0;
			min += row.min ?? 0;
		}
	}
	return { gp, gs, min, mpg: gp > 0 ? min / gp : 0 };
};

type RiskInput = {
	p: Player;
	mpg: number;
	pct: number; // value percentile among college players, 0 = best
	prestige: number;
	winp: number;
	underpaid: boolean;
	brokenPromise: boolean;
	promisedTime: number; // 0, or 0.7 for minutes / 1 for starter, times rep
	lastYear: boolean;
};

// Chance he enters the portal, and why.
const portalRisk = (input: RiskInput) => {
	const reasons: string[] = [];
	let risk = 0.11;

	let minutes = 0;
	if (input.mpg < 8) {
		minutes = 0.32;
	} else if (input.mpg < 15) {
		minutes = 0.18;
	} else if (input.mpg < 22) {
		minutes = 0.06;
	}
	minutes *= 1 - input.promisedTime;
	if (minutes >= 0.06) {
		reasons.push("Wants more minutes");
	}
	risk += minutes;

	if (input.underpaid) {
		risk += 0.12;
		reasons.push("Wants a raise");
	}
	if (input.brokenPromise) {
		risk += 0.35;
		reasons.push("Broken promise");
	}
	if (input.pct < 0.1 && input.prestige < 50) {
		risk += 0.15 * ((50 - input.prestige) / 25);
		reasons.push("Wants a bigger stage");
	}
	if (input.winp < 0.4) {
		risk += 0.08;
		reasons.push("Losing season");
	}
	if (input.lastYear) {
		risk += 0.04;
	}

	return {
		risk: helpers.bound(risk * g.get("collegePortalRate"), 0, 0.95),
		reasons,
	};
};

const valuePercentiles = async () => {
	const players = await idb.cache.players.indexGetAll("playersByTid", [
		0,
		Infinity,
	]);
	players.sort((a, b) => b.value - a.value);
	return new Map(players.map((p, i) => [p.pid, i / players.length]));
};

const riskInputFor = async (
	p: Player,
	pctByPid: Map<number, number>,
	season: number,
) => {
	const t = await idb.cache.teams.get(p.tid);
	const ts = await idb.cache.teamSeasons.indexGet("teamSeasonsByTidSeason", [
		p.tid,
		season,
	]);
	const games = ts ? ts.won + ts.lost : 0;
	const r = p.collegeRetention!;
	const rep = t?.collegePromiseRep ?? 0.8;
	let promisedTime = 0;
	for (const promise of p.collegePromises ?? []) {
		if (promise.season === season + 1 && promise.tid === p.tid) {
			if (promise.type === "starter") {
				promisedTime = Math.max(promisedTime, rep);
			} else if (promise.type === "minutes") {
				promisedTime = Math.max(promisedTime, 0.7 * rep);
			}
		}
	}
	promisedTime = Math.min(1, promisedTime * g.get("collegeRetentionEase"));
	return {
		p,
		mpg: seasonLine(p, season).mpg,
		pct: pctByPid.get(p.pid) ?? 0.5,
		prestige: t?.prestige ?? 30,
		winp: games > 0 ? ts!.won / games : 0.5,
		underpaid: !r.settled && r.demand > p.contract.amount * 1.15,
		brokenPromise: (p.collegePromises ?? []).some(
			(promise) => promise.season === season && promise.status === "broken",
		),
		promisedTime,
		lastYear: collegeFinalSeason(p) === season + 1,
	};
};

const refreshRisk = async (
	p: Player,
	pctByPid: Map<number, number>,
	season: number,
) => {
	const { risk, reasons } = portalRisk(await riskInputFor(p, pctByPid, season));
	p.collegeRetention!.risk = Math.round(risk * 1000) / 1000;
	p.collegeRetention!.reasons = reasons;
};

// Start of the retention period: demands and portal risk for everyone coming
// back, and the AI schools make their moves.
export const collegeStartRetention = async () => {
	const season = g.get("season");
	const nilScale = g.get("collegeNilScale");
	const pctByPid = await valuePercentiles();
	const players = await idb.cache.players.indexGetAll("playersByTid", [
		0,
		Infinity,
	]);

	for (const p of players) {
		const final = collegeFinalSeason(p);
		if (final !== undefined && final <= season) {
			continue;
		}
		const pct = pctByPid.get(p.pid) ?? 0.5;
		const current = p.contract.amount;
		const fair = roundNil(collegeNilForPercentile(pct) * nilScale);
		// Paid over his value? He expects raises anyway.
		const overpaid = current / Math.max(1, fair);
		const expected =
			overpaid > 1
				? roundNil(
						current *
							(1 + helpers.bound(0.05 + 0.1 * (overpaid - 1), 0.05, 0.2)),
					)
				: fair;
		let demand = Math.max(fair, expected);

		// A promised raise is paid automatically.
		const raise = (p.collegePromises ?? []).find(
			(promise) =>
				promise.type === "nilRaise" &&
				promise.season === season &&
				!promise.status,
		);
		let settled = demand <= current * 1.05;
		if (raise) {
			raise.status = "kept";
			const amount = Math.max(roundNil(current * 1.15), demand);
			setNil(p, amount);
			demand = amount;
			settled = true;
		}

		p.collegeRetention = {
			season,
			demand,
			demandRange: nilRange(demand),
			risk: 0,
			reasons: [],
			...(settled ? { settled: true as const } : {}),
		};
		await refreshRisk(p, pctByPid, season);
		await idb.cache.players.put(p);
	}

	await aiRetention(pctByPid);
};

// AI schools: pay the raises they can afford, best players first, and
// promise minutes to good players who want them.
const aiRetention = async (pctByPid: Map<number, number>) => {
	const season = g.get("season");
	const ctxs = await getTeamCtxs([]);
	for (const ctx of ctxs.values()) {
		if (ctx.user && !ctx.auto) {
			continue;
		}
		const roster = (
			await idb.cache.players.indexGetAll("playersByTid", ctx.tid)
		)
			.filter((p) => p.collegeRetention?.season === season)
			.sort((a, b) => b.value - a.value);
		for (const [i, p] of roster.entries()) {
			const r = p.collegeRetention!;
			if (!r.settled && i < 10) {
				const room = ctx.nilBudget - ctx.nilCommitted;
				const increase = r.demandRange[1] * 1.1 - p.contract.amount;
				if (increase <= room) {
					r.talks ??= openTalks(p.collegeProfile, 70, 0);
					let amount = roundNil((r.demandRange[0] + r.demandRange[1]) / 2);
					for (let round = 0; round < 3; round++) {
						const outcome = respondToOffer(r.talks, r.demand, amount);
						if (outcome.type === "accepted") {
							ctx.nilCommitted += outcome.amount - p.contract.amount;
							setNil(p, outcome.amount);
							r.settled = true;
							break;
						}
						if (outcome.type === "walked") {
							break;
						}
						amount = outcome.counter;
					}
				}
			}
			if (i < 8 && r.reasons.includes("Wants more minutes")) {
				p.collegePromises = [
					...(p.collegePromises ?? []),
					{ type: "minutes", tid: ctx.tid, season: season + 1, value: 15 },
				];
			}
			await refreshRisk(p, pctByPid, season);
			await idb.cache.players.put(p);
		}
	}
};

// --- User actions ------------------------------------------------------------

export type RetentionAction =
	| { type: "nil"; pid: number; nil: number }
	| {
			type: "promise";
			pid: number;
			promise: CollegePromiseType;
			value?: number;
	  };

export const collegeRetentionAction = async (
	action: RetentionAction,
): Promise<{ error?: string; outcome?: OfferOutcome }> => {
	const season = g.get("season");
	const userTid = g.get("userTid");
	const p = await idb.cache.players.get(action.pid);
	if (!p || p.tid !== userTid || p.collegeRetention?.season !== season) {
		return { error: "He isn't on your roster for next season." };
	}
	const r: CollegeRetention = p.collegeRetention;

	let outcome: OfferOutcome | undefined;
	if (action.type === "nil") {
		if (r.settled) {
			return { error: "His deal is already settled." };
		}
		if (r.talks?.walked) {
			return { error: "He's done talking." };
		}
		const ctx = (await getTeamCtxs([])).get(userTid)!;
		const nil = roundNil(action.nil);
		const room = ctx.nilBudget - ctx.nilCommitted + p.contract.amount;
		if (nil > room) {
			return {
				error: `That's more than the ${helpers.formatCurrency(room / 1000, "M")} left in your NIL budget.`,
			};
		}
		if (nil <= p.contract.amount) {
			return { error: "That's no raise." };
		}
		r.talks ??= openTalks(p.collegeProfile, 70, 0);
		outcome = respondToOffer(r.talks, r.demand, nil);
		if (outcome.type === "accepted") {
			setNil(p, outcome.amount);
			r.settled = true;
		}
	} else {
		if (action.promise !== "starter" && action.promise !== "minutes") {
			return { error: "Only playing time can be promised now." };
		}
		const already = (p.collegePromises ?? []).some(
			(promise) => promise.season === season + 1 && promise.tid === userTid,
		);
		if (already) {
			return { error: "You already made him a promise." };
		}
		p.collegePromises = [
			...(p.collegePromises ?? []),
			{
				type: action.promise,
				tid: userTid,
				season: season + 1,
				value: action.promise === "minutes" ? (action.value ?? 15) : 60,
			},
		];
	}

	await refreshRisk(p, await valuePercentiles(), season);
	await idb.cache.players.put(p);
	return { outcome };
};

// Promises made for this season: kept or broken, judged on the regular
// season.
export const collegeJudgePromises = async () => {
	const season = g.get("season");
	const players = await idb.cache.players.indexGetAll("playersByTid", [
		0,
		Infinity,
	]);
	for (const p of players) {
		let changed = false;
		for (const promise of p.collegePromises ?? []) {
			if (
				promise.season !== season ||
				promise.status ||
				promise.type === "nilRaise" ||
				promise.type === "noPosition"
			) {
				continue;
			}
			if (promise.tid !== p.tid) {
				// He left; the promise went with him.
				continue;
			}
			const line = seasonLine(p, season);
			const kept =
				promise.type === "starter"
					? line.gp > 0 && line.gs / line.gp >= (promise.value ?? 60) / 100
					: line.mpg >= (promise.value ?? 15);
			promise.status = kept ? "kept" : "broken";
			changed = true;
			await applyPromiseResult(p, promise);
		}
		if (changed) {
			await idb.cache.players.put(p);
		}
	}
};
