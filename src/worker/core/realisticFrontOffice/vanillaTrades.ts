import { COMPOSITE_WEIGHTS, POSITIONS } from "../../../common/constants.ts";
import type { TradeTeams } from "../../../common/types.ts";
import { idb } from "../../db/index.ts";
import { helpers } from "../../util/index.ts";
import compositeRating from "../player/compositeRating.ts";
import isUntradable from "../trade/isUntradable.ts";
import type { LookingFor } from "../trade/makeItWork.ts";
import {
	assetsFor,
	humanMarginFactor,
	load,
	type Loaded,
} from "./humanTrades.ts";
import {
	currentOutlook,
	humanDealNet,
	MIN_GAIN,
	pickUpgrades,
	singlePrice,
	type Asset,
	type TeamState,
} from "./market.ts";
import { ensureRetention, type LeagueOutlook } from "./outlook.ts";

// VANILLA'S TRADE SCREENS, SMARTER TEAMS (owner's call, 2026-11-11: "the
// same identical mechanics as vanilla, it's just that teams are far far
// smarter"): the trading block, "What would make this work" and Trade
// Proposals run vanilla's forward selection (trade/makeItWork.ts) - a team
// adds, or asks for, one asset at a time until the deal is only just good
// enough for it: its best offer - with the team's own read of a deal
// (humanTrades.ts) where vanilla has the stock trade values: what it gains,
// over the bar a human's deal must clear.

// Far below any deal a team would make: one it cannot price (an asset no
// longer there) or one that leaves it unable to fill its roster.
const NO_DEAL = -1e6;

// A deal it only just takes.
const JUST = 1e-9;

const playersIn = (assets: readonly Asset[]) =>
	assets.flatMap((x) => (x.kind === "player" ? [x.p] : []));

// What a deal is worth to the CPU team `tid` that gives `pidsRemove` and
// `dpidsRemove` and gets `pidsAdd` and `dpidsAdd`, in titles over its bar -
// more than 0, it takes it. Vanilla's ValueChangeCalculator, by the mode.
export const frontOfficeValuer = async () => {
	const outlook = await currentOutlook();
	const loaded: Loaded = { outlook, teams: new Map() };
	const factor = humanMarginFactor();
	const cache = new Map<string, number>();
	return {
		outlook,
		team: (tid: number) => load(outlook, tid, loaded),
		evaluate: async ({
			tid,
			pidsAdd,
			pidsRemove,
			dpidsAdd,
			dpidsRemove,
			tradingPartnerTid,
		}: {
			tid: number;
			pidsAdd: number[];
			pidsRemove: number[];
			dpidsAdd: number[];
			dpidsRemove: number[];
			tradingPartnerTid: number | undefined;
		}): Promise<number> => {
			if (tradingPartnerTid === undefined) {
				return NO_DEAL;
			}
			const key = [
				tid,
				tradingPartnerTid,
				[...pidsAdd].sort(),
				[...pidsRemove].sort(),
				[...dpidsAdd].sort(),
				[...dpidsRemove].sort(),
			].join("|");
			const cached = cache.get(key);
			if (cached !== undefined) {
				return cached;
			}
			const a = await load(outlook, tid, loaded);
			const h = await load(outlook, tradingPartnerTid, loaded);
			const giving = assetsFor(a, pidsRemove, dpidsRemove);
			const getting = assetsFor(h, pidsAdd, dpidsAdd);
			let value = NO_DEAL;
			if (giving && getting) {
				// How each side's players would feel about re-signing with the other.
				await ensureRetention(outlook, playersIn(getting), a.tid);
				await ensureRetention(outlook, playersIn(giving), h.tid);
				const net = humanDealNet(outlook, a, h, giving, getting, factor);
				if (net !== undefined) {
					value = net - MIN_GAIN;
				}
				// Nothing but moving up in this draft, it takes as the judge does
				// (humanTrades.ts): only just, so it adds nothing to it.
				const up = pickUpgrades(outlook, giving, getting);
				if (up.pairs > 0 && up.restOut.length + up.restIn.length === 0) {
					value = Math.max(value, JUST);
				}
			}
			cache.set(key, value);
			return value;
		},
	};
};

type Valuer = Awaited<ReturnType<typeof frontOfficeValuer>>;

// Pricing a deal whole takes a team's seasons ahead, both teams', so where
// vanilla prices every asset each step, each step here prices whole only the
// assets nearest its choice: ranked by what each did to the deal when last
// priced (at first, by what it alone is worth to the team), this many that
// would do (and this many that would not, at most) a step.
const PRICED_PASSING = 3;
const PRICED_FAILING = 6;

type Candidate = {
	tid: number;
	type: "player" | "draftPick";
	id: number;
	asset: Asset;
	// vanilla's: only for some "looking for" sorts.
	score: number;
	// The deal with it, if priced whole this step.
	dv?: number;
};

const keyOf = (c: Pick<Candidate, "type" | "id">) => `${c.type}${c.id}`;

const withAsset = (teams: TradeTeams, c: Candidate): TradeTeams => {
	const out = helpers.deepCopy(teams);
	const side = c.tid === out[0].tid ? out[0] : out[1];
	if (c.type === "player") {
		side.pids.push(c.id);
	} else {
		side.dpids.push(c.id);
	}
	return out;
};

const evaluateTeams = (valuer: Valuer, teams: TradeTeams) =>
	valuer.evaluate({
		tid: teams[1].tid,
		pidsAdd: teams[0].pids,
		pidsRemove: teams[1].pids,
		dpidsAdd: teams[0].dpids,
		dpidsRemove: teams[1].dpids,
		tradingPartnerTid: teams[0].tid,
	});

// vanilla's tryAddAsset, step for step.
const tryAddAsset = async (
	valuer: Valuer,
	shift: Map<string, number>,
	teams: TradeTeams,
	holdUserConstant: boolean,
	prevDv: number,
	firstTry: boolean,
	lookingFor?: LookingFor,
): Promise<TradeTeams | undefined> => {
	const outlook: LeagueOutlook = valuer.outlook;
	const user: TeamState = await valuer.team(teams[0].tid);
	const cpu: TeamState = await valuer.team(teams[1].tid);
	const candidates: Candidate[] = [];

	if (!holdUserConstant) {
		for (const p of await idb.cache.players.indexGetAll(
			"playersByTid",
			teams[0].tid,
		)) {
			if (
				teams[0].pids.includes(p.pid) ||
				teams[0].pidsExcluded.includes(p.pid) ||
				isUntradable(p).untradable
			) {
				continue;
			}
			const asset = assetsFor(user, [p.pid], [])?.[0];
			if (asset) {
				candidates.push({
					tid: teams[0].tid,
					type: "player",
					id: p.pid,
					asset,
					score: 1,
				});
			}
		}
	}

	// If looking for a position, the first asset added is a player there.
	let lookingForSpecificPositions: Set<string> | undefined;
	if (firstTry && lookingFor && lookingFor.positions.size > 0) {
		lookingForSpecificPositions = new Set(
			POSITIONS.filter((pos) =>
				[...lookingFor.positions].some((pos2) => pos.includes(pos2)),
			),
		);
	}

	for (const p of await idb.cache.players.indexGetAll(
		"playersByTid",
		teams[1].tid,
	)) {
		if (
			teams[1].pids.includes(p.pid) ||
			teams[1].pidsExcluded.includes(p.pid) ||
			isUntradable(p).untradable
		) {
			continue;
		}
		if (
			lookingForSpecificPositions &&
			!lookingForSpecificPositions.has(p.ratings.at(-1)!.pos)
		) {
			continue;
		}
		const asset = assetsFor(cpu, [p.pid], [])?.[0];
		if (asset) {
			candidates.push({
				tid: teams[1].tid,
				type: "player",
				id: p.pid,
				asset,
				score: 1,
			});
		}
	}

	if (!lookingForSpecificPositions) {
		const sides = holdUserConstant ? [1 as const] : [0 as const, 1 as const];
		for (const i of sides) {
			const t = i === 0 ? user : cpu;
			for (const dp of await idb.cache.draftPicks.indexGetAll(
				"draftPicksByTid",
				teams[i].tid,
			)) {
				if (
					teams[i].dpids.includes(dp.dpid) ||
					teams[i].dpidsExcluded.includes(dp.dpid)
				) {
					continue;
				}
				const asset = assetsFor(t, [], [dp.dpid])?.[0];
				if (asset) {
					candidates.push({
						tid: teams[i].tid,
						type: "draftPick",
						id: dp.dpid,
						asset,
						score: 0,
					});
				}
			}
		}
	}

	if (candidates.length === 0) {
		return;
	}

	// What each would do to the deal, before it is priced whole: the change
	// it made when last priced, or what it alone is worth to the CPU team.
	const estimate = (c: Candidate) => {
		let d = shift.get(keyOf(c));
		if (d === undefined) {
			d =
				c.tid === cpu.tid
					? singlePrice(outlook, cpu, "-", c.asset)
					: singlePrice(outlook, cpu, "+", c.asset);
		}
		return prevDv + d;
	};
	const price = async (c: Candidate) => {
		if (c.dv === undefined) {
			c.dv = await evaluateTeams(valuer, withAsset(teams, c));
			shift.set(keyOf(c), c.dv - prevDv);
		}
		return c.dv;
	};
	const passes = (dv: number) =>
		prevDv > 0 ? dv > 0 && dv < prevDv : dv > prevDv;

	// vanilla's "looking for" sorts.
	let lookingForSort = false;
	if (lookingFor) {
		if (lookingFor.skills.size > 0) {
			for (const c of candidates) {
				if (c.asset.kind === "player") {
					const ratings = c.asset.p.ratings.at(-1)!;
					for (const skill of lookingFor.skills) {
						c.score += compositeRating(
							ratings,
							COMPOSITE_WEIGHTS[skill]!.ratings,
							COMPOSITE_WEIGHTS[skill]!.weights,
							false,
						);
					}
				}
			}
			lookingForSort = true;
		}
		if (lookingFor.bestCurrentPlayers) {
			for (const c of candidates) {
				if (c.asset.kind === "player") {
					c.score *= c.asset.p.valueNoPot;
				}
			}
			lookingForSort = true;
		} else if (lookingFor.prospects) {
			for (const c of candidates) {
				if (c.asset.kind === "player") {
					const ratings = c.asset.p.ratings.at(-1)!;
					c.score += (ratings.pot - ratings.ovr) / 20;
				}
			}
			lookingForSort = true;
		}
	}

	// vanilla's choice among `pool`, those priced whole as above.
	const choose = async (pool: Candidate[]) => {
		if (lookingForSort) {
			// The highest score of those that would do: in order, the first.
			for (const c of [...pool].sort((a, b) => b.score - a.score)) {
				if (passes(await price(c))) {
					return c;
				}
			}
			return undefined;
		}
		const ranked = pool
			.map((c) => ({ c, est: estimate(c) }))
			.sort((a, b) => a.est - b.est);
		const passing: Candidate[] = [];
		let failing = 0;
		if (prevDv > 0) {
			// The best asset it can give and still take the deal: the lowest
			// dv above 0. Those nearest 0 from above first, then below.
			const above = ranked.filter((x) => x.est > 0);
			const below = ranked.filter((x) => x.est <= 0).reverse();
			for (const { c } of [...above, ...below]) {
				if (passing.length >= PRICED_PASSING || failing >= PRICED_FAILING) {
					break;
				}
				if (passes(await price(c))) {
					passing.push(c);
				} else {
					failing += 1;
				}
			}
			passing.sort((a, b) => a.dv! - b.dv!);
			return passing[0];
		}
		// From a deal it would not take toward one it would: the lowest dv
		// above 0 if one gets it there, else the highest.
		const above = ranked.filter((x) => x.est > 0);
		for (const { c } of above) {
			if (passing.length >= PRICED_PASSING || failing >= PRICED_FAILING) {
				break;
			}
			const dv = await price(c);
			if (dv > 0) {
				passing.push(c);
			} else {
				failing += 1;
			}
		}
		if (passing.length > 0) {
			passing.sort((a, b) => a.dv! - b.dv!);
			return passing[0];
		}
		failing = 0;
		let best: Candidate | undefined;
		for (const { c } of [...ranked].reverse()) {
			if (failing >= PRICED_FAILING) {
				break;
			}
			const dv = await price(c);
			if (passes(dv) && (!best || dv > best.dv!)) {
				best = c;
			} else {
				failing += 1;
			}
		}
		return best;
	};

	let asset: Candidate | undefined;
	if (lookingFor) {
		const lookingForSomePlayerBeforeDraftPicks =
			firstTry &&
			lookingFor.draftPicks &&
			(lookingFor.bestCurrentPlayers ||
				lookingFor.prospects ||
				lookingFor.skills.size > 0);
		if (
			!lookingForSpecificPositions &&
			!lookingForSomePlayerBeforeDraftPicks &&
			lookingFor.draftPicks
		) {
			// Draft picks first, if it would give any.
			asset = await choose(candidates.filter((c) => c.type === "draftPick"));
		}
	}
	if (!asset) {
		asset = await choose(candidates);
	}
	if (!asset) {
		return;
	}
	return withAsset(teams, asset);
};

// vanilla's makeItWork, step for step: the CPU team (teams[1]) adds assets,
// or asks for the user's, until the deal is just good enough for it.
export const makeItWorkSmart = async (
	teams: TradeTeams,
	{
		holdUserConstant,
		lookingFor,
		maxAssetsToAdd = Infinity,
		valuer,
		shift = new Map(),
		states = [],
	}: {
		holdUserConstant: boolean;
		lookingFor?: LookingFor;
		maxAssetsToAdd?: number;
		valuer: Valuer;
		shift?: Map<string, number>;
		// Every deal it went through that it would take, in order.
		states?: TradeTeams[];
	},
): Promise<TradeTeams | undefined> => {
	let added = 0;
	let prevDv = await evaluateTeams(valuer, teams);
	const initialSign = prevDv > 0 ? 1 : -1;
	let prevTeams = teams;
	if (prevDv > 0 && !states.includes(teams)) {
		states.push(teams);
	}
	while (true) {
		const newTeams = await tryAddAsset(
			valuer,
			shift,
			prevTeams,
			holdUserConstant,
			prevDv,
			added === 0,
			lookingFor,
		);
		if (!newTeams) {
			return (await evaluateTeams(valuer, prevTeams)) > 0
				? prevTeams
				: undefined;
		}
		added += 1;
		const dv = await evaluateTeams(valuer, newTeams);
		if (Math.sign(prevDv - dv) !== initialSign) {
			return prevDv > 0 ? prevTeams : undefined;
		}
		if (dv > 0) {
			states.push(newTeams);
		}
		if (initialSign === -1) {
			if (dv > 0) {
				// The other way again, so that asking once more changes nothing.
				const newMaxAssetsToAdd = maxAssetsToAdd - added;
				if (newMaxAssetsToAdd > 0) {
					return makeItWorkSmart(newTeams, {
						holdUserConstant,
						maxAssetsToAdd: newMaxAssetsToAdd,
						valuer,
						shift,
						states,
					});
				}
				return newTeams;
			}
		} else if (dv < 0 && prevDv > 0) {
			return prevTeams;
		}
		if (added >= maxAssetsToAdd) {
			return dv > 0 ? newTeams : undefined;
		}
		prevTeams = newTeams;
		prevDv = dv;
	}
};

// The deal it ends on, or if the team's whole judge would not take that -
// before the deadline it may rather wait; `shop`, what it gives may fetch
// more from another team, as a deal only just good enough for it will - the
// last before it that the judge takes, so many tried at most.
export const lastTaken = async (
	states: readonly TradeTeams[],
	takes: (teams: TradeTeams) => Promise<boolean>,
	tries = 4,
) => {
	for (const teams of states.toReversed().slice(0, tries)) {
		if (await takes(teams)) {
			return teams;
		}
	}
	return undefined;
};
