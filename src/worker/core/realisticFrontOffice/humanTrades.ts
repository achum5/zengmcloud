import type { DraftPick, Player } from "../../../common/types.ts";
import { g, helpers } from "../../util/index.ts";
import {
	aiTids,
	count,
	currentOutlook,
	loadTeam,
	marginOf,
	MIN_GAIN,
	packages,
	priceFor,
	salariesWork,
	singlePrice,
	tableAssets,
	waitingOf,
	type Asset,
	type TeamState,
} from "./market.ts";
import { ensureRetention, type LeagueOutlook } from "./outlook.ts";
import { offerOnHeld } from "./windows.ts";

// TRADING WITH HUMANS. A CPU team judges a human's offer exactly as it judges
// a deal with another CPU team - what it does to its title chances, the trade
// penalty included, against the model's margin of error - with two more
// things to clear (owner's calls):
// - a higher bar: a human picks which deals to offer, and so finds the ones
//   the model gets wrong in his favour; the margin counts for more, by an
//   amount tuned until an expert's search for such deals stops paying;
// - shopping: what it gives up must fetch more from the human than the most
//   any other CPU team would pay for it.
// A no says only why in general: no, a better offer (shown in full), or not
// before the deadline.

export const humanTradeSettings = {
	// How much more the margin of error counts in a deal with a human than in
	// one between CPU teams.
	marginFactor: 1.5,
};

// Packages from each other team priced in full when shopping.
const SHOP_CANDIDATES = 6;

export type Verdict =
	| { kind: "yes" }
	| { kind: "no" }
	| { kind: "wait" }
	// What it asks for is in a deal held for bids: a bid on it, if it asks
	// for exactly what that team gives in it (windows.ts), else no.
	| { kind: "bid" }
	| { kind: "held" }
	| {
			kind: "better";
			tid: number;
			// What the other team would give for the same assets.
			players: Player[];
			picks: DraftPick[];
	  };

export const playersIn = (assets: readonly Asset[]) =>
	assets.flatMap((x) => (x.kind === "player" ? [x.p] : []));

export const assetsFor = (
	t: TeamState,
	pids: readonly number[],
	dpids: readonly number[],
): Asset[] | undefined => {
	const out: Asset[] = [];
	for (const pid of pids) {
		const p = t.players.find((x) => x.pid === pid);
		if (!p) {
			return undefined;
		}
		out.push({ kind: "player", p });
	}
	for (const dpid of dpids) {
		const dp = t.picks.find((x) => x.dpid === dpid);
		if (!dp) {
			return undefined;
		}
		out.push({ kind: "pick", dp });
	}
	return out;
};

// Teams loaded once for many judgements in a row, while the outlook holds.
export type Loaded = { outlook: LeagueOutlook; teams: Map<number, TeamState> };

export const load = async (
	outlook: LeagueOutlook,
	tid: number,
	loaded?: Loaded,
) => {
	if (loaded?.outlook !== outlook) {
		return loadTeam(outlook, tid);
	}
	let t = loaded.teams.get(tid);
	if (!t) {
		t = await loadTeam(outlook, tid);
		loaded.teams.set(tid, t);
	}
	return t;
};

// The best another CPU team would give for these assets, by what it is worth
// to the team giving them up (less its margin of error), among packages that
// clear the other team's own bar - undefined if no team would deal, or none
// for more than `beat`.
export const bestElsewhere = async (
	outlook: LeagueOutlook,
	a: TeamState,
	giving: readonly Asset[],
	excluded: readonly number[],
	beat = -Infinity,
	loaded?: Loaded,
) => {
	const costToA = priceFor(outlook, a, giving, []);
	let best: { tid: number; assets: Asset[]; net: number } | undefined;
	for (const tid of await aiTids()) {
		if (tid === a.tid || excluded.includes(tid)) {
			continue;
		}
		const b = await load(outlook, tid, loaded);
		await ensureRetention(outlook, playersIn(giving), tid);
		await ensureRetention(outlook, b.tradablePlayers, a.tid);
		const worthToB = priceFor(outlook, b, [], giving);
		if (worthToB < MIN_GAIN) {
			continue;
		}
		const assets = tableAssets(b);
		const cost = (x: Asset) => singlePrice(outlook, b, "-", x);
		const worth = (x: Asset) => singlePrice(outlook, a, "+", x);
		const sum = (pkg: Asset[], of: (x: Asset) => number) =>
			pkg.reduce((s, x) => s + of(x), 0);
		// (What the other team would not give needs no price for this one.)
		const screened = packages(assets, (x) => -cost(x))
			.filter((pkg) => worthToB + sum(pkg, cost) > 0)
			.map((pkg) => ({ pkg, forA: costToA + sum(pkg, worth) }))
			.filter(({ forA }) => forA > 0)
			.sort((x, y) => y.forA - x.forA)
			.slice(0, SHOP_CANDIDATES);
		for (const { pkg } of screened) {
			if (
				!salariesWork(a, giving, pkg) ||
				!salariesWork(b, pkg, giving) ||
				b.players.length + count(giving) - count(pkg) >
					g.get("maxRosterSize") + 2
			) {
				continue;
			}
			// Worth more to it than the best so far first - most are not, and
			// need no full price to tell - then the other team's bar.
			const bar = Math.max(beat, best?.net ?? -Infinity);
			const marginA = marginOf(outlook, a, giving, pkg);
			const net =
				priceFor(
					outlook,
					a,
					giving,
					pkg,
					b,
					bar === -Infinity ? undefined : bar + marginA,
				) - marginA;
			if (net <= bar) {
				continue;
			}
			const gainB = priceFor(outlook, b, pkg, giving, a);
			if (gainB - marginOf(outlook, b, pkg, giving) < MIN_GAIN) {
				continue;
			}
			best = { tid, assets: pkg, net };
		}
	}
	return best;
};

// A CPU team's answer to a human's offer: it gives `cpu`'s assets for
// `human`'s.
export type Side = {
	tid: number;
	pids: readonly number[];
	dpids: readonly number[];
};

export const judgeHumanOffer = async (
	cpu: Side,
	human: Side,
	{
		loaded,
		shop = true,
	}: {
		// Teams already loaded, for a run of judgements on the same day.
		loaded?: Loaded;
		// Whether it shops what it would give (a trading block bid does not:
		// that is weighed when the human offers the deal).
		shop?: boolean;
	} = {},
): Promise<Verdict> => {
	const held = await offerOnHeld(cpu);
	if (held === "held") {
		return { kind: "held" };
	}
	const outlook = await currentOutlook();
	const a = await load(outlook, cpu.tid, loaded);
	const h = await load(outlook, human.tid, loaded);
	const giving = assetsFor(a, cpu.pids, cpu.dpids);
	const getting = assetsFor(h, human.pids, human.dpids);
	if (!giving || !getting) {
		return { kind: "no" };
	}
	// How each side's players would feel about re-signing with the other.
	await ensureRetention(outlook, playersIn(getting), a.tid);
	await ensureRetention(outlook, playersIn(giving), h.tid);

	// Better than keeping what it has, by more than its margin of error counts
	// for in a deal with a human.
	const net =
		priceFor(outlook, a, giving, getting, h) -
		humanTradeSettings.marginFactor * marginOf(outlook, a, giving, getting);
	if (net < MIN_GAIN) {
		return { kind: "no" };
	}
	// A bid is weighed against the others when the window closes.
	if (held === "bid") {
		return { kind: "bid" };
	}

	// Before the deadline it may rather decide then.
	if (outlook.deadline) {
		const [side] = waitingOf(
			outlook,
			{ tids: [a.tid, h.tid], assets: [giving, getting] },
			[a, h],
		);
		if (side && side.wait > side.now) {
			return { kind: "wait" };
		}
	}

	// Better than the most any other CPU team would give for the same.
	if (shop && giving.length > 0) {
		const elsewhere = await bestElsewhere(
			outlook,
			a,
			giving,
			[h.tid],
			net,
			loaded,
		);
		if (elsewhere && elsewhere.net > net) {
			return {
				kind: "better",
				tid: elsewhere.tid,
				players: elsewhere.assets.flatMap((x) =>
					x.kind === "player" ? [x.p] : [],
				),
				picks: elsewhere.assets.flatMap((x) =>
					x.kind === "pick" ? [x.dp] : [],
				),
			};
		}
	}

	return { kind: "yes" };
};

const pickName = (dp: DraftPick) => {
	const round =
		dp.round === 1 ? "1st" : dp.round === 2 ? "2nd" : `${dp.round}th`;
	const abbrev = g.get("teamInfoCache")[dp.originalTid]?.abbrev;
	return `${dp.season} ${round} round pick${abbrev ? ` (${abbrev})` : ""}`;
};

// What a rejection says.
export const verdictMessage = (verdict: Exclude<Verdict, { kind: "yes" }>) => {
	if (verdict.kind === "bid") {
		return "Bid placed. They decide tomorrow.";
	}
	if (verdict.kind === "held") {
		return "Trade rejected. Part of a pending deal.";
	}
	if (verdict.kind === "wait") {
		return "Trade rejected. They would rather wait for the deadline.";
	}
	if (verdict.kind === "better") {
		const t = g.get("teamInfoCache")[verdict.tid];
		const assets = [
			...verdict.players.map(
				(p) =>
					`<a href="${helpers.leagueUrl(["player", p.pid])}">${p.firstName} ${p.lastName}</a>`,
			),
			...verdict.picks.map(pickName),
		];
		return `Trade rejected. They have a better offer: ${
			assets.length > 0 ? assets.join(", ") : "nothing back"
		} from the ${t?.region ?? ""} ${t?.name ?? ""}.`;
	}
	return "Trade rejected.";
};
