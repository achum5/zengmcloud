import { PHASE } from "../../../common/constants.ts";
import type { DraftPick, Player } from "../../../common/types.ts";
import { idb } from "../../db/index.ts";
import { g, helpers } from "../../util/index.ts";
import { contractOf, draftedAs } from "../realisticFrontOffice/contractOf.ts";
import isUntradable from "../trade/isUntradable.ts";
import summary from "../trade/summary.ts";
import {
	ensureRetention,
	franchiseValue,
	teamStrengths,
	type LeagueOutlook,
	type LetGoDecisions,
} from "../realisticFrontOffice/outlook.ts";

// THE SMART CPU'S WORTH (docs/smart-cpu.md, "The goal, worked out").
//
// A team's goal is titles: its franchise value, the outlook's discounted
// title chances over the next ten seasons (realisticFrontOffice/outlook.ts),
// for what it holds. What a deal does to that is the deal's use to it.
//
// But a team doesn't give anything away for less than the market would pay
// for it, and doesn't count what it gets at less than the market would pay
// it back: each side of a deal is worth to the team the more of its use and
// its market price, as a package (prices of single players add up wrong -
// two good players are worth far less together than apart). A package's
// price is the second-highest use for all of it among the teams keenest on
// it, by their own goals: what an auction would bring.
//
// And a team takes an offer only if no team keen on what it gives would
// really give more (owner, 2026-11-13: "the cpu would not be willing to send
// me good player if another good team would give up more than i'm currently
// offering"): bestRealOffer.

export type Holdings = { players: Player[]; picks: DraftPick[] };

export type Assets = { pids: number[]; dpids: number[] };

export const smartSettings = {
	// Teams whose uses set a package's price: the most valuable franchises,
	// where nearly all the bidding comes from.
	keenest: 6,
	// Teams asked for a real offer, and the most players in one.
	buyers: 4,
	offerSize: 3,
	// Players of a buyer's considered for an offer, best first.
	offerCandidates: 8,
	// A roster over the limit cuts one of its lowest-valued players, the one
	// whose release costs it least.
	cutCandidates: 6,
};

// Money a team would owe players it released, on top of what it owes now.
export type Dead = readonly { amount: number; exp: number }[];

// A player as the GM keys him: who, and on what terms.
const termsOf = (p: Player) => {
	const c = contractOf(p);
	return `${p.pid}:${c.amount}:${c.exp}`;
};

// What the league looks like to the GM, for as long as the outlook it was
// made with: every team's holdings, read afresh from the league each time
// (trades, signings, the draft and humans' moves are seen at once), and its
// values, kept while a team's players, their contracts and its picks are as
// they were.
export class SmartView {
	outlook: LeagueOutlook;
	humans: Set<number>;
	private holdings = new Map<number, { sig: string; h: Holdings }>();
	private values = new Map<string, number>();
	private keenestCache: { sig: string; tids: number[] } | undefined;

	constructor(outlook: LeagueOutlook, humans: Iterable<number>) {
		this.outlook = outlook;
		this.humans = new Set(humans);
	}

	// A team's players and picks as they stand.
	async holdingsOf(tid: number): Promise<Holdings> {
		const players = await idb.cache.players.indexGetAll("playersByTid", tid);
		const picks = (
			await idb.cache.draftPicks.indexGetAll("draftPicksByTid", tid)
		).filter((dp) => typeof dp.season === "number");
		const sig = this.keyOf({ players, picks });
		const had = this.holdings.get(tid);
		if (had && had.sig === sig) {
			return had.h;
		}
		const h = { players, picks };
		this.holdings.set(tid, { sig, h });
		return h;
	}

	// Whom each team lets go when contracts run out, decided on what it holds
	// and kept for every other roster of its (as try 1's market does), so a
	// deal's roster decides afresh only on the players it brings in. Deciding
	// afresh for every roster was 83% of a franchise value's time.
	private letGo = new Map<number, { sig: string; known: LetGoDecisions }>();

	private keyOf(h: Holdings, dead: Dead = []) {
		return `${h.players.map(termsOf).sort().join(",")}|${h.picks
			.map((dp) => `${dp.dpid}:${dp.pick}`)
			.sort()
			.join(",")}|${dead.map((d) => `${d.amount}:${d.exp}`).join(",")}`;
	}

	// A team's franchise value with these players, picks and money owed to
	// players released, as worked out by the outlook; whom it lets go
	// decided on its own roster.
	private raw(tid: number, h: Holdings, dead: Dead) {
		const own = this.holdings.get(tid);
		const ownSig = own?.sig ?? "";
		const sig = this.keyOf(h, dead);
		const key = `${tid}#${ownSig}#${sig}`;
		let v = this.values.get(key);
		if (v !== undefined) {
			return v;
		}
		let known = this.letGo.get(tid);
		if (known && known.sig !== ownSig) {
			known = undefined;
		}
		if (!known && own && sig !== this.keyOf(own.h)) {
			// Its own roster first, to decide on.
			this.raw(tid, own.h, []);
			known = this.letGo.get(tid);
		}
		const recording: LetGoDecisions | undefined =
			!known && own ? { decisions: new Map(), record: true } : undefined;
		const outlook =
			dead.length > 0
				? {
						...this.outlook,
						deadMoney: new Map(this.outlook.deadMoney).set(tid, [
							...(this.outlook.deadMoney.get(tid) ?? []),
							...dead,
						]),
					}
				: this.outlook;
		v = franchiseValue(
			outlook,
			teamStrengths(
				outlook,
				tid,
				h.players,
				h.picks,
				undefined,
				undefined,
				known?.known ?? recording,
			),
			tid,
		);
		if (recording) {
			recording.record = false;
			this.letGo.set(tid, { sig: ownSig, known: recording });
		}
		this.values.set(key, v);
		return v;
	}

	// What a team owes a player it releases (nothing for one just drafted).
	owedIfReleased(p: Player): Dead {
		const phase = g.get("phase");
		const season = g.get("season");
		if (helpers.justDrafted(draftedAs(p), phase, season)) {
			return [];
		}
		const c = contractOf(p);
		return [{ amount: c.amount, exp: c.exp }];
	}

	// A team's franchise value with these holdings, on the roster it will
	// have once games are played: over the limit, it releases whoever costs
	// it least, one at a time, owing them what it must.
	value(tid: number, h: Holdings, dead: Dead = []): number {
		return this.raw(tid, ...this.afterCuts(tid, h, dead));
	}

	// The holdings and money owed once a roster over the limit is cut to it,
	// one release at a time, each the one that costs the team least.
	afterCuts(tid: number, h: Holdings, dead: Dead = []): [Holdings, Dead] {
		const phase = g.get("phase");
		const first = g.get("season") + (phase > PHASE.PLAYOFFS ? 1 : 0);
		// In the offseason a player whose contract is up leaves or is
		// re-signed, not cut.
		const signedNow = (x: Holdings) =>
			x.players.filter((p) => contractOf(p).exp >= first);
		let now = h;
		let owed = dead;
		let signed = signedNow(now);
		while (signed.length > g.get("maxRosterSize")) {
			let best: { v: number; h: Holdings; dead: Dead } | undefined;
			for (const p of [...signed]
				.sort((a, b) => a.value - b.value || a.pid - b.pid)
				.slice(0, smartSettings.cutCandidates)) {
				const next = {
					players: now.players.filter((x) => x !== p),
					picks: now.picks,
				};
				const nextDead = [...owed, ...this.owedIfReleased(p)];
				const v = this.raw(tid, next, nextDead);
				if (!best || v > best.v) {
					best = { v, h: next, dead: nextDead };
				}
			}
			now = best!.h;
			owed = best!.dead;
			signed = signedNow(now);
		}
		return [now, owed];
	}

	// The teams whose bids set prices: the most valuable franchises.
	async keenest() {
		const rows = [];
		const sigs = [];
		for (const tid of this.outlook.teams.keys()) {
			const h = await this.holdingsOf(tid);
			sigs.push(this.holdings.get(tid)!.sig);
			rows.push({ tid, h });
		}
		const sig = sigs.join("/");
		if (this.keenestCache?.sig !== sig) {
			this.keenestCache = {
				sig,
				tids: rows
					.map(({ tid, h }) => ({ tid, v: this.value(tid, h) }))
					.sort((a, b) => b.v - a.v)
					.slice(0, smartSettings.keenest)
					.map((r) => r.tid),
			};
		}
		return this.keenestCache.tids;
	}

	async assetsOf(a: Assets) {
		const players: Player[] = [];
		for (const pid of a.pids) {
			const p = await idb.cache.players.get(pid);
			if (p) {
				players.push(p);
			}
		}
		const picks: DraftPick[] = [];
		for (const dpid of a.dpids) {
			const dp = await idb.cache.draftPicks.get(dpid);
			if (dp) {
				picks.push(dp);
			}
		}
		return { players, picks };
	}

	// Every team's holdings read and who sets prices, for the work done
	// without waiting (the `...Now` methods: free agency prices signings in
	// one go). Read again before each such batch.
	async prime() {
		for (const tid of this.outlook.teams.keys()) {
			await this.holdingsOf(tid);
		}
		await this.keenest();
	}

	// A team's holdings as last read.
	heldNow(tid: number) {
		const had = this.holdings.get(tid);
		if (!had) {
			throw new Error(`smartCpu: team ${tid} not read (prime the view)`);
		}
		return had.h;
	}

	// Holdings with `out` gone and `inn` added (a player in both is replaced:
	// the same player on new terms).
	swappedFrom(h: Holdings, out: Holdings, inn: Holdings): Holdings {
		const outP = new Set(out.players.map((p) => p.pid));
		const outD = new Set(out.picks.map((dp) => dp.dpid));
		const players = h.players.filter((p) => !outP.has(p.pid));
		const picks = h.picks.filter((dp) => !outD.has(dp.dpid));
		return {
			players: [
				...players,
				...inn.players.filter((p) => !players.some((x) => x.pid === p.pid)),
			],
			picks: [
				...picks,
				...inn.picks.filter((dp) => !picks.some((x) => x.dpid === dp.dpid)),
			],
		};
	}

	// A team's holdings with `out` gone and `inn` added.
	async swapped(tid: number, out: Holdings, inn: Holdings): Promise<Holdings> {
		return this.swappedFrom(await this.holdingsOf(tid), out, inn);
	}

	// What these assets do to a team's goal: its use for them, whether it
	// holds them (what it would lose) or not (what it would gain).
	useOfNow(tid: number, a: Holdings, h = this.heldNow(tid)) {
		const none = { players: [], picks: [] };
		const held =
			a.players.some((p) => h.players.some((x) => x.pid === p.pid)) ||
			a.picks.some((dp) => h.picks.some((x) => x.dpid === dp.dpid));
		return held
			? this.value(tid, h) - this.value(tid, this.swappedFrom(h, a, none))
			: this.value(tid, this.swappedFrom(h, none, a)) - this.value(tid, h);
	}

	async useOf(tid: number, a: Holdings) {
		await ensureRetention(this.outlook, a.players, tid);
		return this.useOfNow(tid, a, await this.holdingsOf(tid));
	}

	// Each of a team's players' use to it, on what it holds.
	private uses = new Map<string, Map<number, number>>();
	usesOf(tid: number, h: Holdings) {
		const key = `${tid}#${this.keyOf(h)}`;
		let uses = this.uses.get(key);
		if (!uses) {
			uses = new Map();
			for (const p of h.players) {
				uses.set(p.pid, this.useOfNow(tid, { players: [p], picks: [] }, h));
			}
			this.uses.set(key, uses);
		}
		return uses;
	}

	// What a team would really bid for a package it doesn't hold: its use
	// for it, taken in as the trade rules allow. Over the cap after it, the
	// team sends back salary to match (its least valuable players first, as
	// many as the rule needs) - without this an expensive contract looked as
	// easy to sell as a cheap one, and a free agent worth as much to the
	// market at any salary.
	bidNow(tid: number, a: Holdings) {
		const h = this.heldNow(tid);
		if (a.players.some((p) => h.players.some((x) => x.pid === p.pid))) {
			return this.useOfNow(tid, a, h);
		}
		const salaryOf = (players: readonly Player[]) =>
			players.reduce((sum, p) => sum + contractOf(p).amount, 0);
		const incoming = salaryOf(a.players);
		const capType = g.get("salaryCapType");
		const none = { players: [], picks: [] };
		if (incoming <= 0 || capType === "none") {
			return this.useOfNow(tid, a, h);
		}
		const phase = g.get("phase");
		const first = g.get("season") + (phase > PHASE.PLAYOFFS ? 1 : 0);
		const payroll =
			salaryOf(h.players.filter((p) => contractOf(p).exp >= first)) +
			(this.outlook.deadMoney.get(tid) ?? [])
				.filter((d) => d.exp >= first)
				.reduce((sum, d) => sum + d.amount, 0);
		const cap = g.get("salaryCap");
		if (payroll + incoming <= cap) {
			return this.useOfNow(tid, a, h);
		}
		const need =
			capType === "soft"
				? (incoming * 100) / g.get("softCapTradeSalaryMatch")
				: incoming - Math.max(0, cap - payroll);
		// Over the cap after it, it must send salary back: its bid stands only
		// if some trade it would still gain from does that - the players it
		// would miss least, until the salary matches (any of them, tradable
		// today or not: a price is what the market would pay over the weeks
		// ahead). The players it sends are part of what it pays, so its bid is
		// still its use for the package; a team no such trade suits doesn't
		// bid.
		const use = this.useOfNow(tid, a, h);
		const uses = this.usesOf(tid, h);
		const fillers: Player[] = [];
		let sent = 0;
		for (const p of [...h.players].sort(
			(x, y) => uses.get(x.pid)! - uses.get(y.pid)! || x.pid - y.pid,
		)) {
			if (sent >= need) {
				break;
			}
			fillers.push(p);
			sent += contractOf(p).amount;
		}
		if (sent < need) {
			return -Infinity;
		}
		const net =
			this.value(tid, this.swappedFrom(h, { players: fillers, picks: [] }, a)) -
			this.value(tid, this.swappedFrom(h, none, none));
		return net >= 0 ? use : -Infinity;
	}

	// A package's market price: the second-highest real bid for all of it
	// among the keenest teams other than `not`.
	priceNow(a: Holdings, not: readonly number[]) {
		if (a.players.length === 0 && a.picks.length === 0) {
			return 0;
		}
		const bids = [];
		for (const tid of this.keenestCache?.tids ?? []) {
			if (!not.includes(tid)) {
				const bid = this.bidNow(tid, a);
				// A team that can't take it under the rules doesn't bid.
				if (Number.isFinite(bid)) {
					bids.push(bid);
				}
			}
		}
		bids.sort((x, y) => y - x);
		// Nobody able to take it: no price to hold out for.
		return bids[1] ?? bids[0] ?? -Infinity;
	}

	async price(a: Holdings, not: readonly number[]) {
		for (const tid of await this.keenest()) {
			if (!not.includes(tid)) {
				await ensureRetention(this.outlook, a.players, tid);
			}
		}
		return this.priceNow(a, not);
	}

	// A team's change in worth from giving `out` and getting `inn` (from
	// `partner`): its goal, and each side at least its market price - what
	// it gets only as far as it keeps it once its roster is cut to the limit
	// (a player it must release it can't sell).
	dealWorthNow(
		tid: number,
		partner: number,
		out: Holdings,
		inn: Holdings,
		h = this.heldNow(tid),
	) {
		const after = this.swappedFrom(h, out, inn);
		const dG = this.value(tid, after) - this.value(tid, h);
		const [cut] = this.afterCuts(tid, after);
		const kept = new Set(cut.players);
		const keptIn = {
			players: inn.players.filter((p) => kept.has(p)),
			picks: inn.picks,
		};
		const not = [tid, partner];
		return (
			dG +
			Math.max(0, this.priceNow(keptIn, not) - this.useOfNow(tid, keptIn, h)) -
			Math.max(0, this.priceNow(out, not) - this.useOfNow(tid, out, h))
		);
	}

	async dealWorth(tid: number, partner: number, out: Holdings, inn: Holdings) {
		await ensureRetention(this.outlook, inn.players, tid);
		await this.holdingsOf(tid);
		await this.price(inn, [tid, partner]);
		await this.price(out, [tid, partner]);
		return this.dealWorthNow(tid, partner, out, inn);
	}

	// The same, quicker, for searching: the goal alone.
	async quickWorth(tid: number, out: Holdings, inn: Holdings) {
		await ensureRetention(this.outlook, inn.players, tid);
		const h = await this.holdingsOf(tid);
		return (
			this.value(tid, await this.swapped(tid, out, inn)) - this.value(tid, h)
		);
	}

	// THE MARKET: the best real offer for `out` from the CPU teams keenest on
	// it (not `seller`, not `not`): each its own players, as many as still
	// leave it better off by its own worth, legal under the trade rules, and
	// what that offer is worth to the seller.
	async bestRealOffer(seller: number, out: Holdings, not: number) {
		const s = smartSettings;
		const buyers = [];
		for (const tid of await this.keenest()) {
			if (tid !== seller && tid !== not && !this.humans.has(tid)) {
				buyers.push({ tid, want: await this.useOf(tid, out) });
			}
		}
		buyers.sort((a, b) => b.want - a.want);
		let best: { buyer: number; worth: number; offer: Holdings } | undefined;
		for (const { tid: buyer, want } of buyers.slice(0, s.buyers)) {
			if (want <= 0) {
				continue;
			}
			const theirs = (await this.holdingsOf(buyer)).players.filter(
				(p) => !isUntradable(p).untradable,
			);
			// Best for the seller first.
			const scored = [];
			for (const p of theirs) {
				scored.push({
					p,
					v: await this.useOf(seller, { players: [p], picks: [] }),
				});
			}
			scored.sort((a, b) => b.v - a.v);
			let offer: Player[] = [];
			for (const { p } of scored.slice(0, s.offerCandidates)) {
				if (offer.length >= s.offerSize) {
					break;
				}
				const next = [...offer, p];
				if (
					(await this.quickWorth(buyer, { players: next, picks: [] }, out)) > 0
				) {
					offer = next;
				}
			}
			if (offer.length === 0) {
				continue;
			}
			const legal = await summary([
				{
					tid: buyer,
					pids: offer.map((p) => p.pid),
					dpids: [],
					pidsExcluded: [],
					dpidsExcluded: [],
				},
				{
					tid: seller,
					pids: out.players.map((p) => p.pid),
					dpids: out.picks.map((dp) => dp.dpid),
					pidsExcluded: [],
					dpidsExcluded: [],
				},
			]);
			const inn = { players: offer, picks: [] };
			if (
				legal.warning ||
				(await this.dealWorth(buyer, seller, inn, out)) <= 0
			) {
				continue;
			}
			const worth = await this.dealWorth(seller, buyer, out, inn);
			if (!best || worth > best.worth) {
				best = { buyer, worth, offer: inn };
			}
		}
		return best;
	}

	// What a team says to a deal: how far it comes out ahead (> 0 is yes) -
	// by its worth, and not below the best real offer elsewhere for what it
	// gives up.
	async verdict(
		tid: number,
		partner: number,
		out: Assets,
		inn: Assets,
		{ market = true }: { market?: boolean } = {},
	) {
		const o = await this.assetsOf(out);
		const i = await this.assetsOf(inn);
		const worth = await this.dealWorth(tid, partner, o, i);
		if (
			!market ||
			worth <= 0 ||
			(o.players.length === 0 && o.picks.length === 0)
		) {
			return { worth, gain: worth };
		}
		const best = await this.bestRealOffer(tid, o, partner);
		return {
			worth,
			best,
			gain: best ? Math.min(worth, worth - best.worth) : worth,
		};
	}
}
