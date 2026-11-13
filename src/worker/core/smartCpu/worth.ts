import type { DraftPick, Player } from "../../../common/types.ts";
import { idb } from "../../db/index.ts";
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
};

// What one moment of the league looks like to the GM: the outlook, every
// team's holdings, and caches. Made once per moment; any trade made after
// it calls for a new one.
export class SmartView {
	outlook: LeagueOutlook;
	humans: Set<number>;
	private holdings = new Map<number, Holdings>();
	private values = new Map<string, number>();
	private keenestCache: number[] | undefined;

	constructor(outlook: LeagueOutlook, humans: Iterable<number>) {
		this.outlook = outlook;
		this.humans = new Set(humans);
	}

	async holdingsOf(tid: number): Promise<Holdings> {
		let h = this.holdings.get(tid);
		if (!h) {
			h = {
				players: await idb.cache.players.indexGetAll("playersByTid", tid),
				picks: await idb.cache.draftPicks.indexGetAll("draftPicksByTid", tid),
			};
			this.holdings.set(tid, h);
		}
		return h;
	}

	// Whom each team lets go when contracts run out, decided on what it holds
	// and kept for every other roster of its (as try 1's market does), so a
	// deal's roster decides afresh only on the players it brings in. Deciding
	// afresh for every roster was 83% of a franchise value's time.
	private letGo = new Map<number, LetGoDecisions>();

	private keyOf(tid: number, h: Holdings) {
		return `${tid}|${h.players
			.map((p) => p.pid)
			.sort((a, b) => a - b)
			.join(",")}|${h.picks
			.map((dp) => dp.dpid)
			.sort((a, b) => a - b)
			.join(",")}`;
	}

	// A team's franchise value with these holdings.
	value(tid: number, h: Holdings) {
		const key = this.keyOf(tid, h);
		let v = this.values.get(key);
		if (v === undefined) {
			let known = this.letGo.get(tid);
			const own = this.holdings.get(tid);
			if (!known && own && key !== this.keyOf(tid, own)) {
				// Its own roster first, to decide on.
				this.value(tid, own);
				known = this.letGo.get(tid);
			}
			let recording: LetGoDecisions | undefined;
			if (!known && own) {
				recording = { decisions: new Map(), record: true };
			}
			v = franchiseValue(
				this.outlook,
				teamStrengths(
					this.outlook,
					tid,
					h.players,
					h.picks,
					undefined,
					undefined,
					known ?? recording,
				),
				tid,
			);
			if (recording) {
				recording.record = false;
				this.letGo.set(tid, recording);
			}
			this.values.set(key, v);
		}
		return v;
	}

	// The teams whose bids set prices: the most valuable franchises.
	async keenest() {
		if (!this.keenestCache) {
			const rows = [];
			for (const tid of this.outlook.teams.keys()) {
				rows.push({ tid, v: this.value(tid, await this.holdingsOf(tid)) });
			}
			this.keenestCache = rows
				.sort((a, b) => b.v - a.v)
				.slice(0, smartSettings.keenest)
				.map((r) => r.tid);
		}
		return this.keenestCache;
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

	// A team's holdings with `out` gone and `inn` added.
	async swapped(tid: number, out: Holdings, inn: Holdings): Promise<Holdings> {
		const h = await this.holdingsOf(tid);
		const outP = new Set(out.players.map((p) => p.pid));
		const outD = new Set(out.picks.map((dp) => dp.dpid));
		return {
			players: [
				...h.players.filter((p) => !outP.has(p.pid)),
				...inn.players.filter((p) => !h.players.some((x) => x.pid === p.pid)),
			],
			picks: [
				...h.picks.filter((dp) => !outD.has(dp.dpid)),
				...inn.picks.filter((dp) => !h.picks.some((x) => x.dpid === dp.dpid)),
			],
		};
	}

	// What these assets do to a team's goal: its use for them, whether it
	// holds them (what it would lose) or not (what it would gain).
	async useOf(tid: number, a: Holdings) {
		await ensureRetention(this.outlook, a.players, tid);
		const h = await this.holdingsOf(tid);
		const held =
			a.players.some((p) => p.tid === tid) ||
			a.picks.some((dp) => dp.tid === tid);
		const without = await this.swapped(tid, a, { players: [], picks: [] });
		const withThem = await this.swapped(tid, { players: [], picks: [] }, a);
		return held
			? this.value(tid, h) - this.value(tid, without)
			: this.value(tid, withThem) - this.value(tid, h);
	}

	// A package's market price: the second-highest use for all of it among
	// the keenest teams other than `not`.
	async price(a: Holdings, not: readonly number[]) {
		if (a.players.length === 0 && a.picks.length === 0) {
			return 0;
		}
		const bids = [];
		for (const tid of await this.keenest()) {
			if (!not.includes(tid)) {
				bids.push(await this.useOf(tid, a));
			}
		}
		bids.sort((x, y) => y - x);
		return bids[1] ?? bids[0] ?? 0;
	}

	// A team's change in worth from giving `out` and getting `inn` (from
	// `partner`): its goal, and each side at least its market price.
	async dealWorth(tid: number, partner: number, out: Holdings, inn: Holdings) {
		await ensureRetention(this.outlook, inn.players, tid);
		const h = await this.holdingsOf(tid);
		const dG =
			this.value(tid, await this.swapped(tid, out, inn)) - this.value(tid, h);
		const useIn = await this.useOf(tid, inn);
		const useOut = await this.useOf(tid, out);
		const not = [tid, partner];
		return (
			dG +
			Math.max(0, (await this.price(inn, not)) - useIn) -
			Math.max(0, (await this.price(out, not)) - useOut)
		);
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
