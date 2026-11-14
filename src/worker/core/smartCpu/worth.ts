import { PHASE } from "../../../common/constants.ts";
import type { DraftPick, Player } from "../../../common/types.ts";
import { idb } from "../../db/index.ts";
import { g, helpers } from "../../util/index.ts";
import { contractOf, draftedAs } from "../realisticFrontOffice/contractOf.ts";
import isUntradable from "../trade/isUntradable.ts";
import summary from "../trade/summary.ts";
import { lap, mark, valueCount } from "./clock.ts";
import { salaryRulesAllow } from "./tradeRules.ts";
import { tradedAwayOf } from "../realisticFrontOffice/market.ts";
import {
	moodShiftFromTrading,
	type TradedAway,
} from "../realisticFrontOffice/tradePenalty.ts";
import getPayroll from "../team/getPayroll.ts";
import {
	ensureRetention,
	expectedPlace,
	finishWeights,
	franchiseValue,
	PATIENCE,
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
	offerSize: 4,
	// A buyer's nearest picks considered for an offer.
	offerPicks: 4,
	// Players of a buyer's considered for an offer, best first.
	offerCandidates: 8,
	// Players past offerSize a buyer may add to match salaries.
	fillers: 2,
	// A roster over the limit cuts one of its lowest-valued players, the one
	// whose release costs it least.
	// Two: each tried costs a franchise value, on every roster over the limit
	// (in the preseason, nearly every deal's), and the two lowest valued
	// hold the cut nearly always (try 1 cut the lowest outright).
	cutCandidates: 2,
	// Whom a team lets go of a player it would bring in, decided once on what
	// it holds with him (SmartView.withIncoming) - not afresh on every roster.
	decideOnce: true,
};

// How the real offers searched for came out (for tests and the arena).
export const offerStats = {
	tried: 0,
	empty: 0,
	// No legal offer: not enough salary to send back, or the players that
	// would match it cost the buyer more than the deal gains it.
	cantMatch: 0,
	fillersTooDear: 0,
	illegal: 0,
	buyerSaysNo: 0,
};

const plus = (h: Holdings, x: Player | DraftPick): Holdings =>
	"pid" in x
		? { players: [...h.players, x], picks: h.picks }
		: { players: h.players, picks: [...h.picks, x] };

// Players traded away as the game counts them for its players' mood
// (trade/processTrade.ts): a bad player hardly at all, one just drafted not
// at all.
export const tradedAwayCount = (players: readonly Player[]) => {
	const phase = g.get("phase");
	const season = g.get("season");
	let n = 0;
	for (const p of players) {
		if (!helpers.justDrafted(draftedAs(p), phase, season)) {
			n += helpers.sigmoid(p.valueNoPot / 100, 30, 0.47);
		}
	}
	return n;
};

// Money a team would owe players it released, on top of what it owes now.
export type Dead = readonly { amount: number; exp: number }[];

const salaryOf = (players: readonly Player[]) =>
	players.reduce((sum, p) => sum + contractOf(p).amount, 0);

// A player as the GM keys him: who, and on what terms.
const termsOf = (p: Player) => {
	const c = contractOf(p);
	return `${p.pid}:${c.amount}:${c.exp}`;
};

// Prices against places (0 the best finish), as the best decreasing fit
// (pooling neighbours that rise), then at each place 0 to n - 1: between
// the nearest fitted places, and flat beyond the ends.
const decreasingAt = (rows: { x: number; p: number }[], n: number) => {
	const sorted = [...rows].sort((a, b) => a.x - b.x);
	const blocks: { x: number; p: number; w: number }[] = [];
	for (const r of sorted) {
		blocks.push({ x: r.x, p: r.p, w: 1 });
		while (blocks.length > 1 && blocks.at(-2)!.p < blocks.at(-1)!.p) {
			const b = blocks.pop()!;
			const a = blocks.pop()!;
			const w = a.w + b.w;
			blocks.push({
				x: (a.x * a.w + b.x * b.w) / w,
				p: (a.p * a.w + b.p * b.w) / w,
				w,
			});
		}
	}
	const at: number[] = [];
	for (let k = 0; k < n; k++) {
		if (blocks.length === 0) {
			at.push(0);
			continue;
		}
		const j = blocks.findIndex((b) => b.x >= k);
		if (j === 0) {
			at.push(blocks[0]!.p);
		} else if (j === -1) {
			at.push(blocks.at(-1)!.p);
		} else {
			const a = blocks[j - 1]!;
			const b = blocks[j]!;
			const t = b.x === a.x ? 0 : (k - a.x) / (b.x - a.x);
			at.push(a.p + t * (b.p - a.p));
		}
	}
	return at;
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
	private keenestCache:
		| {
				sig: string;
				tids: number[];
				// What a pick in the next draft fetches, by round and by where
				// its team is expected to finish (pickFloorNow).
				slots?: Map<number, number[]>;
		  }
		| undefined;

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
		// It changes only as the team trades, and so what it holds.
		this.tradedAway.set(tid, await tradedAwayOf(tid));
		return h;
	}

	// Players each team has traded away, as the game counts them for its
	// players' mood (this season, the last two).
	private tradedAway = new Map<number, TradedAway>();

	// THE TRADE PENALTY: a team that trades away many players lowers the mood
	// of the players it keeps, and so its chance of re-signing them (the
	// game's mood, "trades": past about five players a season, each one more
	// counts; tradePenalty.ts). What trading these players away does to it,
	// or nothing if it costs nothing.
	private tradingFor(tid: number, gone: number) {
		const tradedAway = this.tradedAway.get(tid);
		if (!tradedAway || !(gone > 0)) {
			return undefined;
		}
		for (let y = 0; y < 3; y++) {
			if (moodShiftFromTrading(tradedAway, gone, y) !== 0) {
				return { tradedAway, extra: gone };
			}
		}
		return undefined;
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
	private raw(tid: number, h: Holdings, dead: Dead, gone = 0) {
		const own = this.holdings.get(tid);
		const ownSig = own?.sig ?? "";
		const trading = this.tradingFor(tid, gone);
		const sig = this.keyOf(h, dead);
		const key = `${tid}#${ownSig}#${sig}${trading ? `#gone${gone}` : ""}`;
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
		const decided: { letGo?: LetGoDecisions; after?: () => void } =
			known && own && smartSettings.decideOnce
				? this.withIncoming(tid, own, known.known, h, dead)
				: { letGo: known?.known };
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
		valueCount.n += 1;
		v = franchiseValue(
			outlook,
			teamStrengths(
				outlook,
				tid,
				h.players,
				h.picks,
				trading,
				undefined,
				decided.letGo ?? recording,
			),
			tid,
		);
		decided.after?.();
		if (recording) {
			recording.record = false;
			this.letGo.set(tid, { sig: ownSig, known: recording });
		}
		this.values.set(key, v);
		return v;
	}

	// Whom a team lets go of a player or pick it would bring in: decided once,
	// on what it holds with him, and kept for every roster of its he is on -
	// as it decides on its own players once, on what it holds. Deciding afresh
	// on each roster was 45% of a market moment's time (a veteran's re-signing
	// is weighed over every season ahead, and a deal's rosters are many).
	private incoming = new Map<
		string,
		Map<string, { gone: boolean; room: number }>
	>();
	// What each team holds, by its players' terms and its picks, while it
	// holds it.
	private heldSets = new Map<
		number,
		{ sig: string; terms: Set<string>; picks: Set<number> }
	>();
	private withIncoming(
		tid: number,
		own: { sig: string; h: Holdings },
		known: LetGoDecisions,
		h: Holdings,
		dead: Dead,
	): { letGo: LetGoDecisions; after?: () => void } {
		let held = this.heldSets.get(tid);
		if (held?.sig !== own.sig) {
			held = {
				sig: own.sig,
				terms: new Set(own.h.players.map(termsOf)),
				picks: new Set(own.h.picks.map((dp) => dp.dpid)),
			};
			this.heldSets.set(tid, held);
		}
		const ownTerms = held.terms;
		const ownPicks = held.picks;
		const arriving: Holdings[] = [
			...h.players
				.filter((p) => !ownTerms.has(termsOf(p)))
				.map((p) => ({ players: [p], picks: [] })),
			...h.picks
				.filter((dp) => !ownPicks.has(dp.dpid))
				.map((dp) => ({ players: [], picks: [dp] })),
		];
		if (arriving.length === 0) {
			return { letGo: known };
		}
		const none = { players: [], picks: [] };
		// Its decisions on him once made, from a roster it decided them on.
		const addedFrom = (trying: LetGoDecisions) => {
			const added = new Map<string, { gone: boolean; room: number }>();
			for (const [k, d] of trying.decisions) {
				if (known.decisions.get(k) !== d) {
					added.set(k, d);
				}
			}
			return added;
		};
		const keyFor = (a: Holdings) => `${tid}#${own.sig}#${this.keyOf(a)}`;
		// The roster valued is the one to decide on - what it holds with him
		// alone (a signing, nearly always): decided as it is valued.
		const only = arriving.length === 1 ? arriving[0]! : undefined;
		if (
			only &&
			dead.length === 0 &&
			!this.incoming.has(keyFor(only)) &&
			this.keyOf(this.swappedFrom(own.h, none, only)) === this.keyOf(h)
		) {
			const trying: LetGoDecisions = {
				decisions: new Map(known.decisions),
				record: false,
				addFresh: true,
			};
			return {
				letGo: trying,
				after: () => this.incoming.set(keyFor(only), addedFrom(trying)),
			};
		}
		const decisions = new Map(known.decisions);
		for (const a of arriving) {
			const key = keyFor(a);
			let added = this.incoming.get(key);
			if (!added) {
				const trying: LetGoDecisions = {
					decisions: new Map(known.decisions),
					record: false,
					addFresh: true,
				};
				const withHim = this.swappedFrom(own.h, none, a);
				valueCount.n += 1;
				teamStrengths(
					this.outlook,
					tid,
					withHim.players,
					withHim.picks,
					undefined,
					undefined,
					trying,
					true,
				);
				added = addedFrom(trying);
				this.incoming.set(key, added);
			}
			for (const [k, d] of added) {
				decisions.set(k, d);
			}
		}
		return { letGo: { decisions, record: false } };
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
	// `gone`: how many players it trades away to hold this, as the game counts
	// them (tradedAwayCount) - what that costs it in its players' mood.
	value(tid: number, h: Holdings, dead: Dead = [], gone = 0): number {
		return this.raw(tid, ...this.afterCuts(tid, h, dead, gone), gone);
	}

	// The holdings and money owed once a roster over the limit is cut to it,
	// one release at a time, each the one that costs the team least.
	afterCuts(
		tid: number,
		h: Holdings,
		dead: Dead = [],
		gone = 0,
	): [Holdings, Dead] {
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
				const v = this.raw(tid, next, nextDead, gone);
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

	// A team's payroll as the trade rules count it (with players released),
	// kept while what it holds is as it was.
	private tradePayrolls = new Map<number, { sig: string; payroll: number }>();
	async tradePayroll(tid: number) {
		await this.holdingsOf(tid);
		const sig = this.holdings.get(tid)!.sig;
		const had = this.tradePayrolls.get(tid);
		if (had && had.sig === sig) {
			return had.payroll;
		}
		const payroll = await getPayroll(tid);
		this.tradePayrolls.set(tid, { sig, payroll });
		return payroll;
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

	// A rough read of the same, to rank what to look at closely: the roster
	// it would have, not yet cut to the limit (each cut weighed is two more
	// franchise values; what a deal is worth is always worked out in full).
	roughUseOfNow(tid: number, a: Holdings, h = this.heldNow(tid)) {
		const none = { players: [], picks: [] };
		const held =
			a.players.some((p) => h.players.some((x) => x.pid === p.pid)) ||
			a.picks.some((dp) => h.picks.some((x) => x.dpid === dp.dpid));
		return held
			? this.raw(tid, h, []) - this.raw(tid, this.swappedFrom(h, a, none), [])
			: this.raw(tid, this.swappedFrom(h, none, a), []) - this.raw(tid, h, []);
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

	// A team's payroll once games are played: its players under contract then
	// and what it owes players it released.
	payrollNow(tid: number, h = this.heldNow(tid)) {
		const phase = g.get("phase");
		const first = g.get("season") + (phase > PHASE.PLAYOFFS ? 1 : 0);
		return (
			salaryOf(h.players.filter((p) => contractOf(p).exp >= first)) +
			(this.outlook.deadMoney.get(tid) ?? [])
				.filter((d) => d.exp >= first)
				.reduce((sum, d) => sum + d.amount, 0)
		);
	}

	// Whether the trade rules let a team give `out` and take `inn`, on its
	// salaries (trade/summary.ts: over the cap after it, a team may take in
	// no more than softCapTradeSalaryMatch% of what it sends; under a hard
	// cap, nothing that leaves it over and taking on money).
	salariesWork(tid: number, out: Holdings, inn: Holdings) {
		const capType = g.get("salaryCapType");
		if (capType === "none") {
			return true;
		}
		const sent = salaryOf(out.players);
		const taken = salaryOf(inn.players);
		const after = this.payrollNow(tid) + taken - sent;
		if (after <= g.get("salaryCap")) {
			return true;
		}
		if (capType === "hard") {
			return taken <= sent;
		}
		return sent > 0
			? Math.floor((100 * taken) / sent) <= g.get("softCapTradeSalaryMatch")
			: taken === 0;
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
		const incoming = salaryOf(a.players);
		const capType = g.get("salaryCapType");
		const none = { players: [], picks: [] };
		if (incoming <= 0 || capType === "none") {
			return this.useOfNow(tid, a, h);
		}
		const payroll = this.payrollNow(tid, h);
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
			this.value(
				tid,
				this.swappedFrom(h, { players: fillers, picks: [] }, a),
				[],
				tradedAwayCount(fillers),
			) - this.value(tid, this.swappedFrom(h, none, none));
		return net >= 0 ? use : -Infinity;
	}

	// A package's market price: the second-highest real bid for all of it
	// among the keenest teams other than `not` - and for picks in later
	// drafts, at least what they will fetch once their draft is next
	// (pickFloorNow).
	priceNow(a: Holdings, not: readonly number[]) {
		const bid = this.bidPriceNow(a, not);
		const later = a.picks.filter((dp) => this.pickFloorNow(dp) > 0);
		if (later.length === 0) {
			return bid;
		}
		const rest = {
			players: a.players,
			picks: a.picks.filter((dp) => !later.includes(dp)),
		};
		let floor = this.bidPriceNow(rest, not);
		for (const dp of later) {
			floor += this.pickFloorNow(dp);
		}
		return Math.max(bid, floor);
	}

	// The draft whose picks are next to be made.
	private nextDraft() {
		return g.get("season") + (g.get("phase") >= PHASE.DRAFT ? 1 : 0);
	}

	// What a pick in a later draft is worth to any team holding it: what a
	// pick where its team is expected to finish fetches in the next draft
	// now, discounted for the seasons until its own draft - a team can always
	// keep it and sell it then. Without it, a pick several drafts away sold
	// for a tenth of what it would fetch once its draft came (the title
	// chances it brings fall outside the seasons the outlook sees), and a GM
	// gave every first it had for veterans. 0 for the next draft's picks.
	pickFloorNow(dp: DraftPick) {
		const next = this.nextDraft();
		if (typeof dp.season !== "number" || dp.season <= next || dp.pick > 0) {
			return 0;
		}
		const slots = this.slotPricesNow(dp.round);
		if (!slots) {
			return 0;
		}
		const weights = finishWeights(this.outlook, dp);
		let price = 0;
		for (const [k, w] of weights.entries()) {
			price += w * (slots[k] ?? 0);
		}
		return PATIENCE ** (dp.season - next) * price;
	}

	// The next draft's picks in a round, priced, by where each team is
	// expected to finish: decreasing (a better finish never fetches more),
	// place by place in between. Kept as long as the prices' bidders are.
	private slotPricesNow(round: number) {
		const keen = this.keenestCache;
		if (!keen) {
			return undefined;
		}
		keen.slots ??= new Map();
		let slots = keen.slots.get(round);
		if (!slots) {
			const next = this.nextDraft();
			const rows: { x: number; p: number }[] = [];
			for (const tid of this.outlook.teams.keys()) {
				for (const dp of this.heldNow(tid).picks) {
					if (dp.season === next && dp.round === round) {
						const p = this.bidPriceNow({ players: [], picks: [dp] }, [tid]);
						if (Number.isFinite(p)) {
							rows.push({ x: expectedPlace(this.outlook, dp), p });
						}
					}
				}
			}
			slots = decreasingAt(rows, this.outlook.numTeams);
			keen.slots.set(round, slots);
		}
		return slots;
	}

	// The second-highest real bid for all of a package among the keenest
	// teams other than `not`.
	private bidPriceNow(a: Holdings, not: readonly number[]) {
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
		const gone = tradedAwayCount(out.players);
		const dG = this.value(tid, after, [], gone) - this.value(tid, h);
		const [cut] = this.afterCuts(tid, after, [], gone);
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

	// A buyer's real offer for what a seller gives: its players and nearest
	// picks, the seller's favourite first, as many as still leave it better
	// off by its goal, within the trade rules on salary (what it would miss
	// least added to match), and good for it by its full worth - or
	// nothing. The market's offers, and the trade screens' (smartCpu/
	// screens.ts).
	async offerFrom(buyer: number, seller: number, out: Holdings) {
		const s = smartSettings;
		await ensureRetention(this.outlook, out.players, buyer);
		const held = await this.holdingsOf(buyer);
		const m1 = mark();
		// Its players and its nearest picks (as NBA teams pay: a deal
		// without picks was every one of the GM's first 72), best for the
		// seller first.
		const scored: { x: Player | DraftPick; v: number }[] = [];
		for (const p of held.players.filter((x) => !isUntradable(x).untradable)) {
			scored.push({
				x: p,
				v: await this.useOf(seller, { players: [p], picks: [] }),
			});
		}
		scored.sort((a, b) => b.v - a.v);
		const candidates: { x: Player | DraftPick; v: number }[] = scored.slice(
			0,
			s.offerCandidates,
		);
		for (const dp of [...held.picks]
			.sort(
				(a, b) =>
					(a.season as number) - (b.season as number) ||
					a.round - b.round ||
					a.dpid - b.dpid,
			)
			.slice(0, s.offerPicks)) {
			candidates.push({
				x: dp,
				v: await this.useOf(seller, { players: [], picks: [dp] }),
			});
		}
		candidates.sort((a, b) => b.v - a.v);
		lap("offer: scoring", m1);
		const m2 = mark();
		// Within the trade rules on salary: the seller takes in no more than
		// they let it, and the buyer sends back as much as they make it.
		const taken = salaryOf(out.players);
		const sellerPayroll = await this.tradePayroll(seller);
		const buyerPayroll = await this.tradePayroll(buyer);
		const sellerMayTake = (o: Holdings) =>
			salaryRulesAllow(seller, sellerPayroll, salaryOf(o.players), taken);
		const buyerMatches = (o: Holdings) =>
			salaryRulesAllow(buyer, buyerPayroll, taken, salaryOf(o.players));
		// Searching, the goal alone (the full worth is checked below). Its
		// holdings are read, and what it would get has its chance of
		// staying with it (its want, above).
		const gains = (o: Holdings) =>
			this.value(
				buyer,
				this.swappedFrom(held, o, out),
				[],
				tradedAwayCount(o.players),
			) -
				this.value(buyer, held) >
			0;
		let offer: Holdings = { players: [], picks: [] };
		for (const { x } of candidates) {
			if (offer.players.length + offer.picks.length >= s.offerSize) {
				break;
			}
			const next = plus(offer, x);
			if (sellerMayTake(next) && gains(next)) {
				offer = next;
			}
		}
		offerStats.tried += 1;
		if (offer.players.length === 0 && offer.picks.length === 0) {
			offerStats.empty += 1;
			lap("offer: greedy", m2);
			return undefined;
		}
		// Short of salary to send back: the players it would miss least,
		// as many as the rules need (as NBA teams fill a trade), if it still
		// gains.
		if (!buyerMatches(offer)) {
			const uses = this.usesOf(buyer, held);
			for (const p of held.players
				.filter(
					(x) =>
						!isUntradable(x).untradable &&
						!offer.players.some((y) => y.pid === x.pid),
				)
				.sort((a, b) => uses.get(a.pid)! - uses.get(b.pid)! || a.pid - b.pid)) {
				if (
					buyerMatches(offer) ||
					offer.players.length + offer.picks.length >= s.offerSize + s.fillers
				) {
					break;
				}
				const next = plus(offer, p);
				if (sellerMayTake(next)) {
					offer = next;
				}
			}
			if (!buyerMatches(offer)) {
				offerStats.cantMatch += 1;
				lap("offer: greedy", m2);
				return undefined;
			}
			if (!gains(offer)) {
				offerStats.fillersTooDear += 1;
				lap("offer: greedy", m2);
				return undefined;
			}
		}
		lap("offer: greedy", m2);
		const m3 = mark();
		const legal = await summary([
			{
				tid: buyer,
				pids: offer.players.map((p) => p.pid),
				dpids: offer.picks.map((dp) => dp.dpid),
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
		const inn = offer;
		if (legal.warning) {
			offerStats.illegal += 1;
			lap("offer: checks", m3);
			return undefined;
		}
		if ((await this.dealWorth(buyer, seller, inn, out)) <= 0) {
			offerStats.buyerSaysNo += 1;
			lap("offer: checks", m3);
			return undefined;
		}
		lap("offer: checks", m3);
		return { offer: inn };
	}

	// THE MARKET: the best real offer for `out` from the CPU teams keenest on
	// it (not `seller`, not `not`): each its own players, as many as still
	// leave it better off by its own worth, legal under the trade rules, and
	// what that offer is worth to the seller.
	async bestRealOffer(
		seller: number,
		out: Holdings,
		not: number,
		// Teams that may not buy now (the market: those that dealt this moment).
		skip: ReadonlySet<number> = new Set(),
		// How many of the keenest teams are asked.
		asked = smartSettings.buyers,
		// The teams it may be put to (the market: those that want him most of
		// all teams); the most valuable franchises by default.
		among?: readonly number[],
	) {
		const m0 = mark();
		const keen = [];
		for (const tid of among ?? (await this.keenest())) {
			if (tid !== seller && !this.humans.has(tid) && !skip.has(tid)) {
				keen.push({ tid, want: await this.useOf(tid, out) });
			}
		}
		// The teams asked are the keenest on it whoever the deal is with: a
		// team judging an offer weighs the others it would hear from, not
		// the next team down (which asked one more team than the market).
		const buyers = keen
			.sort((a, b) => b.want - a.want || a.tid - b.tid)
			.slice(0, asked)
			.filter((b) => b.tid !== not);
		lap("offer: wants", m0);
		let best: { buyer: number; worth: number; offer: Holdings } | undefined;
		// The next best offer's worth to the seller: what the winner must
		// beat (the market trims its offer to about that).
		let second = Number.NEGATIVE_INFINITY;
		for (const { tid: buyer, want } of buyers) {
			if (want <= 0) {
				continue;
			}
			const made = await this.offerFrom(buyer, seller, out);
			if (!made) {
				continue;
			}
			const inn = made.offer;
			const m3 = mark();
			const worth = await this.dealWorth(seller, buyer, out, inn);
			lap("offer: checks", m3);
			if (!best || worth > best.worth) {
				if (best) {
					second = Math.max(second, best.worth);
				}
				best = { buyer, worth, offer: inn };
			} else {
				second = Math.max(second, worth);
			}
		}
		return best && { ...best, second };
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
