import { PHASE, PLAYER } from "../../../common/constants.ts";
import type { DraftPick, Player } from "../../../common/types.ts";
import { idb } from "../../db/index.ts";
import { g, helpers } from "../../util/index.ts";
import { contractOf, draftedAs } from "../realisticFrontOffice/contractOf.ts";
import isUntradable from "../trade/isUntradable.ts";
import summary from "../trade/summary.ts";
import { lap, mark, valueCount } from "./clock.ts";
import { salaryRulesAllow } from "./tradeRules.ts";
import { marginOf, tradedAwayOf } from "../realisticFrontOffice/market.ts";
import { SMART_ON_COURT_ERROR } from "../realisticFrontOffice/margin.ts";
import {
	moodShiftFromTrading,
	type TradedAway,
} from "../realisticFrontOffice/tradePenalty.ts";
import getPayroll from "../team/getPayroll.ts";
import getRookieContractLength from "../draft/getRookieContractLength.ts";
import { pickSalaries } from "../realisticFrontOffice/payroll.ts";
import {
	ensureRetention,
	expectedPlace,
	finishWeights,
	franchiseValue,
	PATIENCE,
	retentionKey,
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
	// Six: for the league's best players four pieces seldom paid - in
	// random-players leagues the best real offer for a top-30 player came to
	// a median 69% of his price, 99 of 227 turned down at four pieces. With
	// value for value and what it gets priced as the team will stand
	// (2026-11-20), two leagues played four seasons moved 12 top-30 players at
	// six, 6 at four, 5 before; firsts paid for players 27 or older rated
	// under 60, 13, 21 and 27. A market moment takes 40% longer (48 s to 67 s
	// on a desktop).
	offerSize: 6,
	// A buyer's nearest picks considered for an offer.
	offerPicks: 6,
	// Players of a buyer's considered for an offer, best first.
	offerCandidates: 8,
	// Players past offerSize a buyer may add to match salaries.
	fillers: 2,
	// Which: of the ways a buyer could make the salaries work, the one that
	// leaves it best off (SmartView.fillSalary) - not only its players it
	// would miss least, first.
	fillByDollar: true,
	// A roster over the limit cuts one of its lowest-valued players, the one
	// whose release costs it least.
	// Two: each tried costs a franchise value, on every roster over the limit
	// (in the preseason, nearly every deal's), and the two lowest valued
	// hold the cut nearly always (try 1 cut the lowest outright).
	cutCandidates: 2,
	// Whom a team lets go of a player it would bring in, decided once on what
	// it holds with him (SmartView.withIncoming) - not afresh on every roster.
	decideOnce: true,
	// What a team counts (titles) for each player it trades away within a
	// year of trading for him, and each it buys back within a year of trading
	// him away: a cost, not a lock (owner's call). Without, 77 players were
	// traded again within a year and Devin Booker played for four teams in one
	// season (the owner's league). At 0.005, contenders still flipped a
	// player within weeks for a gain the size of the model's noise - Walker
	// Kessler went through GSW twice in a season - and half the flips gained
	// the flipping side under 0.009.
	flipCost: 0.01,
	// And for a flip right after the trade, this many times more again, down
	// to none flipRecentSeasons on. At the flat cost alone, contenders on
	// vanilla's rules still passed players straight along all season - in a
	// third of the deals in a busy season the team gave up a player it had got
	// that season, Kendall Brown was traded four times in one, and the side
	// that flipped gained a median 2.4% of a title by it (the owner's league,
	// 2026-11-17). Still a cost, not a lock.
	flipRecent: 5,
	flipRecentSeasons: 0.5,
	// A pick worth at least what the market would pay for the prospect it is
	// expected to land, on the rookie deal of his slot, off its draft's own
	// class (the next draft's for drafts whose class isn't out). Priced by
	// what a typical pick becomes, the 2033 firsts fetched 2-11% of a title
	// while the 2032 first-rounders they became fetched 10-63%: humans could
	// buy CPU picks for little and sell the rookies back dear, and contenders
	// gave firsts away for bench players (the owner's league).
	// On, the owner's league's next-draft firsts (2025) fetched 23-26% of a
	// title where a top-three pick was expected and 2-4% late, against 19-27%
	// and about 4% for the previous draft's rookies at those places (off:
	// 7% and 1-1.6%), and teams traded 8 firsts a season, not 21.
	picksAsRookies: true,
	// A pick in a draft past the next is priced by the next draft's class,
	// discounted a season for each draft between, never by its own class:
	// prospects years from their draft can't be judged, and in a real-players
	// league the classes for every draft to come are in the file. Priced by
	// their own classes, undiscounted, picks in later drafts fetched several
	// times what any team bid for them and more than the next draft's: in
	// League 10 (2019) MIL's 2021 first 0.71 of a title against bids of
	// 0.08, its 2019 first 0.13; a 2021 second 0.25 against 0.01. Teams out
	// of the race took them for young players as if they were worth that.
	laterPicksByNextClass: true,
	// What a team gets from another team in a trade counts at its own use for
	// the players, and above it only for the picks (their floor stands in for
	// title chances past the outlook's seasons); what it gives still counts
	// at least at its market price. A price is the second-highest keenest
	// team's use - what it would gain, not what it can pay. The market's best
	// real offers come to a quarter to three fifths of it (League 10, 2019).
	// Counting players got at it, teams out of the race traded on the gap
	// alone: in every deal of a League 7 season and of offseasons in Leagues 8
	// and 10, the seller's own goal moved -0.02 to +0.02 of a title and the
	// rest of its gain was the price of what it got (docs/smart-cpu.md).
	boughtAtUse: true,
	// VALUE FOR VALUE: and the players it gets count at their price as far as
	// that makes up for the price of what it gives - never past it, so no
	// deal is made on the gap between players' prices and their use alone.
	// At their use only, a team out of the race could sell its star only for
	// picks: in random-players leagues every best real offer for a top-30
	// player was worth -0.1 to -1.5 of a title to his team - Washington's 23-
	// year-old 68 (0.16 of use to it, a price of 0.82) for two young players
	// and two firsts priced 0.71 together (0.38 of use to it, its own goal up
	// 0.05) was -0.57, the players' price counted at nothing - while the buyer
	// would have gained 1.5 (2026-11-20).
	valueForValue: true,
	// What a team gets priced past its use to the team as it will stand once
	// it has given what it gives - in its place, not beside it. Beside it, a
	// package that replaced a star read as of more use to the seller than it
	// would be, and so its price past that as less.
	gotAsItWillStand: true,
	// What a deal gains a team in the market's prices - what it gets past
	// its use to it, past what it gives - counts toward the deal clearing its
	// margin of error only at this share (SmartView.sureWorthNow); a price
	// given up counts in full. A price is what the keenest teams would gain,
	// and the market's best real offers come to a quarter to three fifths of
	// it (League 10). Counted whole, teams out of the race sold the young
	// players they had for picks years away from the favourites, their own
	// goal falling - in a League 12 run from the 2018 draft Golden State gave
	// Cedi Osman (23, 56/65) for Dallas's 2022 first (-0.014 by its goal, the
	// pick 0.175 in price against 0.010 of use), Milwaukee Tyler Dorsey (23,
	// 54/62) for four scrubs and Dallas's 2023 picks (-0.118), Cleveland
	// Patrick McCaw for John Henson and a 2022 first (-0.037) - and took
	// players they had no use for with a second thrown in.
	// A team drafts by the same sure worth: the prospect's use to it, and his
	// price past that at this share. Priced whole, every team drafted by what
	// contenders - the keenest teams, whose gain sets a price - want: players
	// ready now, older and with less upside (League 12: Abrines, 23, 34th on
	// the board, at #7; Huerter, 16th, at #4); by its use alone a bad team
	// passed on the draft's best prospect when he would likely not re-sign
	// with it, as if it could not trade him.
	premiumRealized: 0.25,
};

// Seasons, with the phases between (a season's phases run 0 to 8).
const whenOf = (season: number, phase: number) =>
	season + Math.max(0, phase) / 9;

// Flips a deal makes for a team: players it gives away that it traded for
// within the past year, and players it takes back that it traded away
// within the past year - each the more the sooner after that trade it comes
// (smartSettings.flipRecent).
export const flipsIn = (
	tid: number,
	out: readonly Player[],
	inn: readonly Player[],
) => {
	const now = whenOf(g.get("season"), g.get("phase"));
	const since = (x: { season: number; phase: number }) =>
		now - whenOf(x.season, x.phase);
	const weight = (x: { season: number; phase: number }) =>
		1 +
		smartSettings.flipRecent *
			Math.max(0, 1 - since(x) / smartSettings.flipRecentSeasons);
	let n = 0;
	for (const p of out) {
		const last = p.transactions?.at(-1);
		if (last?.type === "trade" && last.tid === tid && since(last) < 1) {
			n += weight(last);
		}
	}
	for (const p of inn) {
		const back = p.transactions?.findLast(
			(x) => x.type === "trade" && x.fromTid === tid && since(x) < 1,
		);
		if (back) {
			n += weight(back);
		}
	}
	return n;
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

// What a seller is after in an offer (the trading block's "What are you
// looking for?", smartCpu/screens.ts): the buyer starts with the first of
// `lead` it would give - in `rank` order, the seller's favourite first
// among equals - and with none it makes no offer; then any of `next` it
// would give, then the rest as always; and nothing it puts in fails
// `allowed`.
export type OfferAsk = {
	lead?: (x: Player | DraftPick) => boolean;
	rank?: (x: Player | DraftPick) => number;
	next?: (x: Player | DraftPick) => boolean;
	allowed?: (p: Player) => boolean;
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
	// Each team's strength path on its holdings, and deals' margins.
	private paths = new Map<string, number[]>();
	private margins = new Map<string, number>();
	private keenestCache:
		| {
				sig: string;
				tids: number[];
				// What a pick in the next draft fetches, by round and by where
				// its team is expected to finish (pickFloorNow).
				slots?: Map<number, number[]>;
				// What each draft's prospects fetch as rookies, best first
				// (picksAsRookies), by draft.
				classes?: Map<number, number[]>;
				// The prospects the classes were priced from.
				prospects?: string;
				// Whose bids the prices are: the keenest teams, each with what
				// it holds. A team's bid is its own worth, so the tables stand
				// while the bidders do, whatever other teams sign or trade.
				bidders: string;
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
			const tids = rows
				.map(({ tid, h }) => ({ tid, v: this.value(tid, h) }))
				.sort((a, b) => b.v - a.v)
				.slice(0, smartSettings.keenest)
				.map((r) => r.tid);
			const bidders = tids
				.map((tid) => `${tid}=${this.holdings.get(tid)!.sig}`)
				.join("/");
			const had = this.keenestCache;
			this.keenestCache =
				had?.bidders === bidders ? { ...had, sig } : { sig, tids, bidders };
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
		await this.classPrices();
	}

	// Each coming draft's prospects priced as rookies on the deal of the slot
	// they would go at (by value), best first - what a pick there fetches as
	// the player it becomes (picksAsRookies). Kept as long as the prices'
	// bidders are.
	async classPrices() {
		if (!smartSettings.picksAsRookies) {
			return;
		}
		const tids = await this.keenest();
		const keen = this.keenestCache!;
		const prospects = await idb.cache.players.indexGetAll(
			"playersByTid",
			PLAYER.UNDRAFTED,
		);
		// The classes as they stand: in the draft, each pick takes one out.
		const pool = `${this.nextDraft()}:${prospects
			.map((p) => `${p.pid}:${p.value}`)
			.sort()
			.join(",")}`;
		if (keen.classes && keen.prospects === pool) {
			return;
		}
		const timer = mark();
		const classes = new Map<number, number[]>();
		const n = this.outlook.numTeams;
		const numPicks = n * g.get("numDraftRounds");
		const salary = pickSalaries();
		for (const season of new Set(prospects.map((p) => p.draft.year))) {
			if (
				season < this.nextDraft() - (g.get("phase") >= PHASE.DRAFT ? 1 : 0) ||
				(smartSettings.laterPicksByNextClass && season > this.nextDraft())
			) {
				continue;
			}
			const rookies = prospects
				.filter((p) => p.draft.year === season)
				.sort((a, b) => b.value - a.value || a.pid - b.pid)
				.slice(0, numPicks)
				.map((p, slot) => {
					const round = Math.floor(slot / n) + 1;
					return {
						...p,
						contractDemand: {
							amount: salary({ slot, round, years: 1, ovr: 0 }),
							exp: season + getRookieContractLength(round),
							rookie: true,
						},
					} as Player;
				});
			for (const tid of tids) {
				await ensureRetention(this.outlook, rookies, tid);
			}
			classes.set(
				season,
				rookies
					.map((r) =>
						Math.max(0, this.bidPriceNow({ players: [r], picks: [] }, [])),
					)
					.sort((a, b) => b - a),
			);
		}
		keen.classes = classes;
		keen.prospects = pool;
		lap("class prices", timer);
	}

	// What a pick fetches as the player it is expected to land (picksAsRookies):
	// its draft's class for the next draft (or the one under way), else the
	// next draft's (laterPicksByNextClass, or a draft whose class isn't out),
	// discounted for the seasons until its draft. Undefined with it off.
	private rookieFloorNow(dp: DraftPick) {
		const classes = this.keenestCache?.classes;
		if (!smartSettings.picksAsRookies || !classes || classes.size === 0) {
			return undefined;
		}
		const next = this.nextDraft();
		const own =
			smartSettings.laterPicksByNextClass && (dp.season as number) > next
				? undefined
				: classes.get(dp.season as number);
		const prices = own ?? classes.get(next) ?? [...classes.values()][0]!;
		const n = this.outlook.numTeams;
		const at = (k: number) => prices[(dp.round - 1) * n + k] ?? 0;
		let price: number;
		if (dp.pick > 0) {
			price = at(dp.pick - 1);
		} else {
			price = 0;
			for (const [k, w] of finishWeights(this.outlook, dp).entries()) {
				price += w * at(k);
			}
		}
		const later = Math.max(0, (dp.season as number) - next);
		return (own ? 1 : PATIENCE ** later) * price;
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

	// THE MARGIN OF ERROR (marketSettings.margins): how far off this team's
	// own read of giving `give` for `get` can be, in titles - each player who
	// moves, his on-court value and his career, and each pick, through every
	// season he is expected to be there, by what a point is worth to this
	// team then (realisticFrontOffice/margin.ts, by its own curves).
	marginNow(tid: number, give: Holdings, get: Holdings) {
		const h = this.heldNow(tid);
		const sig = this.keyOf(h);
		const key = `${tid}#${sig}#${this.keyOf(give)}#${this.keyOf(get)}`;
		let m = this.margins.get(key);
		if (m !== undefined) {
			return m;
		}
		let path = this.paths.get(`${tid}#${sig}`);
		if (!path) {
			path = teamStrengths(this.outlook, tid, h.players, h.picks);
			this.paths.set(`${tid}#${sig}`, path);
		}
		const assets = (x: Holdings) => [
			...x.players.map((p) => ({ kind: "player" as const, p })),
			...x.picks.map((dp) => ({ kind: "pick" as const, dp })),
		];
		m = marginOf(
			this.outlook,
			{ tid, players: h.players, picks: h.picks, path },
			assets(give),
			assets(get),
			{ tid, onCourtError: SMART_ON_COURT_ERROR },
		);
		this.margins.set(key, m);
		return m;
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
		if (typeof dp.season !== "number") {
			return 0;
		}
		const asRookie = this.rookieFloorNow(dp);
		if (dp.season <= next || dp.pick > 0) {
			return asRookie ?? 0;
		}
		const slots = this.slotPricesNow(dp.round);
		if (!slots) {
			return asRookie ?? 0;
		}
		const weights = finishWeights(this.outlook, dp);
		let price = 0;
		for (const [k, w] of weights.entries()) {
			price += w * (slots[k] ?? 0);
		}
		return Math.max(asRookie ?? 0, PATIENCE ** (dp.season - next) * price);
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
			const timer = mark();
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
			lap("slot prices", timer);
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
		if (a.picks.length > 0) {
			await this.classPrices();
		}
		for (const tid of await this.keenest()) {
			if (!not.includes(tid)) {
				await ensureRetention(this.outlook, a.players, tid);
			}
		}
		return this.priceNow(a, not);
	}

	// A team's change in worth from giving `out` and getting `inn` (from
	// `partner`): its goal, and what it gives at least its market price -
	// what it gets too, as far as it keeps it once its roster is cut to the
	// limit (a player it must release it can't sell), but of what it gets
	// from another team only its picks (boughtAtUse; a drafted prospect is
	// priced whole - the draft goes by the share of it a team can be sure of,
	// sureWorthNow).
	dealWorthNow(
		tid: number,
		partner: number,
		out: Holdings,
		inn: Holdings,
		h = this.heldNow(tid),
	) {
		const { goal, got, given, flip } = this.dealPartsNow(
			tid,
			partner,
			out,
			inn,
			h,
		);
		return goal + got - given - flip;
	}

	// The same, its parts: the team's goal, the premiums on what it gets and
	// gives, and the cost of flipping.
	dealPartsNow(
		tid: number,
		partner: number,
		out: Holdings,
		inn: Holdings,
		h = this.heldNow(tid),
	) {
		const after = this.swappedFrom(h, out, inn);
		const gone = tradedAwayCount(out.players);
		const goal = this.value(tid, after, [], gone) - this.value(tid, h);
		const [cut] = this.afterCuts(tid, after, [], gone);
		const kept = new Set(cut.players);
		const keptIn = {
			players:
				smartSettings.boughtAtUse && partner >= 0
					? []
					: inn.players.filter((p) => kept.has(p)),
			picks: inn.picks,
		};
		const not = [tid, partner];
		const given = this.premiumNow(tid, out, not, h);
		// What it gets is priced past its use to the team as it will stand -
		// in place of what it gives, not beside it (gotAsItWillStand).
		const base = smartSettings.gotAsItWillStand
			? this.swappedFrom(h, out, { players: [], picks: [] })
			: h;
		let got = this.premiumNow(tid, keptIn, not, base);
		if (
			smartSettings.valueForValue &&
			smartSettings.boughtAtUse &&
			partner >= 0 &&
			given > got
		) {
			const players = inn.players.filter((p) => kept.has(p));
			if (players.length > 0) {
				const all = this.premiumNow(
					tid,
					{ players, picks: inn.picks },
					not,
					base,
				);
				got += Math.min(Math.max(0, all - got), given - got);
			}
		}
		return {
			goal,
			got,
			given,
			flip: smartSettings.flipCost * flipsIn(tid, out.players, inn.players),
		};
	}

	// What a deal gains a team counting on no more of the market's prices than
	// it can be sure of getting (smartSettings.premiumRealized): what it gets
	// past what it gives in price, past their use to it, at that share - and
	// what it gives up in price, in full.
	sureWorthNow(
		tid: number,
		partner: number,
		out: Holdings,
		inn: Holdings,
		h = this.heldNow(tid),
	) {
		const { goal, got, given, flip } = this.dealPartsNow(
			tid,
			partner,
			out,
			inn,
			h,
		);
		const net = got - given;
		return goal + (net > 0 ? smartSettings.premiumRealized * net : net) - flip;
	}

	async sureWorth(tid: number, partner: number, out: Holdings, inn: Holdings) {
		await this.dealWorth(tid, partner, out, inn);
		return this.sureWorthNow(tid, partner, out, inn);
	}

	// What the market would pay for a package past its use to the team
	// holding it - what it gives up selling below market, or gains buying
	// below it - as far as the team can still get that price. A player whose
	// deal ends this season can be sold only until it does, and after it
	// only if he stays: his part counts as far as he stays with the team.
	// Counted whole, a team losing a player for nothing held out for a price
	// it could never get: in League 7, Otto Porter Jr. (0.25 to stay) was
	// priced at 0.67 titles against 0.03 to PHO, and no offer was enough;
	// in League 6, Cam Reddish (none) turned down four players for a price
	// of 0.25 against 0.004, and Mudiay (0.01) D'Angelo Russell.
	// A package's market price, as premiumNow counts what a team gives: its
	// price whole - or, where no keen team could take all of it under the
	// trade rules, piece by piece, each at its own price, or at its use to
	// the team where nobody could take that either. Whole, a first sent with
	// salary nobody else could absorb had no price, and its team gave it at
	// its bare use: in League 12 runs Dallas a 2024 first (priced 0.31, of use
	// 0.02 to it) with two players on the minimum for Aaron Nesmith (42/58),
	// Chicago two firsts (0.38 each) with Jabari Brown for Marvin Bagley -
	// and a young star sent with such a contract would have gone the same way.
	packagePriceNow(
		tid: number,
		x: Holdings,
		not: readonly number[],
		h = this.heldNow(tid),
	) {
		const price = this.priceNow(x, not);
		if (price > -Infinity || x.players.length + x.picks.length <= 1) {
			return price;
		}
		let sum = 0;
		const pieces: Holdings[] = [
			...x.players.map((p) => ({ players: [p], picks: [] })),
			...x.picks.map((dp) => ({ players: [], picks: [dp] })),
		];
		for (const one of pieces) {
			const each = this.priceNow(one, not);
			sum += each > -Infinity ? each : this.useOfNow(tid, one, h);
		}
		return sum;
	}

	private premiumNow(
		tid: number,
		a: Holdings,
		not: readonly number[],
		h: Holdings,
	) {
		const over = (x: Holdings) =>
			Math.max(
				0,
				this.packagePriceNow(tid, x, not, h) - this.useOfNow(tid, x, h),
			);
		const whole = over(a);
		if (!(whole > 0)) {
			return 0;
		}
		const season = g.get("season");
		let premium = whole;
		for (const p of a.players) {
			if (contractOf(p).exp > season) {
				continue;
			}
			const stay = this.outlook.retention.get(retentionKey(p.pid, tid));
			if (stay === undefined || stay >= 1) {
				continue;
			}
			const alone =
				a.players.length === 1 && a.picks.length === 0
					? whole
					: over({ players: [p], picks: [] });
			premium -= (1 - stay) * alone;
		}
		return Math.max(0, premium);
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
	// screens.ts) - there with what the seller is after, if it said.
	// The players a buyer adds to an offer to send back the salary the trade
	// rules need, up to offerSize + fillers pieces in all: of the ways it
	// could - its players it would miss least first, those it would miss
	// least for each dollar they send first, or the one contract it would
	// miss least that does it alone - the one that leaves it best off, or
	// none that work. Least missed first alone, it sent minimum contracts
	// that never added up to a big salary: at one moment of the owner's
	// League 13, 124 of 296 offers had no legal way to match, and every
	// contender but one had none for Karl-Anthony Towns ($30.9M, 11% to
	// re-sign with San Antonio, worth nothing to it and 0.06 to 0.17 of a
	// title to each of them) - as Kevin Durant, Greg Monroe and Ryan
	// Anderson left their losing teams for nothing when their deals ran out
	// (2026-11-20).
	fillSalary(
		buyer: number,
		held: Holdings,
		out: Holdings,
		offer: Holdings,
		allowed: (p: Player) => boolean,
		matches: (o: Holdings) => boolean,
		sellerMayTake: (o: Holdings) => boolean,
	): Holdings | undefined {
		const s = smartSettings;
		const most = s.offerSize + s.fillers;
		const uses = this.usesOf(buyer, held);
		const use = (p: Player) => uses.get(p.pid)!;
		const pay = (p: Player) => Math.max(1, contractOf(p).amount);
		const rest = held.players.filter(
			(x) => allowed(x) && !offer.players.some((y) => y.pid === x.pid),
		);
		const pieces = (o: Holdings) => o.players.length + o.picks.length;
		const greedy = (order: readonly Player[]) => {
			let o = offer;
			for (const p of order) {
				if (matches(o) || pieces(o) >= most) {
					break;
				}
				const next = plus(o, p);
				if (sellerMayTake(next)) {
					o = next;
				}
			}
			return matches(o) ? o : undefined;
		};
		const byUse = [...rest].sort((a, b) => use(a) - use(b) || a.pid - b.pid);
		const ways = [greedy(byUse)];
		if (s.fillByDollar) {
			ways.push(
				greedy(
					[...rest].sort(
						(a, b) => use(a) / pay(a) - use(b) / pay(b) || a.pid - b.pid,
					),
				),
			);
			if (pieces(offer) < most) {
				const single = byUse.find(
					(p) => matches(plus(offer, p)) && sellerMayTake(plus(offer, p)),
				);
				if (single) {
					ways.push(plus(offer, single));
				}
			}
		}
		const found = ways.filter((o): o is Holdings => o !== undefined);
		if (found.length <= 1) {
			return found[0];
		}
		let best = found[0]!;
		let bestValue = Number.NEGATIVE_INFINITY;
		for (const o of found) {
			const v = this.value(
				buyer,
				this.swappedFrom(held, o, out),
				[],
				tradedAwayCount(o.players),
			);
			if (v > bestValue) {
				best = o;
				bestValue = v;
			}
		}
		return best;
	}

	async offerFrom(
		buyer: number,
		seller: number,
		out: Holdings,
		ask?: OfferAsk,
	) {
		const s = smartSettings;
		await ensureRetention(this.outlook, out.players, buyer);
		const held = await this.holdingsOf(buyer);
		const m1 = mark();
		const allowed = (p: Player) =>
			!isUntradable(p).untradable && (ask?.allowed?.(p) ?? true);
		// Its players and its nearest picks (as NBA teams pay: a deal
		// without picks was every one of the GM's first 72), best for the
		// seller first - all its players, when the seller says what it is
		// after. Best as the seller counts a deal (dealWorthNow): its use for
		// a piece, or the market's price if more. Ranked by its use alone, a
		// seller out of the race - whose use for nearly anything is about
		// nothing - was offered whatever came first: in League 7 the best
		// teams paid for stars with filler while their young players and
		// picks stayed home.
		// (What dealWorth reads first, read once for the whole offer: the
		// teams that set prices and the draft classes don't change while it
		// is made, and read for each piece they were 40% of a moment.)
		const none = { players: [], picks: [] };
		const keen = await this.keenest();
		await this.classPrices();
		await this.holdingsOf(seller);
		const sellerHeld = this.heldNow(seller);
		const givenOut =
			smartSettings.valueForValue && smartSettings.boughtAtUse
				? this.premiumNow(seller, out, [seller, buyer], sellerHeld)
				: 0;
		const toSeller = async (a: Holdings) => {
			for (const tid of [seller, ...keen]) {
				if (tid === seller || tid !== buyer) {
					await ensureRetention(this.outlook, a.players, tid);
				}
			}
			// And value for value: a player's price past his use to it, as far
			// as it makes up for the price it gives (the pieces' prices are the
			// same whoever they are offered for; the deal's whole worth for
			// each piece was half again a moment's time).
			const worth = this.dealWorthNow(seller, buyer, none, a);
			if (givenOut > 0 && a.players.length > 0) {
				const premium = this.premiumNow(
					seller,
					{ players: a.players, picks: [] },
					[seller, buyer],
					sellerHeld,
				);
				return worth + Math.min(premium, givenOut);
			}
			return worth;
		};
		const scored: { x: Player | DraftPick; v: number }[] = [];
		for (const p of held.players.filter(allowed)) {
			scored.push({
				x: p,
				v: await toSeller({ players: [p], picks: [] }),
			});
		}
		scored.sort((a, b) => b.v - a.v);
		const candidates: { x: Player | DraftPick; v: number }[] = ask
			? scored
			: scored.slice(0, s.offerCandidates);
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
				v: await toSeller({ players: [], picks: [dp] }),
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
		let order = candidates;
		if (ask) {
			// What the seller is after first: the first of `lead` it would
			// give (none, no offer), then any of `next`.
			const rank = ask.rank;
			const lead = ask.lead;
			const first = lead
				? (rank
						? [...candidates].sort((a, b) => rank(b.x) - rank(a.x) || b.v - a.v)
						: candidates
					).find(
						({ x }) =>
							lead(x) && sellerMayTake(plus(offer, x)) && gains(plus(offer, x)),
					)
				: undefined;
			if (lead && !first) {
				offerStats.tried += 1;
				offerStats.empty += 1;
				lap("offer: greedy", m2);
				return undefined;
			}
			if (first) {
				offer = plus(offer, first.x);
			}
			order = [
				...candidates.filter((c) => c !== first && ask.next?.(c.x)),
				...candidates.filter((c) => c !== first && !ask.next?.(c.x)),
			];
		}
		for (const { x } of order) {
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
		// Short of salary to send back: as many of its players as the rules
		// need (as NBA teams fill a trade), if it still gains - those that
		// cost it least for the salary they send (smartSettings.
		// fillByDollar), the cheapest way it has of making the salaries work.
		if (!buyerMatches(offer)) {
			let filled = this.fillSalary(
				buyer,
				held,
				out,
				offer,
				allowed,
				buyerMatches,
				sellerMayTake,
			);
			// What it would pay already spent, the salary on top was more than
			// he is worth to it: the salary first, then what the seller wants
			// most on top, while it still gains - as a GM builds a trade for a
			// big contract around the salary it must send.
			if (s.fillByDollar && !ask && !(filled && gains(filled))) {
				const most = s.offerSize + s.fillers;
				let o = this.fillSalary(
					buyer,
					held,
					out,
					{ players: [], picks: [] },
					allowed,
					buyerMatches,
					sellerMayTake,
				);
				if (o && gains(o)) {
					for (const { x } of order) {
						if (o.players.length + o.picks.length >= most) {
							break;
						}
						if (
							"pid" in x
								? o.players.some((y) => y.pid === x.pid)
								: o.picks.some((y) => y.dpid === x.dpid)
						) {
							continue;
						}
						const next = plus(o, x);
						if (sellerMayTake(next) && buyerMatches(next) && gains(next)) {
							o = next;
						}
					}
					filled = o;
				} else {
					filled ??= o;
				}
			}
			if (!filled) {
				offerStats.cantMatch += 1;
				lap("offer: greedy", m2);
				return undefined;
			}
			offer = filled;
			if (!gains(offer)) {
				offerStats.fillersTooDear += 1;
				lap("offer: greedy", m2);
				return undefined;
			}
		}
		lap("offer: greedy", m2);
		// Its full worth first: most offers fail there (172 of 299 at a moment
		// of the owner's League 13), and the trade rules' check reads the
		// database.
		const m3 = mark();
		const inn = offer;
		if ((await this.dealWorth(buyer, seller, inn, out)) <= 0) {
			offerStats.buyerSaysNo += 1;
			lap("offer: checks", m3);
			return undefined;
		}
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
		if (legal.warning) {
			offerStats.illegal += 1;
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
