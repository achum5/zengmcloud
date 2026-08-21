import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import type { DraftPick, Player } from "../../../common/types.ts";
import { last } from "../../../common/utils.ts";
import { ValueChangeCalculator } from "../team/ValueChangeCalculator.ts";
import processTrade from "../trade/processTrade.ts";
import {
	buildOfferFromPartner,
	type AttemptContext,
} from "../trade/betweenAiTeams.ts";
import { wasTradedThisSeason } from "../trade/tradeMotivation.ts";
import type { PosBucket, TradePosture } from "../trade/tradePosture.ts";
import { posBucket } from "../trade/tradePosture.ts";
import { scoreProspect } from "./draftBoard.ts";

// ---------------------------------------------------------------------------
// DRAFT-NIGHT TRADES
//
// Real drafts are full of them: a team watches the player it wants slide
// toward someone else's pick and moves up to take him. The engine had all the
// machinery - pick valuation, posture guards, package assembly - and never
// used it on the night it matters most.
//
// The shape is deliberately narrow. When an AI team is on the clock, a team
// picking later may try to buy the slot, but ONLY when its own board says the
// player at risk is clearly better than whoever will still be around at its
// own pick - that ratio is the whole reason anyone trades up. The deal itself
// is assembled and guarded by the same code as every other AI trade, so a
// trade-up can cost a young player or extra picks but never a building block,
// and the team on the clock only sells when its own valuation - which for a
// rebuilder prices incoming picks UP - says yes.
// ---------------------------------------------------------------------------

// How often a sellable pick even looks for a buyer. Drafts have 2x30 picks;
// at 20% each, with most candidates failing the covet ratio or the value
// check, a league sees a couple of draft-night deals a year, not a circus.
export const TRADE_UP_ATTEMPT_CHANCE = 0.2;

// The buyer's board must like the player at risk this much more than its
// likely alternative before it pays to move up.
export const CHASE_RATIO = 1.3;

// Within this many slots, just wait - he might fall to you.
export const MIN_SLOTS_TO_CHASE = 3;

// Should this team try to buy the current pick? Pure, so the trigger - the
// heart of a trade-up - is testable on its own.
export const shouldChase = ({
	topScore,
	fallbackScore,
	slotsUntilOwnPick,
}: {
	// The buyer's own board score for the best prospect available right now...
	topScore: number;
	// ...and for the prospect its board expects at its own pick.
	fallbackScore: number;
	slotsUntilOwnPick: number;
}): boolean =>
	slotsUntilOwnPick >= MIN_SLOTS_TO_CHASE &&
	fallbackScore > 0 &&
	topScore / fallbackScore >= CHASE_RATIO;

// Try to sell the pick currently on the clock to a team drafting later.
// Returns true if a trade happened (the pick's cache row now carries the
// buyer's tid, so the caller's loop just keeps going).
const maybeTradeUp = async ({
	dp,
	laterPicks,
	playersAll,
	postureFor,
	valueChangeCalculator,
	starOvr,
	draftedByTid,
	rand = Math.random,
}: {
	// The pick on the clock, owned by an AI team.
	dp: DraftPick;
	// The picks after this one, in draft order.
	laterPicks: DraftPick[];
	// Undrafted players, best first.
	playersAll: Player[];
	// Lazily computes (and caches) a team's posture - the caller owns the
	// cache, so a posture computed here is free for the pick that follows.
	postureFor: (tid: number) => Promise<TradePosture | undefined>;
	valueChangeCalculator: ValueChangeCalculator;
	starOvr: number;
	draftedByTid: Map<number, Map<PosBucket, number>>;
	rand?: () => number;
}): Promise<boolean> => {
	if (rand() >= TRADE_UP_ATTEMPT_CHANCE) {
		return false;
	}
	if (playersAll.length < MIN_SLOTS_TO_CHASE + 1) {
		return false;
	}

	const season = g.get("season");
	const userTids = g.get("userTids");

	const sellerPosture = await postureFor(dp.tid);
	if (!sellerPosture) {
		return false;
	}

	const boardScore = (posture: TradePosture, p: Player): number => {
		const ratings = last(p.ratings);
		return scoreProspect({
			p: {
				pid: p.pid,
				ovr: ratings.ovr,
				pot: ratings.pot,
				value: p.value,
				age: season - p.born.year,
				pos: ratings.pos,
			},
			posture,
			alreadyDraftedAtPos:
				draftedByTid.get(posture.tid)?.get(posBucket(ratings.pos)) ?? 0,
		});
	};

	// Buyers: the first later pick of each AI team other than the seller.
	const seen = new Set<number>();
	const buyers: { tid: number; dpid: number; slots: number }[] = [];
	for (const [i, later] of laterPicks.entries()) {
		if (
			later.tid !== dp.tid &&
			!userTids.includes(later.tid) &&
			!seen.has(later.tid)
		) {
			seen.add(later.tid);
			buyers.push({ tid: later.tid, dpid: later.dpid, slots: i + 1 });
		}
	}

	for (const buyer of buyers) {
		const buyerPosture = await postureFor(buyer.tid);
		if (!buyerPosture) {
			continue;
		}

		const top = playersAll[0]!;
		const fallbackIndex = Math.min(buyer.slots, playersAll.length - 1);
		const fallback = playersAll[fallbackIndex]!;
		if (
			!shouldChase({
				topScore: boardScore(buyerPosture, top),
				fallbackScore: boardScore(buyerPosture, fallback),
				slotsUntilOwnPick: buyer.slots,
			})
		) {
			continue;
		}

		// The buyer opens with its own later pick for the seller's current one;
		// makeItWork adds what it takes to satisfy the seller, guarded exactly
		// like any other AI deal.
		const buyerPlayers = await idb.cache.players.indexGetAll(
			"playersByTid",
			buyer.tid,
		);
		const initiatorExcluded = [
			...buyerPosture.buildingBlockPids,
			...buyerPlayers
				.filter((p) => wasTradedThisSeason(p.transactions, season))
				.map((p) => p.pid),
		];
		const ctx: AttemptContext = {
			postures: new Map([
				[dp.tid, sellerPosture],
				[buyer.tid, buyerPosture],
			]),
			valueChangeCalculator,
			aiTids: [],
			season,
			starOvr,
		};
		const offer = await buildOfferFromPartner({
			initiator: buyer.tid,
			initPosture: buyerPosture,
			seed: {
				pids: [],
				dpids: [buyer.dpid],
				motivatedDump: false,
				starSale: false,
			},
			initiatorExcluded,
			partner: dp.tid,
			partnerSeedDpids: [dp.dpid],
			ctx,
		});
		if (!offer) {
			continue;
		}

		const { teams } = offer;
		await processTrade(
			[teams[0].tid, teams[1].tid],
			[teams[0].pids, teams[1].pids],
			[teams[0].dpids, teams[1].dpids],
			{
				initiatorTid: buyer.tid,
				tiers: [buyerPosture.tier, sellerPosture.tier],
				dv: Math.round(offer.dv2 * 10) / 10,
				motivation: "draft-trade-up",
			},
		);
		valueChangeCalculator.invalidateCache({
			teams: [teams[0].tid, teams[1].tid],
		});
		return true;
	}

	return false;
};

export default maybeTradeUp;
