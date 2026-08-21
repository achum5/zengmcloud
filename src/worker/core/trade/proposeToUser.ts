import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import type { Player, TradeTeams } from "../../../common/types.ts";
import { choice } from "../../../common/random.ts";
import { last } from "../../../common/utils.ts";
import { ValueChangeCalculator } from "../team/ValueChangeCalculator.ts";
import isUntradable from "./isUntradable.ts";
import {
	getLeagueTradeContext,
	getTradePosture,
	type TradePosture,
} from "./tradePosture.ts";
import { isSelling, wasTradedThisSeason } from "./tradeMotivation.ts";
import {
	buildOfferFromPartner,
	buildSeed,
	type AttemptContext,
} from "./betweenAiTeams.ts";
import {
	ageFitMultiplier,
	positionFitMultiplier,
} from "../freeAgents/frontOffice.ts";

// ---------------------------------------------------------------------------
// THE OFFERS THE USER SEES
//
// The trade proposals page is the AI's face: whatever the engine does among
// its own teams, what a player experiences is the deals that land in their
// inbox. Those used to come from a different, much older brain - five random
// teams, a value-weighted random asset off the user's roster, no notion of
// what the proposing team needed, wanted to keep, or was trying to do. So a
// tearing-down team would offer its young core for a 33-year-old, minutes
// after the smart engine had it shopping veterans to contenders.
//
// Now a proposal is the same act as an AI-AI trade, pointed at the user:
// the initiating team is chosen by MOTIVE, it either shops what its posture
// says to shop (buildSeed - a walk-year dump, a vet on the block, a buyer's
// pick) or calls about the user player its posture actually covets, and the
// package is assembled and guarded by the same code that guards AI-AI deals -
// untouchables stay untouchable, rentals go only to contenders, timelines are
// respected, and the deal must clear the initiator's own tolerance.
//
// Deterministic for a given seed (the page must not reshuffle on every
// render), which is why every random draw threads through `rand`.
// ---------------------------------------------------------------------------

// A seeded stream: cheap, stateless-per-call, good enough for weighting draws.
const makeRand = (seed: number) => {
	let s = Math.floor(Math.abs(seed)) >>> 0;
	return () => {
		s = (s * 1_664_525 + 1_013_904_223) >>> 0;
		return s / 4_294_967_296;
	};
};

// How much this team wants a specific player on the user's roster, by the same
// fit language free agency uses: position need, age against the timeline, and
// a star hunter's hunger. Pure, so the ordering is testable.
export const covetWeight = ({
	posture,
	p,
	starOvr,
}: {
	posture: TradePosture;
	p: { value: number; age: number; pos: string; ovr: number };
	starOvr: number;
}): number => {
	let w =
		Math.max(0.01, p.value) *
		ageFitMultiplier(posture.tier, p.age) *
		positionFitMultiplier(posture, p.pos);

	// A contender missing its star covets the user's star most of all.
	if (
		(posture.tier === "allIn" || posture.tier === "buyer") &&
		posture.starGap &&
		p.ovr >= starOvr
	) {
		w *= 3;
	}

	// A seller has no business calling about a veteran at all.
	if (isSelling(posture.tier) && p.age >= 30) {
		w *= 0.1;
	}

	return w;
};

// How motivated this team is to call the user at all: the same instincts that
// pick the initiator in the AI-AI market, plus how much it covets the user's
// best-fitting player.
export const proposerWeight = ({
	posture,
	bestCovet,
}: {
	posture: TradePosture;
	// The best covetWeight over the user's tradable roster, normalized by the
	// roster's best raw value so it reads as a fit multiple, not a talent level.
	bestCovet: number;
}): number =>
	posture.aggression +
	(posture.shopVeteranPids.length > 0 ? 0.5 : 0) +
	(posture.elite ? 1 : 0) +
	bestCovet;

// Build up to `numOffers` credible proposals from AI teams to the user, or
// undefined when the smart front office is off or a posture read fails -
// callers fall back to the old random offers, so this can never make the page
// emptier than it was.
const proposeToUser = async ({
	numOffers,
	seed,
}: {
	numOffers: number;
	seed: number;
}): Promise<TradeTeams[] | undefined> => {
	if (!g.get("smartAiFrontOffice")) {
		return undefined;
	}

	const userTid = g.get("userTid");
	const season = g.get("season");
	const rand = makeRand(seed);

	let postures: Map<number, TradePosture>;
	let starOvr: number;
	try {
		const context = await getLeagueTradeContext();
		starOvr = context.starOvr;
		postures = new Map();
		for (const t of await idb.cache.teams.getAll()) {
			if (t.disabled || t.tid === userTid) {
				continue;
			}
			postures.set(t.tid, await getTradePosture(t.tid, context));
		}
	} catch (error) {
		console.error("proposeToUser: posture computation failed", error);
		return undefined;
	}
	if (postures.size === 0) {
		return undefined;
	}

	const userPlayers = (
		await idb.cache.players.indexGetAll("playersByTid", userTid)
	).filter((p) => !isUntradable(p).untradable);
	const userPicks = await idb.cache.draftPicks.indexGetAll(
		"draftPicksByTid",
		userTid,
	);
	if (userPlayers.length === 0 && userPicks.length === 0) {
		return undefined;
	}

	const bestUserValue = Math.max(
		1,
		...userPlayers.map((p) => Math.max(0, p.value)),
	);
	const covetTarget = (posture: TradePosture): Player | undefined => {
		if (userPlayers.length === 0) {
			return undefined;
		}
		return choice(
			userPlayers,
			(p) =>
				covetWeight({
					posture,
					p: {
						value: p.value,
						age: season - p.born.year,
						pos: last(p.ratings).pos,
						ovr: last(p.ratings).ovr,
					},
					starOvr,
				}),
			rand(),
		);
	};

	// Teams call in motive order, sampled so the page varies league to league.
	const callers: number[] = [];
	{
		const pool = [...postures.keys()];
		while (pool.length > 0 && callers.length < postures.size) {
			const tid = choice(
				pool,
				(t) => {
					const posture = postures.get(t)!;
					let bestCovet = 0;
					for (const p of userPlayers) {
						bestCovet = Math.max(
							bestCovet,
							covetWeight({
								posture,
								p: {
									value: p.value,
									age: season - p.born.year,
									pos: last(p.ratings).pos,
									ovr: last(p.ratings).ovr,
								},
								starOvr,
							}),
						);
					}
					return Math.max(
						0.01,
						proposerWeight({ posture, bestCovet: bestCovet / bestUserValue }),
					);
				},
				rand(),
			);
			callers.push(tid);
			pool.splice(pool.indexOf(tid), 1);
		}
	}

	const valueChangeCalculator = new ValueChangeCalculator();
	const ctx: AttemptContext = {
		postures,
		valueChangeCalculator,
		aiTids: [...postures.keys()],
		season,
		starOvr,
	};

	const offers: TradeTeams[] = [];
	for (const tid of callers) {
		if (offers.length >= numOffers) {
			break;
		}
		const initPosture = postures.get(tid)!;

		const allInitiatorPlayers = await idb.cache.players.indexGetAll(
			"playersByTid",
			tid,
		);
		const players = allInitiatorPlayers.filter(
			(p) =>
				!isUntradable(p).untradable &&
				!wasTradedThisSeason(p.transactions, season),
		);
		const draftPicks = await idb.cache.draftPicks.indexGetAll(
			"draftPicksByTid",
			tid,
		);
		if (players.length === 0 && draftPicks.length === 0) {
			continue;
		}

		const initiatorExcludedBase = [
			...initPosture.buildingBlockPids,
			...allInitiatorPlayers
				.filter((p) => wasTradedThisSeason(p.transactions, season))
				.map((p) => p.pid),
		];

		// Two ways to open a call, tried in a random order: shop what the
		// posture says to move, or ask about the user player it covets.
		const directions: ("shop" | "covet")[] =
			rand() < 0.5 ? ["covet", "shop"] : ["shop", "covet"];

		for (const direction of directions) {
			let offer: Awaited<ReturnType<typeof buildOfferFromPartner>> = null;
			if (direction === "shop") {
				const tradeSeed = await buildSeed(
					tid,
					initPosture,
					players,
					draftPicks,
					season,
					starOvr,
					rand,
				);
				if (tradeSeed) {
					offer = await buildOfferFromPartner({
						initiator: tid,
						initPosture,
						seed: tradeSeed,
						initiatorExcluded: initiatorExcludedBase.filter(
							(pid) => !tradeSeed.pids.includes(pid),
						),
						partner: userTid,
						ctx,
					});
				}
			} else {
				const target = covetTarget(initPosture);
				if (target) {
					offer = await buildOfferFromPartner({
						initiator: tid,
						initPosture,
						seed: {
							pids: [],
							dpids: [],
							motivatedDump: false,
							starSale: false,
						},
						initiatorExcluded: initiatorExcludedBase,
						partner: userTid,
						partnerSeedPids: [target.pid],
						ctx,
					});
				}
			}

			if (offer) {
				offers.push(offer.teams);
				break;
			}
		}
	}

	if (offers.length === 0) {
		return undefined;
	}

	// buildOfferFromPartner puts the initiator at index 0; every consumer of a
	// user-facing offer (augmentOffers, the proposals page) expects the user
	// side first. Each side carries its own tid, so swapping is safe.
	return offers.map(
		([initiatorSide, userSide]) => [userSide, initiatorSide] as TradeTeams,
	);
};

export default proposeToUser;
