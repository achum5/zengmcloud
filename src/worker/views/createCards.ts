import type { TradingCard, UpdateEvents } from "../../common/types.ts";
import { idb } from "../db/index.ts";
import { g, helpers } from "../util/index.ts";

// The Create Cards page. Two things go over the wire: a light index of every
// player who can appear on a card, and every card already made in the league.
//
// The index stays light on purpose - a deep league has thousands of players, so
// per-player detail (which seasons he has, his stat grid) is fetched only once
// he is actually picked, through the getTradingCard* API calls.

export type CardPlayerIndexEntry = {
	pid: number;
	name: string;
	tid: number;
	abbrev: string;
	pos: string;
	lastSeason: number;
};

export type CardWithPlayer = TradingCard & { playerName: string };

export const attachPlayerNames = async (
	cards: TradingCard[],
): Promise<CardWithPlayer[]> => {
	const names = new Map<number, string>();
	for (const card of cards) {
		if (!names.has(card.pid)) {
			const p = await idb.getCopy.players({ pid: card.pid }, "noCopyCache");
			names.set(
				card.pid,
				p ? `${p.firstName} ${p.lastName}`.trim() : "Unknown player",
			);
		}
	}
	return cards
		.map((card) => ({ ...card, playerName: names.get(card.pid) ?? "" }))
		.sort((a, b) => b.at - a.at);
};

const updateCreateCards = async (
	inputs: unknown,
	updateEvents: UpdateEvents,
) => {
	if (
		updateEvents.includes("firstRun") ||
		updateEvents.includes("playerMovement") ||
		updateEvents.includes("gameSim") ||
		updateEvents.includes("newPhase") ||
		updateEvents.includes("tradingCards")
	) {
		const playersRaw = await idb.getCopies.players({ activeAndRetired: true });

		const players: CardPlayerIndexEntry[] = playersRaw.map((p) => {
			const lastRatings = p.ratings.at(-1);
			const lastStats = p.stats.at(-1);
			return {
				pid: p.pid,
				name: `${p.firstName} ${p.lastName}`.trim(),
				tid: p.tid,
				abbrev: helpers.getAbbrev(p.tid),
				pos: lastRatings?.pos ?? "",
				lastSeason: lastStats?.season ?? lastRatings?.season ?? g.get("season"),
			};
		});
		players.sort((a, b) => a.name.localeCompare(b.name));

		const cards = await attachPlayerNames(
			await idb.cache.tradingCards.getAll(),
		);

		return {
			cards,
			players,
			season: g.get("season"),
			userTid: g.get("userTid"),
		};
	}
};

export default updateCreateCards;
