import { PLAYER } from "../../../../common/constants.ts";
import { idb } from "../../../db/index.ts";
import type { Changeset } from "../changeset.ts";
import { reapplyOwnChangesLocally } from "./applyVersion.ts";

// Realistic free agency keeps a human team's offers on its team record
// (t.offerBoard), and a free-agency day, or the phase change that decides
// re-signings, rewrites the same record. Sync is whole-record, so when a
// publish had to catch up first, a team record it carries can be older than
// the one the catch-up just applied: an offer made as the day began erased
// the day's changes to the team, or the day erased the offer - silently, and
// with the author's device left disagreeing with the room.
//
// So such a record is merged before it goes up: the offer board from the side
// that edited offers, everything else from the other side, keeping only offers
// for players still free agents - and written here too, so this device agrees
// with the room. Teams without boards (every league without the mode) are left
// alone.
const OFFER_ACTIONS = new Set([
	"main.makeFreeAgentOffer",
	"main.withdrawFreeAgentOffer",
	"main.rankFreeAgentOffers",
	"main.decideReSigningOffers",
]);

type Board = { pid: number; amount: number; exp: number }[];

export const mergeOfferBoards = async (
	changeset: Changeset,
	action: string,
): Promise<number> => {
	const offerSide = OFFER_ACTIONS.has(action);
	const merged: Changeset["changes"] = [];
	for (let i = 0; i < changeset.changes.length; i++) {
		const change = changeset.changes[i]!;
		if (change.store !== "teams" || change.type !== "put" || !change.value) {
			continue;
		}
		const mine = change.value as { offerBoard?: Board };
		const local = (await (idb.league as any).get("teams", change.id)) as
			| { offerBoard?: Board }
			| undefined;
		if (!local || (!local.offerBoard && !mine.offerBoard)) {
			continue;
		}
		const board: Board = [];
		for (const offer of (offerSide ? mine : local).offerBoard ?? []) {
			const p = await (idb.league as any).get("players", offer.pid);
			if (p?.tid === PLAYER.FREE_AGENT) {
				board.push(offer);
			}
		}
		const value = { ...(offerSide ? local : mine), offerBoard: board };
		const json = JSON.stringify(value);
		if (json === JSON.stringify(mine) && json === JSON.stringify(local)) {
			continue;
		}
		changeset.changes[i] = { ...change, value };
		merged.push(changeset.changes[i]!);
	}
	if (merged.length > 0) {
		await reapplyOwnChangesLocally(merged);
	}
	return merged.length;
};
