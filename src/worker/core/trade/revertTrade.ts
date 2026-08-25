import { PHASE } from "../../../common/constants.ts";
import { idb } from "../../db/index.ts";
import { changeTracker } from "../../db/changeTracker.ts";
import { g, helpers, toUI, updatePlayMenu } from "../../util/index.ts";
import type { EventBBGM } from "../../../common/types.ts";
import { player } from "../index.ts";
import { getTeammateJerseyNumbers } from "../player/genJerseyNumber.ts";
import { recomputeLocalUITeamOvrs } from "../../util/recomputeLocalUITeamOvrs.ts";

type TradeEvent = Extract<EventBBGM, { type: "trade" }>;

// A trade can only be taken back while everything it moved is still where it
// landed. The moment an asset has moved on - a player re-traded, released or
// retired, a pick spent on draft night or flipped again - unwinding this trade
// would no longer restore the world before it, just scramble the world after
// it. So the plan is all-or-nothing: either every asset can go home, or the
// trade is not revertable and the reason names what moved.
export const planTradeRevert = async (
	event: TradeEvent,
): Promise<
	| {
			// What each side sends BACK: the assets it received in `event`, in
			// event.tids order.
			pids: [number[], number[]];
			dpids: [number[], number[]];
	  }
	| {
			error: string;
	  }
> => {
	if (!event.teams || event.phase === undefined) {
		return { error: "This trade is too old to have the data needed." };
	}

	const pids: [number[], number[]] = [[], []];
	const dpids: [number[], number[]] = [[], []];

	for (const i of [0, 1] as const) {
		const tid = event.tids[i];
		for (const asset of event.teams[i].assets) {
			if ("pid" in asset) {
				// Retired and deleted players are not in the cache, and neither can
				// come back anyway, so one lookup answers both questions.
				const p = await idb.cache.players.get(asset.pid);
				if (!p || p.tid !== tid) {
					return {
						error: `${asset.name} is no longer on the team that traded for him.`,
					};
				}
				pids[i].push(asset.pid);
			} else {
				// A used pick is deleted, so existence is the "not yet drafted" check.
				const dp = await idb.cache.draftPicks.get(asset.dpid);
				if (!dp || dp.tid !== tid) {
					return {
						error: "A traded draft pick has since been used or moved.",
					};
				}
				dpids[i].push(asset.dpid);
			}
		}
	}

	return { pids, dpids };
};

// Undo the trade behind an event, God Mode only, leaving no trace it ever
// happened. Returns an error message, or undefined on success.
//
// This deliberately does NOT run the reversal through processTrade. A normal
// trade leaves footprints beyond the moved assets - a transaction on every
// player, a mood charge on every teamSeason that gave a player up, an event in
// the league log - and undoing an accident means erasing those, not writing a
// second set on top:
//
//   - Each player's `transactions` entry for this trade is removed, and no
//     revert entry is added.
//   - The mood charge (teamSeason.numPlayersTradedAway, which every player's
//     "this franchise ships people out" mood component is derived from) is
//     subtracted back off the season the trade happened in. The original
//     amount was a function of the player's value at trade time and is not
//     stored, so it is recomputed from his value now - exact when the revert
//     is prompt, which an accidental trade's always is, and bounded at zero
//     regardless.
//   - The trade event itself is deleted, which is the one step that needs
//     care in a synced league. Event ids are not stable across devices, so a
//     delete-by-eid could erase an unrelated event elsewhere; instead the
//     deleted row's full content travels with the changeset and each receiver
//     deletes its own row matching that content, or nothing (see
//     DELETE_BY_CONTENT in sync/changeset.ts - a stale log line is
//     recoverable, a wrong-row delete is not).
//
// What is not restored: ptModifier (the trade reset it to 1; its prior value
// was never recorded) and jersey numbers are reassigned rather than restored
// (players prefer their old numbers, which are free again, so in practice
// they come back).
const revertTrade = async (eid: number): Promise<string | undefined> => {
	if (!g.get("godMode")) {
		return "God Mode is required to revert a trade.";
	}

	const event = await idb.getCopy.events({ eid }, "noCopyCache");
	if (!event || event.type !== "trade") {
		return "Trade not found.";
	}

	const plan = await planTradeRevert(event);
	if ("error" in plan) {
		return plan.error;
	}

	const duringSeason = g.get("phase") <= PHASE.PLAYOFFS;

	for (const i of [0, 1] as const) {
		// Side i received these assets, so they go back to the other side - and
		// the other side is who paid the mood charge for giving them up.
		const from = event.tids[i];
		const to = event.tids[i === 0 ? 1 : 0];

		let giverSeason;
		if (plan.pids[i].length > 0) {
			giverSeason = await idb.cache.teamSeasons.indexGet(
				"teamSeasonsBySeasonTid",
				[event.season, to],
			);
		}

		for (const pid of plan.pids[i]) {
			const p = (await idb.cache.players.get(pid))!;
			p.tid = to;

			if (duringSeason) {
				const teamJerseyNumbers = await getTeammateJerseyNumbers(to, [
					pid,
					...plan.pids[i],
				]);
				player.setJerseyNumber(
					p,
					await player.genJerseyNumber(p, teamJerseyNumbers),
				);
			}

			if (p.transactions) {
				p.transactions = p.transactions.filter(
					(t) => !(t.type === "trade" && t.eid === eid),
				);
			}

			if (giverSeason) {
				giverSeason.numPlayersTradedAway = Math.max(
					0,
					giverSeason.numPlayersTradedAway -
						helpers.sigmoid(p.valueNoPot / 100, 30, 0.47),
				);
			}

			await idb.cache.players.put(p);
		}

		if (giverSeason) {
			await idb.cache.teamSeasons.put(giverSeason);
		}

		for (const dpid of plan.dpids[i]) {
			const dp = (await idb.cache.draftPicks.get(dpid))!;
			dp.tid = to;
			await idb.cache.draftPicks.put(dp);
		}

		// Unused, but keeps the loop honest about direction.
		void from;
	}

	await idb.cache.events.delete(eid);
	// The event may live only on disk (the cache holds recent seasons), in
	// which case the delete above recorded no snapshot and would stay local.
	// Re-record it with the row we already loaded, so the content-matched
	// delete reaches every device whatever the cache held.
	changeTracker.record("events", eid, "delete", event);

	await toUI("realtimeUpdate", [["playerMovement"]]);
	await recomputeLocalUITeamOvrs();
	if (g.get("phase") === PHASE.DRAFT) {
		await updatePlayMenu();
	}
};

export default revertTrade;
