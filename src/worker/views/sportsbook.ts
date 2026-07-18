import { g, logEvent } from "../util/index.ts";
import { idb } from "../db/index.ts";
import type { UpdateEvents } from "../../common/types.ts";
import { getLines } from "../core/sportsbook/getLines.ts";
import { SPORTSBOOK_PRESEASON_GRANT } from "../../common/sportsbook.ts";

const updateSportsbook = async (
	inputs: unknown,
	updateEvents: UpdateEvents,
) => {
	if (
		updateEvents.includes("firstRun") ||
		updateEvents.includes("gameSim") ||
		updateEvents.includes("newPhase") ||
		updateEvents.includes("playerMovement") ||
		updateEvents.includes("gameAttributes") ||
		// Bets placed/settled bump this so the wallet + open bets refresh.
		updateEvents.includes("watchList")
	) {
		// Catch-up settlement (a bet whose outcome is already known but that a
		// missed hook didn't settle) is NOT done here. This function runs as a
		// view load, which is deliberately never cloud-tracked for sync (see
		// SKIP_CHANGESET_CAPTURE in worker/index.ts) - settling here would apply
		// the payout to this device's local cache only, and the NEXT time this
		// device reconciles with the room's canonical state, that change would be
		// silently reverted ("money resets"). The UI instead fires the real
		// captured `main.sportsbookSettle` call when this page loads, which goes
		// through the normal mutation pipeline and actually publishes to the room.
		//
		// The board must never take down the page - if the odds engine fails for
		// any reason, render an empty book (wallet + bets still work).
		let board: Awaited<ReturnType<typeof getLines>>;
		try {
			board = await getLines();
		} catch (error) {
			console.error("Sportsbook board failed to compute", error);
			logEvent({
				type: "error",
				text: `Sportsbook lines unavailable: ${
					error instanceof Error ? error.message : String(error)
				}`,
				saveToDb: false,
			});
			board = {
				games: [],
				championship: [],
				conferences: [],
				divisions: [],
				winTotals: [],
				awards: [],
				allLeague: [],
				allDefensive: [],
				allRookie: [],
				allStar: [],
			};
		}

		const userTid = g.get("userTid");
		const teams = await idb.cache.teams.getAll();

		const walletFor = (tid: number) => {
			const t = teams.find((team) => team.tid === tid);
			return t?.sportsbook;
		};

		// The device's own team is the one that bets. A team with no wallet yet
		// (e.g. a league imported mid-season, before its first preseason grant) is
		// shown holding the standard grant; it's persisted the first time a bet is
		// placed.
		const sb = walletFor(userTid);
		const wallet = {
			tid: userTid,
			balance: sb?.balance ?? SPORTSBOOK_PRESEASON_GRANT,
			bets: sb?.bets ?? [],
			history: (sb?.history ?? []).slice(0, 40),
		};

		// The human-managed teams' balances, just for fun (a little leaderboard).
		const userTids = new Set(g.get("userTids"));
		const balances = teams
			.filter((t) => userTids.has(t.tid))
			.map((t) => ({
				tid: t.tid,
				balance: t.sportsbook?.balance ?? SPORTSBOOK_PRESEASON_GRANT,
			}))
			.sort((a, b) => b.balance - a.balance);

		return {
			board,
			wallet,
			balances,
			userTid,
			phase: g.get("phase"),
			season: g.get("season"),
		};
	}
};

export default updateSportsbook;
