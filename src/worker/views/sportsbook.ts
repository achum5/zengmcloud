import { g, logEvent } from "../util/index.ts";
import { idb } from "../db/index.ts";
import type { SportsbookBet, UpdateEvents } from "../../common/types.ts";
import { getLines } from "../core/sportsbook/getLines.ts";
import {
	marketGid,
	SPORTSBOOK_PRESEASON_GRANT,
} from "../../common/sportsbook.ts";

const updateSportsbook = async (
	inputs: { tab?: string },
	updateEvents: UpdateEvents,
	state: any,
) => {
	if (
		updateEvents.includes("firstRun") ||
		updateEvents.includes("gameSim") ||
		updateEvents.includes("newPhase") ||
		updateEvents.includes("playerMovement") ||
		updateEvents.includes("gameAttributes") ||
		// Bets placed/settled bump this so the wallet + open bets refresh.
		updateEvents.includes("watchList") ||
		// Switching tabs changes only the URL, so without this the view returns
		// undefined, the UI keeps the props it already had, and the page stays on
		// whatever tab it first rendered.
		inputs.tab !== state.tab
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
				allStarRosterSize: g.get("allStarNum") * 2,
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

		// Every user team's slips, so league-mates can sweat each other's bets.
		// Bets live on the (synced) team record, so this is just a read.
		const leagueBets = teams
			.filter((t) => userTids.has(t.tid))
			.map((t) => ({
				tid: t.tid,
				balance: t.sportsbook?.balance ?? SPORTSBOOK_PRESEASON_GRANT,
				open: t.sportsbook?.bets ?? [],
				settled: (t.sportsbook?.history ?? []).slice(0, 10),
			}))
			.sort((a, b) => b.balance - a.balance);

		// Every game any bet references (a straight game bet, or any leg of a
		// parlay), resolved to what a box-score link needs: the game's season and
		// a team anchor. Only games that have actually been played (so a box score
		// exists) get an entry, so open bets on unplayed games simply don't link.
		const gidsReferenced = new Set<number>();
		const collectGids = (bet: SportsbookBet) => {
			const one = marketGid(bet.market);
			if (one !== undefined) {
				gidsReferenced.add(one);
			}
			for (const leg of bet.legs ?? []) {
				const g2 = marketGid(leg.market);
				if (g2 !== undefined) {
					gidsReferenced.add(g2);
				}
			}
		};
		for (const bet of wallet.bets) {
			collectGids(bet);
		}
		for (const bet of wallet.history) {
			collectGids(bet);
		}
		for (const teamBets of leagueBets) {
			for (const bet of [...teamBets.open, ...teamBets.settled]) {
				collectGids(bet);
			}
		}

		const teamInfoCache = g.get("teamInfoCache");
		const gameLinks: Record<number, { abbrevTid: string; season: number }> = {};
		for (const gid of gidsReferenced) {
			const game = await idb.getCopy.games({ gid }, "noCopyCache");
			if (!game || !game.won || !game.lost) {
				continue; // not played yet, or box score pruned - nothing to link to
			}
			const tid = game.teams[0].tid;
			const abbrev = teamInfoCache[tid]?.abbrev ?? "";
			gameLinks[gid] = {
				abbrevTid: `${abbrev}_${tid}`,
				season: game.season,
			};
		}

		return {
			board,
			wallet,
			balances,
			leagueBets,
			gameLinks,
			userTid,
			phase: g.get("phase"),
			season: g.get("season"),
			// Which tab the URL asked for - each one is its own route.
			tab: inputs.tab ?? "games",
		};
	}
};

export default updateSportsbook;
