import { roundHalf } from "../../../common/getGameSpread.ts";
import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import toUI from "../../util/toUI.ts";
import { buildGameLinePricer } from "./gameLines.ts";
import { warmSimMargins } from "./simSpreads.ts";

// Keeps the point spread shown OUTSIDE the sportsbook in step with the engine.
//
// Everywhere a game is shown, its spread comes from getUpcoming, which prices it
// off the closed-form line corrected by any simulated margin already cached for
// that exact matchup. So the pages agree with each other for free - as long as
// they're all looking at the same cache. This is what fills that cache, and what
// makes sure nothing is left holding a line from before it was filled.
//
// It is deliberately NOT called while a view is being built. Pricing reads every
// active player, which is fine once on the sportsbook - a page that exists to
// price things - but is real work to add to a page that just lists games. The UI
// asks for this AFTER it has rendered, so the schedule paints at its usual speed
// and refined numbers arrive a moment later.
//
// Like everything else on this path it only ever READS the sim cache. Misses are
// queued for the background warmer and priced off the formula meanwhile.

// One day's slate. A cap anyway, so a malformed day can't queue the season.
// Exported so a test can hold the bound in place - this is the whole reason
// showing lines here doesn't cost anything.
export const MAX_WARM_SPREADS = 20;

// Price one day off whatever the cache currently holds, and push the result to
// the league top bar. The top bar is the one surface that doesn't rebuild when a
// page does - it holds a snapshot of the user's next game - so without this it
// keeps the spread from whenever that snapshot was taken and reads differently
// from the schedule three inches below it.
const priceAndPublish = async (season: number, day: number) => {
	const schedule = await idb.cache.schedule.getAll();
	const matchups = schedule
		.filter((row) => row.day === day)
		.map((row) => ({
			gid: row.gid,
			day: row.day,
			homeTid: row.homeTid,
			awayTid: row.awayTid,
			finals: row.finals,
		}));
	if (matchups.length === 0) {
		return { spreads: {}, pending: [] };
	}

	const teams = await idb.cache.teams.getAll();
	const activeTeams = teams
		.filter((t) => !t.disabled)
		.map((t) => ({
			tid: t.tid,
			playThroughInjuries: t.playThroughInjuries,
		}));

	const pricer = await buildGameLinePricer({
		activeTeams,
		season,
		todayDay: day,
	});

	const spreads: Record<number, number> = {};
	for (const matchup of matchups) {
		const line = pricer.priceGame(matchup);
		if (line) {
			// GameLine.margin is the expected HOME margin, the same orientation
			// getGameSpread returns. Blending leaves it continuous, so round it the
			// way every displayed spread is rounded - a schedule reading "-3.7" next
			// to "-4" elsewhere is worse than being half a point coarse.
			spreads[matchup.gid] = roundHalf(line.margin);
		}
	}

	await toUI("updateGameSpreads", [spreads]);

	return { spreads, pending: pricer.pendingSims() };
};

// Returns what it published, so a test can hold it against what the schedule
// pages independently compute for the same games. The UI ignores it - it gets
// these through local.games and the view rebuild.
export const syncDaySpreads = async ({
	season,
	day,
}: {
	season: number;
	day: number;
}): Promise<Record<number, number>> => {
	if (season !== g.get("season")) {
		// Past seasons have no upcoming games, and a completed game's spread has to
		// stay exactly what getGameSpread says - getTeamAtsRecords re-derives every
		// historical line that way, so showing a different number here would put
		// the displayed spread at odds with the ATS record.
		return {};
	}

	const { spreads, pending } = await priceAndPublish(season, day);
	if (pending.length === 0) {
		return spreads;
	}

	// Fire-and-forget, bounded. warmSimMargins refuses to start a second drain
	// while one is running, so the sportsbook and this can't compound.
	//
	// When margins land it emits a sportsbookLines update, which rebuilds the
	// schedule views off the newly-warm cache. Price once more here so the top
	// bar moves with them rather than a page-load behind.
	void warmSimMargins(pending.slice(0, MAX_WARM_SPREADS)).then(
		async (landed) => {
			if (landed) {
				await priceAndPublish(season, day);
			}
		},
	);

	return spreads;
};
