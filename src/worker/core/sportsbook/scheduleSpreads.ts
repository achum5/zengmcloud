import { roundHalf } from "../../../common/getGameSpread.ts";
import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import { recomputeLocalUITeamOvrs } from "../../util/recomputeLocalUITeamOvrs.ts";
import { buildGameLinePricer } from "./gameLines.ts";
import { warmSimMargins } from "./simSpreads.ts";

// The engine-corrected point spread for the games on one day, for pages OUTSIDE
// the sportsbook.
//
// ScoreBox has always drawn its own spread from the closed-form formula, in the
// UI, which meant the same game showed one number on the Daily Schedule and a
// different (better) one in the sportsbook. This hands the schedule the same
// blended line the book uses, so they agree.
//
// It is deliberately NOT called while a view is being built. buildGameLinePricer
// reads every active player through playersPlus, which is fine once on the
// sportsbook - a page that exists to price things - but is real work to add to a
// page that currently just lists games. The UI asks for this AFTER it has
// rendered, so the schedule is never slower than it is today and the refined
// numbers arrive a moment later, exactly as they already do on the book.
//
// Like everything else on this path it only ever READS the sim cache. Misses are
// queued for the background warmer and priced off the formula meanwhile.

// One day's slate. A cap anyway, so a malformed day can't queue the season.
// Exported so a test can hold the bound in place - this is the whole reason
// showing lines here doesn't cost anything.
export const MAX_WARM_SPREADS = 20;

export const getSimSpreads = async ({
	season,
	day,
}: {
	season: number;
	day: number;
}): Promise<Record<number, number>> => {
	const out: Record<number, number> = {};

	if (season !== g.get("season")) {
		// Past seasons have no upcoming games, and a completed game's spread has to
		// stay exactly what getGameSpread says - getTeamAtsRecords re-derives every
		// historical line that way, so showing a different number here would put
		// the displayed spread at odds with the ATS record.
		return out;
	}

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
		return out;
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

	for (const matchup of matchups) {
		const line = pricer.priceGame(matchup);
		if (line) {
			// GameLine.margin is the expected HOME margin, the same orientation
			// getGameSpread returns, so ScoreBox can use it interchangeably. Blending
			// leaves it continuous, so round it the way every other displayed spread
			// is rounded - a schedule that reads "-3.7" next to "-4" elsewhere is
			// worse than being half a point coarse.
			out[matchup.gid] = roundHalf(line.margin);
		}
	}

	const pending = pricer.pendingSims();
	if (pending.length > 0) {
		// Fire-and-forget, bounded. warmSimMargins refuses to start a second drain
		// while one is running, so the sportsbook and this can't compound.
		//
		// When margins land, the games the top bar and the Schedule page are
		// holding were priced off the formula, so refresh the one the top bar
		// shows. Otherwise the same game reads one number on the Daily Schedule
		// and another three feet above it.
		void warmSimMargins(pending.slice(0, MAX_WARM_SPREADS)).then(
			async (landed) => {
				if (landed) {
					await recomputeLocalUITeamOvrs();
				}
			},
		);
	}

	return out;
};
