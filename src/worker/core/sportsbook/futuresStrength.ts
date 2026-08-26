import { idb } from "../../db/index.ts";
import teamOvr from "../team/ovr.ts";
import { RATINGS } from "../../../common/constants.ts";
import { isSport } from "../../../common/sportFunctions.ts";
import { pregameLineupSynergyFromPlayers } from "../GameSim.basketball/synergy.ts";

// The team strength that drives every basketball futures market, built from the
// SAME engine-measured model as the per-game spread (getGameSpread): overall
// and lineup synergy, priced at the measured coefficients. Futures used to run
// on their own heuristic - (ovr - mean) * 0.6, capped at +/-9, shaded 15%
// preseason - and every one of those numbers disagreed with the engine the
// games are actually decided by, which made the boards free money for anyone
// who knew the engine better than the book did.
//
// Injuries are handled on an expected-availability basis rather than the two
// extremes (today's lineup / everyone magically healthy): a player out for K of
// the team's next H games contributes (1 - K/H) of his marginal value. A star
// lost for the season prices like he's gone; one out a week barely moves the
// number - which is exactly how a real book moves a win total on injury news.

export type FuturesTeamStrength = {
	// Full-strength team overall - every injury healed.
	ovr: number;
	// Overall expected over the team's remaining-games horizon (see above).
	expectedOvr: number;
	// Full-strength pregame lineup synergy, and its expected version. Undefined
	// outside basketball or when the roster is too small to field a lineup.
	synergy: number | undefined;
	expectedSynergy: number | undefined;
};

// Everyone available at full strength: with the game infinitely far away, every
// injury has healed. (playThroughInjuries is then irrelevant.)
const HEALED = {
	numDaysInFuture: Number.POSITIVE_INFINITY,
	playThroughInjuries: [0, 0] as [number, number],
	playoffs: false,
};

// Pure core, separated from the idb wrapper below so the calibration/EV
// harnesses can drive it with plain rosters.
export const futuresStrengthFromPlayers = (
	// teamOvr-shaped rows: { pid, injury, value, ratings: { ovr, pos, ovrs } }.
	players: any[],
	// The same players as raw cache rows (full ratings history), for the synergy
	// composites. Matched to `players` by pid.
	rawPlayers: any[],
	// How many games this team's remaining season holds, for the availability
	// weighting.
	horizonGames: number,
): FuturesTeamStrength => {
	const fullOvr = teamOvr(players, {});
	const fullSynergy = pregameLineupSynergyFromPlayers(rawPlayers, HEALED);

	let expectedOvr = fullOvr;
	let expectedSynergy = fullSynergy;
	const horizon = Math.max(1, horizonGames);
	for (const p of players) {
		const out = p.injury?.gamesRemaining ?? 0;
		if (out <= 0) {
			continue;
		}
		const missFrac = Math.min(1, out / horizon);
		// First-order marginal: what the team number loses while he sits. Joint
		// absences (two injured stars out at once) interact, but that error is a
		// fraction of a point on rosters people actually have.
		const withoutOvr = teamOvr(
			players.filter((x) => x !== p),
			{},
		);
		expectedOvr -= missFrac * Math.max(0, fullOvr - withoutOvr);
		if (fullSynergy !== undefined && expectedSynergy !== undefined) {
			const withoutSynergy = pregameLineupSynergyFromPlayers(
				rawPlayers.filter((x) => x.pid !== p.pid),
				HEALED,
			);
			if (withoutSynergy !== undefined) {
				// Signed on purpose: losing a bad-fit player can RAISE the lineup's
				// synergy, and then the team really does play better without him.
				expectedSynergy -= missFrac * (fullSynergy - withoutSynergy);
			}
		}
	}

	return { ovr: fullOvr, expectedOvr, synergy: fullSynergy, expectedSynergy };
};

export const getFuturesStrengths = async (
	teams: { tid: number; horizonGames: number }[],
	season: number,
): Promise<Map<number, FuturesTeamStrength>> => {
	const ratings = ["ovr", "pos", "ovrs"];
	if (isSport("basketball")) {
		ratings.push(...RATINGS);
	}

	const byTid = new Map<number, FuturesTeamStrength>();
	for (const t of teams) {
		const rawPlayers = await idb.cache.players.indexGetAll(
			"playersByTid",
			t.tid,
		);
		const players = await idb.getCopies.playersPlus(rawPlayers, {
			attrs: ["pid", "tid", "injury", "value"],
			ratings,
			stats: ["season", "tid"],
			season,
			showNoStats: true,
			showRookies: true,
			fuzz: false,
			// Feeds team.ovr, so it needs the real ratings - the display rounding
			// would put the whole league in a handful of ties.
			coarsenRatings: false,
			tid: t.tid,
		});
		byTid.set(
			t.tid,
			futuresStrengthFromPlayers(players as any[], rawPlayers, t.horizonGames),
		);
	}
	return byTid;
};
