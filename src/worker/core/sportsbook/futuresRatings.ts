import { idb } from "../../db/index.ts";
import teamOvr from "../team/ovr.ts";
import { RATINGS } from "../../../common/constants.ts";
import { pregameLineupSynergyFromPlayers } from "../GameSim.basketball/synergy.ts";
import { getActualPlayThroughInjuries } from "../game/loadTeams.ts";
import { rosterAtTipoff } from "../team/rosterAtTipoff.ts";
import {
	BASKETBALL_PLAYOFF_OVR_SLOPE,
	BASKETBALL_PLAYOFF_SYNERGY_COEF,
	BASKETBALL_PLAYOFF_SYNERGY_OVR_SLOPE,
	BASKETBALL_SYNERGY_COEF,
	BASKETBALL_SYNERGY_OVR_SLOPE,
} from "../../../common/getGameSpread.ts";
import type { SpreadBiasEntry } from "../../util/getTeamSpreadBias.ts";
import { player } from "../index.ts";

// EACH TEAM'S STRENGTH ON THE GAME LINES' OWN SCALE. Points of margin against
// a league-average team on a neutral floor, read exactly the way a game's
// spread is: team overall and lineup synergy at the measured coefficients
// (getGameSpread), plus the per-team form correction the lines carry
// (getTeamSpreadBias). So a team the board makes a 6-point favourite tonight
// is the same team, at the same strength, in its title price and its win
// total.
//
// Injuries count for the days they last: the regular season is the team
// averaged over the days it has left (each man back when his injury is up),
// the playoffs the team as it will be when they start.

const SAMPLES = 24;

export type FuturesRating = {
	// Regular-season games.
	season: number;
	// Playoff games: the playoff margin model (synergy counts double).
	playoffs: number;
};

type Strength = { ovr: number; synergy: number | undefined };

export const getLinesModelRatings = async ({
	teams,
	season,
	daysLeft,
	inPlayoffs,
	spreadBiases,
	lengthFactor,
}: {
	teams: { tid: number; playThroughInjuries: [number, number] }[];
	season: number;
	daysLeft: number;
	inPlayoffs: boolean;
	spreadBiases: Map<number, SpreadBiasEntry>;
	lengthFactor: number;
}): Promise<Map<number, FuturesRating>> => {
	const ratings = [
		"ovr",
		"pos",
		"ovrs",
		...(__SPORT === "basketball" ? RATINGS : []),
	] as const;
	const days = Math.max(1, Math.round(daysLeft));
	const step = Math.max(1, Math.ceil(days / SAMPLES));

	const regular = new Map<number, Strength>();
	const playoff = new Map<number, Strength>();
	for (const t of teams) {
		const raw = await rosterAtTipoff(
			t.tid,
			await idb.cache.players.indexGetAll("playersByTid", t.tid),
		);
		const formatted = (await player.formatAll(raw, {
			attrs: ["tid", "injury", "value"],
			ratings,
			stats: ["season", "tid"],
			season,
			showNoStats: true,
			showRookies: true,
			fuzz: false,
			coarsenRatings: false,
			tid: t.tid,
		})) as any[];
		const pti = getActualPlayThroughInjuries(t);
		const at = (numDaysInFuture: number, playoffs: boolean): Strength => {
			const playThroughInjuries: [number, number] = playoffs
				? [pti[1], pti[1]]
				: pti;
			return {
				ovr: teamOvr(formatted, {
					accountForInjuredPlayers: { numDaysInFuture, playThroughInjuries },
					playoffs,
				}),
				synergy: pregameLineupSynergyFromPlayers(raw, {
					numDaysInFuture,
					playThroughInjuries,
					playoffs,
				}),
			};
		};
		let ovrSum = 0;
		let synSum = 0;
		let n = 0;
		let synOk = true;
		for (let d = 0; d < days; d += step) {
			const s = at(d, inPlayoffs);
			ovrSum += s.ovr;
			if (s.synergy === undefined) {
				synOk = false;
			} else {
				synSum += s.synergy;
			}
			n += 1;
		}
		const mean: Strength = {
			ovr: ovrSum / n,
			synergy: synOk ? synSum / n : undefined,
		};
		regular.set(t.tid, mean);
		playoff.set(t.tid, inPlayoffs ? mean : at(days, true));
	}

	const relative = (
		strengths: Map<number, Strength>,
		playoffs: boolean,
	): Map<number, number> => {
		const list = [...strengths.values()];
		const meanOvr = list.reduce((s, x) => s + x.ovr, 0) / list.length;
		const withSyn = list.filter((x) => x.synergy !== undefined);
		const meanSyn =
			withSyn.length > 0
				? withSyn.reduce((s, x) => s + x.synergy!, 0) / withSyn.length
				: 0;
		const out = new Map<number, number>();
		for (const [tid, s] of strengths) {
			const margin =
				s.synergy !== undefined && withSyn.length === list.length
					? (playoffs
							? BASKETBALL_PLAYOFF_SYNERGY_OVR_SLOPE
							: BASKETBALL_SYNERGY_OVR_SLOPE) *
							(s.ovr - meanOvr) +
						(playoffs
							? BASKETBALL_PLAYOFF_SYNERGY_COEF
							: BASKETBALL_SYNERGY_COEF) *
							(s.synergy - meanSyn)
					: (playoffs ? BASKETBALL_PLAYOFF_OVR_SLOPE : 15 / 50) *
						(s.ovr - meanOvr);
			out.set(tid, margin * lengthFactor);
		}
		return out;
	};

	const reg = relative(regular, false);
	const po = relative(playoff, true);
	// The form correction is measured on the regular season's margins.
	const biasMean =
		teams.reduce((s, t) => s + (spreadBiases.get(t.tid)?.bias ?? 0), 0) /
		Math.max(1, teams.length);
	const out = new Map<number, FuturesRating>();
	for (const t of teams) {
		const bias = (spreadBiases.get(t.tid)?.bias ?? 0) - biasMean;
		out.set(t.tid, {
			season: (reg.get(t.tid) ?? 0) + bias,
			playoffs: (po.get(t.tid) ?? 0) + bias,
		});
	}
	return out;
};
