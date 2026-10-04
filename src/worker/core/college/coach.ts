import type { CollegeCoach } from "../../../common/college.ts";
import type { OwnerMood } from "../../../common/types.ts";
import { PHASE } from "../../../common/constants.ts";
import { idb } from "../../db/index.ts";
import { g, helpers } from "../../util/index.ts";
import { league } from "../index.ts";

// YOU AS COACH
//
// The athletic director judges you each season against what your program
// should do: a blue blood is expected to win big and go deep in March, a
// low-major just to be competitive. That's the usual owner mood (wins and
// playoffs, no money), with the bar set by prestige. Contracts run four
// seasons; a coach on the hot seat isn't extended. Other schools come
// calling when you're doing well. Firing and job offers can each be turned
// off.

export const COLLEGE_CONTRACT_SEASONS = 4;

export const getCollegeCoach = async (): Promise<CollegeCoach> => {
	const userTid = g.get("userTid");
	const coach = g.get("collegeCoach");
	if (coach && coach.tid === userTid) {
		return coach;
	}
	// New job (or a league from before coaches existed).
	const start =
		g.get("phase") >= PHASE.PLAYOFFS ? g.get("season") + 1 : g.get("season");
	const next = {
		tid: userTid,
		start,
		exp: start + COLLEGE_CONTRACT_SEASONS - 1,
	};
	await league.setGameAttributes({ collegeCoach: next });
	return next;
};

// The season's change in the athletic director's mood, against expectations
// set by prestige.
export const collegeMoodDeltas = (
	prestige: number,
	won: number,
	lost: number,
	playoffRoundsWon: number,
): OwnerMood => {
	const games = won + lost;
	const winp = games > 0 ? won / games : 0.5;
	const expectedWinp = 0.3 + (0.5 * prestige) / 100;
	// Rounds of the NCAA tournament a program like this should win; -1 is
	// missing it.
	const expectedRounds =
		prestige >= 90 ? 2 : prestige >= 80 ? 1 : prestige >= 65 ? 0 : -1;

	let playoffs = 0.06 * (playoffRoundsWon - expectedRounds);
	if (playoffRoundsWon < 0 && expectedRounds >= 0) {
		playoffs -= 0.1;
	}
	if (playoffRoundsWon >= 6) {
		playoffs = Math.max(playoffs, 0.2);
	}

	return {
		wins: helpers.bound(0.5 * (winp - expectedWinp), -0.25, 0.25),
		playoffs: helpers.bound(playoffs, -0.25, 0.25),
		money: 0,
	};
};

// Stability as recruits see it: how long you've been there, less if you're
// on the hot seat.
export const userCoachStability = async (tid: number) => {
	const coach = g.get("collegeCoach");
	if (!coach || coach.tid !== tid) {
		return undefined;
	}
	const years = g.get("season") - coach.start;
	const ts = await idb.cache.teamSeasons.indexGet("teamSeasonsByTidSeason", [
		tid,
		g.get("season"),
	]);
	const mood = ts?.ownerMood;
	const total = mood ? mood.wins + mood.playoffs + mood.money : 0;
	const hotSeat = total < -0.5 ? 0.5 : 1;
	return helpers.bound(years / 8, 0.1, 1) * hotSeat;
};

// End of season: an extension if the contract is up and the AD is happy
// enough. Returns a line for the evaluation message.
export const collegeContractReview = async (moodTotal: number) => {
	const coach = await getCollegeCoach();
	const season = g.get("season");
	if (season < coach.exp) {
		return { text: `Your contract runs through ${coach.exp}.`, fired: false };
	}
	if (moodTotal < -0.25 && g.get("collegeCoachFiring")) {
		return {
			text: "Your contract is up, and we won't be renewing it.",
			fired: true,
		};
	}
	const exp = season + COLLEGE_CONTRACT_SEASONS;
	await league.setGameAttributes({ collegeCoach: { ...coach, exp } });
	return {
		text: `Your contract has been extended through ${exp}.`,
		fired: false,
	};
};
