import { player } from "../index.ts";
import { idb } from "../../db/index.ts";
import { PLAYER } from "../../../common/constants.ts";
import { COLLEGE_SEASONS } from "../../../common/college.ts";
import { g } from "../../util/index.ts";
import type { Player, PlayerWithoutKey, Team } from "../../../common/types.ts";
import { initRecruitClass } from "./recruiting.ts";
import {
	collegeNilForPercentile,
	collegeStarsForPercentile,
	recruitClassSize,
} from "./util.ts";
import { genCollegeProfile } from "./profile.ts";
import { roundNil } from "./negotiation.ts";
import limitRating from "../player/limitRating.ts";
import { last } from "../../../common/utils.ts";

// Starting rosters for a college league: five classes (freshmen to fifth-year
// seniors) on every team. Each class is one big pool of players; the best
// ones land at the biggest programs more often than not, so blue bloods start
// loaded and low-majors start thin - but a sleeper can still turn up anywhere.
//
// Ratings are on the same scale as a pro league's draft prospects, so a
// player who leaves for the draft is exactly what a pro league expects.

const pickWeighted = <T>(items: T[], weight: (item: T) => number) => {
	let total = 0;
	const weights = items.map((item) => {
		const w = Math.max(0, weight(item));
		total += w;
		return w;
	});
	let r = Math.random() * total;
	for (let i = 0; i < items.length; i++) {
		r -= weights[i]!;
		if (r <= 0) {
			return items[i];
		}
	}
	return items.at(-1);
};

// A freshman is 18. Developing him k seasons makes him a k+1 year player.
export const genCollegePlayer = async (
	tid: number,
	classIndex: number,
	scoutingLevel: number,
) => {
	const name = await player.name();
	const p = player.generate(
		tid,
		18,
		g.get("season"),
		true,
		scoutingLevel,
		name,
	);
	p.collegeYear0 = g.get("season") - classIndex;
	// Fills in ovr and pot, which generate leaves at 0.
	await player.develop(p, 0);
	return p;
};

// Players generated the normal way are all pro prospects - a pro draft class
// is only about 70 deep, and those come from several college classes. A high
// school class is twenty times that, so beyond the very top of the class
// ratings drop off, down to walk-on level at the bottom.
export const calibrateClass = async (players: PlayerWithoutKey[]) => {
	for (const p of players) {
		p.value = player.value(p, { ovrMean: 47, ovrStd: 10 });
	}
	const sorted = [...players].sort((a, b) => b.value - a.value);
	const top = Math.round((40 * g.get("numActiveTeams")) / 365);
	const n = sorted.length;
	for (const [i, p] of sorted.entries()) {
		if (i < top) {
			continue;
		}
		const offset = 13 * ((i - top) / Math.max(1, n - top)) ** 0.45;
		const ratings = last(p.ratings) as unknown as Record<string, unknown>;
		for (const [key, value] of Object.entries(ratings)) {
			if (
				typeof value === "number" &&
				key !== "hgt" &&
				key !== "ovr" &&
				key !== "pot" &&
				key !== "season" &&
				key !== "fuzz"
			) {
				ratings[key] = limitRating(value - offset);
			}
		}
		await player.develop(p, 0);
		p.value = player.value(p, { ovrMean: 47, ovrStd: 10 });
	}
};

// A walk-on made up on the spot, for a school that somehow can't field a
// roster: bottom-of-the-class ratings, any class year.
export const genCollegeWalkOn = async () => {
	const classIndex = Math.floor(Math.random() * 4);
	const p = await genCollegePlayer(PLAYER.FREE_AGENT, classIndex, 0);
	const ratings = last(p.ratings) as unknown as Record<string, unknown>;
	for (const [key, value] of Object.entries(ratings)) {
		if (
			typeof value === "number" &&
			!["hgt", "ovr", "pot", "season", "fuzz"].includes(key)
		) {
			ratings[key] = limitRating(value - 12);
		}
	}
	await player.develop(p, classIndex, true);
	p.collegeStars = 1;
	p.collegeProfile = genCollegeProfile(1);
	player.setContract(
		p,
		{
			amount: g.get("minContract"),
			exp: p.collegeYear0! + COLLEGE_SEASONS - 1,
		},
		false,
	);
	const pid = await idb.cache.players.add(p);
	const added = (await idb.cache.players.get(pid))!;
	await player.updateValues(added);
	return added;
};

const createCollegePlayers = async ({
	activeTids,
	scoutingLevel,
	teams,
}: {
	activeTids: number[];
	scoutingLevel: number;
	teams: Pick<Team, "tid" | "prestige" | "retiredJerseyNumbers">[];
}) => {
	const season = g.get("season");
	const nilScale = g.get("collegeNilScale");
	const rosterSize = g.get("maxRosterSize");
	const prestigeByTid = new Map(teams.map((t) => [t.tid, t.prestige ?? 30]));
	const classSize = recruitClassSize(activeTids.length);

	// Roughly even classes, a few more underclassmen (early departures and
	// transfers thin out the older classes).
	const shares = [0.22, 0.21, 0.2, 0.19, 0.18];
	const perClass = shares.map((share) => Math.round(share * rosterSize));
	perClass[0]! += rosterSize - perClass.reduce((a, b) => a + b, 0);

	const players: PlayerWithoutKey[] = [];
	const jerseyNumbers = new Map<number, string[]>();

	for (let classIndex = 0; classIndex < COLLEGE_SEASONS; classIndex++) {
		const slots = perClass[classIndex]!;
		// The best players of older classes have already left for the draft.
		const numGonePro = Math.round((classIndex * 20 * activeTids.length) / 365);

		const pool: PlayerWithoutKey[] = [];
		for (let i = 0; i < classSize + numGonePro; i++) {
			pool.push(
				await genCollegePlayer(PLAYER.UNDRAFTED, classIndex, scoutingLevel),
			);
		}
		await calibrateClass(pool);
		for (const p of pool) {
			await player.develop(p, classIndex, true);
			p.value = player.value(p, { ovrMean: 47, ovrStd: 10 });
		}
		pool.sort((a, b) => b.value - a.value);
		pool.splice(0, numGonePro);

		const openSlots = new Map(activeTids.map((tid) => [tid, slots]));
		for (const [rank, p] of pool.entries()) {
			const open = activeTids.filter((tid) => openSlots.get(tid)! > 0);
			if (open.length === 0) {
				break;
			}
			const tid = pickWeighted(open, (tid2) =>
				Math.exp(prestigeByTid.get(tid2)! / 15),
			)!;
			openSlots.set(tid, openSlots.get(tid)! - 1);

			p.tid = tid;
			p.collegeStars = collegeStarsForPercentile(rank / classSize);
			p.collegeProfile = genCollegeProfile(p.collegeStars);
			const taken = jerseyNumbers.get(tid) ?? [];
			jerseyNumbers.set(tid, taken);
			player.setJerseyNumber(p, await player.genJerseyNumber(p, taken, []));
			if (p.jerseyNumber !== undefined) {
				taken.push(p.jerseyNumber);
			}
			players.push(p);
		}
	}

	// NIL deals go by where a player ranks in the whole league, on the same
	// scale as recruits' asks. They run through his final season.
	const ranked = [...players].sort((a, b) => b.value - a.value);
	for (const [i, p] of ranked.entries()) {
		player.setContract(
			p,
			{
				amount: roundNil(collegeNilForPercentile(i / ranked.length) * nilScale),
				exp: p.collegeYear0! + COLLEGE_SEASONS - 1,
			},
			true,
		);
	}

	// This year's high school class, waiting to be recruited.
	const recruits: PlayerWithoutKey[] = [];
	for (let i = 0; i < classSize; i++) {
		const p = await genCollegePlayer(PLAYER.UNDRAFTED, 0, scoutingLevel);
		// Still a high school senior: 17, enrolling next season. draft.year is
		// the season they sign, at its end.
		p.born.year = season - 17;
		p.collegeYear0 = season + 1;
		p.draft.year = season;
		recruits.push(p);
	}
	await calibrateClass(recruits);
	initRecruitClass(recruits as Player[]);
	players.push(...recruits);

	return players;
};

export default createCollegePlayers;
