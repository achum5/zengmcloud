import { player } from "../index.ts";
import { PLAYER } from "../../../common/constants.ts";
import { g } from "../../util/index.ts";
import type { Player, PlayerWithoutKey, Team } from "../../../common/types.ts";
import { initRecruitClass } from "./recruiting.ts";
import { getNumPlayersPerTeam } from "../league/create/createRandomPlayers.ts";
import { collegeNilForPercentile, recruitClassSize } from "./util.ts";
import { NUM_COLLEGE_SCHOOLS } from "../../../common/collegeSchools.ts";

// Starting rosters for a college league: four classes (freshmen to seniors)
// on every team. Each class is one big pool of players; the best ones land at
// the biggest programs more often than not, so blue bloods start loaded and
// low-majors start thin - but a sleeper can still turn up anywhere.

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
	await player.develop(p, classIndex, true);
	p.collegeYear0 = g.get("season") - classIndex;
	return p;
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
	const perTeam = getNumPlayersPerTeam();
	const prestigeByTid = new Map(teams.map((t) => [t.tid, t.prestige ?? 30]));

	// Roughly even classes, a few more underclassmen than seniors (transfers
	// and early departures thin out the older classes).
	const perClass = [0.27, 0.26, 0.24, 0.23].map((share) =>
		Math.round(share * perTeam),
	);
	perClass[0]! += perTeam - perClass.reduce((a, b) => a + b, 0);

	const players: PlayerWithoutKey[] = [];
	const jerseyNumbers = new Map<number, string[]>();
	const finalSeasons = new Map<PlayerWithoutKey, number>();

	for (const [classIndex, slots] of perClass.entries()) {
		// The best players of the older classes already left early for the pros
		// (about 25 a year across D1), so generate them and drop them. Plus a
		// little extra so the last teams still get a choice of players.
		const numGonePro = Math.round(
			(classIndex * 25 * activeTids.length) / NUM_COLLEGE_SCHOOLS,
		);
		const poolSize = Math.ceil(slots * activeTids.length * 1.05) + numGonePro;
		const pool: PlayerWithoutKey[] = [];
		for (let i = 0; i < poolSize; i++) {
			const p = await genCollegePlayer(
				PLAYER.UNDRAFTED,
				classIndex,
				scoutingLevel,
			);
			p.value = player.value(p, { ovrMean: 47, ovrStd: 10 });
			pool.push(p);
		}
		pool.sort((a, b) => b.value - a.value);
		pool.splice(0, numGonePro);

		const openSlots = new Map(activeTids.map((tid) => [tid, slots]));
		for (const p of pool) {
			const open = activeTids.filter((tid) => openSlots.get(tid)! > 0);
			if (open.length === 0) {
				break;
			}
			const tid = pickWeighted(open, (tid2) =>
				Math.exp(prestigeByTid.get(tid2)! / 15),
			)!;
			openSlots.set(tid, openSlots.get(tid)! - 1);

			p.tid = tid;
			const taken = jerseyNumbers.get(tid) ?? [];
			jerseyNumbers.set(tid, taken);
			player.setJerseyNumber(p, await player.genJerseyNumber(p, taken, []));
			if (p.jerseyNumber !== undefined) {
				taken.push(p.jerseyNumber);
			}

			// The NIL deal runs through his final season of eligibility.
			finalSeasons.set(p, season + 3 - classIndex);
			players.push(p);
		}
	}

	// NIL deals go by where a player ranks in the whole league, on the same
	// scale as recruits' asks.
	const ranked = [...players].sort((a, b) => b.value - a.value);
	for (const [i, p] of ranked.entries()) {
		player.setContract(
			p,
			{
				amount: collegeNilForPercentile(i / ranked.length),
				exp: finalSeasons.get(p)!,
			},
			true,
		);
	}

	// Next year's high school class, waiting to be recruited.
	const recruits: PlayerWithoutKey[] = [];
	for (let i = 0; i < recruitClassSize(activeTids.length); i++) {
		const p = await genCollegePlayer(PLAYER.UNDRAFTED, 0, scoutingLevel);
		// Still a high school senior: 17, enrolling next season. draft.year is
		// the season they sign, at its end.
		p.born.year = season - 17;
		p.collegeYear0 = season + 1;
		p.draft.year = season;
		p.value = player.value(p, { ovrMean: 47, ovrStd: 10 });
		recruits.push(p);
	}
	initRecruitClass(recruits as Player[]);
	players.push(...recruits);

	return players;
};

export default createCollegePlayers;
