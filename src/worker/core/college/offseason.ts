import { PLAYER } from "../../../common/constants.ts";
import { collegeYear } from "../../../common/college.ts";
import { idb } from "../../db/index.ts";
import { g, helpers, logEvent } from "../../util/index.ts";
import { player } from "../index.ts";
import { genCollegePlayer } from "./createCollegePlayers.ts";
import { recruitClassSize } from "./util.ts";
import { initRecruitClass } from "./recruiting.ts";
import type { Conditions, Player } from "../../../common/types.ts";
import { last } from "../../../common/utils.ts";

// The end of a college season: seniors graduate, and the best underclassmen
// leave early for the pros. Everyone who leaves is retired from the league,
// with a note of why.

// Chance an underclassman turns pro. Only real prospects go, and the longer
// he's been in school the more likely he is to take the leap.
const earlyEntryChance = (p: Player, year: number) => {
	const { ovr } = last(p.ratings);
	const readiness = helpers.bound((ovr - 46) / 8, 0, 0.95);
	const yearFactor = year <= 1 ? 0.5 : year === 2 ? 0.75 : 0.9;
	return readiness * yearFactor;
};

export const collegeDepartures = async (conditions: Conditions) => {
	const season = g.get("season");
	const players = await idb.cache.players.indexGetAll("playersByTid", [
		0,
		Infinity,
	]);

	const departures = new Map<number, { p: Player; draft: boolean }[]>();
	for (const p of players) {
		const year = collegeYear(p, season);
		if (year === undefined) {
			continue;
		}

		let draft = false;
		if (year < 4) {
			if (Math.random() >= earlyEntryChance(p, year)) {
				continue;
			}
			draft = true;
		}

		const tid = p.tid;
		p.collegeExit = draft ? "draft" : "graduated";
		await player.retire(p, conditions, { logRetiredEvent: false });
		await idb.cache.players.put(p);

		const list = departures.get(tid) ?? [];
		list.push({ p, draft });
		departures.set(tid, list);
	}

	for (const [tid, list] of departures) {
		const text = list
			.map(
				({ p, draft }) =>
					`<a href="${helpers.leagueUrl(["player", p.pid])}">${p.firstName} ${
						p.lastName
					}</a> ${draft ? "declared for the draft" : "graduated"}.`,
			)
			.join("<br>");
		logEvent(
			{
				type: "retiredList",
				text,
				showNotification: tid === g.get("userTid"),
				pids: list.map(({ p }) => p.pid),
				tids: [tid],
				saveToDb: false,
			},
			conditions,
		);
	}
};

// A high school class: seniors during `draftYear`, enrolling the season after.
// They sit as prospects until the signing period at the end of that season.
export const genCollegeRecruits = async (draftYear: number) => {
	const existing = (
		await idb.cache.players.indexGetAll("playersByTid", PLAYER.UNDRAFTED)
	).filter((p) => p.draft.year === draftYear);
	const target = recruitClassSize(g.get("numActiveTeams"));

	const added: Player[] = [];
	for (let i = existing.length; i < target; i++) {
		const p = await genCollegePlayer(PLAYER.UNDRAFTED, 0, 0);
		p.collegeYear0 = draftYear + 1;
		p.draft.year = draftYear;
		// 17 as a senior.
		p.born.year = draftYear - 17;
		await idb.cache.players.add(p);
		await player.updateValues(p as Player);
		added.push(p as Player);
	}

	if (added.length > 0) {
		initRecruitClass([...existing, ...added]);
		await idb.cache.players.putAll([...existing, ...added]);
	}
};
