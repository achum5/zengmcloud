import { PLAYER } from "../../../common/constants.ts";
import { collegeFinalSeason, collegeYear } from "../../../common/college.ts";
import { idb } from "../../db/index.ts";
import { g, helpers, logEvent } from "../../util/index.ts";
import { player } from "../index.ts";
import { calibrateClass, genCollegePlayer } from "./createCollegePlayers.ts";
import { recruitClassSize } from "./util.ts";
import { initRecruitClass } from "./recruiting.ts";
import type { Conditions, Player } from "../../../common/types.ts";
import { last } from "../../../common/utils.ts";

// The end of a college season: players out of eligibility move on, and the
// best prospects leave early for the draft. Everyone who leaves is retired
// from the league with a note of why; those who declared are the class a
// linked pro league drafts from (see handoff.ts).

// How ready he is for the pros, on the pro rating scale: what he is now, or
// for a young player with a high ceiling, what he projects to be.
export const proReadiness = (p: Player) => {
	const { ovr, pot } = last(p.ratings);
	return Math.max(ovr, pot - 14);
};

// Chance a player with eligibility left declares. By draft stock on the pro
// scale, not by rank, so anyone who'd be a real pro prospect tends to go -
// otherwise future pros pile up in college and keep developing there.
// Older players are readier to leave.
export const declareChance = (readiness: number, year: number) => {
	let chance = 0;
	if (readiness >= 56) {
		chance = 0.9;
	} else if (readiness >= 53) {
		chance = 0.5;
	} else if (readiness >= 51) {
		chance = 0.2;
	} else if (readiness >= 49) {
		chance = 0.05;
	}
	const yearFactor =
		year <= 1 ? 0.75 : year === 2 ? 0.9 : year === 3 ? 1 : 1.05;
	return helpers.bound(
		chance * yearFactor * g.get("collegeDepartureRate"),
		0,
		0.98,
	);
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
		const final = collegeFinalSeason(p)!;
		const readiness = proReadiness(p);

		let draft = false;
		if (final <= season) {
			// Out of eligibility. The good ones head to the draft.
			draft = readiness >= 51;
		} else if (Math.random() < declareChance(readiness, year)) {
			draft = true;
		} else {
			continue;
		}

		const tid = p.tid;
		p.collegeExit = draft ? "draft" : "graduated";
		await player.retire(p, conditions, { logRetiredEvent: false });
		await idb.cache.players.put(p);

		const list = departures.get(tid) ?? [];
		list.push({ p, draft });
		departures.set(tid, list);
	}

	// Pro pipeline, for recruits who care about it.
	for (const t of await idb.cache.teams.getAll()) {
		if (t.disabled) {
			continue;
		}
		const n = (departures.get(t.tid) ?? []).filter((row) => row.draft).length;
		t.collegePros = [...(t.collegePros ?? []), n].slice(-5);
		await idb.cache.teams.put(t);
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
// They're recruited through that season and its offseason, and sign at the
// end of it.
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
		added.push(p as Player);
	}

	if (added.length > 0) {
		await calibrateClass(added);
		for (const p of added) {
			await player.updateValues(p);
		}
		initRecruitClass([...existing, ...added]);
		await idb.cache.players.putAll([...existing, ...added]);
	}
};
