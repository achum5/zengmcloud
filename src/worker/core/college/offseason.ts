import { PHASE, PLAYER } from "../../../common/constants.ts";
import { collegeYear } from "../../../common/college.ts";
import { idb } from "../../db/index.ts";
import { g, helpers, logEvent } from "../../util/index.ts";
import { player } from "../index.ts";
import { genCollegePlayer } from "./createCollegePlayers.ts";
import { collegeNilForValue, recruitClassSize } from "./util.ts";
import { getNumPlayersPerTeam } from "../league/create/createRandomPlayers.ts";
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

	for (let i = existing.length; i < target; i++) {
		const p = await genCollegePlayer(PLAYER.UNDRAFTED, 0, 0);
		p.collegeYear0 = draftYear + 1;
		p.draft.year = draftYear;
		// 17 as a senior.
		p.born.year = draftYear - 17;
		await idb.cache.players.add(p);
		await player.updateValues(p as Player);
	}
};

// Signing day (stand-in until the full recruiting system): every school fills
// its open scholarships from this year's high school class. The best recruits
// lean heavily toward prestigious programs and teams that just won; nobody is
// guaranteed anyone. Whoever is left unsigned becomes a walk-on.
export const collegeSigningDay = async () => {
	const season = g.get("season");
	const recruits = (
		await idb.cache.players.indexGetAll("playersByTid", PLAYER.UNDRAFTED)
	)
		.filter((p) => p.draft.year === season)
		.sort((a, b) => b.value - a.value);

	const teams = (await idb.cache.teams.getAll()).filter((t) => !t.disabled);
	const teamSeasons = await idb.cache.teamSeasons.indexGetAll(
		"teamSeasonsBySeasonTid",
		[[season], [season, "Z"]],
	);
	const winpByTid = new Map<number, number>();
	for (const ts of teamSeasons) {
		const games = ts.won + ts.lost;
		winpByTid.set(ts.tid, games > 0 ? ts.won / games : 0.5);
	}

	const scholarships = getNumPlayersPerTeam();
	const openByTid = new Map<number, number>();
	const scoreByTid = new Map<number, number>();
	for (const t of teams) {
		const roster = await idb.cache.players.indexGetAll("playersByTid", t.tid);
		openByTid.set(t.tid, Math.max(0, scholarships - roster.length));
		const winp = winpByTid.get(t.tid) ?? 0.5;
		scoreByTid.set(t.tid, (t.prestige ?? 30) + 30 * (winp - 0.5));
	}

	for (const p of recruits) {
		const open = teams.filter((t) => openByTid.get(t.tid)! > 0);
		if (open.length === 0) {
			break;
		}
		let total = 0;
		const weights = open.map((t) => {
			const w = Math.exp(scoreByTid.get(t.tid)! / 15);
			total += w;
			return w;
		});
		let r = Math.random() * total;
		let tid = open.at(-1)!.tid;
		for (let i = 0; i < open.length; i++) {
			r -= weights[i]!;
			if (r <= 0) {
				tid = open[i]!.tid;
				break;
			}
		}
		openByTid.set(tid, openByTid.get(tid)! - 1);

		await player.sign(
			p,
			tid,
			{
				amount: collegeNilForValue(p.value),
				exp: season + 4,
			},
			PHASE.RESIGN_PLAYERS,
		);
		await idb.cache.players.put(p);
	}
};
