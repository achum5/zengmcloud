import { idb } from "../db/index.ts";
import g from "./g.ts";
import { helpers } from "../../common/helpers.ts";
import { takeArenaLooks } from "../core/game/replayLooks.ts";
import type { Team } from "../../common/types.ts";

// A team out warming up in the 3D preview of a court, as a game's box score
// has it: its first nine.
const warmingUp = async (t: Team) => {
	const players = (await idb.cache.players.indexGetAll("playersByTid", t.tid))
		.sort((a, b) => a.rosterOrder - b.rosterOrder)
		.slice(0, 9);
	return {
		tid: t.tid,
		abbrev: t.abbrev,
		region: t.region,
		name: t.name,
		colors: t.colors,
		jersey: t.jersey,
		imgURL: t.imgURL,
		imgURLSmall: t.imgURLSmall,
		jerseySkins: t.jerseySkins,
		players: players.map((p) => {
			const r = p.ratings.at(-1);
			return {
				pid: p.pid,
				name: `${p.firstName} ${p.lastName}`,
				pos: r?.pos,
				skills: r?.skills,
				jerseyNumber: helpers.getJerseyNumber(p),
			};
		}),
	};
};

// THE 3D PREVIEW OF A TEAM'S COURT (see CourtPreview): the team and the next
// one along warming up on it, in its building.
export const courtPreview = async (t: Team) => {
	const teams = (await idb.cache.teams.getAll()).filter(
		(other) => !other.disabled,
	);
	const i = teams.findIndex((other) => other.tid === t.tid);
	const visitors = teams[(i + 1) % teams.length];
	const season = g.get("season");
	return {
		season,
		home: await warmingUp(t),
		away:
			visitors && visitors.tid !== t.tid
				? await warmingUp(visitors)
				: undefined,
		arena: await takeArenaLooks(t.tid, season),
	};
};
