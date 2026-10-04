import { getCollegeTeams } from "../core/college/setup.ts";

// The school picker for a new college league.
const updateNewCollegeLeague = async () => {
	const { confs, teams } = getCollegeTeams();
	return {
		conferences: confs.map((conf) => ({
			cid: conf.cid,
			name: conf.name,
			abbrev: conf.abbrev ?? conf.name,
			teams: teams
				.filter((t) => t.cid === conf.cid)
				.map((t) => ({
					tid: t.tid,
					region: t.region,
					name: t.name,
					abbrev: t.abbrev,
					colors: t.colors,
					prestige: t.prestige,
				})),
		})),
	};
};

export default updateNewCollegeLeague;
