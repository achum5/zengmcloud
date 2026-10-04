import { collegeSeedLine } from "../../common/college.ts";
import { PHASE } from "../../common/constants.ts";
import type { UpdateEvents } from "../../common/types.ts";
import { projectCollegeField } from "../core/college/tournaments.ts";
import { idb } from "../db/index.ts";
import { g } from "../util/index.ts";

// The projected NCAA field during the season (as if it were picked today),
// and the real fields - NCAA and NIT - once the postseason starts.
const updateBracketology = async (
	inputs: unknown,
	updateEvents: UpdateEvents,
) => {
	if (
		!updateEvents.includes("firstRun") &&
		!updateEvents.includes("gameSim") &&
		!updateEvents.includes("newPhase")
	) {
		return;
	}
	if (!g.get("college")) {
		return { college: false as const };
	}

	const season = g.get("season");
	const phase = g.get("phase");
	const teamInfoCache = g.get("teamInfoCache");
	const confs = g.get("confs", "current");
	const numPlayoffTeams = 64;

	const info = async (tid: number) => {
		const t = await idb.cache.teams.get(tid);
		const ts = await idb.cache.teamSeasons.indexGet("teamSeasonsByTidSeason", [
			tid,
			season,
		]);
		return {
			tid,
			abbrev: teamInfoCache[tid]?.abbrev ?? "",
			region: teamInfoCache[tid]?.region ?? "",
			name: teamInfoCache[tid]?.name ?? "",
			conf: confs.find((c) => c.cid === t?.cid)?.abbrev ?? "",
			won: ts?.won ?? 0,
			lost: ts?.lost ?? 0,
		};
	};

	let projected = true;
	let field: number[] = [];
	let autobids = new Set<number>();
	let lastFourIn: number[] = [];
	let firstFourOut: number[] = [];

	const playoffSeries =
		phase >= PHASE.PLAYOFFS
			? await idb.cache.playoffSeries.get(season)
			: undefined;
	if (playoffSeries) {
		projected = false;
		const seeds: [number, number][] = [];
		for (const { home, away } of playoffSeries.series[0] ?? []) {
			seeds.push([home.seed, home.tid]);
			if (away) {
				seeds.push([away.seed, away.tid]);
			}
		}
		field = seeds.sort((a, b) => a[0] - b[0]).map(([, tid]) => tid);
	} else if (phase >= PHASE.REGULAR_SEASON && phase < PHASE.PLAYOFFS) {
		const teams = (await idb.cache.teams.getAll())
			.filter((t) => !t.disabled)
			.map((t) => ({ tid: t.tid, seasonAttrs: { cid: t.cid } }));
		const projection = await projectCollegeField(teams, numPlayoffTeams);
		field = projection.field.map((t) => t.tid);
		autobids = projection.autobids;
		lastFourIn = projection.lastFourIn.map((t) => t.tid);
		firstFourOut = projection.firstFourOut.map((t) => t.tid);
	}

	const seedLines: {
		seed: number;
		teams: Awaited<ReturnType<typeof info>>[];
	}[] = [];
	for (const [i, tid] of field.entries()) {
		const seed = collegeSeedLine(i + 1);
		if (!seedLines[seed - 1]) {
			seedLines[seed - 1] = { seed, teams: [] };
		}
		seedLines[seed - 1]!.teams.push(await info(tid));
	}

	const nitState = g.get("collegeNit");
	const nit =
		nitState?.season === season
			? {
					field: await Promise.all(nitState.field.map(info)),
					alive: nitState.alive,
					champ: nitState.champ,
				}
			: undefined;

	return {
		college: true as const,
		season,
		projected,
		seedLines,
		autobids: [...autobids],
		lastFourIn: await Promise.all(lastFourIn.map(info)),
		firstFourOut: await Promise.all(firstFourOut.map(info)),
		nit,
		userTid: g.get("userTid"),
	};
};

export default updateBracketology;
