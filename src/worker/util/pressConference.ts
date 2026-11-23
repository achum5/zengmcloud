import { idb } from "../db/index.ts";
import { helpers } from "../../common/helpers.ts";
import { PHASE } from "../../common/constants.ts";
import type { EventBBGM, Player } from "../../common/types.ts";
import { takeArenaLooks } from "../core/game/replayLooks.ts";
import { inferRaceFromFace } from "./realisticFaces.ts";
import { getTeamInfoBySeason } from "./getTeamInfoBySeason.ts";
import { teamThen } from "./teamThen.ts";

// THE PRESS CONFERENCE THAT INTRODUCES HIM (see court3d/pressStage.ts): a
// team that traded for a player, or signed one as a free agent, with this
// much potential as he was then shows him off - each team of a trade its own,
// all of those it got at once. Only what the game already did: nothing here
// changes anything.
export const PRESSER_POT = 50;

// His ratings as they were then: the row the move kept, or the season's.
const potThen = (p: Player, season: number, ratingsIndex?: number): number => {
	const kept = ratingsIndex === undefined ? undefined : p.ratings[ratingsIndex];
	const r =
		kept?.season === season
			? kept
			: (p.ratings.findLast((x) => x.season <= season) ?? p.ratings[0]);
	return r?.pot ?? 0;
};

export type Presser = { tid: number; pids: number[] };

// Who holds one for a move, and for whom. (A player whose face is a photo is
// a silhouette on the court: no scene of him.)
export const pressersOf = async (event: EventBBGM): Promise<Presser[]> => {
	if (__SPORT !== "basketball") {
		return [];
	}
	if (event.type === "trade") {
		if (!event.teams) {
			return [];
		}
		const out: Presser[] = [];
		for (const k of [0, 1] as const) {
			const tid = event.tids[k];
			const pids: number[] = [];
			for (const a of event.teams[k].assets) {
				if (!("pid" in a)) {
					continue;
				}
				const p = await idb.merged.players.get(a.pid);
				if (
					p &&
					!p.imgURL &&
					potThen(p, event.season, a.ratingsIndex) >= PRESSER_POT
				) {
					pids.push(p.pid);
				}
			}
			if (pids.length > 0 && tid >= 0) {
				out.push({ tid, pids });
			}
		}
		return out;
	}
	if (event.type === "freeAgent") {
		const pid = event.pids[0];
		const tid = event.tids[0];
		const p = await idb.merged.players.get(pid);
		if (p && !p.imgURL && tid >= 0 && potThen(p, event.season) >= PRESSER_POT) {
			return [{ tid, pids: [pid] }];
		}
	}
	return [];
};

// The teams holding one for a move - only those holding one for this team,
// or for this player, if asked - for the pages that list moves.
export const pressTidsOf = async (
	event: EventBBGM,
	only?: { tid?: number; pid?: number },
): Promise<number[] | undefined> => {
	if (event.type !== "trade" && event.type !== "freeAgent") {
		return undefined;
	}
	const tids = (await pressersOf(event))
		.filter(
			(x) =>
				(only?.tid === undefined || x.tid === only.tid) &&
				(only?.pid === undefined || x.pids.includes(only.pid)),
		)
		.map((x) => x.tid);
	return tids.length > 0 ? tids : undefined;
};

// His number with them: the one he wore for them that season (or the next,
// traded for after it), or the one he has now if he is still there.
const numberWith = (p: Player, tid: number, season: number) => {
	const row = p.stats.find(
		(s) => s.tid === tid && s.season >= season && s.jerseyNumber,
	);
	if (row?.jerseyNumber) {
		return row.jerseyNumber;
	}
	if (p.tid === tid) {
		return helpers.getJerseyNumber(p);
	}
	return undefined;
};

// How many seasons a contract signed then runs: one more if it was signed
// with the season still to finish.
const yearsOf = (exp: number, season: number, phase: number | undefined) =>
	Math.max(
		1,
		exp - season + (phase === undefined || phase <= PHASE.PLAYOFFS ? 1 : 0),
	);

// WHAT A PRESS CONFERENCE'S SCENE IS MADE FROM: the team as it looked that
// year - its colors, logo, uniform and building - and the men it is
// introducing, as they were: their numbers with it, their faces, how big they
// are; where a traded man came from, or a free agent's contract. Nothing, if
// the move has none for this team.
export const pressConference = async (eid: number, tid: number) => {
	if (__SPORT !== "basketball") {
		return undefined;
	}
	const event = await idb.merged.events.get(eid);
	if (!event) {
		return undefined;
	}
	const presser = (await pressersOf(event)).find((x) => x.tid === tid);
	if (!presser) {
		return undefined;
	}
	const season = event.season;
	const team = await teamThen(tid, season);
	if (!team) {
		return undefined;
	}
	const players = [];
	for (const pid of presser.pids) {
		const p = await idb.merged.players.get(pid);
		if (!p) {
			continue;
		}
		const r = p.ratings.findLast((x) => x.season <= season) ?? p.ratings[0];
		players.push({
			pid: p.pid,
			name: `${p.firstName} ${p.lastName}`,
			firstName: p.firstName,
			lastName: p.lastName,
			pos: r?.pos,
			ovr: r?.ovr ?? 0,
			pot: r?.pot ?? 0,
			age: season - p.born.year,
			hgt: p.hgt,
			weight: p.weight,
			jerseyNumber: numberWith(p, tid, season),
			// (His family's faces, his own build, drawn as his.)
			race: p.face ? inferRaceFromFace(p.face) : undefined,
		});
	}
	if (players.length === 0) {
		return undefined;
	}
	// The best of them in the middle (see pressStage.ts) - first here.
	players.sort((a, b) => b.ovr + b.pot - (a.ovr + a.pot));
	let from: { region: string; name: string; abbrev: string } | undefined;
	let contract: { amount: number; years: number } | undefined;
	if (event.type === "trade") {
		const other = event.tids[0] === tid ? event.tids[1] : event.tids[0];
		const info = await getTeamInfoBySeason(other, season);
		if (info) {
			from = { region: info.region, name: info.name, abbrev: info.abbrev };
		}
	} else if (event.type === "freeAgent" && event.contract) {
		contract = {
			amount: event.contract.amount,
			years: yearsOf(event.contract.exp, season, event.phase),
		};
	}
	return {
		type: "pressConference" as const,
		kind: event.type === "trade" ? ("trade" as const) : ("signing" as const),
		eid,
		season,
		arena: await takeArenaLooks(team.tid, season),
		team,
		players,
		...(from ? { from } : {}),
		...(contract ? { contract } : {}),
	};
};

// (As the page asks for it - see court3d/PressStage.tsx.)
export const getPressConference = async ({
	eid,
	tid,
}: {
	eid: number;
	tid: number;
}) => (await pressConference(eid, tid)) ?? null;
