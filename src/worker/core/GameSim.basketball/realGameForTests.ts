// A whole game played by the real sim with real ratings, for the 3D court:
// its play-by-play and roster (pid, side, position, skills, ratings). The same
// seed is the same game. For tests and tools only (it resets the cache).
import { resetCache, resetG } from "../../../test/helpers.ts";
import { idb } from "../../db/index.ts";
import { g, helpers } from "../../util/index.ts";
import { PHASE } from "../../../common/constants.ts";
import { player, team } from "../index.ts";
import GameSim from "../GameSim.ts";
import { processTeam } from "../game/loadTeams.ts";
import createRandomPlayers from "../league/create/createRandomPlayers.ts";
import { DEFAULT_LEVEL } from "../../../common/budgetLevels.ts";

const rng = (seed: number) => {
	let a = seed;
	return () => {
		a |= 0;
		a = (a + 0x6d2b79f5) | 0;
		let t = Math.imul(a ^ (a >>> 15), 1 | a);
		t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
		return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
	};
};
const KEYS = [
	"ovr",
	"hgt",
	"stre",
	"spd",
	"jmp",
	"endu",
	"ins",
	"dnk",
	"ft",
	"fg",
	"tp",
	"oiq",
	"diq",
	"drb",
	"pss",
	"reb",
];

export const simRealGame = async (
	seed: number,
): Promise<{ events: any[]; players: any[] }> => {
	const realRandom = Math.random;
	try {
		Math.random = rng(seed * 7919 + 13);
		resetG();
		g.setWithoutSavingToDB("numActiveTeams", 2);
		g.setWithoutSavingToDB("numTeams", 2);
		g.setWithoutSavingToDB("phase", PHASE.REGULAR_SEASON);
		const teams: any[] = [];
		for (let tid = 0; tid < 2; tid++) {
			teams.push(
				team.generate({
					tid,
					cid: 0,
					did: 0,
					region: `R${tid}`,
					name: `N${tid}`,
					abbrev: `T${tid}`,
					pop: 1,
					imgURL: "",
				} as any),
			);
		}
		const players = await createRandomPlayers({
			activeTids: [0, 1],
			onlyFreeAgents: false,
			scoutingLevel: DEFAULT_LEVEL,
			teams,
		});
		await resetCache({ players, teams, draftPicks: [] });
		for (let tid = 0; tid < 2; tid++) {
			const t = (await idb.cache.teams.get(tid))!;
			await idb.cache.teamSeasons.add(team.genSeasonRow(t) as any);
		}
		for (const p of await idb.cache.players.indexGetAll("playersByTid", [
			0,
			Infinity,
		])) {
			await player.updateValues(p);
			p.injury = { type: "Healthy", gamesRemaining: 0 };
			await idb.cache.players.put(p);
		}
		const load = async (tid: number) => {
			const t = await idb.cache.teams.get(tid);
			const ts = await idb.cache.teamSeasons.indexGet(
				"teamSeasonsBySeasonTid",
				[g.get("season"), tid],
			);
			const ps = await idb.cache.players.indexGetAll("playersByTid", tid);
			return processTeam(t!, ts!, ps);
		};
		const sides = [await load(0), await load(1)];
		const result: any = new GameSim({
			gid: 1,
			day: 1,
			teams: helpers.deepCopy(sides) as any,
			doPlayByPlay: true,
			homeCourtFactor: 1,
			neutralSite: false,
			allStarGame: false,
			baseInjuryRate: 0,
		} as any).run();
		const all = await idb.cache.players.getAll();
		const byPid = new Map(all.map((p: any) => [p.pid, p]));
		const roster = sides.flatMap((side: any, raw) =>
			side.player.map((sp: any) => {
				const p: any = byPid.get(sp.id);
				const r = p?.ratings.at(-1) ?? {};
				return {
					pid: sp.id,
					team: raw === 0 ? 1 : 0,
					pos: r.pos,
					skills: r.skills,
					ratings: Object.fromEntries(
						KEYS.filter((k) => typeof r[k] === "number").map((k) => [k, r[k]]),
					),
				};
			}),
		);
		return { events: result.playByPlay, players: roster };
	} finally {
		Math.random = realRandom;
	}
};
