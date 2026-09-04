import { assert, test } from "vitest";
import { resetCache, resetG } from "../../test/helpers.ts";
import { idb } from "../db/index.ts";
import { g, helpers } from "./index.ts";
import { PHASE } from "../../common/constants.ts";
import { player, team } from "../core/index.ts";
import GameSim from "../core/GameSim.ts";
import { processTeam } from "../core/game/loadTeams.ts";
import createRandomPlayers from "../core/league/create/createRandomPlayers.ts";
import { DEFAULT_LEVEL } from "../../common/budgetLevels.ts";
import { computeRapm, type RapmStint } from "./rapm.ts";
import { decodeShifts, encodeShifts } from "./gameShifts.ts";
import advStats from "./advStats.basketball.ts";
import addStatsRow from "../core/player/addStatsRow.ts";

// IS RAPM WORTH HAVING IN THIS GAME?
//
// rapm.test.ts proves the solver recovers impact from data the model itself
// generated. That is a necessary check and a weak one: the real question is
// whether a season of THIS sim's lineups - where minutes follow ability, so
// good players spend most of the year next to other good players - carries
// enough independent information to separate them.
//
// So this plays a real season with the real engine and asks how well the
// ratings line up with the ability the players were generated with. Raw
// plus-minus per 100 is measured beside it as the floor to beat: it uses the
// same evidence and makes no attempt to untangle who was on the floor.
//
//   SPORT=basketball RAPM_QUALITY=1 npx vitest --run \
//     src/worker/util/rapmQuality.test.ts
//
// Skipped unless RAPM_QUALITY is set, because a full season of game sim is far
// too slow for a normal test run.
const nodeEnv: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};

const NUM_TEAMS = 30;
const GAMES_PER_TEAM = Number(nodeEnv.RAPM_GAMES ?? 82);
const SEED = Number(nodeEnv.RAPM_SEED ?? 1);

const rngFromSeed = (seed: number): (() => number) => {
	let a = seed >>> 0;
	return () => {
		a += 0x6d2b79f5;
		let t = a;
		t = Math.imul(t ^ (t >>> 15), t | 1);
		t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
		return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
	};
};

const stubLeagueDb = () => {
	const store = {
		index: () => store,
		getAll: async () => [],
		get: async () => undefined,
		async *iterate() {},
	};
	(idb as any).league = {
		transaction: () => ({
			store,
			objectStore: () => store,
			done: Promise.resolve(),
		}),
		getAll: async () => [],
		get: async () => undefined,
	};
};

const correlation = (a: number[], b: number[]) => {
	const n = a.length;
	const meanA = a.reduce((sum, x) => sum + x, 0) / n;
	const meanB = b.reduce((sum, x) => sum + x, 0) / n;
	let cov = 0;
	let varA = 0;
	let varB = 0;
	for (let i = 0; i < n; i++) {
		const da = a[i]! - meanA;
		const db = b[i]! - meanB;
		cov += da * db;
		varA += da * da;
		varB += db * db;
	}
	return cov / Math.sqrt(varA * varB);
};

test("RAPM quality", { timeout: 3_600_000 }, async () => {
	if (nodeEnv.RAPM_QUALITY !== "1") {
		return;
	}

	const realRandom = Math.random;
	Math.random = rngFromSeed(SEED * 7919 + 13);
	try {
		await run();
	} finally {
		Math.random = realRandom;
	}
});

const run = async () => {
	resetG();
	g.setWithoutSavingToDB("numActiveTeams", NUM_TEAMS);
	g.setWithoutSavingToDB("numTeams", NUM_TEAMS);
	g.setWithoutSavingToDB("userTids", []);
	g.setWithoutSavingToDB("userTid", 0);
	g.setWithoutSavingToDB("realisticFaces", false);
	g.setWithoutSavingToDB("phase", PHASE.REGULAR_SEASON);

	const teams: any[] = [];
	for (let tid = 0; tid < NUM_TEAMS; tid++) {
		teams.push(
			team.generate({
				tid,
				cid: tid % 2,
				did: tid % 2,
				region: `Region${tid}`,
				name: `Name${tid}`,
				abbrev: `T${tid}`,
				pop: 18 ** (tid / Math.max(1, NUM_TEAMS - 1)),
				imgURL: "",
			} as any),
		);
	}

	const players = await createRandomPlayers({
		activeTids: teams.map((t) => t.tid),
		onlyFreeAgents: false,
		scoutingLevel: DEFAULT_LEVEL,
		teams,
	});
	await resetCache({ players, teams, draftPicks: [] });
	stubLeagueDb();
	for (let tid = 0; tid < NUM_TEAMS; tid++) {
		const t = (await idb.cache.teams.get(tid))!;
		await idb.cache.teamSeasons.add(team.genSeasonRow(t) as any);
	}

	// valueNoPot drives the rotation; freshly generated players have none.
	for (const p of await idb.cache.players.indexGetAll("playersByTid", [
		0,
		Infinity,
	])) {
		await player.updateValues(p);
		addStatsRow(p, g.get("season"), false);
		await idb.cache.players.put(p);
	}

	// The ability the season is graded against. Taken before a game is played,
	// so nothing that happens can leak into it.
	const truth = new Map<number, number>();
	for (const p of await idb.cache.players.indexGetAll("playersByTid", [
		0,
		Infinity,
	])) {
		truth.set(p.pid, p.ratings.at(-1)!.ovr);
	}

	const season = g.get("season");
	const rng = rngFromSeed(SEED);

	const loadSide = async (tid: number) => {
		const [t, teamSeason, ps] = await Promise.all([
			idb.cache.teams.get(tid),
			idb.cache.teamSeasons.indexGet("teamSeasonsBySeasonTid", [season, tid]),
			idb.getCopies.players({ tid }, "noCopyCache"),
		]);
		return processTeam(t!, teamSeason!, ps);
	};

	const stints: RapmStint[] = [];
	// Raw on-court plus-minus and possessions, for the floor comparison.
	const raw = new Map<string, { pm: number; poss: number }>();
	const key = (pid: number, tid: number) => `${pid}|${tid}`;

	let shiftBytes = 0;
	let boxBytes = 0;
	let gid = 1;
	const nextPlayDay = new Map<number, number>();
	let day = 0;
	const target = (NUM_TEAMS * GAMES_PER_TEAM) / 2;

	while (gid <= target) {
		day += 1;

		for (const p of await idb.cache.players.indexGetAll("playersByTid", [
			0,
			Infinity,
		])) {
			if (p.injury.gamesRemaining > 0) {
				p.injury.gamesRemaining -= 1;
				if (p.injury.gamesRemaining <= 0) {
					p.injury = { type: "Healthy", gamesRemaining: 0 };
				}
				await idb.cache.players.put(p);
			}
		}

		const tids = Array.from({ length: NUM_TEAMS }, (_, i) => i).filter(
			(tid) => (nextPlayDay.get(tid) ?? 1) <= day,
		);
		if (tids.length % 2 === 1) {
			tids.pop();
		}
		for (let i = tids.length - 1; i > 0; i--) {
			const j = Math.floor(rng() * (i + 1));
			[tids[i], tids[j]] = [tids[j]!, tids[i]!];
		}

		for (let i = 0; i + 1 < tids.length; i += 2) {
			const home = await loadSide(tids[i]!);
			const away = await loadSide(tids[i + 1]!);

			const result: any = new GameSim({
				gid,
				day,
				teams: helpers.deepCopy([home, away]) as any,
				doPlayByPlay: false,
				homeCourtFactor: 1,
				neutralSite: false,
				allStarGame: false,
				baseInjuryRate: g.get("injuryRate"),
			} as any).run();

			const gameTids = [result.team[0].id, result.team[1].id] as const;

			// Through the same packing the game uses, so the encoding is under
			// test alongside the regression.
			const encoded = encodeShifts(result.shifts, result.numPlayersOnCourt);
			shiftBytes += JSON.stringify(encoded).length;
			boxBytes += JSON.stringify(
				result.team.map((t: any) => ({
					tid: t.id,
					players: t.player.map((sp: any) => ({
						pid: sp.id,
						name: sp.name,
						...sp.stat,
					})),
				})),
			).length;
			for (const shift of decodeShifts({
				shifts: encoded,
				numPlayersOnCourt: result.numPlayersOnCourt,
			})) {
				const lineups = [
					shift.lineups[0].map((pid) => key(pid, gameTids[0])),
					shift.lineups[1].map((pid) => key(pid, gameTids[1])),
				] as const;

				for (const o of [0, 1] as const) {
					if (shift.poss[o] > 0) {
						stints.push({
							off: lineups[o],
							def: lineups[o === 0 ? 1 : 0],
							poss: shift.poss[o],
							pts: shift.pts[o],
						});
					}
				}

				const margin = shift.pts[0] - shift.pts[1];
				const poss = shift.poss[0] + shift.poss[1];
				for (const t of [0, 1] as const) {
					for (const k of lineups[t]) {
						const row = raw.get(k) ?? { pm: 0, poss: 0 };
						row.pm += t === 0 ? margin : -margin;
						row.poss += poss;
						raw.set(k, row);
					}
				}
			}

			// The box score, accumulated exactly as the game accumulates it, so
			// PER and BPM can be computed on this season and compared.
			for (const t1 of [0, 1] as const) {
				const t2 = t1 === 0 ? 1 : 0;
				let ts = (await idb.cache.teamStats.indexGet("teamStatsByPlayoffsTid", [
					false,
					result.team[t1].id,
				])) as any;
				if (!ts) {
					ts = team.genStatsRow(result.team[t1].id, false) as any;
					await idb.cache.teamStats.add(ts);
				}
				ts.gp = (ts.gp ?? 0) + 1;
				for (const [k, v] of Object.entries(result.team[t1].stat)) {
					if (k === "ptsQtrs" || typeof v !== "number") {
						continue;
					}
					ts[k] = (ts[k] ?? 0) + v;
					if (k !== "min") {
						const oppKey = `opp${k[0]!.toUpperCase()}${k.slice(1)}`;
						const oppValue = result.team[t2].stat[k];
						if (typeof oppValue === "number") {
							ts[oppKey] = (ts[oppKey] ?? 0) + oppValue;
						}
					}
				}
				await idb.cache.teamStats.put(ts);

				for (const sp of result.team[t1].player) {
					const p = await idb.cache.players.get(sp.id);
					if (!p) {
						continue;
					}
					const ps = p.stats.at(-1)! as any;
					for (const [k, v] of Object.entries(sp.stat)) {
						if (typeof v !== "number") {
							continue;
						}
						ps[k] = (ps[k] ?? 0) + v;
					}
					await idb.cache.players.put(p);
				}
			}

			for (const j of [0, 1] as const) {
				for (const sp of result.team[j].player) {
					if (sp.newInjury) {
						const p = await idb.cache.players.get(sp.id);
						if (p && p.injury.gamesRemaining === 0) {
							p.injury = player.injury(DEFAULT_LEVEL);
							await idb.cache.players.put(p);
						}
					}
				}
			}

			for (const tid of gameTids) {
				const r = rng();
				nextPlayDay.set(tid, day + (r < 0.2 ? 1 : r < 0.85 ? 2 : 3));
			}

			gid += 1;
		}
	}

	// Sanity: the shifts have to add up to the games they came from, or every
	// number after this is measuring the wrong thing.
	{
		const totalPoss = stints.reduce((sum, s) => sum + s.poss, 0);
		const totalPts = stints.reduce((sum, s) => sum + s.pts, 0);
		const games = gid - 1;
		console.log(
			`shift bytes/game=${(shiftBytes / games).toFixed(0)} box bytes/game=${(
				boxBytes / games
			).toFixed(0)}`,
		);
		console.log(
			`poss/game=${(totalPoss / games).toFixed(1)} pts/game=${(
				totalPts / games
			).toFixed(1)} pts/100=${((100 * totalPts) / totalPoss).toFixed(1)}`,
		);
	}

	const started = Date.now();
	const fit = computeRapm(stints)!;
	const elapsed = Date.now() - started;

	assert.isDefined(fit);

	const rated = [...fit.ratings.entries()];
	const ovr = rated.map(([k]) => truth.get(Number(k.split("|")[0]))!);
	const rapm = rated.map(([, r]) => r.off + r.def);
	const pm100 = rated.map(([k]) => {
		const row = raw.get(k)!;
		return (100 * row.pm) / row.poss;
	});

	// The box-score family, computed by the game's own code on the same season.
	await advStats();
	const box = new Map<string, { bpm: number; per: number }>();
	for (const p of await idb.cache.players.indexGetAll("playersByTid", [
		0,
		Infinity,
	])) {
		const ps = p.stats.at(-1) as any;
		if (ps) {
			box.set(key(p.pid, ps.tid), {
				bpm: (ps.obpm ?? 0) + (ps.dbpm ?? 0),
				per: ps.per ?? 0,
			});
		}
	}

	const rapmR = correlation(ovr, rapm);
	const pmR = correlation(ovr, pm100);
	const bpmR = correlation(
		ovr,
		rated.map(([k]) => box.get(k)?.bpm ?? 0),
	);
	const perR = correlation(
		ovr,
		rated.map(([k]) => box.get(k)?.per ?? 0),
	);

	console.log(
		[
			`games=${gid - 1} stints=${stints.length} rated=${rated.length}`,
			`lambda=${fit.lambda} solveMs=${elapsed}`,
			`r(ovr, RAPM)=${rapmR.toFixed(3)}`,
			`r(ovr, raw +/- per 100)=${pmR.toFixed(3)}`,
			`r(ovr, BPM)=${bpmR.toFixed(3)}`,
			`r(ovr, PER)=${perR.toFixed(3)}`,
			`spread: sd(RAPM)=${Math.sqrt(
				rapm.reduce((s, x) => s + x * x, 0) / rapm.length,
			).toFixed(2)}`,
		].join("\n"),
	);

	// Over a short run the possessions are not there for anything to separate,
	// so the comparison only means something at full length.
	if (GAMES_PER_TEAM >= 82) {
		assert.isAbove(rapmR, pmR);
	}
	assert.isAbove(rated.length, 300);
};
