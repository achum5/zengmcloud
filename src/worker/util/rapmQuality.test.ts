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
import { computeRapm, type RapmFit, type RapmStint } from "./rapm.ts";
import { decodeShifts, encodeShifts } from "./gameShifts.ts";
import advStats from "./advStats.basketball.ts";
import addStatsRow from "../core/player/addStatsRow.ts";
import addRatingsRow from "../core/player/addRatingsRow.ts";

// IS RAPM WORTH HAVING IN THIS GAME?
//
// rapm.test.ts proves the solver recovers impact from data the model itself
// generated. That is a necessary check and a weak one: the real question is
// whether a season of THIS sim's lineups - where minutes follow ability, so
// good players spend most of the year next to other good players - carries
// enough independent information to separate them.
//
// So this plays real seasons with the real engine and asks how well the
// ratings line up with the ability the players were generated with. Three
// things are measured beside it:
//
//   - Raw plus-minus per 100, the floor. Same evidence, no attempt to
//     untangle who else was out there.
//   - PER and BPM, computed by the game's own code on the same season. The
//     box-score family this is supposed to add something to.
//   - The same regression run over a multi-season window, which is how real
//     RAPM is almost always quoted, and the obvious thing to try when one
//     season turns out to be too few possessions.
//
//   SPORT=basketball RAPM_QUALITY=1 RAPM_SEASONS=3 npx vitest --run \
//     src/worker/util/rapmQuality.test.ts
//
// Skipped unless RAPM_QUALITY is set, because a season of game sim is far too
// slow for a normal test run.
const nodeEnv: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};

const NUM_TEAMS = 30;
const GAMES_PER_TEAM = Number(nodeEnv.RAPM_GAMES ?? 82);
const SEED = Number(nodeEnv.RAPM_SEED ?? 1);
// Seasons to play. More than one lets a multi-year window be measured against
// the single season, which is the whole question about it.
const NUM_SEASONS = Number(nodeEnv.RAPM_SEASONS ?? 1);
const PRIOR_WEIGHT = Number(nodeEnv.RAPM_PRIOR_WEIGHT ?? 1);

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

const key = (pid: number, tid: number) => `${pid}|${tid}`;

const activePlayers = () =>
	idb.cache.players.indexGetAll("playersByTid", [0, Infinity]);

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

type SeasonResult = {
	stints: RapmStint[];
	// On-court plus-minus and possessions, for the floor comparison.
	raw: Map<string, { pm: number; poss: number }>;
	shiftBytes: number;
	boxBytes: number;
	games: number;
	poss: number;
	pts: number;
};

const playSeason = async (
	rng: () => number,
	collectBox: boolean,
): Promise<SeasonResult> => {
	const season = g.get("season");

	const loadSide = async (tid: number) => {
		const [t, teamSeason, ps] = await Promise.all([
			idb.cache.teams.get(tid),
			idb.cache.teamSeasons.indexGet("teamSeasonsBySeasonTid", [season, tid]),
			idb.getCopies.players({ tid }, "noCopyCache"),
		]);
		return processTeam(t!, teamSeason!, ps);
	};

	const out: SeasonResult = {
		stints: [],
		raw: new Map(),
		shiftBytes: 0,
		boxBytes: 0,
		games: 0,
		poss: 0,
		pts: 0,
	};

	const nextPlayDay = new Map<number, number>();
	const target = (NUM_TEAMS * GAMES_PER_TEAM) / 2;
	let gid = 1;
	let day = 0;

	while (gid <= target) {
		day += 1;

		for (const p of await activePlayers()) {
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
			out.shiftBytes += JSON.stringify(encoded).length;
			out.boxBytes += JSON.stringify(
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
						out.stints.push({
							off: lineups[o],
							def: lineups[o === 0 ? 1 : 0],
							poss: shift.poss[o],
							pts: shift.pts[o],
						});
						out.poss += shift.poss[o];
						out.pts += shift.pts[o];
					}
				}

				const margin = shift.pts[0] - shift.pts[1];
				const poss = shift.poss[0] + shift.poss[1];
				for (const t of [0, 1] as const) {
					for (const k of lineups[t]) {
						const row = out.raw.get(k) ?? { pm: 0, poss: 0 };
						row.pm += t === 0 ? margin : -margin;
						row.poss += poss;
						out.raw.set(k, row);
					}
				}
			}

			// The box score, accumulated exactly as the game accumulates it, so
			// PER and BPM can be computed on this season and compared.
			if (collectBox) {
				for (const t1 of [0, 1] as const) {
					const t2 = t1 === 0 ? 1 : 0;
					let ts = (await idb.cache.teamStats.indexGet(
						"teamStatsByPlayoffsTid",
						[false, result.team[t1].id],
					)) as any;
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
							if (typeof v === "number") {
								ps[k] = (ps[k] ?? 0) + v;
							}
						}
						await idb.cache.players.put(p);
					}
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

	out.games = gid - 1;
	return out;
};

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

	const addTeamSeasons = async () => {
		for (let tid = 0; tid < NUM_TEAMS; tid++) {
			const t = (await idb.cache.teams.get(tid))!;
			await idb.cache.teamSeasons.add(team.genSeasonRow(t) as any);
		}
	};
	await addTeamSeasons();

	// valueNoPot drives the rotation; freshly generated players have none.
	for (const p of await activePlayers()) {
		await player.updateValues(p);
		await idb.cache.players.put(p);
	}

	const rng = rngFromSeed(SEED);
	const seasons: SeasonResult[] = [];
	const perSeason: RapmFit[] = [];
	let chained: RapmFit | undefined;
	// The ability each season is graded against, per season, taken before a
	// game of it is played so nothing that happens can leak in.
	const truthBySeason: Map<number, number>[] = [];

	for (let i = 0; i < NUM_SEASONS; i++) {
		const last = i === NUM_SEASONS - 1;

		if (i > 0) {
			// A year passes: everybody ages, develops, and is re-valued, so the
			// multi-year window is averaging over players who actually changed.
			g.setWithoutSavingToDB("season", g.get("season") + 1);
			for (const p of await activePlayers()) {
				addRatingsRow(p, DEFAULT_LEVEL);
				await player.develop(p, 1);
				await player.updateValues(p);
				p.injury = { type: "Healthy", gamesRemaining: 0 };
				await idb.cache.players.put(p);
			}
			await addTeamSeasons();
		}

		const truth = new Map<number, number>();
		for (const p of await activePlayers()) {
			truth.set(p.pid, p.ratings.at(-1)!.ovr);
			if (last) {
				// One clean stats row for the season the box-score stats are
				// computed on.
				p.stats = [];
				addStatsRow(p, g.get("season"), false);
				await idb.cache.players.put(p);
			}
		}
		truthBySeason.push(truth);

		seasons.push(await playSeason(rng, last));

		// Each season's fit, and the same season fitted again shrunk toward the
		// season before it. The chained one is what the game could compute with
		// no extra reads at all, so it is the one to beat.
		const plain = computeRapm(seasons.at(-1)!.stints)!;
		chained = computeRapm(seasons.at(-1)!.stints, {
			prior: chained?.ratings,
			priorWeight: PRIOR_WEIGHT,
		})!;
		perSeason.push(plain);
	}

	const finalSeason = seasons.at(-1)!;
	const truth = truthBySeason.at(-1)!;

	// Sanity: the shifts have to add up to the games they came from, or every
	// number after this is measuring the wrong thing.
	console.log(
		[
			`seasons=${NUM_SEASONS} games/season=${finalSeason.games}`,
			`poss/game=${(finalSeason.poss / finalSeason.games).toFixed(1)}`,
			`pts/100=${((100 * finalSeason.pts) / finalSeason.poss).toFixed(1)}`,
			`shift bytes/game=${(finalSeason.shiftBytes / finalSeason.games).toFixed(
				0,
			)}`,
			`box bytes/game=${(finalSeason.boxBytes / finalSeason.games).toFixed(0)}`,
		].join(" "),
	);

	const oneYear = perSeason.at(-1)!;
	assert.isDefined(oneYear);

	const started = Date.now();
	const window =
		NUM_SEASONS > 1
			? computeRapm(seasons.flatMap((season) => season.stints))
			: undefined;
	const windowMs = Date.now() - started;

	// The box-score family, computed by the game's own code on the same season.
	await advStats();
	const box = new Map<string, { bpm: number; per: number }>();
	for (const p of await activePlayers()) {
		const ps = p.stats.at(-1) as any;
		if (ps) {
			box.set(key(p.pid, ps.tid), {
				bpm: (ps.obpm ?? 0) + (ps.dbpm ?? 0),
				per: ps.per ?? 0,
			});
		}
	}

	// Everybody the one-season fit rated, so every column is scored on the same
	// set of players.
	const rated = [...oneYear.ratings.keys()].filter(
		(k) =>
			(window?.ratings.has(k) ?? true) && (chained?.ratings.has(k) ?? true),
	);
	const ovr = rated.map((k) => truth.get(Number(k.split("|")[0]))!);
	const against = (values: number[]) => correlation(ovr, values).toFixed(3);

	const lines = [
		`rated=${rated.length} lambda=${oneYear.lambda}`,
		`r(ovr, RAPM 1yr)=${against(
			rated.map((k) => {
				const r = oneYear.ratings.get(k)!;
				return r.off + r.def;
			}),
		)}`,
		`r(ovr, raw +/- per 100)=${against(
			rated.map((k) => {
				const row = finalSeason.raw.get(k)!;
				return (100 * row.pm) / row.poss;
			}),
		)}`,
		`r(ovr, BPM)=${against(rated.map((k) => box.get(k)?.bpm ?? 0))}`,
		`r(ovr, PER)=${against(rated.map((k) => box.get(k)?.per ?? 0))}`,
	];

	if (NUM_SEASONS > 1 && chained) {
		lines.push(
			`r(ovr, RAPM 1yr shrunk toward last season, weight ${PRIOR_WEIGHT})=${against(
				rated.map((k) => {
					const r = chained!.ratings.get(k)!;
					return r.off + r.def;
				}),
			)}`,
		);
	}

	if (window) {
		lines.push(
			`r(ovr, RAPM ${NUM_SEASONS}yr)=${against(
				rated.map((k) => {
					const r = window.ratings.get(k)!;
					return r.off + r.def;
				}),
			)} lambda=${window.lambda} solveMs=${windowMs}`,
		);
	}

	console.log(lines.join("\n"));

	assert.isAbove(rated.length, 250);
};
