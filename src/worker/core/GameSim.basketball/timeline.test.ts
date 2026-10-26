import { afterAll, assert, beforeAll, describe, test } from "vitest";
import { resetCache, resetG } from "../../../test/helpers.ts";
import { idb } from "../../db/index.ts";
import { g, helpers } from "../../util/index.ts";
import { PHASE } from "../../../common/constants.ts";
import { player, team } from "../index.ts";
import GameSim from "../GameSim.ts";
import { processTeam } from "../game/loadTeams.ts";
import createRandomPlayers from "../league/create/createRandomPlayers.ts";
import { DEFAULT_LEVEL } from "../../../common/budgetLevels.ts";
import { TimelineLog } from "./timelineLog.ts";
import { encodeShifts } from "../../util/gameShifts.ts";
import {
	decodeScoring,
	decodeStints,
	encodeShiftSecs,
	encodeTimeline,
	getBoxScoreFlow,
	seasonLineups,
} from "../../util/gameTimeline.ts";
const seededRandom = () => {
	let a = 0x1234567;
	return () => {
		a |= 0;
		a = (a + 0x6d2b79f5) | 0;
		let t = Math.imul(a ^ (a >>> 15), 1 | a);
		t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
		return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
	};
};
const realRandom = Math.random;

let result: any;

afterAll(() => {
	Math.random = realRandom;
});

beforeAll(async () => {
	Math.random = seededRandom();
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
				region: `Region${tid}`,
				name: `Name${tid}`,
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
		const teamSeason = await idb.cache.teamSeasons.indexGet(
			"teamSeasonsBySeasonTid",
			[g.get("season"), tid],
		);
		const ps = await idb.cache.players.indexGetAll("playersByTid", tid);
		return processTeam(t!, teamSeason!, ps);
	};
	const sides = [await load(0), await load(1)];
	result = new GameSim({
		gid: 1,
		day: 1,
		teams: helpers.deepCopy(sides) as any,
		doPlayByPlay: false,
		homeCourtFactor: 1,
		neutralSite: false,
		allStarGame: false,
		baseInjuryRate: 0,
	} as any).run();
}, 60_000);

// WHEN THINGS HAPPENED, AS THE BOX SCORE DRAWS IT: who was on the floor from
// when to when, every score and when, and how long each ten played together.
// It is all on the clock minutes are counted on, so it has to add up to the
// box score it sits under.

describe("TimelineLog", () => {
	test("on and off at the same dead ball is one stint, and the end closes what is open", () => {
		const log = new TimelineLog();
		log.setLineups([1, 2], 0);
		log.setLineups([2, 3], 100);
		log.setLineups([1, 2], 100);
		log.setLineups([2, 3], 250);
		const { stints } = log.finish(400);
		const of = (pid: number) => stints.find((s) => s.pid === pid)?.spans;
		assert.deepStrictEqual(of(1), [[0, 250]]);
		assert.deepStrictEqual(of(2), [[0, 400]]);
		assert.deepStrictEqual(of(3), [[250, 400]]);
	});

	test("a basket and its and-one are one score", () => {
		const log = new TimelineLog();
		log.addPoints(0, 2, 10);
		log.addPoints(0, 1, 10);
		log.addPoints(1, 3, 30);
		assert.deepStrictEqual(log.finish(40).scoring, [
			{ t: 10, side: 0, pts: 3 },
			{ t: 30, side: 1, pts: 3 },
		]);
	});
});

describe("a game, played", () => {
	test("each man's stints add up to his minutes", () => {
		const stints = new Map<number, [number, number][]>(
			result.timeline.stints.map((s: any) => [s.pid, s.spans]),
		);
		for (const t of result.team) {
			for (const p of t.player) {
				const sec = (stints.get(p.id) ?? []).reduce(
					(sum: number, [a, b]: [number, number]) => sum + b - a,
					0,
				);
				assert.approximately(sec, p.stat.min * 60, 1e-6, `pid ${p.id}`);
			}
		}
	});

	test("the scores add up to the final, and come in order", () => {
		const totals = [0, 0];
		let last = 0;
		for (const s of result.timeline.scoring) {
			totals[s.side] += s.pts;
			assert.isAtLeast(s.t, last);
			last = s.t;
		}
		assert.deepStrictEqual(totals, [
			result.team[0].stat.pts,
			result.team[1].stat.pts,
		]);
	});

	test("the periods end where the game's minutes do", () => {
		const { periodEnds } = result.timeline;
		assert.strictEqual(periodEnds.length, 4 + result.overtimes);
		const gameSec = (result.team[0].stat.min * 60) / result.numPlayersOnCourt;
		assert.approximately(periodEnds.at(-1), gameSec, 1e-6);
	});

	test("every ten's seconds add up to the game", () => {
		const sec = result.shifts.reduce(
			(sum: number, s: any) => sum + (s.sec ?? 0),
			0,
		);
		assert.approximately(sec, result.timeline.periodEnds.at(-1), 1e-6);
	});

	test("packed for the game row and unpacked for the box score, it still adds up", () => {
		const packed = encodeTimeline(result.timeline);
		const n = result.numPlayersOnCourt;
		const game = {
			...packed,
			shifts: encodeShifts(result.shifts, n),
			shiftSecs: encodeShiftSecs(result.shifts, n),
			numPlayersOnCourt: n,
			teams: result.team.map((t: any) => ({
				players: t.player.map((p: any) => ({ pid: p.id })),
			})),
		};
		assert.strictEqual(
			decodeStints(packed.stints).length,
			result.timeline.stints.filter((s: any) => s.spans.length > 0).length,
		);
		assert.strictEqual(
			decodeScoring(packed.scoring).length,
			result.timeline.scoring.length,
		);

		const flow = getBoxScoreFlow(game)!;
		assert.deepStrictEqual(flow.scores.at(-1)!.slice(1), [
			result.team[0].stat.pts,
			result.team[1].stat.pts,
		]);
		for (const side of [0, 1] as const) {
			// Each side's fives cover the game, and its points and the other
			// side's.
			const lines = flow.lineups![side];
			const sum = (key: "sec" | "pts" | "ptsOpp") =>
				lines.reduce((total, line) => total + line[key], 0);
			assert.approximately(sum("sec"), flow.total, lines.length);
			assert.strictEqual(sum("pts"), result.team[side].stat.pts);
			assert.strictEqual(sum("ptsOpp"), result.team[1 - side].stat.pts);
			// And a man's share of it is the time he was out there.
			for (const p of result.team[side].player) {
				const on = flow.onCourt![side].byPid[p.id];
				if (p.stat.min > 0) {
					assert.approximately(on!.sec, p.stat.min * 60, lines.length);
				}
			}
		}
	});
});

describe("seasonLineups", () => {
	test("a five's games, time and points add up across a team's games", () => {
		const n = result.numPlayersOnCourt;
		const packed = {
			shifts: encodeShifts(result.shifts, n),
			shiftSecs: encodeShiftSecs(result.shifts, n),
			numPlayersOnCourt: n,
		};
		const teams = result.team.map((t: any, i: number) => ({
			tid: i,
			players: t.player.map((p: any) => ({ pid: p.id, name: p.name })),
		}));
		// The same game twice, and one the team was not in.
		const games = [
			{ ...packed, teams },
			{ ...packed, teams },
			{
				...packed,
				teams: [
					{ ...teams[0], tid: 7 },
					{ ...teams[1], tid: 8 },
				],
			},
		];
		const once = getBoxScoreFlow({
			...encodeTimeline(result.timeline),
			...packed,
			teams,
		})!.lineups![1];
		const { lineups } = seasonLineups(games, 1);
		assert.strictEqual(lineups.length, once.length);
		for (const line of lineups) {
			const single = once.find((l) => l.pids.join() === line.pids.join())!;
			assert.strictEqual(line.gp, 2);
			assert.strictEqual(line.sec, 2 * single.sec);
			assert.strictEqual(line.pts, 2 * single.pts);
			assert.strictEqual(line.possOpp, 2 * single.possOpp);
		}
	});
});
