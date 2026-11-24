import { test } from "vitest";
import { PHASE, PLAYER } from "../../../common/constants.ts";
import { makeSeededRandom } from "../../../common/random.ts";
import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import { legacyAwardPids } from "../../util/legacyAwards.ts";
import {
	advanceSeasons,
	createArenaLeague,
} from "../../../test/arena/runLeague.ts";
import { game, phase, player, season, team } from "../index.ts";
import getSchedule from "../season/getSchedule.ts";
import { injuryTicksUntil } from "../game/calendarTicks.ts";
import { getInjuryClock } from "../game/classicDays.ts";
import { buildGameLinePricer } from "./gameLines.ts";
import { getLines } from "./getLines.ts";
import getAwardRaceOdds, { playerTalent } from "../season/getAwardRaceOdds.ts";

// THE WHOLE BOOK AGAINST WHAT HAPPENED. Random leagues on the owner's league
// settings, played day by day by the real sim: every game's line as the book
// quoted it on the day it was played (and the days before), every futures and
// award board at points through the season and the playoffs, and what came
// of all of it. Never part of the normal run.
//
//   LINES_AUDIT_OUT=/tmp/audit.jsonl LINES_AUDIT_SEEDS=1,2,3 \
//     npx vitest run --project basketball src/worker/core/sportsbook/linesAudit.test.ts
//
// (ARENA_LEAGUE='{"nbaCalendar":true}' for the calendar.) Scored by
// tools/sportsbookAudit.py.

const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);
const conditions = {} as any;

// How many days ahead each game is quoted before it is played.
const AHEAD = Number(env.LINES_AUDIT_AHEAD ?? 3);

const teamRows = async () =>
	team.getFormattedAll({
		attrs: ["tid", "cid", "did", "disabled", "playThroughInjuries"],
		seasonAttrs: ["won", "lost", "tied", "otl"],
		stats: ["pts", "oppPts", "gp"],
		season: g.get("season"),
		showNoStats: true,
	});

const priceUpcoming = async () => {
	const schedule = await getSchedule();
	if (schedule.length === 0) {
		return [];
	}
	const teams = (await teamRows()).filter((t) => !t.disabled);
	const todayDay = schedule[0]!.day;
	const ticksUntil = injuryTicksUntil(schedule, await getInjuryClock());
	const pricer = await buildGameLinePricer({
		activeTeams: teams as any,
		season: g.get("season"),
		todayDay,
		ticksUntil,
	});
	const byTid = new Map(teams.map((t) => [t.tid, t]));
	const out = [];
	for (const m of schedule) {
		if (m.homeTid < 0 || m.awayTid < 0) {
			continue;
		}
		if (m.day > todayDay + AHEAD) {
			break;
		}
		const line = pricer.priceGame(m);
		if (!line) {
			continue;
		}
		const s = (tid: number) => {
			const st = byTid.get(tid)?.stats as any;
			return [st?.gp ?? 0, st?.pts ?? 0, st?.oppPts ?? 0];
		};
		out.push({
			gid: m.gid,
			day: m.day,
			ahead: m.day - todayDay,
			h: m.homeTid,
			a: m.awayTid,
			margin: line.margin,
			model: line.modelMargin,
			ml: [line.moneyline.home, line.moneyline.away],
			spread: line.spread.line,
			total: line.total.line,
			xTotal: line.expectedTotal,
			lgTotal: pricer.leagueAvgTotal,
			hs: s(m.homeTid),
			as: s(m.awayTid),
		});
	}
	return out;
};

const awardInputs = async () => {
	const out: Record<string, any> = {};
	try {
		for (const race of await getAwardRaceOdds(g.get("season"))) {
			if (race.group !== undefined) {
				continue;
			}
			out[race.shortName] = race.players.map((p) => [
				p.pid,
				p.awardScore,
				playerTalent(p),
				p.injury?.gamesRemaining ?? 0,
			]);
		}
	} catch {}
	const pool = await player.formatAll(
		await idb.cache.players.indexGetAll("playersByTid", [
			PLAYER.FREE_AGENT,
			Infinity,
		]),
		{
			attrs: ["pid", "tid", "injury"],
			ratings: ["ovr"],
			stats: ["ewa", "ws", "gp"],
			season: g.get("season"),
			mergeStats: "totOnly",
			showNoStats: true,
			coarsenRatings: false,
		},
	);
	out.allStarPool = pool
		.map((p: any) => [
			p.pid,
			p.stats?.ewa ?? 0,
			p.stats?.ws ?? 0,
			p.ratings.ovr,
			p.injury.gamesRemaining,
			p.stats?.gp ?? 0,
		])
		.sort(
			(a: any, b: any) =>
				2.5 * b[1] + b[2] - (2.5 * a[1] + a[2]) || b[3] - a[3],
		)
		.slice(0, 80);
	return out;
};

const board = async (tag: string) => {
	const lines = await getLines();
	const ps = await idb.cache.playoffSeries.get(g.get("season"));
	const teams = await teamRows();
	return {
		tag,
		phase: g.get("phase"),
		round: ps?.currentRound,
		teams: teams.map((t) => ({
			tid: t.tid,
			cid: t.cid,
			did: t.did,
			won: t.seasonAttrs.won,
			lost: t.seasonAttrs.lost,
		})),
		title: lines.championship.map((r) => [r.tid, r.americanOdds]),
		conf: lines.conferences.map((c) => [
			c.cid,
			c.teams.map((r) => [r.tid, r.americanOdds]),
		]),
		div: lines.divisions.map((d) => [
			d.did,
			d.teams.map((r) => [r.tid, r.americanOdds]),
		]),
		wins: lines.winTotals.map((r) => [r.tid, r.line, r.over, r.under]),
		awards: lines.awards.map((a) => [
			a.award,
			a.candidates.map((c) => [c.pid, c.americanOdds]),
		]),
		allLeague: lines.allLeague.map((t) => [
			t.tier,
			t.candidates.map((c) => [c.pid, c.americanOdds]),
		]),
		allDefensive: lines.allDefensive.map((t) => [
			t.tier,
			t.candidates.map((c) => [c.pid, c.americanOdds]),
		]),
		allRookie: lines.allRookie.map((c) => [c.pid, c.americanOdds]),
		allStar: lines.allStar.map((c) => [c.pid, c.americanOdds]),
		inputs: await awardInputs(),
	};
};

test.skipIf(!env.LINES_AUDIT_OUT)(
	"sportsbook audit: record",
	{ timeout: 86_400_000 },
	async () => {
		const fs = await importNode("node:fs");
		await importNode("fake-indexeddb/auto");
		for (const seed of (env.LINES_AUDIT_SEEDS ?? "1").split(",").map(Number)) {
			const realRandom = Math.random;
			Math.random = makeSeededRandom(seed);
			(idb as any).league = undefined;
			await createArenaLeague(30_000 + seed);
			// Seasons played first, so the one recorded has a history behind it
			// (last season's stats, a draft and free agency) as a real league does.
			const warmup = Number(env.LINES_AUDIT_WARMUP ?? 1);
			if (warmup > 0) {
				if (g.get("phase") !== PHASE.PRESEASON) {
					await phase.newPhase(PHASE.PRESEASON, conditions);
				}
				await advanceSeasons(warmup);
			}
			const boards = [];
			if (g.get("phase") < PHASE.REGULAR_SEASON) {
				boards.push(await board("preseason"));
			}
			await phase.newPhase(PHASE.REGULAR_SEASON, conditions);
			boards.push(await board("pre"));
			const total = await season.getDaysLeftSchedule();
			const marks = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9].map((f) =>
				Math.round(total * (1 - f)),
			);
			const quotes: any[] = [];
			const results: Record<number, [number, number, number]> = {};
			const seenRound = new Set<number>();
			let allStarBoardTaken = false;
			while (g.get("phase") <= PHASE.PLAYOFFS) {
				if (g.get("phase") < PHASE.PLAYOFFS) {
					const left = await season.getDaysLeftSchedule();
					if (left === 0) {
						await phase.newPhase(PHASE.PLAYOFFS, conditions);
						continue;
					}
					while (marks.length > 0 && left <= marks[0]!) {
						const m = marks.shift()!;
						boards.push(await board(`r${(1 - m / total).toFixed(1)}`));
					}
					if (
						!allStarBoardTaken &&
						(await idb.merged.allStars.get(g.get("season"))) === undefined
					) {
						// The last board before the All-Star roster is picked.
						const next = (await getSchedule(true))[0];
						if (next && next.homeTid < 0) {
							boards.push(await board("allStarEve"));
							allStarBoardTaken = true;
						}
					}
				} else {
					const ps = await idb.cache.playoffSeries.get(g.get("season"));
					const rnd = ps?.currentRound ?? 0;
					if (!seenRound.has(rnd)) {
						seenRound.add(rnd);
						boards.push(await board(`po${rnd}`));
					}
				}
				for (const q of await priceUpcoming()) {
					quotes.push({ ...q, po: g.get("phase") === PHASE.PLAYOFFS ? 1 : 0 });
				}
				const today = (await getSchedule(true)).map((m) => m.gid);
				await game.play(1, conditions);
				for (const gid of today) {
					const gm = await idb.cache.games.get(gid);
					if (gm) {
						results[gid] = [
							gm.teams[0].pts,
							gm.teams[1].pts,
							gm.overtimes ?? 0,
						];
					}
				}
			}
			const ts = await team.getFormattedAll({
				attrs: ["tid", "cid", "did"],
				seasonAttrs: ["won", "winp", "playoffRoundsWon"],
				season: g.get("season"),
			});
			const awards = await idb.merged.awards.get(g.get("season"));
			const legacy = awards ? legacyAwardPids(awards) : undefined;
			const allStars = await idb.merged.allStars.get(g.get("season"));
			fs.appendFileSync(
				env.LINES_AUDIT_OUT,
				JSON.stringify({
					seed,
					calendar: !!g.get("nbaCalendar"),
					numGames: g.get("numGames"),
					rounds: g.get("numGamesPlayoffSeries").length,
					boards,
					quotes,
					results,
					outcome: ts.map((t) => ({
						tid: t.tid,
						cid: t.cid,
						did: t.did,
						won: t.seasonAttrs.won,
						winp: t.seasonAttrs.winp,
						prw: t.seasonAttrs.playoffRoundsWon,
					})),
					awards: legacy
						? {
								individual: Object.fromEntries(
									Object.entries(legacy.individual).map(([k, v]) => [
										k,
										(v as any)?.pid,
									]),
								),
								allLeague: legacy.allLeague.map((t: any[]) =>
									t.map((p) => p.pid),
								),
								allDefensive: legacy.allDefensive.map((t: any[]) =>
									t.map((p) => p.pid),
								),
								allRookie: legacy.allRookie.map((p: any) => p.pid),
							}
						: undefined,
					allStars: allStars
						? [
								...allStars.teams[0],
								...allStars.teams[1],
								...allStars.remaining,
							].map((p: any) => p.pid)
						: undefined,
				}) + "\n",
			);
			Math.random = realRandom;
		}
	},
);
