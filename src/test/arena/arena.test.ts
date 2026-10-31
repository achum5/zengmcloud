import { test } from "vitest";
import { makeSeededRandom } from "../../common/random.ts";
import { getTradeHistoryDump } from "../../worker/api/tradeHistoryDump.ts";
import { league } from "../../worker/core/index.ts";
import {
	bestDealWith,
	currentOutlook,
	deferWatchers,
	loadTeam,
	marketClock,
	priceDeal,
	marketSettings,
	outlookWatchers,
	teamMarketSettings,
	tradeWatchers,
	waitingOf,
	type TeamState,
} from "../../worker/core/realisticFrontOffice/market.ts";
import getDaysLeftSchedule from "../../worker/core/season/getDaysLeftSchedule.ts";
import { marginSettings } from "../../worker/core/realisticFrontOffice/margin.ts";
import {
	freeAgencySettings,
	freeAgencyWatchers,
} from "../../worker/core/realisticFrontOffice/freeAgency.ts";
import { offerSettings } from "../../worker/core/realisticFrontOffice/offers.ts";
import { draftSettings } from "../../worker/core/realisticFrontOffice/draft.ts";
import {
	curveAt,
	ensureRetention,
	OUTLOOK_YEARS,
	outlookSeason,
	outlookSettings,
	projectPlayer,
	retentionOf,
	teamOutlookSettings,
	seasonLine,
	teamPoints,
	teamStrengths,
} from "../../worker/core/realisticFrontOffice/outlook.ts";
import type { Player } from "../../common/types.ts";
import { last } from "../../common/utils.ts";
import { idb } from "../../worker/db/index.ts";
import { g, helpers } from "../../worker/util/index.ts";
import { PHASE } from "../../common/constants.ts";
import {
	playerLine,
	rosterPoints,
	strengthSettings,
	type ValueRatings,
	rosterParts,
} from "../../worker/core/realisticFrontOffice/strength.ts";
import { inGameOrder, play, toSimTeam } from "../impact/simTools.ts";
import {
	advanceSeasons,
	createArenaLeague,
	exportArenaLeague,
	importArenaLeague,
} from "./runLeague.ts";
import { computeMetrics, type LeagueMetrics } from "./metrics.ts";
import { BENCHMARKS, type Benchmark } from "./benchmarks.ts";

// THE ARENA: whole random-players leagues, every team run by the CPU, played
// season after season through the real game code, then measured against the
// NBA. It is how a front office gets judged, so it is never part of the normal
// test run. One league per process, so leagues run in parallel:
//
//   for seed in 1 2 3 4; do
//     ARENA_RUN=1 ARENA_SEED=$seed ARENA_SEASONS=10 ARENA_OUT=/tmp/arena \
//       npx vitest --run --project basketball src/test/arena/arena.test.ts &
//   done; wait
//   ARENA_REPORT=/tmp/arena npx vitest --run --project basketball src/test/arena/arena.test.ts
//
// The report lands in ARENA_REPORT/report.md. ARENA_FO=1 runs the league with
// the new front offices (docs/realistic-front-offices.md) instead of stock's,
// and ARENA_CHECK_TRADES=<games> then grades every trade they make against the
// sim: both teams before and after, healthy, played against the rest of the
// league on the same random numbers, next to what the model expected.

const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};

const importNode = (name: string) => import(/* @vite-ignore */ name);

test.runIf(env.ARENA_RUN === "1")(
	"arena: play a league",
	{ timeout: 6 * 60 * 60 * 1000 },
	async () => {
		const seed = Number(env.ARENA_SEED ?? 1);
		const numSeasons = Number(env.ARENA_SEASONS ?? 10);
		const out = env.ARENA_OUT ?? ".";
		// ARENA_TAG: what this run's files are named by (the seed by default) -
		// a part of a longer run, say.
		const tag = env.ARENA_TAG ?? String(seed);

		// Every random number in the league, from league creation on, comes from
		// the seed - so a league that looks wrong can be played again exactly.
		await importNode("fake-indexeddb/auto");
		const realRandom = Math.random;
		Math.random = makeSeededRandom(seed);
		try {
			const started = Date.now();
			// ARENA_PROFILE=<file>: a CPU profile of the run, for the speed work
			// (vitest's worker never writes one of its own).
			let profiler: any;
			if (env.ARENA_PROFILE) {
				const inspector = await importNode("node:inspector/promises");
				profiler = new inspector.Session();
				profiler.connect();
				await profiler.post("Profiler.enable");
				await profiler.post("Profiler.start");
			}
			await createArenaLeague(seed);
			// ARENA_IMPORT: carry on the league another run exported
			// (ARENA_EXPORT), so a long run can be played in parts.
			if (env.ARENA_IMPORT) {
				await importArenaLeague(env.ARENA_IMPORT);
			}
			const firstSeason = g.get("season");
			if (env.ARENA_FO === "1") {
				await league.setGameAttributes({
					realisticFrontOffices: true,
					// ARENA_STOCK_FA=1: free agency and re-signing by stock's rules.
					realisticFreeAgency: env.ARENA_STOCK_FA !== "1",
				});
				// ARENA_NO_WAIT=1: deals are made as soon as they are found.
				marketSettings.valueWaiting = env.ARENA_NO_WAIT !== "1";
				// ARENA_MARKET='{"oneSided":false}': any other market setting.
				Object.assign(marketSettings, JSON.parse(env.ARENA_MARKET ?? "{}"));
				// ARENA_SPLIT: these market settings for the odd teams only - a
				// league of two versions, to see which builds better
				// franchises (teams-<seed>.json).
				teamMarketSettings.clear();
				if (env.ARENA_SPLIT) {
					const split = JSON.parse(env.ARENA_SPLIT);
					for (let tid = 1; tid < g.get("numTeams"); tid += 2) {
						teamMarketSettings.set(tid, split);
					}
				}
				Object.assign(outlookSettings, JSON.parse(env.ARENA_OUTLOOK ?? "{}"));
				// ARENA_SPLIT_OUTLOOK: these outlook settings for the odd teams'
				// own pricing only (teamOutlookSettings), as ARENA_SPLIT.
				teamOutlookSettings.clear();
				if (env.ARENA_SPLIT_OUTLOOK) {
					const split = JSON.parse(env.ARENA_SPLIT_OUTLOOK);
					for (let tid = 1; tid < g.get("numTeams"); tid += 2) {
						teamOutlookSettings.set(tid, split);
					}
				}
				Object.assign(marginSettings, JSON.parse(env.ARENA_MARGIN ?? "{}"));
				Object.assign(
					freeAgencySettings,
					JSON.parse(env.ARENA_FREE_AGENCY ?? "{}"),
				);
				Object.assign(offerSettings, JSON.parse(env.ARENA_OFFERS ?? "{}"));
				Object.assign(draftSettings, JSON.parse(env.ARENA_DRAFT ?? "{}"));
				Object.assign(strengthSettings, JSON.parse(env.ARENA_STRENGTH ?? "{}"));
			}
			// How much this season's playoffs count at each outlook before the
			// deadline (outlook.playoffWeight).
			const deadlineViews: unknown[] = [];
			outlookWatchers.push((outlook) => {
				if (outlook.deadline) {
					deadlineViews.push({
						season: g.get("season"),
						days: outlook.deadline.daysToDeadline,
						playoffWeight: outlook.playoffWeight ?? null,
					});
				}
			});
			// ARENA_PICK_SURVEY=1: where every outlook expects each team's own
			// picks to land, draft by draft, to set against where they do.
			const pickSurvey: unknown[] = [];
			if (env.ARENA_PICK_SURVEY === "1") {
				outlookWatchers.push((outlook) => {
					pickSurvey.push({
						season: g.get("season"),
						phase: g.get("phase"),
						firstDraft: outlookSeason(outlook, 0),
						gamesLeft:
							[...outlook.remaining.values()].reduce((a, x) => a + x, 0) /
							Math.max(1, outlook.remaining.size),
						places: Object.fromEntries(outlook.draftPlace),
					});
				});
			}
			// ARENA_ROSTER_CHECK=1: for every player a CPU team trades away, each
			// season, whether the roster without him looks better than with him
			// by more than half a point - the lines of both, to reproduce.
			const rosterChecks: unknown[] = [];
			if (env.ARENA_ROSTER_CHECK === "1") {
				tradeWatchers.push((deal) => {
					const { outlook } = deal;
					for (const k of [0, 1] as const) {
						const tid = deal.tids[k];
						const players = deal.rosters[k];
						const capture = (list: Player[]) => {
							const lines: unknown[][] = [];
							teamPoints(
								outlook,
								tid,
								list,
								deal.picks[k],
								undefined,
								undefined,
								(y, seasonLines) => {
									lines[y] = seasonLines.map((x) => ({
										roleFree: x.roleFree,
										offense: x.offense,
										usage: x.usage,
										valueNoPot: x.valueNoPot,
										availability: x.availability,
									}));
								},
							);
							return lines;
						};
						const full = teamPoints(outlook, tid, players, deal.picks[k]);
						for (const pid of deal.give[k].pids) {
							const without = players.filter((p) => p.pid !== pid);
							const points = teamPoints(outlook, tid, without, deal.picks[k]);
							for (const [y, x] of points.entries()) {
								if (x - full[y]! > 0.5) {
									rosterChecks.push({
										season: g.get("season"),
										tid,
										pid,
										y,
										gain: x - full[y]!,
										with: capture(players)[y],
										without: capture(without)[y],
									});
								}
							}
						}
					}
				});
			}
			// ARENA_FA_SURVEY=1: each day of free agency (and of the season), the
			// pool best first and who signed whom - how fast free agents go.
			const faSurvey: unknown[] = [];
			if (env.ARENA_FA_SURVEY === "1") {
				freeAgencyWatchers.push((day) => {
					faSurvey.push({
						season: g.get("season"),
						phase: g.get("phase"),
						daysLeft: g.get("daysLeft"),
						...day,
					});
				});
			}
			const watched: WatchedTrade[] = [];
			// Every trade the front offices make: who moved, and where each team
			// stood (this season's strength, and its average over the next three).
			const tradeLog: unknown[] = [];
			const deferLog: unknown[] = [];
			if (env.ARENA_FO === "1") {
				tradeWatchers.push((deal) => {
					const standing = (tid: number) => {
						const o = deal.outlook.teams.get(tid);
						return o
							? {
									now: Math.round(o.strength[0]! * 10) / 10,
									next3:
										Math.round(
											((o.strength[1]! + o.strength[2]! + o.strength[3]!) / 3) *
												10,
										) / 10,
									title: Math.round(o.title[0]! * 1000) / 1000,
								}
							: undefined;
					};
					const row: Record<string, unknown> = {
						season: g.get("season"),
						phase: g.get("phase"),
						// Free agency's days left (g.daysLeft); in season see the
						// fields below.
						daysLeft: g.get("daysLeft"),
						// Each side's margin of error, and before the deadline what
						// waiting was worth to it (market.ts waitingOf).
						margins: deal.margins.map((x) => Math.round(x * 10000) / 10000),
						waiting: deal.waiting?.map(roundWaiting),
						// Each side's record and its title odds by the cross-team curve
						// the market prices with, next to standing.title (the season
						// sim's, record and all).
						curveTitle: deal.tids.map((tid) => {
							const o = deal.outlook.teams.get(tid);
							return o
								? Math.round(
										curveAt(deal.outlook.titleCurves[0]!, o.strength[0]!) *
											1000,
									) / 1000
								: undefined;
						}),
						// What each side's record is worth this season, in points
						// (outlookSettings.recordOdds).
						recordEdge: deal.tids.map(
							(tid) =>
								Math.round((deal.outlook.recordEdge.get(tid) ?? 0) * 100) / 100,
						),
						breakdown: deal.breakdown,
						tids: deal.tids,
						gains: deal.gains.map((x) => Math.round(x * 10000) / 10000),
						standing: deal.tids.map(standing),
						// Each side's payroll before the deal (players on the roster),
						// as a share of the cap.
						payroll: deal.rosters.map(
							(roster) =>
								Math.round(
									(1000 * roster.reduce((a, p) => a + p.contract.amount, 0)) /
										g.get("salaryCap"),
								) / 1000,
						),
						moved: ([0, 1] as const).flatMap((k) =>
							deal.rosters[k]
								.filter((p) => deal.give[k].pids.includes(p.pid))
								.map((p) => {
									const to = deal.tids[k === 0 ? 1 : 0];
									const { outlook } = deal;
									return {
										pid: p.pid,
										from: deal.tids[k],
										age: g.get("season") - p.born.year,
										ovr: last(p.ratings).ovr,
										exp: p.contract.exp,
										amount: p.contract.amount,
										// What the buyer expected of him, season by season:
										// overall, and his chance of still being there.
										projected: [1, 2, 3].map((y) => {
											const line = seasonLine(
												outlook.lines,
												p,
												outlook.season,
												outlook.offset,
												y,
												outlook.remaining.get(to) ?? 0,
												retentionOf(outlook, p.pid, to),
											);
											return {
												season: outlookSeason(outlook, y),
												ovr: Math.round(line.valueNoPot * 10) / 10,
												// The spread of his overall that season across
												// simulated careers: luck the model already expects.
												ovrSd:
													Math.round(
														projectPlayer(p, outlook.season).ovrSd[
															y + outlook.offset
														]! * 10,
													) / 10,
												availability:
													Math.round(line.availability * 1000) / 1000,
											};
										}),
									};
								}),
						),
						picks: [deal.give[0].dpids.length, deal.give[1].dpids.length],
					};
					tradeLog.push(row);
					// The schedule and records cannot change before this trade is
					// processed, so these land on the row as of the trade.
					if (g.get("phase") === PHASE.REGULAR_SEASON) {
						void stampSeasonClock(row, deal.tids);
					}
				});
				deferWatchers.push((deal) => {
					const row: Record<string, unknown> = {
						season: g.get("season"),
						tids: deal.tids,
						gains: deal.gains.map((x) => Math.round(x * 10000) / 10000),
						margins: deal.margins.map((x) => Math.round(x * 10000) / 10000),
						waiting: deal.waiting.map(roundWaiting),
					};
					deferLog.push(row);
					void stampSeasonClock(row, deal.tids);
				});
			}
			const suspicious: unknown[] = [];
			if (env.ARENA_DEBUG_GAINS !== undefined) {
				const threshold = Number(env.ARENA_DEBUG_GAINS);
				tradeWatchers.push((deal) => {
					if (Math.max(...deal.gains) < threshold) {
						return;
					}
					const { outlook } = deal;
					const sides = ([0, 1] as const).map((k) => {
						const j = k === 0 ? 1 : 0;
						const outP = new Set(deal.give[k].pids);
						const outD = new Set(deal.give[k].dpids);
						const players = deal.rosters[k];
						const picks = deal.picks[k];
						const after = [
							...players.filter((p) => !outP.has(p.pid)),
							...deal.rosters[j].filter((p) =>
								deal.give[j].pids.includes(p.pid),
							),
						];
						const afterPicks = [
							...picks.filter((dp) => !outD.has(dp.dpid)),
							...deal.picks[j].filter((dp) =>
								deal.give[j].dpids.includes(dp.dpid),
							),
						];
						const lines = (p: Player) =>
							Array.from({ length: OUTLOOK_YEARS + 1 }, (_, y) => {
								const line = seasonLine(
									outlook.lines,
									p,
									outlook.season,
									outlook.offset,
									y,
									outlook.remaining.get(deal.tids[k]) ?? 0,
									retentionOf(outlook, p.pid, deal.tids[k]),
								);
								return [
									Math.round(line.roleFree * 100) / 100,
									Math.round(line.offense * 100) / 100,
									Math.round(line.usage * 1000) / 1000,
									Math.round(line.valueNoPot),
									Math.round(line.availability * 100) / 100,
								];
							});
						return {
							tid: deal.tids[k],
							gain: deal.gains[k],
							rosterSize: [players.length, after.length],
							before: teamStrengths(outlook, deal.tids[k], players, picks),
							after: teamStrengths(outlook, deal.tids[k], after, afterPicks),
							moved: deal.rosters[k]
								.filter((p) => outP.has(p.pid))
								.map((p) => ({
									pid: p.pid,
									age: g.get("season") - p.born.year,
									ovr: last(p.ratings).ovr,
									exp: p.contract.exp,
									lines: lines(p),
								})),
							picksMoved: picks
								.filter((dp) => outD.has(dp.dpid))
								.map((dp) => [dp.season, dp.round, dp.pick, dp.originalTid]),
						};
					});
					suspicious.push({
						season: g.get("season"),
						phase: g.get("phase"),
						outlookSeason: outlook.season,
						offset: outlook.offset,
						meanPoints: outlook.meanPoints,
						titleCurves: outlook.titleCurves,
						sides,
					});
				});
			}
			if (env.ARENA_CHECK_TRADES !== undefined) {
				tradeWatchers.push((deal) => {
					// Not the outlook or the breakdown: held for every trade of a
					// long run, they ran it out of memory.
					const standing = (tid: number) => {
						const o = deal.outlook.teams.get(tid);
						return { title: o?.title[0], strength: o?.strength[0] };
					};
					watched.push({
						tids: deal.tids,
						give: deal.give,
						gains: deal.gains,
						standing: [standing(deal.tids[0]), standing(deal.tids[1])],
						season: g.get("season"),
						phase: g.get("phase"),
						rosters: [
							helpers.deepCopy(deal.rosters[0]),
							helpers.deepCopy(deal.rosters[1]),
						],
					});
				});
			}
			// ARENA_DEAL_SURVEY=1: at stops before each deadline (ARENA_SURVEY_AT,
			// days to go), every team's best deal with anyone, found but not made -
			// how much of today's deal is still on offer later, for the deadline
			// timing. Each pair of teams is priced once, as both see it.
			const survey: Record<string, unknown>[] = [];
			// Each team's best deal at each stop, priced again at every later stop
			// of the season: is that very deal still there, and still wanted by both?
			const surveyBest: {
				season: number;
				daysToDeadline: number;
				tid: number;
				tids: [number, number];
				give: NonNullable<ReturnType<typeof bestDealWith>>["give"];
				net: number;
			}[] = [];
			const revisits: Record<string, unknown>[] = [];
			const surveyAt: number[] = JSON.parse(
				env.ARENA_SURVEY_AT ?? "[56, 42, 28, 14, 7, 1]",
			);
			const snapshots = await advanceSeasons(
				numSeasons,
				env.ARENA_DEAL_SURVEY === "1"
					? {
							surveyAt,
							onSurvey: async (daysToDeadline) => {
								const outlook = await currentOutlook();
								const events = await idb.merged.events.getAll({
									season: g.get("season"),
								});
								const teams = (await idb.cache.teams.getAll()).filter(
									(t) => !t.disabled,
								);
								const states = new Map<number, TeamState>();
								for (const t of teams) {
									states.set(t.tid, await loadTeam(outlook, t.tid));
								}
								type Found = NonNullable<ReturnType<typeof bestDealWith>>;
								const deals: Found[] = [];
								const single = new Map<string, number>();
								for (const [i, x] of teams.entries()) {
									for (const y of teams.slice(i + 1)) {
										const a = states.get(x.tid)!;
										const b = states.get(y.tid)!;
										await ensureRetention(outlook, b.tradablePlayers, a.tid);
										await ensureRetention(outlook, a.tradablePlayers, b.tid);
										const deal = bestDealWith(outlook, a, b, single);
										if (deal) {
											deals.push(deal);
										}
									}
								}
								const net = (d: Pick<Found, "gains" | "margins">) =>
									Math.min(
										d.gains[0] - d.margins[0],
										d.gains[1] - d.margins[1],
									);
								for (const earlier of surveyBest) {
									if (earlier.season !== g.get("season")) {
										continue;
									}
									const again = priceDeal(
										outlook,
										states.get(earlier.tids[0])!,
										states.get(earlier.tids[1])!,
										earlier.give,
									);
									const k = earlier.tids[0] === earlier.tid ? 0 : 1;
									revisits.push({
										season: earlier.season,
										from: earlier.daysToDeadline,
										at: daysToDeadline,
										tid: earlier.tid,
										netThen: earlier.net,
										// Still possible (both still have what they give).
										possible: again !== undefined,
										net: again ? net(again) : undefined,
										gain: again ? again.gains[k] : undefined,
									});
								}
								for (const t of teams) {
									const mine = deals.filter((d) => d.tids.includes(t.tid));
									let best: Found | undefined;
									for (const d of mine) {
										if (!best || net(d) > net(best)) {
											best = d;
										}
									}
									const o = outlook.teams.get(t.tid)!;
									const ts = await idb.cache.teamSeasons.indexGet(
										"teamSeasonsBySeasonTid",
										[g.get("season"), t.tid],
									);
									const k = best ? (best.tids[0] === t.tid ? 0 : 1) : 0;
									if (best) {
										surveyBest.push({
											season: g.get("season"),
											daysToDeadline,
											tid: t.tid,
											tids: best.tids,
											give: best.give,
											net: net(best),
										});
									}
									const waiting =
										best && outlook.deadline
											? waitingOf(outlook, best, [
													states.get(best.tids[0])!,
													states.get(best.tids[1])!,
												])
											: undefined;
									const stats = (await idb.cache.teamStats.indexGet(
										"teamStatsByPlayoffsTid",
										[false, t.tid],
									)) as
										| { gp?: number; pts?: number; oppPts?: number }
										| undefined;
									survey.push({
										season: g.get("season"),
										daysToDeadline,
										tid: t.tid,
										won: ts?.won ?? 0,
										lost: ts?.lost ?? 0,
										// Points scored and allowed so far, to set the strength the
										// market reads against what the team then does.
										gp: stats?.gp ?? 0,
										pts: stats?.pts ?? 0,
										oppPts: stats?.oppPts ?? 0,
										strength: Math.round(o.strength[0]! * 100) / 100,
										recordEdge: outlook.recordEdge.get(t.tid) ?? 0,
										title: o.title[0],
										curveTitle: curveAt(
											outlook.titleCurves[0]!,
											o.strength[0]!,
										),
										// Partners it has a deal with that both would make.
										partners: mine.length,
										gain: best ? best.gains[k] : 0,
										best: best
											? {
													partner: best.tids[k === 0 ? 1 : 0],
													gains: [best.gains[k], best.gains[k === 0 ? 1 : 0]],
													margins: [
														best.margins[k],
														best.margins[k === 0 ? 1 : 0],
													],
													net: net(best),
													give: best.give[k],
													get: best.give[k === 0 ? 1 : 0],
													waiting: waiting
														? [waiting[k]!, waiting[k === 0 ? 1 : 0]!].map(
																roundWaiting,
															)
														: undefined,
												}
											: undefined,
										tradesSoFar: events.filter(
											(e) => e.type === "trade" && e.tids?.includes(t.tid),
										).length,
									});
								}
							},
						}
					: {},
			);
			const metrics = await computeMetrics(snapshots);
			const fs = await importNode("node:fs/promises");
			await fs.mkdir(out, { recursive: true });
			if (env.ARENA_FO === "1") {
				await fs.writeFile(
					`${out}/trades-${tag}.txt`,
					await getTradeHistoryDump(numSeasons + 1),
				);
			}
			if (env.ARENA_FO === "1") {
				await fs.writeFile(
					`${out}/tradelog-${tag}.json`,
					JSON.stringify(tradeLog),
				);
				await fs.writeFile(
					`${out}/deferred-${tag}.json`,
					JSON.stringify(deferLog),
				);
				// What became of every traded player: his overall each season, and
				// the teams he played for.
				const pids = new Set(
					(tradeLog as { moved: { pid: number }[] }[]).flatMap((t) =>
						t.moved.map((m) => m.pid),
					),
				);
				const realized: Record<number, unknown> = {};
				for (const pid of pids) {
					const p = await idb.merged.players.get(pid);
					if (p) {
						realized[pid] = {
							ratings: p.ratings.map((r) => [r.season, r.ovr]),
							teams: p.stats
								.filter((row) => !row.playoffs && (row.gp ?? 0) > 0)
								.map((row) => [row.season, row.tid]),
						};
					}
				}
				await fs.writeFile(
					`${out}/realized-${tag}.json`,
					JSON.stringify(realized),
				);
			}
			await fs.writeFile(
				`${out}/deadline-views-${tag}.json`,
				JSON.stringify(deadlineViews),
			);
			// Every team's seasons: how it did, which version ran it, and how
			// old it was, by minutes played.
			{
				const ages = new Map<string, [number, number]>();
				for (const p of await idb.league.getAll("players")) {
					for (const row of p.stats) {
						const min = row.min ?? 0;
						if (!row.playoffs && min > 0) {
							const key = `${row.season}:${row.tid}`;
							const a = ages.get(key) ?? [0, 0];
							a[0] += (row.season - p.born.year) * min;
							a[1] += min;
							ages.set(key, a);
						}
					}
				}
				const seasons: unknown[] = [];
				for (let s = firstSeason; s <= g.get("season"); s++) {
					for (let tid = 0; tid < g.get("numTeams"); tid++) {
						const [ts] = await idb.merged.teamSeasons.getAll({
							tid,
							season: s,
						});
						if (ts) {
							seasons.push({
								season: s,
								tid,
								split:
									teamMarketSettings.has(tid) || teamOutlookSettings.has(tid),
								won: ts.won,
								lost: ts.lost,
								playoffRoundsWon: ts.playoffRoundsWon,
								age: (() => {
									const a = ages.get(`${s}:${tid}`);
									return a && a[1] > 0
										? Math.round((a[0] / a[1]) * 10) / 10
										: undefined;
								})(),
							});
						}
					}
				}
				await fs.writeFile(`${out}/teams-${tag}.json`, JSON.stringify(seasons));
			}
			// Every player drafted while the league ran: where he went and how
			// he turned out, to judge the drafting.
			{
				const drafts: unknown[] = [];
				for (const p of await idb.cache.players.getAll()) {
					if (p.draft.year >= firstSeason && p.draft.round > 0) {
						drafts.push({
							pid: p.pid,
							season: p.draft.year,
							round: p.draft.round,
							pick: p.draft.pick,
							tid: p.draft.tid,
							ovr: p.ratings.map((r) => [r.season, r.ovr]),
						});
					}
				}
				await fs.writeFile(`${out}/drafts-${tag}.json`, JSON.stringify(drafts));
			}
			if (env.ARENA_PICK_SURVEY === "1") {
				await fs.writeFile(
					`${out}/pick-survey-${tag}.json`,
					JSON.stringify({
						projections: pickSurvey,
						drafts: snapshots.map((s) => ({
							season: s.season,
							order: s.draftOrder,
						})),
					}),
				);
			}
			if (env.ARENA_ROSTER_CHECK === "1") {
				await fs.writeFile(
					`${out}/roster-checks-${tag}.json`,
					JSON.stringify(rosterChecks),
				);
			}
			if (env.ARENA_FA_SURVEY === "1") {
				await fs.writeFile(
					`${out}/fa-survey-${tag}.json`,
					JSON.stringify(faSurvey),
				);
			}
			if (env.ARENA_DEAL_SURVEY === "1") {
				await fs.writeFile(
					`${out}/survey-${tag}.json`,
					JSON.stringify({ stops: survey, revisits }),
				);
			}
			if (env.ARENA_DEBUG_GAINS !== undefined) {
				await fs.writeFile(
					`${out}/suspicious-${tag}.json`,
					JSON.stringify(suspicious),
				);
			}
			if (env.ARENA_CHECK_TRADES !== undefined) {
				tradeWatchers.length = 0;
				await fs.writeFile(
					`${out}/trade-check-${tag}.json`,
					JSON.stringify(
						await checkTrades(watched, Number(env.ARENA_CHECK_TRADES)),
					),
				);
			}
			// ARENA_EXPORT: the league as it ends, for another run to carry on
			// (ARENA_IMPORT) - before league-<tag>.json, which marks the run
			// done.
			if (env.ARENA_EXPORT) {
				await exportArenaLeague(env.ARENA_EXPORT);
			}
			if (profiler) {
				const { profile } = await profiler.post("Profiler.stop");
				await fs.writeFile(env.ARENA_PROFILE!, JSON.stringify(profile));
				profiler.disconnect();
			}
			await fs.writeFile(
				`${out}/league-${tag}.json`,
				JSON.stringify({
					seed,
					seconds: (Date.now() - started) / 1000,
					market: env.ARENA_FO === "1" ? marketClock : undefined,
					metrics,
					freeAgents: snapshots.map((s) => ({
						season: s.season,
						rows: s.freeAgents,
						// Each team's cap room as free agency opened, by tid.
						capSpace: s.capSpace,
					})),
					expiring: snapshots.map((s) => ({
						season: s.season,
						rows: s.expiring,
					})),
					retirements: snapshots.map((s) => ({
						season: s.season,
						rows: s.retirements,
					})),
				}),
			);
		} finally {
			Math.random = realRandom;
		}
	},
);

const roundWaiting = (w: {
	before: number;
	after: number;
	later: number;
	now: number;
	wait: number;
	stillThere: number;
}) => ({
	before: Math.round(w.before * 100) / 100,
	after: Math.round(w.after * 100) / 100,
	later: Math.round(w.later * 100000) / 100000,
	now: Math.round(w.now * 100000) / 100000,
	wait: Math.round(w.wait * 100000) / 100000,
	stillThere: Math.round(w.stillThere * 1000) / 1000,
});

// Where in the regular season a trade happened: days to the deadline and days
// of games left, and each side's record.
const stampSeasonClock = async (
	row: Record<string, unknown>,
	tids: readonly number[],
) => {
	try {
		row.daysToDeadline = await getDaysLeftSchedule("tradeDeadline");
	} catch {
		row.daysToDeadline = null;
	}
	row.daysLeftSeason = await getDaysLeftSchedule();
	const records = [];
	for (const tid of tids) {
		const ts = await idb.cache.teamSeasons.indexGet("teamSeasonsBySeasonTid", [
			g.get("season"),
			tid,
		]);
		records.push([ts?.won ?? 0, ts?.lost ?? 0]);
	}
	row.records = records;
};

type WatchedTrade = Pick<
	Parameters<(typeof tradeWatchers)[number]>[0],
	"tids" | "give" | "gains" | "rosters"
> & {
	// Each side's title odds and strength this season, at the trade.
	standing: [
		{ title?: number; strength?: number },
		{ title?: number; strength?: number },
	];
	season: number;
	phase: number;
};

const healthy = (players: readonly Player[]) =>
	players.map((p) => {
		const copy = helpers.deepCopy(p);
		copy.injury = { type: "Healthy", gamesRemaining: 0 };
		return copy;
	});

const strengthPlayers = (players: readonly Player[]) =>
	players.map((p) => ({
		...playerLine(
			last(p.ratings) as unknown as ValueRatings,
			last(p.ratings).pos,
		),
		valueNoPot: p.valueNoPot,
	}));

const modelPoints = (players: readonly Player[]) =>
	rosterPoints(strengthPlayers(players));

// The model's points in their parts (players, synergy, positions), and the
// skills of the eight who will play most, for the fit audit.
const fitOf = (players: readonly Player[]) => {
	const { total, ...parts } = rosterParts(strengthPlayers(players));
	const skills: Record<string, number> = {};
	const posCounts: Record<string, number> = {};
	for (const p of [...players]
		.sort((a, b) => b.valueNoPot - a.valueNoPot)
		.slice(0, 8)) {
		for (const skill of last(p.ratings).skills) {
			skills[skill] = (skills[skill] ?? 0) + 1;
		}
		const pos = last(p.ratings).pos;
		posCounts[pos] = (posCounts[pos] ?? 0) + 1;
	}
	return { total, ...parts, skills, posCounts };
};

// For each side of each trade: what the model expected its healthy roster to
// gain this season, and what it gained in games against the rest of the league
// as it stands at the end of the run.
const checkTrades = async (trades: WatchedTrade[], games: number) => {
	const realRandom = Math.random;
	const opponents: Player[][] = [];
	for (let tid = 0; tid < g.get("numTeams"); tid++) {
		opponents.push(
			healthy(await idb.cache.players.indexGetAll("playersByTid", tid)),
		);
	}
	const marginOf = async (tid: number, roster: Player[], seed: number) => {
		const team = await toSimTeam(tid, inGameOrder(roster));
		let total = 0;
		for (let i = 0; i < games; i++) {
			const oppTid =
				(tid + 1 + (i % (opponents.length - 1))) % opponents.length;
			const oppTeam = await toSimTeam(oppTid, inGameOrder(opponents[oppTid]!));
			Math.random = makeSeededRandom(seed * 1_000_003 + i);
			const home = i % 2 === 0;
			const result = home
				? play(helpers.deepCopy(team), oppTeam)
				: play(oppTeam, helpers.deepCopy(team));
			const margin = result.team[0].stat.pts - result.team[1].stat.pts;
			total += home ? margin : -margin;
		}
		return total / games;
	};
	const rows = [];
	try {
		for (const [n, trade] of trades.entries()) {
			for (const k of [0, 1] as const) {
				const j = k === 0 ? 1 : 0;
				const before = healthy(trade.rosters[k]);
				const outgoing = new Set(trade.give[k].pids);
				const after = [
					...before.filter((p) => !outgoing.has(p.pid)),
					...healthy(
						trade.rosters[j].filter((p) => trade.give[j].pids.includes(p.pid)),
					),
				];
				const o = trade.standing[k];
				rows.push({
					trade: n,
					season: trade.season,
					phase: trade.phase,
					tid: trade.tids[k],
					gain: trade.gains[k],
					// Where the team stood: its title odds and strength this season.
					title: o.title,
					strength: o.strength,
					fitBefore: fitOf(before),
					fitAfter: fitOf(after),
					modelNow: modelPoints(after) - modelPoints(before),
					simNow:
						(await marginOf(trade.tids[k], after, n * 2 + k)) -
						(await marginOf(trade.tids[k], before, n * 2 + k)),
				});
			}
		}
	} finally {
		Math.random = realRandom;
	}
	return { games, rows };
};

const mean = (xs: number[]) =>
	xs.length === 0 ? Number.NaN : xs.reduce((a, x) => a + x, 0) / xs.length;

const median = (xs: number[]) => {
	if (xs.length === 0) {
		return Number.NaN;
	}
	const sorted = [...xs].sort((a, b) => a - b);
	const mid = Math.floor(sorted.length / 2);
	return sorted.length % 2 === 1
		? sorted[mid]!
		: (sorted[mid - 1]! + sorted[mid]!) / 2;
};

const share = (xs: boolean[]) => mean(xs.map((x) => (x ? 1 : 0)));

const correlation = (pairs: [number, number][]) => {
	const xs = pairs.map((p) => p[0]);
	const ys = pairs.map((p) => p[1]);
	const mx = mean(xs);
	const my = mean(ys);
	let num = 0;
	let dx = 0;
	let dy = 0;
	for (const [x, y] of pairs) {
		num += (x - mx) * (y - my);
		dx += (x - mx) ** 2;
		dy += (y - my) ** 2;
	}
	return num / Math.sqrt(dx * dy);
};

// Everything pooled across leagues, by benchmark key.
export const summarize = (leagues: LeagueMetrics[]) => {
	const pool = <K extends keyof LeagueMetrics>(key: K) =>
		leagues.flatMap((l) => l[key] as any[]);
	const sum = (key: keyof LeagueMetrics) =>
		leagues.reduce((a, l) => a + (l[key] as number), 0);
	const starMoves = sum("starMovesByTrade") + sum("starMovesByFreeAgency");
	const top10 =
		sum("top10FaStayed") + sum("top10FaLeft") + sum("top10FaUnsigned");
	return {
		winSd: mean(pool("winSdPerSeason")),
		yoyCorr: correlation(pool("yoyPairs")),
		teams60: mean(pool("teams60PerSeason")),
		under25: mean(pool("under25PerSeason")),
		bestRecordTitle: share(pool("bestRecordWonTitle")),
		championWins: mean(pool("championWins")),
		distinctChampions: mean(
			leagues.map(
				(l) =>
					(new Set(l.champions.slice(0, 10)).size * 10) /
					Math.min(10, l.champions.length),
			),
		),
		middleShare: sum("middleTeamSeasons") / pool("winsPerTeamSeason").length,
		middleStuck: sum("middleFourthPlus") / pool("winsPerTeamSeason").length,
		rebuildMedian: median(pool("rebuildLengths")),
		ageTop4: mean(pool("ageTop4")),
		ageBottom5: mean(pool("ageBottom5")),
		continuity: mean(pool("continuity")),
		trades: mean(pool("tradesPerSeason")),
		inSeasonTrades: mean(pool("inSeasonTradesPerSeason")),
		offseasonTrades: mean(pool("offseasonTradesPerSeason")),
		deadlineWeek: mean(pool("deadlineWeekShare")),
		playersPerTrade: mean(pool("playersPerTrade")),
		tradesWithFirst: share(pool("tradesWithFirst")),
		firsts: mean(pool("firstsPerSeason")),
		starMoves: mean(pool("starMovesPerSeason")),
		starMovesByTrade:
			starMoves > 0 ? sum("starMovesByTrade") / starMoves : Number.NaN,
		taxpayers: mean(pool("taxpayersPerSeason")),
		capSpaceTeams: mean(pool("capSpaceTeamsPerSummer")),
		deadMoneyMedian: median(pool("deadMoneyShares")),
		top10FaStay: top10 > 0 ? sum("top10FaStayed") / top10 : Number.NaN,
		top10VetFaStay:
			sum("top10VetFas") > 0
				? sum("top10VetFaStayed") / sum("top10VetFas")
				: Number.NaN,
	} satisfies Record<keyof typeof BENCHMARKS, number>;
};

const format = (b: Benchmark, x: number) =>
	!Number.isFinite(x)
		? "-"
		: b.percent
			? `${Math.round(x * 100)}%`
			: x < 10
				? x.toFixed(1)
				: x.toFixed(0);

export const renderReport = (
	title: string,
	columns: { name: string; leagues: LeagueMetrics[] }[],
) => {
	const summaries = columns.map((c) => summarize(c.leagues));
	const lines = [
		`# ${title}`,
		"",
		columns
			.map(
				(c) =>
					`${c.name}: ${c.leagues.length} leagues, ${c.leagues.reduce((a, l) => a + l.numSeasons, 0)} seasons`,
			)
			.join(". "),
		"",
		`| | ${columns.map((c) => c.name).join(" | ")} | NBA |`,
		`|---|${columns.map(() => "---|").join("")}---|`,
	];
	for (const [key, b] of Object.entries(BENCHMARKS) as [
		keyof typeof BENCHMARKS,
		Benchmark,
	][]) {
		const cells = summaries.map((s) => {
			const x = s[key];
			const ok = x >= b.low && x <= b.high;
			return `${format(b, x)}${Number.isFinite(x) && !ok ? " ⚠" : ""}`;
		});
		lines.push(`| ${b.label} | ${cells.join(" | ")} | ${b.nba} |`);
	}
	const gaps = columns.map((c) =>
		c.leagues.flatMap((l) => l.tradeHindsightGaps),
	);
	lines.push(
		"",
		"Lopsided trades, judged three seasons later by the win shares each side got:",
		"",
		...columns.map(
			(c, i) =>
				`- ${c.name}: median gap ${Math.round(median(gaps[i]!) * 100)}%, ${Math.round(share(gaps[i]!.map((x) => x >= 0.6)) * 100)}% of ${gaps[i]!.length} trades one-sided (80/20 or worse)`,
		),
	);
	return lines.join("\n");
};

test.runIf(env.ARENA_REPORT !== undefined)("arena: report", async () => {
	const fs = await importNode("node:fs/promises");
	const columns: { name: string; leagues: LeagueMetrics[] }[] = [];
	for (const dir of env.ARENA_REPORT!.split(",")) {
		const files = (await fs.readdir(dir)).filter((f: string) =>
			/^league-\d+\.json$/.test(f),
		);
		const leagues: LeagueMetrics[] = [];
		for (const f of files) {
			leagues.push(
				JSON.parse(await fs.readFile(`${dir}/${f}`, "utf8")).metrics,
			);
		}
		columns.push({ name: dir.split("/").filter(Boolean).at(-1)!, leagues });
	}
	const report = renderReport("Arena report", columns);
	await fs.writeFile(`${env.ARENA_REPORT!.split(",")[0]}/report.md`, report);
});
