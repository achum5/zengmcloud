import { test } from "vitest";
import { makeSeededRandom } from "../../common/random.ts";
import { getTradeHistoryDump } from "../../worker/api/tradeHistoryDump.ts";
import { league } from "../../worker/core/index.ts";
import {
	currentOutlook,
	findBestDeal,
	loadTeam,
	marketClock,
	marketSettings,
	outlookWatchers,
	tradeWatchers,
} from "../../worker/core/realisticFrontOffice/market.ts";
import { marginSettings } from "../../worker/core/realisticFrontOffice/margin.ts";
import { freeAgencyWatchers } from "../../worker/core/realisticFrontOffice/freeAgency.ts";
import {
	OUTLOOK_YEARS,
	outlookSeason,
	outlookSettings,
	projectPlayer,
	retentionOf,
	seasonLine,
	teamPoints,
	teamStrengths,
} from "../../worker/core/realisticFrontOffice/outlook.ts";
import type { Player } from "../../common/types.ts";
import { last } from "../../common/utils.ts";
import { idb } from "../../worker/db/index.ts";
import { g, helpers } from "../../worker/util/index.ts";
import {
	playerLine,
	rosterPoints,
	type ValueRatings,
} from "../../worker/core/realisticFrontOffice/strength.ts";
import { play, setOrder, toSimTeam } from "../impact/simTools.ts";
import { advanceSeasons, createArenaLeague } from "./runLeague.ts";
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

		// Every random number in the league, from league creation on, comes from
		// the seed - so a league that looks wrong can be played again exactly.
		await importNode("fake-indexeddb/auto");
		const realRandom = Math.random;
		Math.random = makeSeededRandom(seed);
		try {
			const started = Date.now();
			await createArenaLeague(seed);
			if (env.ARENA_FO === "1") {
				await league.setGameAttributes({ realisticFrontOffices: true });
				// ARENA_NO_WAIT=1: deals are made as soon as they are found.
				marketSettings.valueWaiting = env.ARENA_NO_WAIT !== "1";
				// ARENA_MARKET='{"oneSided":false}': any other market setting.
				Object.assign(marketSettings, JSON.parse(env.ARENA_MARKET ?? "{}"));
				Object.assign(outlookSettings, JSON.parse(env.ARENA_OUTLOOK ?? "{}"));
				Object.assign(marginSettings, JSON.parse(env.ARENA_MARGIN ?? "{}"));
			}
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
					tradeLog.push({
						season: g.get("season"),
						phase: g.get("phase"),
						// Days of the regular season left, to place in-season trades.
						daysLeft: g.get("daysLeft"),
						breakdown: deal.breakdown,
						tids: deal.tids,
						gains: deal.gains.map((x) => Math.round(x * 10000) / 10000),
						standing: deal.tids.map(standing),
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
					});
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
					watched.push({
						...deal,
						season: g.get("season"),
						phase: g.get("phase"),
						rosters: [
							helpers.deepCopy(deal.rosters[0]),
							helpers.deepCopy(deal.rosters[1]),
						],
					});
				});
			}
			// ARENA_DEAL_SURVEY=1: at stops before each deadline, every team's best
			// deal with anyone, found but not made - how much of today's deal is
			// still on offer later, for the deadline timing.
			const survey: {
				season: number;
				daysToDeadline: number;
				tid: number;
				gain: number;
				tradesSoFar: number;
			}[] = [];
			const snapshots = await advanceSeasons(
				numSeasons,
				env.ARENA_DEAL_SURVEY === "1"
					? {
							surveyAt: [56, 42, 28, 14, 7, 1],
							onSurvey: async (daysToDeadline) => {
								const outlook = await currentOutlook();
								const events = await idb.merged.events.getAll({
									season: g.get("season"),
								});
								const teams = (await idb.cache.teams.getAll()).filter(
									(t) => !t.disabled,
								);
								const single = new Map<string, number>();
								for (const t of teams) {
									const best = await findBestDeal(
										outlook,
										await loadTeam(outlook, t.tid),
										teams.filter((x) => x.tid !== t.tid).map((x) => x.tid),
										single,
									);
									survey.push({
										season: g.get("season"),
										daysToDeadline,
										tid: t.tid,
										gain: best ? best.gains[0] : 0,
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
					`${out}/trades-${seed}.txt`,
					await getTradeHistoryDump(numSeasons + 1),
				);
			}
			if (env.ARENA_FO === "1") {
				await fs.writeFile(
					`${out}/tradelog-${seed}.json`,
					JSON.stringify(tradeLog),
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
					`${out}/realized-${seed}.json`,
					JSON.stringify(realized),
				);
			}
			if (env.ARENA_PICK_SURVEY === "1") {
				await fs.writeFile(
					`${out}/pick-survey-${seed}.json`,
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
					`${out}/roster-checks-${seed}.json`,
					JSON.stringify(rosterChecks),
				);
			}
			if (env.ARENA_FA_SURVEY === "1") {
				await fs.writeFile(
					`${out}/fa-survey-${seed}.json`,
					JSON.stringify(faSurvey),
				);
			}
			if (env.ARENA_DEAL_SURVEY === "1") {
				await fs.writeFile(
					`${out}/survey-${seed}.json`,
					JSON.stringify(survey),
				);
			}
			if (env.ARENA_DEBUG_GAINS !== undefined) {
				await fs.writeFile(
					`${out}/suspicious-${seed}.json`,
					JSON.stringify(suspicious),
				);
			}
			if (env.ARENA_CHECK_TRADES !== undefined) {
				tradeWatchers.length = 0;
				await fs.writeFile(
					`${out}/trade-check-${seed}.json`,
					JSON.stringify(
						await checkTrades(watched, Number(env.ARENA_CHECK_TRADES)),
					),
				);
			}
			await fs.writeFile(
				`${out}/league-${seed}.json`,
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
				}),
			);
		} finally {
			Math.random = realRandom;
		}
	},
);

type WatchedTrade = Parameters<(typeof tradeWatchers)[number]>[0] & {
	season: number;
	phase: number;
};

const healthy = (players: readonly Player[]) =>
	players.map((p) => {
		const copy = helpers.deepCopy(p);
		copy.injury = { type: "Healthy", gamesRemaining: 0 };
		return copy;
	});

const modelPoints = (players: readonly Player[]) =>
	rosterPoints(
		players.map((p) => ({
			...playerLine(last(p.ratings) as unknown as ValueRatings),
			valueNoPot: p.valueNoPot,
		})),
	);

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
		const sorted = [...roster].sort((a, b) => b.valueNoPot - a.valueNoPot);
		setOrder(sorted);
		const team = await toSimTeam(tid, sorted);
		let total = 0;
		for (let i = 0; i < games; i++) {
			const oppTid =
				(tid + 1 + (i % (opponents.length - 1))) % opponents.length;
			const opp = [...opponents[oppTid]!].sort(
				(a, b) => b.valueNoPot - a.valueNoPot,
			);
			setOrder(opp);
			const oppTeam = await toSimTeam(oppTid, opp);
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
				rows.push({
					trade: n,
					season: trade.season,
					phase: trade.phase,
					tid: trade.tids[k],
					gain: trade.gains[k],
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
