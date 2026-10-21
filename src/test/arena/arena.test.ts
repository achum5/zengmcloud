import { test } from "vitest";
import { makeSeededRandom } from "../../common/random.ts";
import { getTradeHistoryDump } from "../../worker/api/tradeHistoryDump.ts";
import { league } from "../../worker/core/index.ts";
import {
	marketClock,
	tradeWatchers,
} from "../../worker/core/realisticFrontOffice/market.ts";
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
			}
			const watched: WatchedTrade[] = [];
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
			const snapshots = await advanceSeasons(numSeasons);
			const metrics = await computeMetrics(snapshots);
			const fs = await importNode("node:fs/promises");
			await fs.mkdir(out, { recursive: true });
			if (env.ARENA_FO === "1") {
				await fs.writeFile(
					`${out}/trades-${seed}.txt`,
					await getTradeHistoryDump(numSeasons + 1),
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
