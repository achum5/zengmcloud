import { test } from "vitest";
import { makeSeededRandom } from "../../common/random.ts";
import { advanceSeasons, createArenaLeague } from "./runLeague.ts";
import { computeMetrics, type LeagueMetrics } from "./metrics.ts";
import { BENCHMARKS, type Benchmark } from "./benchmarks.ts";

// THE ARENA: whole real-players leagues, every team run by the CPU, played
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
// The report lands in ARENA_REPORT/report.md.

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
			const snapshots = await advanceSeasons(numSeasons);
			const metrics = await computeMetrics(snapshots);
			const fs = await importNode("node:fs/promises");
			await fs.mkdir(out, { recursive: true });
			await fs.writeFile(
				`${out}/league-${seed}.json`,
				JSON.stringify({
					seed,
					seconds: (Date.now() - started) / 1000,
					metrics,
				}),
			);
		} finally {
			Math.random = realRandom;
		}
	},
);

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
