import { test } from "vitest";
import { makeSeededRandom } from "../../common/random.ts";
import { g } from "../../worker/util/index.ts";
import { autopilot, shark } from "./policies.ts";
import { gmPolicy } from "./gmPolicy.ts";
import type { BenchPolicy } from "./policy.ts";
import {
	BENCH_SETTINGS,
	createBenchLeague,
	runBench,
	type SeasonRecord,
	type TradeRecord,
} from "./runBench.ts";

// THE BENCH (yardstick A, docs/smart-cpu.md "The bench"): one team run by a
// policy as a human, the rest by today's CPU, a random-players league with the
// owner's settings. One league per process, never part of the normal test run:
//
//   BENCH_RUN=1 BENCH_POLICY=autopilot BENCH_SEED=1 BENCH_SEASONS=10 \
//     BENCH_OUT=/tmp/bench npx vitest --run --project basketball \
//     src/test/bench/bench.test.ts
//   BENCH_REPORT=/tmp/bench npx vitest --run --project basketball \
//     src/test/bench/bench.test.ts
//
// BENCH_POLICY: autopilot (default) or shark; BENCH_SHARK='{"decideGames":
// 600}' its options. BENCH_TIDS=7,17: the policy's teams (default one,
// 7 x seed mod 30); BENCH_TEAMS=3 three of them, ten apart. BENCH_FO=1:
// realistic front offices in place of the vanilla CPU. BENCH_HARD_CAP=0: no
// secondary hard cap on the policy's teams. BENCH_LEAGUE='{"numGames":41}':
// league settings over the owner's. BENCH_FALLBACK_EVERY: days between auto
// play's signing stops in the season (7). BENCH_LABEL: what the report
// groups the run by (the policy by default), for a variant.
// BENCH_AUTOPLAY_SIGNING=1: a check on the bench - the fallbacks sign as auto
// play does (at his demand, whatever his mood), not through negotiation.

const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};

const importNode = (name: string) => import(/* @vite-ignore */ name);

export type BenchRunFile = {
	seed: number;
	policy: string;
	// What the report groups runs by: BENCH_LABEL, or the policy.
	label?: string;
	tids: number[];
	fo: boolean;
	hardCap: boolean;
	autoPlaySigning?: boolean;
	settings: Record<string, unknown>;
	numTeams: number;
	seasons: SeasonRecord[];
	trades: TradeRecord[];
	minutes: number;
	done: boolean;
};

const policyFor = (name: string, seed: number): BenchPolicy => {
	if (name === "autopilot") {
		return autopilot();
	}
	if (name === "shark") {
		return shark({ seed, ...JSON.parse(env.BENCH_SHARK ?? "{}") });
	}
	if (name === "gm") {
		return gmPolicy(JSON.parse(env.BENCH_GM ?? "{}"));
	}
	throw new Error(`Unknown policy ${name}`);
};

test.runIf(env.BENCH_RUN === "1")(
	"bench: play a league",
	{ timeout: 24 * 60 * 60 * 1000 },
	async () => {
		const seed = Number(env.BENCH_SEED ?? 1);
		const numSeasons = Number(env.BENCH_SEASONS ?? 10);
		const policyName = env.BENCH_POLICY ?? "autopilot";
		const out = env.BENCH_OUT ?? ".";
		const fo = env.BENCH_FO === "1";
		const hardCap = env.BENCH_HARD_CAP !== "0";
		const first = (7 * seed) % 30;
		const tids = env.BENCH_TIDS
			? env.BENCH_TIDS.split(",").map(Number)
			: Array.from(
					{ length: Number(env.BENCH_TEAMS ?? 1) },
					(_, i) => (first + 10 * i) % 30,
				);
		const league = JSON.parse(env.BENCH_LEAGUE ?? "{}");
		const label = env.BENCH_LABEL ?? policyName;
		const tag = `${label}${fo ? "-fo" : ""}-${seed}`;
		const fs = await importNode("node:fs/promises");
		await fs.mkdir(out, { recursive: true });
		const logLine = (line: string) => {
			void fs.appendFile(`${out}/progress-${tag}.log`, `${line}\n`);
		};

		await importNode("fake-indexeddb/auto");
		const realRandom = Math.random;
		Math.random = makeSeededRandom(seed);
		const started = Date.now();
		try {
			const options = {
				seed,
				seasons: numSeasons,
				tids,
				policy: policyFor(policyName, seed),
				fo,
				hardCap,
				league,
				fallbackEvery: Number(env.BENCH_FALLBACK_EVERY ?? 7),
				autoPlaySigning: env.BENCH_AUTOPLAY_SIGNING === "1",
				log: logLine,
			};
			await createBenchLeague(options);
			logLine(
				`league ${seed}: ${policyName}, tids ${tids.join(",")}, ${fo ? "realistic front offices" : "vanilla"}, hard cap ${hardCap ? "on" : "off"} [${((Date.now() - started) / 1000).toFixed(0)} s to create]`,
			);
			const file: BenchRunFile = {
				seed,
				policy: policyName,
				label,
				tids,
				fo,
				hardCap,
				autoPlaySigning: options.autoPlaySigning,
				settings: { ...BENCH_SETTINGS, ...league },
				numTeams: g.get("numTeams"),
				seasons: [],
				trades: [],
				minutes: 0,
				done: false,
			};
			const write = async () => {
				file.minutes = (Date.now() - started) / 60000;
				await fs.writeFile(`${out}/bench-${tag}.json`, JSON.stringify(file));
			};
			const result = await runBench({
				...options,
				onSeason: async (record, trades) => {
					file.seasons.push(record);
					file.trades = trades;
					await write();
				},
			});
			file.trades = result.trades;
			file.done = true;
			await write();
		} finally {
			Math.random = realRandom;
		}
	},
);

// ---------------------------------------------------------------------------
// The report: every run in BENCH_REPORT pooled by policy.

// Wilson's 95% interval for a rate.
export const wilson = (x: number, n: number) => {
	if (n === 0) {
		return [0, 1] as const;
	}
	const z = 1.96;
	const p = x / n;
	const denom = 1 + (z * z) / n;
	const center = (p + (z * z) / (2 * n)) / denom;
	const half =
		(z * Math.sqrt((p * (1 - p)) / n + (z * z) / (4 * n * n))) / denom;
	return [Math.max(0, center - half), Math.min(1, center + half)] as const;
};

// The title rate's 95% interval with whole leagues resampled: a league's
// seasons are not independent (one team can win most of them).
export const leagueBootstrap = (
	perLeague: { titles: number; n: number }[],
	draws = 2000,
) => {
	if (perLeague.length < 2) {
		return [Number.NaN, Number.NaN] as const;
	}
	const random = makeSeededRandom(1);
	const rates: number[] = [];
	for (let d = 0; d < draws; d++) {
		let titles = 0;
		let n = 0;
		for (let i = 0; i < perLeague.length; i++) {
			const x = perLeague[Math.floor(random() * perLeague.length)]!;
			titles += x.titles;
			n += x.n;
		}
		rates.push(n > 0 ? titles / n : 0);
	}
	rates.sort((a, b) => a - b);
	return [
		rates[Math.floor(0.025 * draws)]!,
		rates[Math.floor(0.975 * draws)]!,
	] as const;
};

const mean = (xs: number[]) =>
	xs.length === 0 ? Number.NaN : xs.reduce((a, x) => a + x, 0) / xs.length;
const se = (xs: number[]) => {
	const m = mean(xs);
	return xs.length < 2
		? Number.NaN
		: Math.sqrt(
				xs.reduce((a, x) => a + (x - m) ** 2, 0) / (xs.length - 1) / xs.length,
			);
};
const pct = (x: number, digits = 1) =>
	Number.isFinite(x) ? `${(100 * x).toFixed(digits)}%` : "-";
const fmt = (x: number, digits = 1) =>
	Number.isFinite(x) ? x.toFixed(digits) : "-";

export const benchReport = (runs: BenchRunFile[]) => {
	const groups = new Map<string, BenchRunFile[]>();
	for (const r of runs) {
		const key = `${r.label ?? r.policy}${r.fo ? " (realistic FO)" : ""}${r.hardCap ? "" : " (no hard cap)"}`;
		groups.set(key, [...(groups.get(key) ?? []), r]);
	}
	const lines = [
		"# Bench report",
		"",
		"Each row pools every season of every run of a policy: its team-seasons (one per policy team per season). Title rate with Wilson's 95% interval, and with whole leagues resampled (a league's seasons hang together); parity is 1 in 30 (3.3%). Rounds: where its season ended.",
		"",
		"| policy | leagues | team-seasons | titles | title rate (95% CI) | leagues resampled | win% | rating rank start / playoffs | trades a season | s a season (policy) |",
		"|---|---|---|---|---|---|---|---|---|---|",
	];
	const roundRows: string[] = [];
	const cpuRows: string[] = [];
	for (const [key, rs] of [...groups].sort()) {
		const rows = rs.flatMap((r) => r.seasons.flatMap((s) => s.policy));
		const seasons = rs.flatMap((r) => r.seasons);
		const titles = rows.filter((x) => x.title).length;
		const [lo, hi] = wilson(titles, rows.length);
		const winPct = rows.map((x) => x.won / Math.max(1, x.won + x.lost));
		const [blo, bhi] = leagueBootstrap(
			rs.map((r) => {
				const xs = r.seasons.flatMap((s) => s.policy);
				return { titles: xs.filter((x) => x.title).length, n: xs.length };
			}),
		);
		lines.push(
			`| ${key} | ${rs.length} (seeds ${rs.map((r) => r.seed).join(", ")}) | ${rows.length} | ${titles} | ${pct(titles / rows.length)} (${pct(lo)}-${pct(hi)}) | ${pct(blo)}-${pct(bhi)} | ${fmt(100 * mean(winPct))} ± ${fmt(100 * se(winPct))} | ${fmt(mean(rows.map((x) => x.rankStart)))} / ${fmt(mean(rows.map((x) => x.rankPlayoffs)))} | ${fmt(mean(rows.map((x) => x.trades)), 2)} | ${fmt(mean(seasons.map((s) => s.seconds)), 0)} (${fmt(mean(seasons.map((s) => s.policySeconds)), 0)}) |`,
		);
		const rounds = Math.max(4, ...seasons.map((s) => s.rounds));
		const counts = Array.from({ length: rounds + 2 }, () => 0);
		for (const x of rows) {
			counts[x.roundsWon + 1]! += 1;
		}
		roundRows.push(
			`| ${key} | ${counts.map((c) => `${c} (${pct(c / rows.length, 0)})`).join(" | ")} |`,
		);
		// The CPU teams in the same leagues: titles a team-season, to set the
		// policy's against.
		const cpuSeasons = seasons.reduce(
			(a, s) => a + s.teams.length - s.policy.length,
			0,
		);
		const cpuTitles = seasons.filter(
			(s) => s.champion >= 0 && !s.policy.some((x) => x.tid === s.champion),
		).length;
		const cpuWin = seasons.flatMap((s) =>
			s.teams
				.filter((t) => !s.policy.some((x) => x.tid === t.tid))
				.map((t) => t.won / Math.max(1, t.won + t.lost)),
		);
		cpuRows.push(
			`| ${key} | ${cpuSeasons} | ${cpuTitles} | ${pct(cpuTitles / cpuSeasons)} | ${fmt(100 * mean(cpuWin))} |`,
		);
	}
	lines.push(
		"",
		"Where the policy team's seasons ended (rounds won; -1 missed the playoffs, the last column the title):",
		"",
		"| policy | missed | out R1 | out R2 | out R3 | lost final | title |",
		"|---|---|---|---|---|---|---|",
		...roundRows,
		"",
		"The CPU teams in the same leagues:",
		"",
		"| policy | CPU team-seasons | CPU titles | a team-season | CPU win% |",
		"|---|---|---|---|---|",
		...cpuRows,
		"",
		"Each run: the policy's teams, and the champion season by season (* the policy's).",
		"",
	);
	for (const r of [...runs].sort(
		(a, b) =>
			(a.label ?? a.policy).localeCompare(b.label ?? b.policy) ||
			a.seed - b.seed,
	)) {
		const champions = r.seasons.map((s) =>
			r.tids.includes(s.champion) ? `${s.champion}*` : String(s.champion),
		);
		lines.push(
			`- ${r.label ?? r.policy}${r.fo ? " (FO)" : ""} seed ${r.seed}, tids ${r.tids.join(",")}: ${champions.join(" ")}${r.done ? "" : " (unfinished)"}; ${r.trades.length} trades`,
		);
	}
	return lines.join("\n");
};

test.runIf(env.BENCH_REPORT !== undefined)("bench: report", async () => {
	const fs = await importNode("node:fs/promises");
	const dir = env.BENCH_REPORT!;
	const runs: BenchRunFile[] = [];
	for (const f of await fs.readdir(dir)) {
		if (/^bench-.*\.json$/.test(f)) {
			runs.push(JSON.parse(await fs.readFile(`${dir}/${f}`, "utf8")));
		}
	}
	const report = benchReport(runs);
	await fs.writeFile(`${dir}/report.md`, report);
	console.log(report);
});
