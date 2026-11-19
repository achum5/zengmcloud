import { test } from "vitest";
import { g } from "../../worker/util/index.ts";
import { loadLeagueFile } from "./loadLeague.ts";

// THE CURVE SLOPE PROBE: what a point of strength is worth in title odds to
// each of the league's best teams, read off the league's title curve (one
// logistic fit across every team) and off the team's own (ownCurves), and
// played out directly - the season simulated again with just that team's
// strength moved, everyone else as they are.
//   CS_LEAGUE=<league.json> CS_OUT=<file> [CS_STEP=0.8] [CS_TOP=8] \
//     [CS_ITER=20000] [CS_YEARS=0,1,2] npx vitest --run --project basketball \
//     src/test/smart/curveSlopeProbe.test.ts -t "curve slope"
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

// The league loaded, its outlook built, and each season of it simulated with
// any one team's strength moved.
const setUp = async () => {
	await importNode("fake-indexeddb/auto");
	const fs = await importNode("node:fs");
	const lines: string[] = [];
	const log = (x: string) => {
		lines.push(x);
		fs.writeFileSync(env.CS_OUT ?? "/dev/stdout", lines.join("\n") + "\n");
	};
	await loadLeagueFile(env.CS_LEAGUE!);
	const { league } = await import("../../worker/core/index.ts");
	await league.setGameAttributes({
		spectator: true,
		smartCpu: true,
		realisticFrontOffices: false,
	});
	const { idb } = await import("../../worker/db/index.ts");
	const o = await import("../../worker/core/realisticFrontOffice/outlook.ts");
	const { simulateSeason } =
		await import("../../worker/core/realisticFrontOffice/seasonSim.ts");
	const outlook: any = await o.buildOutlook();
	const season = g.get("season");
	const teams = (await idb.cache.teams.getAll()).filter((t) => !t.disabled);
	const abbrev = new Map(teams.map((t) => [t.tid, t.abbrev]));
	const numGames = g.get("numGames");
	const records = new Map<number, { won: number; lost: number }>();
	for (const t of teams) {
		const ts = await idb.cache.teamSeasons.indexGet("teamSeasonsBySeasonTid", [
			season,
			t.tid,
		]);
		records.set(t.tid, { won: ts?.won ?? 0, lost: ts?.lost ?? 0 });
	}
	const inSeason = outlook.progress > 0;
	const doubtOf = (y: number) =>
		o.strengthUncertainty(y, y === 0 ? outlook.progress : 0);
	const run = (
		y: number,
		seed: number,
		iterations: number,
		bump?: [number, number],
		shifts?: number[],
	) => {
		const now = y === 0 && inSeason;
		return simulateSeason({
			teams: teams.map((t) => ({
				tid: t.tid,
				cid: t.cid,
				strength:
					outlook.teams.get(t.tid).strength[y] +
					(bump && bump[0] === t.tid ? bump[1] : 0),
				won: now ? records.get(t.tid)!.won : 0,
				lost: now ? records.get(t.tid)!.lost : 0,
				gamesRemaining: now
					? Math.max(
							0,
							numGames - records.get(t.tid)!.won - records.get(t.tid)!.lost,
						)
					: numGames,
			})),
			numGamesPlayoffSeries: g.get("numGamesPlayoffSeries", "current"),
			playIn: g.get("playIn"),
			byConference: g.get("playoffsByConf", "current") ?? true,
			uncertainty: doubtOf(y),
			iterations,
			seed,
			shifts,
		});
	};
	// Where a team stands on its curves, its record counted this season.
	const x = (tid: number, y: number) =>
		outlook.teams.get(tid).strength[y] +
		(y === 0 ? (outlook.recordEdge?.get(tid) ?? 0) : 0);
	return { o, outlook, teams, abbrev, records, run, x, doubtOf, log, season };
};

test.skipIf(!env.CS_LEAGUE)(
	"curve slope probe",
	{ timeout: 3_600_000 },
	async () => {
		const { o, outlook, teams, abbrev, records, run, x, doubtOf, log, season } =
			await setUp();
		const step = Number(env.CS_STEP ?? 0.8);
		const iterations = Number(env.CS_ITER ?? 20000);
		for (const y of (env.CS_YEARS ?? "0").split(",").map(Number)) {
			const base = run(y, 12345, iterations);
			const order = [...teams].sort(
				(a, b) => base.get(b.tid)!.title - base.get(a.tid)!.title,
			);
			log(
				`${season} y${y}, progress ${outlook.progress.toFixed(2)}, uncertainty ${doubtOf(y).toFixed(2)}, step ${step}`,
			);
			for (const t of order.slice(0, Number(env.CS_TOP ?? 8))) {
				const at = x(t.tid, y);
				const up = run(y, 12345, iterations, [t.tid, step]).get(t.tid)!.title;
				const league = outlook.titleCurves[y];
				const own = o.curvesFor(outlook, t.tid).title[y]!;
				const pct = (v: number) => (100 * v).toFixed(1);
				log(
					`${abbrev.get(t.tid)} strength ${outlook.teams.get(t.tid).strength[y].toFixed(2)} record ${records.get(t.tid)!.won}-${records.get(t.tid)!.lost}: title ${pct(base.get(t.tid)!.title)}% -> ${pct(up)}% played out; league curve ${pct(o.curveAt(league, at))}% -> ${pct(o.curveAt(league, at + step))}%; own ${pct(o.curveAt(own, at))}% -> ${pct(o.curveAt(own, at + step))}%`,
				);
			}
		}
	},
);

// CS_NOISE=<seeds>: how far each team's own curve moves from one set of
// simulated seasons to another - the title chance it adds for CS_STEP
// points, read off its own curve at each of that many seeds (CS_ITER_OWN
// seasons each), beside the league's curve and the seasons played again.
test.skipIf(!env.CS_NOISE)(
	"own curve noise",
	{ timeout: 3_600_000 },
	async () => {
		const { o, outlook, teams, abbrev, run, x, doubtOf, log } = await setUp();
		const step = Number(env.CS_STEP ?? 0.8);
		const seeds = Number(env.CS_NOISE);
		const iterations = Number(env.CS_ITER_OWN ?? 1000);
		for (const y of (env.CS_YEARS ?? "0").split(",").map(Number)) {
			const shifts = o.OWN_SHIFTS.map(
				(f: number) => f * Math.min(doubtOf(y), o.OWN_SHIFT_MOST),
			);
			const all = [0, ...shifts];
			const gains = new Map<number, number[]>();
			for (let k = 0; k < seeds; k++) {
				const sim = run(y, 7919 * (k + 1) + y, iterations, undefined, shifts);
				for (const t of teams) {
					const r = sim.get(t.tid)!;
					const at = x(t.tid, y);
					const curve = o.ownCurve(
						outlook.titleCurves[y],
						at,
						all,
						[r.rounds, ...r.shifted!].map((f: number[]) => f.at(-1)!),
						r.title * iterations,
					);
					const list = gains.get(t.tid) ?? [];
					list.push(o.curveAt(curve, at + step) - o.curveAt(curve, at));
					gains.set(t.tid, list);
				}
			}
			const big = run(y, 99991 + y, 20000);
			log(
				`y${y}: title gain for +${step}, own curve at ${seeds} seeds (${iterations} seasons each)`,
			);
			const order = [...teams].sort(
				(a, b) => big.get(b.tid)!.title - big.get(a.tid)!.title,
			);
			for (const t of order.slice(0, Number(env.CS_TOP ?? 12))) {
				const list = gains.get(t.tid)!;
				const mean = list.reduce((a, v) => a + v, 0) / list.length;
				const sd = Math.sqrt(
					list.reduce((a, v) => a + (v - mean) ** 2, 0) / (list.length - 1),
				);
				const played =
					run(y, 99991 + y, 20000, [t.tid, step]).get(t.tid)!.title -
					big.get(t.tid)!.title;
				const at = x(t.tid, y);
				const league =
					o.curveAt(outlook.titleCurves[y], at + step) -
					o.curveAt(outlook.titleCurves[y], at);
				const pp = (v: number) => (100 * v).toFixed(2);
				log(
					`  ${abbrev.get(t.tid)} title ${(100 * big.get(t.tid)!.title).toFixed(1)}%: played +${pp(played)}, league +${pp(league)}, own +${pp(mean)} sd ${pp(sd)}`,
				);
			}
		}
	},
);
