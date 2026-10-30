// REALISTIC DRAFT PROSPECTS CALIBRATION - how realisticProspectsData.ts is made,
// kept runnable so the tables can be remade when generation or progression
// changes.
//
// Skipped unless REALISTIC_PROSPECTS_CALIBRATION=1, because a run is ~25 full
// simmed seasons (about five minutes):
//
//   REALISTIC_PROSPECTS_CALIBRATION=1 npx vitest --run --project basketball \
//     src/worker/core/draft/realisticProspectsCalibration.test.ts
//
// THE TARGET is real NBA rotation players (2023-2026 seasons, 500+ minutes),
// with the game's own real-player ratings. Those are the right yardstick for
// this engine: simming real rosters with them reproduces real box scores'
// spread of scoring, assists and rebounding closely.
//
// THE COMPARISON has to be like for like, so it is never "draft prospects vs
// NBA players". Each league here is 15 generated draft classes, class k aged k
// seasons with the game's own progression, the best 390 rostered on 30 teams
// and simmed for a season; its rotation players are the ones compared. The
// reshaping is applied at the draft, so whatever progression and roster
// selection do to it on the way is part of what gets measured.
//
// THE FIT works on normal scores (see realisticProspects.ts) and iterates: after
// each round, whatever still separates the simmed rotation players from the
// real ones - in each skill's distribution and in how the skills co-vary - is
// added back onto the draft-time target, and the leagues are rebuilt. Four or
// five rounds settle. Two things are deliberately held fixed:
//
//   - How skills depend on the body (bodySlopes) stays as generated. Real data
//     ties skill to athleticism harder, but only because the players who made
//     the league are the ones good at several things at once; fitting that
//     made athletic prospects better at everything (top picks ~2 ovr better).
//   - Each skill keeps stock generation's average. Random and real players sit
//     at different levels (random ones dribble and pass ~10 better and rebound
//     ~9 worse), and the engine's league-wide numbers are tuned to the random
//     levels: matching the real ones raised turnovers and steals. So the target
//     is the real distribution moved to the stock average - or, where the two
//     averages are far apart and the real shape is mostly pile-up against 0 or
//     100, the stock shape stretched to the real spread.
import { assert, test, vi } from "vitest";
import { mockIDBLeague, resetCache, resetG } from "../../../test/helpers.ts";
import { idb } from "../../db/index.ts";
import { g, helpers } from "../../util/index.ts";
import genPlayersWithoutSaving from "./genPlayersWithoutSaving.ts";
import {
	BODY_KEYS,
	QUANTILES,
	SKILL_KEYS,
	normInv,
	quantileOf,
	reshapeProspect,
	type ReshapeTables,
} from "./realisticProspects.ts";
import { DEFAULT_LEVEL } from "../../../common/budgetLevels.ts";
import { DEFAULT_PLAY_THROUGH_INJURIES } from "../../../common/constants.ts";
import { developSeasonSync } from "../player/developSeason.ts";
import ovr from "../player/ovr.ts";
import pos from "../player/pos.ts";
import skills from "../player/skills.ts";
import GameSim from "../GameSim.ts";
import { processTeam } from "../game/loadTeams.ts";
import { findStarters } from "../team/rosterAutoSort.basketball.ts";

const nodeEnv: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const RUN = nodeEnv.REALISTIC_PROSPECTS_CALIBRATION === "1";
const NUM_LEAGUES = Number(nodeEnv.REALISTIC_PROSPECTS_LEAGUES ?? 4);
const ROUNDS = Number(nodeEnv.REALISTIC_PROSPECTS_ROUNDS ?? 5);
const OUT_FILE =
	nodeEnv.REALISTIC_PROSPECTS_OUT ??
	"src/worker/core/draft/realisticProspectsData.ts";

const NUM_COHORTS = 15;
const NUM_TEAMS = 30;
const ROSTER = 13;
// A season here is a double round robin, 58 games; real seasons are 82.
const NUM_GAMES = 2 * (NUM_TEAMS - 1);
const ROTATION_MINUTES_REAL = 500;
const ROTATION_MINUTES_SIM = (500 * NUM_GAMES) / 82;
// Where the real and stock averages differ by more than this, use the stock
// shape stretched to the real spread (see the top).
const SHAPE_GAP = 5;

const ALL_KEYS = [...BODY_KEYS, ...SKILL_KEYS];
type SkillKey = (typeof SKILL_KEYS)[number];

// MATH

type Mat = number[][];
const zeros = (n: number, m: number): Mat =>
	Array.from({ length: n }, () => new Array(m).fill(0));
const identity = (n: number): Mat => {
	const I = zeros(n, n);
	for (let i = 0; i < n; i++) {
		I[i]![i] = 1;
	}
	return I;
};
const mul = (A: Mat, B: Mat): Mat => {
	const C = zeros(A.length, B[0]!.length);
	for (let i = 0; i < A.length; i++) {
		for (let k = 0; k < B.length; k++) {
			for (let j = 0; j < B[0]!.length; j++) {
				C[i]![j]! += A[i]![k]! * B[k]![j]!;
			}
		}
	}
	return C;
};
const transpose = (A: Mat): Mat => A[0]!.map((_, j) => A.map((row) => row[j]!));
const add = (A: Mat, B: Mat, scale = 1): Mat =>
	A.map((row, i) => row.map((x, j) => x + scale * B[i]![j]!));
const inverse = (A: Mat): Mat => {
	const n = A.length;
	const M = A.map((row, i) => [...row, ...identity(n)[i]!]);
	for (let c = 0; c < n; c++) {
		let pivot = c;
		for (let r = c + 1; r < n; r++) {
			if (Math.abs(M[r]![c]!) > Math.abs(M[pivot]![c]!)) {
				pivot = r;
			}
		}
		[M[c], M[pivot]] = [M[pivot]!, M[c]!];
		const pv = M[c]![c]!;
		for (let j = 0; j < 2 * n; j++) {
			M[c]![j]! /= pv;
		}
		for (let r = 0; r < n; r++) {
			if (r !== c) {
				const f = M[r]![c]!;
				for (let j = 0; j < 2 * n; j++) {
					M[r]![j]! -= f * M[c]![j]!;
				}
			}
		}
	}
	return M.map((row) => row.slice(n));
};

// Symmetric matrix to a power, by Jacobi eigendecomposition. Eigenvalues are
// floored first, which also projects a not-quite-valid covariance back to a
// valid one.
const symPow = (S: Mat, power: number, floor: number): Mat => {
	const n = S.length;
	const A = S.map((row) => [...row]);
	const V = identity(n);
	for (let sweep = 0; sweep < 100; sweep++) {
		let off = 0;
		for (let i = 0; i < n; i++) {
			for (let j = i + 1; j < n; j++) {
				off += A[i]![j]! ** 2;
			}
		}
		if (off < 1e-14) {
			break;
		}
		for (let p = 0; p < n; p++) {
			for (let q = p + 1; q < n; q++) {
				if (Math.abs(A[p]![q]!) < 1e-15) {
					continue;
				}
				const theta = (A[q]![q]! - A[p]![p]!) / (2 * A[p]![q]!);
				const t =
					Math.sign(theta || 1) /
					(Math.abs(theta) + Math.sqrt(theta * theta + 1));
				const c = 1 / Math.sqrt(t * t + 1);
				const s = t * c;
				for (const M of [A, V]) {
					for (let k = 0; k < n; k++) {
						const mkp = M[k]![p]!;
						const mkq = M[k]![q]!;
						M[k]![p] = c * mkp - s * mkq;
						M[k]![q] = s * mkp + c * mkq;
					}
				}
				for (let k = 0; k < n; k++) {
					const apk = A[p]![k]!;
					const aqk = A[q]![k]!;
					A[p]![k] = c * apk - s * aqk;
					A[q]![k] = s * apk + c * aqk;
				}
			}
		}
	}
	const D = zeros(n, n);
	for (let i = 0; i < n; i++) {
		D[i]![i] = Math.max(A[i]![i]!, floor) ** power;
	}
	return mul(mul(V, D), transpose(V));
};

const mean = (xs: number[]) => xs.reduce((a, b) => a + b, 0) / xs.length;
const sd = (xs: number[]) => {
	const m = mean(xs);
	return Math.sqrt(mean(xs.map((x) => (x - m) ** 2)));
};
const corr = (xs: number[], ys: number[]) => {
	const mx = mean(xs);
	const my = mean(ys);
	let num = 0;
	let dx = 0;
	let dy = 0;
	for (let i = 0; i < xs.length; i++) {
		num += (xs[i]! - mx) * (ys[i]! - my);
		dx += (xs[i]! - mx) ** 2;
		dy += (ys[i]! - my) ** 2;
	}
	return num / Math.sqrt(dx * dy);
};
const meanVec = (X: Mat) =>
	X[0]!.map((_, j) => X.reduce((total, row) => total + row[j]!, 0) / X.length);
const cov = (X: Mat, Y: Mat): Mat => {
	const mx = meanVec(X);
	const my = meanVec(Y);
	const C = zeros(mx.length, my.length);
	for (let r = 0; r < X.length; r++) {
		for (let i = 0; i < mx.length; i++) {
			for (let j = 0; j < my.length; j++) {
				C[i]![j]! += (X[r]![i]! - mx[i]!) * (Y[r]![j]! - my[j]!);
			}
		}
	}
	return C.map((row) => row.map((x) => x / (X.length - 1)));
};

const quantiles = (values: number[]) => {
	const sorted = values.toSorted((a, b) => a - b);
	return QUANTILES.map((q) => {
		const position = q * (sorted.length - 1);
		const lo = Math.floor(position);
		const hi = Math.min(sorted.length - 1, lo + 1);
		return sorted[lo]! + (position - lo) * (sorted[hi]! - sorted[lo]!);
	});
};
// Strictly increasing and within 0-100, so a table always inverts.
const monotone = (table: number[]) => {
	let prev = -Infinity;
	return table.map((x) => {
		prev = Math.max(prev + 1e-3, helpers.bound(x, 0, 100));
		return prev;
	});
};

// SIMULATION

const makeRng = (seed: number) => {
	let s = seed >>> 0;
	return () => {
		s = (s * 1_664_525 + 1_013_904_223) >>> 0;
		let t = s ^ (s >>> 15);
		t = Math.imul(t, 0x2c1b3c6d) >>> 0;
		t ^= t >>> 12;
		return (t >>> 0) / 4_294_967_296;
	};
};
const withSeed = async <T>(seed: number, f: () => T | Promise<T>) => {
	const spy = vi.spyOn(Math, "random").mockImplementation(makeRng(seed));
	try {
		return await f();
	} finally {
		spy.mockRestore();
	}
};

type Ratings = Record<string, any>;
type Reshape = ((p: { ratings: Ratings[] }) => void) | undefined;

let pid = 0;
const makePlayer = (ratings: Ratings, age: number, tid: number) => ({
	pid: pid++,
	tid,
	firstName: "Player",
	lastName: String(pid),
	born: { year: g.get("season") - age, loc: "" },
	ratings: [{ ...ratings, season: g.get("season"), fuzz: 0 } as Ratings],
	injury: { type: "Healthy", gamesRemaining: 0 },
	value: ratings.ovr,
	valueNoPot: ratings.ovr,
	stats: [],
	jerseyNumber: "0",
	ptModifier: 1,
	rosterOrder: 0,
});

// Every class is drawn from the same seeds whatever the reshaping, and aged
// with the same seeds, so leagues differ by the reshaping and nothing else.
const buildLeague = async (
	league: number,
	reshape: Reshape,
	draftRatings?: Ratings[],
) => {
	const pool: { ratings: Ratings; age: number }[] = [];
	for (let k = 0; k < NUM_COHORTS; k++) {
		const seed = 10_000 * (league + 1) + k;
		const draftClass = await withSeed(seed, () =>
			genPlayersWithoutSaving(g.get("season"), DEFAULT_LEVEL, []),
		);
		for (const [i, p] of draftClass.entries()) {
			const ratings = p.ratings.at(-1) as Ratings;
			draftRatings?.push({ ...ratings });
			if (reshape) {
				await withSeed(seed * 1000 + i + 3, () => reshape(p as any));
			}
			const draftAge = ratings.season - p.born.year;
			await withSeed(seed * 1000 + i + 7, () => {
				for (let age = draftAge + 1; age <= draftAge + k; age++) {
					developSeasonSync(ratings as any, age, DEFAULT_LEVEL, undefined);
				}
			});
			ratings.ovr = ovr(ratings as any);
			ratings.pos = pos(ratings as any);
			ratings.skills = skills(ratings as any);
			pool.push({ ratings, age: draftAge + k });
		}
	}

	// Best 390 to 30 teams, snake draft by ovr; then starters first.
	pool.sort((a, b) => b.ratings.ovr - a.ratings.ovr);
	const teams: ReturnType<typeof makePlayer>[][] = Array.from(
		{ length: NUM_TEAMS },
		() => [],
	);
	pool.slice(0, NUM_TEAMS * ROSTER).forEach((entry, i) => {
		const round = Math.floor(i / NUM_TEAMS);
		const pick = i % NUM_TEAMS;
		const tid = round % 2 === 0 ? pick : NUM_TEAMS - 1 - pick;
		teams[tid]!.push(makePlayer(entry.ratings, entry.age, tid));
	});
	return teams.map((players) => {
		const starters = findStarters(players.map((p) => p.ratings[0]!.pos));
		const ordered = [
			...starters.map((i) => players[i]!),
			...players.filter((_, i) => !starters.includes(i)),
		];
		ordered.forEach((p, i) => {
			p.rosterOrder = i;
		});
		return ordered;
	});
};

// A double round robin; returns the ratings of everyone who played rotation
// minutes.
const simRotation = async (teams: ReturnType<typeof makePlayer>[][]) => {
	const sides: any[] = [];
	for (const [tid, players] of teams.entries()) {
		sides.push(
			await processTeam(
				{
					tid,
					playThroughInjuries: DEFAULT_PLAY_THROUGH_INJURIES,
					depth: undefined,
				},
				{ won: 0, lost: 0, tied: 0, otl: 0, cid: 0, did: 0 },
				players as any,
			),
		);
	}
	const minutes = new Map<number, { min: number; fga: number }>();
	let gid = 1;
	for (let i = 0; i < sides.length; i++) {
		for (let j = 0; j < sides.length; j++) {
			if (i === j) {
				continue;
			}
			const result: any = new GameSim({
				gid: gid++,
				day: 1,
				teams: helpers.deepCopy([sides[i], sides[j]]),
				doPlayByPlay: false,
				homeCourtFactor: 1,
				neutralSite: false,
				allStarGame: false,
				baseInjuryRate: 0,
			} as any).run();
			for (const t of result.team) {
				for (const p of t.player) {
					const total = minutes.get(p.id) ?? { min: 0, fga: 0 };
					total.min += p.stat.min;
					total.fga += p.stat.fga;
					minutes.set(p.id, total);
				}
			}
		}
	}
	return teams
		.flat()
		.filter((p) => {
			const total = minutes.get(p.pid);
			return (
				total !== undefined &&
				total.min >= ROTATION_MINUTES_SIM &&
				total.fga >= 50
			);
		})
		.map((p) => p.ratings[0]!);
};

const buildAndSim = async (reshape: Reshape, draftRatings?: Ratings[]) => {
	const rotation: Ratings[] = [];
	for (let league = 0; league < NUM_LEAGUES; league++) {
		const teams = await buildLeague(league, reshape, draftRatings);
		rotation.push(...(await simRotation(teams)));
	}
	return rotation;
};

const loadRealRotation = async () => {
	const fs = await import(("node" + ":fs") as any);
	const data = JSON.parse(
		fs.readFileSync("data/real-player-data.basketball.json", "utf8"),
	);
	const stats = JSON.parse(
		fs.readFileSync("data/real-player-stats.basketball.json", "utf8"),
	).stats;
	const ratingsBySlugSeason = new Map<string, Ratings>();
	for (const row of data.ratings) {
		ratingsBySlugSeason.set(`${row.slug}_${row.season}`, row);
	}
	const rotation: Ratings[] = [];
	for (const [slug, rows] of Object.entries<any[]>(stats)) {
		// Traded players have a row per team.
		const bySeason = new Map<number, { min: number; fga: number }>();
		for (const row of rows) {
			if (row.playoffs || row.season < 2023 || row.season > 2026) {
				continue;
			}
			const total = bySeason.get(row.season) ?? { min: 0, fga: 0 };
			total.min += row.min ?? 0;
			total.fga += row.fga ?? 0;
			bySeason.set(row.season, total);
		}
		for (const [season, total] of bySeason) {
			const ratings = ratingsBySlugSeason.get(`${slug}_${season}`);
			if (ratings && total.min >= ROTATION_MINUTES_REAL && total.fga >= 50) {
				rotation.push({ ...ratings, ovr: ovr(ratings as any) });
			}
		}
	}
	return rotation;
};

// THE FIT

// A population on normal scores: each rating ranked within the population,
// ties spread as in realisticProspects.ts, then regressed on the body scores.
const normalFit = (population: Ratings[], rng: () => number) => {
	const tables = Object.fromEntries(
		ALL_KEYS.map((key) => [key, quantiles(population.map((r) => r[key]))]),
	) as Record<string, number[]>;
	const score = (key: string, rating: number) =>
		normInv(quantileOf(tables[key]!, rating + rng() - 0.5));
	const body = population.map((r) =>
		BODY_KEYS.map((key) => score(key, r[key])),
	);
	const skill = population.map((r) =>
		SKILL_KEYS.map((key) => score(key, r[key])),
	);
	const bodyCov = cov(body, body);
	const slopes = mul(cov(skill, body), inverse(bodyCov));
	// What the body doesn't explain.
	const rest = add(
		cov(skill, skill),
		mul(mul(slopes, bodyCov), transpose(slopes)),
		-1,
	);
	return { tables, bodyCov, slopes, rest };
};

const skillMeans = (population: Ratings[]) =>
	SKILL_KEYS.map((key) => mean(population.map((r) => r[key])));

const round = (x: number, digits: number) => +x.toFixed(digits);

test.skipIf(!RUN || __SPORT !== "basketball")(
	"realistic draft prospects calibration",
	{ timeout: 3_600_000 },
	async () => {
		resetG();
		await resetCache();
		idb.league = mockIDBLeague();
		g.setWithoutSavingToDB("specializedDraftProspects", false);
		g.setWithoutSavingToDB("realisticDraftProspects", false);
		g.setWithoutSavingToDB("userTids", []);
		g.setWithoutSavingToDB("userTid", 0);
		const rng = makeRng(20_261_029);

		const real = await loadRealRotation();
		const draftRatings: Ratings[] = [];
		const stock = await buildAndSim(undefined, draftRatings);

		const source = normalFit(draftRatings, rng);
		const realFit = normalFit(real, rng);
		let simmed = normalFit(stock, rng);

		// The rotation-level distribution each skill should end up with.
		const realMeans = skillMeans(real);
		const stockMeans = skillMeans(stock);
		const stockSkillTables = Object.fromEntries(
			SKILL_KEYS.map((key) => [key, quantiles(stock.map((r) => r[key]))]),
		);
		const goal = Object.fromEntries(
			SKILL_KEYS.map((key, i) => {
				const gap = realMeans[i]! - stockMeans[i]!;
				if (Math.abs(gap) > SHAPE_GAP) {
					const stretch =
						sd(real.map((r) => r[key])) / sd(stock.map((r) => r[key]));
					return [
						key,
						stockSkillTables[key]!.map(
							(x) => stockMeans[i]! + (x - stockMeans[i]!) * stretch,
						),
					];
				}
				return [key, realFit.tables[key]!.map((x) => x - gap)];
			}),
		) as Record<SkillKey, number[]>;

		let rest = source.rest;
		let target = Object.fromEntries(
			SKILL_KEYS.map((key) => [key, source.tables[key]!]),
		) as Record<SkillKey, number[]>;
		let tables: ReshapeTables | undefined;
		let rotation = stock;

		for (let iteration = 1; iteration <= ROUNDS; iteration++) {
			rest = symPow(add(rest, add(realFit.rest, simmed.rest, -1)), 1, 1e-3);
			target = Object.fromEntries(
				SKILL_KEYS.map((key) => [
					key,
					monotone(
						target[key].map(
							(x, j) => x + goal[key]![j]! - simmed.tables[key]![j]!,
						),
					),
				]),
			) as Record<SkillKey, number[]>;
			const total = add(
				mul(mul(source.slopes, source.bodyCov), transpose(source.slopes)),
				rest,
			);
			tables = {
				source: source.tables as ReshapeTables["source"],
				bodySlopes: source.slopes,
				reshape: mul(symPow(rest, 0.5, 1e-3), symPow(source.rest, -0.5, 1e-3)),
				spread: SKILL_KEYS.map((_, i) => Math.sqrt(total[i]![i]!)),
				target,
			};
			const fixed = tables;
			rotation = await buildAndSim((p) => reshapeProspect(p as any, fixed));
			simmed = normalFit(rotation, rng);
		}

		// WRITE

		const fs = await import(("node" + ":fs") as any);
		const table = (t: readonly number[]) => t.map((x) => round(x, 1));
		const matrix = (M: readonly (readonly number[])[]) =>
			M.map((row) => row.map((x) => round(x, 4)));
		const data = {
			source: Object.fromEntries(
				ALL_KEYS.map((key) => [key, table(tables!.source[key])]),
			),
			bodySlopes: matrix(tables!.bodySlopes),
			reshape: matrix(tables!.reshape),
			spread: tables!.spread.map((x) => round(x, 4)),
			target: Object.fromEntries(
				SKILL_KEYS.map((key) => [key, table(tables!.target[key])]),
			),
		};
		fs.writeFileSync(
			OUT_FILE,
			`// GENERATED by realisticProspectsCalibration.test.ts - rerun it rather than\n// editing by hand. See realisticProspects.ts for what each table is.\nimport type { ReshapeTables } from "./realisticProspects.ts";\n\nexport const REALISTIC_PROSPECTS: ReshapeTables = ${JSON.stringify(data)};\n`,
		);

		// REPORT: stock rotation players, reshaped ones, real ones.

		const populations = { stock, realistic: rotation, real };
		const lines = [
			`${"".padEnd(18)}${Object.keys(populations)
				.map((name) => name.padStart(10))
				.join("")}`,
		];
		const row = (label: string, f: (rs: Ratings[]) => number, digits = 1) =>
			lines.push(
				`${label.padEnd(18)}${Object.values(populations)
					.map((rs) => f(rs).toFixed(digits).padStart(10))
					.join("")}`,
			);
		row("players", (rs) => rs.length, 0);
		row("skill spread", (rs) =>
			mean(
				rs.map((r) => {
					const values = SKILL_KEYS.map((key) => r[key]);
					return Math.max(...values) - Math.min(...values);
				}),
			),
		);
		for (const key of SKILL_KEYS) {
			row(`${key} mean`, (rs) => mean(rs.map((r) => r[key])));
			row(`${key} sd`, (rs) => sd(rs.map((r) => r[key])));
		}
		for (const [a, b] of [
			["tp", "fg"],
			["tp", "ft"],
			["tp", "dnk"],
			["drb", "pss"],
			["spd", "drb"],
			["hgt", "tp"],
		] as const) {
			row(
				`corr ${a},${b}`,
				(rs) =>
					corr(
						rs.map((r) => r[a]),
						rs.map((r) => r[b]),
					),
				2,
			);
		}
		row(
			"tp < 20 %",
			(rs) => (100 * rs.filter((r) => r.tp < 20).length) / rs.length,
		);
		row("ovr mean", (rs) => mean(rs.map((r) => r.ovr)));
		const report = lines.join("\n");
		if (nodeEnv.REALISTIC_PROSPECTS_REPORT) {
			fs.writeFileSync(nodeEnv.REALISTIC_PROSPECTS_REPORT, `${report}\n`);
		}
		console.log(report);

		// The fit has to have actually moved the class toward the real players.
		const tpFg = (rs: Ratings[]) =>
			corr(
				rs.map((r) => r.tp),
				rs.map((r) => r.fg),
			);
		assert.isBelow(tpFg(rotation), tpFg(stock) - 0.3);
	},
);
