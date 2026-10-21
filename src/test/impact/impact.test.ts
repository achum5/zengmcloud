import { test } from "vitest";
import { makeSeededRandom, randInt } from "../../common/random.ts";
import { PHASE, PLAYER } from "../../common/constants.ts";
import { getGameSpread } from "../../common/getGameSpread.ts";
import type { Player } from "../../common/types.ts";
import { idb } from "../../worker/db/index.ts";
import { g, helpers } from "../../worker/util/index.ts";
import {
	game as gameModule,
	phase as phaseModule,
	player,
	season as seasonModule,
} from "../../worker/core/index.ts";
import {
	pregameLineupSynergy,
	synergyCompositeRating,
	synergyForLineup,
	synergyTotal,
} from "../../worker/core/GameSim.basketball/synergy.ts";
import { last } from "../../common/utils.ts";
import {
	IMPACT_RATINGS,
	lineupFeatures,
	NUM_LINEUP_FEATURES,
	playerFeatures,
	shotShares,
	shotWeight,
	type ImpactRatings,
} from "../../worker/core/realisticFrontOffice/playerImpact.ts";
import {
	HOME_COURT_POINTS,
	playerLine,
	rosterPoints,
	type ValueRatings,
} from "../../worker/core/realisticFrontOffice/strength.ts";
import { createArenaLeague } from "../arena/runLeague.ts";
import { play, setOrder, toSimTeam } from "./simTools.ts";

// FITTING THE LINEUP MODEL (worker/core/realisticFrontOffice/playerImpact.ts)
// on this game's own sim. Never part of the normal test run.
//
// 1. Generate, one process per seed: arena league rosters with other players
//    swapped in, so every kind of build gets minutes, playing real games. Every
//    ten-man stint adds a row to running normal equations, and the run also
//    records how often each rotation spot shares the floor with each other one.
//
//      for seed in 1 2 3 4; do
//        IMPACT_GEN=1 IMPACT_SEED=$seed IMPACT_GAMES=50000 IMPACT_OUT=/tmp/impact \
//          npx vitest --run --project basketball src/test/impact/impact.test.ts &
//      done; wait
//
// 2. Fit: IMPACT_FIT=/tmp/impact sums them, solves the ridge regression and
//    writes /tmp/impact/model.json.
//
// 3. Evaluate: IMPACT_EVAL=/tmp/impact plays fresh matchups many times each and
//    compares what the model and team ovr predicted before tip-off with what
//    happened. IMPACT_SWAP=/tmp/impact drops players the two disagree about
//    into the same roster spot and plays it out.

const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};

const importNode = (name: string) => import(/* @vite-ignore */ name);

// IMPACT_FEATURES=ovr describes each player by his overall alone (and its
// square), leaving the lineup terms to say what overall cannot.
const OVR_ONLY = env.IMPACT_FEATURES === "ovr";
const NUM_LINEUP = OVR_ONLY ? 2 * 2 + 1 : NUM_LINEUP_FEATURES;

// Lineup features, then the lineup synergy difference, then home court.
const NUM_FEATURES = NUM_LINEUP + 2;
const SYNERGY = NUM_LINEUP;
const HOME = NUM_LINEUP + 1;
const MAX_SLOTS = 30;

const ratingsOf = (p: Player) => last(p.ratings) as unknown as ImpactRatings;

type Model = {
	coefficients: number[];
	synergy: number;
	home: number;
	pace: number;
	slotMinutes: number[];
	// How often slot k is on the floor while slot j is.
	together: number[][];
};

// An arena league's rosters, plus a pool of other players to swap into them: the
// rest of the league, and draft prospects grown a random number of years so the
// pool holds builds no current roster does.
const buildWorld = async (seed: number) => {
	await createArenaLeague(seed);
	g.setWithoutSavingToDB("phase", PHASE.REGULAR_SEASON);

	const heal = (p: Player) => {
		p.injury = { type: "Healthy", gamesRemaining: 0 };
		return p;
	};
	const bases: Player[][] = [];
	for (let tid = 0; tid < g.get("numTeams"); tid++) {
		bases.push(
			(await idb.cache.players.indexGetAll("playersByTid", tid)).map((p) =>
				heal(helpers.deepCopy(p)),
			),
		);
	}
	const pool: Player[] = bases.flat();
	for (const p of await idb.cache.players.indexGetAll(
		"playersByTid",
		PLAYER.FREE_AGENT,
	)) {
		pool.push(heal(helpers.deepCopy(p)));
	}
	for (const p of await idb.cache.players.indexGetAll(
		"playersByTid",
		PLAYER.UNDRAFTED,
	)) {
		const copy = heal(helpers.deepCopy(p));
		await player.develop(copy, randInt(0, 8));
		await player.updateValues(copy);
		pool.push(copy);
	}
	return { bases, pool };
};

const makeRoster = (
	base: Player[],
	pool: Player[],
	used: Set<number>,
	maxSwaps: number,
) => {
	const roster = base.filter((p) => !used.has(p.pid));
	const numSwaps = randInt(0, maxSwaps);
	for (let i = 0; i < numSwaps && roster.length > 0; i++) {
		let candidate: Player | undefined;
		for (let tries = 0; tries < 20; tries++) {
			const c = pool[randInt(0, pool.length - 1)]!;
			if (!used.has(c.pid) && !roster.includes(c)) {
				candidate = c;
				break;
			}
		}
		if (candidate) {
			roster[randInt(0, roster.length - 1)] = candidate;
		}
	}
	// The order a stock CPU team plays its men in.
	roster.sort((a, b) => b.valueNoPot - a.valueNoPot);
	setOrder(roster);
	for (const p of roster) {
		used.add(p.pid);
	}
	return roster;
};

// Per-player inputs, computed once each.
const makeCaches = () => {
	const features = new Map<number, number[]>();
	const weights = new Map<number, number>();
	const synergy = new Map<
		number,
		{ compositeRating: ReturnType<typeof synergyCompositeRating> }
	>();
	return {
		featuresOf: (p: Player) => {
			let f = features.get(p.pid);
			if (!f) {
				if (OVR_ONLY) {
					const ovr = last(p.ratings).ovr / 100;
					f = [ovr, ovr * ovr];
				} else {
					f = playerFeatures(ratingsOf(p));
				}
				features.set(p.pid, f);
			}
			return f;
		},
		weightOf: (p: Player) => {
			let w = weights.get(p.pid);
			if (w === undefined) {
				w = shotWeight(ratingsOf(p));
				weights.set(p.pid, w);
			}
			return w;
		},
		synergyInputOf: (p: Player) => {
			let x = synergy.get(p.pid);
			if (!x) {
				x = { compositeRating: synergyCompositeRating(last(p.ratings)) };
				synergy.set(p.pid, x);
			}
			return x;
		},
	};
};

type Caches = ReturnType<typeof makeCaches>;

const lineupOf = (caches: Caches, players: Player[]) => {
	const shares = shotShares(players.map(caches.weightOf));
	return lineupFeatures(
		players.map((p, i) => ({
			features: caches.featuresOf(p),
			share: shares[i]!,
		})),
	);
};

test.runIf(env.IMPACT_GEN === "1")(
	"impact: generate games",
	{ timeout: 6 * 60 * 60 * 1000 },
	async () => {
		const seed = Number(env.IMPACT_SEED ?? 1);
		const numGames = Number(env.IMPACT_GAMES ?? 10000);
		const out = env.IMPACT_OUT ?? ".";
		await importNode("fake-indexeddb/auto");
		const realRandom = Math.random;
		Math.random = makeSeededRandom(seed);
		try {
			const { bases, pool } = await buildWorld(seed);
			const caches = makeCaches();
			const xtx = new Float64Array(NUM_FEATURES * NUM_FEATURES);
			const xty = new Float64Array(NUM_FEATURES);
			let yy = 0;
			let numRows = 0;
			let possessions = 0;
			const slotMinutes = new Array<number>(MAX_SLOTS).fill(0);
			const slotGames = new Array<number>(MAX_SLOTS).fill(0);
			const together = Array.from({ length: MAX_SLOTS }, () =>
				new Array<number>(MAX_SLOTS).fill(0),
			);
			const x = new Float64Array(NUM_FEATURES);

			for (let n = 0; n < numGames; n++) {
				const tid0 = randInt(0, bases.length - 1);
				let tid1 = randInt(0, bases.length - 2);
				if (tid1 >= tid0) {
					tid1 += 1;
				}
				const used = new Set<number>();
				const rosters = [
					makeRoster(bases[tid0]!, pool, used, 6),
					makeRoster(bases[tid1]!, pool, used, 6),
				];
				const teams = [
					await toSimTeam(tid0, rosters[0]!),
					await toSimTeam(tid1, rosters[1]!),
				];
				const byPid = rosters.map(
					(roster) => new Map(roster.map((p) => [p.pid, p])),
				);
				const result = play(teams[0]!, teams[1]!);

				for (const k of [0, 1] as const) {
					for (const rp of result.team[k].player) {
						const slot = byPid[k]!.get(rp.id)!.rosterOrder;
						slotMinutes[slot] += rp.stat.min ?? 0;
						slotGames[slot]! += 1;
					}
				}

				// One row per ten-man matchup, so a player is only ever credited
				// with what happened while he was on the floor - garbage time
				// belongs to the benches that played it. Scaled per 100
				// possessions, and weighted by length, since a short stint's
				// margin is mostly noise.
				for (const shift of result.shifts as {
					lineups: [number[], number[]];
					poss: [number, number];
					pts: [number, number];
				}[]) {
					possessions += shift.poss[0] + shift.poss[1];
					const len = (shift.poss[0] + shift.poss[1]) / 200;
					if (len <= 0) {
						continue;
					}
					x.fill(0);
					const syn: number[] = [];
					for (const k of [0, 1] as const) {
						const sign = k === 0 ? 1 : -1;
						const players = shift.lineups[k].map((pid) => byPid[k]!.get(pid)!);
						const f = lineupOf(caches, players);
						for (let i = 0; i < f.length; i++) {
							x[i]! += sign * len * f[i]!;
						}
						syn.push(
							synergyTotal(
								synergyForLineup(players.map(caches.synergyInputOf)),
							),
						);
						for (const a of players) {
							for (const b of players) {
								together[a.rosterOrder]![b.rosterOrder]! += len;
							}
						}
					}
					x[SYNERGY] = len * (syn[0]! - syn[1]!);
					x[HOME] = len;
					const y = shift.pts[0] - shift.pts[1];
					const w = 1 / len;

					// Symmetric, so only the upper triangle; fit mirrors it.
					for (let i = 0; i < NUM_FEATURES; i++) {
						const xi = x[i]!;
						if (xi === 0) {
							continue;
						}
						const wxi = w * xi;
						xty[i]! += wxi * y;
						const row = i * NUM_FEATURES;
						for (let j = i; j < NUM_FEATURES; j++) {
							xtx[row + j]! += wxi * x[j]!;
						}
					}
					yy += w * y * y;
					numRows += 1;
				}
			}

			const fs = await importNode("node:fs/promises");
			await fs.mkdir(out, { recursive: true });
			await fs.writeFile(
				`${out}/gen-${seed}.json`,
				JSON.stringify({
					numGames,
					numRows,
					possessions,
					xtx: [...xtx],
					xty: [...xty],
					yy,
					slotMinutes,
					slotGames,
					together,
				}),
			);
		} finally {
			Math.random = realRandom;
		}
	},
);

// Solves (A + lambda I) x = b for symmetric positive definite A, by Cholesky.
const ridgeSolve = (
	a: Float64Array,
	b: Float64Array,
	n: number,
	lambda: number,
) => {
	const l = new Float64Array(n * n);
	for (let i = 0; i < n; i++) {
		for (let j = 0; j <= i; j++) {
			let sum = a[i * n + j]! + (i === j ? lambda : 0);
			for (let k = 0; k < j; k++) {
				sum -= l[i * n + k]! * l[j * n + k]!;
			}
			if (i === j) {
				if (sum <= 0) {
					throw new Error("Matrix not positive definite");
				}
				l[i * n + i] = Math.sqrt(sum);
			} else {
				l[i * n + j] = sum / l[j * n + j]!;
			}
		}
	}
	const z = new Float64Array(n);
	for (let i = 0; i < n; i++) {
		let sum = b[i]!;
		for (let k = 0; k < i; k++) {
			sum -= l[i * n + k]! * z[k]!;
		}
		z[i] = sum / l[i * n + i]!;
	}
	const x = new Float64Array(n);
	for (let i = n - 1; i >= 0; i--) {
		let sum = z[i]!;
		for (let k = i + 1; k < n; k++) {
			sum -= l[k * n + i]! * x[k]!;
		}
		x[i] = sum / l[i * n + i]!;
	}
	return x;
};

test.runIf(env.IMPACT_FIT !== undefined)("impact: fit", async () => {
	const dir = env.IMPACT_FIT!;
	const lambda = Number(env.IMPACT_LAMBDA ?? 1);
	const fs = await importNode("node:fs/promises");
	const files = (await fs.readdir(dir)).filter((f: string) =>
		/^gen-\d+\.json$/.test(f),
	);
	const xtx = new Float64Array(NUM_FEATURES * NUM_FEATURES);
	const xty = new Float64Array(NUM_FEATURES);
	let yy = 0;
	let numGames = 0;
	let numRows = 0;
	let possessions = 0;
	const slotMinutes = new Array<number>(MAX_SLOTS).fill(0);
	const slotGames = new Array<number>(MAX_SLOTS).fill(0);
	const together = Array.from({ length: MAX_SLOTS }, () =>
		new Array<number>(MAX_SLOTS).fill(0),
	);
	for (const f of files) {
		const d = JSON.parse(await fs.readFile(`${dir}/${f}`, "utf8"));
		d.xtx.forEach((v: number, i: number) => (xtx[i]! += v));
		d.xty.forEach((v: number, i: number) => (xty[i]! += v));
		yy += d.yy;
		numGames += d.numGames;
		numRows += d.numRows;
		possessions += d.possessions;
		d.slotMinutes.forEach((v: number, i: number) => (slotMinutes[i]! += v));
		d.slotGames.forEach((v: number, i: number) => (slotGames[i]! += v));
		d.together.forEach((row: number[], i: number) =>
			row.forEach((v, j) => (together[i]![j]! += v)),
		);
	}
	for (let i = 0; i < NUM_FEATURES; i++) {
		for (let j = 0; j < i; j++) {
			xtx[i * NUM_FEATURES + j] = xtx[j * NUM_FEATURES + i]!;
		}
	}
	const beta = ridgeSolve(xtx, xty, NUM_FEATURES, lambda);

	let rss = yy;
	for (let i = 0; i < NUM_FEATURES; i++) {
		rss -= 2 * beta[i]! * xty[i]!;
		for (let j = 0; j < NUM_FEATURES; j++) {
			rss += beta[i]! * beta[j]! * xtx[i * NUM_FEATURES + j]!;
		}
	}
	const model: Model & Record<string, unknown> = {
		numGames,
		lambda,
		coefficients: Array.from(beta.subarray(0, NUM_LINEUP)),
		synergy: beta[SYNERGY]!,
		home: beta[HOME]!,
		// Per 100 possessions, length-weighted, across stints.
		rmse: Math.sqrt(rss / numRows),
		sdMargin: Math.sqrt(yy / numRows),
		// Possessions per team per game.
		pace: possessions / numGames / 2,
		// Average minutes by place in the rotation, for predicting a game
		// before it is played.
		slotMinutes: slotMinutes.map((m, i) =>
			slotGames[i]! > 0 ? m / slotGames[i]! : 0,
		),
		together: together.map((row, j) =>
			row.map((v) => (together[j]![j]! > 0 ? v / together[j]![j]! : 0)),
		),
	};
	await fs.writeFile(`${dir}/model.json`, JSON.stringify(model));
});

// A roster's margin per game against an average team, before tip-off, from the
// model: each man's minutes, and his share of the shots given who he usually
// shares the floor with. Minutes go by valueNoPot, not by roster order: that is
// what the sim's substitutions rank players on, wherever they are listed.
const predictRoster = (model: Model, caches: Caches, rosterInput: Player[]) => {
	const roster = [...rosterInput].sort((a, b) => b.valueNoPot - a.valueNoPot);
	const weights = roster.map(caches.weightOf);
	let total = 0;
	for (const [j, p] of roster.entries()) {
		const minutes = model.slotMinutes[j] ?? 0;
		if (minutes <= 0) {
			continue;
		}
		let others = 0;
		for (let k = 0; k < roster.length; k++) {
			if (k !== j) {
				others += (model.together[j]?.[k] ?? 0) * weights[k]!;
			}
		}
		const share = weights[j]! / (weights[j]! + others);
		const f = lineupFeatures([{ features: caches.featuresOf(p), share }]);
		let value = 0;
		for (let i = 0; i < f.length; i++) {
			value += f[i]! * model.coefficients[i]!;
		}
		total += (minutes / 48) * value;
	}
	return (total * model.pace) / 100;
};

test.runIf(env.IMPACT_EVAL !== undefined)(
	"impact: evaluate",
	{ timeout: 6 * 60 * 60 * 1000 },
	async () => {
		const dir = env.IMPACT_EVAL!;
		const numMatchups = Number(env.IMPACT_MATCHUPS ?? 300);
		const k = Number(env.IMPACT_K ?? 30);
		const fs = await importNode("node:fs/promises");
		const model: Model = JSON.parse(
			await fs.readFile(`${dir}/model.json`, "utf8"),
		);
		await importNode("fake-indexeddb/auto");
		const realRandom = Math.random;
		const seed = Number(env.IMPACT_SEED ?? 999);
		Math.random = makeSeededRandom(seed);
		try {
			const { bases, pool } = await buildWorld(seed);
			const caches = makeCaches();
			const perGame = model.pace / 100;
			const rows: {
				truth: number;
				ovr: number;
				model: number;
				noiseVar: number;
			}[] = [];
			for (let n = 0; n < numMatchups; n++) {
				const tid0 = randInt(0, bases.length - 1);
				let tid1 = randInt(0, bases.length - 2);
				if (tid1 >= tid0) {
					tid1 += 1;
				}
				const used = new Set<number>();
				const rosters = [
					makeRoster(bases[tid0]!, pool, used, 6),
					makeRoster(bases[tid1]!, pool, used, 6),
				];
				const teams = [
					await toSimTeam(tid0, rosters[0]!),
					await toSimTeam(tid1, rosters[1]!),
				];
				const syn = teams.map((t) => pregameLineupSynergy(t.player) ?? 0);
				const margins: number[] = [];
				for (let i = 0; i < k; i++) {
					const result = play(
						helpers.deepCopy(teams[0]!),
						helpers.deepCopy(teams[1]!),
					);
					margins.push(result.team[0].stat.pts - result.team[1].stat.pts);
				}
				const mean = margins.reduce((a, x) => a + x, 0) / k;
				const variance =
					margins.reduce((a, x) => a + (x - mean) ** 2, 0) / (k - 1);
				rows.push({
					truth: mean,
					noiseVar: variance / k,
					ovr: getGameSpread({
						ovr0: teams[0]!.ovr,
						ovr1: teams[1]!.ovr,
						homeCourtAdvantage: 1,
						neutralSite: false,
						numPeriods: g.get("numPeriods"),
						quarterLength: g.get("quarterLength"),
						synergyDiff: syn[0]! - syn[1]!,
						playoffs: false,
					})!,
					model:
						predictRoster(model, caches, rosters[0]!) -
						predictRoster(model, caches, rosters[1]!) +
						(model.synergy * (syn[0]! - syn[1]!) + model.home) * perGame,
				});
			}
			// Error against the true expected margin: squared error minus the
			// part that is just the mean of k noisy games.
			const noise = rows.reduce((a, r) => a + r.noiseVar, 0) / rows.length;
			const err = (key: "ovr" | "model") =>
				Math.sqrt(
					Math.max(
						0,
						rows.reduce((a, r) => a + (r[key] - r.truth) ** 2, 0) /
							rows.length -
							noise,
					),
				);
			await fs.writeFile(
				`${dir}/eval.json`,
				JSON.stringify({
					numMatchups,
					k,
					noiseFloor: Math.sqrt(noise),
					ovrError: err("ovr"),
					modelError: err("model"),
					rows,
				}),
			);
		} finally {
			Math.random = realRandom;
		}
	},
);

// THE CLAIM BEHIND THE MODEL, tested directly: players the model and overall
// disagree about most, each dropped into the same roster spot on the same team
// and played against the same opponents on the same random numbers. Whoever's
// team does better was the better player there, whatever either number said.
test.runIf(env.IMPACT_SWAP !== undefined)(
	"impact: swap test",
	{ timeout: 6 * 60 * 60 * 1000 },
	async () => {
		const dir = env.IMPACT_SWAP!;
		const numPairs = Number(env.IMPACT_PAIRS ?? 12);
		const gamesPerSide = Number(env.IMPACT_SWAP_GAMES ?? 1500);
		const slot = Number(env.IMPACT_SWAP_SLOT ?? 5);
		const fs = await importNode("node:fs/promises");
		const model: Model = JSON.parse(
			await fs.readFile(`${dir}/model.json`, "utf8"),
		);
		await importNode("fake-indexeddb/auto");
		const realRandom = Math.random;
		const seed = Number(env.IMPACT_SEED ?? 777);
		Math.random = makeSeededRandom(seed);
		try {
			const { bases } = await buildWorld(seed);
			const caches = makeCaches();

			// One middling team as the host, every other team as an opponent.
			const hostTid = 7;
			const host = [...bases[hostTid]!].sort(
				(a, b) => b.valueNoPot - a.valueNoPot,
			);
			const withPlayer = (p: Player) => {
				const roster = [...host.slice(0, slot), p, ...host.slice(slot, 12)];
				setOrder(roster);
				return roster;
			};
			const modelValue = (p: Player) =>
				predictRoster(model, caches, withPlayer(p));

			const candidates = bases
				.flat()
				.filter((p) => p.tid !== hostTid && p.rosterOrder < 10)
				.map((p) => ({ p, ovr: last(p.ratings).ovr, value: modelValue(p) }));

			// Model value on the overall scale, for reporting and for finding the
			// biggest disagreements.
			const n = candidates.length;
			const mo = candidates.reduce((a, x) => a + x.ovr, 0) / n;
			const mv = candidates.reduce((a, x) => a + x.value, 0) / n;
			let sxy = 0;
			let syy = 0;
			for (const x of candidates) {
				sxy += (x.value - mv) * (x.ovr - mo);
				syy += (x.value - mv) ** 2;
			}
			const playsLike = (value: number) => mo + (sxy / syy) * (value - mv);
			const gap = (x: (typeof candidates)[number]) =>
				playsLike(x.value) - x.ovr;

			const bucket = (p: Player) => {
				const pos = last(p.ratings).pos;
				return pos.includes("C") ? "C" : pos.includes("F") ? "F" : "G";
			};
			const usedPids = new Set<number>();
			const pairs: [
				(typeof candidates)[number],
				(typeof candidates)[number],
			][] = [];
			for (const a of [...candidates].sort((x, y) => gap(x) - gap(y))) {
				if (pairs.length >= numPairs) {
					break;
				}
				if (usedPids.has(a.p.pid)) {
					continue;
				}
				// The one at his position the model likes most relative to his
				// overall, among those with a lower overall but a higher model
				// value.
				const b = candidates
					.filter(
						(c) =>
							!usedPids.has(c.p.pid) &&
							c.p.pid !== a.p.pid &&
							bucket(c.p) === bucket(a.p) &&
							c.ovr < a.ovr &&
							c.value > a.value,
					)
					.sort((x, y) => gap(y) - gap(x))[0];
				if (b) {
					pairs.push([a, b]);
					usedPids.add(a.p.pid);
					usedPids.add(b.p.pid);
				}
			}

			const results = [];
			for (const [a, b] of pairs) {
				// IMPACT_SWAP_EQUAL=1 gives both men the same claim on minutes: the
				// sim's substitutions rank by valueNoPot, so each is entered at the
				// value of the man he displaced. That isolates what he does per
				// minute from how many minutes his overall earns him.
				const equalValue = host[slot]?.valueNoPot ?? 0;
				const marginWith = async (p: Player) => {
					const team = await toSimTeam(hostTid, withPlayer(p));
					if (env.IMPACT_SWAP_EQUAL === "1") {
						const sp = team.player.find((q: any) => q.id === p.pid);
						if (sp) {
							sp.valueNoPot = equalValue;
						}
					}
					let total = 0;
					let minutes = 0;
					let plusMinus = 0;
					for (let i = 0; i < gamesPerSide; i++) {
						const oppTid =
							(hostTid + 1 + (i % (bases.length - 1))) % bases.length;
						const opp = bases[oppTid]!.filter(
							(q) => q.pid !== a.p.pid && q.pid !== b.p.pid,
						);
						setOrder(opp);
						const oppTeam = await toSimTeam(oppTid, opp);
						// Common random numbers: game i is played on the same random
						// sequence whichever of the two is in the lineup.
						Math.random = makeSeededRandom(seed * 1_000_003 + i);
						const homeGame = i % 2 === 0;
						const result = homeGame
							? play(helpers.deepCopy(team), oppTeam)
							: play(oppTeam, helpers.deepCopy(team));
						const margin = result.team[0].stat.pts - result.team[1].stat.pts;
						total += homeGame ? margin : -margin;
						const me = result.team[homeGame ? 0 : 1].player.find(
							(q: any) => q.id === p.pid,
						);
						minutes += me?.stat.min ?? 0;
						plusMinus += me?.stat.pm ?? 0;
					}
					return {
						margin: total / gamesPerSide,
						minutes: minutes / gamesPerSide,
						plusMinusPer48: minutes > 0 ? (48 * plusMinus) / minutes : 0,
					};
				};
				const withA = await marginWith(a.p);
				const withB = await marginWith(b.p);
				const per100 = (p: Player) => {
					const roster = [...withPlayer(p)].sort(
						(x, y) => y.valueNoPot - x.valueNoPot,
					);
					const j = roster.indexOf(p);
					const weights = roster.map(caches.weightOf);
					let others = 0;
					for (let k = 0; k < roster.length; k++) {
						if (k !== j) {
							others += (model.together[j]?.[k] ?? 0) * weights[k]!;
						}
					}
					const share = weights[j]! / (weights[j]! + others);
					const f = lineupFeatures([{ features: caches.featuresOf(p), share }]);
					return f.reduce((sum, x, i) => sum + x * model.coefficients[i]!, 0);
				};
				results.push({
					a: {
						name: `${a.p.firstName} ${a.p.lastName}`,
						ovr: a.ovr,
						playsLike: Math.round(playsLike(a.value)),
					},
					b: {
						name: `${b.p.firstName} ${b.p.lastName}`,
						ovr: b.ovr,
						playsLike: Math.round(playsLike(b.value)),
					},
					actual: Math.round((withB.margin - withA.margin) * 100) / 100,
					predicted: Math.round((b.value - a.value) * 100) / 100,
					winner: withB.margin > withA.margin ? "model" : "ovr",
					minutes: [withA.minutes, withB.minutes].map((x) => Math.round(x)),
					plusMinusPer48: [withA.plusMinusPer48, withB.plusMinusPer48].map(
						(x) => Math.round(x * 10) / 10,
					),
					modelPer100: [per100(a.p), per100(b.p)].map(
						(x) => Math.round(x * 10) / 10,
					),
				});
			}
			await fs.writeFile(
				`${dir}/swap.json`,
				JSON.stringify(
					{
						gamesPerSide,
						slot,
						results,
						leaderboard: candidates
							.map((x) => ({
								name: `${x.p.firstName} ${x.p.lastName}`,
								age: g.get("season") - x.p.born.year,
								pos: last(x.p.ratings).pos,
								ovr: x.ovr,
								playsLike: Math.round(playsLike(x.value)),
							}))
							.sort((x, y) => y.playsLike - x.playsLike),
					},
					null,
					1,
				),
			);
		} finally {
			Math.random = realRandom;
		}
	},
);

// DOES PRODUCTION BEAT OVERALL? The owner's experience is that some players
// produce far above their overall and others far below, and that the stock AI
// misses it. Box-score production is collected over a block of ordinary games,
// then players are paired where production and overall disagree most, and each
// pair is played out at an equal claim on minutes, as in the swap test.
test.runIf(env.IMPACT_PRODUCTION !== undefined)(
	"impact: production vs overall",
	{ timeout: 6 * 60 * 60 * 1000 },
	async () => {
		const dir = env.IMPACT_PRODUCTION!;
		const numPairs = Number(env.IMPACT_PAIRS ?? 16);
		const gamesPerSide = Number(env.IMPACT_SWAP_GAMES ?? 800);
		const sampleGames = Number(env.IMPACT_SAMPLE_GAMES ?? 4000);
		const slot = 5;
		const fs = await importNode("node:fs/promises");
		await importNode("fake-indexeddb/auto");
		const realRandom = Math.random;
		const seed = Number(env.IMPACT_SEED ?? 555);
		Math.random = makeSeededRandom(seed);
		try {
			const { bases } = await buildWorld(seed);
			for (const roster of bases) {
				roster.sort((a, b) => b.valueNoPot - a.valueNoPot);
				setOrder(roster);
			}

			// Game score per 48 minutes over a block of real-roster games.
			const totals = new Map<number, { min: number; gmsc: number }>();
			for (let n = 0; n < sampleGames; n++) {
				const tid0 = randInt(0, bases.length - 1);
				let tid1 = randInt(0, bases.length - 2);
				if (tid1 >= tid0) {
					tid1 += 1;
				}
				const result = play(
					await toSimTeam(tid0, bases[tid0]!),
					await toSimTeam(tid1, bases[tid1]!),
				);
				for (const k of [0, 1]) {
					for (const p of result.team[k].player) {
						const st = p.stat;
						const gmsc =
							st.pts +
							0.4 * st.fg -
							0.7 * st.fga -
							0.4 * (st.fta - st.ft) +
							0.7 * st.orb +
							0.3 * st.drb +
							st.stl +
							0.7 * st.ast +
							0.7 * st.blk -
							0.4 * st.pf -
							st.tov;
						const t = totals.get(p.id) ?? { min: 0, gmsc: 0 };
						t.min += st.min;
						t.gmsc += gmsc;
						totals.set(p.id, t);
					}
				}
			}

			const hostTid = 7;
			const host = bases[hostTid]!;
			const candidates = bases
				.flat()
				.filter((p) => p.tid !== hostTid)
				.map((p) => {
					const t = totals.get(p.pid);
					return {
						p,
						ovr: last(p.ratings).ovr,
						minutes: t?.min ?? 0,
						production: t && t.min > 0 ? (48 * t.gmsc) / t.min : 0,
					};
				})
				.filter((x) => x.minutes >= 200 * (sampleGames / 300));

			// Production on the overall scale, so a gap means something.
			const n = candidates.length;
			const mo = candidates.reduce((a, x) => a + x.ovr, 0) / n;
			const mp = candidates.reduce((a, x) => a + x.production, 0) / n;
			let sxy = 0;
			let syy = 0;
			for (const x of candidates) {
				sxy += (x.production - mp) * (x.ovr - mo);
				syy += (x.production - mp) ** 2;
			}
			const producesLike = (x: (typeof candidates)[number]) =>
				mo + (sxy / syy) * (x.production - mp);
			const gap = (x: (typeof candidates)[number]) => producesLike(x) - x.ovr;
			const bucket = (p: Player) => {
				const pos = last(p.ratings).pos;
				return pos.includes("C") ? "C" : pos.includes("F") ? "F" : "G";
			};
			const usedPids = new Set<number>();
			const pairs: [
				(typeof candidates)[number],
				(typeof candidates)[number],
			][] = [];
			for (const a of [...candidates].sort((x, y) => gap(x) - gap(y))) {
				if (pairs.length >= numPairs) {
					break;
				}
				if (usedPids.has(a.p.pid)) {
					continue;
				}
				const b = candidates
					.filter(
						(c) =>
							!usedPids.has(c.p.pid) &&
							bucket(c.p) === bucket(a.p) &&
							c.ovr < a.ovr &&
							c.production > a.production,
					)
					.sort((x, y) => gap(y) - gap(x))[0];
				if (b) {
					pairs.push([a, b]);
					usedPids.add(a.p.pid);
					usedPids.add(b.p.pid);
				}
			}

			const equalValue = host[slot]?.valueNoPot ?? 0;
			const marginWith = async (p: Player, avoid: number[]) => {
				const roster = [...host.slice(0, slot), p, ...host.slice(slot, 12)];
				setOrder(roster);
				const team = await toSimTeam(hostTid, roster);
				const sp = team.player.find((q: any) => q.id === p.pid);
				if (sp) {
					sp.valueNoPot = equalValue;
				}
				let total = 0;
				for (let i = 0; i < gamesPerSide; i++) {
					const oppTid =
						(hostTid + 1 + (i % (bases.length - 1))) % bases.length;
					const opp = bases[oppTid]!.filter((q) => !avoid.includes(q.pid));
					setOrder(opp);
					const oppTeam = await toSimTeam(oppTid, opp);
					Math.random = makeSeededRandom(seed * 1_000_003 + i);
					const homeGame = i % 2 === 0;
					const result = homeGame
						? play(helpers.deepCopy(team), oppTeam)
						: play(oppTeam, helpers.deepCopy(team));
					const margin = result.team[0].stat.pts - result.team[1].stat.pts;
					total += homeGame ? margin : -margin;
				}
				for (const [i, q] of roster.entries()) {
					q.rosterOrder = i;
				}
				return total / gamesPerSide;
			};
			const results = [];
			for (const [a, b] of pairs) {
				const avoid = [a.p.pid, b.p.pid];
				const withA = await marginWith(a.p, avoid);
				const withB = await marginWith(b.p, avoid);
				results.push({
					a: {
						name: `${a.p.firstName} ${a.p.lastName}`,
						ovr: a.ovr,
						producesLike: Math.round(producesLike(a)),
					},
					b: {
						name: `${b.p.firstName} ${b.p.lastName}`,
						ovr: b.ovr,
						producesLike: Math.round(producesLike(b)),
					},
					actual: Math.round((withB - withA) * 100) / 100,
					winner: withB > withA ? "production" : "ovr",
				});
				// Restore the opponents' order for the next pair.
				for (const roster of bases) {
					setOrder(roster);
				}
			}
			await fs.writeFile(
				`${dir}/production.json`,
				JSON.stringify({ gamesPerSide, results }, null, 1),
			);
		} finally {
			Math.random = realRandom;
		}
	},
);

// WHAT A BUILD IS WORTH BEYOND ITS OVERALL, measured directly. Each player in a
// broad sample is dropped into the same rotation spot on one of a few host
// teams, at the same claim on minutes as the man he displaces, and played
// against rotating opponents on fixed random numbers - the same numbers the
// host played on without him. The difference is what he adds per game in that
// role, net of everything else. One process per seed:
//
//   IMPACT_CALIBRATE=/tmp/calib IMPACT_SEED=1 IMPACT_PLAYERS=250 \
//     npx vitest --run --project basketball src/test/impact/impact.test.ts
test.runIf(env.IMPACT_CALIBRATE !== undefined)(
	"impact: calibrate builds",
	{ timeout: 6 * 60 * 60 * 1000 },
	async () => {
		const dir = env.IMPACT_CALIBRATE!;
		const numPlayers = Number(env.IMPACT_PLAYERS ?? 250);
		const gamesPerPlayer = Number(env.IMPACT_SWAP_GAMES ?? 400);
		const gameSeedOffset = Number(env.IMPACT_GAME_SEED ?? 0);
		const slot = Number(env.IMPACT_SLOT ?? 5);
		const minOvr = Number(env.IMPACT_MIN_OVR ?? 35);
		const fs = await importNode("node:fs/promises");
		await importNode("fake-indexeddb/auto");
		const realRandom = Math.random;
		// Same league for every process; only the sample of players differs.
		Math.random = makeSeededRandom(4242);
		try {
			const { bases, pool } = await buildWorld(4242);
			for (const roster of bases) {
				roster.sort((a, b) => b.valueNoPot - a.valueNoPot);
				setOrder(roster);
			}
			const hostTids = [3, 7, 12, 18, 25];
			const hostPids = new Set(
				hostTids.flatMap((tid) => bases[tid]!.map((p) => p.pid)),
			);

			const playHost = async (hostTid: number, insert: Player | undefined) => {
				const host = bases[hostTid]!;
				const roster = insert
					? [...host.slice(0, slot), insert, ...host.slice(slot + 1, 13)]
					: host.slice(0, 13);
				setOrder(roster);
				const team = await toSimTeam(hostTid, roster);
				if (insert) {
					const sp = team.player.find((q: any) => q.id === insert.pid);
					if (sp) {
						sp.valueNoPot = host[slot]!.valueNoPot;
					}
				}
				let total = 0;
				for (let i = 0; i < gamesPerPlayer; i++) {
					const oppTid =
						(hostTid + 1 + (i % (bases.length - 1))) % bases.length;
					const opp = bases[oppTid]!.filter((q) => q.pid !== insert?.pid);
					setOrder(opp);
					const oppTeam = await toSimTeam(oppTid, opp);
					// IMPACT_GAME_SEED replays a sample on different games, to measure
					// how much of a player's number is the games he happened to get.
					Math.random = makeSeededRandom(
						hostTid * 1_000_003 + i + gameSeedOffset,
					);
					const homeGame = i % 2 === 0;
					const result = homeGame
						? play(helpers.deepCopy(team), oppTeam)
						: play(oppTeam, helpers.deepCopy(team));
					const margin = result.team[0].stat.pts - result.team[1].stat.pts;
					total += homeGame ? margin : -margin;
				}
				setOrder(bases[hostTid]!);
				return total / gamesPerPlayer;
			};

			const baselines = new Map<number, number>();
			for (const tid of hostTids) {
				baselines.set(tid, await playHost(tid, undefined));
			}

			// The sample: anyone not on a host team, drawn on this process's own
			// stream so processes measure different players.
			const pick = makeSeededRandom(Number(env.IMPACT_SEED ?? 1));
			const eligible = pool.filter(
				(p) => !hostPids.has(p.pid) && last(p.ratings).ovr >= minOvr,
			);
			const rows = [];
			for (let n = 0; n < numPlayers; n++) {
				const p = eligible[Math.floor(pick() * eligible.length)]!;
				const hostTid = hostTids[n % hostTids.length]!;
				const margin = await playHost(hostTid, p);
				const r = last(p.ratings);
				rows.push({
					pid: p.pid,
					age: g.get("season") - p.born.year,
					ovr: r.ovr,
					pos: r.pos,
					ratings: Object.fromEntries(
						(Object.keys(r) as (keyof typeof r)[])
							.filter((k) => typeof r[k] === "number")
							.map((k) => [k, r[k]]),
					),
					hostTid,
					slot,
					displacedOvr: last(bases[hostTid]![slot]!.ratings).ovr,
					delta: margin - baselines.get(hostTid)!,
				});
			}
			await fs.mkdir(dir, { recursive: true });
			await fs.writeFile(
				`${dir}/calib-${env.IMPACT_SEED ?? 1}${slot !== 5 ? `-slot${slot}` : ""}${gameSeedOffset ? `-replay${gameSeedOffset}` : ""}.json`,
				JSON.stringify(rows),
			);
		} finally {
			Math.random = realRandom;
		}
	},
);

// THE OWNER'S CASE: similar overalls, very different BPM. One regular season of
// the arena league is played first so everyone has a BPM, then players at the
// same position are paired where BPM says one thing and overall another, and
// each pair is played out at an equal claim on minutes.
test.runIf(env.IMPACT_BPM !== undefined)(
	"impact: bpm vs overall",
	{ timeout: 6 * 60 * 60 * 1000 },
	async () => {
		const dir = env.IMPACT_BPM!;
		const numPairs = Number(env.IMPACT_PAIRS ?? 16);
		const gamesPerSide = Number(env.IMPACT_SWAP_GAMES ?? 800);
		const slot = 5;
		const fs = await importNode("node:fs/promises");
		await importNode("fake-indexeddb/auto");
		const realRandom = Math.random;
		const seed = Number(env.IMPACT_SEED ?? 321);
		Math.random = makeSeededRandom(seed);
		try {
			await createArenaLeague(seed);
			await phaseModule.newPhase(PHASE.REGULAR_SEASON, {} as any);
			await gameModule.play(
				await seasonModule.getDaysLeftSchedule(),
				{} as any,
			);
			const season = g.get("season");
			const players = await idb.cache.players.indexGetAll("playersByTid", [
				0,
				Infinity,
			]);
			const seasonRow = (p: Player) =>
				p.stats.find((row) => row.season === season && !row.playoffs) as
					| any
					| undefined;
			const bases: Player[][] = [];
			for (let tid = 0; tid < g.get("numTeams"); tid++) {
				const roster = players
					.filter((p) => p.tid === tid)
					.map((p) => {
						const copy = helpers.deepCopy(p);
						copy.injury = { type: "Healthy", gamesRemaining: 0 };
						return copy;
					})
					.sort((a, b) => b.valueNoPot - a.valueNoPot);
				setOrder(roster);
				bases.push(roster);
			}
			g.setWithoutSavingToDB("phase", PHASE.REGULAR_SEASON);

			const hostTid = 7;
			const host = bases[hostTid]!;
			const candidates = bases
				.flat()
				.filter((p) => p.tid !== hostTid)
				.map((p) => {
					const row = seasonRow(p);
					return {
						p,
						ovr: last(p.ratings).ovr,
						min: row?.min ?? 0,
						bpm: (row?.obpm ?? 0) + (row?.dbpm ?? 0),
					};
				})
				.filter((x) => x.min >= 1000);
			const bucket = (p: Player) => {
				const pos = last(p.ratings).pos;
				return pos.includes("C") ? "C" : pos.includes("F") ? "F" : "G";
			};

			// Pairs: same position, the higher-BPM man no better than 3 overall
			// points above the other and at least 4 BPM better - biggest BPM gaps
			// relative to overall first.
			const pairsAll: {
				hi: (typeof candidates)[number];
				lo: (typeof candidates)[number];
				score: number;
			}[] = [];
			for (const hi of candidates) {
				for (const lo of candidates) {
					if (
						hi !== lo &&
						bucket(hi.p) === bucket(lo.p) &&
						hi.bpm - lo.bpm >= 4 &&
						hi.ovr - lo.ovr <= 0 &&
						lo.ovr - hi.ovr <= 4
					) {
						pairsAll.push({
							hi,
							lo,
							score: hi.bpm - lo.bpm + 2 * (lo.ovr - hi.ovr),
						});
					}
				}
			}
			pairsAll.sort((a, b) => b.score - a.score);
			const used = new Set<number>();
			const pairs = [];
			for (const pair of pairsAll) {
				if (pairs.length >= numPairs) {
					break;
				}
				if (used.has(pair.hi.p.pid) || used.has(pair.lo.p.pid)) {
					continue;
				}
				used.add(pair.hi.p.pid);
				used.add(pair.lo.p.pid);
				pairs.push(pair);
			}

			const equalValue = host[slot]?.valueNoPot ?? 0;
			const marginWith = async (p: Player, avoid: number[]) => {
				const roster = [...host.slice(0, slot), p, ...host.slice(slot, 12)];
				setOrder(roster);
				const team = await toSimTeam(hostTid, roster);
				const sp = team.player.find((q: any) => q.id === p.pid);
				if (sp) {
					sp.valueNoPot = equalValue;
				}
				let total = 0;
				for (let i = 0; i < gamesPerSide; i++) {
					const oppTid =
						(hostTid + 1 + (i % (bases.length - 1))) % bases.length;
					const opp = bases[oppTid]!.filter((q) => !avoid.includes(q.pid));
					setOrder(opp);
					const oppTeam = await toSimTeam(oppTid, opp);
					Math.random = makeSeededRandom(seed * 1_000_003 + i);
					const homeGame = i % 2 === 0;
					const result = homeGame
						? play(helpers.deepCopy(team), oppTeam)
						: play(oppTeam, helpers.deepCopy(team));
					const margin = result.team[0].stat.pts - result.team[1].stat.pts;
					total += homeGame ? margin : -margin;
				}
				setOrder(host);
				for (const roster2 of bases) {
					setOrder(roster2);
				}
				return total / gamesPerSide;
			};
			const results = [];
			for (const { hi, lo } of pairs) {
				const avoid = [hi.p.pid, lo.p.pid];
				const withHi = await marginWith(hi.p, avoid);
				const withLo = await marginWith(lo.p, avoid);
				results.push({
					highBpm: {
						name: `${hi.p.firstName} ${hi.p.lastName}`,
						age: season - hi.p.born.year,
						ovr: hi.ovr,
						bpm: Math.round(hi.bpm * 10) / 10,
					},
					lowBpm: {
						name: `${lo.p.firstName} ${lo.p.lastName}`,
						age: season - lo.p.born.year,
						ovr: lo.ovr,
						bpm: Math.round(lo.bpm * 10) / 10,
					},
					highBpmBetterBy: Math.round((withHi - withLo) * 100) / 100,
				});
			}
			await fs.mkdir(dir, { recursive: true });
			await fs.writeFile(
				`${dir}/bpm.json`,
				JSON.stringify({ gamesPerSide, results }, null, 1),
			);
		} finally {
			Math.random = realRandom;
		}
	},
);

const strengthPlayer = (p: Player, valueNoPot = p.valueNoPot) => {
	const r = last(p.ratings);
	return {
		...playerLine(r as unknown as ValueRatings),
		valueNoPot,
	};
};

// What the strength model says replacing the man in roster spot `slot` with p
// is worth, in points a game.
const modelDelta = (host: Player[], slot: number, p: Player) =>
	rosterPoints([
		...host.slice(0, slot).map((q) => strengthPlayer(q)),
		strengthPlayer(p, host[slot]!.valueNoPot),
		...host.slice(slot + 1, 13).map((q) => strengthPlayer(q)),
	]) - rosterPoints(host.slice(0, 13).map((q) => strengthPlayer(q)));

// The strength model against the calibration swaps: IMPACT_MODELCHECK=<the
// IMPACT_CALIBRATE directory> writes modelcheck.json, one row per swap.
test.runIf(env.IMPACT_MODELCHECK !== undefined)(
	"impact: strength model vs calibration swaps",
	{ timeout: 60 * 60 * 1000 },
	async () => {
		const dir = env.IMPACT_MODELCHECK!;
		const fs = await importNode("node:fs/promises");
		await importNode("fake-indexeddb/auto");
		const realRandom = Math.random;
		Math.random = makeSeededRandom(4242);
		try {
			const { bases, pool } = await buildWorld(4242);
			for (const roster of bases) {
				roster.sort((a, b) => b.valueNoPot - a.valueNoPot);
			}
			const byPid = new Map(pool.map((p) => [p.pid, p]));
			const synergyOf = async (tid: number, roster: Player[]) => {
				setOrder(roster);
				const team = await toSimTeam(tid, roster);
				return pregameLineupSynergy(team.player) ?? 0;
			};
			const hostSynergy = new Map<number, number>();
			const files = (await fs.readdir(dir)).filter((f: string) =>
				/^calib-\d+(-slot\d+)?\.json$/.test(f),
			);
			const out = [];
			for (const file of files) {
				const rows = JSON.parse(await fs.readFile(`${dir}/${file}`, "utf8"));
				for (const row of rows) {
					const p = byPid.get(row.pid)!;
					const host = bases[row.hostTid]!;
					const slot = row.slot ?? 5;
					if (!hostSynergy.has(row.hostTid)) {
						hostSynergy.set(
							row.hostTid,
							await synergyOf(row.hostTid, host.slice(0, 13)),
						);
					}
					const insertSynergy = await synergyOf(row.hostTid, [
						...host.slice(0, slot),
						p,
						...host.slice(slot + 1, 13),
					]);
					setOrder(host);
					out.push({
						...row,
						ratings: undefined,
						pred: modelDelta(host, slot, p),
						synergyDelta: insertSynergy - hostSynergy.get(row.hostTid)!,
					});
				}
			}
			await fs.writeFile(`${dir}/modelcheck.json`, JSON.stringify(out));
			// The host rosters as the model sees them (valueNoPot, overall, then
			// the ratings), so team-level fits can be tried offline.
			const rosterRows = (roster: Player[]) =>
				roster.map((p) => {
					const r = ratingsOf(p);
					return [
						Math.round(p.valueNoPot * 100) / 100,
						last(p.ratings).ovr,
						...IMPACT_RATINGS.map((key) => r[key]),
					];
				});
			await fs.writeFile(
				`${dir}/hosts.json`,
				JSON.stringify(
					Object.fromEntries(
						[...hostSynergy.keys()].map((tid) => [
							tid,
							{
								roster: rosterRows(bases[tid]!.slice(0, 13)),
								synergy: hostSynergy.get(tid),
							},
						]),
					),
				),
			);
		} finally {
			Math.random = realRandom;
		}
	},
);

// Does a season's box score know anything the ratings do not? Plays one regular
// season of the arena league, then measures every player with enough minutes
// the way IMPACT_CALIBRATE does - dropped into the same spot of the same host
// rosters - and records his BPM beside the result. Split across processes with
// IMPACT_PART=0..IMPACT_PARTS-1.
test.runIf(env.IMPACT_BPM_CALIB !== undefined)(
	"impact: bpm calibration",
	{ timeout: 6 * 60 * 60 * 1000 },
	async () => {
		const dir = env.IMPACT_BPM_CALIB!;
		const part = Number(env.IMPACT_PART ?? 0);
		const parts = Number(env.IMPACT_PARTS ?? 1);
		const gamesPerPlayer = Number(env.IMPACT_SWAP_GAMES ?? 400);
		const minMinutes = Number(env.IMPACT_MIN ?? 500);
		const slot = 5;
		const fs = await importNode("node:fs/promises");
		await importNode("fake-indexeddb/auto");
		const realRandom = Math.random;
		const seed = Number(env.IMPACT_SEED ?? 321);
		Math.random = makeSeededRandom(seed);
		try {
			await createArenaLeague(seed);
			await phaseModule.newPhase(PHASE.REGULAR_SEASON, {} as any);
			await gameModule.play(
				await seasonModule.getDaysLeftSchedule(),
				{} as any,
			);
			const season = g.get("season");
			const players = await idb.cache.players.indexGetAll("playersByTid", [
				0,
				Infinity,
			]);
			const bases: Player[][] = [];
			for (let tid = 0; tid < g.get("numTeams"); tid++) {
				const roster = players
					.filter((p) => p.tid === tid)
					.map((p) => {
						const copy = helpers.deepCopy(p);
						copy.injury = { type: "Healthy", gamesRemaining: 0 };
						return copy;
					})
					.sort((a, b) => b.valueNoPot - a.valueNoPot);
				setOrder(roster);
				bases.push(roster);
			}
			g.setWithoutSavingToDB("phase", PHASE.REGULAR_SEASON);

			const hostTids = [3, 7, 12, 18, 25];
			const playHost = async (hostTid: number, insert: Player | undefined) => {
				const host = bases[hostTid]!;
				const roster = insert
					? [...host.slice(0, slot), insert, ...host.slice(slot + 1, 13)]
					: host.slice(0, 13);
				setOrder(roster);
				const team = await toSimTeam(hostTid, roster);
				if (insert) {
					const sp = team.player.find((q: any) => q.id === insert.pid);
					if (sp) {
						sp.valueNoPot = host[slot]!.valueNoPot;
					}
				}
				let total = 0;
				for (let i = 0; i < gamesPerPlayer; i++) {
					const oppTid =
						(hostTid + 1 + (i % (bases.length - 1))) % bases.length;
					const opp = bases[oppTid]!.filter((q) => q.pid !== insert?.pid);
					setOrder(opp);
					const oppTeam = await toSimTeam(oppTid, opp);
					Math.random = makeSeededRandom(hostTid * 1_000_003 + i);
					const homeGame = i % 2 === 0;
					const result = homeGame
						? play(helpers.deepCopy(team), oppTeam)
						: play(oppTeam, helpers.deepCopy(team));
					const margin = result.team[0].stat.pts - result.team[1].stat.pts;
					total += homeGame ? margin : -margin;
				}
				for (const roster2 of bases) {
					setOrder(roster2);
				}
				return total / gamesPerPlayer;
			};
			const baselines = new Map<number, number>();
			for (const tid of hostTids) {
				baselines.set(tid, await playHost(tid, undefined));
			}

			const box = (p: Player) => {
				let min = 0;
				let bpm = 0;
				for (const row of p.stats as any[]) {
					if (row.season === season && !row.playoffs && row.min > 0) {
						min += row.min;
						bpm += (row.obpm + row.dbpm) * row.min;
					}
				}
				return { min, bpm: min > 0 ? bpm / min : 0 };
			};
			const eligible = bases
				.flat()
				.filter((p) => !hostTids.includes(p.tid) && box(p).min >= minMinutes)
				.sort((a, b) => a.pid - b.pid)
				.filter((_, i) => i % parts === part);
			const rows = [];
			for (const [n, p] of eligible.entries()) {
				const hostTid = hostTids[(n * parts + part) % hostTids.length]!;
				const margin = await playHost(hostTid, p);
				const r = last(p.ratings);
				rows.push({
					pid: p.pid,
					name: `${p.firstName} ${p.lastName}`,
					age: season - p.born.year,
					ovr: r.ovr,
					pos: r.pos,
					ratings: Object.fromEntries(
						(Object.keys(r) as (keyof typeof r)[])
							.filter((k) => typeof r[k] === "number")
							.map((k) => [k, r[k]]),
					),
					...box(p),
					hostTid,
					displacedOvr: last(bases[hostTid]![slot]!.ratings).ovr,
					pred: modelDelta(bases[hostTid]!, slot, p),
					delta: margin - baselines.get(hostTid)!,
				});
			}
			await fs.mkdir(dir, { recursive: true });
			await fs.writeFile(`${dir}/bpmcalib-${part}.json`, JSON.stringify(rows));
		} finally {
			Math.random = realRandom;
		}
	},
);

// The strength model against whole matchups: league rosters with players swapped
// in, each matchup played IMPACT_K times. IMPACT_EVAL_STRENGTH=<dir> writes
// eval-strength-<seed>.json, with stock's pregame spread alongside.
test.runIf(env.IMPACT_EVAL_STRENGTH !== undefined)(
	"impact: evaluate strength model",
	{ timeout: 6 * 60 * 60 * 1000 },
	async () => {
		const dir = env.IMPACT_EVAL_STRENGTH!;
		const numMatchups = Number(env.IMPACT_MATCHUPS ?? 300);
		const k = Number(env.IMPACT_K ?? 30);
		const fs = await importNode("node:fs/promises");
		await importNode("fake-indexeddb/auto");
		const realRandom = Math.random;
		const seed = Number(env.IMPACT_SEED ?? 999);
		Math.random = makeSeededRandom(seed);
		try {
			const { bases, pool } = await buildWorld(seed);
			const rows = [];
			for (let n = 0; n < numMatchups; n++) {
				const tid0 = randInt(0, bases.length - 1);
				let tid1 = randInt(0, bases.length - 2);
				if (tid1 >= tid0) {
					tid1 += 1;
				}
				const used = new Set<number>();
				const rosters = [
					makeRoster(bases[tid0]!, pool, used, 6),
					makeRoster(bases[tid1]!, pool, used, 6),
				];
				const teams = [
					await toSimTeam(tid0, rosters[0]!),
					await toSimTeam(tid1, rosters[1]!),
				];
				const syn = teams.map((t) => pregameLineupSynergy(t.player) ?? 0);
				const margins: number[] = [];
				for (let i = 0; i < k; i++) {
					const result = play(
						helpers.deepCopy(teams[0]!),
						helpers.deepCopy(teams[1]!),
					);
					margins.push(result.team[0].stat.pts - result.team[1].stat.pts);
				}
				const mean = margins.reduce((a, x) => a + x, 0) / k;
				const variance =
					margins.reduce((a, x) => a + (x - mean) ** 2, 0) / (k - 1);
				rows.push({
					truth: mean,
					noiseVar: variance / k,
					ovr: getGameSpread({
						ovr0: teams[0]!.ovr,
						ovr1: teams[1]!.ovr,
						homeCourtAdvantage: 1,
						neutralSite: false,
						numPeriods: g.get("numPeriods"),
						quarterLength: g.get("quarterLength"),
						synergyDiff: syn[0]! - syn[1]!,
						playoffs: false,
					})!,
					strength:
						rosterPoints(rosters[0]!.map((p) => strengthPlayer(p))) -
						rosterPoints(rosters[1]!.map((p) => strengthPlayer(p))) +
						HOME_COURT_POINTS,
					synergy: syn,
					// Each roster as the model sees it, so team-level fits can be
					// tried offline: valueNoPot, overall, then the ratings.
					rosters: rosters.map((roster) =>
						roster.map((p) => {
							const r = ratingsOf(p);
							return [
								Math.round(p.valueNoPot * 100) / 100,
								last(p.ratings).ovr,
								...IMPACT_RATINGS.map((key) => r[key]),
							];
						}),
					),
				});
			}
			await fs.mkdir(dir, { recursive: true });
			await fs.writeFile(
				`${dir}/eval-strength-${seed}.json`,
				JSON.stringify({ numMatchups, k, rows }),
			);
		} finally {
			Math.random = realRandom;
		}
	},
);
