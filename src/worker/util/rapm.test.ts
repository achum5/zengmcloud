import { assert, describe, test } from "vitest";
import { computeRapm, type RapmStint } from "./rapm.ts";

// A seeded generator, so a failure here is always the same failure.
const makeRandom = (seed: number) => {
	let state = seed >>> 0;
	const next = () => {
		state = (state * 1664525 + 1013904223) >>> 0;
		return state / 4294967296;
	};
	return {
		next,
		normal: () => {
			const u = Math.max(next(), 1e-12);
			return Math.sqrt(-2 * Math.log(u)) * Math.cos(2 * Math.PI * next());
		},
		int: (n: number) => Math.floor(next() * n),
	};
};

const correlation = (a: number[], b: number[]) => {
	const n = a.length;
	const meanA = a.reduce((sum, x) => sum + x, 0) / n;
	const meanB = b.reduce((sum, x) => sum + x, 0) / n;
	let cov = 0;
	let varA = 0;
	let varB = 0;
	for (let i = 0; i < n; i++) {
		const da = a[i]! - meanA;
		const db = b[i]! - meanB;
		cov += da * db;
		varA += da * da;
		varB += db * db;
	}
	return cov / Math.sqrt(varA * varB);
};

type FakePlayer = {
	key: string;
	off: number;
	def: number;
	// Share of his team's possessions.
	share: number;
};

// A season of made-up basketball where every player's true impact is known,
// rotations are lopsided the way real ones are, and the score is generated from
// exactly the model RAPM assumes. If it cannot recover the truth here, it can
// never recover it anywhere.
const fakeSeason = ({
	seed = 1,
	numTeams = 30,
	gamesPerTeam = 82,
	possPerGame = 100,
	possPerStint = 6,
	// Points per possession has a standard deviation a shade over one, so a
	// stint's score is mostly noise. That is the real problem RAPM faces.
	noise = 1.1,
}: {
	seed?: number;
	numTeams?: number;
	gamesPerTeam?: number;
	possPerGame?: number;
	possPerStint?: number;
	noise?: number;
} = {}) => {
	const random = makeRandom(seed);

	const teams: FakePlayer[][] = [];
	for (let tid = 0; tid < numTeams; tid++) {
		const players: FakePlayer[] = [];
		for (let i = 0; i < 10; i++) {
			players.push({
				key: `${tid}-${i}`,
				off: 2 * random.normal(),
				def: 2 * random.normal(),
				// Starters play about twice a reserve's minutes.
				share: i < 5 ? 0.75 : 0.4,
			});
		}
		teams.push(players);
	}

	// Five men, sampled so the heavy-minutes players show up together most of
	// the time - which is exactly the collinearity that makes this hard.
	const lineup = (players: FakePlayer[]) => {
		const picked: FakePlayer[] = [];
		while (picked.length < 5) {
			const candidate = players[random.int(players.length)]!;
			if (picked.includes(candidate) || random.next() > candidate.share) {
				continue;
			}
			picked.push(candidate);
		}
		return picked;
	};

	const stints: RapmStint[] = [];
	const numGames = (numTeams * gamesPerTeam) / 2;
	for (let game = 0; game < numGames; game++) {
		const home = random.int(numTeams);
		let away = random.int(numTeams);
		while (away === home) {
			away = random.int(numTeams);
		}

		for (let n = 0; n < possPerGame; n += possPerStint) {
			const lineups = [lineup(teams[home]!), lineup(teams[away]!)];
			for (const o of [0, 1] as const) {
				const off = lineups[o]!;
				const def = lineups[o === 0 ? 1 : 0]!;
				const perPoss =
					1.1 +
					off.reduce((sum, p) => sum + p.off, 0) / 100 -
					def.reduce((sum, p) => sum + p.def, 0) / 100;
				const pts =
					perPoss * possPerStint +
					noise * Math.sqrt(possPerStint) * random.normal();
				stints.push({
					off: off.map((p) => p.key),
					def: def.map((p) => p.key),
					poss: possPerStint,
					pts,
				});
			}
		}
	}

	return { teams, stints, players: teams.flat() };
};

describe("computeRapm", () => {
	test("nothing to fit", () => {
		assert.strictEqual(computeRapm([]), undefined);
		assert.strictEqual(
			computeRapm([{ off: ["a"], def: ["b"], poss: 0, pts: 0 }]),
			undefined,
		);
	});

	// The whole point of the ridge penalty: a player nobody can separate from
	// his teammates comes back near average rather than at some enormous
	// offsetting number.
	test("a tiny sample is shrunk toward average, not blown up", () => {
		const stints: RapmStint[] = [];
		for (let i = 0; i < 400; i++) {
			stints.push(
				{
					off: ["a", "b", "c", "d", "e"],
					def: ["v", "w", "x", "y", "z"],
					poss: 5,
					pts: 6,
				},
				{
					off: ["v", "w", "x", "y", "z"],
					def: ["a", "b", "c", "d", "e"],
					poss: 5,
					pts: 5,
				},
			);
		}

		const fit = computeRapm(stints, { minPoss: 100 });
		assert.isDefined(fit);
		for (const rating of fit!.ratings.values()) {
			assert.isBelow(Math.abs(rating.off), 10);
			assert.isBelow(Math.abs(rating.def), 10);
		}
	});

	test("a player below the possession threshold is not rated", () => {
		const { stints } = fakeSeason({ gamesPerTeam: 4 });
		stints.push({
			off: ["cameo", "0-0", "0-1", "0-2", "0-3"],
			def: ["1-0", "1-1", "1-2", "1-3", "1-4"],
			poss: 4,
			pts: 4,
		});

		const fit = computeRapm(stints, { minPoss: 300 })!;
		assert.isUndefined(fit.ratings.get("cameo"));
	});

	// The real test. A full season of the model's own data, and the estimates
	// have to line up with the truth they were generated from.
	test("recovers known impact from a season of lineups", () => {
		const { players, stints } = fakeSeason();
		const fit = computeRapm(stints)!;

		const rated = players.filter((p) => fit.ratings.has(p.key));
		assert.isAbove(rated.length, players.length * 0.95);

		const truth = rated.map((p) => p.off + p.def);
		const estimate = rated.map((p) => {
			const rating = fit.ratings.get(p.key)!;
			return rating.off + rating.def;
		});
		const r = correlation(truth, estimate);
		console.log(
			`total r=${r.toFixed(3)} lambda=${fit.lambda} stints=${stints.length}`,
		);
		assert.isAbove(r, 0.65);

		const offR = correlation(
			rated.map((p) => p.off),
			rated.map((p) => fit.ratings.get(p.key)!.off),
		);
		const defR = correlation(
			rated.map((p) => p.def),
			rated.map((p) => fit.ratings.get(p.key)!.def),
		);
		console.log(`off r=${offR.toFixed(3)} def r=${defR.toFixed(3)}`);
		assert.isAbove(offR, 0.55);
		assert.isAbove(defR, 0.55);
	});

	// Ridge shrinks, so estimates are deliberately smaller than the truth. What
	// must not happen is a systematic tilt.
	test("the ratings are centered on average, not on something else", () => {
		const { players, stints } = fakeSeason({ seed: 7 });
		const fit = computeRapm(stints)!;

		let weighted = 0;
		let weight = 0;
		for (const p of players) {
			const rating = fit.ratings.get(p.key);
			if (rating) {
				weighted += rating.poss * (rating.off + rating.def);
				weight += rating.poss;
			}
		}
		assert.isBelow(Math.abs(weighted / weight), 0.5);
	});

	// A player the season can barely see keeps most of what he came in with,
	// which is the whole reason to carry a prior at all.
	test("a prior holds where the season has little to say", () => {
		const { stints } = fakeSeason({ gamesPerTeam: 6 });
		const prior = new Map([["0-0", { off: 6, def: 4 }]]);

		const without = computeRapm(stints, { minPoss: 100 })!;
		const with_ = computeRapm(stints, { minPoss: 100, prior })!;

		const before = without.ratings.get("0-0")!;
		const after = with_.ratings.get("0-0")!;
		assert.isAbove(after.off, before.off + 2);
		assert.isAbove(after.def, before.def + 1);

		// And it is his prior, not everybody's.
		const other = with_.ratings.get("0-1")!;
		assert.isBelow(Math.abs(other.off - without.ratings.get("0-1")!.off), 0.5);
	});

	// A prior is evidence, not a verdict. Enough of a season pointing the other
	// way overrules it.
	test("a full season overrules a prior that disagrees", () => {
		const { players, stints } = fakeSeason();

		// The best man in the league, told he was the worst.
		const best = [...players].sort(
			(a, b) => b.off + b.def - (a.off + a.def),
		)[0]!;
		const prior = new Map([[best.key, { off: -8, def: -8 }]]);

		const fit = computeRapm(stints, { prior })!;
		const plain = computeRapm(stints)!;

		const withPrior = fit.ratings.get(best.key)!;
		const total = withPrior.off + withPrior.def;
		const plainTotal =
			plain.ratings.get(best.key)!.off + plain.ratings.get(best.key)!.def;

		// Still dragged down by what it was told, but most of the way back to
		// what the season actually showed.
		assert.isBelow(total, plainTotal);
		assert.isBelow(Math.abs(total - plainTotal), Math.abs(total - -16));
	});

	test("no prior weight is no prior", () => {
		const { stints } = fakeSeason({ gamesPerTeam: 6 });
		const prior = new Map([["0-0", { off: 6, def: 4 }]]);
		assert.deepStrictEqual(
			computeRapm(stints, { minPoss: 100, prior, priorWeight: 0 })!.ratings.get(
				"0-0",
			),
			computeRapm(stints, { minPoss: 100 })!.ratings.get("0-0"),
		);
	});

	test("the same input always gives the same ratings", () => {
		const { stints } = fakeSeason({ gamesPerTeam: 10 });
		const a = computeRapm(stints)!;
		const b = computeRapm(stints)!;
		assert.strictEqual(a.lambda, b.lambda);
		for (const [key, rating] of a.ratings) {
			assert.strictEqual(rating.off, b.ratings.get(key)!.off);
			assert.strictEqual(rating.def, b.ratings.get(key)!.def);
		}
	});
});
