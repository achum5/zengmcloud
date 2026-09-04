// REGULARIZED ADJUSTED PLUS-MINUS.
//
// Every other player-value stat in the game reads a box score and guesses at
// impact. PER weights the counting stats by hand; BPM applies coefficients
// fitted to real NBA seasons, which is a borrowed mapping the sim was never
// part of. Both of them can only see what a player did with the ball in his
// hands, which is most of offense and almost none of defense.
//
// RAPM asks a different question, and it is the question the real analytics
// community settled on: holding the other nine players on the floor constant,
// how many points per hundred possessions does this man move the game? The
// data needed is the lineup and the score, and a sim knows both exactly.
//
// The regression is one row per (offensive five, defensive five) matchup:
//
//     points per 100  =  intercept
//                      + (offensive rating of each of the five on offense)
//                      - (defensive rating of each of the five on defense)
//
// Solved as-is, this is hopeless: teammates share nearly all of their floor
// time, so the design matrix is close to singular and the fit hands out
// enormous offsetting ratings that mean nothing. The "regularized" in the name
// is the fix - a ridge penalty that pulls every rating toward zero unless the
// data insists otherwise. A player with a few hundred possessions ends up near
// average, which is the honest answer, and one with a full season's worth is
// left where the data puts him.
//
// The strength of that penalty is the one free parameter, and rather than
// import a value tuned on real basketball - the exact mistake BPM makes - it
// is chosen here by cross-validation on the league's own possessions. Whatever
// the sim's scoring variance and talent spread turn out to be, the penalty
// adapts to them.

// One matchup: a five on offense, a five on defense, and what happened.
export type RapmStint = {
	// Opaque keys, one per player. Anything with the same key is treated as the
	// same player.
	off: readonly string[];
	def: readonly string[];
	poss: number;
	pts: number;
};

export type RapmRating = {
	// Points per 100 possessions added on offense, above league average.
	off: number;
	// Points per 100 possessions prevented on defense, above league average -
	// so bigger is better on BOTH sides, and the two add up to total impact.
	def: number;
	// Possessions the estimate rests on, offense and defense together.
	poss: number;
};

export type RapmOptions = {
	// A player with fewer possessions than this is not estimated at all. He is
	// pooled with every other such player into one shared "replacement" rating,
	// which keeps the regression from spending a column on somebody who played
	// two minutes, and keeps his noise out of everybody else's estimate.
	minPoss?: number;

	// Penalty strengths to choose between. Cross-validation picks one.
	lambdas?: readonly number[];

	// Every nth stint is held out to score the penalties on.
	holdoutEvery?: number;

	maxIterations?: number;
	tolerance?: number;
};

const DEFAULT_LAMBDAS = [125, 250, 500, 1000, 2000, 4000, 8000, 16000] as const;

// About a game and a half of rotation minutes. Below that a season's estimate
// is dominated by who he happened to share the floor with.
const DEFAULT_MIN_POSS = 300;

// The pooled column for everybody under the threshold. Not a real key, and
// never returned.
const REPLACEMENT = -1;

type Design = {
	// Column index per player, or REPLACEMENT.
	columnByKey: Map<string, number>;
	// Two coefficients per column (offense, defense) plus one intercept.
	numColumns: number;
	// Flattened, `width` entries per row: offensive columns then defensive.
	rows: Int32Array;
	width: number;
	numRows: number;
	// Points per 100 possessions.
	y: Float64Array;
	// Possessions.
	w: Float64Array;
	possByKey: Map<string, number>;
};

const buildDesign = (
	stints: readonly RapmStint[],
	minPoss: number,
): Design | undefined => {
	const possByKey = new Map<string, number>();
	const addPoss = (key: string, poss: number) => {
		possByKey.set(key, (possByKey.get(key) ?? 0) + poss);
	};

	let width = 0;
	for (const stint of stints) {
		if (!(stint.poss > 0)) {
			continue;
		}
		width = Math.max(width, stint.off.length + stint.def.length);
		for (const key of stint.off) {
			addPoss(key, stint.poss);
		}
		for (const key of stint.def) {
			addPoss(key, stint.poss);
		}
	}

	if (width === 0) {
		return undefined;
	}

	const columnByKey = new Map<string, number>();
	let next = 0;
	for (const [key, poss] of possByKey) {
		columnByKey.set(key, poss >= minPoss ? next++ : REPLACEMENT);
	}

	// The pooled column always exists, even when nobody lands in it - one wasted
	// column is cheaper than a branch on every row.
	const replacementColumn = next++;
	const numColumns = next;

	const usable = stints.filter((stint) => stint.poss > 0);
	const numRows = usable.length;
	const rows = new Int32Array(numRows * width);
	const y = new Float64Array(numRows);
	const w = new Float64Array(numRows);

	const column = (key: string, defense: boolean) => {
		const index = columnByKey.get(key);
		const base =
			index === undefined || index === REPLACEMENT ? replacementColumn : index;
		return 2 * base + (defense ? 1 : 0);
	};

	for (const [i, stint] of usable.entries()) {
		const offset = i * width;
		let j = 0;
		for (const key of stint.off) {
			rows[offset + j++] = column(key, false);
		}
		for (const key of stint.def) {
			rows[offset + j++] = column(key, true);
		}
		// A shorthanded stint (an ejection, a sport with a different lineup size
		// in one row) leaves the tail empty rather than misaligned.
		while (j < width) {
			rows[offset + j++] = -1;
		}

		y[i] = (100 * stint.pts) / stint.poss;
		w[i] = stint.poss;
	}

	return {
		columnByKey,
		numColumns,
		rows,
		width,
		numRows,
		y,
		w,
		possByKey,
	};
};

// The unknowns are two per column plus a trailing intercept.
const numUnknowns = (design: Design) => 2 * design.numColumns + 1;

// One row's prediction.
const predict = (design: Design, x: Float64Array, i: number) => {
	const offset = i * design.width;
	let sum = x[2 * design.numColumns]!;
	for (let j = 0; j < design.width; j++) {
		const c = design.rows[offset + j]!;
		if (c >= 0) {
			sum += x[c]!;
		}
	}
	return sum;
};

// A conjugate-gradient solve of (X'WX + lambda*P) b = X'Wy, where P penalizes
// every player coefficient and leaves the intercept free. X is never formed:
// each row has at most eleven nonzeros, so the two products it takes per
// iteration are linear in the number of stints. Jacobi preconditioning matters
// here because possession counts vary by two orders of magnitude between a
// starter and the twelfth man.
const solve = (
	design: Design,
	lambda: number,
	rowIndices: Int32Array,
	{
		maxIterations,
		tolerance,
		initial,
	}: {
		maxIterations: number;
		tolerance: number;
		initial?: Float64Array;
	},
) => {
	const n = numUnknowns(design);
	const interceptColumn = n - 1;

	// b = X'Wy, and the diagonal of X'WX for the preconditioner.
	const b = new Float64Array(n);
	const diagonal = new Float64Array(n);
	for (const i of rowIndices) {
		const offset = i * design.width;
		const wi = design.w[i]!;
		const wy = wi * design.y[i]!;
		for (let j = 0; j < design.width; j++) {
			const c = design.rows[offset + j]!;
			if (c >= 0) {
				b[c]! += wy;
				diagonal[c]! += wi;
			}
		}
		b[interceptColumn]! += wy;
		diagonal[interceptColumn]! += wi;
	}
	for (let c = 0; c < interceptColumn; c++) {
		diagonal[c]! += lambda;
	}

	const applyA = (v: Float64Array, out: Float64Array) => {
		out.fill(0);
		const intercept = v[interceptColumn]!;
		for (const i of rowIndices) {
			const offset = i * design.width;
			let dot = intercept;
			for (let j = 0; j < design.width; j++) {
				const c = design.rows[offset + j]!;
				if (c >= 0) {
					dot += v[c]!;
				}
			}
			const scaled = design.w[i]! * dot;
			for (let j = 0; j < design.width; j++) {
				const c = design.rows[offset + j]!;
				if (c >= 0) {
					out[c]! += scaled;
				}
			}
			out[interceptColumn]! += scaled;
		}
		for (let c = 0; c < interceptColumn; c++) {
			out[c]! += lambda * v[c]!;
		}
	};

	const x = initial ? Float64Array.from(initial) : new Float64Array(n);
	const r = new Float64Array(n);
	const z = new Float64Array(n);
	const p = new Float64Array(n);
	const ap = new Float64Array(n);

	applyA(x, ap);
	let bNorm = 0;
	for (let c = 0; c < n; c++) {
		r[c] = b[c]! - ap[c]!;
		bNorm += b[c]! * b[c]!;
	}
	bNorm = Math.sqrt(bNorm);
	if (bNorm === 0) {
		return x;
	}

	const precondition = () => {
		for (let c = 0; c < n; c++) {
			const d = diagonal[c]!;
			z[c] = d > 0 ? r[c]! / d : r[c]!;
		}
	};

	precondition();
	p.set(z);
	let rz = 0;
	for (let c = 0; c < n; c++) {
		rz += r[c]! * z[c]!;
	}

	for (let iteration = 0; iteration < maxIterations; iteration++) {
		applyA(p, ap);

		let pap = 0;
		for (let c = 0; c < n; c++) {
			pap += p[c]! * ap[c]!;
		}
		if (!(pap > 0)) {
			// The system is positive definite by construction, so this only
			// happens once rounding has taken over. Whatever we have is the
			// answer.
			break;
		}

		const alpha = rz / pap;
		let rNorm = 0;
		for (let c = 0; c < n; c++) {
			x[c]! += alpha * p[c]!;
			r[c]! -= alpha * ap[c]!;
			rNorm += r[c]! * r[c]!;
		}

		if (Math.sqrt(rNorm) <= tolerance * bNorm) {
			break;
		}

		precondition();
		let rzNext = 0;
		for (let c = 0; c < n; c++) {
			rzNext += r[c]! * z[c]!;
		}
		const beta = rzNext / rz;
		rz = rzNext;
		for (let c = 0; c < n; c++) {
			p[c] = z[c]! + beta * p[c]!;
		}
	}

	return x;
};

// Possession-weighted mean squared error of a fit on rows it did not see.
const holdoutError = (
	design: Design,
	x: Float64Array,
	rowIndices: Int32Array,
) => {
	let total = 0;
	let weight = 0;
	for (const i of rowIndices) {
		const error = design.y[i]! - predict(design, x, i);
		total += design.w[i]! * error * error;
		weight += design.w[i]!;
	}
	return weight > 0 ? total / weight : Infinity;
};

export type RapmFit = {
	ratings: Map<string, RapmRating>;
	// The penalty cross-validation settled on. Reported because it is the one
	// number that says how much this league's lineups could actually resolve.
	lambda: number;
};

export const computeRapm = (
	stints: readonly RapmStint[],
	{
		minPoss = DEFAULT_MIN_POSS,
		lambdas = DEFAULT_LAMBDAS,
		holdoutEvery = 5,
		maxIterations = 400,
		tolerance = 1e-8,
	}: RapmOptions = {},
): RapmFit | undefined => {
	const design = buildDesign(stints, minPoss);
	if (!design || design.numRows === 0 || lambdas.length === 0) {
		return undefined;
	}

	const all = new Int32Array(design.numRows);
	for (let i = 0; i < design.numRows; i++) {
		all[i] = i;
	}

	// Deterministic, so the same season always produces the same ratings.
	const train: number[] = [];
	const holdout: number[] = [];
	for (let i = 0; i < design.numRows; i++) {
		(i % holdoutEvery === 0 ? holdout : train).push(i);
	}

	let lambda = lambdas[0]!;
	if (holdout.length > 0 && train.length > 0 && lambdas.length > 1) {
		const trainRows = Int32Array.from(train);
		const holdoutRows = Int32Array.from(holdout);

		// Descending, so each solve warm-starts from a more heavily penalized
		// one. The heavy end converges in a handful of iterations and every
		// step after it begins close to its answer.
		const descending = [...lambdas].sort((a, b) => b - a);
		let best = Infinity;
		let warm: Float64Array | undefined;
		for (const candidate of descending) {
			const x = solve(design, candidate, trainRows, {
				maxIterations,
				tolerance,
				initial: warm,
			});
			warm = x;
			const error = holdoutError(design, x, holdoutRows);
			if (error < best) {
				best = error;
				lambda = candidate;
			}
		}
	}

	const x = solve(design, lambda, all, { maxIterations, tolerance });

	const ratings = new Map<string, RapmRating>();
	for (const [key, column] of design.columnByKey) {
		if (column === REPLACEMENT) {
			continue;
		}
		ratings.set(key, {
			off: x[2 * column]!,
			// Stored so that bigger is better on both sides: the regression
			// learns how much a defender ADDS to the opponent's scoring, and a
			// good one subtracts from it.
			def: -x[2 * column + 1]!,
			poss: design.possByKey.get(key)!,
		});
	}

	return { ratings, lambda };
};
