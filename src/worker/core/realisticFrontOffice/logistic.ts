// Logistic regression of probabilities on a few predictors (an intercept is
// added in front), by Newton's method on the cross-entropy. Every step must
// lower the loss or it is halved, and a sliver of ridge keeps the answer finite
// even when the data would let it grow without bound - plain Newton once
// returned a title curve that jumped from no chance to a certain title at one
// strength, and with it trades "worth" a whole championship.

const RIDGE = 1e-3;

const loss = (
	rows: readonly { x: readonly number[]; p: number }[],
	w: readonly number[],
) => {
	let total = 0;
	for (const { x, p } of rows) {
		let z = w[0]!;
		for (let i = 0; i < x.length; i++) {
			z += w[i + 1]! * x[i]!;
		}
		// log(1 + e^z) without overflow.
		const softplus =
			z > 0 ? z + Math.log1p(Math.exp(-z)) : Math.log1p(Math.exp(z));
		total += softplus - p * z;
	}
	for (let i = 1; i < w.length; i++) {
		total += (RIDGE / 2) * w[i]! ** 2;
	}
	return total;
};

// Solves m x = b by Gaussian elimination with partial pivoting.
const solve = (m: number[][], b: number[]) => {
	const n = b.length;
	const a = m.map((row, i) => [...row, b[i]!]);
	for (let col = 0; col < n; col++) {
		let pivot = col;
		for (let r = col + 1; r < n; r++) {
			if (Math.abs(a[r]![col]!) > Math.abs(a[pivot]![col]!)) {
				pivot = r;
			}
		}
		if (Math.abs(a[pivot]![col]!) < 1e-14) {
			return undefined;
		}
		[a[col], a[pivot]] = [a[pivot]!, a[col]!];
		for (let r = col + 1; r < n; r++) {
			const f = a[r]![col]! / a[col]![col]!;
			for (let c = col; c <= n; c++) {
				a[r]![c]! -= f * a[col]![c]!;
			}
		}
	}
	const x = new Array<number>(n).fill(0);
	for (let r = n - 1; r >= 0; r--) {
		let s = a[r]![n]!;
		for (let c = r + 1; c < n; c++) {
			s -= a[r]![c]! * x[c]!;
		}
		x[r] = s / a[r]![r]!;
	}
	return x;
};

// Returns [intercept, ...one weight per predictor].
export const fitLogistic = (
	rows: readonly { x: readonly number[]; p: number }[],
	start: readonly number[],
): number[] => {
	const k = start.length;
	let w = [...start];
	let current = loss(rows, w);
	for (let iter = 0; iter < 100; iter++) {
		const grad = new Array<number>(k).fill(0);
		const hess = Array.from({ length: k }, () => new Array<number>(k).fill(0));
		for (const { x, p } of rows) {
			const v = [1, ...x];
			let z = 0;
			for (let i = 0; i < k; i++) {
				z += w[i]! * v[i]!;
			}
			const q = 1 / (1 + Math.exp(-z));
			for (let i = 0; i < k; i++) {
				grad[i]! += (q - p) * v[i]!;
				for (let j = 0; j < k; j++) {
					hess[i]![j]! += q * (1 - q) * v[i]! * v[j]!;
				}
			}
		}
		for (let i = 1; i < k; i++) {
			grad[i]! += RIDGE * w[i]!;
			hess[i]![i]! += RIDGE;
		}
		const step = solve(hess, grad);
		if (!step) {
			break;
		}
		let scale = 1;
		let next = w;
		let nextLoss = current;
		while (scale > 1e-6) {
			next = w.map((x, i) => x - scale * step[i]!);
			nextLoss = loss(rows, next);
			if (nextLoss <= current) {
				break;
			}
			scale /= 2;
		}
		if (nextLoss > current) {
			break;
		}
		const moved = step.reduce((a, x) => a + Math.abs(x), 0) * scale;
		w = next;
		current = nextLoss;
		if (moved < 1e-10) {
			break;
		}
	}
	return w;
};
