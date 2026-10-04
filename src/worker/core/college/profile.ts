import {
	COLLEGE_PRIORITIES,
	type CollegePriority,
	type CollegeProfile,
} from "../../../common/college.ts";
import { realGauss } from "../../../common/random.ts";
import { helpers } from "../../util/index.ts";

// A player's personality: what he wants from a school, and how much haggling
// he'll put up with. Generated once, in high school, and kept for his whole
// career (it matters again for NIL renegotiations and the portal).

// Gamma(k, 1) sample (Marsaglia-Tsang), for Dirichlet-distributed weights.
const gamma = (k: number): number => {
	if (k < 1) {
		return gamma(k + 1) * Math.random() ** (1 / k);
	}
	const d = k - 1 / 3;
	const c = 1 / Math.sqrt(9 * d);
	for (;;) {
		let x;
		let v;
		do {
			x = realGauss();
			v = 1 + c * x;
		} while (v <= 0);
		v = v * v * v;
		const u = Math.random();
		if (u < 1 - 0.0331 * x ** 4) {
			return d * v;
		}
		if (Math.log(u) < 0.5 * x * x + d * (1 - v + Math.log(v))) {
			return d * v;
		}
	}
};

export const genCollegeProfile = (stars: number): CollegeProfile => {
	// Low concentration, so most players have a few things they care about a
	// lot. Better players lean toward prestige and the pros; lesser ones toward
	// playing time.
	const alpha: Record<CollegePriority, number> = {
		prestige: 0.7 + 0.12 * stars,
		winning: 0.9,
		proximity: 0.9,
		playingTime: 1.3 - 0.15 * stars,
		proPotential: 0.3 + 0.2 * stars,
		nil: 0.9,
		conference: 0.5,
		coachStability: 0.5,
		facilities: 0.5,
	};
	const raw = COLLEGE_PRIORITIES.map((key) => gamma(alpha[key]));
	const total = raw.reduce((a, b) => a + b, 0);
	const weights = {} as Record<CollegePriority, number>;
	for (const [i, key] of COLLEGE_PRIORITIES.entries()) {
		weights[key] = Math.round((raw[i]! / total) * 1000) / 1000;
	}
	return {
		weights,
		patience: Math.round(helpers.bound(realGauss(3, 1), 1, 5)),
	};
};
