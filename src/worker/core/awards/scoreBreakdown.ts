// HOW AN AWARD SCORE ADDS UP, FOR THE AWARD RACES POPUP.
//
// The formula, the value of each stat it reads for this player, and - when the
// formula is a sum - what each piece of the sum contributed.

import { FormulaEvaluator } from "../../util/FormulaEvaluator.ts";
import {
	AWARD_STATS_ALL,
	PLAYOFF_SERIES_AWARD_STATS_ALL,
} from "./getPlayers.ts";

export type ScoreBreakdown = {
	formula: string;
	values: [string, number][];
	terms?: { sign: 1 | -1; text: string; value: number }[];
	// MIP and bounce back compare this season's formula score with earlier ones.
	compare?: {
		kind: "mip" | "bounceBack";
		current: number;
		prev: number;
		max: number;
	};
};

const NESTED_VARIABLES = ["numWon", "numWonConsecutive"];

// The formula's top-level + and - pieces: not inside parentheses, not a unary
// minus, not the minus of an exponent like 1e-5.
export const splitTerms = (formula: string) => {
	const terms: { sign: 1 | -1; text: string }[] = [];
	let depth = 0;
	let sign: 1 | -1 = 1;
	let start = 0;
	let prev = "";
	for (let i = 0; i < formula.length; i++) {
		const c = formula[i]!;
		if (c === "(") {
			depth += 1;
		} else if (c === ")") {
			depth -= 1;
		} else if (
			(c === "+" || c === "-") &&
			depth === 0 &&
			prev !== "" &&
			!"+-*/^(,".includes(prev)
		) {
			const exponent =
				(prev === "e" || prev === "E") &&
				/(?:^|[^\w.])[\d.]+[Ee]$/.test(formula.slice(0, i).trimEnd());
			if (!exponent) {
				terms.push({ sign, text: formula.slice(start, i).trim() });
				sign = c === "+" ? 1 : -1;
				start = i + 1;
				prev = c;
				continue;
			}
		}
		if (c.trim() !== "") {
			prev = c;
		}
	}
	terms.push({ sign, text: formula.slice(start).trim() });
	return terms;
};

const evaluatorCache = new Map<string, FormulaEvaluator<any, any> | null>();
const getEvaluator = (formula: string, playoffSeries: boolean) => {
	const key = `${playoffSeries ? "s" : "n"}${formula}`;
	let evaluator = evaluatorCache.get(key);
	if (evaluator === undefined) {
		try {
			evaluator = new FormulaEvaluator(
				formula,
				playoffSeries ? PLAYOFF_SERIES_AWARD_STATS_ALL : AWARD_STATS_ALL,
				NESTED_VARIABLES,
			);
		} catch {
			evaluator = null;
		}
		evaluatorCache.set(key, evaluator);
	}
	return evaluator;
};

export const getScoreBreakdown = (
	formula: string,
	stats: Record<string, any>,
	playoffSeries: boolean,
): ScoreBreakdown | undefined => {
	const evaluator = getEvaluator(formula, playoffSeries);
	if (!evaluator) {
		return;
	}
	const total = evaluator.evaluate(stats);
	const breakdown: ScoreBreakdown = {
		formula,
		values: evaluator.variableValues(stats),
	};

	const pieces = splitTerms(formula);
	if (pieces.length > 1) {
		const terms: NonNullable<ScoreBreakdown["terms"]> = [];
		let sum = 0;
		for (const { sign, text } of pieces) {
			const termEvaluator = getEvaluator(text, playoffSeries);
			if (!termEvaluator) {
				return breakdown;
			}
			const value = sign * termEvaluator.evaluate(stats);
			sum += value;
			terms.push({ sign, text, value });
		}
		// Only show the pieces if they really add up to the score.
		if (Math.abs(sum - total) <= 1e-9 * Math.max(1, Math.abs(total))) {
			breakdown.terms = terms;
		}
	}

	return breakdown;
};
