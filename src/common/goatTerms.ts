// Break a GOAT formula into the additive terms a reader thinks in, so the GOAT
// Lab can show where a player's score actually came from.
//
// Only top-level + and - are split on. Anything joined by * / ^ stays whole,
// because splitting "(a + b) * 2" into a and b would report numbers that don't
// add up to the term. That rule is what makes every node here independently
// evaluatable: each one is a balanced sub-expression whose value contributes to
// its parent with a sign, so children always sum to their parent.

export type GoatTerm = {
	// A valid formula on its own, with any redundant wrapping parens removed
	text: string;

	// Whether this term is subtracted from its parent
	negated: boolean;

	children: GoatTerm[];
};

const WHITESPACE = new Set([" ", "\t", "\n", "\r"]);

// A + or - right after one of these is a sign, not an operator between terms
const BEFORE_UNARY = new Set(["+", "-", "*", "/", "^", "(", ","]);

const previousNonSpace = (formula: string, i: number) => {
	for (let j = i - 1; j >= 0; j--) {
		const c = formula[j]!;
		if (!WHITESPACE.has(c)) {
			return c;
		}
	}
	return undefined;
};

export const splitAdditive = (
	formula: string,
): { text: string; negated: boolean }[] => {
	const parts: { text: string; negated: boolean }[] = [];

	let depth = 0;
	let start = 0;
	let negated = false;

	const push = (end: number) => {
		const text = formula.slice(start, end).trim();
		if (text !== "") {
			parts.push({ text, negated });
		}
	};

	for (let i = 0; i < formula.length; i++) {
		const c = formula[i]!;
		if (c === "(") {
			depth += 1;
		} else if (c === ")") {
			depth -= 1;
		} else if (depth === 0 && (c === "+" || c === "-")) {
			const prev = previousNonSpace(formula, i);
			if (prev !== undefined && !BEFORE_UNARY.has(prev)) {
				push(i);
				negated = c === "-";
				start = i + 1;
			}
		}
	}
	push(formula.length);

	return parts;
};

// "((a + b))" -> "a + b", but "(a) * (b)" is left alone - its parens aren't
// wrapping the whole expression, they just happen to sit at both ends.
export const stripOuterParens = (formula: string): string => {
	let text = formula.trim();

	while (text.startsWith("(") && text.endsWith(")")) {
		let depth = 0;
		let wrapsWhole = true;

		for (let i = 0; i < text.length; i++) {
			const c = text[i]!;
			if (c === "(") {
				depth += 1;
			} else if (c === ")") {
				depth -= 1;
				if (depth === 0 && i < text.length - 1) {
					wrapsWhole = false;
					break;
				}
			}
		}

		if (!wrapsWhole) {
			break;
		}

		text = text.slice(1, -1).trim();
	}

	return text;
};

// "(a + b) / 10" and "2 * (a + b)". Multiplication and division by a constant
// distribute over the sum, so such a term can still be broken down - each child
// carries the same factor and they still add up to the parent. Without this the
// biggest blocks of a formula, which are usually written as a parenthesised sum
// over a divisor, are the only ones that can't be opened up.
const SCALED_SUM = /^\((.+)\)\s*([*/])\s*([\d.]+)$/;
const SUM_SCALED = /^([\d.]+)\s*\*\s*\((.+)\)$/;

const distribute = (
	text: string,
): { parts: { text: string; negated: boolean }[] } | undefined => {
	const scaled = SCALED_SUM.exec(text);
	const inner = scaled ? scaled[1]! : SUM_SCALED.exec(text)?.[2];
	if (inner === undefined) {
		return undefined;
	}

	// The regex is greedy, so "(a) * (b)" would match with inner "a) * (b" -
	// splitting that would produce nonsense. Only a balanced inner is a real sum.
	if (stripOuterParens(`(${inner})`) !== inner.trim()) {
		return undefined;
	}

	const parts = splitAdditive(inner);
	if (parts.length < 2) {
		return undefined;
	}

	const apply = scaled
		? (child: string) => `(${child}) ${scaled[2]} ${scaled[3]}`
		: (child: string) => `${SUM_SCALED.exec(text)![1]} * (${child})`;

	return {
		parts: parts.map((part) => ({
			text: apply(part.text),
			negated: part.negated,
		})),
	};
};

export const MAX_TERM_DEPTH = 3;

export const buildGoatTerms = (
	formula: string,
	maxDepth = MAX_TERM_DEPTH,
): GoatTerm[] => {
	const build = (raw: string, negated: boolean, depth: number): GoatTerm => {
		const text = stripOuterParens(raw);

		let children: GoatTerm[] = [];
		if (depth < maxDepth) {
			let parts = splitAdditive(text);
			if (parts.length < 2) {
				parts = distribute(text)?.parts ?? parts;
			}

			// A subtraction inside a term means the term is one net quantity - a net
			// rating, a count of missed shots - not a list of separate things.
			// Pulling "ortg - drtg" apart replaces a +2.5 contribution with a +12.8
			// and a -10.3, two numbers that are individually meaningless and that
			// dominate any ordering by size.
			//
			// The formula's own top-level blocks are split before build() is
			// reached, so a penalty block subtracted there still gets its own line.
			if (parts.some((part) => part.negated)) {
				parts = [];
			}

			if (parts.length > 1) {
				children = parts.map((part) =>
					build(part.text, part.negated, depth + 1),
				);
			}
		}

		return { text, negated, children };
	};

	return splitAdditive(stripOuterParens(formula)).map((part) =>
		build(part.text, part.negated, 1),
	);
};

const IDENTIFIER = /[A-Za-z_][\w]*(?:\.[\w]+)?/g;

// Functions, not variables - "min" is both, so it only counts as a variable
// when it isn't being called
const FUNCTIONS = new Set(["abs", "min", "max"]);

export const variablesUsed = (formula: string): string[] => {
	const found = new Set<string>();

	for (const match of formula.matchAll(IDENTIFIER)) {
		const name = match[0];

		if (FUNCTIONS.has(name)) {
			let after = match.index + name.length;
			while (after < formula.length && WHITESPACE.has(formula[after]!)) {
				after += 1;
			}
			if (formula[after] === "(") {
				continue;
			}
		}

		found.add(name);
	}

	return Array.from(found).sort();
};

// Only the leaves carry distinct content - a parent is just its children added
// up - so a flat reading of a formula is its leaves, each with the sign it
// contributes to the total through however many levels of nesting.
export const goatLeaves = (
	formula: string,
	maxDepth = MAX_TERM_DEPTH,
): { text: string; sign: number }[] => {
	const leaves: { text: string; sign: number }[] = [];

	const walk = (terms: GoatTerm[], parentSign: number) => {
		for (const term of terms) {
			const sign = term.negated ? -parentSign : parentSign;

			if (term.children.length > 0) {
				walk(term.children, sign);
			} else {
				leaves.push({ text: term.text, sign });
			}
		}
	};
	walk(buildGoatTerms(formula, maxDepth), 1);

	return leaves;
};
