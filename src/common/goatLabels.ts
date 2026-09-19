// Turning a GOAT formula's leaf terms into something a person reads without
// knowing the formula: "3x First Team All-League", not "awards.NBA1 *0.8".

// goatFormula derives these from every stat, so a variable name is a base stat
// plus at most one of them. Longest first - "PlayoffsPerGame" also ends with
// "PerGame".
const STAT_SUFFIXES = [
	["PlayoffsPerGame", "playoffs, per game"],
	["PeakPerGame", "peak season, per game"],
	["Playoffs", "playoffs"],
	["PerGame", "per game"],
	["Peak", "peak season"],
] as const;

export const splitStatSuffix = (
	name: string,
): { base: string; qualifier?: string } => {
	for (const [suffix, qualifier] of STAT_SUFFIXES) {
		if (name.length > suffix.length && name.endsWith(suffix)) {
			return { base: name.slice(0, -suffix.length), qualifier };
		}
	}

	return { base: name };
};

// Counts of things that happened, so they read as "3x Champion". Everything
// else is a measured amount and reads as "PER 24.1".
export const COUNT_VARIABLES = new Set(["champ", "allStar", "allStarMvp"]);

export const isCountVariable = (name: string) =>
	name.startsWith("awards.") || COUNT_VARIABLES.has(name);

export type GoatLabeller = {
	// shortName (or shortName + team number) to the award's name in this league
	awards: Record<string, string>;

	// base stat name to its full description, eg "per" to "Player Efficiency Rating"
	stats: Record<string, string>;

	// base stat name to its column abbrev, eg "per" to "PER"
	short: Record<string, string>;
};

// "((a + b))" -> "a + b", leaving "(a) - (b)" alone
const stripWrappingParens = (text: string): string => {
	let out = text.trim();

	while (out.startsWith("(") && out.endsWith(")")) {
		let depth = 0;
		let wraps = true;
		for (let i = 0; i < out.length; i++) {
			if (out[i] === "(") {
				depth += 1;
			} else if (out[i] === ")") {
				depth -= 1;
				if (depth === 0 && i < out.length - 1) {
					wraps = false;
					break;
				}
			}
		}
		if (!wraps) {
			break;
		}
		out = out.slice(1, -1).trim();
	}

	return out;
};

const SIMPLE_LABELS: Record<string, string> = {
	champ: "Champion",
	allStar: "All-Star",
	allStarMvp: "All-Star MVP",
	numSeasons: "Seasons played",
};

export const labelForVariable = (
	name: string,
	labeller: GoatLabeller,
): string => {
	if (name.startsWith("awards.")) {
		const shortName = name.slice("awards.".length);
		return labeller.awards[shortName] ?? shortName;
	}

	const simple = SIMPLE_LABELS[name];
	if (simple !== undefined) {
		return simple;
	}

	const { base, qualifier } = splitStatSuffix(name);
	const described = labeller.stats[base] ?? base;

	return qualifier === undefined ? described : `${described} (${qualifier})`;
};

// A term with more than one variable can't borrow a single name, so the
// variables' short names go in place of the raw identifiers, operators intact -
// "ORtg - DRtg", not "ORtg and DRtg", which would read as a sum. The scaling is
// dropped: the points column already says what the term was worth, and carrying
// "/10" through would add noise to every row.
const SCALE = /^\s*(?:([\d.]+)\s*\*\s*)?(.*?)(?:\s*[*/]\s*([\d.]+))?\s*$/;

const IDENTIFIER = /[A-Za-z_][\w]*(?:\.[\w]+)?/g;

export const labelForTerm = (
	text: string,
	variables: string[],
	labeller: GoatLabeller,
): string => {
	if (variables.length === 1) {
		return labelForVariable(variables[0]!, labeller);
	}

	// Every variable measured the same way - all peaks, all playoffs - reads
	// better with that said once at the end than repeated on each name.
	const qualifiers = new Set(
		variables.map((name) => splitStatSuffix(name).qualifier),
	);
	const shared = qualifiers.size === 1 ? [...qualifiers][0] : undefined;

	const bare = SCALE.exec(text)?.[2] ?? text;
	const inner = stripWrappingParens(bare);

	const body = inner.replaceAll(IDENTIFIER, (name) => {
		const { base, qualifier } = splitStatSuffix(name);
		const short = labeller.short[base] ?? base;
		return shared !== undefined || qualifier === undefined
			? short
			: `${short} ${qualifier}`;
	});

	return shared === undefined ? body : `${body} (${shared})`;
};
