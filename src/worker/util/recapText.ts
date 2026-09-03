// THE RECAP ENGINE'S VOCABULARY.
//
// Everything the sentence builders share: the seeded rng, the phrasing memory
// behind pick(), the verb ledger, and the small text helpers - articles,
// ordinals, plurals, possessives, injury names, the stat line, the team
// labels. Lives apart from getAutoRecap.ts so the beats that read the season
// around a game (recapBeats.ts) speak in the same voice without importing the
// engine that assembles them.

import type { RecapPlayer, RecapTeam } from "./getDayGamesForRecap.ts";

export const rngFromSeed = (seed: number): (() => number) => {
	let a = seed >>> 0;
	return () => {
		a += 0x6d2b79f5;
		let t = a;
		t = Math.imul(t ^ (t >>> 15), t | 1);
		t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
		return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
	};
};

// Phrasing memory, so a night of recaps doesn't say the same thing over and
// over. Every game seeds its own rng from its gid, which makes each game
// reproducible but leaves the choices independent - across fourteen games that
// reliably produced "the Bucks routed the Suns, the Hornets routed the Pacers,
// the Nets routed the Heat" and five straight "got past"es.
//
// So pick() remembers what a batch has already used from each pool and prefers
// something else until the pool is exhausted. Keyed by the pool itself, so
// unrelated pools never interfere, and reset per day by beginRecapBatch. A
// single game generated on its own (the box score page) just starts empty,
// which is the right behavior there.
const phraseMemory = new Map<string, Set<string>>();

// Outside a batch, one game is one batch. That keeps a single recap
// reproducible - generating the same game twice has to give the same text - so
// the memory only spans calls when something explicitly opens a batch.
let inBatch = false;

export const beginRecapBatch = () => {
	phraseMemory.clear();
	inBatch = true;
};

export const endRecapBatch = () => {
	inBatch = false;
	phraseMemory.clear();
};

export const isInBatch = () => inBatch;

export const clearPhraseMemory = () => {
	phraseMemory.clear();
};

// A pool's identity is its SHAPE, not its rendered text. Keying on the finished
// string means a pool that interpolates a name or a number is a different key
// in every game, so the rotation never engages for it. Blanking the variable
// parts makes the key the template the author actually wrote.
//
// This is a backstop, not the main mechanism: pools whose OPTIONS differ in
// shape game to game (one game's star has a double-double and the next one's
// doesn't, so the phrase is "# points and # rebounds" here and "#" there) still
// split into variants, and those need an explicit `poolId` to share one
// rotation. The headline pools carry ids for exactly that reason. Measured on a
// twelve-game slate this normalization changes nothing by itself - the ids do
// the work - but it makes `pick`'s contract hold for every pool added later
// without anyone having to remember the id.
const poolKey = (arr: unknown[]): string =>
	arr
		.map((option) =>
			String(option)
				// Proper nouns - team nicknames, player names - matched as runs so
				// "Trail Blazers" is one placeholder rather than two.
				.replaceAll(/\b[A-Z][\w'.\u2019-]*(?:\s+[A-Z][\w'.\u2019-]*)*/g, "~")
				.replaceAll(/\d+(?:\.\d+)?/g, "#")
				.replaceAll(/\s+/g, " ")
				.trim(),
		)
		.join("\u0000");

export const pick = <T>(rng: () => number, arr: T[], poolId?: string): T => {
	if (arr.length <= 1) {
		return arr[0]!;
	}

	const key = poolId ?? poolKey(arr);
	let used = phraseMemory.get(key);
	if (!used) {
		used = new Set<string>();
		phraseMemory.set(key, used);
	}

	// Remembered by INDEX, so an interpolated pool rotates through its shapes
	// even though the rendered text differs every time.
	let fresh = arr.map((_, i) => i).filter((i) => !used!.has(String(i)));
	if (fresh.length === 0) {
		// Everything has been used once; start the rotation over rather than
		// refusing to say anything.
		used.clear();
		fresh = arr.map((_, i) => i);
	}

	const chosenIdx = fresh[Math.floor(rng() * fresh.length)]!;
	used.add(String(chosenIdx));
	return arr[chosenIdx]!;
};

// Fisher-Yates using the seeded rng, so ordering is deterministic per game.
export const shuffle = <T>(rng: () => number, arr: T[]): T[] => {
	const out = [...arr];
	for (let i = out.length - 1; i > 0; i--) {
		const j = Math.floor(rng() * (i + 1));
		[out[i], out[j]] = [out[j]!, out[i]!];
	}
	return out;
};

export const naturalList = (items: string[]): string => {
	if (items.length === 0) {
		return "";
	}
	if (items.length === 1) {
		return items[0]!;
	}
	if (items.length === 2) {
		return `${items[0]} and ${items[1]}`;
	}
	return `${items.slice(0, -1).join(", ")}, and ${items.at(-1)}`;
};

export const stripHtml = (s: string): string =>
	s
		.replace(/<[^>]*>/g, "")
		.replace(/\s+/g, " ")
		.trim();

export const cap = (s: string): string =>
	s ? s.charAt(0).toUpperCase() + s.slice(1) : s;

const ORDINALS = [
	"",
	"first",
	"second",
	"third",
	"fourth",
	"fifth",
	"sixth",
	"seventh",
	"eighth",
	"ninth",
	"tenth",
	"eleventh",
	"twelfth",
	"thirteenth",
	"fourteenth",
	"fifteenth",
];

export const ordinal = (n: number): string => ORDINALS[n] ?? `${n}th`;

export const plural = (n: number, word: string): string => {
	if (n === 1) {
		return `${n} ${word}`;
	}
	// "5 passs" made it into a real recap. A sibilant ending takes -es.
	const suffix = /(?:s|sh|ch|x|z)$/.test(word) ? "es" : "s";
	return `${n} ${word}${suffix}`;
};

// "a 12-point hole" but "an 18-point hole" - the article follows the number's
// SOUND. For the margins/deficits recaps deal in (single digits through the
// 60s), the vowel-initial numbers are 8, 11, and 18.
// Sentence-initial numbers read badly as digits ("4 of the 14 games...").
const NUM_WORDS = [
	"zero",
	"one",
	"two",
	"three",
	"four",
	"five",
	"six",
	"seven",
	"eight",
	"nine",
	"ten",
	"eleven",
	"twelve",
];

export const numWord = (n: number): string => NUM_WORDS[n] ?? String(n);

export const aNum = (n: number): string =>
	n === 8 || n === 11 || n === 18 ? `an ${n}` : `a ${n}`;

// "a sprained ankle" but "an Achilles injury".
export const aWord = (s: string): string =>
	/^[aeiou]/i.test(s) ? `an ${s}` : `a ${s}`;

// Injury types come capitalized ("Sprained Ankle"); prose wants "sprained
// ankle" - but acronyms keep their caps ("Torn ACL" -> "torn ACL").
export const lowerInjury = (type: string): string =>
	type
		.split(" ")
		.map((w) => (w.length >= 2 && w === w.toUpperCase() ? w : w.toLowerCase()))
		.join(" ");

// "a sprained ankle", but "plantar fasciitis" and "back spasms" take no article
// at all - "went down with a plantar fasciitis" is the kind of line that gives
// the whole thing away. Conditions ending in -itis are uncountable, and a
// plural name is already plural.
export const injuryPhrase = (type: string): string => {
	const lower = lowerInjury(type);
	// A BARE PARTICIPLE IS NOT A THING YOU CAN HAVE. Injury types are normally
	// noun phrases ("Sprained Ankle"), but a league can carry a type that is only
	// an adjective - the generic "Injured" that arrives with some imported
	// rosters is the one seen in the wild, and it produced "sat out with an
	// injured", which is the sort of line that gives the whole generator away.
	// One word ending in -ed describes the player, not the injury, so name the
	// injury instead. Nothing is lost: "Injured" never said more than that.
	if (!lower.includes(" ") && lower.endsWith("ed")) {
		return "an injury";
	}
	// "ss" is singular ("abscess"), and so is "us" ("torn meniscus"); both
	// still take an article.
	if (
		lower.endsWith("itis") ||
		(lower.endsWith("s") && !lower.endsWith("ss") && !lower.endsWith("us"))
	) {
		return lower;
	}
	return aWord(lower);
};

export const gbText = (gb: number): string =>
	gb === 0.5 ? "half a game" : `${gb} game${gb === 1 ? "" : "s"}`;

// The five double-double-eligible stats; a category counts only at 10+.
export const doubleCategories = (p: RecapPlayer): string[] => {
	const cats: [string, number][] = [
		["points", p.pts],
		["rebounds", p.reb],
		["assists", p.ast],
		["steals", p.stl],
		["blocks", p.blk],
	];
	return cats.filter(([, v]) => (v ?? 0) >= 10).map(([k]) => k);
};

export const doubleWord = (count: number): string | undefined => {
	if (count >= 5) {
		return "quintuple-double";
	}
	if (count === 4) {
		return "quadruple-double";
	}
	if (count === 3) {
		return "triple-double";
	}
	if (count === 2) {
		return "double-double";
	}
	return undefined;
};

// The winner's supporting cast: everyone but the story player, best first.
// ONE VOCABULARY FOR "SCORED N", shared by every sentence that says it.
//
// These lists used to be written out separately at each site and three of them
// overlapped, so a single recap could read "Al Horford was good for 18 points"
// in one paragraph and "Keith Bogans was good for 18 points in defeat" two
// sentences later. `pick` rotates WITHIN a pool and cannot see across two of
// them, so the only way one rotation covers all three is for all three to BE
// one pool - hence the shared id as well as the shared array.
export const SCORED_VERBS = [
	"added",
	"chipped in",
	"contributed",
	"kicked in",
	"pitched in with",
	"tacked on",
	"was good for",
	"supplied",
	"came up with",
	// "finished with" is deliberately NOT here, and not in leadVerb either. The
	// team-total lines ("They finished with 28 assists"), the foul-out line and
	// the star's shooting line ("finished 7-of-15") all use it, and `pick` cannot
	// see across builders - so leaving it in produced "finished 7-of-15...
	// finished with 28 assists... Role One finished with 16 points" in one
	// recap. Taking it out of SCORED_VERBS alone was not enough: leadVerb still
	// held it, and "Marbury finished with 18 points and 12 assists. They finished
	// with 28 assists." came straight back. The word belongs to those builders
	// and to no pool. Thirteen other verbs cost nothing.
	"had",
	"put up",
	"posted",
	"went for",
] as const;

// Scoring verbs already spent in the recap being written, BY WORD.
//
// pick() rotates within one pool and cannot see another. The lead sentence
// (leadVerb) and the supporting-cast sentences (scoredVerb) draw from different
// pools that share words - "put up", "posted", "had" - so one recap could open
// "Marbury put up 18 points and 12 assists" and close "Role One put up 16
// points", and which seeds did that was pure luck. The old fix pulled one
// overlapping word ("finished with") out of one list, which moved the collision
// rather than removing it: a headline change elsewhere in this file was enough
// to surface it again at a different gid.
//
// Recording the WORD is what makes it impossible, whatever the pools hold and
// however the rng lands. Reset per recap by resetVerbLedger.
const usedVerbs = new Set<string>();

export const resetVerbLedger = () => {
	usedVerbs.clear();
};

// Draw from a pool, stepping past anything this recap has already said. pick()
// walks a pool by index before repeating, so at most pool.length draws sees
// every option; if they are all spent (a recap with more scoring sentences than
// the pool has verbs) the last one stands rather than saying nothing.
export const takeVerb = (
	rng: () => number,
	pool: readonly string[],
	poolId?: string,
): string => {
	let verb = pick(rng, [...pool], poolId);
	for (let i = 0; i < pool.length && usedVerbs.has(verb); i++) {
		verb = pick(rng, [...pool], poolId);
	}
	usedVerbs.add(verb);
	return verb;
};

// A verb written into a sentence directly rather than drawn from a pool. Says
// so, so a later draw does not pick the same word.
export const claimVerb = (verb: string): string => {
	usedVerbs.add(verb);
	return verb;
};

// Every scoring verb any builder can put in a sentence. Several pools hold
// whole SENTENCES with one of these baked in, and pick() rotates inside one
// pool while blind to the rest - so a recap read "Evan Green finished with 21
// points and 9 rebounds for the Clippers... The Pacers finished with 11
// blocks... Foul trouble cost Cade Green, who finished with 17 points in 24
// minutes." Three pools, three independent draws, one word, three times on a
// page. Each of those pools already carried alternatives that avoid it; they
// just had no way to know it was spent.
const LEDGER_VERBS = [
	...SCORED_VERBS,
	"finished with",
	"totaled",
	"recorded",
	"collected",
	"scored",
	"poured in",
	"erupted for",
	"exploded for",
	"piled up",
	"racked up",
];

// pick() over a pool of whole sentences, stepping past any option that repeats
// a verb this recap already spent, then claiming whatever it lands on. Falls
// back to the plain rotation when every option is spent, so a pool can never
// refuse to say anything.
export const pickSentence = (
	rng: () => number,
	options: string[],
	poolId?: string,
): string => {
	const spent = (text: string) =>
		LEDGER_VERBS.some((v) => usedVerbs.has(v) && text.includes(` ${v} `));
	let text = pick(rng, options, poolId);
	for (let i = 0; i < options.length && spent(text); i++) {
		text = pick(rng, options, poolId);
	}
	for (const v of LEDGER_VERBS) {
		if (text.includes(` ${v} `)) {
			usedVerbs.add(v);
		}
	}
	return text;
};

export const scoredVerb = (rng: () => number): string =>
	takeVerb(rng, SCORED_VERBS, "scoredVerb");

// "34 points, 12 rebounds and 9 assists" - points first, then up to two more
// categories worth mentioning (double-double stats always make the cut).
export const statPhrase = (p: RecapPlayer, maxExtras = 2): string => {
	const dd = new Set(doubleCategories(p));
	// [sortWeight, text]. Steals and blocks are rarer and more telling, so they're
	// weighted up - a 6-block night should out-rank a 7-assist one when trimming.
	const extras: [number, string][] = [];
	if (p.reb >= 8 || dd.has("rebounds")) {
		extras.push([p.reb, plural(p.reb, "rebound")]);
	}
	if (p.ast >= 6 || dd.has("assists")) {
		extras.push([p.ast, plural(p.ast, "assist")]);
	}
	if (p.stl >= 4 || dd.has("steals")) {
		extras.push([p.stl * 1.7, plural(p.stl, "steal")]);
	}
	if (p.blk >= 4 || dd.has("blocks")) {
		extras.push([p.blk * 1.7, plural(p.blk, "block")]);
	}
	extras.sort((a, b) => b[0] - a[0]);
	const chosen = extras.slice(0, maxExtras).map((e) => e[1]);
	return naturalList([plural(p.pts, "point"), ...chosen]);
};

export type TeamStats = {
	fg: number;
	fga: number;
	tp: number;
	tpa: number;
	ft: number;
	fta: number;
	reb: number;
	ast: number;
	tov: number;
	stl: number;
	blk: number;
	fgp: number;
	tpp: number;
	dblFig: number;
};

export const teamStats = (t: RecapTeam): TeamStats => {
	const s = {
		fg: 0,
		fga: 0,
		tp: 0,
		tpa: 0,
		ft: 0,
		fta: 0,
		reb: 0,
		ast: 0,
		tov: 0,
		stl: 0,
		blk: 0,
		fgp: 0,
		tpp: 0,
		dblFig: 0,
	};
	for (const p of t.players) {
		s.fg += p.fg;
		s.fga += p.fga;
		s.tp += p.tp;
		s.tpa += p.tpa;
		s.ft += p.ft;
		s.fta += p.fta;
		s.reb += p.reb;
		s.ast += p.ast;
		s.tov += p.tov;
		s.stl += p.stl;
		s.blk += p.blk;
		if (p.pts >= 10) {
			s.dblFig += 1;
		}
	}
	s.fgp = s.fga > 0 ? Math.round((s.fg / s.fga) * 1000) / 10 : 0;
	s.tpp = s.tpa > 0 ? Math.round((s.tp / s.tpa) * 1000) / 10 : 0;
	return s;
};

export const nick = (t: RecapTeam): string =>
	t.name || t.region || "the home team";

export const theNick = (t: RecapTeam): string => `the ${nick(t)}`;

// Possessive that reads right for plural team nicknames ("the Kings'", "the
// 76ers'") and singular names ("Kobe Bryant's").
export const poss = (s: string): string =>
	s.endsWith("s") ? `${s}'` : `${s}'s`;
