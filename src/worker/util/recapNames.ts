// SECOND REFERENCES.
//
// Every paper in the country writes a man's whole name once and his surname
// after that: "Lonnie Ingram had 31 points... Ingram's layup tied it at 114".
// The recap engine wrote the whole name every time, and a piece that says
// "Lonnie Ingram" five times reads like a form letter, not a story. And now
// and then the city stands in for the nickname, the way a beat writer
// alternates them: "for Indiana", "Detroit's bench".
//
// Both run over FINISHED text, as the last pass, so none of the sentence
// builders have to know about them - and both refuse anything a reader could
// misread. A surname two men in the box score share stays a full name, as
// does one that is also a first name, a team or a city in the story.

import { poss, rngFromSeed } from "./recapText.ts";

// "Gary Trent Jr." is Trent on second reference, "Robert Williams III" is
// Williams.
const SUFFIX = /^(?:jr\.?|sr\.?|ii|iii|iv|v|vi)$/i;

// The words that belong to the surname in front of them: Nick Van Exel is
// Van Exel, Juan Carlos de la Fuente is de la Fuente.
const PARTICLES = new Set([
	"al",
	"bin",
	"da",
	"das",
	"de",
	"del",
	"della",
	"den",
	"der",
	"di",
	"do",
	"dos",
	"du",
	"el",
	"la",
	"le",
	"st.",
	"ten",
	"ter",
	"van",
	"von",
]);

// The name a paper uses after the first one, or undefined for a man who goes
// by one name already (Nene).
export const surnameOf = (name: string): string | undefined => {
	const tokens = name.trim().split(/\s+/);
	while (tokens.length > 2 && SUFFIX.test(tokens.at(-1)!)) {
		tokens.pop();
	}
	if (tokens.length < 2 || SUFFIX.test(tokens.at(-1)!)) {
		return undefined;
	}
	let start = tokens.length - 1;
	while (start > 1 && PARTICLES.has(tokens[start - 1]!.toLowerCase())) {
		start -= 1;
	}
	const surname = tokens.slice(start).join(" ");
	return surname.length >= 2 ? surname : undefined;
};

// Surnames that would read as a word of the sentence on their own: "He had
// 20 points" for Lin He is a pronoun to every reader. They stay whole names.
const PLAIN_WORDS = new Set([
	"a",
	"an",
	"and",
	"at",
	"but",
	"by",
	"for",
	"he",
	"her",
	"him",
	"his",
	"i",
	"in",
	"it",
	"its",
	"man",
	"men",
	"of",
	"on",
	"or",
	"she",
	"so",
	"the",
	"them",
	"they",
	"to",
	"us",
	"we",
	"you",
]);

const firstNameOf = (name: string): string => name.trim().split(/\s+/)[0] ?? "";

const escape = (s: string) =>
	s.replaceAll(/[$()*+.?[\\\]^{|}]/g, String.raw`\$&`);

// Letters on either side mean the match is inside another word or name.
const nameRegex = (names: string[]) =>
	new RegExp(
		String.raw`(?<![\p{L}\p{M}\p{N}'-])(?:${names
			.sort((a, b) => b.length - a.length)
			.map(escape)
			.join("|")})(?![\p{L}\p{M}\p{N}-])`,
		"gu",
	);

export type ReferenceTeam = { name: string; region: string };

// Which surnames can stand alone in a story about these men and these teams:
// the ones no one else in the box score shares, and that are not anyone's
// first name or any team's name or city. Mapped back to the full name.
export const shortNames = (
	players: readonly string[],
	teams: readonly ReferenceTeam[],
): Map<string, string> => {
	const names = [...new Set(players.map((n) => n.trim()).filter(Boolean))];
	const taken = new Map<string, number>();
	const reserved = new Set<string>();
	for (const t of teams) {
		for (const word of `${t.region} ${t.name}`.split(/\s+/)) {
			reserved.add(word.toLowerCase());
			// "King's layup" in a story about the Kings.
			reserved.add(word.toLowerCase().replace(/s$/, ""));
		}
		reserved.add(t.region.toLowerCase());
		reserved.add(t.name.toLowerCase());
	}
	for (const n of names) {
		reserved.add(firstNameOf(n).toLowerCase());
		const s = surnameOf(n)?.toLowerCase();
		if (s) {
			taken.set(s, (taken.get(s) ?? 0) + 1);
		}
	}
	const out = new Map<string, string>();
	for (const n of names) {
		const s = surnameOf(n);
		if (
			s &&
			taken.get(s.toLowerCase()) === 1 &&
			!reserved.has(s.toLowerCase()) &&
			!PLAIN_WORDS.has(s.toLowerCase()) &&
			// A surname that is a whole other man's full name ("Grant" for
			// "Jerami Grant" beside a "Grant") is caught above; one that reads as
			// part of a team name is too.
			!s.split(/\s+/).some((w) => reserved.has(w.toLowerCase()))
		) {
			out.set(n, s);
		}
	}
	return out;
};

// Where the story starts: after the headline and, on a day recap, the deck.
const splitHead = (text: string): { head: string; body: string } => {
	const parts = text.split("\n\n");
	let i = 0;
	while (i < parts.length && /^\*[^\n]*\*$/.test(parts[i]!.trim())) {
		i += 1;
	}
	return {
		head: parts.slice(0, i).join("\n\n"),
		body: parts.slice(i).join("\n\n"),
	};
};

const join = (head: string, body: string) =>
	head === "" ? body : body === "" ? head : `${head}\n\n${body}`;

const atSentenceStart = (text: string, at: number) =>
	at === 0 ||
	/(?:[!.?]["')]?\s+|\n)$/.test(text.slice(Math.max(0, at - 4), at));

// The surname on every mention after a man's first in the body. The headline
// keeps whole names, and his first mention in the story is whole whatever the
// headline said.
export const secondReferences = (
	text: string,
	players: readonly string[],
	teams: readonly ReferenceTeam[],
): string => {
	const short = shortNames(players, teams);
	if (short.size === 0) {
		return text;
	}
	const { head, body } = splitHead(text);
	const seen = new Set<string>();
	// "The Lakers' Devin Fields" tells a reader whose man he is; by his
	// second mention they know, and "Los Angeles' Fields" reads as a noun.
	const tags = teams
		.flatMap((t) => [
			t.name.trim() === "" ? "" : `[Tt]he ${escape(t.name.trim())}`,
			t.region.trim() === "" ? "" : escape(t.region.trim()),
		])
		.filter(Boolean);
	const names = [...short.keys()]
		.sort((a, b) => b.length - a.length)
		.map(escape)
		.join("|");
	const re = new RegExp(
		String.raw`(?<![\p{L}\p{M}\p{N}'-])(${tags.length > 0 ? `(?:${tags.join("|")})'s? ` : "(?!)"})?(${names})(?![\p{L}\p{M}\p{N}-])`,
		"gu",
	);
	const out = body.replace(
		re,
		(
			match: string,
			tag: string | undefined,
			full: string,
			at: number,
			whole: string,
		) => {
			if (!seen.has(full)) {
				seen.add(full);
				return match;
			}
			let s = short.get(full)!;
			if (atSentenceStart(whole, at)) {
				s = s[0]!.toUpperCase() + s.slice(1);
			}
			// "...a free throw from Gary Trent Jr. The Lakers..." - the period
			// that closed "Jr." was closing the sentence too.
			if (
				full.endsWith(".") &&
				/^(?:\s+["'(]?[A-Z]|\s*$)/.test(whole.slice(at + match.length))
			) {
				s += ".";
			}
			return s;
		},
	);
	return join(head, out);
};

// The checker's side of it: a story with surnames in it, back to whole names,
// so every number can still be held against the man it belongs to.
export const expandSecondReferences = (
	text: string,
	players: readonly string[],
	teams: readonly ReferenceTeam[],
): string => {
	const short = shortNames(players, teams);
	if (short.size === 0) {
		return text;
	}
	// "de la Fuente", and "De la Fuente" opening a sentence.
	const full = new Map<string, string>();
	for (const [n, s] of short) {
		full.set(s, n);
		full.set(s[0]!.toUpperCase() + s.slice(1), n);
	}
	// A surname is only ever written after its full name, so protect those
	// first and expand what is left.
	const marks: string[] = [];
	const guarded = text.replace(nameRegex([...short.keys()]), (m: string) => {
		marks.push(m);
		return `\uE000${marks.length - 1}\uE000`;
	});
	return guarded
		.replace(nameRegex([...full.keys()]), (m: string) => full.get(m) ?? m)
		.replaceAll(/\uE000(\d+)\uE000/g, (_, i: string) => marks[Number(i)]!);
};

// THE CITY FOR THE NICKNAME, now and then. Only where the grammar is the same
// either way - a possessive ("the Pacers' bench", "Indiana's bench") or the
// object of a preposition or of "beat" - since "the Pacers were" and "Indiana
// was" are not interchangeable. Never in the opening sentence, never on a
// team's first mention, at most twice a team, and never for a city both teams
// share.
const OBJECT_BEFORE = "for|over|against|to|from|past|beat|by|than|with";

export const cityReferences = (
	text: string,
	teams: readonly ReferenceTeam[],
	players: readonly string[],
	seed: number,
): string => {
	if (teams.length !== 2) {
		return text;
	}
	const [a, b] = teams as [ReferenceTeam, ReferenceTeam];
	const regionA = a.region.trim();
	const regionB = b.region.trim();
	if (regionA.toLowerCase() === regionB.toLowerCase()) {
		return text;
	}
	const words = new Set<string>();
	for (const n of players) {
		for (const w of n.split(/\s+/)) {
			words.add(w.toLowerCase());
		}
	}
	const usable = (t: ReferenceTeam) => {
		const region = t.region.trim();
		return (
			region !== "" &&
			t.name.trim() !== "" &&
			region.toLowerCase() !== t.name.trim().toLowerCase() &&
			/^\p{Lu}/u.test(region) &&
			!/^the\s/i.test(region) &&
			!region.split(/\s+/).some((w) => words.has(w.toLowerCase()))
		);
	};
	const rng = rngFromSeed(seed);
	const { head, body } = splitHead(text);
	// The opening sentence is the wire lede, nickname and record and all.
	const ledeEnd = body.split(/(?<=[!.?])\s+(?=[A-Z])/)[0]!.length;
	const sentenceOf = (text: string, at: number) =>
		text.slice(0, at).split(/(?<=[!.?])\s+(?=[A-Z])/).length;
	const used = new Set<number>();
	let out = body;
	for (const t of teams) {
		if (!usable(t)) {
			continue;
		}
		const name = escape(t.name.trim());
		const re = new RegExp(
			String.raw`\b(?:(${OBJECT_BEFORE}) the ${name}(?!['\w])(?! \()(?!, wh)|[Tt]he ${name}'s?(?=\s))`,
			"g",
		);
		const other = teams.find((x) => x !== t)!.name.trim();
		// The sentence a match sits in.
		const around = (at: number) => {
			const from = out.slice(0, at).search(/[^!.?]*$/);
			const to = out.slice(at).search(/[!.?](?:\s|$)/);
			return out.slice(from, to < 0 ? undefined : at + to);
		};
		const spots = [...out.matchAll(re)].filter(
			(m) =>
				m.index > ledeEnd &&
				out.slice(0, m.index).includes(t.name) &&
				// "15-of-28 for Utah, 5-of-25 for the Cavaliers" breaks a pair
				// that was written to match.
				!around(m.index)
					.toLowerCase()
					.includes(
						(m[1] ? `${m[1]} the ${other}` : `the ${other}'`).toLowerCase(),
					),
		);
		// Two at most, one in a short story; and not every time.
		const budget = out.length > 600 ? 2 : 1;
		const chosen: RegExpExecArray[] = [];
		for (const m of spots) {
			const sentence = sentenceOf(out, m.index);
			// Not in the sentence right after another one, either: "The
			// second half belonged to Toronto. Devin Nance kicked in 22 points
			// for Toronto" is the city twice running.
			// Nor beside one that names the city already: "Game 6 is
			// win-or-go-home for Oklahoma City. The series shifts to Oklahoma
			// City."
			const sentences = out.split(/(?<=[!.?])\s+(?=[A-Z])/);
			const near = [sentence - 1, sentence, sentence + 1].some(
				(n) =>
					used.has(n) || (sentences[n - 1] ?? "").includes(t.region.trim()),
			);
			if (chosen.length < budget && !near && rng() < 0.4) {
				chosen.push(m);
				used.add(sentence);
			}
		}
		// Right to left, so the earlier indexes stay good.
		for (const m of chosen.reverse()) {
			const region = t.region.trim();
			const replacement = m[1]
				? `${m[1]} ${region}`
				: m[0].endsWith("'") || m[0].endsWith("'s")
					? poss(region)
					: region;
			out =
				out.slice(0, m.index) + replacement + out.slice(m.index + m[0].length);
		}
	}
	return join(head, out);
};

// A WOMEN'S LEAGUE. Every template says "he", "his" and "six men in double
// figures"; in a league of women that is wrong in nearly every recap. The
// last pass turns them, with every player and team name held out of it first
// - a surname "He", a club called the Iron Men - so only the prose changes.
export const inWomensLeague = (
	text: string,
	players: readonly string[],
	teams: readonly ReferenceTeam[],
): string => {
	const keep = [
		...new Set(
			[
				...players,
				// A bare surname only where one can stand alone (see
				// PLAIN_WORDS): a "He" on its own is the pronoun.
				...players.map((n) => {
					const s = surnameOf(n) ?? "";
					return PLAIN_WORDS.has(s.toLowerCase()) ? "" : s;
				}),
				...teams.flatMap((t) => [t.name, t.region]),
			]
				.map((n) => n.trim())
				.filter((n) => n !== ""),
		),
	];
	const marks: string[] = [];
	const held =
		keep.length === 0
			? text
			: text.replace(nameRegex(keep), (m: string) => {
					marks.push(m);
					return `${marks.length - 1}`;
				});
	return held
		.replaceAll(/\bthe open man\b/g, "the open teammate")
		.replaceAll(/\bmen\b/g, "players")
		.replaceAll(/\bman\b/g, "player")
		.replaceAll(/\bhimself\b/g, "herself")
		.replaceAll(/\bHimself\b/g, "Herself")
		.replaceAll(/\bhim\b/g, "her")
		.replaceAll(/\bhis\b/g, "her")
		.replaceAll(/\bHis\b/g, "Her")
		.replaceAll(/\bhe\b/g, "she")
		.replaceAll(/\bHe\b/g, "She")
		.replaceAll(/(\d+)/g, (_, i: string) => marks[Number(i)]!);
};
