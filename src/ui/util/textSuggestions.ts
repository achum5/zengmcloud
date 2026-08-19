// THE SUGGESTION STRIP'S BRAIN, for the on-screen chat keyboard.
//
// What Apple's keyboard does with corrections is an OS service running a
// language model, and no web page can call it - that door closed the moment
// the chat stopped focusing an input, which is the entire point of the
// in-page keyboard. What CAN be honest is the QuickType bar itself: tap a
// suggestion to take it, and NOTHING is ever replaced automatically. An
// autocorrect that silently rewrites words needs to be right almost always
// to be tolerable, and a dictionary this size is not; a tappable suggestion
// costs nothing when it is wrong.
//
// The two automatic things it does do are the two safe ones iOS also does:
// a lone "i" becomes "I", and shift arms itself at the start of a message
// and after sentence-ending punctuation.

// Ordered by rough frequency - earlier wins ties. Common English heavy on
// the words a game chat actually uses, plus the basketball and franchise
// vocabulary of this app.
const WORD_LIST = `
the be to of and a in that have it for not on with he as you do at this but
his by from they we say her she or an will my one all would there their what
so up out if about who get which go me when make can like time no just him
know take people into year your good some could them see other than then now
look only come its over think also back after use two how our work first well
way even new want because any these give day most us
is are was were been has had did does doing having am
basketball game games play played playing player players team teams season seasons win
wins winning won lose loses losing lost score scored scores scoring point
points shot shots shoot shooting miss missed misses makes made basket court quarter quarters half halftime overtime clock buzzer beater
hoop rim rebound rebounds board boards assist assists steal steals block blocks
blocked foul fouls fouled turnover turnovers three threes triple double
net arc corner wing post
dunk dunks dunked layup layups jumper fadeaway floater airball swish clutch
bench benched starter starters starting lineup rotation minutes stats stat
lead leads leading leader trailed trailing tied tie run streak comeback
defense defensive offense offensive press paint perimeter fastbreak
coach coaching timeout ref refs referee whistle call calls flagrant
technical ejected injury injured hurt healthy trade traded trades deal
deals sign signed signing signs contract extension waive waived cut
draft drafted pick picks lottery prospect prospects rookie rookies veteran
roster cap salary luxury tax free agent agents agency buyout
playoffs playoff finals champ champs champion champions championship ring
rings title titles seed seeding bracket series sweep swept elimination
eliminated standings record streaky mvp allstar franchise dynasty rebuild
tank tanking sim simmed simming league legacy retire retired jersey
number goat washed cooked cooking fire ice cold hot heater slump
yes yeah yep nah nope ok okay sure right wrong true false real fake
great nice sweet awesome amazing incredible insane crazy wild sick
terrible awful horrible trash garbage worse worst better best huge big
small tiny long short fast slow quick late early soon never always
sometimes maybe probably definitely honestly literally actually seriously
totally pretty very really super too much many few more less least
close closer closest far behind ahead final finally almost barely exactly
lol lmao omg wow bro dude man guys buddy fam gg ez wp glhf rip oof yikes
haha hahaha hey hi hello yo sup thanks thank please sorry oops whoops
gonna wanna gotta kinda sorta dunno lemme gimme cmon welp whatever
what's that's it's he's she's who's there's here's let's i'm i'll i've i'd
you're you'll you've don't doesn't didn't can't couldn't won't wouldn't
shouldn't isn't aren't wasn't weren't hasn't haven't hadn't ain't we're
we'll we've they're they'll they've
watch watching watched looking looked feel feeling felt keep
keeps keeping kept let lets goes going gone comes coming came
need needs needed try tries trying tried calling called tell telling
told ask asking asked start started stop stopped happen happens happened
believe hope hoped wish guess bet bets betting odds spread favorite
underdog upset upsets choke choked choking blew blow blown collapse
tonight today tomorrow yesterday morning night week weekend month next
last again still already yet ever once twice game-winner
` as const;

export const WORDS: readonly string[] = WORD_LIST.split(/\s+/).filter(
	(w) => w.length > 0,
);

const RANK = new Map<string, number>();
for (const [i, w] of WORDS.entries()) {
	if (!RANK.has(w)) {
		RANK.set(w, i);
	}
}

// One typo's distance: a single substitution, insertion, deletion, or swap of
// adjacent letters (the "hte" -> "the" case, which plain Levenshtein charges
// two for and every real keyboard forgives).
export const withinOneTypo = (a: string, b: string): boolean => {
	if (a === b) {
		return true;
	}
	const la = a.length;
	const lb = b.length;
	if (Math.abs(la - lb) > 1) {
		return false;
	}
	if (la === lb) {
		// One substitution, or one adjacent swap.
		let i = 0;
		while (i < la && a[i] === b[i]) {
			i++;
		}
		if (i === la) {
			return true;
		}
		if (a[i] === b[i + 1] && a[i + 1] === b[i]) {
			return a.slice(i + 2) === b.slice(i + 2);
		}
		return a.slice(i + 1) === b.slice(i + 1);
	}
	// One insertion/deletion: align the longer against the shorter.
	const [short, long] = la < lb ? [a, b] : [b, a];
	let i = 0;
	while (i < short.length && short[i] === long[i]) {
		i++;
	}
	return short.slice(i) === long.slice(i + 1);
};

// Match the suggestion's case to what was typed: "Definit" completes to
// "Definitely", "OMG" corrects to "OMG"-cased words.
const matchCase = (typed: string, suggestion: string): string => {
	if (typed.length >= 2 && typed === typed.toUpperCase()) {
		return suggestion.toUpperCase();
	}
	if (typed[0] !== undefined && typed[0] === typed[0]!.toUpperCase()) {
		return suggestion.charAt(0).toUpperCase() + suggestion.slice(1);
	}
	return suggestion;
};

// Up to `limit` suggestions for the word being typed: completions when it is
// on its way to known words, corrections when it looks like a typo of one.
// Corrections come first when the word itself is not a word - that is the
// case where the person most wants help.
export const suggestionsFor = (typed: string, limit = 3): string[] => {
	const w = typed.toLowerCase();
	if (w.length < 2) {
		return [];
	}
	const isWord = RANK.has(w);

	const completions: string[] = [];
	const corrections: string[] = [];
	for (const word of WORDS) {
		if (word !== w && word.startsWith(w)) {
			completions.push(word);
		} else if (
			!isWord &&
			w.length >= 3 &&
			word !== w &&
			withinOneTypo(w, word)
		) {
			corrections.push(word);
		}
	}

	const merged = isWord
		? completions
		: [...corrections.slice(0, 2), ...completions];
	const seen = new Set<string>();
	const out: string[] = [];
	for (const word of merged) {
		if (!seen.has(word)) {
			seen.add(word);
			out.push(matchCase(typed, word));
		}
		if (out.length >= limit) {
			break;
		}
	}
	return out;
};

// The word currently being typed: the trailing run of letters/apostrophes,
// or "" at a word boundary.
export const currentWordOf = (text: string): string =>
	/['A-Za-z]+$/.exec(text)?.[0] ?? "";

// Take a suggestion: replace the word being typed with it, plus the space
// iOS adds so the next word can start immediately.
export const applySuggestion = (text: string, suggestion: string): string =>
	text.replace(/['A-Za-z]*$/, suggestion) + " ";

// Append one typed key, with the single automatic correction that is always
// safe: a lone "i" capitalizes when a space or punctuation seals it off.
export const afterKeyText = (text: string, key: string): string => {
	const next = text + key;
	if (/[\s!,.:;?]/.test(key)) {
		return next.replace(/(^|\s)i(?=[\s!,.:;?]$)/, "$1I");
	}
	return next;
};

// Should shift arm itself, given the text so far? At the start of a message
// and after sentence-ending punctuation - exactly when iOS does.
export const shouldAutoShift = (text: string): boolean =>
	text === "" || /[!.?]\s+$/.test(text);
