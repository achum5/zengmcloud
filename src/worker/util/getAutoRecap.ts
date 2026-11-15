import { clockLeft, type GameFlowSide } from "../../common/gameFlow.ts";
import type {
	RecapAverages,
	RecapDayStandings,
	RecapGame,
	RecapInjuryOut,
	RecapPlayer,
	RecapTeam,
} from "./getDayGamesForRecap.ts";
import {
	aNum,
	fixArticles,
	aNumWord,
	aOrdinal,
	beginRecapBatch,
	cap,
	claimVerb,
	clearPhraseMemory,
	doubleCategories,
	endRecapBatch,
	gbText,
	injuryPhrase,
	isInBatch,
	lowerInjury,
	naturalList,
	nick,
	numWord,
	ordinal,
	otPhrase,
	pick,
	pickByDay,
	pickSentence,
	plural,
	poss,
	resetVerbLedger,
	rngFromSeed,
	seeded,
	shuffle,
	spelledNum,
	statPhrase,
	stripHtml,
	takeVerb,
	teamStats,
	theNick,
} from "./recapText.ts";
export { beginRecapBatch, endRecapBatch, pick } from "./recapText.ts";
import {
	benchBeat,
	homeRoadBeat,
	milestoneBeat,
	nextGameBeat,
	playerCountBeat,
	foldSeasonHigh,
	playerHighBeat,
	playerStreakBeat,
	returnBeat,
	scoringNormBeat,
	seriesBeat,
	seriesShapeBeat,
	standingsBeat,
	teamHighBeat,
	vsOpponentBeat,
	type BeatContext,
} from "./recapBeats.ts";
import { shotName, winningShotKind, withSettledLead } from "./recapFinish.ts";
import { gameHeadline, writeDayRecap } from "./recapDay.ts";
import {
	composeCast,
	composeLede,
	composeStory,
	type StoryInput,
} from "./recapStory.ts";
import {
	cityReferences,
	inWomensLeague,
	secondReferences,
	type ReferenceTeam,
} from "./recapNames.ts";

// A procedural, no-AI recap engine. getAutoRecap turns one RecapGame into a
// bold headline plus a couple of tight paragraphs; getAutoDayRecap turns a whole
// day's slate into a league-wide wrap. Every clause is anchored to a real number,
// name, or event from the box score - nothing is invented - and wording is varied
// by a per-game seed so a slate never reads from one template. These are the
// always-on recaps; the "Copy AI Prompt" flow stays the on-demand upgrade.

// --- Seeded RNG (mulberry32), so the same game always reads the same way -------

// --- Small text helpers --------------------------------------------------------

// Soften back-to-back sentences that open with the same "The <Team>" subject by
// turning the second one's subject into "They" - so "The Spurs led wire to wire.
// The Spurs shot 54.7%." becomes "...The Spurs led wire to wire. They shot
// 54.7%." The `[a-z]` guard means a two-word nickname ("The Trail Blazers ...")
// is left alone rather than mangled.
export const dedupeSubjects = (
	sentences: string[],
	otherNick?: string,
	// The nicknames in play, so a MULTI-WORD one is recognised. The pattern
	// below can only see a one-word nickname followed by a lowercase verb, so
	// every team called two things - the Blue Chips, the Gold Club, the Trail
	// Blazers - was silently skipped, and their recaps read "The Blue Chips took
	// over in the fourth. The Blue Chips turned 29 turnovers into offense."
	knownNicks: readonly string[] = [],
): string[] => {
	const out = [...sentences];
	// A sentence with two subjects leaves nothing for "They" to attach to (the
	// two-team injury line "The Hawks were without X; the Timberwolves were
	// without Y."). Merely naming the other team as the OBJECT ("The Celtics
	// topped the Grizzlies 115-105.") is still a clean antecedent.
	//
	// The SEMICOLON is the signal, and the only one: a two-team injury line is
	// always built by joining its clauses with "; ". Also testing for " were
	// without " caught every ONE-team injury sentence too, and those are a
	// perfectly good antecedent - which is why "The Aztecs were without Parker
	// Dismuke (sprained knee). The Aztecs ran their streak to 4." kept the name
	// twice.
	// A sentence with two subjects leaves nothing for "They" to attach to. The
	// semicolon marks the two-team injury line; "the X and the Y" marks every
	// other compound subject, and without it the day wrap produced "The Pistons
	// and the Magic combined for 245 points. They stretched their run to 11
	// wins." - where "they" reads as both clubs and means one.
	// And "The Thunder led by 10 in the second, but the Timberwolves led
	// 65-64 at halftime" hands the reader's eye to the SECOND team: a "They"
	// two sentences on meant the Thunder and read as the Timberwolves.
	// So does a "but" clause that ends on the other club in any order: "The
	// Lakers ran off 11 straight points in the second, but it was 65-63 the
	// Pistons at halftime. They trailed..." - "They" read as the Pistons.
	const butTheOther = (sentence: string) => {
		const clause = /, but (.*)$/.exec(sentence)?.[1];
		const own = /^The (\w+(?: \w+)?)/.exec(sentence)?.[1] ?? "";
		return (
			!!clause &&
			knownNicks.some((n) => !own.startsWith(n) && clause.includes(n))
		);
	};
	const ambiguous = (sentence: string) =>
		sentence.includes(";") ||
		/^The \w+[\w ]*? and the \w+/.test(sentence) ||
		/, (?:but|and) the [A-Z]\w*[\w ]*? (?:led|were|took|won|came|had)\b/.test(
			sentence,
		) ||
		butTheOther(sentence);

	const subjectOf = (sentence: string): string | undefined => {
		for (const n of knownNicks) {
			if (sentence.startsWith(`The ${n} `)) {
				return n;
			}
		}
		// A nickname is capitalised: "The win lifted the Jazz into fourth.
		// The win finished a 2-0 season sweep" is two notes on the result,
		// and collapsing the second to "They finished" handed it to a team.
		return /^The ([\dA-Z]\w*) [a-z]/.exec(sentence)?.[1];
	};

	// A player named on the losing side without his team ("... 19 points in
	// defeat") is a competing subject too: reaching past him to the winner's
	// last sentence turned "The Rockets get the 76ers in two days" into "They
	// get the 76ers", with the reader's eye on the Bucks.
	const loserTail =
		/\b(?:in defeat|in the loss|in a losing cause|for the losing side)\b/;

	for (let i = 1; i < out.length; i++) {
		// A compound subject is never collapsed: "The Timberwolves and the
		// Celtics have split four meetings" became "They and the Celtics".
		// The other club arriving later in the sentence is no bar to it,
		// though - it only makes the sentence a poor antecedent for the NEXT
		// one: "The Raptors were up 58-40 at the break. The Raptors were still
		// up 75-71 going into the fourth, and the Bucks won the fourth" can
		// read "They were still up".
		if (out[i]!.includes(";") || /^The \w+[\w ]*? and the \w+/.test(out[i]!)) {
			continue;
		}
		// "The win lifted the Jazz into fourth. The win finished a season
		// sweep" - a result, or the two clubs together, carried on from the
		// sentence just before: "It also ...", "They ...".
		const noun = /^The (win|loss|teams) [a-z]/.exec(out[i]!)?.[1];
		if (noun) {
			if (out[i - 1]!.startsWith(`The ${noun} `)) {
				const rest = out[i]!.slice(`The ${noun} `.length);
				out[i] = noun === "teams" ? `They ${rest}` : `It also ${rest}`;
			}
			continue;
		}
		const cur = subjectOf(out[i]!);
		if (!cur) {
			continue;
		}
		// The immediately preceding sentence, or the one before it when what sits
		// between is about a player rather than a team.
		for (const back of [1, 2]) {
			const j = i - back;
			if (j < 0) {
				break;
			}
			const candidate = out[j]!;
			if (ambiguous(candidate)) {
				break;
			}
			if (back === 2) {
				// Only reach past a sentence that keeps the reader's eye where it
				// was: an impersonal one, or one that already leans on the same
				// pronoun. A sentence about a named man moves it - "No one scored
				// more than Keegan Lowry... They got 20 from Zeke Dunn" read as
				// Lowry's team, and meant the other one.
				const between = out[i - 1]!;
				// A "They ..." sentence between is the same subject carrying
				// on, and the other club's name as its object does not move
				// the reader's eye: "The Bucks stayed top... They took the
				// series with the Nets. The Bucks improved to 8-0 at home"
				// was three "The Bucks" in a row.
				// "From there, they outscored the Rockets 61-49" carries the same
				// subject as surely as "They outscored...".
				const carriesThey = /^They\b|^[A-Z][\w ]{0,24}, they\b/.test(between);
				if (
					ambiguous(between) ||
					subjectOf(between) ||
					loserTail.test(between) ||
					(!!otherNick && between.includes(otherNick) && !carriesThey) ||
					!(carriesThey || /^(?:It|That|There)\b/.test(between))
				) {
					break;
				}
			}
			if (subjectOf(candidate) === cur) {
				out[i] = `They ${out[i]!.slice(`The ${cur} `.length)}`;
				break;
			}
		}
	}
	return out;
};

// THE SAME MAN, THREE SENTENCES RUNNING. The third paragraph is built one
// beat at a time and three of them can be about the star - his shooting
// against his season, his season high, his streak - so it read "Yuta Dunn shot
// 64.3%... Yuta Dunn had not scored more than 47... It was the 16th game in a
// row Yuta Dunn has reached 20." A sentence that opens on the name the one
// before it opened on gets "He" (or "His"). Only then: reaching further back,
// or past a sentence about somebody else, is how a pronoun ends up pointing at
// the wrong man.
export const dedupePlayerSubjects = (
	sentences: string[],
	names: readonly string[],
): string[] => {
	const out = [...sentences];
	const opensOn = (sentence: string): string | undefined =>
		names.find(
			(name) =>
				sentence.startsWith(`${name} `) || sentence.startsWith(`${name}'`),
		);
	// "It was Obi King's best night on the glass. It was Obi King's fourth
	// straight double-double." - the possessive frame, one note after another.
	const itWas = (sentence: string): string | undefined =>
		names.find((name) => sentence.startsWith(`It was ${name}'`));
	const afterPossessive = (sentence: string, name: string): string => {
		const rest = sentence.slice(name.length);
		return rest.startsWith("'s ") ? rest.slice(3) : rest.slice(2);
	};
	for (let i = 1; i < out.length; i++) {
		const framed = itWas(out[i]!);
		if (framed) {
			const prev = sentences[i - 1]!;
			const othersIn = names.some(
				(other) => other !== framed && prev.includes(other),
			);
			// "It was the sixth game in a row Obi Brooks has reached 20. It was
			// Obi Brooks' best night on the glass this season." - any "It was"
			// about him alone sets up the "also".
			if (
				(itWas(prev) === framed ||
					(prev.startsWith("It was ") && prev.includes(framed))) &&
				!othersIn
			) {
				out[i] = `It was also his ${afterPossessive(out[i]!.slice(7), framed)}`;
			} else if (opensOn(prev) === framed && !othersIn) {
				out[i] = `It was his ${afterPossessive(out[i]!.slice(7), framed)}`;
			}
			continue;
		}
		const name = opensOn(out[i]!);
		// A sentence that opens on him but ends on somebody else ("Evan
		// Jackson's 27 points fronted the Thunder, and Franz Jackson added
		// 18") leaves "He" pointing at the wrong man. The name stays.
		const another = (sentence: string, who: string) =>
			names.some((other) => other !== who && sentence.includes(other));
		if (
			!name ||
			opensOn(sentences[i - 1]!) !== name ||
			another(sentences[i - 1]!, name)
		) {
			// "...four in a row for Obi King. The 14 rebounds were a season high
			// for Obi King." - the name closing two sentences running. When the
			// sentence before names him and this one names nobody else, the
			// second becomes "for him".
			// ...and the sentence before names nobody else either: "19 points
			// and 13 rebounds from Cade Ingram, and Anthony Ingram supplied 18.
			// The 13 rebounds were a season high for him." - him was Anthony.
			const closing = names.find(
				(n) =>
					out[i]!.endsWith(` for ${n}.`) &&
					sentences[i - 1]!.includes(n) &&
					!names.some((other) => other !== n && out[i]!.includes(other)) &&
					!another(sentences[i - 1]!, n),
			);
			if (closing) {
				out[i] = `${out[i]!.slice(0, -(closing.length + 1))}him.`;
			}
			continue;
		}
		const rest = out[i]!.slice(name.length);
		out[i] = rest.startsWith("'s ")
			? `His ${rest.slice(3)}`
			: rest.startsWith("' ")
				? `His ${rest.slice(2)}`
				: `He ${rest.slice(1)}`;
	}
	return out;
};

// Two short sentences on the same subject become one. "The Bucks never
// trailed. They got double figures out of 7 men." is a reporter's "The Bucks
// never trailed and got double figures out of 7 men." Only plain pairs: a
// first sentence already carrying an "and", a participial tail ("..., led
// by") or a second subject is left as it is, and a pair joins once - the
// third sentence starts fresh.
export const joinShortPairs = (sentences: string[]): string[] => {
	const out: string[] = [];
	const plain = (t: string) =>
		!/ and |;| - |, (?:led|winning|breaking|leading|outscoring|making|thanks)\b/.test(
			t,
		) &&
		// "It was 35 made free throws to 7 in the Team's favor and led 55-46 at
		// halftime" - an impersonal opener has no subject for the second
		// clause to share. And the halftime score stands on its own, as it
		// does in every wire recap.
		!/^It\b/.test(t) &&
		// ...and so does the moment the lead changed hands for the last
		// time: "took the lead for good on his basket with 11:27 left and won
		// the glass 52-40" is two facts that have nothing to do with each
		// other.
		!/halftime|the half\b|the break|second half|for good|ahead for the last time|went in front/.test(
			t,
		) &&
		// ...and so does any other beat of the game's course: "They did not
		// trail again and forced 24 turnovers", "They were 5 up going into the
		// fourth and hit 14 from deep" tied a moment of the game to a number
		// from the box score.
		!/\btrail(?:ed)? again|going into the|entering the|got back/.test(t) &&
		// ...and so does the next game: "The Pelicans were beaten at the line,
		// 3 attempts to 24, and play the Pacers next, at home."
		!/\bnext\b/.test(t) &&
		// ...and so does who was out: "The Mavericks ran their streak to four
		// and were missing Scoot Dunn (sore back)", "took both meetings with
		// the Suns this season and were without Jaden Mathis".
		!/\bwere (?:again )?(?:without|missing)\b|\bsat out\b|\bkept [A-Z]/.test(t);
	// The FIRST sentence has to start with its own subject, because the join
	// hands its subject to the second clause. "Over six games the aggregate
	// favors the Nets by 37." + "They meet again tomorrow." became "...the
	// aggregate favors the Nets by 37 and meet again tomorrow" - a plural verb
	// hanging off a singular subject buried behind an adverbial opener. Only a
	// sentence that leads with "The Bucks", "Zeke Dunn" or a pronoun can lend
	// what follows a subject.
	const leadsWithSubject = (t: string) =>
		/^(?:The [A-Z]|They\b|He\b|[A-Z][\w'.’-]+ [A-Z])/.test(t);
	for (let i = 0; i < sentences.length; i++) {
		const a = sentences[i]!;
		const b = sentences[i + 1];
		const pronoun = b && /^(?:They|He) /.exec(b)?.[0];
		if (
			b &&
			pronoun &&
			a.length <= 75 &&
			b.length <= 60 &&
			a.endsWith(".") &&
			plain(a) &&
			leadsWithSubject(a) &&
			plain(b) &&
			// Not on the same verb: "They led the rest of the way and led by
			// as many as 27."
			!a.includes(` ${b.slice(pronoun.length).split(" ")[0]} `)
		) {
			// After an appositive ("controlled the boards, 54-38") the join
			// takes a comma of its own.
			const glue = a.includes(",") ? ", and " : " and ";
			out.push(`${a.slice(0, -1)}${glue}${b.slice(pronoun.length)}`);
			i += 1;
		} else {
			out.push(a);
		}
	}
	return out;
};

// --- Player performance --------------------------------------------------------

// "47%" over "47.0%": a percentage is printed to one decimal only when the
// decimal says something.
const fmtPct = (x: number): string =>
	Number.isInteger(Math.round(x * 10) / 10)
		? String(Math.round(x))
		: x.toFixed(1);

// A rough impact score, only ever used to pick which player's night is the story.
// Never shown to the user.
export const impact = (p: RecapPlayer): number =>
	p.pts +
	0.4 * p.reb +
	0.7 * p.ast +
	1.4 * p.stl +
	1.4 * p.blk -
	0.7 * p.tov +
	0.5 * p.fg -
	0.4 * p.fga +
	0.4 * p.tp;

// Who the story is about. Deliberately the SAME weighting the box-score card
// uses to pick the player it features (common/getBestPlayerBoxScore), because
// the recap is printed directly under that card: when the two disagreed, the
// card said "Etan Thomas 13 PTS, 9 TRB, 3 BLK" and the sentence beneath it
// opened on Brad Miller's 19 points, and the page looked like it hadn't read
// itself.
//
// `impact` below stays as it is for ORDERING the rest of the mentions, where
// its efficiency and turnover terms genuinely help decide who is worth a
// sentence next.
const storyScore = (p: RecapPlayer): number => {
	// A triple-double is a story whatever the sums say - a 17-10-14 night was
	// losing the star slot to a teammate's 16 points and 5 steals, because five
	// steals weigh like a scoring binge in the raw score. A double-double gets
	// a nudge, enough to beat a same-size line that is only points.
	const cats = doubleCategories(p).length;
	const bonus = cats >= 3 ? 10 : cats === 2 ? 2.5 : 0;
	// A steal or a block at 1.2, not 1.7: at 1.7 one weighed as much as
	// three and a half points, and "Naji Hayes 25 and 12 with two steals and
	// a block" took the lede from a teammate's 32 and 12.
	return (
		0.5 * p.pts + 0.5 * p.reb + 0.5 * p.ast + 1.2 * p.blk + 1.2 * p.stl + bonus
	);
};

// A line that is the story whoever outscored it.
const rareLine = (p: RecapPlayer): boolean =>
	doubleCategories(p).length >= 3 ||
	p.reb >= 15 ||
	p.ast >= 13 ||
	p.blk >= 6 ||
	p.stl >= 6;

// How big a losing side's line is, in points: the rebounds or assists that
// make a night more than its scoring count for something.
const loserLineSize = (p: RecapPlayer): number =>
	p.pts +
	(p.reb >= 20 ? 10 : p.reb >= 15 ? 6 : 0) +
	(p.ast >= 12 ? 6 : 0) +
	(doubleCategories(p).length >= 3 ? 10 : 0);

// A losing line big enough to be the headline, or the lede's last clause,
// for a game decided by `margin`. A 24-and-10 in a 19-point loss is not the
// story of the night; a 39-and-21 is, by any margin.
const loserLineCarries = (p: RecapPlayer, margin: number): boolean =>
	loserLineSize(p) >= 22 + Math.max(0, margin - 5);

// A line with nothing in it worth a sentence: under 18 points, no
// double-double, and none of the other columns even at the loose bar.
const quietLine = (p: RecapPlayer): boolean =>
	p.pts < 18 &&
	doubleCategories(p).length < 2 &&
	p.reb < 6 &&
	p.ast < 5 &&
	p.stl < 3 &&
	p.blk < 3;

export const bestOf = (players: RecapPlayer[]): RecapPlayer | undefined => {
	let best: RecapPlayer | undefined;
	let bestScore = -Infinity;
	for (const p of players) {
		const s = storyScore(p);
		if (s > bestScore) {
			bestScore = s;
			best = p;
		}
	}
	return best;
};

export const supportingCast = (
	players: RecapPlayer[],
	star: RecapPlayer,
): RecapPlayer[] =>
	players.filter((p) => p !== star).sort((a, b) => impact(b) - impact(a));

// WHO THE STORY IS ABOUT. The best line on the winning side, with the
// corrections a desk makes before it writes the lede.
//
// The impact score occasionally crowns a low-scoring stat-stuffer (a 9-point,
// 4-steal night) whose line reads absurd as the LEAD ("Nene chipped in 9
// points... as the Lakers won"). When the pick barely scored, the lead goes
// to the winner's best real scoring line instead - the impact pick still
// shows up in the supporting-cast sentence. The same by a hair: a 16-point,
// 4-rebound, 2-block line edging a 16-and-10 on the impact score gave the
// lead to the man with nothing to say about him.
//
// And in a game decided at the end, the man who decided it. A 20-point night
// that included the go-ahead basket with 47 seconds left is the story of a
// two-point game, whatever the whole-line score says about a teammate's 18
// and 8 - but only when his scoring stands up on its own, so a 9-point man
// whose layup happened to be the last lead change does not headline over the
// 30-point night that got the team there.
export const storyPlayerOf = (
	game: RecapGame,
	shape: Shape,
): RecapPlayer | undefined => {
	let star = bestOf(shape.winner.players) ?? bestOf(shape.loser.players);
	if (star && star.pts < 18 && shape.winner.players.includes(star)) {
		const low = star;
		const alt = supportingCast(shape.winner.players, star).find(
			(p) =>
				(low.pts < 12 && p.pts >= 15) ||
				// A 9-point, 6-rebound line is nobody's lede when a teammate
				// outscored it; a double-double that small can still be one.
				(low.pts < 12 && doubleCategories(low).length < 2 && p.pts > low.pts) ||
				(low.pts < 15 && p.pts >= low.pts + 7) ||
				// "Zeke Foster posted 13 points and 3 blocks as the Clippers
				// pulled away" over a teammate's 19: a line that small and
				// without a double-double leads only a game nobody outscored it.
				(low.pts < 15 &&
					doubleCategories(low).length < 2 &&
					p.pts >= low.pts + 5) ||
				(low.pts < 15 && p.pts >= 12 && doubleCategories(p).length >= 2) ||
				// A 16-and-8 against a teammate's 25 and 12.
				(p.pts >= low.pts + 8 && doubleCategories(p).length >= 2) ||
				p.pts >= low.pts + 12,
		);
		// A 12-and-12 is a line; a teammate's 22 in a game decided by six is
		// the story. The double-double keeps the lede only when it is a big
		// one, or when nobody outscored him by ten.
		// And a big glass night still gives way to a teammate who outscored
		// it by a dozen: "11 points and 17 rebounds" headlined a win in which
		// somebody else scored 36, and "14 and 16 leads the Nets" one where a
		// teammate had 28.
		if (
			alt &&
			(doubleCategories(low).length < 2 ||
				(alt.pts >= low.pts + 10 &&
					low.reb < 15 &&
					low.ast < 12 &&
					doubleCategories(low).length < 3) ||
				(alt.pts >= 25 &&
					alt.pts >= low.pts + 12 &&
					doubleCategories(low).length < 3))
		) {
			star = alt;
		}
	}
	if (star && shape.winner.players.includes(star) && quietLine(star)) {
		const quiet = star;
		const alt = supportingCast(shape.winner.players, star).find(
			(p) =>
				(p.pts >= 12 && doubleCategories(p).length >= 2) ||
				p.pts >= quiet.pts + 4,
		);
		if (alt) {
			star = alt;
		}
	}
	// THE NIGHT'S SCORER. A teammate who outscored the pick by eight or more
	// - 25 on 10-of-13 against a 17-and-10 on 6-of-17, 20 against an 11-and-10
	// in a 43-point rout - is the man a reporter leads with. The pick keeps
	// the lede only with a line nobody outscores: a triple-double, 15
	// rebounds, 13 assists, six blocks or six steals.
	// A rare line holds off all but a night bigger still: 40 points and 10
	// assists is the story over a teammate's 23 and 16, whatever the
	// rebounds.
	if (star && shape.winner.players.includes(star)) {
		const pick = star;
		const top = shape.winner.players
			.filter((p) => p !== pick)
			.sort((a, b) => b.pts - a.pts)[0];
		const bigger =
			top &&
			top.pts >= 18 &&
			top.pts >= pick.pts + 8 &&
			(!rareLine(pick) ||
				(doubleCategories(pick).length < 3 &&
					top.pts >= 30 &&
					top.pts >= pick.pts + 12 &&
					doubleCategories(top).length >= 2));
		// Two double-doubles: the one that scored more, by six or more. "Devin
		// Ingram had 16 points and 11 rebounds" led a win in which Cade Ingram
		// had 23 and 11.
		const twoDoubles =
			top &&
			top.pts >= 18 &&
			top.pts >= pick.pts + 6 &&
			!rareLine(pick) &&
			doubleCategories(pick).length === 2 &&
			doubleCategories(top).length >= 2;
		if (top && (bigger || twoDoubles)) {
			star = top;
		}
	}
	// A NIGHT HE SHOT HIMSELF OUT OF. Twenty and ten on 5-of-20 led a win in
	// which a teammate scored 27 on 12-of-17, and the second sentence of the
	// lede was "The shooting was not pretty for Xavier Ingram". Under 36% on
	// 14 or more shots, the pick gives way to a teammate who outscored him on
	// an honest night; a rare line holds unless that man scored eight more,
	// and a triple-double always holds.
	if (
		star &&
		shape.winner.players.includes(star) &&
		star.fga >= 14 &&
		star.fg / star.fga < 0.36 &&
		doubleCategories(star).length < 3
	) {
		const pick = star;
		const top = shape.winner.players
			.filter((p) => p !== pick)
			.sort((a, b) => b.pts - a.pts)[0];
		if (
			top &&
			top.pts >= pick.pts + (rareLine(pick) ? 8 : 3) &&
			top.fga > 0 &&
			top.fg / top.fga >= 0.45
		) {
			star = top;
		}
	}
	// A LINE THAT IS ONLY POINTS is the story only when nobody on his side
	// outscored it. Picked for what the line does not show - seven rebounds,
	// two steals - he read as the wrong man: "Miles Ingram's 24 leads the
	// Pelicans" over a teammate's 26, "Keegan Lowry scores 30" over one's 32.
	if (
		star &&
		shape.winner.players.includes(star) &&
		statPhrase(star) === plural(star.pts, "point")
	) {
		const top = [...shape.winner.players].sort((a, b) => b.pts - a.pts)[0];
		if (top && top !== star && top.pts > star.pts) {
			star = top;
		}
	}
	const last = game.flow?.lastLead;
	const close = shape.margin <= 5 || shape.ot > 0;
	if (
		close &&
		star &&
		last &&
		last.pid !== undefined &&
		last.period >= shape.regPeriods &&
		last.clock <= 120 &&
		last.side === (game.teams[0].tid === shape.winner.tid ? 0 : 1)
	) {
		const hero = shape.winner.players.find((p) => p.pid === last.pid);
		if (
			hero &&
			hero !== star &&
			(hero.pts >= star.pts + 3 || hero.pts >= 25) &&
			doubleCategories(star).length < 3
		) {
			star = hero;
		}
	}
	return star;
};

// A shooting flourish for a big scorer, when the line is efficient or three-heavy.
// A count of threes is something he did, not what the line was made of:
// "30 points, 8 rebounds, and 7 assists on 4 threes" put the rebounds on
// the threes too. Said as he did it: "..., hitting 4 threes".
export const shootingFlourish = (p: RecapPlayer): string | undefined => {
	if (p.tp >= 6) {
		return `hitting ${p.tp} three-pointers`;
	}
	if (p.fga >= 10 && p.fg / p.fga >= 0.6) {
		return `on ${p.fg}-of-${p.fga} shooting`;
	}
	// A hot night from deep is told as the split when the attempts are
	// known: "hitting 5 of 7 threes" says more than "on 5 threes". Not
	// "after going 5-of-7", which put the shooting before the points it
	// made, and not "29 points, including 4-of-7 from three": a shooting
	// line is not part of a points total.
	if (p.tp >= 4 && p.tpa >= p.tp && p.tp / p.tpa >= 0.55) {
		return p.tp === p.tpa
			? `hitting all ${p.tp} of his threes`
			: `hitting ${p.tp} of ${p.tpa} threes`;
	}
	if (p.tp >= 4) {
		// Not "with": every caller introduces the stat line with "with", and
		// "led all scorers with 33 points with 5 threes" doubled it.
		return `hitting ${p.tp} threes`;
	}
	// A night lived at the line.
	if (p.ft >= 12 && p.fta >= p.ft) {
		return p.ft === p.fta
			? `making all ${p.ft} of his free throws`
			: `making ${p.ft} of ${p.fta} free throws`;
	}
	return undefined;
};

// The line with its flourish: "38 points and 17 rebounds, making 13 of 17
// free throws", "25 points, hitting 5 of 9 threes".
// The aside is closed with a comma when the sentence goes on: "26 points,
// hitting 4 of 6 threes, as the Magic got past the Bucks".
// A bare count of four or five threes is left off a line that already runs
// to three numbers: "37 points, 13 assists, and 7 steals, hitting 4 threes,
// in the Nuggets' win" buried the line under it.
const withFlourish = (
	line: string,
	flourish: string | undefined,
	continues = false,
): string =>
	flourish === undefined ||
	(/^hitting [45] threes$/.test(flourish) && line.includes(", and "))
		? line
		: /^(?:hitting|making) /.test(flourish)
			? `${line}, ${flourish}${continues ? "," : ""}`
			: `${line} ${flourish}`;

// The verb that carries the LEAD sentence, scaled to how big the star's night
// was. Never a weak "added"/"chipped in" - those are for the supporting cast,
// and they undersold the game's best player ("Pau Gasol added 22" as a lead).
// "scored" only when the line is points-only ("scored 25 points and 6 assists"
// is not English).
export const leadVerb = (
	p: RecapPlayer,
	hasExtras: boolean,
	rng: () => number,
): string => {
	let pool: string[];
	if (p.pts >= 35) {
		// "poured in" is a points verb: "poured in 36 points and 8 assists"
		// pours in the assists too.
		pool = hasExtras
			? ["erupted for", "exploded for", "piled up"]
			: ["poured in", "erupted for", "exploded for", "piled up"];
	} else if (p.pts >= 25) {
		pool = hasExtras
			? ["had", "posted", "put up", "racked up"]
			: ["scored", "had", "put up", "racked up"];
	} else {
		// Not "totaled": the word of a box score, not of a writer, and it
		// opened one recap in six.
		pool = hasExtras
			? ["posted", "put up", "had"]
			: ["scored", "had", "put up"];
	}
	return takeVerb(rng, pool);
};

// The averages a player brought INTO the game (playoff line in the postseason,
// else the season line), only when there are enough games to be meaningful.
const enteringLine = (
	p: RecapPlayer,
	playoffs: boolean,
): RecapAverages | undefined => {
	const line =
		playoffs && p.playoffAvg && p.playoffAvg.gp >= 2
			? p.playoffAvg
			: p.seasonAvg;
	return line && line.gp >= 3 ? line : undefined;
};

// Everyone from either roster whose name appears in a stretch of text. Reading
// the finished prose rather than tracking as we go means the set cannot fall out
// of step with what a reader actually sees.
const namesIn = (text: string, shape: Shape): Set<string> => {
	const out = new Set<string>();
	for (const t of [shape.winner, shape.loser]) {
		for (const p of t.players) {
			if (text.includes(p.name)) {
				out.add(p.name);
			}
		}
	}
	return out;
};

// --- Team aggregates -----------------------------------------------------------

// --- Team-name labels ----------------------------------------------------------

// --- Game-shape detection ------------------------------------------------------

export type Shape = {
	game: RecapGame;
	winner: RecapTeam;
	loser: RecapTeam;
	margin: number;
	ot: number;
	wq: number[];
	lq: number[];
	regPeriods: number;
	// Winner's largest deficit at any period boundary (0 if it never trailed).
	comebackFrom: number;
	comebackPeriod: number;
	// The loser's biggest lead at any point, from the score log (0 without
	// one). A team can be 20 down in the second quarter and level by the
	// break, which the period boundaries never see.
	loserPeak: number;
	// Margin the winner led/trailed by entering the final regulation period.
	marginEnteringLast: number;
	// Never behind. From the score log when there is one: leading at every
	// period boundary is not the same thing - a team 10 down midway through
	// the first and ahead after it "led wire to wire" in 121 of 953 recaps.
	wireToWire: boolean;
	// The single period where the winner most outscored the loser (a decisive
	// run), with the winners' margin going into it and coming out of it.
	bigRun?: {
		period: number;
		wpts: number;
		lpts: number;
		margin: number;
		before: number;
		after: number;
	};
};

export const analyzeShape = (game: RecapGame): Shape => {
	const [home, away] = game.teams;
	const winner = game.winnerTid === home.tid ? home : away;
	const loser = winner === home ? away : home;
	const margin = winner.pts - loser.pts;
	const ot = game.overtimes ?? 0;

	// Quarters that do not add up to the final are no quarters: a comeback
	// or a big third quarter read from them contradicts the score.
	const sum = (q: number[]) => q.reduce((a, b) => a + b, 0);
	const adds =
		Array.isArray(winner.ptsQtrs) &&
		Array.isArray(loser.ptsQtrs) &&
		sum(winner.ptsQtrs) === winner.pts &&
		sum(loser.ptsQtrs) === loser.pts;
	const wq = adds ? winner.ptsQtrs! : [];
	const lq = adds ? loser.ptsQtrs! : [];
	const regPeriods = Math.max(1, wq.length - ot);

	let comebackFrom = 0;
	let comebackPeriod = 0;
	let wireToWire = false;
	let marginEnteringLast = 0;
	let bigRun: Shape["bigRun"];

	if (wq.length > 0 && wq.length === lq.length) {
		let wCum = 0;
		let lCum = 0;
		let ledEveryBoundary = true;
		for (let i = 0; i < wq.length; i++) {
			const runMargin = (wq[i] ?? 0) - (lq[i] ?? 0);
			const before = wCum - lCum;
			wCum += wq[i] ?? 0;
			lCum += lq[i] ?? 0;
			const diff = wCum - lCum;
			if (!bigRun || runMargin > bigRun.margin) {
				bigRun = {
					period: i + 1,
					wpts: wq[i] ?? 0,
					lpts: lq[i] ?? 0,
					margin: runMargin,
					before,
					after: diff,
				};
			}
			if (i === regPeriods - 2) {
				marginEnteringLast = diff;
			}
			if (i < wq.length - 1 && diff <= 0) {
				ledEveryBoundary = false;
			}
			if (-diff > comebackFrom) {
				comebackFrom = -diff;
				comebackPeriod = i + 1;
			}
		}
		wireToWire = ledEveryBoundary && comebackFrom === 0 && wq.length >= 2;
	}

	const loserPeak = game.flow ? game.flow.maxLead[sideOf(game, loser)] : 0;
	if (game.flow && loserPeak > 0) {
		wireToWire = false;
	}

	return {
		game,
		winner,
		loser,
		margin,
		ot,
		wq,
		lq,
		regPeriods,
		comebackFrom,
		comebackPeriod,
		loserPeak,
		marginEnteringLast,
		wireToWire,
		bigRun,
	};
};

// What the winners' biggest quarter did to the game. It "broke it open" only
// when the game was still a game going in and they came out of it clear; a
// 28-19 second quarter that turned an eight-point deficit into a one-point
// halftime lead broke nothing open (it turned the game, barely), and one
// that padded a 20-point lead did neither.
export const bigQuarterRole = (
	shape: Shape,
): "broke" | "turned" | undefined => {
	const r = shape.bigRun;
	if (!r) {
		return undefined;
	}
	if (r.before <= 6 && r.after >= 10) {
		return "broke";
	}
	if (r.before < 0 && r.after >= 5) {
		return "turned";
	}
	return undefined;
};

// The losing streak the WINNER carried into this game, when it was long enough
// to be a story (undefined otherwise). The 76ers ending an 18-game skid was
// being written as "it had been a rough stretch for the 76ers, 0-9 in their
// previous 9" - true, and a tenth of the story.
export const snappedSkid = (shape: Shape): number | undefined => {
	const before = shape.winner.streakBefore;
	return before && !before.won && before.count >= 5 ? before.count : undefined;
};

// A streak long enough to be the headline: the winner's skid from seven, or
// the loser's winning streak from six. The longer one when both.
export const longStreakEnded = (
	shape: Shape,
): { kind: "skid" | "run"; n: number } | undefined => {
	const skid = snappedSkid(shape) ?? 0;
	const lRun = shape.loser.streakBefore;
	const run = lRun?.won ? lRun.count : 0;
	if (skid >= 7 && skid >= run) {
		return { kind: "skid", n: skid };
	}
	return run >= 6 ? { kind: "run", n: run } : undefined;
};

export const isUpset = (game: RecapGame, shape: Shape): boolean => {
	if (shape.margin === 0) {
		return false;
	}
	const s = game.spread;
	// A 4-point dog winning is a coin flip that landed; "pull the upset" was
	// the second most common headline on the slate. Six points is where a
	// result starts to surprise anyone.
	if (!s || s.points < 6) {
		return false;
	}
	// Not when the records say otherwise. The line reads who is in uniform
	// tonight, the reader reads the standings: "Trail Blazers pull the upset
	// over the 76ers" of a 22-8 team beating a 10-21 one reads as a mistake,
	// whatever the books thought.
	const wr = shape.winner.record;
	const lr = shape.loser.record;
	if (!game.playoffs && wr && lr) {
		const wGames = wr.won + wr.lost - 1;
		const lGames = lr.won + lr.lost - 1;
		if (
			wGames >= 10 &&
			lGames >= 10 &&
			(wr.won - 1) / wGames - lr.won / lGames >= 0.15
		) {
			return false;
		}
	}
	return s.favTid === shape.loser.tid;
};

// A game-winning (or game-tying) shot from the clutch-play log, parsed into the
// player, shot type, and timing so it can lead a headline and get its own beat
// in the body. `shot` is display-ready: the log's generic "basket" becomes
// "buzzer-beater" / "game-winner", a real shot type ("three-pointer") is kept.
export const clutchShot = (
	game: RecapGame,
):
	| {
			name: string;
			shot: string;
			tying: boolean;
			buzzer: boolean;
			seconds?: number;
	  }
	| undefined => {
	for (const raw of game.clutchPlays) {
		const text = stripHtml(raw);
		const m =
			/^(.+?) made a (game-winning|game-tying) ([ a-z-]+?)(?: with| at| to|\.|$)/.exec(
				text,
			);
		if (m) {
			const buzzer = /at the buzzer|with no time on the clock/.test(text);
			const secondsMatch = /with ([\d.:]+) seconds remaining/.exec(text);
			const seconds = secondsMatch
				? Number.parseFloat(secondsMatch[1]!.replace(":", "."))
				: undefined;
			const rawShot = m[3]!.trim();
			const tying = m[2] === "game-tying";
			// The sim's note says "basket"; the finish log knows whether it
			// was a layup or a jumper, which is what a desk would print.
			const known =
				rawShot === "basket" && !tying && seconds !== undefined
					? winningShotKind(game, game.winnerTid, seconds)
					: undefined;
			const shot =
				rawShot === "basket"
					? buzzer && !tying
						? known && known !== "layup"
							? `buzzer-beating ${known}`
							: "buzzer-beater"
						: tying
							? "game-tying basket"
							: (known ?? "game-winner")
					: rawShot;
			return {
				name: m[1]!.trim(),
				shot,
				tying,
				buzzer,
				seconds: Number.isFinite(seconds) ? seconds : undefined,
			};
		}
	}
	// The sim only calls the LAST score of the game a game-winner, so a
	// go-ahead basket with a second left, answered by one free throw, went
	// unrecorded and the recap opened on the box score. The score log has it.
	const last = game.flow?.lastLead;
	if (last && last.clock <= 5 && game.teams.length === 2) {
		const winnerSide = game.teams[0].tid === game.winnerTid ? 0 : 1;
		const regPeriods =
			(game.teams[0].ptsQtrs?.length ?? 0) - (game.overtimes ?? 0);
		const name = nameOfPid(game, last.pid);
		// A trip to the line for two is not "a go-ahead basket", and every
		// headline that names the shot takes it as one thing ("X's free
		// throw sinks the Suns"): the lede's go-ahead clause tells those.
		const trip = (game.flow?.finish ?? []).find(
			(x) =>
				x.side === winnerSide &&
				x.period === last.period &&
				x.kind === "ft" &&
				x.pts >= 2 &&
				Math.abs(x.clock - last.clock) < 0.15,
		);
		if (
			last.side === winnerSide &&
			regPeriods > 0 &&
			last.period >= regPeriods &&
			name &&
			!trip
		) {
			return {
				name,
				shot:
					last.by === 1
						? "free throw"
						: (winningShotKind(game, game.winnerTid, last.clock) ??
							"game-winner"),
				tying: false,
				buzzer: false,
				seconds: Math.round(last.clock * 10) / 10,
			};
		}
	}
	return undefined;
};

// THE BASKET THAT DECIDED A CLOSE GAME, when it came in the last minute and
// the sim did not call it a game-winner (it calls only the last score that).
// "Miles Foster answered with a jumper with 19.9 seconds to go that put the
// Pacers up 108-107" was the third sentence of the story under a lede about
// six men in double figures; a desk leads with it. The lede's closing clause:
// " on Miles Foster's go-ahead jumper with 19.9 seconds left".
export const lateGoAhead = (
	game: RecapGame,
	shape: Shape,
	shot: ReturnType<typeof clutchShot>,
): { text: string; name: string; what: string; left: string } | undefined => {
	if ((shot && !shot.tying) || shape.margin > 3 || shape.ot > 0) {
		return undefined;
	}
	const last = game.flow?.lastLead;
	const wSide = sideOf(game, shape.winner);
	if (
		!last ||
		last.side !== wSide ||
		last.period < shape.regPeriods ||
		!(last.clock <= 60)
	) {
		return undefined;
	}
	const name = nameOfPid(game, last.pid);
	const left = clockLeft(last.clock);
	// What it was, from the log's own row: "free throws", not "basket".
	const e = (game.flow?.finish ?? [])
		.filter(
			(x) =>
				x.side === wSide &&
				x.period === last.period &&
				Math.abs(x.clock - last.clock) < 0.15,
		)
		.at(-1);
	if (!name || !left || !e) {
		return undefined;
	}
	const kind = shotName(e).replace(/^(?:two|three) /, "");
	return {
		text: ` on ${poss(name)} go-ahead ${kind} with ${left} left`,
		name,
		what: kind,
		left,
	};
};

// "a free throw with 0.5 seconds left" / "a buzzer-beater" - the winning shot
// described, without the shooter. Shared by the standalone beat and the
// merged-into-the-lead form (used when the shooter IS the lead star, so the
// name isn't repeated in back-to-back sentences).
export const clutchWhat = (
	shot: NonNullable<ReturnType<typeof clutchShot>>,
): string => {
	const timing = shot.buzzer
		? " at the buzzer"
		: shot.seconds !== undefined
			? ` with ${plural(shot.seconds, "second")} left`
			: "";
	// Don't say "won it with a buzzer-beater at the buzzer"...
	if (shot.shot === "buzzer-beater") {
		return "a buzzer-beater";
	}
	// ...and "won it with a game-winner" is a tautology - in this position the
	// generic label becomes the concrete play.
	const what =
		shot.shot === "game-winner" ? "a go-ahead basket" : `a ${shot.shot}`;
	return `${what}${timing}`;
};

export type ClutchShot = NonNullable<ReturnType<typeof clutchShot>>;

export const scoreTag = (shape: Shape): string => {
	const ot =
		shape.ot > 0 ? ` (${shape.ot === 1 ? "OT" : `${shape.ot}OT`})` : "";
	return `${shape.winner.pts}-${shape.loser.pts}${ot}`;
};

// --- Postseason context --------------------------------------------------------

export const roundName = (round: number, numRounds: number): string => {
	if (round === numRounds) {
		return "the Finals";
	}
	if (round === numRounds - 1) {
		return "the Conference Finals";
	}
	if (round === numRounds - 2) {
		return "the Conference Semifinals";
	}
	if (round === 1) {
		return "the First Round";
	}
	return `Round ${round}`;
};

// A rich clause (or two) describing what this playoff/play-in game meant and how
// it moved the series - the heart of a postseason recap.
export type PostseasonContext = {
	// A short stakes phrase for the headline ("a Game 7", "an elimination game"),
	// joined with " in ".
	headlineTag?: string;
	// A headline ending that carries its own preposition, for the stakes " in "
	// cannot express - "to win the title", "to reach the Finals". Wins over
	// headlineTag when both are set.
	//
	// It is a PARTICIPIAL clause, so its implied subject is whatever the headline
	// made the subject - and the winner is the only side it can truthfully be.
	// "Pandas fall to the Blizzard 107-79, advancing to the Conference
	// Semifinals" says the swept team advanced. buildHeadline drops the
	// loser-subject templates whenever this is set.
	//
	// Without this a championship-clinching game got a headline indistinguishable
	// from a Tuesday in January: "Celtics lean on Paul Pierce to take down the
	// Pistons", with the fact that it won them the title left to the body.
	headlineTail?: string;
	// Set when this game won the series. The headline is built from it directly
	// - a clinch IS the headline, not a clause hung off a Tuesday one - and the
	// day wrap and the notability score read it instead of pattern-matching
	// the body's prose (which is how a 4-2 series win came to be headlined as
	// "take a 4-2 lead": the sentence it looked for had been reworded).
	clinch?: {
		title: boolean;
		sweep: boolean;
		// The last possible game of the series - "Game 7" of a best-of-7.
		decider: boolean;
		deciderTag: string;
		games: number;
		wAfter: number;
		lBefore: number;
		nextRound: string;
		seedUpset: boolean;
		wSeed?: number;
		lSeed?: number;
	};
	// The series after this game, when it is not over: what a playoff
	// headline is actually about ("take a 2-1 lead", "even the series",
	// "force a Game 7", "stay alive").
	state?: {
		round: string;
		wAfter: number;
		lBefore: number;
		gameNo: number;
		need?: number;
		facingElimination: boolean;
		forcesDecider: boolean;
		deciderTag: string;
		seedUpset: boolean;
		wSeed?: number;
		lSeed?: number;
	};
	// A play-in game's stakes, for the lede after the score: " to claim the
	// No. 7 seed", " to end the Pacers' season".
	playInLede?: string;
	// Full sentences for the body.
	sentences: string[];
};

export const postseasonContext = (
	game: RecapGame,
	shape: Shape,
	// Optional on purpose. notability() calls this only to SCORE a game, and
	// pick() mutates the shared phrase memory - handing that call a live rng
	// burns a night's rotation on text nobody ever reads. Without an rng the
	// canonical phrasing is used and the pools are left alone.
	rng?: () => number,
): PostseasonContext => {
	const out: PostseasonContext = { sentences: [] };
	const choose = (options: string[], poolId: string): string =>
		rng ? pick(rng, options, poolId) : options[0]!;
	// WITH THE ARTICLE. Every sentence built here opens with one of these, and
	// they were bare nicknames while the rest of the piece says "the Celtics" -
	// so a playoff recap read "The Celtics got past the Pistons 104-96... Celtics
	// took a 3-1 lead in the Conference Semifinals." flowSentence has always used
	// theNick for the same reason.
	const w = theNick(shape.winner);
	const l = theNick(shape.loser);

	// THE STAKES ARE THE STORY, and they lead: "Keegan Green posted 23
	// points and 13 assists as the Lakers cruised past the Pacers" opened
	// the night the Lakers took the last playoff berth, and the berth waited
	// for the end of the second paragraph. The lede says what the game won
	// ("...beat the Pacers 122-101 to claim the last playoff berth"), the
	// headline carries it, and the body adds the other side.
	if (game.playIn) {
		const p = game.playIn;
		const seed =
			typeof p.prizeSeed === "number"
				? `the No. ${p.prizeSeed} seed`
				: undefined;
		if (p.kind === "seed7v8") {
			out.headlineTail = `, claiming ${seed ?? "the higher seed"}`;
			out.playInLede = ` to claim ${seed ?? "the higher seed"}`;
			out.sentences.push(
				`${cap(l)} drop into a win-or-go-home game for the final spot.`,
			);
		} else if (p.kind === "seed9v10") {
			out.headlineTail = `, ending ${poss(l)} season`;
			out.playInLede = ` to end ${poss(l)} season`;
			out.sentences.push(`${cap(w)} move on to the final play-in game.`);
		} else {
			out.headlineTail = ", claiming the last playoff berth";
			out.playInLede = ` to claim the last playoff berth${seed ? ` as ${seed}` : ""}`;
			out.sentences.push(`${cap(poss(l))} season is over.`);
		}
		return out;
	}

	const s = game.series;
	if (!s) {
		return out;
	}

	const rnd = roundName(s.round, s.numRounds);
	const winnerIsHome = shape.winner.abbrev === s.homeAbbrev;
	const wBefore = winnerIsHome ? s.homeWon : s.awayWon;
	const lBefore = winnerIsHome ? s.awayWon : s.homeWon;
	const wAfter = wBefore + 1;
	const gameNo = wBefore + lBefore + 1;
	const need =
		typeof s.bestOf === "number" && s.bestOf > 0
			? Math.floor(s.bestOf / 2) + 1
			: undefined;
	const facingElimination = need !== undefined && lBefore === need - 1;
	// The last possible game of the series (Game 5 of a best-of-5, Game 7 of a
	// best-of-7) - a true winner-take-all. "Game 7" is only literal for a best-of-7.
	const isDecider =
		typeof s.bestOf === "number" && s.bestOf > 1 && gameNo === s.bestOf;
	const deciderTag = s.bestOf === 7 ? "a Game 7" : `a decisive Game ${gameNo}`;

	if (isDecider) {
		out.headlineTag = deciderTag;
	} else if (facingElimination) {
		out.headlineTag = "an elimination game";
	}

	// THE SEEDS, when they say something. A 2-vs-3 series is just a series; a
	// low seed beating a high one is the fact everyone repeats about it, and the
	// numbers were sitting in the payload unread.
	const wSeed = winnerIsHome ? s.homeSeed : s.awaySeed;
	const lSeed = winnerIsHome ? s.awaySeed : s.homeSeed;
	const seedUpset =
		typeof wSeed === "number" &&
		typeof lSeed === "number" &&
		wSeed - lSeed >= 3 &&
		// Not in the Finals: the two sides' seeds are from different
		// conferences, and "#7 Magic cut #4 Rockets' lead" compares numbers
		// that never met.
		s.round < s.numRounds;

	// The clinching case: series won.
	if (need !== undefined && wAfter >= need) {
		// The headline has to carry it. A title is the biggest single result in
		// the game and it was reading like any other win.
		// A PARTICIPLE, not an infinitive. Several headline templates already
		// contain a "to" ("Celtics lean on Paul Pierce to take down the
		// Pistons"), and appending another one stacked them: "...to take down the
		// Pistons to win the title". A comma clause reads correctly after every
		// template in the pool.
		const clinchTail =
			s.round === s.numRounds
				? ", clinching the title"
				: `, advancing to ${roundName(s.round + 1, s.numRounds)}`;
		// A clincher that is ALSO the decider keeps both facts. Overriding the tag
		// dropped "Game 7" out of the piece entirely, which is the one detail
		// nobody would leave out.
		out.headlineTail = isDecider
			? `${clinchTail} in ${deciderTag}`
			: clinchTail;
		out.clinch = {
			title: s.round === s.numRounds,
			sweep: lBefore === 0,
			decider: isDecider,
			deciderTag,
			games: wAfter + lBefore,
			wAfter,
			lBefore,
			nextRound:
				s.round === s.numRounds
					? "the title"
					: roundName(s.round + 1, s.numRounds),
			seedUpset,
			wSeed,
			lSeed,
		};
		// Two sentences: what the winner did, then WHOSE SEASON JUST ENDED. The
		// losing side of a series result was never written at all - the recap
		// reported who advanced and simply stopped, which reads like half a
		// sentence in the one round where the loser's story is the bigger one.
		out.sentences.push(
			...(s.round === s.numRounds
				? [
						lBefore === 0
							? `${cap(w)} are champions, sweeping the title series ${wAfter}-0.`
							: `${cap(w)} are champions, taking the title series ${wAfter}-${lBefore}.`,
						isDecider
							? `It came down to the last night of the season, and ${theNick(shape.loser)} finish as runners-up.`
							: seedUpset
								? `The ${seeded(lSeed)} ${nick(shape.loser)} finish as runners-up to the No. ${wSeed} seed.`
								: `${cap(theNick(shape.loser))} finish as runners-up.`,
					]
				: [
						// A round is not a series: "closed out the First Round 4-2",
						// "evened the First Round at 1-1" and "took the First Round
						// 4-1 and move on" all said the wrong noun.
						isDecider
							? `${cap(w)} closed out the series ${wAfter}-${lBefore} with ${
									s.bestOf === 7 ? "a Game 7" : `a decisive Game ${gameNo}`
								} win and advanced.`
							: lBefore === 0
								? `${cap(w)} finished a ${wAfter}-0 sweep and advanced.`
								: choose(
										[
											`${cap(w)} closed out the series ${wAfter}-${lBefore} and advanced.`,
											`${cap(w)} won the series ${wAfter}-${lBefore}.`,
											`The series goes to ${w}, ${wAfter}-${lBefore}.`,
										],
										"seriesClinch",
									),
						seedUpset
							? // "The #4 seed puts out the #1 seed" named nobody, and
								// the wire writes "No. 4", not "#4".
								`The ${seeded(lSeed)} ${nick(shape.loser)} are out, beaten by the No. ${wSeed} seed.`
							: lBefore === 0
								? `${cap(theNick(shape.loser))} go out without winning a game.`
								: choose(
										[
											`${poss(cap(theNick(shape.loser)))} season is over.`,
											`${cap(theNick(shape.loser))} are done for the year.`,
											`It is the end of the road for ${theNick(shape.loser)}.`,
										],
										"seriesLoserOut",
									),
					]),
		);
		// Franchise history, on the one night it matters. "First championship in
		// franchise history" vs "first since 2019" vs "back-to-back" is the line
		// every real title story carries, and the data was already in teamSeasons.
		if (s.round === s.numRounds) {
			const th = shape.winner.titleHistory;
			if (th) {
				if (th.titles.length === 0) {
					// Only worth saying for a franchise with a real past - a
					// three-season expansion team's "first in franchise history"
					// reads like a joke.
					if (th.seasons >= 3) {
						out.sentences.push(
							"It is the first championship in franchise history.",
						);
					}
				} else {
					const last = th.titles.at(-1)!;
					if (last === th.season - 1) {
						out.sentences.push("That makes it back-to-back championships.");
					} else {
						out.sentences.push(
							`It is the ${ordinal(th.titles.length + 1)} championship in franchise history, and the first since ${last}.`,
						);
					}
				}
			}
		}
		return out;
	}

	out.state = {
		round: rnd,
		wAfter,
		lBefore,
		gameNo,
		need,
		facingElimination,
		forcesDecider:
			need !== undefined && wAfter === need - 1 && lBefore === need - 1,
		deciderTag,
		seedUpset,
		wSeed,
		lSeed,
	};

	// The series continues - describe the new state and the stakes met.
	//
	// Every branch here used to have exactly ONE phrasing, so a slate of eight
	// first-round games produced "The X drew first blood in the First Round, 1-0."
	// six times down the page. pick() with an explicit pool id rotates them
	// across the night; the id is required because the default key is derived
	// from the rendered strings, which differ per game and so never collide.
	//
	// roundName() returns an ARTICLED name ("the First Round"), so it can only
	// sit where an article is wanted: "in the First Round" is fine, "their the
	// First Round deficit" and "trail the First Round 3-2" were not.
	if (wAfter === lBefore) {
		out.sentences.push(
			choose(
				[
					`${cap(w)} evened the series at ${wAfter}-${wAfter} with a Game ${gameNo} win.`,
					`Game ${gameNo} went to ${w}, and the series is tied ${wAfter}-${wAfter}.`,
					`${cap(w)} answered in Game ${gameNo} to square the series at ${wAfter}-${wAfter}.`,
				],
				"seriesEven",
			),
		);
	} else if (wAfter > lBefore) {
		out.sentences.push(
			facingElimination
				? choose(
						[
							`${cap(w)} staved off elimination and trail ${lBefore}-${wAfter} in ${rnd}.`,
							`${cap(w)} live to play another game, down ${lBefore}-${wAfter} in ${rnd}.`,
						],
						"seriesSurviveTrail",
					)
				: wBefore === 0 && lBefore === 0
					? choose(
							[
								`${cap(w)} are up ${wAfter}-${lBefore} in ${rnd}.`,
								`${cap(w)} lead the series ${wAfter}-${lBefore} after taking the opener.`,
								`Game 1 of ${rnd} went to ${w}.`,
								`${cap(w)} struck first in ${rnd}.`,
							],
							"seriesOpener",
						)
					: choose(
							[
								`${cap(w)} took a ${wAfter}-${lBefore} lead in ${rnd}.`,
								`That is a ${wAfter}-${lBefore} edge for ${w} in ${rnd}.`,
								`${cap(w)} moved ahead ${wAfter}-${lBefore} in ${rnd}.`,
								`${cap(w)} are up ${wAfter}-${lBefore} in ${rnd}.`,
							],
							"seriesLead",
						),
		);
	} else {
		// Winner still trails the series even after this win.
		out.sentences.push(
			facingElimination
				? choose(
						[
							`${cap(w)} staved off elimination but still trail in ${rnd}, ${lBefore}-${wAfter}.`,
							`${cap(w)} survived, though ${l} still lead the series ${lBefore}-${wAfter}.`,
						],
						"seriesSurviveStillTrail",
					)
				: choose(
						[
							`${cap(w)} cut their series deficit to ${lBefore}-${wAfter}.`,
							`${cap(w)} got one back, and now trail ${lBefore}-${wAfter} in ${rnd}.`,
						],
						"seriesTrail",
					),
		);
	}

	// WHAT IT SETS UP. A series score on its own is a fact; what a reader wants
	// next is who is now in trouble, and the old context stopped before saying
	// it. The nickname of the losing side was computed and thrown away here -
	// this is the sentence it was always missing.
	if (need !== undefined) {
		const winnerNeeds = need - wAfter;
		const loserNeeds = need - lBefore;
		if (winnerNeeds === 1 && loserNeeds > 1) {
			out.sentences.push(
				loserNeeds >= 3
					? // "straight" is already plural in this idiom - plural() would make
						// it "3 straights".
						choose(
							[
								`${cap(l)} must win ${numWord(loserNeeds)} straight to survive; a win in Game ${gameNo + 1} ends it.`,
								`${cap(l)} now need ${numWord(loserNeeds)} in a row, starting in Game ${gameNo + 1}.`,
								`Nothing short of ${numWord(loserNeeds)} straight saves ${l}, and Game ${gameNo + 1} could end it.`,
							],
							"seriesLoserMustRun",
						)
					: choose(
							[
								`${cap(l)} face elimination in Game ${gameNo + 1}.`,
								`Game ${gameNo + 1} is win-or-go-home for ${l}.`,
								`${cap(l)} are one loss from the end of their season.`,
							],
							"seriesLoserFacingOut",
						),
			);
		} else if (winnerNeeds === 1) {
			// Both one win away - the next game decides it either way.
			//
			// Pooled like its siblings, and for a sharper reason than most: the
			// rounds run in lockstep, so the night a series reaches 3-3 is the
			// night several of them do. One phrasing here meant four recaps on
			// the same page opening this paragraph with the same sentence.
			const prize = s.round === s.numRounds ? "the title" : "the next round";
			out.sentences.push(
				choose(
					[
						`Game ${gameNo + 1} decides it, with each team one win from ${prize}.`,
						`It comes down to Game ${gameNo + 1}, with ${prize} going to the winner.`,
						`Game ${gameNo + 1} is winner-take-all, with ${prize} on the line.`,
					],
					"seriesDecider",
				),
			);
		} else if (loserNeeds === 1) {
			// The winner survived; the other side can still close it out next time.
			out.sentences.push(
				choose(
					[
						`${cap(l)} can still close it out in Game ${gameNo + 1}.`,
						`${cap(l)} get another chance to finish it in Game ${gameNo + 1}.`,
						`${cap(l)} are still a win away, with Game ${gameNo + 1} next.`,
					],
					"seriesLoserCanClose",
				),
			);
		} else if (seedUpset && wAfter > lBefore) {
			const wins = `${numWord(winnerNeeds)} ${winnerNeeds === 1 ? "win" : "wins"}`;
			out.sentences.push(
				choose(
					[
						`${cap(w)}, the No. ${wSeed} seed, lead the series with ${wins} to go.`,
						// "In trouble" after Game 1 of seven is not trouble yet. And
						// it is the other side's wins that put them out: "two wins
						// from going out" said the opposite.
						...(winnerNeeds <= 2
							? [
									`The ${seeded(lSeed)} ${nick(shape.loser)} are in trouble, ${numWord(winnerNeeds)} ${winnerNeeds === 1 ? "loss" : "losses"} from going out.`,
								]
							: []),
						`${cap(w)}, seeded No. ${wSeed}, are ${wins} from putting out the No. ${lSeed} seed.`,
					],
					"seriesSeedUpset",
				),
			);
		}
	}

	return out;
};

// --- Headline ------------------------------------------------------------------

// The deficit the winner came back from, when it was a real one: the period
// boundaries first, else the score log's peak. Zero for a game that was
// never a comeback.
// The deficit the winners came back from: the score log's biggest when there
// is one. The quarter break's was preferred, which made "Down 13, the Lakers
// come back" of a game the Lakers trailed by 17, and left a 20-point hole the
// Magic climbed out of in the first quarter (10 down at the break) out of the
// headline altogether.
export const comebackSize = (shape: Shape): number =>
	shape.comebackFrom >= 12 || shape.loserPeak >= 15
		? Math.max(shape.comebackFrom, shape.loserPeak)
		: 0;

// THE VERB FITS THE ARC, not only the final margin. "The Pistons held off
// the Mavericks" of a game they trailed 87-79 after three and took 36-20 in
// the fourth; "The Nuggets handled the Hornets 115-110" of one in which a
// 15-point lead came down to five. Behind going into the last quarter, they
// rallied - by five or more - and handled nobody; a big lead mostly let go
// was held on to, not handled.
export const verbPool = (game: RecapGame, shape: Shape): string[] => {
	const pool = marginVerbPool(game, shape);
	const upset = isUpset(game, shape);
	if (shape.ot === 0 && shape.wq.length >= 2 && shape.marginEnteringLast < 0) {
		if (shape.marginEnteringLast <= -5 && !upset && shape.margin < 15) {
			return ["rally past", "come back to top"];
		}
		const fits = pool.filter(
			(v) => !["handle", "cruise past", "roll past"].includes(v),
		);
		return fits.length > 0 ? fits : ["beat", "top"];
	}
	const wMax = game.flow ? game.flow.maxLead[sideOf(game, shape.winner)] : 0;
	if (shape.margin <= 8 && wMax >= 15 && wMax >= shape.margin + 9) {
		const fits = pool.filter(
			(v) =>
				![
					"handle",
					"cruise past",
					"roll past",
					"run away from",
					"pull away from",
					"blow out",
					"rout",
				].includes(v),
		);
		// Held off a push that came up short: one that got within reach late.
		if (
			!upset &&
			closeLate(game) &&
			heldOn(game) &&
			!fits.includes("hold off")
		) {
			fits.push("hold off");
		}
		return fits.length > 0 ? fits : ["beat", "top"];
	}
	return pool;
};

const marginVerbPool = (game: RecapGame, shape: Shape): string[] => {
	if (isUpset(game, shape)) {
		// An upset by 34 is a rout first: "Cavaliers knock off the Clippers
		// 132-98" undersold it. The underdog part is told by the line note.
		if (shape.margin >= 20) {
			return ["rout", "blow out", "run away from"];
		}
		// "Stunned" and "shocked" belong to a real number. The day wrap has
		// always held them back for a 7-point dog; the game recap was calling
		// a 4.5-point dog's win a shock.
		return (game.spread?.points ?? 0) >= 7
			? ["stun", "upset", "shock", "knock off"]
			: ["upset", "knock off", "take down", "get past"];
	}
	// "The Nuggets rallied past the Knicks 126-106": a side that trailed and
	// then won by 20 did not rally past anybody. It pulled away.
	if (comebackSize(shape) > 0) {
		return shape.margin >= 15
			? ["pull away from", "run away from"]
			: ["rally past", "storm back to beat", "come back to top"];
	}
	// Overtime is close by definition until the extra period says it was
	// not: "edged the Nuggets 98-82 (OT)" was a 16-point win.
	if (shape.ot > 0 && shape.margin > 6) {
		return ["outlast", "pull away from", "take down"];
	}
	if (shape.ot > 0 || shape.margin <= 4) {
		return heldOn(game)
			? ["hold off", "edge", "outlast", "slip past", "survive"]
			: ["edge", "outlast", "slip past", "survive"];
	}
	// "Rolled past the Bucks 139-82" undersold a 57-point night: past 35
	// the cruising verbs go.
	if (shape.margin >= 35) {
		return ["rout", "blow out", "crush", "trounce"];
	}
	if (shape.margin >= 20) {
		return ["rout", "cruise past", "run away from", "roll past", "blow out"];
	}
	// "Get past" a team you beat by 15 reads as a scare it wasn't. Comfortable
	// wins get their own band so the verb matches the scoreboard.
	// "Held off" is a push that came up short; a side never within seven
	// over the last five minutes made none.
	if (shape.margin <= 9) {
		return closeLate(game) && heldOn(game)
			? ["beat", "top", "get past", "hold off", "take down"]
			: ["beat", "top", "get past", "take down", "handle"];
	}
	return ["beat", "top", "take down", "handle", "pull away from"];
};

const IRREGULAR: Record<string, string> = {
	beat: "beat",
	"come back to top": "came back to top",
	"storm back to beat": "stormed back to beat",
	"rally past": "rallied past",
	"hold off": "held off",
	"slip past": "slipped past",
	outlast: "outlasted",
	survive: "survived",
	edge: "edged",
	rout: "routed",
	"cruise past": "cruised past",
	"run away from": "ran away from",
	"roll past": "rolled past",
	"blow out": "blew out",
	down: "downed",
	top: "topped",
	"take down": "took down",
	"get past": "got past",
	handle: "handled",
	"pull away from": "pulled away from",
	stun: "stunned",
	upset: "upset",
	shock: "shocked",
	"knock off": "knocked off",
};

export const pastTense = (verb: string): string => {
	if (IRREGULAR[verb]) {
		return IRREGULAR[verb]!;
	}
	if (verb.endsWith("e")) {
		return `${verb}d`;
	}
	return `${verb}ed`;
};

// A headline, plus whether it already told the star's story. When it did, the
// body opens on the RESULT instead of restating the same sentence with synonyms
// swapped in - the thing that made a page of recaps read as
// "X goes for 26 as the Celtics beat the Grizzlies / X scored 26 as the Celtics
// topped the Grizzlies 115-105."
export type Headline = {
	// With whole names, for checking what it said against the body.
	text: string;
	// As printed: each man by his surname.
	display?: string;
	// The headline led with the star (his name, his numbers, or both).
	spentStar: boolean;
	// The headline led with the LOSING team's best line ("Walker's 27 and 10 not
	// enough..."), so the losing-side sentence must not print it again.
	spentLoserStar?: boolean;
	// The headline printed the star's FULL stat line, not just his name. The
	// body's star sentence would then be the same numbers a few words later
	// ("DerMarr Johnson goes for 22 points as..." / "DerMarr Johnson scored 22
	// points."), so it's dropped instead.
	spentLine?: boolean;
	// The headline printed the SCORE, as every result-shaped one does. The body's
	// opening sentence then printed it again a dozen words later: "**Rockets fall
	// to the Trail Blazers 103-85** / Corey King posted 28 points ... as the
	// Trail Blazers beat the Rockets 103-85." Over a 600-recap corpus that was a
	// third of every recap. The body keeps the RESULT - it still says who beat
	// whom - and drops the repeated number.
	spentScore?: boolean;
	// The headline carried the series score ("take a 2-1 lead"), so the
	// body's series-state sentence would be the same numbers again.
	spentState?: boolean;
};

// THE HEADLINE, as the wire writes one (recapDay.ts writes it, for a game
// and for a night): "Young leads Pistons past 76ers 110-107", "Brooks' 3-
// pointer lifts Bulls past Hawks 101-100", "Rockets beat Bucks 119-112 in
// OT, take 2-0 series lead". What it said, the body need not say again.
const buildHeadline = (
	game: RecapGame,
	shape: Shape,
	star: RecapPlayer,
	rng: () => number,
): Headline => {
	const { text, display, story } = gameHeadline(game, rng);
	const named = text.includes(star.name);
	const line = statPhrase(star, 1);
	// His whole line, when the headline gave all of it: "scores 41" of a
	// 41-point night, "has 31 points, 25 rebounds" of a 31-and-25 one.
	const spentLine =
		named &&
		(line === plural(star.pts, "point")
			? text.includes(`scores ${star.pts}`) ||
				text.includes(`${star.pts} points`)
			: line === `${star.pts} points and ${star.reb} rebounds` &&
				text.includes(`${star.pts} points, ${star.reb} rebounds`));
	const loser = story.loserStar ?? story.outplayed;
	const sr = story.series;
	return {
		text,
		display,
		spentStar: named,
		spentLoserStar: !!loser && text.includes(loser.name),
		spentLine,
		spentScore: text.includes(`${shape.winner.pts}-${shape.loser.pts}`),
		// The series score, when it said it (or "Game 1", "Game 7", which
		// are one).
		spentState:
			story.kind === "series" &&
			!!sr &&
			(text.includes(`${sr.wAfter}-${sr.lAfter}`) ||
				text.includes(`${sr.lAfter}-${sr.wAfter}`) ||
				sr.state === "opener" ||
				sr.state === "force"),
	};
};

// --- Body sentence builders ----------------------------------------------------

// THE CLAUSE THAT MAKES A LEDE A LEDE. A wire lede does not stop at the
// score: "...beat the Raptors 141-118 for their sixth straight win", "...held
// off the Mavericks 125-123 despite 31 points and 12 rebounds from Payton
// Dunn", "...topped the Kings 104-96, snapping a five-game losing streak". The
// engine had every one of those facts and put each in its own sentence three
// paragraphs down. One of them, the biggest, rides on the result sentence.
export type LedeTail = {
	text: string;
	// The tail told a streak, so the stakes sentence must not.
	streak?: "skid" | "run" | "win";
	// The tail named the losing side's best man with his line.
	loserStar?: RecapPlayer;
};

const ledeTail = (
	game: RecapGame,
	shape: Shape,
	headline: Headline,
	star: RecapPlayer,
	rng: () => number,
): LedeTail => {
	// A streak in the headline is told here too, as a wire lede retells its
	// headline: "Hawks end the Kings' 9-game winning streak" sat over a body
	// that never said so, three times in four. When the headline named the
	// losers' run and the winners also snapped a skid, the run goes here and
	// the skid to the notes.
	if (!game.playoffs) {
		const skid = snappedSkid(shape);
		const lRun = shape.loser.streakBefore;
		const runInHeadline = /winning streak|-game run/.test(headline.text);
		if (skid !== undefined && !(runInHeadline && lRun?.won)) {
			return {
				text: `, snapping ${aNumWord(skid)}-game losing streak`,
				streak: "skid",
			};
		}
		if (lRun?.won && lRun.count >= 5) {
			return {
				text: `, ending ${poss(theNick(shape.loser))} ${numWord(lRun.count)}-game winning streak`,
				streak: "run",
			};
		}
		const s = shape.winner.streak;
		if (s?.won && s.count >= 4) {
			return {
				text: ` for their ${ordinal(s.count)} ${pick(
					rng,
					["straight win", "win in a row", "consecutive win"],
					"ledeStreak",
				)}`,
				streak: "win",
			};
		}
	}
	const loserBest = bestOf(shape.loser.players);
	if (
		loserBest &&
		loserBest.pts >= 28 &&
		loserBest.pts >= star.pts + 4 &&
		loserLineCarries(loserBest, shape.margin) &&
		!headline.spentLoserStar &&
		!headline.text.includes(loserBest.name)
	) {
		return {
			text: ` despite ${statPhrase(loserBest, 1)} from ${loserBest.name}`,
			loserStar: loserBest,
		};
	}
	return { text: "" };
};

// "hit a three-pointer", "made a layup", "converted a three-point play".
export const clutchVerb = (what: string): string =>
	/three-point play|four-point play/.test(what)
		? "converted"
		: /layup|free throw|hook/.test(what)
			? "made"
			: "hit";

// A NIGHT THAT BELONGED TO THE TEAM. Two men with the same number ("scored
// 16 apiece"), or six or seven in double figures behind a quiet leader, is a
// lede a desk writes about the team - "Anthony Lowry put up 16" over two
// teammates at 16 and 14 made the piece look as if it had picked a name out
// of a hat.
// A teammate with the same number as the star, neither with a double-double:
// the two are told together ("scored 26 apiece"), never one after the other.
// Not when his line is more than points: "Franz Foster and Tyrese Foster
// scored 17 points apiece" dropped the 7 steals he was picked for.
export const twinOf = (
	shape: Shape,
	star: RecapPlayer,
): RecapPlayer | undefined =>
	doubleCategories(star).length >= 2 ||
	statPhrase(star) !== plural(star.pts, "point")
		? undefined
		: shape.winner.players.find(
				(p) =>
					p !== star &&
					p.pts === star.pts &&
					p.pts >= 14 &&
					doubleCategories(p).length < 2,
			);

// "Held off" says the winners were in front and kept the other side off.
// "The Pistons held off the Kings 119-114" of a game tied at 112 with 1:35
// left and won on a three with 54 seconds to go was a game they went and
// took. Without the lead log, nothing says otherwise.
export const heldOn = (game: RecapGame): boolean => {
	const last = game.flow?.lastLead;
	if (!last) {
		return true;
	}
	const regPeriods = game.teams[0].ptsQtrs
		? Math.max(4, game.teams[0].ptsQtrs.length - game.overtimes)
		: 4;
	const winnerSide = game.teams[0].tid === game.winnerTid ? 0 : 1;
	return (
		last.side === winnerSide &&
		(last.period < regPeriods ||
			(last.period === regPeriods && last.clock > 120))
	);
};

// Was it still a game inside the last two minutes? A quarter that "broke it
// open" is not the story of a game the losers had within one with 1:15 left
// - the final margin only says how the free throws went.
export const closeLate = (game: RecapGame): boolean => {
	const flow = game.flow;
	if (!flow) {
		return false;
	}
	if (flow.late?.some((m) => Math.abs(m.pts[0] - m.pts[1]) <= 5)) {
		return true;
	}
	return (flow.finish ?? []).some(
		(e) => Math.abs(e.score[0] - e.score[1]) <= 3,
	);
};

const halftimeNote = (
	shape: Shape,
	rng: () => number,
	// The lede has already said the winner came from behind. If they were
	// in front by the break, "trailed by 17 and still won... led 62-44 at
	// halftime" reads as two stories; the line says when it turned.
	comebackTold = false,
): string | undefined => {
	const { wq, lq, regPeriods } = shape;
	if (
		regPeriods < 2 ||
		regPeriods % 2 !== 0 ||
		wq.length < regPeriods ||
		lq.length < regPeriods
	) {
		return undefined;
	}
	const half = regPeriods / 2;
	let w = 0;
	let l = 0;
	for (let i = 0; i < half; i++) {
		w += wq[i] ?? 0;
		l += lq[i] ?? 0;
	}
	if (w === l) {
		return pick(
			rng,
			[
				`It was ${w}-${w} at halftime.`,
				`The teams were tied at ${w} at the break.`,
			],
			"halftimeTied",
		);
	}
	const leader = w > l ? shape.winner : shape.loser;
	const a = Math.max(w, l);
	const b = Math.min(w, l);
	const by = a - b;
	if (comebackTold && leader === shape.winner) {
		return pick(
			rng,
			[
				`${cap(theNick(leader))} had turned it around by halftime, ${a}-${b}.`,
				`By the break ${theNick(leader)} were up ${a}-${b}.`,
				`${cap(theNick(leader))} were in front ${a}-${b} by halftime.`,
			],
			"halftimeRecovered",
		);
	}
	// Many shapes, because this line is in most recaps on a page, the way it
	// is in most wire recaps - and a page that says it the same way three
	// times reads as a form.
	return pick(
		rng,
		[
			`${cap(theNick(leader))} led ${a}-${b} at halftime.`,
			`${cap(theNick(leader))} were ahead ${a}-${b} at the break.`,
			`${cap(theNick(leader))} took a ${a}-${b} lead into halftime.`,
			`${cap(theNick(leader))} were up ${a}-${b} at the half.`,
			`${cap(theNick(leader))} led by ${by} at the break, ${a}-${b}.`,
			`${cap(theNick(leader))} went into the break ahead ${a}-${b}.`,
			`${cap(theNick(leader))} carried a ${a}-${b} lead into the second half.`,
		],
		"halftime",
	);
};

// A player who controlled the game by plus-minus (when it's tracked and big).
const plusMinusNote = (
	shape: Shape,
	star: RecapPlayer,
	rng: () => number,
	// Everyone the piece has already named. The plus-minus note shares a
	// paragraph with the supporting-cast sentence, and the best plus-minus on a
	// blowout winner is very often the same man the support sentence just wrote
	// up - which read as "Pavlovic pitched in with 27 points... Pavlovic
	// finished +34, the best mark on the floor." Skipping to the next-best swing
	// keeps the note and introduces a new face.
	said?: ReadonlySet<string>,
): string | undefined => {
	const candidates = [...shape.winner.players]
		.filter((p) => typeof p.pm === "number" && p !== star)
		.sort((a, b) => (b.pm ?? 0) - (a.pm ?? 0));
	let best = candidates.find((p) => !said?.has(p.name));
	// If everyone with a big swing is already named, the note has nothing new to
	// say and is better left out than repeated - but a note about the outright
	// best mark is still worth having when nobody has been named at all.
	if (!best && !said) {
		best = candidates[0];
	}
	// A PLUS-MINUS MEANS SOMETHING ONLY NEXT TO THE SCORE. "Nobody swung it
	// further than Isaiah Ellis, +27 across 24 minutes" of a 3-point win left
	// the reader to do the sum that makes it news - the team lost the minutes
	// he sat by 24 - and "Trey Dunn was +34" of a 28-point rout was the margin
	// with a name on it. The note says both halves, and runs only when both
	// are big: +25 or better with him, 15 or worse without.
	const pm = best?.pm ?? 0;
	const off = pm - shape.margin;
	if (!best || pm < 25 || off < 15) {
		return undefined;
	}
	const W = cap(theNick(shape.winner));
	return pick(
		rng,
		[
			`${W} outscored ${theNick(shape.loser)} by ${pm} with ${best.name} on the floor and were outscored by ${off} without him.`,
			`${best.name} was +${pm} in ${best.min} minutes; ${theNick(shape.winner)} were outscored by ${off} when he sat.`,
			`${W} won ${poss(best.name)} ${best.min} minutes by ${pm} and lost the rest by ${off}.`,
		],
		"plusMinus",
	);
};

// Stakes and context: streaks, record, standings implication, the spread.
const stakesSentence = (
	game: RecapGame,
	shape: Shape,
	rng: () => number,
	// The headline already quoted the number the books had ("11-point
	// underdogs, the Bulls upset the Pistons"), so the body saying it again
	// three sentences later is the same fact twice.
	spreadTold = false,
	// The result lead already carried the winner's streak ("for their fifth
	// win in a row"), so the streak option here would say it again.
	streakTold = false,
	// ...or the loser's, ending it: "ending the Knicks' six-game winning
	// streak" in the lede, then "It snapped the Knicks' six-game winning
	// streak" in the notes.
	loserStreakTold = false,
): string | undefined => {
	const options: string[] = [];
	const W = cap(theNick(shape.winner));
	const wn = theNick(shape.winner);

	// The skid the winner ended. Not one option among several: when a team
	// has lost twelve straight, the win that ends it is about the twelve.
	const skid = snappedSkid(shape);
	if (skid !== undefined && !streakTold) {
		return pick(
			rng,
			[
				`It ended ${aNumWord(skid)}-game losing streak for ${wn}.`,
				// A count in prose is a word: "had lost six in a row".
				`${W} had lost ${numWord(skid)} in a row coming in.`,
				`That was ${poss(wn)} first win in ${numWord(skid + 1)} games.`,
				`The skid is over at ${numWord(skid)} for ${wn}.`,
			],
			"skidEnded",
		);
	}

	const streak = streakTold ? undefined : shape.winner.streak;
	// In a series, a streak no longer than the wins in that series says nothing
	// the series line has not already said - a 4-0 sweep followed by "have now
	// won 4 straight games" is the same fact twice.
	const seriesWins = (() => {
		const ser = game.series;
		if (!game.playoffs || !ser) {
			return 0;
		}
		const winnerIsHome = shape.winner.abbrev === ser.homeAbbrev;
		return (winnerIsHome ? ser.homeWon : ser.awayWon) + 1;
	})();
	if (streak && streak.won && streak.count >= 4 && streak.count > seriesWins) {
		options.push(
			pick(
				rng,
				[
					`The win was ${poss(theNick(shape.winner))} ${ordinal(streak.count)} in a row.`,
					`${cap(theNick(shape.winner))} have now won ${numWord(streak.count)} straight games.`,
					`That is ${numWord(streak.count)} wins in a row for ${theNick(shape.winner)}.`,
					`${cap(theNick(shape.winner))} ran their streak to ${numWord(streak.count)}.`,
				],
				"streak",
			),
		);
	}

	// A losing streak the winner snapped, from the loser's last-10 log (index 0 is
	// this game).
	const l10 = shape.loser.last10;
	const lBefore = shape.loser.streakBefore;
	if (loserStreakTold) {
		// Told in the lede.
	} else if (lBefore && lBefore.won && lBefore.count >= 4) {
		const run = lBefore.count;
		options.push(
			pick(
				rng,
				[
					`The win snapped ${poss(theNick(shape.loser))} ${numWord(run)}-game winning streak.`,
					`${cap(theNick(shape.loser))} had won ${numWord(run)} in a row until this one.`,
					// Not "That is the end of a four-game run for the 76ers": a run
					// of what, and "is" of a streak that ended last night.
					`${cap(poss(theNick(shape.loser)))} ${numWord(run)}-game winning streak is over.`,
				],
				"snappedStreak",
			),
		);
	} else if (!lBefore && Array.isArray(l10) && l10.length >= 5) {
		let run = 0;
		for (let i = 1; i < l10.length; i++) {
			if (l10[i]!.won) {
				run += 1;
			} else {
				break;
			}
		}
		if (run >= 4) {
			options.push(
				pick(
					rng,
					[
						`The win snapped ${poss(theNick(shape.loser))} ${numWord(run)}-game winning streak.`,
						`${cap(theNick(shape.loser))} had won ${numWord(run)} in a row until this one.`,
						// Not "That is the end of a four-game run for the 76ers": a run
						// of what, and "is" of a streak that ended last night.
						`${cap(poss(theNick(shape.loser)))} ${numWord(run)}-game winning streak is over.`,
					],
					"snappedStreak",
				),
			);
		}
	}

	// An upset is news; the number the books had mostly is not. Real coverage
	// says a team was not supposed to win this one, not that it "was getting 5
	// and did not need them" - and it never says either about an elimination
	// game. So the line is quoted only when it was big enough to be the story,
	// in plain words, and not at all in the postseason.
	if (isUpset(game, shape) && game.spread && !spreadTold && !game.playoffs) {
		const dog = game.spread.points;
		options.push(
			pick(
				rng,
				dog >= 7
					? [
							`${cap(theNick(shape.winner))} came in as ${dog}-point underdogs.`,
							`Nobody had ${theNick(shape.winner)} winning this one - they were ${dog}-point underdogs.`,
							`${cap(theNick(shape.winner))} were not supposed to win this one - the books had ${theNick(shape.loser)} by ${dog}.`,
						]
					: // The number or nothing: "were not supposed to win this one"
						// over a 2.5-point line is a claim the line does not support.
						[
							`${cap(theNick(shape.winner))} came in as ${dog}-point underdogs.`,
							`The books had ${theNick(shape.loser)} by ${dog}.`,
						],
				dog >= 7 ? "underdogBig" : "underdogSmall",
			),
		);
	}

	// The record note earns its sentence only when the record is actually good
	// ("The Warriors improved to 2-8" is not news), and even then only sometimes
	// - it was showing up in nearly every recap on a full slate.
	//
	// And it now prefers something the record MEANS to a bare restatement of it.
	// Over a captured season the three "improved to 30-8" shapes accounted for
	// 170 sentences - 2.7% of everything written, all of it saying only what the
	// standings already say. A round-numbered win and a big cushion over .500
	// are facts a reader does not already have.
	const rec = shape.winner.record;
	if (rec && rec.won + rec.lost >= 10 && !game.playoffs && rec.won > rec.lost) {
		const w = cap(theNick(shape.winner));
		const them = theNick(shape.winner);
		const over = rec.won - rec.lost;
		// A tenth win is not a milestone; a twentieth, fortieth or sixtieth is.
		if (rec.won % 10 === 0 && rec.won >= 20) {
			options.push(
				pick(
					rng,
					[
						`It was ${poss(them)} ${ordinal(rec.won)} win of the season.`,
						`That is ${rec.won} wins on the year for ${them}.`,
					],
					"recordMilestone",
				),
			);
		} else if (rec.lost === 0 && over >= 12 && rng() < 0.3) {
			// "Have won 16 more than they have lost, at 16-0" is arithmetic
			// where the fact is that nobody has beaten them. The plain record
			// is no longer a sentence at all: it sits in parentheses after the
			// first mention of the team, where wire copy keeps it.
			options.push(
				pick(
					rng,
					[
						`${w} are still unbeaten at ${rec.won}-0.`,
						`Nobody has beaten ${them} yet: ${rec.won}-0.`,
						`That is ${rec.won} wins without a loss for ${them}.`,
					],
					"recordUnbeaten",
				),
			);
		}
	}

	if (options.length === 0) {
		return undefined;
	}
	return pick(rng, options);
};

// The absences on a team worth a sentence, biggest role first. Without usage
// data (older box scores, fixtures) the list is taken as given.
const notableAbsences = (t: RecapTeam): RecapInjuryOut[] => {
	const outs = t.injuries ?? [];
	if (!outs.some((o) => o.mpg !== undefined || o.missed !== undefined)) {
		return outs;
	}
	return outs
		.filter((o) => (o.mpg ?? 0) >= 18)
		.sort((a, b) => (b.mpg ?? 0) - (a.mpg ?? 0));
};

// Whether to mention an absence tonight. Always in the first couple of games
// he has missed; after that, a starter's absence comes up now and then and a
// rotation player's rarely. In a playoff series a starter stays news - but
// as "still out", not as if he had just gone down.
const absenceWorthNoting = (
	out: RecapInjuryOut,
	playoffs: boolean,
	rng: () => number,
): boolean => {
	if (out.missed === undefined || out.missed <= 1) {
		return true;
	}
	const star = (out.mpg ?? 0) >= 30;
	// Not every game of a series: "missed an eleventh straight game", then
	// "a twelfth", then "a thirteenth" ran down every recap of one team's
	// postseason.
	if (playoffs) {
		return rng() < (star ? 0.6 : 0.3);
	}
	return rng() < (star ? 0.35 : 0.15);
};

// "five games", "a game", "14 games" - a count of games the way copy prints
// it, small numbers spelled out.
const gamesWord = (n: number): string =>
	n === 1 ? "a game" : `${numWord(n)} games`;

// WHAT HAPPENED TO HIM, the way a reporter puts it: "sprained his ankle",
// "tore his ACL". An injury named for what was done to a body part is told as
// the thing that happened in the game. The rest - a sore knee, a concussion,
// back spasms - he left with. "Picked up a sore ankle" and "went down with
// plantar fasciitis" were how it read before.
const HURT_VERBS = new Map([
	["sprained", "sprained"],
	["strained", "strained"],
	["bruised", "bruised"],
	["fractured", "fractured"],
	["dislocated", "dislocated"],
	["torn", "tore"],
	["broken", "broke"],
]);
export const hurtInGame = (type: string): string => {
	const [first = "", ...part] = lowerInjury(type).split(" ");
	const verb = HURT_VERBS.get(first);
	return verb && part.length > 0
		? `${verb} his ${part.join(" ")}`
		: `left with ${injuryPhrase(type)}`;
};

// Whether a man hurt tonight is news. A starter or a rotation player, yes; the
// deep reserve who tweaked something in garbage time is a line in the box
// score, not in the story. Without usage data (fixtures, a first game) he is
// taken as news.
export const hurtIsNews = (p: RecapPlayer): boolean =>
	p.starter === true ||
	(p.seasonAvg ? p.seasonAvg.min >= 15 : p.starter === undefined);

// "about five games", but "the next game" - "about a game" is no estimate.
// And not "about 175 games": past what is left of the regular season it is
// "the rest of the season" ("the rest of the regular season" when it ends
// near it), and in the playoffs a month is the rest of them.
export const gamesOut = (
	n: number,
	left: number | undefined,
	playoffs: boolean,
): string =>
	left !== undefined && n > left
		? playoffs
			? "the rest of the playoffs"
			: n > left + 10
				? "the rest of the season"
				: "the rest of the regular season"
		: n === 1
			? "the next game"
			: `about ${gamesWord(n)}`;

// The games a team has left: what is left of its regular season, or, in the
// playoffs, a cap past which an absence is the rest of the postseason.
export const gamesLeft = (
	game: RecapGame,
	t: RecapTeam,
): number | undefined => {
	if (game.playoffs) {
		return 20;
	}
	if (!game.seasonGames || !t.record) {
		return undefined;
	}
	return Math.max(0, game.seasonGames - t.record.won - t.record.lost);
};

// Injury color: returns, playing through, new injuries, and notable inactives.
const injurySentence = (
	shape: Shape,
	playoffs: boolean,
	rng: () => number,
	// The recap so far. A man it has not named yet gets his team: "Isaiah
	// King strained his hamstring" left the reader to guess whose loss it was.
	written: string,
): string | undefined => {
	const bits: string[] = [];
	// The club the clause before this one named, so the next one about the
	// same club can lean on it: "The Timberwolves' Bennedict Hayes left with
	// a sore ankle...; a sprained wrist kept Devin Ingram out for the
	// Timberwolves" named them twice in one sentence.
	let namedTid: number | undefined;
	for (const t of [shape.winner, shape.loser]) {
		let teammate = false;
		// "...is expected to miss about four games; teammate Yuta Lowry
		// sprained his elbow and is expected to miss about four games."
		let lastSpan: string | undefined;
		for (const p of t.players) {
			if (p.injury?.playingThrough && p.pts >= 18) {
				bits.push(
					`${p.name} played through ${injuryPhrase(p.injury.type)} for ${p.pts}`,
				);
				namedTid = undefined;
			} else if (
				p.injury?.newThisGame &&
				p.injury.gamesRemaining > 0 &&
				hurtIsNews(p)
			) {
				const who = written.includes(p.name)
					? p.name
					: teammate
						? `teammate ${p.name}`
						: `${poss(theNick(t))} ${p.name}`;
				namedTid = who === p.name ? undefined : t.tid;
				teammate = true;
				const hurt = hurtInGame(p.injury.type);
				const span = gamesOut(
					p.injury.gamesRemaining,
					gamesLeft(shape.game, t),
					shape.game.playoffs,
				);
				bits.push(
					span === lastSpan
						? `${who} ${hurt} and will be out as long`
						: pick(
								rng,
								[
									`${who} ${hurt} and is expected to miss ${span}`,
									`${who} ${hurt} and will miss ${span}`,
									`${who} ${hurt} and will be out for ${span}`,
								],
								"injuryNew",
							),
				);
				lastSpan = span;
			}
		}
	}
	// A key player held out entirely. The box score lists everyone who sat,
	// and the 14th man's sore knee is not news - so when the loader knows what
	// each man had been playing, keep it to rotation players, biggest role
	// first. And an absence is news the first night or two; a man three weeks
	// into a broken foot does not get re-reported every game.
	for (const t of [shape.winner, shape.loser]) {
		const outs = notableAbsences(t);
		for (const out of outs) {
			if (!absenceWorthNoting(out, playoffs, rng)) {
				continue;
			}
			if (namedTid === t.tid && bits.length === 1) {
				bits.push(
					`they were ${(out.missed ?? 0) >= 2 ? "still" : "also"} without ${out.name} (${lowerInjury(out.type)})`,
				);
				break;
			}
			// He has been out a while: say so, rather than reporting the
			// absence as new for the fifth game running.
			if ((out.missed ?? 0) >= 2) {
				const nth = ordinal(out.missed! + 1);
				const aNth = aOrdinal(out.missed! + 1);
				bits.push(
					pick(
						rng,
						[
							`${theNick(t)} were again without ${out.name} (${lowerInjury(out.type)})`,
							`${out.name} missed ${aNth} straight game for ${theNick(t)} with ${injuryPhrase(out.type)}`,
							`it was ${aNth} straight game out for ${out.name} (${lowerInjury(out.type)})`,
							`${theNick(t)} played their ${nth} game without ${out.name}, still out with ${injuryPhrase(out.type)}`,
						],
						"injuryStillOut",
					),
				);
				break;
			}
			bits.push(
				pick(
					rng,
					[
						`${theNick(t)} were without ${out.name} (${lowerInjury(out.type)})`,
						`${out.name} sat out for ${theNick(t)} with ${injuryPhrase(out.type)}`,
						`${theNick(t)} were missing ${out.name} (${lowerInjury(out.type)})`,
						// NOT pre-capitalized: these bits get joined with "; " and only
						// the whole string is capped, so a capital here reads as "a
						// Torn achilles tendon kept..." in the middle of a sentence.
						`${injuryPhrase(out.type)} kept ${out.name} out for ${theNick(t)}`,
					],
					"injuryOut",
				),
			);
			break; // one per team is enough
		}
	}
	if (bits.length === 0) {
		return undefined;
	}
	return `${cap(bits.slice(0, 2).join("; "))}.`;
};

// --- Extra colour: angles the two core paragraphs don't cover -----------------
//
// Everything below is optional. Each returns undefined when the game gives it
// nothing to say, so a quiet 98-92 gets a short recap and a wild one gets a
// long one - which is how it should read. They exist because the box score
// carries far more than a lead scorer and a final score: what a man's night
// was against his own average, how the two sides shot it from deep, who got to
// the line, who has been playing well lately.

// A player's line measured against what he had been doing all season. This is
// the difference between "scored 31" and "scored 31, and he came in averaging
// 12" - the second is a story.
const vsAverageNote = (
	shape: Shape,
	star: RecapPlayer,
	playoffs: boolean,
	rng: () => number,
	// The lead sentence has its own "who came in averaging N" clause. Saying it
	// again two sentences later is the same fact twice.
	alreadySaidAverage: boolean,
	// The lead already printed his shooting split ("on 10-of-15 shooting"),
	// so the note can refer to it; otherwise it has to state it, or "a long
	// way clear of his 46.2% season mark" is a comparison with nothing.
	splitTold = false,
): string | undefined => {
	const avg = enteringLine(star, playoffs);
	if (!avg || avg.gp < 5 || alreadySaidAverage) {
		return undefined;
	}
	const over = star.pts - avg.pts;
	// A big night ONLY counts as a departure if it's both a large absolute jump
	// and a large relative one - 28 from a 24-point scorer is Tuesday.
	if (star.pts >= 20 && over >= 11 && star.pts >= avg.pts * 1.5) {
		// Each shape closes the comparison itself. "He had been averaging 15.6
		// to this point." on its own left the reader to work out what tonight
		// had to do with it.
		const beat =
			star.pts >= avg.pts * 2.5
				? "more than doubled it"
				: star.pts >= avg.pts * 2
					? "doubled it"
					: undefined;
		// What he had been doing, next to what he did - in one sentence. "Anthony
		// Mathis came in averaging 17.3 points a game." opened a paragraph on
		// its own and left the reader to go back for the 33.
		// Without his points a second time: the lede has them.
		const ratio = star.pts / avg.pts;
		const against =
			ratio > 2.02
				? "more than doubled"
				: ratio >= 1.98
					? "doubled"
					: ratio >= 1.8
						? "nearly doubled"
						: "scored well above";
		// More than one way into it: "more than doubled his 10.7-point average"
		// was the line for every such night in a season, worded the same.
		const a = fmtPct(avg.pts);
		return pick(
			rng,
			[
				`${star.name} ${against} his ${a}-point average.`,
				...(beat
					? [
							`${star.name} had been averaging ${a} points a game, and he ${beat}.`,
						]
					: []),
				// Always four or more: with the two "doubled" shapes out of reach
				// the pool was two lines, and one of them ran thirty times in a
				// season.
				`It was an unusual night for ${star.name}, who came in averaging ${a} points a game.`,
				`${star.name} entered the game averaging ${a} points.`,
				`Coming in, ${star.name} had been averaging ${a} points a game.`,
			],
			"vsAvgHigh",
		);
	}
	// The reverse: a star who was held down, but only when he actually is one.
	if (avg.pts >= 18 && star.pts <= avg.pts - 10 && star.pts <= 12) {
		return pick(
			rng,
			[
				`It was a quiet night by ${poss(star.name)} standards - he came in averaging ${fmtPct(avg.pts)} points a game.`,
				`The ${fmtPct(avg.pts)} a night ${star.name} averages never showed up.`,
			],
			"vsAvgLow",
		);
	}
	// An efficient volume night against a middling shooting season.
	// Only beside the night's split. When the lede already printed it, a
	// bare "Naji Hayes' season mark coming in was 46.7%." two paragraphs
	// later is a number with nothing to measure it against, and the split
	// made the point on its own.
	if (
		!splitTold &&
		star.fga >= 12 &&
		avg.fgp > 0 &&
		star.fg / star.fga >= 0.6 &&
		star.fg / star.fga - avg.fgp / 100 >= 0.12
	) {
		const split = `${star.fg}-of-${star.fga}`;
		return pick(
			rng,
			[
				`${star.name} was ${split} from the floor, far better than the ${fmtPct(avg.fgp)}% he had managed on the season.`,
				`${star.name} came in shooting ${fmtPct(avg.fgp)}% on the year and went ${split}.`,
				`${star.name}, a ${fmtPct(avg.fgp)}% shooter on the season, went ${split}.`,
			],
			"vsSeasonFgp",
		);
	}
	return undefined;
};

// Career arc. The season averages of every previous year are in the payload and
// were going unread, which meant a player quietly having the best year of his
// life read exactly like anyone else.
const careerArcNote = (
	star: RecapPlayer,
	playoffs: boolean,
	rng: () => number,
): string | undefined => {
	const avg = star.seasonAvg;
	// A season of a handful of games is not a season a man "averaged"
	// anything in: a two-game year at 39 points became "some way down from the
	// 39.1 he once averaged" under a centre's name.
	const past = (star.career ?? []).filter((c) => c.gp >= 20);
	if (playoffs || !avg || avg.gp < 12 || past.length < 2) {
		return undefined;
	}
	// A FACT ABOUT THE SEASON, NOT THE NIGHT. It is as true in one game as
	// the next, so it was eligible every time the man led a recap, and a
	// reader following his team met the same sentence game after game. It
	// runs now and then - seeded on the man and his games played, so the
	// same game always tells it the same way.
	if (rngFromSeed(star.pid * 7919 + avg.gp * 104729)() >= 0.2) {
		return undefined;
	}
	const bestPast = Math.max(...past.map((c) => c.pts));
	const seasons = (star.career?.length ?? 0) + 1;
	if (avg.pts > bestPast + 1.5 && avg.pts >= 15) {
		return pick(
			rng,
			[
				`At ${fmtPct(avg.pts)} a night, ${star.name} is having the best scoring season of his career.`,
				`${poss(star.name)} ${fmtPct(avg.pts)} per game is a career best in year ${seasons}.`,
				`No season ${star.name} has played has scored like this one.`,
			],
			"careerBest",
		);
	}
	// A veteran past his peak, on a night he played like he was at it. On any
	// other night the decline is not the story of the game, and "some way
	// down from what he once averaged" under a 33-point line read as a jab.
	if (
		seasons >= 10 &&
		bestPast >= avg.pts + 6 &&
		star.pts >= Math.max(20, bestPast)
	) {
		return pick(
			rng,
			[
				`${cap(spelledNum(seasons))} seasons in, ${star.name} turned back the clock.`,
				`It was a night from ${poss(star.name)} prime, ${numWord(seasons)} seasons into his career.`,
			],
			"careerVeteran",
		);
	}
	return undefined;
};

// Defensive stat lines: blocks and steals, which almost never surfaced because
// the lead sentence is built around scoring.
const defensiveNote = (
	shape: Shape,
	rng: () => number,
	said: Set<string>,
): string | undefined => {
	const w = teamStats(shape.winner);
	const bigBlocker = shape.winner.players.find(
		(p) => p.blk >= 5 && !said.has(p.name),
	);
	const bigThief = [...shape.winner.players, ...shape.loser.players].find(
		(p) => p.stl >= 5 && !said.has(p.name),
	);
	// A man being introduced here for the first time needs his team: "Darius
	// Mathis collected 5 steals" on its own leaves the reader guessing which
	// side he was on.
	const thiefTeam = bigThief
		? theNick(
				shape.winner.players.includes(bigThief) ? shape.winner : shape.loser,
			)
		: "";
	const options: string[] = [];
	// A man with more to his night than the steals or blocks gets the line:
	// "Vince King recorded 5 steals" of a night with 11 rebounds the piece
	// never told.
	const more = (p: RecapPlayer) => p.pts >= 15 || p.reb >= 10 || p.ast >= 8;
	// A real block party only: nine is one winner in eight, and "The Spurs
	// blocked 9 shots." closed too many paragraphs.
	if (w.blk >= 12) {
		options.push(
			pickSentence(
				rng,
				[
					`${cap(theNick(shape.winner))} blocked ${w.blk} shots.`,
					`${cap(theNick(shape.winner))} sent back ${w.blk} shots.`,
					`${cap(theNick(shape.winner))} had ${w.blk} blocked shots.`,
					`${cap(theNick(shape.winner))} finished with ${w.blk} blocks.`,
				],
				"teamBlk",
			),
		);
	}
	if (bigBlocker) {
		options.push(
			more(bigBlocker)
				? `${bigBlocker.name} had ${statPhrase(bigBlocker)}.`
				: `${bigBlocker.name} protected the rim with ${plural(bigBlocker.blk, "block")}.`,
		);
	}
	if (bigThief) {
		options.push(
			`${bigThief.name} ${takeVerb(rng, ["came up with", "recorded", "collected"], "thiefVerb")} ${more(bigThief) ? statPhrase(bigThief) : plural(bigThief.stl, "steal")} for ${thiefTeam}.`,
		);
	}
	if (options.length === 0) {
		return undefined;
	}
	const chosen = pick(rng, options);
	for (const p of [bigBlocker, bigThief]) {
		if (p && chosen.startsWith(p.name)) {
			said.add(p.name);
		}
	}
	return chosen;
};

// Somebody fouled out. pf is in the box score and nothing was reading it.
const foulOutNote = (
	shape: Shape,
	said: Set<string>,
	rng: () => number,
): string | undefined => {
	for (const t of [shape.winner, shape.loser]) {
		for (const p of t.players) {
			// Only when it cost something: a rotation player in a game that was
			// still a game, or a man with a real line. A 9-point reserve fouling
			// out of a 20-point loss is a box-score footnote, and it was getting
			// a sentence in one recap in six.
			const mattered =
				(p.min >= 24 || p.pts >= 15) && (shape.margin <= 12 || p.pts >= 20);
			if (p.pf >= 6 && mattered && !said.has(p.name)) {
				said.add(p.name);
				const line = `${plural(p.pts, "point")} in ${p.min} minutes`;
				// He is new to the piece, and either side's: "Foul trouble cost
				// Lonnie Dunn" after a sentence about the other team's free
				// throws left the reader to guess whose.
				const who = `${poss(theNick(t))} ${p.name}`;
				return pickSentence(
					rng,
					[
						`${cap(who)} fouled out with ${line}.`,
						`${cap(who)} was gone by the end, fouling out with ${line}.`,
						`Foul trouble cost ${who}, who finished with ${line}.`,
						`${cap(who)} picked up his sixth foul, done for the night with ${line}.`,
					],
					"foulOut",
				);
			}
		}
	}
	return undefined;
};

// Recent form from the last-10 log, which was only being read to find a snapped
// streak. How a team has been playing is context a reader actually wants.
const formNote = (
	shape: Shape,
	rng: () => number,
	// What the recap has said so far. A team whose recent form has ALREADY been
	// described must not also get a form note: "The Celtics have now won 6
	// straight" followed by "the Celtics have won 5 of their last 6" is the same
	// fact told twice, and worse, the two numbers look like they disagree.
	//
	// Checked PER TEAM. This used to be a single boolean applied to the winner,
	// which missed the other half of it - the snapped-streak sentence is about
	// the LOSER, so "It snapped the Monuments' 8-game winning streak." was still
	// followed by "The Monuments came in having won 8 of their last 9."
	alreadyWritten = "",
): string | undefined => {
	// "straight", not "straight game": the skid note says "the Suns' eighth
	// straight LOSS" and the streak note says "fourth straight WIN", and
	// neither matched, so "It was the Suns' eighth straight loss." was still
	// followed by "The Suns had lost every one of their last seven coming
	// in." - the same fact twice, with two numbers that look like they
	// disagree. Every other use of the word in the engine is a streak too.
	// And "That is the end of a nine-game run for the Nets." was followed by
	// "That was nine wins in nine games for the Nets coming in."
	const STREAK =
		/in a row|straight|ran their streak|winning streak|losing streak|streak to \d|skid|first win in|-game run/;
	const sentences = alreadyWritten.split(/(?<=[!.?])\s+/);
	const formTold = (t: RecapTeam): boolean => {
		const nickname = nick(t);
		return sentences.some((x) => x.includes(nickname) && STREAK.test(x));
	};
	const describe = (t: RecapTeam): string | undefined => {
		const l10 = t.last10;
		// Index 0 is this game, so the FORM entering it is everything after.
		if (!Array.isArray(l10) || l10.length < 7) {
			return undefined;
		}
		// "9 wins in 9 games coming in" under 16-0 is the small number when
		// the big one is the story; the record note has the unbeaten team.
		if (t.record && t.record.lost === 0 && t.record.won >= 7) {
			return undefined;
		}
		const prior = l10.slice(1);
		const won = prior.filter((x) => x.won).length;
		// The log holds nine games; the streak can be longer than that, and
		// "had lost every one of their last 9" under a sixteen-game skid is the
		// small number when the big one is in hand.
		const before = t.streakBefore;
		if (before && !before.won && before.count > prior.length) {
			return pick(
				rng,
				[
					`${theNick(t)} came in on ${aNum(before.count)}-game losing streak`,
					`${theNick(t)} had lost ${numWord(before.count)} in a row coming in`,
					`${theNick(t)} arrived without a win in ${before.count} games`,
				],
				"formSkid",
			);
		}
		// EVERY sentence here has to be past tense, describing the form the team
		// carried INTO this game, because that is the window `prior` measures.
		// Present tense reads as a claim about right now and is then flatly
		// wrong for the team that just lost: a 9-0 run into a defeat became
		// "the Celtics are 9-0 over their last 9" underneath a box score of
		// them losing. It is wrong for the winner too, just less visibly - they
		// are 10-0 including tonight, not the 9-0 the sentence "now" asserts.
		if (won >= prior.length - 1 && prior.length >= 6) {
			const of = won === prior.length ? "every one of" : `${numWord(won)} of`;
			return pick(
				rng,
				[
					`${theNick(t)} came in having won ${of} their last ${numWord(prior.length)}`,
					`that was ${numWord(won)} wins in ${numWord(prior.length)} games for ${theNick(t)} coming in`,
					`${theNick(t)} entered the night ${won}-${prior.length - won} over their previous ${numWord(prior.length)}`,
				],
				"formHot",
			);
		}
		if (won <= 1 && prior.length >= 6) {
			// "dropped 9 of their last 9" is how a losing streak reads when
			// nobody checks for the clean sweep; the hot branch above has always
			// said "every one of".
			const lost = prior.length - won;
			const allOf = won === 0 ? "every one of" : `${numWord(lost)} of`;
			return pick(
				rng,
				[
					`${theNick(t)} had lost ${allOf} their last ${numWord(prior.length)} coming in`,
					`${theNick(t)} arrived having dropped ${allOf} their last ${numWord(prior.length)}`,
					`it had been a rough stretch for ${theNick(t)}, ${won}-${lost} in their previous ${numWord(prior.length)}`,
				],
				"formCold",
			);
		}
		return undefined;
	};
	const options = [
		formTold(shape.winner) ? undefined : describe(shape.winner),
		formTold(shape.loser) ? undefined : describe(shape.loser),
	].filter((x): x is string => !!x);
	if (options.length === 0) {
		return undefined;
	}
	return `${cap(pick(rng, options))}.`;
};

// A heavy favorite that barely got out. That is the one thing the line adds
// to a recap of a game the favorite won: a favorite that beat the number, or
// fell short of it, is a ledger entry, and "won comfortably enough but fell
// half a point short of the number" was in a third of all recaps and belongs
// in none. Never in the postseason, and not every time even then.
const spreadNote = (
	game: RecapGame,
	shape: Shape,
	rng: () => number,
): string | undefined => {
	const s = game.spread;
	if (!s || game.playoffs || s.favTid === shape.loser.tid) {
		return undefined;
	}
	if (s.points >= 7 && shape.margin <= 3 && rng() < 0.5) {
		return pick(
			rng,
			[
				`The ${s.points}-point favorites had to sweat this one out.`,
				`${cap(theNick(shape.winner))} were favored by ${s.points} and won by ${shape.margin}.`,
				`${cap(theNick(shape.winner))} were supposed to win this one comfortably, and barely did.`,
			],
			"spreadScare",
		);
	}
	return undefined;
};

// Heavy minutes - a starter who barely came off the floor.
const minutesNote = (shape: Shape, said: Set<string>): string | undefined => {
	// Five more for every overtime: "Marcus Dunn logged 46 minutes" of a game
	// that ran 58 was less than most of the men who started it.
	const iron = [...shape.winner.players, ...shape.loser.players].find(
		(p) => p.min >= 46 + 5 * shape.ot && !said.has(p.name),
	);
	if (!iron) {
		return undefined;
	}
	said.add(iron.name);
	const team = shape.winner.players.includes(iron) ? shape.winner : shape.loser;
	return `${cap(poss(theNick(team)))} ${iron.name} logged ${iron.min} minutes.`;
};

// --- All-Star Game -------------------------------------------------------------

const buildAllStar = (game: RecapGame, rng: () => number): string => {
	const [home, away] = game.teams;
	const winner = game.winnerTid === home.tid ? home : away;
	const loser = winner === home ? away : home;
	const as = game.allStar ?? {};
	const star = bestOf(winner.players) ?? bestOf(loser.players);
	const loserStar = bestOf(loser.players);

	// The squads' REAL names. game.teams carries the sentinel All-Star tids,
	// which resolve to region "All-Stars" and name "1"/"2" - so this used to
	// read "1 beat 2 155-148 in the All-Star Game".
	const [name0, name1] = as.teamNames ?? [];
	const squad = (t: RecapTeam): string => {
		const real = t === home ? name0 : name1;
		if (real && real !== "") {
			return real;
		}
		// No stored names (an imported league, an older save): fall back to
		// something that at least reads as a side rather than a digit.
		return t === home ? "the home team" : "the visitors";
	};
	const w = squad(winner);
	const l = squad(loser);
	const total = winner.pts + loser.pts;

	// One a season, so across seasons the same three headlines came round
	// again and again: more of them, with his points and his side's win
	// when the box score has them.
	const mvpRow = as.mvp
		? [...winner.players, ...loser.players].find((p) => p.name === as.mvp)
		: undefined;
	const mvpWon = !!mvpRow && winner.players.includes(mvpRow);
	const headline = as.mvp
		? pick(
				rng,
				[
					`${as.mvp} takes home All-Star Game MVP`,
					`${as.mvp} is the All-Star Game MVP`,
					`${as.mvp} steals the show at the All-Star Game`,
					...(mvpRow && mvpRow.pts >= 20
						? [`${as.mvp} scores ${mvpRow.pts} and wins All-Star MVP`]
						: []),
					...(mvpWon ? [`${as.mvp} leads ${w} to an All-Star Game win`] : []),
				],
				"allStarHeadlineMvp",
			)
		: pick(
				rng,
				[
					"The stars come out for the All-Star Game",
					`${w} take the All-Star Game`,
				],
				"allStarHeadline",
			);

	const sentences: string[] = [];
	sentences.push(
		pick(
			rng,
			[
				`${w} beat ${l} ${winner.pts}-${loser.pts} in the All-Star Game.`,
				`It was ${w} over ${l}, ${winner.pts}-${loser.pts}, in the All-Star Game.`,
				`${w} came out on top of ${l} ${winner.pts}-${loser.pts} in the All-Star Game.`,
			],
			"allStarResult",
		),
	);

	// The MVP is the story, so lead with HIS line when he can be found in the
	// box score - the old version named whoever the impact score liked best and
	// left the headline's MVP unexplained.
	const named = new Set<string>();
	const mvpLine = mvpRow;
	if (mvpLine) {
		named.add(mvpLine.name);
		// With how he got there ("on 8 three-pointers"), and the award said in
		// so many words: "The award went to Obi King, on 38 points" left the
		// reader to work out which award.
		const line = withFlourish(statPhrase(mvpLine), shootingFlourish(mvpLine));
		sentences.push(
			pick(
				rng,
				[
					`${mvpLine.name} was named MVP after putting up ${line}.`,
					`${mvpLine.name} took MVP honors with ${line}.`,
					`MVP ${mvpLine.name} finished with ${line}.`,
				],
				"allStarMvpLine",
			),
		);
	} else if (star) {
		named.add(star.name);
		sentences.push(`${star.name} poured in ${plural(star.pts, "point")}.`);
	}

	// One more line from each side, so the piece names more than one man.
	const other = [...winner.players]
		.sort((a, b) => b.pts - a.pts)
		.find((p) => !named.has(p.name) && p.pts >= 15);
	if (other) {
		named.add(other.name);
		sentences.push(
			`${other.name} ${claimVerb("added")} ${statPhrase(other, 1)} for ${w}.`,
		);
	}
	if (loserStar && !named.has(loserStar.name) && loserStar.pts >= 15) {
		sentences.push(
			pick(
				rng,
				[
					`${loserStar.name} led ${l} with ${statPhrase(loserStar, 1)}.`,
					`The best of it for ${l} was ${statPhrase(loserStar, 1)} from ${loserStar.name}.`,
				],
				"allStarLoserStar",
			),
		);
	}

	// Nobody plays defense in this game, and the scoreboard says so.
	if (total >= 280) {
		sentences.push(
			pick(
				rng,
				[
					`Defense was optional, as usual: ${total} points between them.`,
					`The teams combined for ${total} points.`,
				],
				"allStarTotal",
			),
		);
	}

	// The weekend's contests, with the field named - the runners-up were in the
	// data all along and never reached the page.
	const contests: string[] = [];
	const extras: string[] = [];
	const field = (c: { winner?: string; players: string[] } | undefined) => {
		if (!c?.winner) {
			return "";
		}
		const rest = c.players.filter((p) => p !== c.winner);
		return rest.length > 0 ? ` over ${naturalList(rest)}` : "";
	};
	if (as.dunk?.winner) {
		extras.push(`${as.dunk.winner} won the dunk contest${field(as.dunk)}`);
	}
	if (as.three?.winner) {
		extras.push(
			`${as.three.winner} took the three-point shootout${field(as.three)}`,
		);
	}
	if (extras.length > 0) {
		// Joined with a semicolon, not naturalList: each clause can already end
		// in its own "A and B" field list, and "won the dunk contest over Zach
		// LaVine and Derrick Jones Jr. and Stephen Curry took the three-point
		// shootout" runs the two together.
		contests.push(
			`${pick(
				rng,
				["Over the weekend", "Elsewhere on the weekend", "On the undercard"],
				"allStarExtras",
			)}, ${extras.join("; ")}.`,
		);
	}

	// The weekend's contests are their own paragraph: the game is one story
	// and the dunk contest another.
	return [`**${headline}**`, sentences.join(" "), ...contests].join("\n\n");
};

// --- Entry point: one game -----------------------------------------------------

// --- How it unfolded: the sim's score log, when the game recorded one ---------
//
// Everything above reads the box score and the quarter scores, which can say
// a game was close and not how. These read the flow summary
// (common/gameFlow.ts): who took the lead for good and when, the last tie,
// how many times it changed hands, the run that turned it, the lead the loser
// let go. Each says nothing on a game from before the log existed.

export const sideOf = (game: RecapGame, t: RecapTeam): GameFlowSide =>
	game.teams[0].tid === t.tid ? 0 : 1;

export const nameOfPid = (
	game: RecapGame,
	pid: number | undefined,
): string | undefined => {
	if (pid === undefined) {
		return undefined;
	}
	for (const t of game.teams) {
		for (const p of t.players) {
			if (p.pid === pid) {
				return p.name;
			}
		}
	}
	return undefined;
};

// "the fourth", "overtime", "the second overtime".
const periodTag = (period: number, regPeriods: number): string =>
	period <= regPeriods
		? `the ${ordinal(period)}`
		: period === regPeriods + 1
			? "overtime"
			: `the ${ordinal(period - regPeriods)} overtime`;

// How a close game finished. The go-ahead score that held, or failing that
// the last tie, and the count of lead changes when there were a lot of them.
export const finishNote = (
	game: RecapGame,
	shape: Shape,
	rng: () => number,
	// A game-winning shot already has its own sentence; a go-ahead basket
	// inside the last half minute is that shot told twice.
	shotTold: boolean,
	// The man the sentence before this one was about, when there is one, so
	// "Zeke Foster put up 29... Zeke Foster's basket with 4:43 to go" reads
	// "his basket" instead.
	justNamed?: string,
	// The closing sequence has been written from the finish log, which
	// already told the go-ahead score and the last tie.
	sequenceTold = false,
): string[] => {
	const flow = game.flow;
	const close = shape.margin <= 6 || shape.ot > 0;
	// When a comeback took the lead for good is its key beat, whatever the
	// final margin; the tight-game notes below stay for tight games.
	const comeback = comebackSize(shape) > 0;
	// And a lead the losers took late and gave back, in a game won by up to
	// nine: "They blew a 16-point lead and won anyway" was the last word on
	// a game the Pelicans went back in front in with 2:51 left.
	const lastLead = flow?.lastLead;
	const retook =
		!!lastLead &&
		lastLead.side === sideOf(game, shape.winner) &&
		lastLead.period >= shape.regPeriods &&
		lastLead.clock <= 300 &&
		shape.margin <= 9;
	if (!flow || (!close && !comeback && !retook)) {
		return [];
	}
	const out: string[] = [];
	if (sequenceTold) {
		// Only the character of the whole game is left to say.
		return leadChangesNote(flow, shape, rng);
	}
	const W = cap(theNick(shape.winner));
	const wSide = sideOf(game, shape.winner);
	const inFinal = (period: number) => period >= shape.regPeriods;

	const last = flow.lastLead;
	// In a tight game the lead for good is a fourth-quarter fact. In a
	// comeback it is the turn of the game, wherever it came after halftime -
	// unless the comeback turned into a rout: "outscored the Bulls 71-44 and
	// pushed the lead to 33. The Spurs went in front for good with 2:57 left
	// in the third" ran the clock backward to a moment that no longer mattered.
	const halfway = Math.ceil(shape.regPeriods / 2);
	if (
		last &&
		last.side === wSide &&
		(inFinal(last.period) ||
			(comeback && last.period > halfway && shape.margin <= 15)) &&
		!(shotTold && last.clock <= 30)
	) {
		const when = clockLeft(last.clock);
		const who = nameOfPid(game, last.pid);
		const where =
			last.period > shape.regPeriods
				? ` in ${periodTag(last.period, shape.regPeriods)}`
				: last.period < shape.regPeriods
					? ` in the ${ordinal(last.period)}`
					: "";
		if (when) {
			out.push(
				pick(
					rng,
					who && who === justNamed
						? [
								`His basket with ${when} to go${where} put ${theNick(shape.winner)} ahead for the last time.`,
								`${W} went in front for good on his basket with ${when} left${where}.`,
							]
						: who
							? [
									`${W} took the lead for good on ${poss(who)} basket with ${when} left${where}.`,
									`${cap(poss(who))} basket with ${when} to go${where} put ${theNick(shape.winner)} ahead for the last time.`,
									`${W} went in front for good with ${when} left${where}, on a basket by ${who}.`,
								]
							: [`${W} took the lead for good with ${when} left${where}.`],
					"leadForGood",
				),
			);
		}
	} else if (close && flow.lastTie && inFinal(flow.lastTie.period)) {
		const when = clockLeft(flow.lastTie.clock);
		if (when) {
			out.push(
				pick(
					rng,
					[
						`It was tied at ${flow.lastTie.pts} with ${when} to play.`,
						`The last tie came at ${flow.lastTie.pts}, with ${when} left.`,
					],
					"lastTie",
				),
			);
		}
	}

	if (!close) {
		return out;
	}
	out.push(...leadChangesNote(flow, shape, rng));
	return out;
};

// A game nobody got away in: either the lead kept changing hands, or
// neither side ever built one. One sentence, not both - and only when the
// number says something, since "8 lead changes" is an ordinary night.
const leadChangesNote = (
	flow: NonNullable<RecapGame["flow"]>,
	shape: Shape,
	rng: () => number,
): string[] => {
	const out: string[] = [];
	const biggest = Math.max(flow.maxLead[0], flow.maxLead[1]);
	// Ten lead changes is an ordinary close game; the count was in one recap
	// in eight. Fourteen or more is a seesaw worth the line every time.
	if (flow.leadChanges >= 14 || (flow.leadChanges >= 10 && rng() < 0.5)) {
		const tieWord = flow.ties === 1 ? "tie" : "ties";
		out.push(
			flow.ties > 0
				? pick(
						rng,
						[
							`There were ${flow.leadChanges} lead changes and ${flow.ties} ${tieWord}.`,
							`It was a back-and-forth game, with ${flow.leadChanges} lead changes and ${flow.ties} ${tieWord}.`,
							`The lead changed hands ${flow.leadChanges} times, with ${flow.ties} ${
								flow.ties === 1 ? "tie" : "ties"
							}.`,
							// "Nobody led by more than 12" is not a tight game.
							...(biggest <= 8
								? [
										`Nobody led by more than ${biggest} in a game of ${flow.leadChanges} lead changes.`,
									]
								: []),
						],
						"leadChanges",
					)
				: `The lead changed hands ${flow.leadChanges} times.`,
		);
	} else if (biggest <= 6 && shape.regPeriods >= 4) {
		out.push(
			pick(
				rng,
				[
					`Neither side led by more than ${biggest} all night.`,
					`The biggest lead either way was ${biggest}.`,
				],
				"tightAllNight",
			),
		);
	}
	return out;
};

// The losers' losing streak, which the body never said: a team that has just
// dropped its fifth straight is news on its own page, whatever the winner
// did. The headline's upset branch sometimes carries it; the form note
// covers a long one from the last-ten log. This is the plain fact for the
// rest.
const loserSkidNote = (
	shape: Shape,
	rng: () => number,
	written: string,
): string | undefined => {
	const s = shape.loser.streak;
	// Four, not three: every team drops three in a row now and then, and
	// "That is three straight losses for the 76ers" closed one recap in
	// seven.
	if (!s || s.won || s.count < 4 || shape.game.playoffs) {
		return undefined;
	}
	const ln = nick(shape.loser);
	// Already said, in any of its forms.
	if (
		new RegExp(
			String.raw`${ln}[^.]*(?:straight|in a row|skid|losing streak)|(?:straight|in a row|skid|losing streak)[^.]*${ln}`,
		).test(written)
	) {
		return undefined;
	}
	const L = cap(theNick(shape.loser));
	return pick(
		rng,
		[
			`It was ${poss(theNick(shape.loser))} ${ordinal(s.count)} straight loss.`,
			`${L} have lost ${numWord(s.count)} in a row.`,
			`The loss was ${poss(theNick(shape.loser))} ${ordinal(s.count)} in a row.`,
			`That is ${numWord(s.count)} straight losses for ${theNick(shape.loser)}.`,
		],
		"loserSkid",
	);
};

const buildGameRecap = (game: RecapGame): string => {
	if (!isInBatch()) {
		clearPhraseMemory();
	}
	// Per RECAP, not per batch: a verb spent on this game is free again on the
	// next one (pick's own rotation spreads those), but never twice here.
	resetVerbLedger();
	const rng = rngFromSeed((game.gid + 1) * 2654435761);

	if (game.allStar) {
		return buildAllStar(game, rng);
	}

	const shape = analyzeShape(game);
	if (shape.margin === 0) {
		return buildTie(game, shape, rng);
	}
	const star = storyPlayerOf(game, shape);

	if (!star) {
		const verb = pastTense(pick(rng, verbPool(game, shape)));
		return `**${nick(shape.winner)} ${verb} the ${nick(shape.loser)}, ${scoreTag(
			shape,
		)}**\n\n${cap(theNick(shape.winner))} ${verb} ${theNick(
			shape.loser,
		)} ${scoreTag(shape)}.`;
	}

	const post = game.playoffs
		? postseasonContext(game, shape, rng)
		: { sentences: [] as string[] };

	const headline = buildHeadline(game, shape, star, rng);
	const tail = ledeTail(game, shape, headline, star, rng);
	const shot = clutchShot(game);
	const winningShot = shot && !shot.tying ? shot : undefined;
	// A game decided in its last minute, on the basket that decided it, when
	// nothing else holds the lede's last slot.
	const goAhead =
		tail.text === "" && !winningShot
			? lateGoAhead(game, shape, shot)
			: undefined;

	// THE SERIES IT ENDED, in the lede. "Grant Green put up 29 points as the
	// Lakers took down the Pistons 124-114" was the whole lede of the night
	// that won the Lakers the title, and not one of 45 series-ending ledes
	// said the series was over: "beat the Clippers 104-93 to close out the
	// series 4-1", "in Game 7 to reach the Conference Semifinals".
	const clinch = post.clinch;
	let clinchLede: { text: string; next: boolean } | undefined;
	if (clinch) {
		const games = clinch.wAfter + clinch.lBefore;
		const score = `${clinch.wAfter}-${clinch.lBefore}`;
		clinchLede = clinch.decider
			? clinch.title
				? { text: ` in Game ${games} to win the title`, next: true }
				: {
						text: ` in Game ${games} to reach ${clinch.nextRound}`,
						next: true,
					}
			: clinch.title
				? {
						text: clinch.sweep
							? ` to finish a ${numWord(clinch.wAfter)}-game sweep for the title`
							: ` to win the Finals ${score}`,
						next: true,
					}
				: {
						text: clinch.sweep
							? ` to finish a ${numWord(clinch.wAfter)}-game sweep`
							: pick(
									rng,
									[
										` to close out the series ${score}`,
										` to win the series in ${numWord(games)} games`,
									],
									"ledeClinch",
								),
						next: false,
					};
	}
	// A playoff lede says which game of which round, whatever the headline
	// said: "The Magic rallied past the Grizzlies 113-111" opened Game 1 of
	// the Finals, and the round turned up three sentences on in "The Magic
	// are up 1-0 in the Finals".
	const where = post.state
		? ` in Game ${post.state.gameNo} of ${post.state.round}`
		: (clinchLede?.text ?? post.playInLede);

	// THE ORDER A REPORTER WRITES IN. What happened - one sentence with the
	// result, the man of the night and what made the game what it was. Then
	// how it happened, as a story. Then who did it, the winners and then the
	// losers, each with the reason on it. Last, what it means. See
	// recapStory.ts, which writes the first three.
	const input: StoryInput = {
		game,
		shape,
		star,
		headline,
		tail,
		shot: winningShot,
		goAhead,
		where,
		rng,
	};
	const lede = composeLede(input);
	const told = lede.told;
	const para1: string[] = [...lede.sentences];
	const storyPara: string[] = composeStory(input, told);

	const postSentences = headline.spentState
		? // "The Cavaliers are up 2-1 in the First Round" under "Cavaliers take
			// a 2-1 series lead" is the same numbers twice; the stakes sentence
			// that follows it is the one with something to add.
			post.sentences.slice(1)
		: post.clinch
			? [
					// The lede has the series result now ("to close out the series
					// 4-1"), so the winner's series sentence is not said again -
					// only where they go next, when nothing above said it.
					...(!post.clinch.title &&
					!clinchLede?.next &&
					!headline.text.includes(post.clinch.nextRound.replace(/^the /, ""))
						? [
								`${cap(theNick(shape.winner))} move on to ${post.clinch.nextRound}.`,
							]
						: []),
					...post.sentences.slice(1),
				]
			: post.sentences;
	// Where the series stands is part of what happened, and closes the lede.
	// When the headline already carried it, what is left is the stakes
	// ("The Raptors can still close it out in Game 5"), which is context.
	// The lede named the round, so the series score does not name it again:
	// "The Magic lead the series 1-0", not "...are up 1-0 in the Finals"
	// under "in Game 1 of the Finals".
	const rnd = post.state?.round;
	const seriesState =
		headline.spentState || postSentences[0] === undefined
			? undefined
			: rnd && para1.join(" ").includes(` of ${rnd}`)
				? postSentences[0]
						.replace(
							new RegExp(String.raw`^Game \d+ of ${rnd} went to (.+)\.$`),
							(_, w: string) =>
								`${cap(w)} lead the series ${post.state!.wAfter}-${post.state!.lBefore}.`,
						)
						// "...in Game 2 of the First Round. Game 2 went to the
						// Nuggets, and the series is tied 1-1."
						.replace(/^Game \d+ went to [^,]+, and (.+)$/, (_, rest: string) =>
							cap(rest),
						)
						.replace(
							new RegExp(String.raw` are up (\d+-\d+) in ${rnd}\.$`),
							" lead the series $1.",
						)
						.replace(` in ${rnd}`, "")
				: postSentences[0];
	const postContext = headline.spentState
		? postSentences
		: postSentences.slice(1);
	if (seriesState) {
		para1.push(seriesState);
	}
	// A first title in franchise history, or a repeat, is part of the lede's
	// news, not the last line of the piece: it goes up with the result.
	const history = postContext.find((t) =>
		/championship in franchise history|back-to-back championships/.test(t),
	);
	if (history) {
		para1.push(history);
	}

	const written1 = [headline.text, ...para1, ...storyPara].join(" ");
	let writtenSoFar = written1;

	// --- Who did it: the winners. The man in the lede first - what his night
	// was against his own season - then the men around him. ---
	const winners: string[] = [];
	const everyone = [...shape.winner.players, ...shape.loser.players];
	const othersOf = (p: RecapPlayer) =>
		everyone.filter((x) => x !== p).map((x) => x.name);
	const cast = composeCast(input, told, writtenSoFar);
	// A season high goes into his line when the line is on the page
	// ("racked up a season-high 32 points"), and is then no note of its own.
	const starHighFolded = foldSeasonHigh(
		[para1, storyPara, cast.winners],
		star,
		othersOf(star),
	);
	writtenSoFar = [written1, ...cast.winners].join(" ");
	// What his night was against his season: the career arc first - it is
	// the rarest note and the one a season high or an against-his-average
	// line would otherwise crowd out. One note; two for a night that big.
	const starColour = [
		() => careerArcNote(star, game.playoffs, rng),
		() =>
			vsAverageNote(
				shape,
				star,
				game.playoffs,
				rng,
				/averaging|average/.test(writtenSoFar),
				writtenSoFar.includes(`${star.fg}-of-${star.fga}`),
			),
		() => playerStreakBeat(star, rng),
		() => playerCountBeat(star, rng, writtenSoFar),
		() =>
			starHighFolded ? undefined : playerHighBeat(star, rng, writtenSoFar),
		() => vsOpponentBeat(star, nick(shape.loser), rng),
	];
	const colour: string[] = [];
	for (const beat of starColour) {
		if (colour.length >= (star.pts >= 35 ? 2 : 1)) {
			break;
		}
		const text = beat();
		if (text) {
			colour.push(text);
			writtenSoFar = `${writtenSoFar} ${text}`;
		}
	}
	// His notes sit right behind his own sentence when the cast opens on
	// him, and open the paragraph when the lede was his.
	const starFirst = cast.starFirst ? 1 : 0;
	winners.push(
		...cast.winners.slice(0, starFirst),
		...colour,
		...cast.winners.slice(starFirst),
	);
	const addWinner = (beat: () => string | undefined, cap = 5) => {
		if (winners.length >= cap) {
			return;
		}
		const text = beat();
		if (text) {
			winners.push(text);
			writtenSoFar = `${writtenSoFar} ${text}`;
		}
	};
	// A +20 in a 25-point rout is the rout restated. In a game decided by
	// that much the swing is a note only when it outran the final margin.
	addWinner(() => {
		if (shape.margin >= 18) {
			const swing = Math.max(
				...shape.winner.players
					.filter((p) => p !== star)
					.map((p) => (typeof p.pm === "number" ? p.pm : 0)),
			);
			if (swing < shape.margin + 6) {
				return undefined;
			}
		}
		return plusMinusNote(shape, star, rng, namesIn(writtenSoFar, shape));
	}, 3);

	// --- The losers: their best man and why it was not enough. ---
	const losers: string[] = [...cast.losers];
	writtenSoFar = [writtenSoFar, ...losers].join(" ");
	const addLoser = (beat: () => string | undefined, cap = 5) => {
		if (losers.length >= cap) {
			return;
		}
		const text = beat();
		if (text) {
			losers.push(text);
			writtenSoFar = `${writtenSoFar} ${text}`;
		}
	};
	const said = namesIn(writtenSoFar, shape);
	said.add(star.name);
	const loserBest = bestOf(shape.loser.players);
	// The losing side's best man had a season high of his own - straight
	// after his line, and only once the piece has introduced him, so the
	// sentence has a line to refer to.
	addLoser(
		() =>
			loserBest &&
			loserBest !== star &&
			said.has(loserBest.name) &&
			!foldSeasonHigh(
				[para1, storyPara, losers],
				loserBest,
				othersOf(loserBest),
			)
				? playerHighBeat(loserBest, rng, writtenSoFar)
				: undefined,
		3,
	);
	// One note of colour from the box score - a block party, a man fouling
	// out, 48 minutes - in the paragraph of the side it is about.
	const plusMinusTold = winners.some((t) =>
		/on the floor|when he sat|minutes by \d+ and lost/.test(t),
	);
	for (const beat of shuffle(rng, [
		() => defensiveNote(shape, rng, said),
		() => foulOutNote(shape, said, rng),
		() => minutesNote(shape, said),
	])) {
		const text = plusMinusTold ? undefined : beat();
		if (!text) {
			continue;
		}
		const aboutWinner =
			shape.winner.players.some((p) => text.startsWith(p.name)) ||
			(text.includes(nick(shape.winner)) && !text.includes(nick(shape.loser)));
		if (aboutWinner) {
			addWinner(() => text, 4);
		} else {
			addLoser(() => text, 3);
		}
		break;
	}

	// --- 4. What it means: the series, the season, who was missing, and
	// what is next. ---
	// Everyone named by now, the losers' second man included: "Walker Mathis
	// added 17 points in defeat. Walker Mathis returned from a three-game
	// absence with 17 points." came of a set taken before he was written.
	for (const name of namesIn(writtenSoFar, shape)) {
		said.add(name);
	}
	const context: string[] = [];
	const beatCtx: BeatContext = {
		game,
		winner: shape.winner,
		loser: shape.loser,
		margin: shape.margin,
		said,
		written: writtenSoFar,
	};
	// Three notes of context, then who was out and what is next. Six was a
	// wall of bookkeeping - streak, margin, standings, home record, season
	// series - and a reporter picks the two or three that matter, what the
	// result MEANS first and the color after.
	const addContext = (beat: () => string | undefined, cap = 3) => {
		if (context.length >= cap) {
			return;
		}
		const text = beat();
		if (text) {
			context.push(text);
			beatCtx.written = `${beatCtx.written} ${text}`;
		}
	};
	for (const text of postContext) {
		if (text !== history) {
			addContext(() => text);
		}
	}
	const seriesCount = context.length;
	// What the lede and the story told, and the body so far - without the
	// headline, whose streak is the body's to tell too.
	const story1 = writtenSoFar.slice(headline.text.length);
	addContext(() =>
		stakesSentence(
			game,
			shape,
			rng,
			/underdog|-point dogs|the wrong side of the line/.test(headline.text),
			// Not "ran off 13 straight points".
			tail.streak === "skid" ||
				tail.streak === "win" ||
				/in a row|straight (?:win|game|victor)|skid|losing streak/.test(story1),
			tail.streak === "run" ||
				new RegExp(
					String.raw`${nick(shape.loser)}'s? [\w-]+-game winning streak`,
				).test(story1),
		),
	);
	addContext(() =>
		loserSkidNote(shape, rng, beatCtx.written.slice(headline.text.length)),
	);
	addContext(() => standingsBeat(beatCtx, rng));
	addContext(() => teamHighBeat(beatCtx, rng));
	addContext(() => formNote(shape, rng, beatCtx.written));
	for (const beat of shuffle(rng, [
		() => spreadNote(game, shape, rng),
		() => scoringNormBeat(beatCtx, rng),
		() => seriesShapeBeat(beatCtx, rng),
		() => seriesBeat(beatCtx, rng),
		() => milestoneBeat(beatCtx, rng),
		() => returnBeat(beatCtx, rng),
		() => homeRoadBeat(beatCtx, rng),
		() => benchBeat(beatCtx, rng),
	])) {
		addContext(beat);
	}
	// ONE TEAM AT A TIME. The notes are chosen by what matters most, which
	// left them alternating - "The loss was the Rockets' third in a row. The
	// Jazz had not scored that many all season. ..." - and a reader switching
	// teams every sentence. The series talk stays first; after it, the
	// winners' notes and then the losers', each group in the order chosen,
	// so two facts about one team sit together and can share a subject.
	{
		const wNick = nick(shape.winner);
		const lNick = nick(shape.loser);
		const rest = context.splice(seriesCount);
		const groups: Record<"w" | "l" | "both", string[]> = {
			w: [],
			l: [],
			both: [],
		};
		let prev: "w" | "l" = "w";
		for (const text of rest) {
			const wi = text.indexOf(wNick);
			const li = text.indexOf(lNick);
			// "The Nuggets and the Rockets finished their season series level"
			// is about both, and closes the notes rather than splitting one
			// team's pair of sentences.
			if (/^The [A-Z][\w ]*? and the [A-Z]/.test(text)) {
				groups.both.push(text);
				continue;
			}
			const side: "w" | "l" =
				wi < 0 && li < 0 ? prev : li < 0 || (wi >= 0 && wi < li) ? "w" : "l";
			groups[side].push(text);
			prev = side;
		}
		context.push(...groups.w, ...groups.l, ...groups.both);
	}
	// Never dropped: who was missing is a fact a reader checks the recap
	// for, so it gets its slot however much else the game gave.
	const injury = injurySentence(
		shape,
		!!game.playoffs,
		rng,
		[beatCtx.written, ...context].join(" "),
	);
	if (injury) {
		context.push(injury);
	}
	const closer = nextGameBeat(beatCtx, rng);
	if (closer) {
		context.push(closer);
	}

	// Paragraphing. The lede stands alone, as a wire story opens, unless the
	// game story is a single sentence, which then finishes the thought. The
	// two casts are one paragraph when together they are short, and a lone
	// closing note joins the paragraph before it.
	// A lede that could only say who won takes the whole story with it.
	if (
		para1.length === 1 &&
		(storyPara.length === 1 ||
			(storyPara.length === 2 && para1[0]!.length < 90))
	) {
		// Straight after the lede, the winners are "they".
		const W = `${cap(theNick(shape.winner))} `;
		if (storyPara[0]!.startsWith(W) && para1[0]!.startsWith(W)) {
			storyPara[0] = `They ${storyPara[0]!.slice(W.length)}`;
		}
		para1.push(...storyPara.splice(0));
	}
	let people: string[][];
	if (winners.length + losers.length <= 3) {
		people = [[...winners, ...losers]];
	} else {
		people = [winners, losers];
	}
	people = people.filter((p) => p.length > 0);
	const notes = [...context];
	if (notes.length === 1 && people.length > 0 && people.at(-1)!.length <= 3) {
		people.at(-1)!.push(notes.pop()!);
	}

	const otherNick = nick(shape.loser);
	const nicks = [nick(shape.winner), otherNick];
	// Longest first, so a "Marcus Jackson Jr." is not read as "Marcus Jackson".
	const playerNames = [...shape.winner.players, ...shape.loser.players]
		.map((p) => p.name)
		.sort((a, b) => b.length - a.length);
	// The lede, the story and the casts are written whole (recapStory.ts) and
	// need no stitching. The notes are still one fact to a sentence, chosen
	// one at a time, and get the passes that make neighbours share a subject.
	const tidyNotes = (para: string[]) =>
		joinShortPairs(
			spareDoubleLabels(
				dedupePlayerSubjects(
					dedupeSubjects(spareForThe(para), otherNick, nicks),
					playerNames,
				),
			),
		).join(" ");
	const paragraphs = [para1.join(" ")];
	if (storyPara.length > 0) {
		paragraphs.push(storyPara.join(" "));
	}
	const settled = new Set<number>();
	for (const p of people) {
		settled.add(paragraphs.length);
		paragraphs.push(dedupePlayerSubjects(p, playerNames).join(" "));
	}
	if (notes.length > 0) {
		paragraphs.push(tidyNotes(notes));
	}
	// "...a 60-49 lead into the break. Neither team could shake the other
	// after the break" - one word for halftime at a time.
	const varied = paragraphs.map((para) =>
		varyHalfWords(para.split(/(?<=[!.?]) (?=[A-Z])/)).join(" "),
	);
	return `**${headline.display ?? headline.text}**\n\n${withStandingTag(
		withRecords(teamTheSwitch(varied, shape, settled), game, shape),
		game,
		shape,
		rng,
	).join("\n\n")}`;
};

// A MAN WHOSE SIDE THE SENTENCE BEFORE DID NOT SET UP. A sentence that opens
// on a bare name right after one about the other club reads as that club's
// man: "The Grizzlies got to the line more than twice as often, 32 attempts
// to 11. Cade Brooks sprained his shoulder..." - and Cade Brooks plays for
// the Hawks. He gets his club, as the wire writes it: "The Hawks' Cade
// Brooks sprained his shoulder". Across paragraphs too: a notes paragraph
// opens straight after the losers' lines.
export const teamTheSwitch = (
	paragraphs: string[],
	shape: Shape,
	// Paragraphs that say whose men they are in every sentence (the casts):
	// read for what the next paragraph follows, never rewritten.
	settled: ReadonlySet<number> = new Set(),
): string[] => {
	const sides = [shape.winner, shape.loser];
	const sideOfName = new Map<string, RecapTeam>();
	for (const t of sides) {
		for (const p of t.players) {
			sideOfName.set(p.name, t);
		}
	}
	const names = [...sideOfName.keys()].sort((a, b) => b.length - a.length);
	let prev = "";
	return paragraphs.map((para, at) => {
		const sentences = para.split(/(?<=[!.?])\s+(?=[A-Z])/);
		if (settled.has(at)) {
			prev = sentences.at(-1) ?? prev;
			return para;
		}
		for (let i = 0; i < sentences.length; i++) {
			const cur = sentences[i]!;
			const who = names.find(
				(n) => cur.startsWith(`${n} `) && !cur.startsWith(`${n}'`),
			);
			const team = who ? sideOfName.get(who) : undefined;
			if (who && team) {
				const other = sides.find((t) => t !== team)!;
				const mentions = (s: string, t: RecapTeam) =>
					s.includes(nick(t)) ||
					t.players.some((p) => p.name !== who && s.includes(p.name));
				// About the other club: names it or its men and not his. Not a
				// sentence of the game's flow that names it as the object of
				// "they" ("In the second half, they outscored the Spurs 53-37"),
				// or as the side that never got back in front: those are about
				// both.
				const aboutOther =
					mentions(prev, other) &&
					!/\bthey\b|\bnever (?:led|trailed|got back)/i.test(prev);
				if (
					prev !== "" &&
					!cur.includes(nick(team)) &&
					!prev.includes(who) &&
					aboutOther &&
					!mentions(prev, team)
				) {
					sentences[i] = `${cap(poss(theNick(team)))} ${cur}`;
				}
			}
			prev = sentences[i]!;
		}
		return sentences.join(" ");
	});
};

// Everyone the text could name: who played, and who sat out hurt.
const peopleIn = (games: readonly RecapGame[]): string[] =>
	games.flatMap((g) =>
		g.teams.flatMap((t) => [
			...t.players.map((p) => p.name),
			...(t.injuries ?? []).map((p) => p.name),
		]),
	);
const teamsIn = (games: readonly RecapGame[]): ReferenceTeam[] =>
	games.flatMap((g) =>
		g.teams.map((t) => ({ name: t.name ?? "", region: t.region ?? "" })),
	);

// A RUN, SAID MORE THAN ONE WAY. "Ran off 14 straight points" was in more
// than a quarter of all recaps, the same three words every time; a desk
// writes "went on a 14-0 run" and "scored 14 unanswered points" as often.
// The body only (headlines have their own verbs), each run on its own draw,
// seeded by the game so the same game always reads the same way.
export const varyRuns = (text: string, seed: number): string => {
	const rng = rngFromSeed(seed * 2246822519 + 3266489917);
	const cut = text.indexOf("\n\n");
	if (cut < 0) {
		return text;
	}
	// Sentence by sentence, so "scored" is not put beside another "scored":
	// "they outscored the Bulls 64-38, scored 18 unanswered points".
	const body = text
		.slice(cut)
		.split(/(?<=[!.?])(?=\s)/)
		.map((sentence) =>
			sentence.replaceAll(
				/\bran off (\d+) straight points\b/g,
				(m, n: string) => {
					const r = rng();
					const scoredAlready = /scor/.test(sentence);
					return r < 0.4
						? m
						: r < 0.75 || scoredAlready
							? `went on a ${n}-0 run`
							: `scored ${n} unanswered points`;
				},
			),
		)
		.join("");
	return text.slice(0, cut) + body;
};

// "It was 78-78 going into the fourth" is a box score talking; a writer says
// "tied at 78". Only where the checker reads a tie the same way: at the
// half, going into a quarter, and on the clock late.
export const tiedScores = (text: string): string => {
	const cut = text.indexOf("\n\n");
	if (cut < 0) {
		return text;
	}
	const body = text
		.slice(cut)
		.split(/(?<=[!.?])(?=\s)/)
		.map((sentence) => {
			let out = sentence.replaceAll(
				/\b([Ii]t was) (\d{2,3})-\2(?= (?:at halftime|at the break|at the half\b|going into the|with (?:two minutes|\d)))/g,
				"$1 tied at $2",
			);
			if (
				/seconds? (?:left|to go|to play)|\d:\d\d (?:left|to go|to play)|two minutes/.test(
					out,
				)
			) {
				out = out
					.replace(
						/^(\s*With two minutes (?:to play|left), it was) (\d{2,3})-\2\./,
						"$1 tied at $2.",
					)
					.replaceAll(/\bmade it (\d{2,3})-\1\b/g, "tied it at $1");
			}
			return out;
		})
		.join("");
	return text.slice(0, cut) + body;
};

// "The Thunder were held to 77. The Thunder's Corey Gilmore left with a
// concussion. The Thunder play the Suns next" - the second time running that
// a team opens a sentence, a writer says "They". Not when the sentence before
// names the other team too, and not across a paragraph break.
export const teamOpeners = (
	text: string,
	teams: readonly ReferenceTeam[],
): string => {
	const cut = text.indexOf("\n\n");
	const nicks = teams.map((t) => t.name.trim()).filter(Boolean);
	if (cut < 0 || nicks.length === 0) {
		return text;
	}
	const opener = (sentence: string): string | undefined => {
		const t = sentence.trimStart();
		return nicks.find((n) =>
			[" ", ", ", "' ", "'s "].some((end) => t.startsWith(`The ${n}${end}`)),
		);
	};
	const parts = text.slice(cut).split(/(?<=[!.?])(?=\s)/);
	for (let i = 1; i < parts.length; i++) {
		const prev = parts[i - 1]!;
		const cur = parts[i]!;
		const team = opener(prev);
		if (
			!team ||
			/^\s*\n/.test(cur) ||
			!cur.trimStart().startsWith(`The ${team} `) ||
			nicks.some((n) => n !== team && prev.includes(n))
		) {
			continue;
		}
		parts[i] = cur.replace(`The ${team} `, "They ");
	}
	return text.slice(0, cut) + parts.join("");
};

export const getAutoRecap = (game: RecapGame): string => {
	const text = fixArticles(
		tiedScores(varyRuns(buildGameRecap(withSettledLead(game)), game.gid + 1)),
	);
	const names = peopleIn([game]);
	const teams = teamsIn([game]);
	const referenced = secondReferences(
		game.allStar
			? text
			: cityReferences(
					teamOpeners(text, teams),
					teams,
					names,
					(game.gid + 1) * 7919 + 17,
				),
		names,
		teams,
	);
	return game.gender === "female"
		? inWomensLeague(referenced, names, teams)
		: referenced;
};

// NOT "FOR THE LAKERS" TWICE RUNNING. "Naji Green supplied 19 points and
// Keegan Green 16 for the Lakers. Shots fell for the Lakers - 54.2% from the
// field." Three stock lines say "for the X", and right behind a sentence that
// just did, the pair is an echo (43 of them in a 954-game corpus). They take
// a sibling's shape instead: "The Lakers shot 54.2% from the field."
export const spareForThe = (sentences: string[]): string[] =>
	sentences.map((cur, i) => {
		const prev = sentences[i - 1];
		const team =
			/^(?:Shots fell|It came from everywhere|Up next|Next up) for the (.+?)(?: - |: | is )/.exec(
				cur,
			)?.[1];
		if (prev === undefined || !team || !prev.includes(`for the ${team}`)) {
			return cur;
		}
		return cur
			.replace(
				/^Shots fell for the (.+?) - ([\d.]+%) from the field\.$/,
				"The $1 shot $2 from the field.",
			)
			.replace(
				/^It came from everywhere for the (.+?) - (\w+) in double figures\.$/,
				(_m, t: string, n: string) => `${cap(n)} ${t} reached double figures.`,
			)
			.replace(
				/^(?:Up next for the (.+?): |Next up for the (.+?) is )(the .+?), (at home|on the road)\.$/,
				(
					_m,
					a: string | undefined,
					b: string | undefined,
					opp: string,
					where: string,
				) => `The ${a ?? b} face ${opp} next, ${where}.`,
			);
	});

// ONE "DOUBLE-DOUBLE" A PARAGRAPH. "Walker Ingram had a double-double of
// his own, 21 points and 12 assists, and Xavier Ingram posted a
// double-double, 18 points and 10 rebounds. Keegan Green finished with a
// double-double (16 points, 10 assists, and 5 blocks) for the Lakers." - the
// label three times in two sentences, and twice or more in one paragraph of
// ten. The numbers say it: the label is said once, and the lines after it
// are told plain ("Xavier Ingram posted 18 points and 10 rebounds"). A
// triple-double is always called one.
export const spareDoubleLabels = (sentences: string[]): string[] => {
	let said = false;
	return sentences.map((sentence) =>
		sentence.replace(
			/\ba double-double(?: of his own)?(?:,| with) (?=\d|a season-high)|\ba double-double \(((?:a season-high )?\d[^)]*)\)|(?:'s|') double-double \(((?:a season-high )?\d[^)]*)\)|\bdouble-double\b(?!s)/g,
			(m: string, line?: string, owned?: string) => {
				if (!said) {
					said = true;
					return m;
				}
				// "Jalen Green's double-double (14 points and 16 rebounds) was
				// the best of the Clippers": the man stays the subject, not his
				// numbers.
				if (owned !== undefined) {
					return ` (${owned})`;
				}
				if (line !== undefined) {
					return line;
				}
				return m.startsWith("a ") ? "" : m;
			},
		),
	);
};

// ONE WORD FOR HALFTIME AT A TIME. "The Timberwolves took a 56-52 lead into
// the break. After the break, they outscored the Pacers 56-43" - each
// sentence fine, the pair an echo, and in one recap of 16. A sentence that
// says "after the break" or "after halftime" right behind one that used the
// same word takes another name for it.
const HALF_WORDS = [
	["after halftime", /halftime/],
	["after the break", /\bbreak\b/],
	["in the second half", /second half/],
] as const;
export const varyHalfWords = (sentences: string[]): string[] => {
	const out = [...sentences];
	for (let i = 1; i < out.length; i++) {
		const prev = out[i - 1]!;
		const cur = out[i]!;
		for (const [phrase, word] of HALF_WORDS) {
			const at = cur.toLowerCase().indexOf(phrase);
			if (at < 0 || !word.test(prev)) {
				continue;
			}
			const rest = cur.slice(0, at) + cur.slice(at + phrase.length);
			const other = HALF_WORDS.find(
				([, w]) => !w.test(prev) && !w.test(rest),
			)?.[0];
			if (other) {
				const said = at === 0 ? cap(other) : other;
				out[i] = `${cur.slice(0, at)}${said}${cur.slice(at + phrase.length)}`;
			}
			break;
		}
	}
	return out;
};

// The series sentences postseasonContext writes for a game, for tests that
// need every wording of them.
export const postseasonSentences = (
	game: RecapGame,
	rng: () => number,
): string[] => postseasonContext(game, analyzeShape(game), rng).sentences;

// THE RECORD IN PARENTHESES. "The Wizards (10-3) held off the 76ers (5-8)" is
// the most recognizable mark of wire copy there is, and the one fact every
// reader wants that a box score does not show. Each team gets it once, on
// its first mention in the body that is not a possessive - "the Wizards'
// (10-3) win" is not a sentence anyone writes - and only in the regular
// season, whose record it is.
const withRecords = (
	paragraphs: string[],
	game: RecapGame,
	shape: Shape,
): string[] => {
	if (game.playoffs || game.allStar) {
		return paragraphs;
	}
	const out = [...paragraphs];
	for (const t of [shape.winner, shape.loser]) {
		const rec = t.record;
		if (!rec || rec.won + rec.lost === 0) {
			continue;
		}
		const tag = `${rec.won}-${rec.lost}`;
		// The body already states it ("improved to 10-3"). Whole numbers
		// only: "0-1" is inside "110-109". The body as written, not as this
		// pass has tagged it: two 3-3 teams each get their record, where the
		// second used to find the first one's and go without.
		const stated = new RegExp(String.raw`(?<![\d-])${tag}(?![\d-])`);
		if (paragraphs.some((p) => stated.test(p))) {
			continue;
		}
		// In the lede, where wire copy carries it, or not at all. A team the
		// lede did not name by its plain nickname had its record hung on the
		// next mention down the piece, wherever that was: "In the second
		// half, they outscored the Raptors (0-4) 52-34" reads as a score.
		const re = new RegExp(
			String.raw`\b([Tt]he (?:visiting )?${nick(t)})(?=[ ,.;:])`,
		);
		const m = re.exec(out[0] ?? "");
		if (m) {
			out[0] = `${out[0]!.slice(0, m.index + m[0].length)} (${tag})${out[0]!.slice(m.index + m[0].length)}`;
		}
	}
	return out;
};

// THE EAST-LEADING KNICKS. A wire lede tags a conference leader where it
// names it - "The Raptors held off the East-leading Knicks 114-109" - and
// the engine knew where every team stood without ever saying it there. Only
// a team that came in first, a dozen games or more into a season, in a
// league whose conferences are East and West; the loser first, since a
// leader beaten is the news, and not every time.
const withStandingTag = (
	paragraphs: string[],
	game: RecapGame,
	shape: Shape,
	rng: () => number,
): string[] => {
	if (game.playoffs || game.allStar || paragraphs.length === 0) {
		return paragraphs;
	}
	const lede = paragraphs[0]!;
	const firstEnd = lede.search(/[!.?](?:\s|$)/);
	const first = firstEnd < 0 ? lede : lede.slice(0, firstEnd);
	for (const t of [shape.loser, shape.winner]) {
		const st = t.standing;
		const rec = t.record;
		const conf = /^(east|west)(?:ern)? conference$/i.exec(st?.conf ?? "")?.[1];
		if (
			!st ||
			!rec ||
			!conf ||
			st.rankBefore !== 1 ||
			st.teams < 4 ||
			rec.won + rec.lost - 1 < 12
		) {
			continue;
		}
		const word = `${conf[0]!.toUpperCase()}${conf.slice(1).toLowerCase()}`;
		// The rest of the piece about where they stand says it in its own
		// words; a lede tag on top of "atop the East" is the same fact twice.
		if (
			paragraphs.some((p) =>
				new RegExp(
					String.raw`-leading|\b(?:atop|top of|first in|lead) the ${word}`,
					"i",
				).test(p),
			)
		) {
			return paragraphs;
		}
		const re = new RegExp(String.raw`\b([Tt])he ${nick(t)}(?=[ ,.;:])(?!')`);
		const m = re.exec(first);
		if (!m || /visiting $/.test(first.slice(0, m.index)) || rng() >= 0.6) {
			continue;
		}
		const tagged = `${m[1]}he ${word}-leading ${nick(t)}`;
		return [
			lede.slice(0, m.index) + tagged + lede.slice(m.index + m[0].length),
			...paragraphs.slice(1),
		];
	}
	return paragraphs;
};

// A GAME NOBODY WON. A league that caps overtime can end a game level; one
// that settles it with a shootout stores equal points and a shootout score.
// The stored row still names a "winner" (the home side), and the engine
// wrote "the Wizards beat the 76ers 100-100". A tie reads as a tie, a
// shootout as a shootout, and neither leans on anything the shape of a won
// game supplies.
const buildTie = (game: RecapGame, shape: Shape, rng: () => number): string => {
	const [home, away] = game.teams;
	const pts = home.pts;
	const H = theNick(home);
	const A = theNick(away);
	const ot =
		shape.ot === 0
			? ""
			: shape.ot === 1
				? " after an overtime"
				: ` after ${numWord(shape.ot)} overtimes`;
	const bestH = bestOf(home.players);
	const bestA = bestOf(away.players);
	const lines = [
		bestH ? ([bestH, home] as const) : undefined,
		bestA ? ([bestA, away] as const) : undefined,
	]
		.filter((x): x is readonly [RecapPlayer, RecapTeam] => !!x)
		.map(([p, t]) => `${p.name} had ${statPhrase(p)} for ${theNick(t)}`);
	if (game.shootout) {
		const winner = game.winnerTid === home.tid ? home : away;
		const loser = winner === home ? away : home;
		const so = game.shootout;
		return [
			`**${nick(winner)} beat the ${nick(loser)} in a shootout, ${pts}-${pts} (${so.won}-${so.lost})**`,
			`${cap(theNick(winner))} and ${theNick(loser)} were tied at ${pts}${ot}, and ${theNick(winner)} took the shootout ${so.won}-${so.lost}.${lines.length > 0 ? ` ${cap(lines.join(", and "))}.` : ""}`,
		].join("\n\n");
	}
	// The headline has the score; the body has the rest.
	const body: string[] = [
		pick(
			rng,
			[
				`${cap(H)} and ${A} played to a tie${ot}.`,
				`Nobody won: ${H} and ${A} finished tied${ot}.`,
				`${cap(H)} and ${A} settled for a tie${ot}.`,
			],
			"tieLede",
		),
	];
	if (lines.length > 0) {
		body.push(`${cap(lines.join(", and "))}.`);
	}
	const half = halftimeNote(shape, rng);
	if (half) {
		body.push(half);
	}
	return `**${nick(home)} and ${nick(away)} play to a ${pts}-${pts} tie**\n\n${body.join(" ")}`;
};

// --- Entry point: a whole day --------------------------------------------------

export type AutoDayRecapInput = {
	season: number;
	day: number;
	playoffs: boolean;
	games: RecapGame[];
	standings?: RecapDayStandings;
};

// One player's line pulled up to league level, with team/opponent context, so the
// day recap can call out the best performances across every game.
export type LeaguePerformance = {
	p: RecapPlayer;
	team: RecapTeam;
	opp: RecapTeam;
	won: boolean;
	game: RecapGame;
};

// How much a game deserves to be the day's marquee story.
export const notability = (game: RecapGame): number => {
	if (game.allStar) {
		return 1000;
	}
	const shape = analyzeShape(game);
	const star = storyPlayerOf(game, shape);
	let n = star ? impact(star) : 0;
	if (clutchShot(game) && !clutchShot(game)!.tying) {
		n += 120;
	}
	if (game.playoffs) {
		n += 60;
		const post = postseasonContext(game, shape);
		if (post.headlineTag) {
			n += 40;
		}
		// A clinch is the biggest story - bigger than a game-winner in a
		// Game 4 (+120): "Payton King's 29-point double-double powers the Suns
		// past the Heat" headlined a night the Bucks finished a sweep, told
		// in the fourth clause of a list.
		if (post.clinch) {
			n += post.clinch.title ? 200 : 150;
		}
		// THE SERIES STAKES. A night full of playoff games was ranked almost
		// entirely on box scores, so a routine 2-1 with a big star line
		// out-scored the game that put a team on the brink and the eighth seed
		// taking a series lead from the first - the two stories any bracket
		// reader looks for first.
		const series = game.series;
		if (series?.bestOf && series.bestOf > 1) {
			const winnerIsHome = shape.winner.abbrev === series.homeAbbrev;
			const wAfter = (winnerIsHome ? series.homeWon : series.awayWon) + 1;
			const lWins = winnerIsHome ? series.awayWon : series.homeWon;
			const need = Math.floor(series.bestOf / 2) + 1;
			if (wAfter === need - 1 && wAfter - lWins >= 2) {
				// Pushed the opponent to the brink.
				n += 45;
			}
			if (lWins === need - 1) {
				// Survived an elimination game.
				n += 55;
			}
			const wSeed = winnerIsHome ? series.homeSeed : series.awaySeed;
			const lSeed = winnerIsHome ? series.awaySeed : series.homeSeed;
			if (
				wSeed !== undefined &&
				lSeed !== undefined &&
				wSeed - lSeed >= 3 &&
				wAfter > lWins &&
				series.round < series.numRounds
			) {
				// A low seed leading a high one. Not in the Finals, where the
				// seeds are from different conferences.
				n += 15 * (wSeed - lSeed);
			}
			n += 20 * (series.round - 1);
		}
	}
	if (isUpset(game, shape)) {
		n += 35 + (game.spread?.points ?? 0);
	}
	n += shape.ot * 25;
	if (shape.margin <= 3) {
		n += 20;
	}
	if (comebackSize(shape) >= 15) {
		n += 25;
	}
	if (star && doubleCategories(star).length >= 3) {
		n += 35;
	}
	if (star && star.pts >= 40) {
		n += star.pts;
	}
	// A 50-point night in a loss is the night's story wherever it happened.
	const loserBest = bestOf(shape.loser.players);
	if (loserBest && loserBest.pts >= 45) {
		n += loserBest.pts;
	}
	return n;
};

export const conferencePictureSentence = (
	standings: RecapDayStandings | undefined,
	rng: () => number,
	// Teams this sentence names, so the movers beat after it does not say
	// "the Grizzlies climbed to first" right under "the Grizzlies are 1 game
	// clear at the top".
	said?: Set<number>,
	// The schedule day, which anchors the phrasing rotation: this sentence
	// runs every night, and without the anchor "cling to the top of the
	// West" showed up three evenings running.
	day?: number,
	// Tonight's results, and where each team that played stood before them.
	// A conference's top is news on a night that changed it - a new leader,
	// or the leader losing - and a perfect record at every fifth win.
	// Otherwise the sentence said every night what the night before had
	// said: "the Nets cling to the top of the Eastern Conference" on six
	// evenings running.
	tonight?: {
		played: ReadonlySet<number>;
		won: ReadonlySet<number>;
		rankBefore: ReadonlyMap<number, number>;
	},
): string | undefined => {
	if (!standings || standings.confs.length === 0) {
		return undefined;
	}
	const bits: string[] = [];
	for (const conf of standings.confs) {
		const leader = conf.teams[0];
		const second = conf.teams[1];
		if (!leader) {
			continue;
		}
		// "Hawks (1-0) lead the East" is noise, not news - the standings only mean
		// something once a real sample exists. Ten games, or five without a
		// loss: "the Timberwolves (4-1) hold a narrow lead in the Western
		// Conference" was 15 of a corpus's 25 standings lines, and said
		// nothing; "still perfect at 5-0" is a story.
		if (leader.won + leader.lost < (leader.lost === 0 ? 5 : 10)) {
			continue;
		}
		const moved =
			!tonight ||
			leader.tid === undefined ||
			(tonight.played.has(leader.tid) &&
				((tonight.rankBefore.get(leader.tid) ?? 1) > 1 ||
					!tonight.won.has(leader.tid) ||
					(leader.lost === 0 && leader.won % 5 === 0)));
		if (!moved) {
			continue;
		}
		if (leader.tid !== undefined) {
			said?.add(leader.tid);
		}
		// Nicknames, like every other team reference in the piece. "Cleveland
		// Cavaliers (47-4) lead the Eastern Conference" in a paragraph that has
		// said "the Cavaliers" four times reads like it was pasted in.
		const who = `the ${leader.name} (${leader.won}-${leader.lost})`;
		// Rotated, because a two-conference league renders this branch TWICE in
		// one sentence and the fixed phrasing made both halves identical: "the
		// Heat (30-12) lead the Eastern Conference by three games and the Lakers
		// (32-10) lead the Western Conference by two games."
		//
		// An unbeaten leader is the story itself, not a "narrow lead".
		if (leader.lost === 0) {
			bits.push(
				pickByDay(
					day,
					rng,
					[
						`the ${leader.name} are still perfect at ${leader.won}-0 atop the ${conf.name}`,
						// No "and" inside a bit: the bits are joined with one, and
						// "16-0 and top of the East and the Hornets lead the West"
						// was one sentence.
						`nobody has beaten the ${leader.name} yet, ${leader.won}-0 at the top of the ${conf.name}`,
					],
					"dayStandingsPerfect",
				),
			);
		} else if (second && second.gb >= 1) {
			bits.push(
				pickByDay(
					day,
					rng,
					[
						`${who} lead the ${conf.name} by ${gbText(second.gb)}`,
						`${who} are ${gbText(second.gb)} clear at the top of the ${conf.name}`,
						`${gbText(second.gb)} ${second.gb === 1 ? "separates" : "separate"} ${who} from the rest of the ${conf.name}`,
					],
					"dayStandingsLead",
				),
			);
		} else if (second) {
			bits.push(
				pickByDay(
					day,
					rng,
					[
						`${who} hold a narrow lead in the ${conf.name}`,
						// No trailing comma clause here. These bits are joined with
						// " and ", so "...are top of the East, but only just" ran
						// straight into the next conference as one long run-on.
						`${who} cling to the top of the ${conf.name}`,
					],
					"dayStandingsNarrow",
				),
			);
		} else {
			bits.push(`${who} sit atop the ${conf.name}`);
		}
	}
	if (bits.length === 0) {
		return undefined;
	}
	// The same closer every night reads like a form letter; the frame rotates
	// even though the facts inside it cannot.
	//
	// "Nobody has beaten the Bucks yet, 6-0 at the top of the East and the
	// Timberwolves are still perfect" ran two conferences together: a bit
	// carrying its own comma needs one before the "and" too.
	const list =
		bits.length === 2 && bits.some((b) => b.includes(","))
			? bits.join(", and ")
			: naturalList(bits);
	return pickByDay(
		day,
		rng,
		[
			`In the standings, ${list}.`,
			`As for the standings, ${list}.`,
			`As it stands, ${list}.`,
			`The bigger picture: ${list}.`,
		],
		"dayStandings",
	);
};

// A compact, varied series-state clause for one playoff/play-in game, for the day
// wrap's postseason roundup (lower-cased, no trailing period).
//
// The round is returned SEPARATELY rather than baked into the text. A day of
// eight first-round games produced "the Curses are up 3-1 in the First Round,
// the Riots grabbed a 3-1 edge in the First Round, ..." - the same four words
// in every clause. The caller factors a shared round out to the front instead.
// WHAT A PLAYOFF RESULT DID TO ITS SERIES, said on the result itself. The
// night wrap told each game twice: "the Thunder downed the Nets 84-74" in
// one paragraph, "the Thunder are up 1-0 on the Nets" in a list two
// paragraphs on - and the list stopped at four, so some games had a state
// and some did not. A wire roundup says it once: "the Thunder downed the
// Nets 84-74 for a 1-0 series lead". `tail` follows a clause whose subject
// is the winner ("...beat the Nets 84-74"); `who` follows one that ends on
// the winner as its object ("...loss to the Heat, who lead the series 3-1").
//
// Each comes in a few wordings, used in turn down the wrap: a night of
// openers read "for a 1-0 series lead" eight times.
export const seriesTail = (
	g: RecapGame,
): { kind: string; tails: string[]; whos: string[] } | undefined => {
	const s = g.series;
	if (g.playIn || !s || typeof s.bestOf !== "number" || s.bestOf <= 1) {
		return undefined;
	}
	const shape = analyzeShape(g);
	if (shape.margin === 0) {
		return undefined;
	}
	const winnerIsHome = shape.winner.abbrev === s.homeAbbrev;
	const lBefore = winnerIsHome ? s.awayWon : s.homeWon;
	const wAfter = (winnerIsHome ? s.homeWon : s.awayWon) + 1;
	const need = Math.floor(s.bestOf / 2) + 1;
	const games = wAfter + lBefore;
	const score = `${wAfter}-${lBefore}`;
	const down = `${lBefore}-${wAfter}`;
	if (wAfter >= need) {
		if (s.round === s.numRounds) {
			return {
				kind: "title",
				tails: ["to win the title"],
				whos: ["who won the title"],
			};
		}
		if (lBefore === 0) {
			return {
				kind: "sweep",
				tails: [
					"to sweep the series",
					"to finish a sweep",
					`to complete ${aNumWord(games)}-game sweep`,
				],
				whos: [
					"who swept the series",
					"who finished a sweep",
					`who completed ${aNumWord(games)}-game sweep`,
				],
			};
		}
		if (games === s.bestOf) {
			return {
				kind: "decider",
				tails: [
					`to win the series in Game ${games}`,
					`to take Game ${games} and the series`,
				],
				whos: [
					`who won the series in Game ${games}`,
					`who took Game ${games} and the series`,
				],
			};
		}
		const n = numWord(games);
		return {
			kind: "close",
			tails: [`to close out the series in ${n}`, `to win the series in ${n}`],
			whos: [`who closed out the series in ${n}`, `who won the series in ${n}`],
		};
	}
	if (wAfter === lBefore) {
		return wAfter === need - 1
			? {
					kind: "force",
					tails: [`to force a Game ${s.bestOf}`],
					whos: [`who forced a Game ${s.bestOf}`],
				}
			: {
					kind: "even",
					tails: [
						`to even the series ${score}`,
						`to tie the series ${score}`,
						`to square the series at ${score}`,
					],
					whos: [
						`who evened the series ${score}`,
						`who tied the series ${score}`,
						`who squared the series at ${score}`,
					],
				};
	}
	if (games === 1) {
		return {
			kind: "opener",
			tails: ["in Game 1", "to open the series", "for a 1-0 series lead"],
			whos: [
				"who took Game 1",
				"who won the opener",
				"who lead the series 1-0",
			],
		};
	}
	if (wAfter > lBefore) {
		return {
			kind: "lead",
			tails: [
				`for a ${score} series lead`,
				`to go up ${score}`,
				`to take a ${score} lead in the series`,
			],
			whos: [
				`who lead the series ${score}`,
				`who went up ${score}`,
				`who took a ${score} lead in the series`,
			],
		};
	}
	// No comma inside: these sit in comma-separated lists, and "the Clippers
	// got past the 76ers 113-106 to stay alive, down 3-1, the Cavaliers held
	// off..." lost the reader. And not "to stay alive and pull within 3-1":
	// a series score is not a margin to pull within. What the win bought is
	// the next game.
	if (lBefore === need - 1) {
		return wAfter === 1
			? {
					kind: "alive",
					tails: ["to avoid a sweep", `to force a Game ${games + 1}`],
					whos: ["who avoided a sweep", `who forced a Game ${games + 1}`],
				}
			: {
					kind: "alive",
					tails: [`to force a Game ${games + 1}`],
					whos: [`who forced a Game ${games + 1}`],
				};
	}
	return {
		kind: "within",
		tails: [
			`to cut the series deficit to ${down}`,
			`to trim their series deficit to ${down}`,
		],
		whos: [
			`who cut the series deficit to ${down}`,
			`who trimmed their series deficit to ${down}`,
		],
	};
};

// League-wide notes read off the whole slate: the night's blowout, its
// shootout, the best shooting performance. A day wrap that only lists results
// never tells you what KIND of night it was.
export const leagueNotes = (
	games: RecapGame[],
	rng: () => number,
	limit: number,
	// What the wrap has written so far. "The Pistons routed the Jazz by 34"
	// in the results paragraph, then "the night's most one-sided result was
	// the Pistons' 34-point win over the Jazz" two sentences later, is the
	// same fact told twice.
	written = "",
): string[] => {
	const real = games.filter((g) => !g.allStar);
	if (real.length < 4) {
		return [];
	}

	// tid: which team the note is ABOUT, so the wrap doesn't spend two of its
	// three league-wide observations on the same club - "the most one-sided
	// result was the Raccoons' 35-point win" followed by "the Raccoons shot the
	// ball best of anyone" is one team's night, not the league's.
	type Cand = { sort: number; text: string; tid?: number };
	const cands: Cand[] = [];

	let biggest: { shape: Shape; margin: number } | undefined;
	let highest: { shape: Shape; total: number } | undefined;
	let hottest: { shape: Shape; fgp: number } | undefined;
	let bombs: { shape: Shape; tp: number } | undefined;
	// The game that kept changing hands, from the score log where there is one.
	let seesaw: { shape: Shape; changes: number } | undefined;
	for (const g of real) {
		const shape = analyzeShape(g);
		const total = shape.winner.pts + shape.loser.pts;
		const changes = g.flow?.leadChanges ?? 0;
		// A seesaw is a game that stayed one: "traded the lead 14 times" over
		// a 13-point final is a first-half fact about a second-half rout. And
		// one the night has to notice: 12 changes is a tenth of all games, so
		// most slates had one and the note ran in 65 of 177 wraps; 16 is one
		// game in twenty.
		if (
			changes >= 16 &&
			shape.margin <= 8 &&
			(!seesaw || changes > seesaw.changes)
		) {
			seesaw = { shape, changes };
		}
		if (!biggest || shape.margin > biggest.margin) {
			biggest = { shape, margin: shape.margin };
		}
		if (!highest || total > highest.total) {
			highest = { shape, total };
		}
		const w = teamStats(shape.winner);
		if (w.fga >= 60 && (!hottest || w.fgp > hottest.fgp)) {
			hottest = { shape, fgp: w.fgp };
		}
		if (w.tp >= 15 && (!bombs || w.tp > bombs.tp)) {
			bombs = { shape, tp: w.tp };
		}
	}

	// A rout the wrap already told, by its margin or by its score: after
	// "the Mavericks routed the Rockets 121-93", "The night's most one-sided
	// result was the Mavericks' 28-point win over the Rockets" only did the
	// subtraction.
	const routTold =
		biggest !== undefined &&
		(new RegExp(
			String.raw`${nick(biggest.shape.winner).replaceAll(/[$()*+.?[\\\]^{|}]/g, String.raw`\$&`)}.*(?:by ${biggest.margin}\b|${biggest.margin}-point)`,
		).test(written) ||
			written
				.split(/(?<=[!.?])\s+/)
				.some(
					(t) =>
						t.includes(nick(biggest!.shape.winner)) &&
						t.includes(nick(biggest!.shape.loser)) &&
						t.includes(
							`${biggest!.shape.winner.pts}-${biggest!.shape.loser.pts}`,
						),
				));
	if (biggest && biggest.margin >= 25 && !routTold) {
		cands.push({
			sort: 0,
			tid: biggest.shape.winner.tid,
			text: `The night's most one-sided result was ${poss(
				theNick(biggest.shape.winner),
			)} ${biggest.margin}-point win over ${theNick(biggest.shape.loser)}.`,
		});
	}
	// These notes run most nights, so their shapes rotate by the calendar like
	// the standings furniture: "combined for 245 points, the most of any game
	// on the slate" two evenings running is the tell of a template.
	const day = games[0]?.day;
	if (seesaw) {
		cands.push({
			sort: 0.5,
			tid: seesaw.shape.winner.tid,
			text: pickByDay(
				day,
				rng,
				[
					`${cap(theNick(seesaw.shape.winner))} and ${theNick(seesaw.shape.loser)} traded the lead ${seesaw.changes} times.`,
					// poss(), not a hardcoded apostrophe: four nicknames in a
					// default league do not end in s, and this read "the Magic'
					// game with the Raptors".
					`${cap(poss(theNick(seesaw.shape.winner)))} game with ${theNick(seesaw.shape.loser)} had ${seesaw.changes} lead changes.`,
				],
				"daySeesaw",
			),
		});
	}
	// Nor a total the wrap has already given as a score: "No game topped the
	// Bucks' 253-point shootout with the Pistons" closed a four-game wrap that
	// had opened on the Bucks' 129-124 win.
	const totalTold =
		highest !== undefined &&
		highest.total < 260 &&
		written.includes(`${highest.shape.winner.pts}-${highest.shape.loser.pts}`);
	if (highest && highest.total >= 240 && !totalTold) {
		const ot = highest.shape.ot;
		cands.push({
			sort: 1,
			tid: highest.shape.winner.tid,
			text:
				ot > 0
					? `${cap(theNick(highest.shape.winner))} and ${theNick(
							highest.shape.loser,
						)} put up the slate's biggest total, ${highest.total} points in ${
							ot === 1
								? "an overtime game"
								: ot <= 3
									? `a ${otPhrase(ot).replace(" ", "-")} game`
									: `a game that went ${numWord(ot)} overtimes`
						}.`
					: pickByDay(
							day,
							rng,
							[
								`${cap(theNick(highest.shape.winner))} and ${theNick(
									highest.shape.loser,
								)} combined for ${highest.total} points, the night's most.`,
								`No game topped ${poss(theNick(highest.shape.winner))} ${highest.total}-point shootout with ${theNick(highest.shape.loser)}.`,
							],
							"dayHighTotal",
						),
		});
	}
	// 53% is a good night; it is not "best of anyone" news two nights in three.
	// Nor is 56%: some club shoots that most nights.
	if (hottest && hottest.fgp >= 58) {
		cands.push({
			sort: 2,
			tid: hottest.shape.winner.tid,
			text: pickByDay(
				day,
				rng,
				[
					`${cap(theNick(hottest.shape.winner))} shot the ball best of anyone, ${hottest.fgp}% from the field.`,
					`Nobody shot it better than ${theNick(hottest.shape.winner)}, ${hottest.fgp}% from the field.`,
				],
				"dayHotShooting",
			),
		});
	}
	// Twenty, not seventeen: some club hits seventeen most nights, and "The
	// Thunder made the most threes on the slate, 18" closed one wrap in six.
	if (bombs && bombs.tp >= 20) {
		cands.push({
			sort: 3,
			tid: bombs.shape.winner.tid,
			text: pickByDay(
				day,
				rng,
				[
					`${cap(theNick(bombs.shape.winner))} made the most threes on the slate, ${bombs.tp}.`,
					`No team hit more threes than ${poss(theNick(bombs.shape.winner))} ${bombs.tp}.`,
				],
				"dayThrees",
			),
		});
	}

	// (The home teams' night is nightThemeSentence's, which opens the
	// paragraph these notes fill.)

	const chosen: Cand[] = [];
	const usedTids = new Set<number>();
	for (const cand of shuffle(rng, cands).sort((a, b) => a.sort - b.sort)) {
		if (cand.tid !== undefined && usedTids.has(cand.tid)) {
			continue;
		}
		if (cand.tid !== undefined) {
			usedTids.add(cand.tid);
		}
		chosen.push(cand);
		if (chosen.length >= limit) {
			break;
		}
	}
	return chosen.map((c) => c.text);
};

// One night, one pool of phrasing - see pick(). The batch has to be CLOSED
// again: leaving it open left inBatch true for the rest of the session, so a
// single recap generated afterwards (the box score page) skipped its own reset
// and inherited a day's worth of exhausted phrase pools - and generating the
// same game twice stopped producing the same text.
export const getAutoDayRecap = (input: AutoDayRecapInput): string => {
	beginRecapBatch();
	resetVerbLedger();
	try {
		const names = peopleIn(input.games);
		const teams = teamsIn(input.games);
		const text = secondReferences(
			fixArticles(
				buildDayRecap({ ...input, games: input.games.map(withSettledLead) }),
			),
			names,
			teams,
		);
		return input.games.some((g) => g.gender === "female")
			? inWomensLeague(text, names, teams)
			: text;
	} finally {
		endRecapBatch();
	}
};

const buildDayRecap = (input: AutoDayRecapInput): string => {
	const { games, day } = input;

	const rng = rngFromSeed((day + 1) * 40503 + games.length * 97);

	if (games.length === 0) {
		return "";
	}

	// The All-Star showcase gets its own wrap.
	const allStarGame = games.find((g) => g.allStar);
	if (allStarGame && games.length <= 2) {
		return getAutoRecap(allStarGame);
	}

	return writeDayRecap(input, rng);
};

export default getAutoRecap;
