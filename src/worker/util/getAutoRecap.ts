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
	aFigure,
	aNum,
	fixArticles,
	aNumWord,
	aOrdinal,
	beginRecapBatch,
	cap,
	claimVerb,
	clearPhraseMemory,
	doubleCategories,
	doubleWord,
	endRecapBatch,
	gbText,
	injuryPhrase,
	isInBatch,
	lineVerb,
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
	scoredVerb,
	seeded,
	shuffle,
	statPhrase,
	stripHtml,
	takeVerb,
	teamStats,
	theNick,
} from "./recapText.ts";
export { beginRecapBatch, endRecapBatch, pick } from "./recapText.ts";
import {
	benchBeat,
	dayBracketWatch,
	dayColdStreak,
	dayMilestones,
	dayRaceSentence,
	daySeasonHighs,
	dayStandingsMovers,
	dayNextSlate,
	homeRoadBeat,
	milestoneBeat,
	nextGameBeat,
	playerCountBeat,
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
	type DayBeatContext,
} from "./recapBeats.ts";
import {
	finishStory,
	shotName,
	winningShotKind,
	withSettledLead,
} from "./recapFinish.ts";

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
			plain(b)
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
const impact = (p: RecapPlayer): number =>
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

const bestOf = (players: RecapPlayer[]): RecapPlayer | undefined => {
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

const supportingCast = (
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
const storyPlayerOf = (
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
const shootingFlourish = (p: RecapPlayer): string | undefined => {
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
const leadVerb = (
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
			? ["posted", "put up", "totaled", "racked up"]
			: ["scored", "posted", "put up", "racked up"];
	} else {
		pool = hasExtras
			? ["posted", "put up", "totaled", "had"]
			: ["scored", "totaled", "put up", "had"];
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

type Shape = {
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

const analyzeShape = (game: RecapGame): Shape => {
	const [home, away] = game.teams;
	const winner = game.winnerTid === home.tid ? home : away;
	const loser = winner === home ? away : home;
	const margin = winner.pts - loser.pts;
	const ot = game.overtimes ?? 0;

	const wq = Array.isArray(winner.ptsQtrs) ? winner.ptsQtrs : [];
	const lq = Array.isArray(loser.ptsQtrs) ? loser.ptsQtrs : [];
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
const bigQuarterRole = (shape: Shape): "broke" | "turned" | undefined => {
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
const snappedSkid = (shape: Shape): number | undefined => {
	const before = shape.winner.streakBefore;
	return before && !before.won && before.count >= 5 ? before.count : undefined;
};

// The loser was at home, unbeaten there until tonight, with enough home games
// behind it for that to mean something. The home record runs THROUGH this
// game, so the one loss on it is this one.
const firstHomeLoss = (game: RecapGame, shape: Shape): boolean => {
	const hr = shape.loser.homeRecord;
	return (
		!game.playoffs &&
		game.teams[0].tid === shape.loser.tid &&
		!!hr &&
		hr.lost === 1 &&
		hr.won >= 6
	);
};

const isUpset = (game: RecapGame, shape: Shape): boolean => {
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
	return s.favTid === shape.loser.tid;
};

// A game-winning (or game-tying) shot from the clutch-play log, parsed into the
// player, shot type, and timing so it can lead a headline and get its own beat
// in the body. `shot` is display-ready: the log's generic "basket" becomes
// "buzzer-beater" / "game-winner", a real shot type ("three-pointer") is kept.
const clutchShot = (
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
const lateGoAhead = (
	game: RecapGame,
	shape: Shape,
	shot: ReturnType<typeof clutchShot>,
): { text: string; name: string } | undefined => {
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
	return { text: ` on ${poss(name)} go-ahead ${kind} with ${left} left`, name };
};

// "a free throw with 0.5 seconds left" / "a buzzer-beater" - the winning shot
// described, without the shooter. Shared by the standalone beat and the
// merged-into-the-lead form (used when the shooter IS the lead star, so the
// name isn't repeated in back-to-back sentences).
const clutchWhat = (
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

// The clutch shot's own beat in the body: who won it, with what, and when.
// The winning shot, with the hero's full line folded in when it deserves the
// ink - "Viktor Khryapa won it with a three-point play" hid that he ALSO had 23
// points, so the supporting-cast sentence re-introduced the headline hero as if
// he were a bench note. Tells the caller whose line it printed so nothing else
// prints it again. A modest total stays out - "and finished with 6 points" is
// an anticlimax stapled to the biggest moment of the game.
const clutchSentence = (
	shot: NonNullable<ReturnType<typeof clutchShot>>,
	shape?: Shape,
): { text: string; told?: string } => {
	const hero = shape?.winner.players.find((x) => x.name === shot.name);
	if (hero && hero.pts >= 15) {
		return {
			// "on", not "with": clutchWhat carries its own "with 8.3 seconds
			// left", and the two together read "won it with a three-pointer
			// with 8.3 seconds left".
			text: `${shot.name} won it on ${clutchWhat(shot)} and finished with ${statPhrase(hero, 1)}.`,
			told: hero.name,
		};
	}
	return { text: `${shot.name} won it on ${clutchWhat(shot)}.` };
};

const plainScore = (shape: Shape): string =>
	`${shape.winner.pts}-${shape.loser.pts}`;

const scoreTag = (shape: Shape): string => {
	const ot =
		shape.ot > 0 ? ` (${shape.ot === 1 ? "OT" : `${shape.ot}OT`})` : "";
	return `${shape.winner.pts}-${shape.loser.pts}${ot}`;
};

// --- Postseason context --------------------------------------------------------

const roundName = (round: number, numRounds: number): string => {
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
type PostseasonContext = {
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
	// Full sentences for the body.
	sentences: string[];
};

const postseasonContext = (
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

	if (game.playIn) {
		const p = game.playIn;
		if (p.kind === "seed7v8") {
			out.headlineTag = "a play-in game";
			out.sentences.push(
				`With the win in the 7-vs-8 play-in game, ${theNick(
					shape.winner,
				)} claimed the ${
					typeof p.prizeSeed === "number" ? `No. ${p.prizeSeed}` : "higher"
				} seed, while ${theNick(
					shape.loser,
				)} dropped into a win-or-go-home game for the final spot.`,
			);
		} else if (p.kind === "seed9v10") {
			out.headlineTag = "an elimination game";
			out.sentences.push(
				`The 9-vs-10 play-in win kept ${theNick(
					shape.winner,
				)} alive and sent them to the final play-in game; ${poss(
					theNick(shape.loser),
				)} season is over.`,
			);
		} else {
			out.headlineTag = "a win-or-go-home game";
			out.sentences.push(
				`${cap(w)} grabbed the last playoff berth${
					typeof p.prizeSeed === "number"
						? ` as the No. ${p.prizeSeed} seed`
						: ""
				}, ending ${poss(theNick(shape.loser))} season in the final play-in game.`,
			);
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
const comebackSize = (shape: Shape): number =>
	shape.comebackFrom >= 12 || shape.loserPeak >= 15
		? Math.max(shape.comebackFrom, shape.loserPeak)
		: 0;

const verbPool = (game: RecapGame, shape: Shape): string[] => {
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
	if (comebackSize(shape) > 0) {
		return ["rally past", "storm back to beat", "come back to top"];
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

// "carries the Bucks PAST the Suns."
//
// Ten headline shapes end on this connective and every one of them used to
// spell it "past", which put it beyond the reach of the rotation in pick():
// that walks a POOL by index, so two templates rendering the same tail look
// completely distinct to it. Measured over a 375-game corpus the word landed in
// 32.8% of game headlines - eleven of the fifteen on one night - while the
// template shapes themselves repeated only 4.3% of the time. The variety was
// real and it was all above the tail.
//
// Pooling the connective itself is what lets the batch memory spread it, and
// because the pool is shared it spreads across EVERY shape that needs one
// rather than each shape rotating on its own. Only verbs that take both read
// right, so "ride his 30 past the Suns" keeps its own word.
const winOver = (rng: () => number): string =>
	pick(rng, ["past", "over"], "headline:connective");

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

const pastTense = (verb: string): string => {
	if (IRREGULAR[verb]) {
		return IRREGULAR[verb]!;
	}
	if (verb.endsWith("e")) {
		return `${verb}d`;
	}
	return `${verb}ed`;
};

// The star's line as a headline noun phrase, plus whether it's grammatically
// plural - "Randolph's 24 points and 11 rebounds LEAD the Cavaliers", not
// "leads". A bare numeral ("Randolph's 24 leads...") is idiomatically singular
// in wire copy, so only the spelled-out categories count as plural.
const starHeadline = (p: RecapPlayer): { text: string; plural: boolean } => {
	const dd = doubleWord(doubleCategories(p).length);
	if (dd === "triple-double" || dd === "quadruple-double") {
		return { text: dd, plural: false };
	}
	if (p.pts >= 38) {
		return { text: `${p.pts}`, plural: false };
	}
	if (dd === "double-double" && p.pts < 25) {
		const cats = doubleCategories(p);
		const other = cats.find((c) => c !== "points");
		const otherVal =
			other === "rebounds"
				? p.reb
				: other === "assists"
					? p.ast
					: other === "steals"
						? p.stl
						: p.blk;
		return {
			text: `${p.pts} points and ${otherVal} ${other}`,
			plural: true,
		};
	}
	// A big-scoring double-double reads bigger than the bare number ("28-point
	// double-double" over "28").
	if (dd === "double-double") {
		return { text: `${p.pts}-point double-double`, plural: false };
	}
	return { text: `${p.pts}`, plural: false };
};

// A headline, plus whether it already told the star's story. When it did, the
// body opens on the RESULT instead of restating the same sentence with synonyms
// swapped in - the thing that made a page of recaps read as
// "X goes for 26 as the Celtics beat the Grizzlies / X scored 26 as the Celtics
// topped the Grizzlies 115-105."
type Headline = {
	text: string;
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
	// The headline IS the series result ("Pistons close out the Bulls in six"),
	// so the body's postseason opener must not say it again.
	spentClinch?: boolean;
	// The headline carried the series score ("take a 2-1 lead"), so the
	// body's series-state sentence would be the same numbers again.
	spentState?: boolean;
};

const h = (
	text: string,
	spentStar: boolean,
	spentLoserStar = false,
	spentLine = false,
): Headline => ({ text, spentStar, spentLoserStar, spentLine });

// A HEADLINE FITS ON A LINE. A tenth of them ran past 80 characters - "Evan
// Carter has 35 points and 10 assists as the #8 Hornets push top-seeded
// Warriors to the brink, 3-1" - nearly always because a man's whole line
// sat in front of the result. Past 80, his line comes down to his points,
// the way a wire headline has it ("Carter scores 35 as..."); the lede
// under it gives the rest, since it prints whatever the headline did not.
const HEADLINE_MAX = 80;
// The number kept is the one the night was about: "Anthony Foster scores 10
// as the Rockets take down the Jazz" was a 10-point, 16-rebound night, and
// "Franz Foster scores 17" a 7-steal one - five steals or blocks is a
// signature line, whatever the points.
const TRIM_VERB: Record<string, string> = {
	rebounds: "grabs",
	assists: "hands out",
	steals: "has",
	blocks: "has",
};
export const trimHeadline = (text: string): string => {
	if (text.length <= HEADLINE_MAX) {
		return text;
	}
	return text
		.replace(
			/ (?:has|turns in|goes for|puts up|posts) (\d+) points(?:, \d+ \w+,)? and (\d+) (rebounds|assists|steals|blocks)(?= as | in |, and )/,
			(_, pts: string, n: string, what: string) =>
				Number(n) > Number(pts) ||
				(Number(n) >= 5 && (what === "steals" || what === "blocks"))
					? ` ${TRIM_VERB[what]} ${n} ${what}`
					: ` scores ${pts}`,
		)
		.replace(
			/('s?) (\d+) points and (\d+) (rebounds|assists|steals|blocks) (?=not enough|power|lead|carry|lift|sink)/,
			(_, s: string, pts: string, n: string, what: string) =>
				Number(n) > Number(pts) ||
				(Number(n) >= 5 && (what === "steals" || what === "blocks"))
					? `${s} ${n} ${what} `
					: `${s} ${pts} `,
		);
};

const buildHeadline = (
	game: RecapGame,
	shape: Shape,
	star: RecapPlayer,
	post: PostseasonContext,
	rng: () => number,
): Headline => {
	const built = buildHeadlineText(game, shape, star, post, rng);
	const trimmed = trimHeadline(built.text);
	// Cut down to his points, a man a teammate outscored reads as the wrong
	// man: "Xavier Ingram's 24 not enough" over a teammate's 28 was a 24-
	// and-8 night. A headline a little long beats one that misleads.
	const misleads =
		trimmed !== built.text &&
		game.teams.some((t) =>
			t.players.some((p) => {
				const m = new RegExp(
					String.raw`${p.name.replaceAll(/[$()*+.?[\\\]^{|}]/g, String.raw`\$&`)}(?:'s?| scores) (\d+)\b(?! (?:points|rebounds|assists|steals|blocks))`,
				).exec(trimmed);
				return (
					!!m && Number(m[1]) === p.pts && t.players.some((q) => q.pts > p.pts)
				);
			}),
		);
	const text = misleads ? built.text : trimmed;
	return {
		...built,
		text,
		// Cut to length, it no longer carries his whole line, and the lede
		// gives it: "Franz Foster scores 17" over a 7-steal night left the
		// steals out of the piece.
		spentLine: built.spentLine && text === built.text,
		spentScore: text.includes(scoreTag(shape)),
	};
};

const buildHeadlineText = (
	game: RecapGame,
	shape: Shape,
	star: RecapPlayer,
	post: PostseasonContext,
	rng: () => number,
): Headline => {
	const winnerN = nick(shape.winner);
	const loserN = nick(shape.loser);
	const shot = clutchShot(game);
	const tag =
		post.headlineTail ?? (post.headlineTag ? ` in ${post.headlineTag}` : "");

	// A CLINCH IS THE HEADLINE. Hanging ", advancing to the Conference
	// Semifinals" off whatever headline the game would otherwise have got gave
	// "Pistons get double-doubles from Franz Ellis and Keegan Ellis in a
	// 118-110 win, advancing to the Conference Semifinals" - twenty words to
	// bury the only fact that matters. The series result leads; the star, the
	// shot, the comeback and the seeds decorate it.
	const clinch = post.clinch;
	if (clinch) {
		const games = numWord(clinch.games);
		// "Game 7" stands alone; a best-of-5's "decisive Game 5" wants its
		// article ("take the decisive Game 5 from the Pistons").
		const decider = clinch.deciderTag
			.replace(/^a decisive/, "the decisive")
			.replace(/^a /, "");
		const ddCount = doubleCategories(star).length;
		const bigStar = star.pts >= 30 || ddCount >= 3;
		const starLine =
			ddCount >= 3 ? doubleWord(ddCount)! : starHeadline(star).text;
		// "Jennings scores 39" is how a 39-point Game 7 is headlined anywhere
		// real; the betting line is not, and "8.5-point underdogs, the Nets
		// win Game 7" read like a tout sheet. The seeds carry the upset.
		const seedTag =
			clinch.seedUpset &&
			typeof clinch.wSeed === "number" &&
			typeof clinch.lSeed === "number"
				? {
						w: `#${clinch.wSeed} ${winnerN}`,
						l:
							clinch.lSeed === 1
								? `top-seeded ${loserN}`
								: `#${clinch.lSeed} ${loserN}`,
					}
				: undefined;
		const starVerbLine =
			ddCount >= 3
				? `${star.name} posts a ${starLine}`
				: statPhrase(star, 1) === plural(star.pts, "point")
					? `${star.name} scores ${star.pts}`
					: `${star.name} has ${statPhrase(star, 1)}`;
		const options: string[] = [];
		const starOptions: string[] = [];
		if (clinch.title) {
			if (shot && !shot.tying) {
				options.push(
					`${poss(shot.name)} ${shot.shot} wins the ${winnerN} the title`,
					`${shot.name} beats the ${loserN} at the wire, and the ${winnerN} are champions`,
				);
			} else if (clinch.sweep) {
				options.push(
					`${winnerN} sweep the ${loserN} for the title`,
					`${winnerN} finish the sweep and are champions`,
				);
			} else if (clinch.decider) {
				options.push(
					`${winnerN} win the title in ${decider}`,
					`${winnerN} beat the ${loserN} in ${decider} to win the title`,
				);
			} else {
				options.push(
					`${winnerN} finish off the ${loserN} in ${games} for the title`,
					`${winnerN} are champions, closing out the ${loserN} in ${games}`,
				);
			}
			if (bigStar && !(shot && !shot.tying)) {
				starOptions.push(
					`${poss(star.name)} ${starLine} carries the ${winnerN} to the title`,
					clinch.decider
						? `${starVerbLine} as the ${winnerN} win ${decider} and the title`
						: `${starVerbLine} as the ${winnerN} clinch the title`,
				);
			}
		} else {
			const next = clinch.nextRound;
			if (shot && !shot.tying) {
				options.push(
					`${poss(shot.name)} ${shot.shot} sends the ${winnerN} to ${next}`,
					`${shot.name} beats the ${loserN} at the wire to close out the series`,
				);
			} else if (clinch.sweep) {
				options.push(
					`${winnerN} sweep the ${loserN}`,
					`${winnerN} finish off the sweep of the ${loserN}, ${scoreTag(shape)}`,
					`${winnerN} complete the sweep and move on to ${next}`,
				);
			} else if (clinch.decider) {
				options.push(
					`${winnerN} take ${decider} from the ${loserN}, ${scoreTag(shape)}`,
					`${winnerN} beat the ${loserN} in ${decider}, advance to ${next}`,
					...(seedTag
						? [
								`${seedTag.w} oust ${seedTag.l} in ${decider}`,
								`${seedTag.w} eliminate ${seedTag.l} in ${decider}`,
							]
						: shape.margin >= 15
							? // "Survive" a 21-point Game 7 is the wrong word.
								[
									`${winnerN} run away with ${decider}, ${scoreTag(shape)}`,
									`${winnerN} make ${decider} a rout, ${scoreTag(shape)}`,
								]
							: shape.margin <= 6
								? [`${winnerN} survive ${decider} against the ${loserN}`]
								: [`${winnerN} oust the ${loserN} in ${decider}`]),
				);
				if (bigStar) {
					starOptions.push(
						`${starVerbLine} as the ${winnerN} oust the ${loserN} in ${decider}`,
						`${starVerbLine}, and the ${winnerN} take ${decider} from the ${loserN}`,
					);
				}
			} else if (shape.margin <= 3 && shape.ot === 0) {
				// "Put away the Lakers in six" over a 115-114 finish.
				options.push(
					`${winnerN} close out the ${loserN} with ${aFigure(scoreTag(shape))} escape`,
					`${winnerN} edge the ${loserN} ${scoreTag(shape)} to close out the series in ${games}`,
					...(heldOn(game)
						? [
								`${winnerN} hold off the ${loserN} ${scoreTag(shape)} and move on to ${next}`,
							]
						: []),
				);
			} else if (shape.ot > 0) {
				options.push(
					`${winnerN} close out the ${loserN} in ${otPhrase(shape.ot)}, ${scoreTag(shape)}`,
					`${winnerN} need ${shape.ot === 1 ? "an extra period" : `${numWord(shape.ot)} extra periods`} to finish off the ${loserN} in ${games}`,
				);
			} else if (comebackSize(shape) > 0) {
				options.push(
					`${winnerN} rally from ${comebackSize(shape)} down to close out the ${loserN}`,
					`Down ${comebackSize(shape)}, the ${winnerN} come back to finish off the ${loserN} in ${games}`,
				);
			} else {
				options.push(
					`${winnerN} close out the ${loserN} in ${games}`,
					`${winnerN} finish off the ${loserN}, ${scoreTag(shape)}, and move on to ${next}`,
					`${winnerN} put away the ${loserN} in ${games} games`,
				);
			}
			if (seedTag && !clinch.decider) {
				options.push(`${seedTag.w} oust ${seedTag.l} in ${games}`);
			}
			if (bigStar && !(shot && !shot.tying) && !clinch.decider) {
				starOptions.push(
					`${poss(star.name)} ${starLine} sends the ${winnerN} through to ${next}`,
					clinch.sweep
						? `${starVerbLine} as the ${winnerN} sweep the ${loserN}`
						: `${starVerbLine} as the ${winnerN} close out the ${loserN} in ${games}`,
				);
			}
		}
		// A 35-point night in a clincher IS the headline; a 30-point one shares
		// the rotation with the series result.
		const text = pick(
			rng,
			star.pts >= 35 && starOptions.length > 0
				? starOptions
				: [...options, ...starOptions],
			"headline:clinch",
		);
		const headline = h(
			text,
			text.includes(star.name),
			false,
			text.includes(statPhrase(star, 1)),
		);
		headline.spentClinch = true;
		return headline;
	}

	// A PLAYOFF GAME IS HEADLINED BY THE SERIES. "Cavaliers get past the
	// Pacers 124-118" over a Game 3 that put a #8 seed up 2-1 on the #1 is
	// not how anyone writes it: the series state is the news, the star and
	// the seeds carry it, and the clutch shot still leads when there is one.
	const state = post.state;
	if (state && !(shot && !shot.tying)) {
		const L = `the ${loserN}`;
		const seeds =
			state.seedUpset &&
			typeof state.wSeed === "number" &&
			typeof state.lSeed === "number"
				? {
						w: `#${state.wSeed} ${winnerN}`,
						l:
							state.lSeed === 1
								? `top-seeded ${loserN}`
								: `#${state.lSeed} ${loserN}`,
					}
				: undefined;
		const W = seeds ? seeds.w : winnerN;
		const Lx = seeds ? seeds.l : L;
		const score = `${state.wAfter}-${state.lBefore}`;
		// The state as a verb phrase the winner is the subject of - with the
		// opponent in it, and without, for the "beat the Kings 93-79 to ..."
		// shape that has already named them.
		let stateVerb: string;
		let stateShort: string;
		if (state.forcesDecider) {
			stateVerb = `force ${state.deciderTag} with ${Lx}`;
			stateShort = `force ${state.deciderTag}`;
		} else if (state.wAfter === 1 && state.lBefore === 0) {
			stateVerb = `take Game 1 from ${Lx}`;
			stateShort = `take Game 1`;
		} else if (state.wAfter === state.lBefore) {
			stateVerb = `even the series with ${Lx} at ${score}`;
			stateShort = `even the series at ${score}`;
		} else if (state.wAfter > state.lBefore) {
			const brink = state.need !== undefined && state.wAfter === state.need - 1;
			stateVerb = brink
				? `push ${Lx} to the brink, ${score}`
				: `take a ${score} series lead over ${Lx}`;
			stateShort = brink ? `go up ${score}` : `lead the series ${score}`;
		} else if (state.facingElimination) {
			stateVerb = `stay alive against ${Lx}, ${state.lBefore}-${state.wAfter}`;
			stateShort = `stay alive in the series`;
		} else {
			stateVerb = `cut ${poss(Lx)} series lead to ${state.lBefore}-${state.wAfter}`;
			stateShort = `cut the series deficit to ${state.lBefore}-${state.wAfter}`;
		}
		const ddCount = doubleCategories(star).length;
		const starLine = statPhrase(star, 1);
		const starVerbLine =
			ddCount >= 3
				? `${star.name} posts a ${doubleWord(ddCount)!}`
				: starLine === plural(star.pts, "point")
					? `${star.name} scores ${star.pts}`
					: `${star.name} has ${starLine}`;
		// A signature line - five blocks, five steals - headlines like a big
		// scoring night does.
		const bigStar =
			star.pts >= 28 || ddCount >= 3 || star.blk >= 5 || star.stl >= 5;
		// "Take down the Cavaliers to take Game 1" is the verb twice.
		let verb = pick(rng, verbPool(game, shape), "playoffVerb");
		if (verb.startsWith("take") && stateShort.startsWith("take")) {
			verb = "beat";
		}
		// The game's own character outranks the plain forms: a 25-point
		// comeback or a 48-point rout is the headline, whatever the series.
		const special = [
			...(shape.margin >= 20
				? [
						`${W} rout ${Lx} ${scoreTag(shape)} to ${stateShort}`,
						`${W} blow out ${Lx} by ${shape.margin} and ${stateShort}`,
					]
				: []),
			...(comebackSize(shape) > 0
				? [
						`${W} rally from ${comebackSize(shape)} down to ${stateVerb}`,
						`Down ${comebackSize(shape)}, the ${W} come back to ${stateShort}`,
					]
				: []),
			...(shape.ot > 0
				? [`${W} outlast ${Lx} in ${otPhrase(shape.ot)} to ${stateShort}`]
				: []),
		];
		const options =
			special.length > 0 && (comebackSize(shape) >= 15 || shape.margin >= 25)
				? special
				: [
						`${W} ${stateVerb}`,
						`${W} ${verb} ${L} ${scoreTag(shape)} to ${stateShort}`,
						...special,
					];
		const starOptions = bigStar
			? [
					`${starVerbLine} as the ${W} ${stateVerb}`,
					`${starVerbLine}, and the ${W} ${stateVerb}`,
				]
			: [];
		const text = pick(
			rng,
			star.pts >= 35 && starOptions.length > 0
				? starOptions
				: [...options, ...starOptions],
			"headline:series-state",
		);
		const headline = h(
			text,
			text.includes(star.name),
			false,
			text.includes(starLine),
		);
		headline.spentState = true;
		return headline;
	}

	if (shot && !shot.tying) {
		// A regular-season skid is not the story of a playoff game.
		const skid = game.playoffs ? undefined : snappedSkid(shape);
		// A night the shot did not outshine: "Yuta Dunn's layup sinks the
		// Bulls" over a teammate's 45 points, a season high, in a game that
		// went to two overtimes. Both go in, the big night first.
		const bigNight =
			skid === undefined &&
			star.name !== shot.name &&
			shape.winner.players.includes(star) &&
			(star.pts >= 40 ||
				doubleCategories(star).length >= 3 ||
				(star.pts >= 20 && star.reb >= 20));
		if (bigNight) {
			const did =
				doubleCategories(star).length >= 3
					? `posts a ${doubleWord(doubleCategories(star).length)}`
					: star.pts >= 40
						? `scores ${star.pts}`
						: `has ${star.pts} and ${star.reb}`;
			return h(
				`${star.name} ${did}, and ${poss(shot.name)} ${shot.shot} sinks the ${loserN}${tag}`,
				true,
			);
		}
		return h(
			skid !== undefined
				? // The shot ended a skid: both facts, and the skid is the bigger.
					pick(
						rng,
						[
							`${poss(shot.name)} ${shot.shot} ends ${aNum(skid)}-game skid for the ${winnerN}`,
							`${winnerN} end ${aNum(skid)}-game losing streak on ${poss(shot.name)} ${shot.shot}`,
						],
						"headline:clutch-skid",
					)
				: pick(
						rng,
						[
							`${poss(shot.name)} ${shot.shot} sinks the ${loserN}${tag}`,
							`${poss(shot.name)} ${shot.shot} lifts the ${winnerN} ${winOver(rng)} the ${loserN}${tag}`,
							`${shot.name} beats the ${loserN} with a ${shot.shot}${tag}`,
						],
						"headline:clutch-shot",
					),
			false,
		);
	}

	const verb = pick(rng, verbPool(game, shape));
	const ddCount = doubleCategories(star).length;

	if (ddCount >= 3) {
		const word = doubleWord(ddCount)!;
		return h(
			pick(
				rng,
				[
					`${poss(star.name)} ${word} carries the ${winnerN} ${winOver(rng)} the ${loserN}${tag}`,
					`${star.name} posts a ${word} as the ${winnerN} ${verb} the ${loserN}${tag}`,
					`${star.name} fills the sheet with a ${word} in ${poss(`the ${winnerN}`)} win${tag}`,
					`A ${word} from ${star.name} sinks the ${loserN}${tag}`,
				],
				"headline:multi-double",
			),
			true,
		);
	}

	// THE UPSET, WHICH HAD ONE SHAPE. Twelve per cent of every headline the
	// engine wrote came out of this branch, and all of it was "X <verb> the Y,
	// 103-98" with the verb rotating - which is exactly the tell the rest of
	// this file exists to remove. The number the books had, the record the
	// favorite carried in, and the run either side was on are all in hand, and
	// each of them says something the bare result does not.
	// A comeback of 15 or more outranks the upset: "Magic knock off the
	// Knicks 118-113" of a game the 7-point underdogs trailed by 20. The line
	// note still says who was favored.
	if (isUpset(game, shape) && comebackSize(shape) < 15) {
		// No betting line in a headline. Real desks write the upset as what the
		// favorite lost - a streak, a home record, a rare defeat - or as the
		// man who beat them; the number the books had is body color.
		const options = [
			`${winnerN} ${verb} the ${loserN}, ${scoreTag(shape)}${tag}`,
			`${winnerN} ${verb} the ${loserN} ${scoreTag(shape)}${tag}`,
			`${winnerN} pull the upset over the ${loserN} ${scoreTag(shape)}${tag}`,
			// Most real upsets are not even labeled: "Jazz beat Bucks 98-91",
			// "Mathis scores 27 as Jazz top Bucks". Not one by 30, though.
			...(shape.margin >= 20
				? []
				: [`${winnerN} beat the ${loserN} ${scoreTag(shape)}${tag}`]),
		];
		if (star.pts >= 20) {
			options.push(
				`${
					statPhrase(star, 1) === plural(star.pts, "point")
						? `${star.name} scores ${star.pts}`
						: `${star.name} has ${statPhrase(star, 1)}`
				} as the ${winnerN} ${verb} the ${loserN}${tag}`,
			);
		}
		const loserRun = shape.loser.streakBefore;
		if (loserRun?.won && loserRun.count >= 5) {
			options.push(
				`${winnerN} snap ${poss(`the ${loserN}`)} ${loserRun.count}-game winning streak${tag}`,
				`${winnerN} end ${poss(`the ${loserN}`)} ${loserRun.count}-game run${tag}`,
			);
		}
		if (firstHomeLoss(game, shape)) {
			options.push(
				`${winnerN} hand the ${loserN} their first home loss of the season${tag}`,
			);
		}
		const loserRec = shape.loser.record;
		// The record is the regular season's, so in the playoffs "a rare loss"
		// was being written over a team that had lost the night before.
		if (
			loserRec &&
			!game.playoffs &&
			loserRec.won >= loserRec.lost * 2 &&
			loserRec.won >= 12
		) {
			options.push(
				`${winnerN} hand the ${loserN} a rare loss, ${scoreTag(shape)}${tag}`,
			);
		}
		const loserStreak = shape.loser.streak;
		if (loserStreak && !loserStreak.won && loserStreak.count >= 4) {
			// The loser's streak ENDS with this game, so it counts tonight.
			options.push(
				`${winnerN} send the ${loserN} to ${aOrdinal(loserStreak.count)} straight defeat${tag}`,
			);
		}
		const winnerStreak = shape.winner.streak;
		if (winnerStreak?.won && winnerStreak.count >= 4) {
			options.push(
				`${winnerN} make it ${winnerStreak.count} straight with an upset of the ${loserN}${tag}`,
			);
		}
		const skid = snappedSkid(shape);
		if (skid !== undefined) {
			options.push(
				`${winnerN} end ${aNum(skid)}-game skid with an upset of the ${loserN}${tag}`,
			);
		}
		if (star.pts >= 24) {
			options.push(
				// A stat line is a plural subject: "29 points and 9 rebounds
				// stun", never "stuns".
				`${poss(star.name)} ${statPhrase(star, 1)} stun the ${loserN} as the ${winnerN} pull the upset${tag}`,
				`${star.name} leads the ${winnerN} to an upset of the ${loserN}${tag}`,
			);
		}
		const text = pick(rng, options, "headline:upset");
		// Named in the headline is told, whatever his total: "Walker Green has
		// 22 points and 10 assists as the Grizzlies stun the Pacers" over
		// "Walker Green totaled 22 points and 10 assists as the Grizzlies..."
		return h(
			text,
			text.includes(star.name),
			false,
			text.includes(statPhrase(star, 1)),
		);
	}

	const cb = comebackSize(shape);
	if (cb > 0) {
		// Either way the comeback is now SAID. The body must not say it a second
		// and third time ("rallies" / "stormed back to beat" / "trailed by 14 and
		// stormed back" all landed in one recap).
		return h(
			pick(
				rng,
				[
					// A bare verb after "to": the comeback pool's own verbs made
					// this "erase a 19-point hole to come back to top the Wolves".
					`${winnerN} erase ${aNum(cb)}-point hole to ${pick(
						rng,
						["beat", "top", "take down", "knock off"],
						"comebackVerb",
					)} the ${loserN}${tag}`,
					`${star.name} rallies the ${winnerN} past the ${loserN}${tag}`,
					`Down ${cb}, the ${winnerN} come back to beat the ${loserN}${tag}`,
					...(shape.margin >= 15
						? [
								`${winnerN} fall behind by ${cb}, then run away from the ${loserN} ${scoreTag(shape)}${tag}`,
								`${winnerN} overcome ${aNum(cb)}-point deficit and rout the ${loserN}${tag}`,
							]
						: []),
				],
				"headline:comeback",
			),
			true,
		);
	}

	// A heavy favorite that barely escaped: the scare IS the story, not the
	// favorite's underwhelming stat line ("Paul Pierce's 16 leads..." on a
	// -11.5 favorite winning by 3 buried the real angle).
	if (
		game.spread &&
		game.spread.favTid === shape.winner.tid &&
		game.spread.points >= 8 &&
		shape.margin <= 5
	) {
		return h(
			pick(
				rng,
				[
					`${winnerN} survive a scare from the ${loserN}`,
					`${winnerN} escape the ${loserN} ${scoreTag(shape)}`,
					...(heldOn(game)
						? [`${winnerN} hold off the ${loserN} ${scoreTag(shape)}`]
						: []),
				],
				"headline:scare",
			),
			false,
		);
	}

	// A 20-20 game headlines itself - "scores 24" would bury the 22 rebounds.
	if (star.pts >= 20 && star.reb >= 20) {
		return h(
			pick(
				rng,
				[
					`${star.name} dominates with ${star.pts} points and ${star.reb} rebounds as the ${winnerN} ${verb} the ${loserN}${tag}`,
					`${poss(star.name)} ${star.pts}-point, ${star.reb}-rebound night carries the ${winnerN} ${winOver(rng)} the ${loserN}${tag}`,
					`${star.name} pulls down ${star.reb} to go with ${star.pts} in ${poss(`the ${winnerN}`)} win${tag}`,
					`${star.pts} points and ${star.reb} rebounds from ${star.name} bury the ${loserN}${tag}`,
				],
				"headline:20-20",
			),
			true,
		);
	}

	// A scoring duel when both stars go off.
	const loserStar = bestOf(shape.loser.players);
	if (
		loserStar &&
		star.pts >= 32 &&
		loserStar.pts >= 30 &&
		ddCount < 3 &&
		shape.margin <= 12
	) {
		return h(
			pick(
				rng,
				[
					`${star.name} outduels ${loserStar.name} as the ${winnerN} ${verb} the ${loserN}${tag}`,
					`${poss(star.name)} ${star.pts} edges ${poss(loserStar.name)} ${loserStar.pts} in ${poss(
						`the ${winnerN}`,
					)} win${tag}`,
				],
				"headline:duel",
			),
			true,
		);
	}

	if (star.pts >= 40) {
		return h(
			pick(
				rng,
				[
					`${star.name} drops ${star.pts} as the ${winnerN} ${verb} the ${loserN}${tag}`,
					`${poss(star.name)} ${star.pts} sinks the ${loserN}${tag}`,
					`${star.name} pours in ${star.pts} in ${poss(`the ${winnerN}`)} win${tag}`,
				],
				"headline:40-point",
			),
			true,
		);
	}

	// Two double-doubles from the winner, when no single scorer dominates.
	const winnerDoubles = shape.winner.players
		.filter((p) => doubleCategories(p).length >= 2 && p.pts >= 12)
		.sort((a, b) => impact(b) - impact(a));
	// Only when the STAR is one of the two. The body is built around the star and
	// his top supporting man, so a pair chosen independently of him could - and
	// did - put a name in the headline that the recap never mentions again.
	if (
		winnerDoubles.length >= 2 &&
		star.pts < 30 &&
		winnerDoubles.includes(star)
	) {
		const partner = winnerDoubles.find((p) => p !== star)!;
		return h(
			pick(
				rng,
				[
					`${star.name} and ${partner.name} lead the ${winnerN} ${winOver(rng)} the ${loserN}${tag}`,
					`Double-doubles from ${star.name} and ${partner.name} carry the ${winnerN} ${winOver(rng)} the ${loserN}${tag}`,
					`${star.name} and ${partner.name} post double-doubles as the ${winnerN} ${verb} the ${loserN}${tag}`,
					`${winnerN} get double-doubles from ${star.name} and ${partner.name} in ${aFigure(scoreTag(shape))} win${tag}`,
					// "Too much for" is a game that was not close.
					...(shape.margin >= 8
						? [
								`${star.name} and ${partner.name} are too much for the ${loserN}${tag}`,
							]
						: []),
				],
				"headline:two-doubles",
			),
			true,
		);
	}

	// A defensive showcase.
	if (star.blk >= 5) {
		return h(
			pick(
				rng,
				[
					`${poss(star.name)} ${star.blk} blocks anchor the ${winnerN} ${winOver(rng)} the ${loserN}${tag}`,
					`${star.name} swats ${star.blk} as the ${winnerN} ${verb} the ${loserN}${tag}`,
					`${star.name} protects the rim with ${plural(star.blk, "block")} in ${poss(`the ${winnerN}`)} win${tag}`,
				],
				"headline:blocks",
			),
			true,
		);
	}
	if (star.stl >= 5 && star.pts >= 15) {
		return h(
			pick(
				rng,
				[
					`${star.name} takes over with ${plural(star.pts, "point")} and ${plural(
						star.stl,
						"steal",
					)} as the ${winnerN} ${verb} the ${loserN}${tag}`,
					// Not "picks the 76ers apart": that is a passer's line, and
					// this one is points and steals.
					`${star.name} scores ${star.pts} and picks ${star.stl} pockets as the ${winnerN} ${verb} the ${loserN}${tag}`,
					`${poss(star.name)} ${plural(star.stl, "steal")} turn the game as the ${winnerN} ${verb} the ${loserN}${tag}`,
				],
				"headline:steals",
			),
			true,
		);
	}

	const sh = starHeadline(star);
	const shVerb = star.pts >= 25 ? "power" : "lead";
	// When the best player on the floor lost, saying so is more honest than
	// crowning the winner's 15-point leading scorer. (A 27-and-10 night on the
	// losing side was being headlined as "McGrady leads the Nuggets".)
	if (
		loserStar &&
		storyScore(loserStar) > storyScore(star) * 1.2 &&
		loserStar.pts >= 22 &&
		star.pts < 25 &&
		loserLineCarries(loserStar, shape.margin)
	) {
		return h(
			pick(
				rng,
				[
					`${poss(loserStar.name)} ${statPhrase(loserStar, 1)} not enough as the ${winnerN} ${verb} the ${loserN}${tag}`,
					`${winnerN} ${verb} the ${loserN} despite ${poss(loserStar.name)} ${statPhrase(loserStar, 1)}${tag}`,
				],
				"headline:loser-star",
			),
			false,
			true,
		);
	}

	// Deliberately more shapes than a slate has games of any one kind. The
	// rotation in `pick` walks a pool before repeating, so a pool of four ran
	// dry on a twelve-game night and the page went back to sounding like one
	// sentence with the names swapped.
	// Six or seven in double figures behind a quiet leader is a team night,
	// and the headline says so.
	const balanced = teamStats(shape.winner).dblFig >= 6 && star.pts < 18;
	const starTemplates = [
		`${poss(star.name)} ${sh.text} ${shVerb}${
			sh.plural ? "" : "s"
		} the ${winnerN} ${winOver(rng)} the ${loserN}${tag}`,
		// "goes for" + the full stat phrase, so a 24-and-12 night isn't flattened
		// into "scores 24".
		`${star.name} goes for ${statPhrase(star, 1)} as the ${winnerN} ${verb} the ${loserN}${tag}`,
		`${winnerN} ${verb} the ${loserN} behind ${poss(star.name)} ${sh.text}${tag}`,
		`${star.name} leads the ${winnerN} over the ${loserN}${tag}`,
		`${star.name} has ${statPhrase(star, 1)} in ${poss(`the ${winnerN}`)} ${scoreTag(shape)} win${tag}`,
		`${winnerN} lean on ${star.name} to ${verb} the ${loserN}${tag}`,
		`${winnerN} ride ${poss(star.name)} ${sh.text} past the ${loserN}${tag}`,
		`${star.name} turns in ${statPhrase(star, 1)} as the ${winnerN} ${verb} the ${loserN}${tag}`,
	];
	const resultTemplates = [
		`${winnerN} ${verb} the ${loserN}, ${scoreTag(shape)}${tag}`,
		`${winnerN} ${verb} the ${loserN} ${scoreTag(shape)}${tag}`,
		// Only when nothing is hanging off the end: post.headlineTail is a
		// participle that has to attach to the winner, and these are the ones
		// that make the loser the subject.
		...(post.headlineTail
			? []
			: [
					`${loserN} fall to the ${winnerN} ${scoreTag(shape)}${tag}`,
					// Over a 110-107 game won on a jumper with 19.9 seconds left,
					// "cannot keep up" told a different game.
					...(shape.margin >= 12
						? [
								`${loserN} cannot keep up with the ${winnerN}, ${scoreTag(shape)}${tag}`,
							]
						: []),
				]),
		...(shape.margin >= 8 && shape.margin < 25
			? [`${winnerN} take care of the ${loserN} ${scoreTag(shape)}${tag}`]
			: []),
		// Not "see off": that is the football page's English.
		// Not over a rout either: "Pistons turn back the Nets 116-73".
		...(shape.margin >= 4 && shape.margin < 20
			? [`${winnerN} turn back the ${loserN} ${scoreTag(shape)}${tag}`]
			: []),
		shape.margin >= 15
			? `${winnerN} pull away from the ${loserN} for ${aNum(shape.margin)}-point win${tag}`
			: `${winnerN} get past the ${loserN} ${scoreTag(shape)}${tag}`,
		shape.margin >= 15
			? `${winnerN} have too much for the ${loserN}, ${scoreTag(shape)}${tag}`
			: `${winnerN} get the better of the ${loserN} ${scoreTag(shape)}${tag}`,
		...(firstHomeLoss(game, shape)
			? [
					`${winnerN} hand the ${loserN} their first home loss of the season${tag}`,
				]
			: []),
		...(balanced
			? [
					`${winnerN} get ${numWord(teamStats(shape.winner).dblFig)} men into double figures in ${aFigure(scoreTag(shape))} win over the ${loserN}${tag}`,
					// No pronoun in a headline: not "Six Thunder score in
					// double figures as they roll past the Trail Blazers".
					`${winnerN} put ${numWord(teamStats(shape.winner).dblFig)} in double figures and ${verb} the ${loserN}${tag}`,
				]
			: []),
		// A team on a real run is a story the bare score does not carry.
		...(shape.winner.streak?.won && shape.winner.streak.count >= 5
			? [
					`${winnerN} make it ${shape.winner.streak.count} straight against the ${loserN}${tag}`,
				]
			: []),
		...(shape.winner.seasonHighs?.pts &&
		(shape.winner.seasonHighs.priorGames ?? 0) >= 15
			? [
					`${winnerN} post a season high in ${aFigure(scoreTag(shape))} win over the ${loserN}${tag}`,
				]
			: []),
		// A long skid ending is the story of the night for that team, however
		// ordinary the win that ended it.
		...(snappedSkid(shape) !== undefined
			? [
					`${winnerN} snap ${aNum(snappedSkid(shape)!)}-game losing streak against the ${loserN}${tag}`,
					`${winnerN} end ${aNum(snappedSkid(shape)!)}-game skid with a win over the ${loserN}${tag}`,
				]
			: []),
	];
	// A big margin with a modest star line is a result story, always - a
	// "Michael Doleac's 17 leads..." headline on a 22-point blowout misses it.
	// And a sub-15-point "star" never headlines at all ("...behind Chris
	// Webber's 13" on a 3-point game buried the actual game).
	// A modest line does not get to headline a game. "Kings slip past the Jazz
	// behind Tim Young's 15" put a quiet 15 points in lights on a night the
	// losing side had a 22, and told the reader nothing about the game.
	//
	// Measured on the whole line, not the points column: a 15-and-14 night is a
	// perfectly good headline and a 19-point, three-rebound one is not.
	const starHasDouble = ddCount >= 2;
	const outscoredByLoser =
		!starHasDouble && loserStar !== undefined && loserStar.pts >= star.pts + 6;
	// Same on his own side: "Pelicans take down the Pacers behind Miles
	// Ingram's 19" on a night a teammate had 28 is the wrong man in lights.
	const outscoredByTeammate =
		!starHasDouble &&
		shape.winner.players.some((p) => p !== star && p.pts >= star.pts + 6);
	const useResult =
		balanced ||
		(!starHasDouble && star.pts < 17) ||
		(!starHasDouble && star.pts < 20 && shape.margin >= 15) ||
		outscoredByLoser ||
		outscoredByTeammate ||
		(shape.margin >= 18 && rng() < 0.5) ||
		(snappedSkid(shape) !== undefined && rng() < 0.7);
	const text = pick(
		rng,
		useResult ? resultTemplates : starTemplates,
		useResult ? "headline:result" : "headline:star",
	);
	// Whether the headline printed his WHOLE line, however it was phrased.
	// Matching on " goes for " alone missed the "has 27 points in the Raccoons'
	// 99-94 win" and "turns in ..." shapes, so the body opened by saying the
	// same numbers again one sentence later.
	return h(text, !useResult, false, text.includes(statPhrase(star, 1)));
};

// --- Body sentence builders ----------------------------------------------------

// THE CLAUSE THAT MAKES A LEDE A LEDE. A wire lede does not stop at the
// score: "...beat the Raptors 141-118 for their sixth straight win", "...held
// off the Mavericks 125-123 despite 31 points and 12 rebounds from Payton
// Dunn", "...topped the Kings 104-96, snapping a five-game losing streak". The
// engine had every one of those facts and put each in its own sentence three
// paragraphs down. One of them, the biggest, rides on the result sentence.
type LedeTail = {
	text: string;
	// The tail told a streak, so the stakes sentence must not.
	streak?: boolean;
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
	const headlineStreak =
		/straight|in a row|skid|losing streak|winning streak|-game run/.test(
			headline.text,
		);
	if (!game.playoffs && !headlineStreak) {
		const skid = snappedSkid(shape);
		if (skid !== undefined) {
			return {
				text: `, snapping ${aNumWord(skid)}-game losing streak`,
				streak: true,
			};
		}
		const lRun = shape.loser.streakBefore;
		if (lRun?.won && lRun.count >= 5) {
			return {
				text: `, ending ${poss(theNick(shape.loser))} ${numWord(lRun.count)}-game winning streak`,
				streak: true,
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
				streak: true,
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
const clutchVerb = (what: string): string =>
	/three-point play|four-point play/.test(what)
		? "converted"
		: /layup|free throw|hook/.test(what)
			? "made"
			: "hit";

// The lead: the result carried by the winner's best player, with the line he
// brought in when it makes the night pop.
// The result, told first, with the game's own character folded in. Used when the
// headline already spent the star, so the body does new work instead of
// restating him. `covers` names the angle it already told, so the flow sentence
// doesn't say it a second time.
const resultLead = (
	game: RecapGame,
	shape: Shape,
	rng: () => number,
	// The headline already printed the score, so this sentence says who beat
	// whom and leaves the number to it - see Headline.spentScore.
	scoreTold = false,
	// The headline already carried the winner's streak ("make it 12 straight"),
	// so the bare-result fallback below must not lean on it again.
	streakTold = false,
	// The clause that closes the sentence - the streak, the skid, the losing
	// side's big night. See ledeTail.
	tail = "",
	// The man the recap is about, for the bare-result case to lean on, and
	// whether the headline already printed his whole line.
	star?: RecapPlayer,
	starLineTold = false,
): {
	text: string;
	covers?: "comeback" | "wire" | "ot" | "run";
	// The opener carried the star's line, so the body must not.
	starFolded?: boolean;
} => {
	const verb = pastTense(pick(rng, verbPool(game, shape)));
	const w = theNick(shape.winner);
	const l = theNick(shape.loser);
	const score = scoreTold ? "" : ` ${scoreTag(shape)}`;

	if (shape.comebackFrom >= 12 && shape.comebackPeriod > 0) {
		const down = comebackSize(shape);
		// A 25-point win that began 14 down is not "erased a deficit to beat":
		// they fell behind, then ran away.
		if (shape.margin >= 15) {
			const at =
				shape.loserPeak > shape.comebackFrom
					? (game.flow?.maxLeadAt?.[sideOf(game, shape.loser)] ??
						shape.comebackPeriod)
					: shape.comebackPeriod;
			const early = at <= 2 ? " early" : "";
			return {
				text: pick(
					rng,
					[
						`${cap(w)} fell behind by ${down}${early} and still ran away from ${l}${score}${tail}.`,
						`${cap(w)} spotted ${l} ${aNum(down)}-point lead${early} and won going away${score ? `,${score}` : ""}${tail}.`,
					],
					"comebackRout",
				),
				covers: "comeback",
			};
		}
		// "to" takes a BARE INFINITIVE, so neither the past-tense verb nor the
		// comeback pool belongs in this slot - between them they produced "erased
		// an 18-point deficit to stormed back to beat the Rivers". The sentence is
		// already the comeback; the verb just has to finish it.
		return {
			text: `${cap(w)} erased ${aNum(down)}-point deficit to ${pick(
				rng,
				["beat", "top", "take down", "knock off"],
				"comebackVerb",
			)} ${l}${score}${tail}.`,
			covers: "comeback",
		};
	}
	if (shape.loserPeak >= 15) {
		// Level at every period boundary, 20 down in between: the score log
		// is the only place this comeback shows, so the lead says it.
		return {
			text: pick(
				rng,
				[
					`${cap(w)} came from ${shape.loserPeak} down to ${pick(
						rng,
						["beat", "top", "take down", "knock off"],
						"comebackVerb",
					)} ${l}${score}${tail}.`,
					// A plain verb: the pool's own is a comeback verb, and "trailed
					// by 16 and still came back to top" said it twice.
					`${cap(w)} trailed by ${shape.loserPeak} and still ${pastTense(
						pick(rng, ["beat", "top", "take down"], "comebackVerb"),
					)} ${l}${score}${tail}.`,
				],
				"flowComebackLead",
			),
			covers: "comeback",
		};
	}
	if (shape.ot > 0) {
		return {
			text: `It took ${
				shape.ot === 1
					? "an extra period"
					: `${numWord(shape.ot)} extra periods`
			}, but ${w} ${verb} ${l}${score}${tail}.`,
			covers: "ot",
		};
	}
	if (shape.wireToWire && shape.margin >= 10) {
		return {
			// The tail closes the sentence, so the shapes that end on a
			// participle ("...112-99, leading start to finish") cannot take one.
			text: pick(
				rng,
				tail === ""
					? [
							`${cap(w)} led wire to wire and ${verb} ${l}${score}.`,
							`${cap(w)} ${verb} ${l}${score} without ever trailing.`,
							`${cap(w)} were in front from the opening tip and ${verb} ${l}${score}.`,
							`${cap(w)} ${verb} ${l}${score}, leading start to finish.`,
						]
					: [
							`${cap(w)} led wire to wire and ${verb} ${l}${score}${tail}.`,
							`${cap(w)} were in front from the opening tip and ${verb} ${l}${score}${tail}.`,
							`${cap(w)} never trailed and ${verb} ${l}${score}${tail}.`,
						],
				"wireLead",
			),
			covers: "wire",
		};
	}
	const quarterRole = bigQuarterRole(shape);
	if (
		shape.bigRun &&
		quarterRole &&
		shape.bigRun.margin >= 9 &&
		shape.margin >= 8 &&
		shape.bigRun.period > 1 &&
		!closeLate(game)
	) {
		return {
			text: `${cap(w)} ${verb} ${l}${score}, ${
				quarterRole === "broke" ? "breaking it open" : "taking over"
			} with a ${shape.bigRun.wpts}-${shape.bigRun.lpts} ${ordinal(
				shape.bigRun.period,
			)} quarter.`,
			covers: "run",
		};
	}
	// Nothing about the game's shape to carry, and the headline has the score:
	// "The Mavericks topped the 76ers." is a sentence with nothing in it. The
	// streak the win extended or the venue gives it a reason to exist.
	if (scoreTold && tail === "" && star) {
		// Failing those, the star's line rides on it - "The Hawks edged the
		// Bulls behind Ace Hawk's 28 points" - and his own sentence is not
		// needed. Only when the headline did not already print that line.
		const streak = shape.winner.streak;
		const winnerHome = game.teams[0].tid === shape.winner.tid;
		const hasReason =
			(streak?.won && streak.count >= 3 && !game.playoffs && !streakTold) ||
			(!winnerHome && !game.playoffs);
		if (!hasReason && !starLineTold) {
			return {
				text: `${cap(w)} ${verb} ${l} behind ${poss(star.name)} ${statPhrase(star)}.`,
				starFolded: true,
			};
		}
	}
	if (scoreTold && tail === "") {
		const streak = shape.winner.streak;
		if (streak?.won && streak.count >= 3 && !game.playoffs && !streakTold) {
			return {
				text: `${cap(w)} ${verb} ${l} for their ${ordinal(streak.count)} win in a row.`,
			};
		}
		const winnerHome = game.teams[0].tid === shape.winner.tid;
		if (!winnerHome && !game.playoffs) {
			return { text: `${cap(w)} went on the road and ${verb} ${l}.` };
		}
	}
	return { text: `${cap(w)} ${verb} ${l}${score}${tail}.` };
};

// "the visiting 76ers" - the wire tag for the away side in a result
// sentence, used now and then so the venue is in the lede without a
// sentence of its own. Only for the LOSER as object: "the visiting Wizards
// beat" reads fine too, but the road-win shapes already carry that.
const loserTag = (game: RecapGame, shape: Shape, rng: () => number): string => {
	const loserAway = game.teams[1].tid === shape.loser.tid;
	if (loserAway && !game.playoffs && rng() < 0.3) {
		return `the visiting ${nick(shape.loser)}`;
	}
	return theNick(shape.loser);
};

const leadSentence = (
	game: RecapGame,
	shape: Shape,
	star: RecapPlayer,
	rng: () => number,
	// The result has already been stated (by the headline and the result lead),
	// so give the star's line on its own rather than tacking the score on again.
	omitResult = false,
	// Weaker than omitResult: the result clause stays, only the score goes,
	// because the headline already printed it.
	scoreTold = false,
	// The clause that closes the result. See ledeTail.
	tail = "",
): string => {
	// A plain verb for a comeback: the flow sentence that follows tells the
	// comeback itself, and "stormed back to beat the Spurs... trailed by 15
	// and stormed back" was the same verb twice in two sentences.
	const verb = pastTense(
		pick(
			rng,
			comebackSize(shape) > 0
				? ["beat", "top", "take down", "knock off"]
				: verbPool(game, shape),
			comebackSize(shape) > 0 ? "comebackVerb" : undefined,
		),
	);

	// A big blowout with no standout line is a TEAM story - "Brad Miller had 13
	// points as the Clippers won by 31" makes 13 points sound like the reason.
	if (star.pts < 15 && shape.margin >= 20) {
		if (omitResult) {
			return shape.winner.players.some((p) => p.pts > star.pts)
				? `${star.name} had ${statPhrase(star)}.`
				: `${star.name} led the way with ${statPhrase(star)}.`;
		}
		const ledTeam = !shape.winner.players.some((p) => p.pts > star.pts);
		return `${cap(theNick(shape.winner))} ${verb} ${theNick(shape.loser)}${
			scoreTold ? "" : ` ${scoreTag(shape)}`
		}${tail}, ${ledTeam ? `led by ${poss(star.name)}` : `with ${star.name} putting up`} ${statPhrase(star)}.`;
	}

	// When the result has already been stated, the star's sentence is the only
	// place his line lands in the body, so let the shooting split in at a lower
	// bar - it's what makes restating the headline's numbers worth reading.
	const flourish =
		star.pts >= (omitResult ? 16 : 25) ? shootingFlourish(star) : undefined;
	const line = enteringLine(star, game.playoffs);

	let subject = star.name;
	// Occasionally frame the star against the average he came in with.
	if (line && star.pts >= line.pts + 12 && star.pts >= 24 && rng() < 0.6) {
		subject = `${star.name}, who came in averaging ${line.pts} points a game,`;
	} else if (star.rookie && star.pts >= 20 && rng() < 0.5) {
		// "Rookie Jalen Green scored 24" - the word a desk puts on a
		// first-year man's big night. Not every night: a page where he is
		// "Rookie" in all twelve of his recaps reads as a label.
		subject = `Rookie ${star.name}`;
	} else if (
		star.age !== undefined &&
		star.age >= 34 &&
		star.pts >= 24 &&
		rng() < 0.35
	) {
		subject = `${star.name}, ${star.age},`;
	}

	// The story pick is made on the whole line, so a 19-point night with 7
	// rebounds, 5 assists and 3 steals can be the story over a 28-point one.
	// Printed as "19 points" alone it looks like the wrong man; when he is not
	// the top scorer and his line would come out points-only, let the
	// all-round numbers in at a lower bar.
	const topPts = Math.max(...shape.winner.players.map((p) => p.pts));
	const plain = statPhrase(star);
	const statText =
		plain === plural(star.pts, "point") && star.pts < topPts
			? statPhrase(star, 2, true)
			: plain;
	// A triple-double (or bigger) deserves a strong verb even when the point total
	// is modest - "chipped in 18, 12 and 12" undersells it.
	const doubles = doubleCategories(star).length;
	const hasExtras = statText !== plural(star.pts, "point");
	const actionVerb =
		doubles >= 3
			? pick(rng, [
					"posted",
					"recorded",
					"produced",
					"stuffed the stat sheet with",
				])
			: leadVerb(star, hasExtras, rng);
	if (omitResult) {
		return `${subject} ${actionVerb} ${withFlourish(statText, flourish)}.`;
	}
	return `${subject} ${actionVerb} ${withFlourish(statText, flourish, true)} as ${theNick(
		shape.winner,
	)} ${verb} ${loserTag(game, shape, rng)}${scoreTold ? "" : ` ${scoreTag(shape)}`}${tail}.`;
};

// A NIGHT THAT BELONGED TO THE TEAM. Two men with the same number ("scored
// 16 apiece"), or six or seven in double figures behind a quiet leader, is a
// lede a desk writes about the team - "Anthony Lowry put up 16" over two
// teammates at 16 and 14 made the piece look as if it had picked a name out
// of a hat.
// A teammate with the same number as the star, neither with a double-double:
// the two are told together ("scored 26 apiece"), never one after the other.
// Not when his line is more than points: "Franz Foster and Tyrese Foster
// scored 17 points apiece" dropped the 7 steals he was picked for.
const twinOf = (shape: Shape, star: RecapPlayer): RecapPlayer | undefined =>
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

const teamLede = (
	game: RecapGame,
	shape: Shape,
	star: RecapPlayer,
	rng: () => number,
	scoreTold: boolean,
	tail: string,
): { text: string; twin?: RecapPlayer; dblFig?: boolean } | undefined => {
	const verb = pastTense(
		pick(
			rng,
			comebackSize(shape) > 0
				? ["beat", "top", "take down", "knock off"]
				: verbPool(game, shape),
			comebackSize(shape) > 0 ? "comebackVerb" : undefined,
		),
	);
	const W = theNick(shape.winner);
	const L = theNick(shape.loser);
	const score = scoreTold ? "" : ` ${scoreTag(shape)}`;
	const dd = doubleCategories(star).length;
	const twin = twinOf(shape, star);
	if (twin) {
		return {
			text: `${star.name} and ${twin.name} scored ${star.pts} points apiece as ${W} ${verb} ${L}${score}${tail}.`,
			twin,
		};
	}
	const w = teamStats(shape.winner);
	if (w.dblFig >= 6 && star.pts < 18 && dd < 3) {
		// "Led by" and "leading the way" say he led the team, which is false
		// when a teammate outscored him - "led by Jaden Mathis' 10 points and
		// 10 assists" over a teammate's 18. Then he is only named.
		const ledTeam = !shape.winner.players.some((p) => p.pts > star.pts);
		return {
			text: pick(
				rng,
				ledTeam
					? [
							// Not "Six Jazz scored in double figures as the Jazz got
							// past..., Keegan Mathis' 15 points leading the way": the
							// team twice in one clause, and the man an afterthought.
							`${poss(star.name)} ${statPhrase(star)} led ${numWord(w.dblFig)} players in double figures as ${W} ${verb} ${L}${score}${tail}.`,
							`${cap(W)} got ${numWord(w.dblFig)} men into double figures and ${verb} ${L}${score}${tail}, led by ${poss(star.name)} ${statPhrase(star)}.`,
						]
					: [
							`${star.name} had ${statPhrase(star)}, and ${numWord(w.dblFig)} players reached double figures as ${W} ${verb} ${L}${score}${tail}.`,
							`${cap(W)} got ${numWord(w.dblFig)} men into double figures and ${verb} ${L}${score}${tail}, with ${star.name} putting up ${statPhrase(star)}.`,
						],
				"teamLedeBalance",
			),
			dblFig: true,
		};
	}
	return undefined;
};

// The winner's second-half scoring edge (winner pts - loser pts after halftime),
// when the quarter data supports it. Shared by the comeback flow line and the
// standalone second-half note.
const secondHalfSplit = (
	shape: Shape,
): { w: number; l: number } | undefined => {
	const { wq, lq, regPeriods } = shape;
	if (regPeriods < 4 || wq.length < regPeriods || lq.length < regPeriods) {
		return undefined;
	}
	const half = Math.floor(regPeriods / 2);
	let w = 0;
	let l = 0;
	for (let i = half; i < regPeriods; i++) {
		w += wq[i] ?? 0;
		l += lq[i] ?? 0;
	}
	return { w, l };
};

// "Held off" says the winners were in front and kept the other side off.
// "The Pistons held off the Kings 119-114" of a game tied at 112 with 1:35
// left and won on a three with 54 seconds to go was a game they went and
// took. Without the lead log, nothing says otherwise.
const heldOn = (game: RecapGame): boolean => {
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
const closeLate = (game: RecapGame): boolean => {
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

// How the game unfolded, from the quarter-by-quarter scoring.
type FlowCover = "comeback" | "wire" | "ot" | "run";

const flowSentence = (
	shape: Shape,
	rng: () => number,
): { text: string; covers?: FlowCover } | undefined => {
	if (shape.comebackFrom >= 12 && shape.comebackPeriod > 0) {
		// One rich sentence that tells the whole comeback - deficit, when, and the
		// second-half surge that erased it - instead of two half-sentences spread
		// across the recap.
		const half = secondHalfSplit(shape);
		const surge =
			half && half.w - half.l >= 8
				? `, outscoring ${theNick(shape.loser)} ${half.w}-${half.l} after halftime`
				: "";
		return {
			covers: "comeback",
			text: pick(rng, [
				`The ${nick(shape.winner)} trailed by ${
					shape.comebackFrom
				} after the ${ordinal(shape.comebackPeriod)} and stormed back${surge}.`,
				`It was ${aNum(shape.comebackFrom)}-point comeback: down that much after the ${ordinal(
					shape.comebackPeriod,
				)}, ${theNick(shape.winner)} charged home${surge}.`,
			]),
		};
	}
	// A specific run that broke the game open beats the generic wire-to-wire
	// note (which was showing up in most recaps on a chalky night).
	const quarterRole = bigQuarterRole(shape);
	if (
		shape.bigRun &&
		quarterRole &&
		shape.bigRun.margin >= 9 &&
		shape.margin >= 8 &&
		shape.regPeriods >= 3 &&
		shape.bigRun.period > 1 &&
		!closeLate(shape.game)
	) {
		const run = `${shape.bigRun.wpts}-${shape.bigRun.lpts}`;
		const per = ordinal(shape.bigRun.period);
		return {
			covers: "run",
			text: pick(
				rng,
				quarterRole === "broke"
					? [
							`A ${run} ${per} quarter broke it open.`,
							`The ${per} quarter settled it: ${run}.`,
							`${cap(theNick(shape.winner))} took over in the ${per}, ${run}.`,
							`The game turned on a ${run} ${per} quarter.`,
						]
					: [
							`${cap(theNick(shape.winner))} took over in the ${per}, ${run}.`,
							`The game turned on a ${run} ${per} quarter.`,
						],
				`flowQuarter:${quarterRole}`,
			),
		};
	}
	if (shape.wireToWire && shape.margin >= 10) {
		// Varied, and carrying the halftime score when the lead was already big.
		const half = secondHalfSplit(shape);
		const wFirst = half ? shape.winner.pts - half.w : undefined;
		const lFirst = half ? shape.loser.pts - half.l : undefined;
		const options = [
			`${cap(theNick(shape.winner))} never trailed.`,
			`${cap(theNick(shape.winner))} led from start to finish.`,
		];
		if (wFirst !== undefined && lFirst !== undefined && wFirst - lFirst >= 10) {
			options.push(
				`${cap(theNick(shape.winner))} were in control throughout, up ${wFirst}-${lFirst} at the half.`,
			);
		}
		return { text: pick(rng, options), covers: "wire" };
	}
	if (shape.ot > 0) {
		const extra =
			shape.ot === 1 ? "an extra period" : `${shape.ot} extra periods`;
		return {
			covers: "ot",
			text: pick(
				rng,
				[
					`Neither side could pull away in regulation, and it took ${extra} to settle it.`,
					// No fixed minute count here: quarter length and the number of
					// periods are both league settings.
					`Regulation settled nothing, and it took ${extra}.`,
				],
				"otLede",
			),
		};
	}
	// Down at the break and won: the story of that game is its second half,
	// not "led by 3 entering the fourth". This used to surface a paragraph
	// later, after the injury news, as an afterthought to the weaker line.
	const half = secondHalfSplit(shape);
	if (half && shape.comebackFrom < 12) {
		const n = Math.floor(shape.regPeriods / 2);
		let wFirst = 0;
		let lFirst = 0;
		for (let i = 0; i < n; i++) {
			wFirst += shape.wq[i] ?? 0;
			lFirst += shape.lq[i] ?? 0;
		}
		const down = lFirst - wFirst;
		if (down >= 6) {
			return {
				covers: "comeback",
				text: pick(
					rng,
					[
						`Down ${down} at the break, ${theNick(shape.winner)} outscored ${theNick(shape.loser)} ${half.w}-${half.l} after halftime.`,
						`${cap(theNick(shape.winner))} trailed by ${down} at halftime and won the second half ${half.w}-${half.l}.`,
						`${cap(aNum(down))}-point halftime deficit turned into a ${half.w}-${half.l} second half.`,
					],
					"halfComeback",
				),
			};
		}
	}
	if (
		Math.abs(shape.marginEnteringLast) <= 4 &&
		shape.margin <= 10 &&
		shape.regPeriods >= 4
	) {
		const m = shape.marginEnteringLast;
		const last = ordinal(shape.regPeriods);
		if (m === 0) {
			return {
				text: pick(
					rng,
					[
						`The game was tied entering the ${last}.`,
						`It was tied going into the ${last}.`,
						...(shape.regPeriods === 4
							? [`Nothing separated them after three.`]
							: []),
					],
					"tiedEnteringLast",
				),
			};
		}
		const leader = cap(theNick(m > 0 ? shape.winner : shape.loser));
		const by = Math.abs(m);
		return {
			text: pick(
				rng,
				[
					`${leader} led by ${by} entering the ${last}.`,
					`${leader} took ${aNum(by)}-point lead into the ${last}.`,
					`It was ${aNum(by)}-point game going into the ${last}.`,
					...(shape.regPeriods === 4
						? [`${leader} were ${by} up with a quarter to play.`]
						: []),
				],
				"leadEnteringLast",
			),
		};
	}
	return undefined;
};

// A team-level statistical note: shooting, rebounding, turnovers, or balance.
// Which team-level fact a note is built on, so two sentences can't both spend
// it ("The Nets turned 19 Magic turnovers into offense. ... The Magic were
// undone by 19 turnovers.").
type StatFact = "loserTov" | "loserFgp" | "loserThrees" | "other";

// What a team-stat sentence is ABOUT, so paragraph three doesn't spend its one
// three-point observation restating the one paragraph one already made. This
// was the most common tell in the whole thing: "They knocked down 16 threes."
// two sentences before "From deep it was no contest - 16 threes to 6."
type StatTopic = "threes" | "freeThrows";

const statNote = (
	shape: Shape,
	rng: () => number,
	// The opening surge has already been described, so don't offer it again.
	skipHotStart = false,
	// The lede already counted the men in double figures.
	skipDblFig = false,
): { text: string; fact: StatFact; topic?: StatTopic } | undefined => {
	const w = teamStats(shape.winner);
	const l = teamStats(shape.loser);
	const options: { text: string; fact: StatFact; topic?: StatTopic }[] = [];

	const add = (text: string, fact: StatFact = "other", topic?: StatTopic) => {
		options.push({ text, fact, topic });
	};
	// A team number earns a sentence when it is unusual, roughly one winner
	// in ten: at 14 threes, 28 assists or 16 forced turnovers - a quarter to
	// half of all winners - "They racked up 28 assists" sat in the middle of
	// the story in most recaps and explained nothing.

	if (w.fga >= 20 && w.fgp >= 52) {
		// One fixed sentence here put "The X shot 52% from the field." in nine
		// recaps out of twelve on one slate - by far the most repeated line on
		// the page, and the easiest to vary.
		add(
			pick(
				rng,
				[
					`${cap(theNick(shape.winner))} shot ${w.fgp}% from the field.`,
					`${cap(theNick(shape.winner))} were efficient all night, ${w.fgp}% from the floor.`,
					`${cap(theNick(shape.winner))} hit ${w.fgp}% of their shots.`,
					`Shots fell for the ${nick(shape.winner)} - ${w.fgp}% from the field.`,
					`${cap(theNick(shape.winner))} shot it at a ${w.fgp}% clip.`,
				],
				"stat:winner-fgp",
			),
		);
	}
	if (w.tp >= 16) {
		add(
			pick(
				rng,
				[
					`${cap(theNick(shape.winner))} knocked down ${w.tp} threes.`,
					`${cap(theNick(shape.winner))} hit ${w.tp} from deep.`,
					`${cap(theNick(shape.winner))} made ${w.tp} three-pointers.`,
					// Not "got 21 to fall from deep": 21 of what, from how many.
					`${cap(theNick(shape.winner))} went ${w.tp}-of-${w.tpa} from three.`,
				],
				"statThrees",
			),
			"other",
			"threes",
		);
	}
	// Realistic team totals only (a full box score), so partial data can't yield
	// an absurd "won the glass 22-0".
	if (w.reb >= 30 && l.reb >= 20 && w.reb - l.reb >= 12) {
		add(
			pick(
				rng,
				[
					`${cap(theNick(shape.winner))} won the glass ${w.reb}-${l.reb}.`,
					`${cap(theNick(shape.winner))} controlled the boards, ${w.reb}-${l.reb}.`,
					`${cap(theNick(shape.winner))} out-rebounded ${theNick(shape.loser)} ${w.reb}-${l.reb}.`,
					`The glass belonged to ${theNick(shape.winner)}, ${w.reb}-${l.reb}.`,
				],
				"statReb",
			),
		);
	}
	// Six-plus is a genuinely balanced night; five is routine and was showing up
	// in half the recaps on a given day.
	// Seven: in this league six in double figures is a third of all winners,
	// and "They had six players score in double figures" was the sentence
	// most often repeated word for word.
	if (w.dblFig >= 7 && !skipDblFig) {
		add(
			pick(
				rng,
				[
					`${cap(theNick(shape.winner))} had ${numWord(w.dblFig)} players score in double figures.`,
					`${cap(theNick(shape.winner))} got double figures out of ${numWord(w.dblFig)} men.`,
					`${cap(numWord(w.dblFig))} ${nick(shape.winner)} reached double figures.`,
					`It came from everywhere for ${theNick(shape.winner)} - ${numWord(w.dblFig)} in double figures.`,
				],
				"statDblFig",
			),
		);
	}
	if (w.stl >= 10 && l.tov >= 21 && l.tov - w.tov >= 5) {
		add(
			pick(
				rng,
				[
					`${cap(theNick(shape.winner))} forced ${l.tov} turnovers.`,
					`${cap(theNick(shape.winner))} turned ${l.tov} ${nick(shape.loser)} turnovers into offense.`,
					`${cap(theNick(shape.winner))} hounded ${theNick(shape.loser)} into ${l.tov} turnovers.`,
				],
				"forcedTov",
			),
			"loserTov",
		);
	}
	if (w.ast >= 30) {
		add(
			pickSentence(
				rng,
				[
					`${cap(theNick(shape.winner))} piled up ${w.ast} assists on the night.`,
					`${cap(theNick(shape.winner))} finished with ${w.ast} assists.`,
					// What the wire says of a passing night: how many of the
					// baskets came off a pass. "Shared it, 31 assists on the
					// night" was neither a sentence nor news.
					`${cap(theNick(shape.winner))} assisted on ${w.ast} of their ${w.fg} baskets.`,
					`${cap(theNick(shape.winner))} racked up ${w.ast} assists.`,
				],
				"teamAst",
			),
		);
	}
	// A hot start: the winner's first-quarter margin.
	if (
		!skipHotStart &&
		shape.bigRun &&
		shape.bigRun.period === 1 &&
		shape.bigRun.margin >= 10 &&
		shape.wq.length > 0
	) {
		const q = `${shape.bigRun.wpts}-${shape.bigRun.lpts}`;
		add(
			pick(
				rng,
				// An 18-point first quarter in a one-point game is not a hot
				// start, it is a lead that nearly got away - and "jumped out to a
				// 41-23 first quarter" under "survived the Bucks 115-114" left the
				// reader to work out for himself that the Bucks came all the way
				// back.
				shape.margin <= 5
					? [
							`${cap(theNick(shape.winner))} led ${q} after one and then had to hold on.`,
							`It was ${q} after the first, and ${theNick(shape.loser)} spent the rest of the night clawing back.`,
							`${cap(aNum(shape.bigRun.margin))}-point first quarter was very nearly not enough.`,
						]
					: [
							`${cap(theNick(shape.winner))} jumped out to a ${q} first quarter.`,
							`It was ${q} after one.`,
							`${cap(theNick(shape.winner))} were ahead almost immediately, ${q} after the first.`,
						],
				shape.margin <= 5 ? "firstQuarterHeldOn" : "firstQuarterRun",
			),
		);
	}
	// A big edge at the free-throw line.
	if (w.ft >= 24 && w.ft - l.ft >= 10) {
		add(
			pick(
				rng,
				[
					`${cap(theNick(shape.winner))} made ${w.ft} free throws to ${l.ft} for ${theNick(
						shape.loser,
					)}.`,
					`${cap(theNick(shape.winner))} lived at the line, making ${w.ft} free throws to ${poss(theNick(shape.loser))} ${l.ft}.`,
					`It was ${w.ft} made free throws to ${l.ft} in ${poss(
						theNick(shape.winner),
					)} favor.`,
				],
				"ledeFreeThrows",
			),
			"other",
			"freeThrows",
		);
	}
	if (options.length === 0) {
		return undefined;
	}
	const chosen = pick(
		rng,
		options.map((o) => o.text),
	);
	return options.find((o) => o.text === chosen)!;
};

// The score at the break, stated plainly. Every wire recap has it; this
// engine only had it when the half was part of a bigger story.
// A sentence about the second half or the finish: a third or fourth quarter,
// a run late on, the go-ahead and the clinching free throws, overtime, a lead
// nearly let go. Anything the halftime score must come BEFORE.
const SECOND_HALF_OR_FINISH =
	/\b(?:third|fourth|final) (?:quarter|period)|\bin the (?:third|fourth)\b|\bthe (?:third|fourth)\b|quarter to play|with \d+(?:\.\d+)? seconds|with (?:no time|\d+:\d\d)|for good|sealed it|put it away|finished it|closed it out|down the stretch|second half|had to hold on|let it go\.|won anyway|before winning it|settled it|overtime|got as close as|cut it to|pulled within|was down to \d+ at the end|late run|the last (?:minute|two minutes)/i;

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

// The halftime / second-half story, from the quarter scores.
const secondHalfNote = (
	shape: Shape,
	rng: () => number,
): string | undefined => {
	const { wq, lq, regPeriods } = shape;
	if (regPeriods < 4 || wq.length < regPeriods || lq.length < regPeriods) {
		return undefined;
	}
	// A wire-to-wire game is already summarized by the flow line; don't echo it.
	if (shape.wireToWire) {
		return undefined;
	}
	// A double-digit comeback's flow line already carries the second-half surge
	// (see flowSentence) - a trailing "pulled away after halftime" would repeat it.
	if (shape.comebackFrom >= 12) {
		return undefined;
	}
	const half = Math.floor(regPeriods / 2);
	let wFirst = 0;
	let lFirst = 0;
	let wSecond = 0;
	let lSecond = 0;
	for (let i = 0; i < regPeriods; i++) {
		if (i < half) {
			wFirst += wq[i] ?? 0;
			lFirst += lq[i] ?? 0;
		} else {
			wSecond += wq[i] ?? 0;
			lSecond += lq[i] ?? 0;
		}
	}
	const halfMargin = wFirst - lFirst;
	const secondMargin = wSecond - lSecond;
	// A halftime-deficit comeback (only when the quarter-flow line didn't already
	// lead with a bigger comeback). Down 1-3 at the break is a coin flip, not a
	// story - it takes a real deficit to be worth a sentence.
	// Six, like the lede's own halftime-comeback line: down four at the break
	// is a coin flip, and a sentence about it after "led by as many as 16" read
	// as two stories arguing.
	if (halfMargin <= -6 && shape.margin > 0 && shape.comebackFrom < 12) {
		return pick(
			rng,
			[
				`Down ${-halfMargin} at the break, ${theNick(
					shape.winner,
				)} outscored ${theNick(shape.loser)} ${wSecond}-${lSecond} in the second half.`,
				`${cap(theNick(shape.winner))} trailed by ${-halfMargin} at halftime and took the second half ${wSecond}-${lSecond}.`,
				`The second half belonged to ${theNick(
					shape.winner,
				)}, ${wSecond}-${lSecond} after going in ${-halfMargin} down.`,
				`${cap(aNum(-halfMargin))}-point halftime deficit turned into a ${wSecond}-${lSecond} second half.`,
			],
			"halfComeback",
		);
	}
	// "Never looked back" and "never troubled again" are claims about the
	// end of the game, and a 22-point halftime lead that was caught and taken
	// to overtime is neither.
	const stayedAway = shape.ot === 0 && !closeLate(shape.game);
	if (secondMargin >= 12) {
		return pick(
			rng,
			[
				`${cap(theNick(shape.winner))} pulled away after halftime, taking the second half ${wSecond}-${lSecond}.`,
				`The game got away from ${theNick(
					shape.loser,
				)} after the break - ${wSecond}-${lSecond} over the last two quarters.`,
				...(stayedAway
					? [
							`${cap(theNick(shape.winner))} won the second half ${wSecond}-${lSecond} and were never troubled again.`,
						]
					: []),
			],
			"secondHalfPull",
		);
	}
	if (halfMargin >= 15 && stayedAway) {
		return pick(
			rng,
			[
				`${cap(theNick(shape.winner))} led ${wFirst}-${lFirst} at halftime and never looked back.`,
				`It was ${wFirst}-${lFirst} at the break and the outcome was already clear.`,
				`${cap(theNick(shape.winner))} built ${aNum(halfMargin)}-point halftime lead and sat on it.`,
			],
			"halftimeLead",
		);
	}
	return undefined;
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

// THE GAME, TOLD IN ORDER.
//
// Every fact about how a game went used to arrive as its own sentence from
// its own generator - the halftime score, the longest run, the biggest lead,
// the second-half split - and the recap read like a list of them: "The Heat
// won the glass 57-43. They led by 29 at the break, 59-30. They put
// together a 12-0 run in the third. The Heat led by as many as 43." A
// reporter tells the same facts as one story in the order they happened,
// with the joins a story has: "and", "but", "after the break".
//
// So this reads the half, the run, both sides' biggest leads and when they
// came, and the score entering the last period, and writes one or two
// sentences that walk through them. Every figure in it is one the accuracy
// reader already checks (halftime scores, second-half splits, "ran off N
// straight", "as many as N", "led by more than N").
type StoryCover =
	| "firstQuarter"
	| "half"
	| "secondHalf"
	| "run"
	| "winnerPeak"
	| "loserPeak"
	| "entering"
	| "lastQuarter"
	| "comeback";

const gameStory = (
	game: RecapGame,
	shape: Shape,
	rng: () => number,
	opts: {
		// The lede already said the winners came from behind.
		comebackTold: boolean;
		// The lede already said the winners never trailed.
		wireTold?: boolean;
		// The lede already gave the last quarter's score ("taking over with a
		// 34-25 fourth quarter").
		lastQuarterTold?: boolean;
	},
): { sentences: string[]; covers: Set<StoryCover> } | undefined => {
	const { wq, lq, regPeriods } = shape;
	if (
		regPeriods < 4 ||
		regPeriods % 2 !== 0 ||
		wq.length < regPeriods ||
		lq.length < regPeriods
	) {
		return undefined;
	}
	const halfN = regPeriods / 2;
	let wh = 0;
	let lh = 0;
	let w2 = 0;
	let l2 = 0;
	for (let i = 0; i < regPeriods; i++) {
		if (i < halfN) {
			wh += wq[i] ?? 0;
			lh += lq[i] ?? 0;
		} else {
			w2 += wq[i] ?? 0;
			l2 += lq[i] ?? 0;
		}
	}
	const W = cap(theNick(shape.winner));
	const w = theNick(shape.winner);
	const L = cap(theNick(shape.loser));
	const l = theNick(shape.loser);
	const flow = game.flow;
	const wSide = sideOf(game, shape.winner);
	const lSide: GameFlowSide = wSide === 0 ? 1 : 0;
	const wMax = flow?.maxLead[wSide] ?? 0;
	const lMax = flow?.maxLead[lSide] ?? 0;
	const wMaxAt = flow?.maxLeadAt?.[wSide];
	const lMaxAt = flow?.maxLeadAt?.[lSide];
	const raw = flow?.run;
	// A run across halftime belongs to neither half the story tells, so it
	// is left to the run note, which goes after the halftime score.
	const rawEnd = raw?.endPeriod ?? raw?.period ?? 0;
	const run =
		raw &&
		raw.period <= regPeriods &&
		!(raw.period <= halfN && rawEnd > halfN) &&
		(raw.side === wSide ? winnersRunIsNews(shape, raw) : raw.pts >= 12)
			? {
					winner: raw.side === wSide,
					pts: raw.pts,
					period: raw.period,
					end: rawEnd,
				}
			: undefined;
	// "in the second", or "across the third and fourth" for a run that
	// began in one quarter and ended in the next.
	const runAt = (r: { period: number; end: number }) =>
		runSpan(r.period, r.end, regPeriods);
	const when = (period: number) => periodTag(period, regPeriods);
	const lastLead = flow?.lastLead;
	// The winners were in front for good by the break.
	const aheadForGoodByHalf =
		!!lastLead && lastLead.side === wSide && lastLead.period <= halfN;
	const close = shape.margin <= 6 || shape.ot > 0 || closeLate(game);
	// The first quarter, when the winners made it one: "won the first
	// quarter 31-21" is checked against the box score like any quarter.
	const w1 = wq[0] ?? 0;
	const l1 = lq[0] ?? 0;
	const hot = w1 - l1 >= 8;
	// Ahead at the break, the first quarter is told when it was big or built
	// the whole halftime lead: "won the first quarter 34-26 and led 61-46 at
	// halftime" opened a fifth of all recaps and said no more than the half.
	const hotStart = w1 - l1 >= 12 || (hot && w1 - l1 >= wh - lh);
	const covers = new Set<StoryCover>();
	const out: string[] = [];
	// THE LEAD THAT GOT AWAY - up 18, home by seven - is told where it was
	// built, and the scores after it show where it went. A closing "They let
	// an 18-point lead get away and still won" landed after the quarter that
	// won it back: "They trailed 86-79 going into the fourth, then took the
	// fourth quarter 37-23. They let an 18-point lead get away..."
	const bigGone =
		!!flow && shape.margin <= 8 && wMax >= 15 && wMax >= shape.margin + 9;
	// The biggest lead a sentence of the story has already shown. A peak no
	// more than a basket or so past it is that lead: "won the first quarter
	// 33-17" has said "led by 16".
	let toldLead = 0;
	const peakShown = (): boolean => {
		if (bigGone && wMax <= toldLead + 3) {
			covers.add("winnerPeak");
		}
		return covers.has("winnerPeak");
	};
	// Gone before the last quarter that won it back, it is told before that
	// quarter: "They let an 18-point lead get away. It was 88-88 going into
	// the fourth, and the Bucks won the fourth quarter 22-18."
	const gotAway = (): string[] => {
		if (!bigGone || peakShown()) {
			return [];
		}
		covers.add("winnerPeak");
		return [`${W} let ${aNum(wMax)}-point lead get away.`];
	};

	// "a, b and c".
	const series = (parts: string[]) =>
		parts.length <= 1
			? (parts[0] ?? "")
			: `${parts.slice(0, -1).join(", ")} and ${parts.at(-1)}`;

	// The halftime score as a sentence. One shared pool, and a wide one:
	// this line is in nearly every recap on a page, and three recaps saying
	// "led 55-50 at halftime" the same way is what makes a slate read as a
	// form.
	const halfLine = (leader: RecapTeam, a: number, b: number): string => {
		const T = cap(theNick(leader));
		const t = theNick(leader);
		return pick(
			rng,
			[
				`${T} led ${a}-${b} at halftime.`,
				`${T} took a ${a}-${b} lead into the break.`,
				`${T} were ahead ${a}-${b} at the half.`,
				`${T} were up ${a - b} at the half, ${a}-${b}.`,
				`${T} went into halftime ahead ${a}-${b}.`,
				`${T} had a ${a}-${b} lead at the break.`,
				`By halftime ${t} were in front ${a}-${b}.`,
				`${T} carried a ${a}-${b} lead into the second half.`,
			],
			"story:half",
		);
	};

	// The score going into the last period, for a game still in doubt then.
	// `anyLead`: in a game that was close at the end, the winners' lead
	// going into the last period is told whatever its size - "The Suns were
	// up 1 at the half" and then nothing at all, in a game the Bucks got
	// back into late, stopped the story at halftime.
	const entering = (anyLead = false): string | undefined => {
		const m = shape.marginEnteringLast;
		const last = ordinal(regPeriods);
		// A lead mostly lost before the fourth goes with the score it fell
		// to: "They led by as many as 16 in the third and were still 12 up
		// going into the fourth." One from before the break the half has not
		// shown is told looking back - "had led by as many as 24 in the
		// second" - and one built in the last quarter after its score.
		if (m > 0 && (m <= 6 || anyLead)) {
			toldLead = Math.max(toldLead, m);
		}
		const peak =
			bigGone &&
			m > 0 &&
			!peakShown() &&
			wMaxAt !== undefined &&
			wMax >= toldLead + 4
				? wMaxAt > halfN && wMaxAt < regPeriods
					? `${W} led by as many as ${wMax} in ${when(wMaxAt)}`
					: wMaxAt <= halfN
						? `${W} had led by as many as ${wMax} in ${when(wMaxAt)}`
						: undefined
				: undefined;
		const stretched =
			bigGone &&
			m > 0 &&
			!peakShown() &&
			wMaxAt === regPeriods &&
			wMax >= toldLead + 4
				? `${W} took ${aNum(m)}-point lead into the ${last} and stretched it to ${wMax}`
				: undefined;
		if (peak || stretched) {
			covers.add("winnerPeak");
		}
		// THE LEAD THAT NEARLY WENT. Up 13 going into the last quarter, won by
		// 3: the losers took that quarter by 10 or more, and the recap went
		// straight from "It was a 13-point game entering the fourth" to a
		// three-pointer that cut it to one with 8 seconds left - in all 16
		// such games of a 953-game corpus, the quarter that made it close was
		// never told.
		const lLast = lq[regPeriods - 1] ?? 0;
		const wLast = wq[regPeriods - 1] ?? 0;
		if (stretched) {
			covers.add("entering");
			return `${stretched}.`;
		}
		if (m > 0 && shape.ot === 0 && shape.margin <= 6 && lLast - wLast >= 10) {
			covers.add("entering").add("lastQuarter");
			if (peak) {
				// Two sentences, not one of 140 characters that named the
				// winners twice running: the lead, then where it stood.
				out.push(`${peak}.`);
				return `It was ${aNum(m)}-point game going into the ${last}, but ${l} took the ${last} quarter ${lLast}-${wLast}.`;
			}
			return `${pick(
				rng,
				[
					`${W} were ${m} up going into the ${last}`,
					`It was ${aNum(m)}-point game entering the ${last}`,
					`Going into the ${last}, ${w} were up ${m}`,
				],
				"story:enteringUp",
			)}, but ${l} took the ${last} quarter ${lLast}-${wLast}.`;
		}
		if (peak) {
			covers.add("entering");
			return m * 3 <= wMax
				? `${peak}, but it was ${aNum(m)}-point game going into the ${last}.`
				: `${peak} and were still ${m} up going into the ${last}.`;
		}
		if (Math.abs(m) > 6 && !(anyLead && m > 0)) {
			return undefined;
		}
		covers.add("entering");
		// Close going into the last quarter and not at the end: the quarter
		// that decided it is the story's last word. "It was a 2-point game
		// entering the fourth." closed a 120-106 game.
		const pulledAway =
			m >= 0 &&
			shape.ot === 0 &&
			shape.margin - m >= 8 &&
			!opts.lastQuarterTold;
		const decided = (s: string) => {
			if (!pulledAway) {
				return s;
			}
			covers.add("lastQuarter");
			const who = s.includes(w) || s.startsWith(W) ? "they" : w;
			return `${s.slice(0, -1)}, and ${who} won the ${last} quarter ${wLast}-${lLast}.`;
		};
		if (m === 0) {
			return decided(
				pick(
					rng,
					[
						`It was tied going into the ${last}.`,
						`Nothing separated them going into the ${last}.`,
					],
					"story:enteringTied",
				),
			);
		}
		if (m > 0) {
			return decided(
				pick(
					rng,
					[
						`${W} were ${m} up going into the ${last}.`,
						`It was ${aNum(m)}-point game entering the ${last}.`,
						`Going into the ${last}, ${w} were up ${m}.`,
					],
					"story:enteringUp",
				),
			);
		}
		return pick(
			rng,
			[
				`${L} were ${-m} up going into the ${last}.`,
				`${L} took ${aNum(-m)}-point lead into the ${last}.`,
				`Going into the ${last}, ${l} were up ${-m}.`,
			],
			"story:enteringDown",
		);
	};

	// THE LAST QUARTER, when that is where it was won: behind or level going
	// into it, winners at the end. "The second half belonged to the Nuggets,
	// 64-50" was all one recap said of a game the Nuggets trailed by nine
	// after three and took 38-21 in the fourth.
	const lastIdx = regPeriods - 1;
	const sum = (q: number[]) =>
		q.slice(0, lastIdx).reduce((a, b) => a + (b ?? 0), 0);
	const wIn = sum(wq);
	const lIn = sum(lq);
	const wLast = wq[lastIdx] ?? 0;
	const lLast = lq[lastIdx] ?? 0;
	const wonInLast = shape.ot === 0 && wIn <= lIn;
	const lastQuarterTurn = (): string => {
		covers.add("entering").add("lastQuarter").add("secondHalf");
		const last = ordinal(regPeriods);
		const q = `${wLast}-${lLast}`;
		const lastRun =
			run && run.winner && run.period === regPeriods ? run : undefined;
		// A run in the quarter before, which got them back in it: told first,
		// so the clock runs forward.
		const lateRun =
			run && run.winner && run.period > halfN && run.end < regPeriods
				? run
				: undefined;
		if (lastRun || lateRun) {
			covers.add("run");
		}
		// The lede gave the quarter's score, so the story gives where it
		// stood going into it: "taking over with a 34-25 fourth quarter" and
		// then "the Warriors won the fourth quarter 34-25" was one fact twice.
		if (opts.lastQuarterTold) {
			if (lateRun) {
				const ran = `${W} ran off ${lateRun.pts} straight points ${runAt(lateRun)}`;
				return wIn === lIn
					? `${ran}, and it was ${wIn}-${lIn} going into the ${last}.`
					: `${ran} but were still ${lIn - wIn} down going into the ${last}.`;
			}
			return wIn === lIn
				? `It was ${wIn}-${lIn} going into the ${last}${lastRun ? `, and ${w} ran off ${lastRun.pts} straight points` : ""}.`
				: `${W} trailed ${lIn}-${wIn} going into the ${last}${lastRun ? `, then ran off ${lastRun.pts} straight points` : ""}.`;
		}
		if (lateRun) {
			const ran = `${W} ran off ${lateRun.pts} straight points ${runAt(lateRun)}`;
			return wIn === lIn
				? `${ran}, and it was ${wIn}-${lIn} going into the ${last} before they won the ${last} quarter ${q}.`
				: `${ran} but were still ${lIn - wIn} down going into the ${last}, then won the ${last} quarter ${q}.`;
		}
		if (wIn === lIn) {
			return lastRun
				? `It was ${wIn}-${lIn} going into the ${last}, and ${w} ran off ${lastRun.pts} straight points on the way to a ${q} ${last} quarter.`
				: pick(
						rng,
						[
							`It was ${wIn}-${lIn} going into the ${last}, and ${w} won the ${last} quarter ${q}.`,
							`Tied at ${wIn} going into the ${last}, ${w} took the ${last} quarter ${q}.`,
						],
						"story:lastTied",
					);
		}
		return lastRun
			? `${W} trailed ${lIn}-${wIn} going into the ${last}, then ran off ${lastRun.pts} straight points on the way to a ${q} ${last} quarter.`
			: pick(
					rng,
					[
						// "Still" only after a halftime they led: "The Cavaliers went
						// into halftime ahead 57-43. The Magic were still up 82-80".
						`${L} were ${lh > wh ? "still " : ""}up ${lIn}-${wIn} going into the ${last}, and ${w} won the ${last} quarter ${q}.`,
						`${W} trailed ${lIn}-${wIn} going into the ${last}, then took the ${last} quarter ${q}.`,
						`${W} were ${lIn - wIn} down going into the ${last} and won the ${last} quarter ${q}.`,
					],
					"story:lastTurn",
				);
	};

	if (lh > wh) {
		// --- DOWN AT THE BREAK, AND WON. The story is the turn. ---
		covers.add("half").add("comeback");
		const early = run && run.winner && run.period <= halfN ? run : undefined;
		if (hot || early) {
			// The winners were the better team early, lost the thread, and
			// found it again: three acts, and the first one used to be a loose
			// sentence after the second.
			covers.add("firstQuarter");
			if (early) {
				covers.add("run");
			}
			const opener =
				early && early.end === 1 && hot
					? `${W} ran off ${early.pts} straight points in the first and won the quarter ${w1}-${l1}`
					: early
						? `${W} ran off ${early.pts} straight points ${runAt(early)}`
						: `${W} won the first quarter ${w1}-${l1}`;
			if (hot && !(early && early.period > 1)) {
				toldLead = w1 - l1;
			}
			out.push(
				hot
					? `${opener}, but ${l} came back to lead ${lh}-${wh} at halftime.`
					: `${opener}, but ${l} led ${lh}-${wh} at halftime.`,
			);
		} else if (run && !run.winner && run.period <= halfN) {
			covers.add("run");
			// The deficit the winners came back from, when the run built more
			// than the halftime score shows: "Lakers rally from 15 down" sat
			// over "The Pistons were up 58-45 at the break after they ran off
			// 12 straight points in the second" and never said 15.
			const peakFirst =
				!opts.comebackTold &&
				lMaxAt !== undefined &&
				lMaxAt <= halfN &&
				lMax >= 12 &&
				lMax > lh - wh &&
				// Fifteen is a comeback the headline can name; under it, only a
				// peak well past the halftime score is worth the words.
				(lMax >= 15 || lMax >= lh - wh + 3);
			if (peakFirst) {
				covers.add("loserPeak");
			}
			out.push(
				peakFirst
					? `${L} ran off ${run.pts} straight points ${runAt(run)}, led by as many as ${lMax} and were up ${lh}-${wh} at halftime.`
					: pick(
							rng,
							[
								`${L} ran off ${run.pts} straight points ${runAt(run)} and led ${lh}-${wh} at halftime.`,
								`${L} were up ${lh}-${wh} at the break after they ran off ${run.pts} straight points ${runAt(run)}.`,
							],
							"story:downRun",
						),
			);
		} else if (
			!opts.comebackTold &&
			lMaxAt !== undefined &&
			lMaxAt <= halfN &&
			lMax >= lh - wh + 4
		) {
			covers.add("loserPeak");
			out.push(
				pick(
					rng,
					[
						`${L} led by as many as ${lMax} in ${when(lMaxAt)} and were still up ${lh}-${wh} at halftime.`,
						`${L} were up by as many as ${lMax} in ${when(lMaxAt)} and took a ${lh}-${wh} lead into the break.`,
					],
					"story:downPeak",
				),
			);
		} else {
			out.push(halfLine(shape.loser, lh, wh));
		}
		covers.add("secondHalf");
		if (shape.ot > 0) {
			out.push(
				// Not "to force overtime": the side that tied it at the end may
				// have been the other one, and the finish says who. And not "and
				// it went to overtime" either: the lead has said that, and the
				// finish says it again with the shot that forced it. The score
				// it ended at is the fact neither of them has.
				`${W} won the second half ${w2}-${l2}, and regulation ended tied at ${wh + w2}.`,
			);
		} else if (wonInLast) {
			out.push(...gotAway(), lastQuarterTurn());
		} else if (run && run.winner && run.period > halfN) {
			covers.add("run");
			out.push(
				pick(
					rng,
					[
						`${W} took over after the break: they ran off ${run.pts} straight points ${runAt(run)} and won the second half ${w2}-${l2}.`,
						`After halftime it was all ${w}, ${w2}-${l2}, and they ran off ${run.pts} straight points ${runAt(run)}.`,
					],
					"story:turnRun",
				),
			);
		} else {
			// Opened on the winners already ("The Mavericks ran off 12 straight
			// in the first... but the Hawks came back"), the turn opens on the
			// clock rather than naming them a second time running.
			const openedOnWinner = out[0]!.startsWith(W);
			out.push(
				pick(
					rng,
					openedOnWinner
						? [
								`After the break, ${w} outscored ${l} ${w2}-${l2}.`,
								`After halftime it was ${w} again, ${w2}-${l2} over the second half.`,
							]
						: w2 - l2 >= 8
							? [
									`${W} took over after the break, winning the second half ${w2}-${l2}.`,
									`After halftime it was all ${w}, ${w2}-${l2}.`,
									`The second half belonged to ${w}, ${w2}-${l2}.`,
								]
							: [
									`${W} outscored ${l} ${w2}-${l2} after the break.`,
									`${W} won the second half ${w2}-${l2}.`,
								],
					openedOnWinner ? "story:turnAgain" : "story:turn",
				),
			);
		}
		return { sentences: out, covers };
	}

	covers.add("half");
	if (wh === lh) {
		// --- TIED AT THE BREAK. ---
		// A lead from before the break goes with the tie it fell to.
		const tiedHalf = (): string => {
			if (bigGone && wMaxAt !== undefined && wMaxAt <= halfN) {
				covers.add("winnerPeak");
				return `${W} led by as many as ${wMax} in ${when(wMaxAt)}, but it was ${wh}-${lh} at halftime.`;
			}
			// Or the losers' lead, which is the comeback: "The Pistons had a
			// 19-0 run in the first, and it still was not enough. The teams
			// were tied at 56 at the break" over "rally from 19 down".
			if (
				!opts.comebackTold &&
				lMax >= 12 &&
				lMaxAt !== undefined &&
				lMaxAt <= halfN
			) {
				covers.add("loserPeak").add("comeback");
				if (run && !run.winner && run.end <= halfN) {
					covers.add("run");
					return `${L} ran off ${run.pts} straight points ${runAt(run)} and led by as many as ${lMax}, but it was ${wh}-${lh} at halftime.`;
				}
				return `${L} led by as many as ${lMax} in ${when(lMaxAt)}, but it was ${wh}-${lh} at halftime.`;
			}
			return pick(
				rng,
				[
					`It was ${wh}-${lh} at halftime.`,
					`The teams were tied at ${wh} at the break.`,
				],
				"story:halfTied",
			);
		};
		if (wonInLast) {
			const half = tiedHalf();
			out.push(half, ...gotAway(), lastQuarterTurn());
			return { sentences: out, covers };
		}
		const loserPeakFirst =
			!opts.comebackTold &&
			lMax >= 12 &&
			lMaxAt !== undefined &&
			lMaxAt <= halfN;
		if (!close && w2 - l2 >= 8) {
			covers.add("secondHalf");
			const parts = [`won the second half ${w2}-${l2}`];
			if (run && run.winner && run.period > halfN) {
				covers.add("run");
				parts.push(`ran off ${run.pts} straight points ${runAt(run)}`);
			}
			// The losers' early lead first, then the half that won it: "The
			// Kings ran off 16 straight points in the first and led by as many
			// as 12, but it was 57-57 at halftime" ended the story there, over
			// a 54-41 second half.
			if (loserPeakFirst) {
				out.push(tiedHalf(), `${W} ${series(parts)}.`);
			} else {
				out.push(
					`It was ${wh}-${lh} at halftime before ${w} ${series(parts)}.`,
				);
			}
			return { sentences: out, covers };
		}
		out.push(tiedHalf());
		const e = entering();
		if (e) {
			out.push(e);
		}
		return { sentences: out, covers };
	}

	// --- IN FRONT AT THE BREAK. ---
	const up = wh - lh;
	const firstRun = run && run.winner && run.period <= halfN ? run : undefined;
	if (
		!opts.comebackTold &&
		lMaxAt !== undefined &&
		lMaxAt <= halfN &&
		lMax >= 10
	) {
		// Behind early, in front by the break.
		covers.add("loserPeak");
		if (firstRun) {
			covers.add("run");
		}
		out.push(
			`${L} led by as many as ${lMax} in ${when(lMaxAt)}, but ${w} ${
				firstRun
					? `ran off ${firstRun.pts} straight points ${runAt(firstRun)} and `
					: ""
			}${pick(rng, [`led ${wh}-${lh} at halftime`, `were in front ${wh}-${lh} by the break`], "story:turnedByHalf")}.`,
		);
	} else if (
		bigGone &&
		!firstRun &&
		wMaxAt !== undefined &&
		wMaxAt <= halfN &&
		wMax >= up + 4 &&
		!(hotStart && wMax <= w1 - l1 + 3)
	) {
		// The lead that got away outranks the first quarter that built it.
		covers.add("winnerPeak");
		out.push(
			pick(
				rng,
				[
					`${W} led by as many as ${wMax} in ${when(wMaxAt)} and were still up ${wh}-${lh} at halftime.`,
					`${W} were up by as many as ${wMax} in ${when(wMaxAt)} and took a ${wh}-${lh} lead into the break.`,
				],
				"story:upPeak",
			),
		);
	} else if (firstRun && firstRun.end === 1 && hotStart) {
		covers.add("run").add("firstQuarter");
		toldLead = w1 - l1;
		out.push(
			`${W} ran off ${firstRun.pts} straight points in the first, won the quarter ${w1}-${l1} and led ${wh}-${lh} at halftime.`,
		);
	} else if (!firstRun && hotStart) {
		covers.add("firstQuarter");
		toldLead = w1 - l1;
		out.push(
			pick(
				rng,
				[
					`${W} won the first quarter ${w1}-${l1} and led ${wh}-${lh} at halftime.`,
					`${W} took the first quarter ${w1}-${l1} and were up ${wh}-${lh} at the break.`,
				],
				"story:hotStart",
			),
		);
	} else if (firstRun) {
		covers.add("run");
		out.push(
			pick(
				rng,
				[
					`${W} ran off ${firstRun.pts} straight points ${runAt(firstRun)} and led ${wh}-${lh} at halftime.`,
					`${W} were up ${wh}-${lh} at the break after they ran off ${firstRun.pts} straight points ${runAt(firstRun)}.`,
				],
				"story:upRun",
			),
		);
	} else {
		out.push(halfLine(shape.winner, wh, lh));
	}
	toldLead = Math.max(toldLead, up);
	peakShown();

	if (wonInLast) {
		out.push(...gotAway(), lastQuarterTurn());
		return { sentences: out, covers };
	}

	if (close) {
		// Tight to the end: how tight, going into the last period. The finish
		// takes it from there.
		const most = Math.max(wMax, lMax);
		if (flow && most > 0 && most <= 8) {
			out.push(
				pick(
					rng,
					[
						`Neither side led by more than ${most} all night.`,
						`It never got away from either team: neither led by more than ${most}.`,
					],
					"story:neverApart",
				),
			);
		} else {
			const e = entering(true);
			if (e) {
				out.push(e);
			}
		}
		return { sentences: out, covers };
	}

	// DECIDED, BUT NOT AT THE BREAK. "The Hornets led 67-54 at halftime.
	// They pushed on after the break and led by as many as 20" - of a game
	// the Clippers took the third quarter 37-27 and were down three going
	// into the fourth. A lead that came back to a few points before the last
	// quarter is the second half's story, whatever the final margin.
	const mIn = shape.marginEnteringLast;
	if (shape.ot === 0 && mIn > 0 && mIn <= 6 && up - mIn >= 6) {
		covers.add("entering").add("secondHalf");
		const last = ordinal(regPeriods);
		const after = regPeriods === 4 ? "after three" : `going into the ${last}`;
		// How far they came back: in front in the last quarter, in front
		// or level before it, or only close. "Cut it to 69-68 after three"
		// of a game the Raptors then led by five in the fourth told half of
		// it.
		const tie = flow?.lastTie;
		const loserLedAt =
			lMax > 0 && lMaxAt !== undefined && lMaxAt > halfN ? lMaxAt : undefined;
		const tiedAt =
			loserLedAt === undefined && tie && tie.period > halfN
				? tie.period
				: undefined;
		// Their peak and the comeback in one quarter could have come in
		// either order; "led by 17 in the third, but tied it in the third"
		// would claim one.
		const peak =
			!covers.has("winnerPeak") &&
			wMaxAt !== undefined &&
			wMaxAt > halfN &&
			wMaxAt < regPeriods &&
			wMax >= up + 4 &&
			wMaxAt !== loserLedAt &&
			wMaxAt !== tiedAt;
		if (peak) {
			covers.add("winnerPeak");
		}
		// How it got away again, unless the lede gave the quarter: with the
		// run in it when there was one, or the run is told on its own later
		// as "The game got away in the fourth" after the quarter's score.
		const lastRun =
			run && run.winner && run.period === regPeriods ? run : undefined;
		if (lastRun && !opts.lastQuarterTold) {
			covers.add("run");
		}
		const finish = opts.lastQuarterTold
			? undefined
			: lastRun
				? `${w} ran off ${lastRun.pts} straight points in a ${wLast}-${lLast} ${last}`
				: `${w} won the ${last} ${wLast}-${lLast}`;
		if (finish) {
			covers.add("lastQuarter");
		}
		const cut = `cut it to ${wIn}-${lIn} ${after}`;
		const clause =
			loserLedAt === regPeriods
				? `${cut} and went up ${lMax} in the ${last}`
				: loserLedAt !== undefined
					? `went up ${lMax} in the ${ordinal(loserLedAt)} but trailed ${wIn}-${lIn} ${after}`
					: tie && tiedAt !== undefined && tiedAt < regPeriods
						? `tied it at ${tie.pts} in the ${ordinal(tiedAt)}`
						: tie && tiedAt === regPeriods
							? `${cut} and tied it at ${tie.pts} in the ${last}`
							: cut;
		// "Went up 5 in the fourth. The Clippers won the fourth 31-25": the
		// quarter, once it has been named.
		const fin =
			finish && clause.includes(` the ${last}`)
				? finish.replace(` ${last}`, " quarter")
				: finish;
		// One "but" to a sentence.
		const tail =
			fin && !clause.includes(" but ") && !peak ? `, but ${fin}` : "";
		const lRun =
			run && !run.winner && run.period > halfN && run.end < regPeriods
				? run
				: undefined;
		if (peak) {
			// The halftime line was about them, unless it led with the
			// losers' early lead.
			const they = covers.has("loserPeak") ? W : "They";
			out.push(
				`${they} led by as many as ${wMax} in ${when(wMaxAt)}, but ${l} ${clause.replace(" but trailed", " and trailed")}.`,
			);
		} else if (lRun) {
			// Their run is how they got back in it.
			covers.add("run");
			out.push(
				`${L} ran off ${lRun.pts} straight points ${runAt(lRun)} and ${clause}${tail}.`,
			);
		} else {
			out.push(`${L} ${clause}${tail}.`);
		}
		if (fin && !tail) {
			out.push(`${cap(fin)}.`);
		}
		return { sentences: out, covers };
	}

	// Decided: what the second half added.
	const parts: string[] = [];
	if (w2 - l2 >= 8) {
		covers.add("secondHalf");
		parts.push(`outscored ${l} ${w2}-${l2}`);
	}
	if (run && run.winner && run.period > halfN) {
		covers.add("run");
		parts.push(`ran off ${run.pts} straight points ${runAt(run)}`);
	}
	if (
		!covers.has("winnerPeak") &&
		wMaxAt !== undefined &&
		wMaxAt > halfN &&
		wMax >= Math.max(up, shape.margin) + 3
	) {
		covers.add("winnerPeak");
		parts.push(`led by as many as ${wMax}`);
	}
	if (parts.length > 0) {
		// The sentence before was about the winners, so they carry on as
		// "they": "The Lakers were up 56-44 at the break. From there, the
		// Lakers led by as many as 17" names them twice running.
		const aboutWinner = !covers.has("loserPeak");
		const onlyPeak = parts.length === 1 && covers.has("winnerPeak");
		out.push(
			onlyPeak
				? // The commonest case - nothing to add but how big it got - and
					// the frame a slate repeated most, so it has shapes of its own.
					pick(
						rng,
						[
							`${pick(rng, ["After the break", "In the second half", "From there"], "story:after")}, ${aboutWinner ? "they" : w} led by as many as ${wMax}.`,
							`The lead reached ${wMax} in the second half.`,
							`After halftime the lead reached ${wMax}.`,
							`${aboutWinner ? "They" : W} pushed on after the break and led by as many as ${wMax}.`,
						],
						"story:peakOnly",
					)
				: `${pick(rng, ["After the break", "In the second half", "From there"], "story:after")}, ${aboutWinner ? "they" : w} ${series(parts)}.`,
		);
	} else if (l2 - w2 >= 8) {
		// The lead the second half ate into: "The Magic were ahead 63-39 at
		// the half" and nothing after it, over a 106-97 final.
		covers.add("secondHalf");
		const lateRun =
			run && !run.winner && run.period > halfN && run.end <= regPeriods
				? run
				: undefined;
		if (lateRun) {
			covers.add("run");
		}
		// The lead's peak after the break, when it outran the halftime one,
		// goes with it, in order: "The Bulls won the second half 60-40. The
		// Nuggets led by as many as 35." told the 35 after the half that cut
		// into it.
		const peakAfter =
			!covers.has("winnerPeak") &&
			wMaxAt !== undefined &&
			wMaxAt > halfN &&
			wMax >= up + 3;
		// A peak a basket past the halftime lead is that lead: "led 61-28 at
		// halftime... The Nuggets led by as many as 35" said it twice.
		if (peakAfter || wMax <= up + 2) {
			covers.add("winnerPeak");
		}
		out.push(
			peakAfter
				? `${W} led by as many as ${wMax} in ${when(wMaxAt)}, but ${l} won the second half ${l2}-${w2}${lateRun ? ` and ran off ${lateRun.pts} straight points ${runAt(lateRun)}` : ""}.`
				: lateRun
					? `${L} won the second half ${l2}-${w2} and ran off ${lateRun.pts} straight points ${runAt(lateRun)}.`
					: pick(
							rng,
							[
								`${L} won the second half ${l2}-${w2}.`,
								`${L} outscored ${w} ${l2}-${w2} after the break.`,
							],
							"story:loserHalf",
						),
		);
	} else if (aheadForGoodByHalf && shape.margin >= 8) {
		// "Never got back in front" says they had been; a side that never led
		// at all gets the plain fact, unless the lede already said it.
		if (lMax > 0) {
			out.push(
				pick(
					rng,
					[
						`${cap(l)} never got back in front.`,
						`${W} did not trail again.`,
						`${W} led the rest of the way.`,
					],
					"story:held",
				),
			);
		} else if (!opts.wireTold) {
			out.push(
				pick(rng, [`${L} never led.`, `${W} never trailed.`], "story:neverLed"),
			);
		}
	} else if (run && !run.winner && run.period > halfN && aheadForGoodByHalf) {
		// The losers' push that came up short.
		covers.add("run");
		out.push(
			`${L} ran off ${run.pts} straight points ${runAt(run)} but could not get ${
				lMax > 0 ? "back in front" : "in front"
			}.`,
		);
	}
	return { sentences: out, covers };
};

// WHICH RUN OF THE WINNERS' IS NEWS. The game's longest run is 10 or 11
// points in a typical game, and telling it in every recap made "ran off 11
// straight points in the second" the commonest sentence on a page. It is
// told when it is 15 or more; at 12 or more when its quarter broke a close
// game open or took the winners from behind to in front; and at 10 or more
// when it came in a last quarter the game was won in.
const winnersRunIsNews = (
	shape: Shape,
	r: { pts: number; period: number; endPeriod?: number },
): boolean => {
	const marginAfter = (n: number) =>
		shape.wq.slice(0, n).reduce((a, b) => a + b, 0) -
		shape.lq.slice(0, n).reduce((a, b) => a + b, 0);
	const end = r.endPeriod ?? r.period;
	const before = marginAfter(r.period - 1);
	const after = marginAfter(end);
	return (
		r.pts >= 15 ||
		(r.pts >= 12 &&
			((before <= 4 && after >= 6) || (before < 0 && after > 0))) ||
		(r.pts >= 10 &&
			end === shape.regPeriods &&
			r.period === shape.regPeriods &&
			shape.ot === 0 &&
			marginAfter(shape.regPeriods - 1) <= 0)
	);
};

// The scoreboard's overall character: a shootout or a defensive grind.
const combinedNote = (shape: Shape, rng: () => number): string | undefined => {
	const total = shape.winner.pts + shape.loser.pts;
	// A game one side pulled away in isn't a grind, whatever the final total -
	// "a rock fight from the opening tip" next to "pulled away after halftime"
	// argues with itself.
	if (shape.margin >= 12) {
		return undefined;
	}
	if (shape.ot === 0 && shape.regPeriods >= 4) {
		if (total >= 240) {
			return `The teams combined for ${total} points in an up-and-down affair.`;
		}
		// "Neither offense got going" must be true of BOTH offenses. A 94-65
		// blowout where the winner shot 50%+ is a defensive mauling of one team,
		// not a grind - so the winner must also have scored low and shot poorly.
		if (
			total <= 165 &&
			shape.winner.pts <= 90 &&
			teamStats(shape.winner).fgp < 47
		) {
			return pick(rng, [
				`Neither offense could find a rhythm, the teams combining for just ${total} points.`,
				`Points were hard to come by all night: ${total} of them between the two teams.`,
				`It was a rock fight from the opening tip.`,
				`Neither team could get anything going, and it showed on the scoreboard.`,
			]);
		}
	}
	return undefined;
};

// The winner's supporting cast - the second (and maybe third) big contributor.
const supportSentence = (
	shape: Shape,
	star: RecapPlayer,
	rng: () => number,
	// A clutch hero whose full line the winning-shot sentence already printed.
	alreadyTold?: string,
	// A teammate the headline named beside the star. "Evan Hayes and Darius
	// Hayes post double-doubles" sat over a body whose supporting cast was
	// the two next scorers, and Darius Hayes was never mentioned.
	headlined?: RecapPlayer,
): string | undefined => {
	const cast = supportingCast(shape.winner.players, star).filter(
		(p) =>
			p.name !== alreadyTold &&
			(p === headlined ||
				p.pts >= 12 ||
				doubleCategories(p).length >= 2 ||
				p.reb >= 12),
	);
	if (cast.length === 0) {
		return undefined;
	}
	// The man the box-score card features goes first when the lede went to
	// somebody else: a 13-13-8 gave the lede to a 22-point scorer and then
	// vanished from the piece, a fraction of an impact point behind two
	// teammates in a sentence with room for two.
	const featured = bestOf(shape.winner.players);
	const at = featured ? cast.indexOf(featured) : -1;
	if (featured && at > 0) {
		cast.splice(at, 1);
		cast.unshift(featured);
	}
	// And the man who outscored the star is never left out for a better
	// all-round line: "Anthony Jackson chipped in 17 points, and Jaden
	// Jackson supplied 15" said nothing of Miles Jackson's team-high 18.
	const scorer = [...shape.winner.players].sort((a, b) => b.pts - a.pts)[0];
	const scorerAt = scorer ? cast.indexOf(scorer) : -1;
	if (scorer && scorerAt > 1 && scorer.pts > star.pts) {
		cast.splice(scorerAt, 1);
		cast.splice(1, 0, scorer);
	}
	// Impact decides WHO is worth a mention, but when the sentence only shows
	// points, the order has to follow them: "Keegan Mathis threw in 13 points,
	// and Quentin Mathis posted 18" reads as though the piece cannot count.
	const [first, next] = cast;
	if (
		first &&
		next &&
		doubleCategories(first).length < 2 &&
		doubleCategories(next).length < 2 &&
		next.pts > first.pts
	) {
		cast[0] = next;
		cast[1] = first;
	}
	// The headline's man takes one of the two places - never the one held
	// by a teammate who outscored the star.
	const hAt = headlined ? cast.indexOf(headlined) : -1;
	if (headlined && hAt > 1) {
		cast.splice(hAt, 1);
		const slot = cast[1] && cast[1] === scorer && scorer.pts > star.pts ? 0 : 1;
		const bumped = cast[slot];
		cast[slot] = headlined;
		if (bumped) {
			cast.splice(2, 0, bumped);
		}
	}
	const second = cast[0]!;

	// THE MAN WHO ACTUALLY LED THE SCORING. The story player is chosen on a
	// whole line - rebounds, assists and steals all count - so the winner's
	// leading scorer is frequently somebody else, and "chipped in 33" under a
	// lead of 22 reads as though the piece cannot count. Whoever it is, in
	// whichever slot, gets a verb that matches his line and the fact is stated.
	// Measured over 359 engine-simmed recaps this was 24 of them.
	const topScorer = [...shape.winner.players].sort((a, b) => b.pts - a.pts)[0];
	const topPts = topScorer?.pts ?? 0;
	// A team high two men shared is neither's alone: "Anthony Jackson pitched
	// in with 18 points, and Cade Jackson was the leading scorer with 18
	// points."
	const topCount = shape.winner.players.filter((p) => p.pts === topPts).length;
	const sharedTop = topCount > 1;
	// Any margin at all: "Keegan Green put up 27... Lonnie Green tacked on 31"
	// reads as though the piece cannot count, however small the gap. Three or
	// more level at the top is nobody's lead.
	const outscoredStar = (p: RecapPlayer) =>
		p.pts === topPts && topCount <= 2 && p.pts > star.pts && p.pts >= 14;

	const topScorerText = (p: RecapPlayer) =>
		sharedTop
			? `${p.name} shared the team high with ${statPhrase(p)}`
			: pickSentence(
					rng,
					[
						`${p.name} led the scoring with ${statPhrase(p)}`,
						`${p.name} led the ${nick(shape.winner)} with ${statPhrase(p)}`,
						// Not "No one scored more than X, who had 16 points, and Y
						// supplied 14": the long way round to a team high, and it
						// read as a tie that was not one.
						`${p.name} had a team-high ${statPhrase(p)}`,
						`${p.name} was the leading scorer with ${statPhrase(p)}`,
					],
					"supportTopScorer",
				);
	// Both of them in this sentence: one clause, "apiece".
	const partner = cast[1];
	if (
		sharedTop &&
		partner &&
		outscoredStar(second) &&
		outscoredStar(partner) &&
		doubleCategories(second).length < 2 &&
		doubleCategories(partner).length < 2
	) {
		return `${second.name} and ${partner.name} led the scoring with ${topPts} points apiece.`;
	}

	const ddw = doubleWord(doubleCategories(second).length);
	const secondText = outscoredStar(second)
		? topScorerText(second)
		: ddw
			? pickSentence(
					rng,
					[
						`${second.name} added a ${ddw} with ${statPhrase(second)}`,
						// "Of his own" says the star had one too: "Evan Hayes had a
						// double-double of his own" under a 21-point lede had nothing
						// to be his own beside.
						...(doubleWord(doubleCategories(star).length) === ddw
							? [
									`${second.name} had a ${ddw} of his own, ${statPhrase(second)}`,
								]
							: []),
						// A triple-double is always called one; "went for 15 points,
						// 10 rebounds, and 10 assists" buried it.
						...(doubleCategories(second).length >= 3
							? []
							: [`${second.name} went for ${statPhrase(second)}`]),
						`${second.name} ${claimVerb("chipped in")} a ${ddw}, ${statPhrase(second)}`,
					],
					"supportDouble",
				)
			: `${second.name} ${lineVerb(second, rng)} ${statPhrase(second)}`;

	const third = cast[1];
	if (third && (third.pts >= 14 || doubleCategories(third).length >= 2)) {
		const thirdDdw = doubleWord(doubleCategories(third).length);
		const thirdText = outscoredStar(third)
			? topScorerText(third)
			: thirdDdw
				? // The full line, and named: trimmed to one extra, a 15-10-10
					// came out as "15 points and 10 rebounds".
					`${third.name} ${scoredVerb(rng)} a ${thirdDdw}, ${statPhrase(third)}`
				: `${third.name} ${lineVerb(third, rng)} ${statPhrase(third, 1)}`;
		// Not always "A verbed X, and B verbed Y": nine straight recaps of one
		// team used that same two-verb frame, and a beat reader notices the
		// skeleton before the words. When both are plain scoring lines, the
		// wire's elided form takes a turn - one verb, the second man's number
		// riding on it.
		const plainSecond = statPhrase(second, 1) === plural(second.pts, "point");
		const plainThird = statPhrase(third, 1) === plural(third.pts, "point");
		if (
			!outscoredStar(second) &&
			!outscoredStar(third) &&
			!ddw &&
			!thirdDdw &&
			plainSecond &&
			plainThird &&
			rng() < 0.45
		) {
			return `${second.name} ${scoredVerb(rng)} ${second.pts} points and ${third.name} ${third.pts}.`;
		}
		return `${secondText}, and ${thirdText}.`;
	}
	return `${secondText}.`;
};

// The losing side: their leader, and (when there's a clear culprit) why it wasn't
// enough.
const loserSentence = (
	shape: Shape,
	rng: () => number,
	// Facts the winner's stat note already spent. Saying "the Nets turned 19
	// Magic turnovers into offense" and then "the Magic were undone by 19
	// turnovers" is the same fact twice in three sentences.
	spent: Set<StatFact> = new Set(),
	// The headline already gave this man's line.
	skipLeader = false,
): string | undefined => {
	const best = bestOf(shape.loser.players);

	// A REASON IS A DIFFERENCE. "But 18 turnovers did them in" of a game the
	// winners turned it over 16 times blamed the loss on something both teams
	// did - 225 of the 418 turnover reasons in a corpus run were that. A
	// number is the reason they lost only when it is well worse than the
	// winners'.
	const lossStats = teamStats(shape.loser);
	const winStats = teamStats(shape.winner);
	const tovReason =
		lossStats.tov >= 18 &&
		lossStats.tov - winStats.tov >= 5 &&
		!spent.has("loserTov");
	const fgpReason =
		lossStats.fga >= 20 &&
		lossStats.fgp <= 40 &&
		winStats.fgp - lossStats.fgp >= 5 &&
		!spent.has("loserFgp");
	const threesReason =
		lossStats.tpa >= 25 &&
		lossStats.tpp <= 28 &&
		winStats.tpp - lossStats.tpp >= 8 &&
		!spent.has("loserThrees");

	// THE HEADLINE SUBJECT HAS TO BE IN THE STORY. Dropping him because the
	// headline "already said it" produced recaps like "Isaac Adkins' 24 and 11
	// not enough..." over a body that never mentions Adkins again - the surest
	// sign in the whole piece that nobody wrote it. Keep him, but with a fact
	// the headline did NOT print: how he shot. Failing that, hand the sentence
	// to the next man up, who at least is new information.
	if (skipLeader && best) {
		const stats = teamStats(shape.loser);
		const why = tovReason
			? `, but ${theNick(shape.loser)} gave it away ${stats.tov} times`
			: fgpReason
				? `, but ${theNick(shape.loser)} shot ${stats.fgp}% as a team`
				: "";
		// ALWAYS returns a sentence naming him. Returning undefined here is what
		// left "Marko Jaric headlined but never appears" - the headline subject
		// dropping out of his own story. Each rung adds something the headline
		// did not print, so his line is never simply restated.
		if (best.fga >= 12) {
			const forTeam = why === "" ? ` for ${theNick(shape.loser)}` : "";
			const fgp = best.fga > 0 ? (100 * best.fg) / best.fga : 0;
			return pick(
				rng,
				[
					`${best.name} got his ${best.pts} on ${best.fg}-of-${best.fga} shooting${forTeam}${why}.`,
					`${best.name} shot ${best.fg}-of-${best.fga} on the way to his ${best.pts}${forTeam}${why}.`,
					...(fgp <= 40
						? [
								`${best.name} needed ${best.fga} shots for his ${best.pts}${forTeam}${why}.`,
							]
						: []),
					...(fgp >= 55
						? [
								`${best.name} took just ${best.fga} shots to get his ${best.pts}${forTeam}${why}.`,
							]
						: []),
				],
				"loserHeadlinedShooting",
			);
		}
		const second = supportingCast(shape.loser.players, best)[0];
		if (second && (second.pts >= 14 || doubleCategories(second).length >= 2)) {
			return `${best.name} got what help there was from ${second.name}, who had ${statPhrase(second)}${why}.`;
		}
		return `${best.name} led ${theNick(shape.loser)} in scoring${why === "" ? ", to no avail" : why}.`;
	}

	// The story pick is made on the whole line, so the losing side's "best" can
	// be a 12-point, 9-rebound, 3-steal night - too quiet for this sentence,
	// which then said nothing, and the man who actually scored 20 for them
	// turned up two paragraphs later as an afterthought. When the pick is
	// quiet, the sentence goes to the scorer.
	const topScorer = [...shape.loser.players].sort((a, b) => b.pts - a.pts)[0];
	const quiet = (p: RecapPlayer | undefined) =>
		!p || (p.pts < 18 && doubleCategories(p).length < 2);
	const leader = skipLeader
		? undefined
		: quiet(best) && topScorer && topScorer.pts >= 18
			? topScorer
			: best;
	const stats = teamStats(shape.loser);
	// "reason" is appended after "...led the Loser", so it uses a pronoun rather
	// than repeating the team name.
	let reason = "";
	if (tovReason) {
		reason = pick(
			rng,
			[
				`, but ${stats.tov} turnovers did them in`,
				`, but they coughed it up ${stats.tov} times`,
				`, but ${stats.tov} turnovers proved costly`,
				`, though ${stats.tov} giveaways kept undoing the good work`,
				`, and ${stats.tov} turnovers were far too many`,
				`, on a night they gave it away ${stats.tov} times`,
			],
			"loserTov",
		);
	} else if (fgpReason) {
		reason = pick(
			rng,
			[
				`, but they shot just ${stats.fgp}% as a team`,
				`, but the team shot ${stats.fgp}% from the floor`,
				`, on a ${stats.fgp}% shooting night for the team`,
			],
			"loserFgp",
		);
	} else if (threesReason) {
		// Why they lost, in the same sentence as who tried hardest to stop it,
		// instead of a bare "The Nets shot 25% from three on 32 attempts" two
		// paragraphs away.
		spent.add("loserThrees");
		reason = pick(
			rng,
			[
				`, but they went ${stats.tp}-of-${stats.tpa} from three`,
				`, though the threes would not fall - ${stats.tp}-of-${stats.tpa}`,
				`, on a night they shot ${stats.tpp}% from deep`,
			],
			"loserThrees",
		);
	}

	if (leader && (leader.pts >= 18 || doubleCategories(leader).length >= 2)) {
		const verb = pick(rng, [
			"led",
			"paced",
			"topped",
			"headed",
			"fronted",
			"was the best of",
		]);
		// A 40-point night on the losing side is its own story, not "the best
		// the Nets could offer".
		if (leader.pts >= 40) {
			const them = theNick(shape.loser);
			return pickSentence(
				rng,
				[
					`${leader.name} poured in ${leader.pts} for ${them}${reason || ", and it still was not enough"}.`,
					`${cap(them)} got ${plural(leader.pts, "point")} from ${leader.name}${reason || " and lost anyway"}.`,
					`${leader.name} went for ${statPhrase(leader)} in a losing cause${reason}.`,
				],
				"loserBig",
			);
		}
		// A 19-rebound night is not an ordinary double-double, and burying it in
		// "(19 points and 19 rebounds)" reads like it was.
		if (leader.reb >= 18) {
			return `${leader.name} was everywhere on the glass for ${theNick(
				shape.loser,
			)}, pulling down ${leader.reb} rebounds to go with ${plural(
				leader.pts,
				"point",
			)}${reason}.`;
		}
		const ddw = doubleWord(doubleCategories(leader).length);
		const leaderLine = ddw
			? `${poss(leader.name)} ${ddw} (${statPhrase(leader)})`
			: `${poss(leader.name)} ${statPhrase(leader)}`;
		// Bracketed, not dashed: "leaned on Scoot Carter for a double-double -
		// 17 points and 11 rebounds, and Xavier Carter contributed 16" ran the
		// next man into his line.
		const line = ddw ? `a ${ddw} (${statPhrase(leader)})` : statPhrase(leader);
		const them = theNick(shape.loser);

		// SHAPES, not synonyms. Rotating six verbs through one frame - "X's 22
		// points led/paced/topped/headed the Y" - is the single most obvious tell
		// that nobody wrote this: every losing team in the league gets the same
		// sentence with the nouns swapped. These put the player, the team and the
		// numbers in genuinely different places.
		// "28 points and 9 rebounds WERE the best of the Grizzlies"; a lone
		// "20 points was" is a quantity and stays singular - and so is "X's
		// double-double (28 points and 12 rebounds)", whatever is inside the
		// brackets.
		const agreed =
			verb === "was the best of" && !ddw && / and /.test(leaderLine)
				? "were the best of"
				: verb;
		// With no reason clause to carry, the losers' second man rides along:
		// "Obi Brooks' 18 points led the Celtics, and Keegan Brooks had 16"
		// over two sentences of the same shape in a row.
		// The next scorer down, never one who outscored the man just called
		// their leader, and with a verb the ledger has not spent.
		const second =
			reason === ""
				? [...shape.loser.players]
						.filter((p) => p !== leader && p.pts >= 14 && p.pts <= leader.pts)
						.sort((a, b) => b.pts - a.pts)[0]
				: undefined;
		// The shape is picked WITHOUT the tail: with the second man's verb
		// baked into every candidate, the ledger saw all four as spent and the
		// pick could no longer steer away from a "finished with" already used.
		// The dash form leaves the man dangling after his numbers - "got a
		// double-double - 14 points and 10 assists from Tyrese Ellis" - so
		// these frames name him first or bracket the line. A double-double's
		// numbers say what it was; a triple-double is always called one.
		const triple = doubleCategories(leader).length >= 3;
		// Level with the next man and nothing else to his line: "Scoot Ellis'
		// 21 points was the best of the Warriors, and Xavier Ellis supplied
		// 21 points" crowned one of two equals.
		if (
			second &&
			second.pts === leader.pts &&
			statPhrase(leader) === plural(leader.pts, "point") &&
			statPhrase(second, 1) === plural(second.pts, "point")
		) {
			return `${cap(them)} got ${leader.pts} apiece from ${leader.name} and ${second.name}.`;
		}
		const gotLine = !ddw
			? `${line} from ${leader.name}`
			: triple
				? `a ${ddw} from ${leader.name} (${statPhrase(leader)})`
				: `${statPhrase(leader)} from ${leader.name}`;
		const base = pickSentence(
			rng,
			[
				`${leaderLine} ${agreed} ${them}`,
				`${cap(them)} got ${gotLine}`,
				`${leader.name} finished with ${ddw ? `a ${ddw} (${statPhrase(leader)})` : line} for ${them}`,
				// Not "22 points from X was..." - a sentence does not open with a
				// numeral, and every one of these lines starts with one. And only
				// for a modest line: "The best the Pelicans could offer was 32
				// points and 11 assists" undersold the best line on the floor.
				...(leader.pts < 25 && !triple
					? [
							`The best ${them} could offer was ${ddw ? leaderLine : `${line} from ${leader.name}`}`,
						]
					: []),
				`For ${them}, it was ${leader.name} with ${line}`,
				`${cap(them)} leaned on ${leader.name} for ${line}`,
			],
			"loserShape",
		);
		const tail =
			reason ||
			(second
				? `, and ${second.name} ${lineVerb(second, rng)} ${statPhrase(second, 1)}`
				: "");
		return `${base}${tail}.`;
	}
	// No standout to hang it on. The leading scorer still gets his name in -
	// a recap that never says who led the losers reads as if nobody did -
	// unless the top line is so quiet it would embarrass him.
	if (!skipLeader && topScorer && topScorer.pts >= 12) {
		const them = theNick(shape.loser);
		// After "got no more than 12 from anyone" the reason is more of the
		// same, not a contrast: "..., but the team shot 26.7%" set two bad
		// facts against each other, and "kept undoing the good work" had no
		// good work to undo.
		const more = tovReason
			? `, and they turned it over ${stats.tov} times`
			: fgpReason
				? `, and they shot ${stats.fgp}% as a team`
				: threesReason
					? `, and they went ${stats.tp}-of-${stats.tpa} from three`
					: "";
		// A shared high is not one man's lead: "Nobody had more than Jalen
		// Hayes' 16 for the Heat... Anthony Hayes was good for 16 points."
		const level = shape.loser.players.filter(
			(p) => p !== topScorer && p.pts === topScorer.pts,
		);
		if (level.length === 1) {
			return `${cap(them)} got ${topScorer.pts} apiece from ${topScorer.name} and ${level[0]!.name}${reason}.`;
		}
		if (level.length > 1) {
			return `${cap(them)} got no more than ${topScorer.pts} from anyone${more}.`;
		}
		return pickSentence(
			rng,
			[
				`${topScorer.name} led ${them} with ${plural(topScorer.pts, "point")}${reason}.`,
				`Nobody had more than ${poss(topScorer.name)} ${topScorer.pts} for ${them}${more}.`,
				`${cap(them)} got no more than ${topScorer.pts} from anyone, ${topScorer.name} leading the way${more}.`,
			],
			"loserQuietLeader",
		);
	}
	// Name the team directly.
	if (tovReason) {
		return pick(
			rng,
			[
				`${cap(theNick(shape.loser))} were undone by ${stats.tov} turnovers.`,
				`${cap(theNick(shape.loser))} gave the ball away ${stats.tov} times.`,
				`It was turnovers that beat ${theNick(shape.loser)} - ${stats.tov} of them.`,
				`${cap(theNick(shape.loser))} could not hold on to it - ${stats.tov} giveaways.`,
			],
			"loserTovAlone",
		);
	}
	if (fgpReason) {
		return pick(
			rng,
			[
				`${cap(theNick(shape.loser))} shot just ${stats.fgp}% as a team.`,
				`${cap(theNick(shape.loser))} never found the range - ${stats.fgp}% for the game.`,
				`Shooting ${stats.fgp}% was never going to win ${theNick(shape.loser)} anything.`,
			],
			"loserColdTeam",
		);
	}
	return undefined;
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
					`It snapped ${poss(theNick(shape.loser))} ${numWord(run)}-game winning streak.`,
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
						`It snapped ${poss(theNick(shape.loser))} ${numWord(run)}-game winning streak.`,
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
	if (playoffs) {
		return star || rng() < 0.4;
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
const hurtIsNews = (p: RecapPlayer): boolean =>
	p.starter === true ||
	(p.seasonAvg ? p.seasonAvg.min >= 15 : p.starter === undefined);

// "about five games", but "the next game" - "about a game" is no estimate.
// And not "about 175 games": past what is left of the regular season it is
// "the rest of the season" ("the rest of the regular season" when it ends
// near it), and in the playoffs a month is the rest of them.
const gamesOut = (
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
const gamesLeft = (game: RecapGame, t: RecapTeam): number | undefined => {
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
		return pick(
			rng,
			[
				`${star.name} ${against} his ${fmtPct(avg.pts)}-point average.`,
				...(beat
					? [
							`${star.name} had been averaging ${fmtPct(avg.pts)} points a game, and he ${beat}.`,
						]
					: []),
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
				`${cap(numWord(seasons))} seasons in, ${star.name} turned back the clock.`,
				`It was a night from ${poss(star.name)} prime, ${numWord(seasons)} seasons into his career.`,
			],
			"careerVeteran",
		);
	}
	return undefined;
};

// The three-point battle, when one side clearly won it. Volume from deep is the
// single biggest swing factor in a modern box score and nothing was reading it
// except a bare "knocked down 14 threes".
const threeNote = (
	shape: Shape,
	rng: () => number,
	// Paragraph one already spent the night's three-point line.
	told = false,
): string | undefined => {
	const w = teamStats(shape.winner);
	const l = teamStats(shape.loser);
	if (told || w.tpa < 10 || l.tpa < 10) {
		return undefined;
	}
	const diff = w.tp - l.tp;
	if (diff >= 7) {
		return pick(
			rng,
			[
				`The difference was behind the arc: ${w.tp}-of-${w.tpa} for ${theNick(
					shape.winner,
				)}, ${l.tp}-of-${l.tpa} for ${theNick(shape.loser)}.`,
				`${cap(theNick(shape.winner))} made ${diff} more threes than ${theNick(
					shape.loser,
				)}, ${w.tp} to ${l.tp}.`,
				`From deep it was no contest - ${w.tp} threes to ${l.tp}.`,
			],
			"threeGap",
		);
	}
	if (l.tpa >= 28 && l.tpp <= 26) {
		return pick(
			rng,
			[
				`${cap(theNick(shape.loser))} went ${l.tp}-of-${l.tpa} from three.`,
				`${cap(theNick(shape.loser))} missed ${l.tpa - l.tp} of their ${l.tpa} threes.`,
				`${cap(theNick(shape.loser))} shot ${l.tpp}% from three on ${l.tpa} attempts.`,
				`${cap(theNick(shape.loser))} kept firing from outside and kept missing: ${l.tp}-of-${l.tpa}.`,
			],
			"loserColdThrees",
		);
	}
	return undefined;
};

// Who got to the line. A lopsided free-throw night is a real explanation for a
// close result and it was only ever mentioned from the winner's side.
const freeThrowNote = (
	shape: Shape,
	rng: () => number,
	// Same for the free-throw disparity: "made 26 free throws to 11" up top and
	// "shot 31 free throws to 14" down here are one fact with two numbers, which
	// reads worse than either alone.
	told = false,
): string | undefined => {
	const w = teamStats(shape.winner);
	const l = teamStats(shape.loser);
	if (told || w.fta + l.fta < 20) {
		return undefined;
	}
	if (w.fta - l.fta >= 14) {
		return pick(
			rng,
			[
				`${cap(theNick(shape.winner))} shot ${w.fta} free throws to ${l.fta}.`,
				`The whistle went one way: ${w.fta} attempts from the line for ${theNick(
					shape.winner,
				)}, ${l.fta} for ${theNick(shape.loser)}.`,
				`${cap(theNick(shape.winner))} got to the line ${
					w.fta >= l.fta * 2 ? "more than twice as often" : "far more often"
				}, ${w.fta} attempts to ${l.fta}.`,
				`${cap(theNick(shape.loser))} were beaten at the line, ${l.fta} attempts to ${w.fta}.`,
			],
			"ftGap",
		);
	}
	if (l.fta - w.fta >= 14 && shape.margin <= 10) {
		return pick(
			rng,
			[
				`${cap(theNick(shape.loser))} had the better of the whistle - ${l.fta} free throws to ${w.fta} - and still lost.`,
				`${cap(theNick(shape.loser))} shot ${l.fta} free throws to ${w.fta} and lost anyway.`,
				`${cap(theNick(shape.loser))} shot ${l.fta} free throws to ${w.fta} and still lost.`,
			],
			"ftGapLoser",
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
	if (w.blk >= 9) {
		options.push(
			pickSentence(
				rng,
				[
					`${cap(theNick(shape.winner))} blocked ${w.blk} shots.`,
					`${cap(theNick(shape.winner))} sent back ${w.blk} shots.`,
					`Nothing came easy inside - ${w.blk} ${nick(shape.winner)} blocks.`,
					`${cap(theNick(shape.winner))} finished with ${w.blk} blocks.`,
				],
				"teamBlk",
			),
		);
	}
	if (bigBlocker) {
		options.push(
			`${bigBlocker.name} protected the rim with ${plural(bigBlocker.blk, "block")}.`,
		);
	}
	if (bigThief) {
		options.push(
			`${bigThief.name} ${takeVerb(rng, ["came up with", "recorded", "collected"], "thiefVerb")} ${plural(bigThief.stl, "steal")} for ${thiefTeam}.`,
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

// The loser's second man. Only the losing side's single best player ever got
// named, so a 24-point night from their number two vanished entirely.
const loserSupportNote = (
	shape: Shape,
	rng: () => number,
	// Anyone the recap has already named. Without this the note kept
	// re-introducing the losing side's best player, who the loser sentence had
	// just finished describing.
	said: Set<string>,
): string | undefined => {
	// The losing side's man was named on his overall line, not purely on points,
	// so the next-best scorer can have MORE points than him. "Odom's 22 led the
	// Grizzlies... Van Horn added 23 in defeat" is a flat contradiction, so a
	// candidate who outscored the man already called the leader is skipped.
	const namedPts = shape.loser.players
		.filter((p) => said.has(p.name))
		.map((p) => p.pts);
	const ceiling = namedPts.length > 0 ? Math.max(...namedPts) : Infinity;

	const second = [...shape.loser.players]
		.sort((a, b) => b.pts - a.pts)
		.find(
			(p) =>
				!said.has(p.name) &&
				p.pts <= ceiling &&
				(p.pts >= 16 || doubleCategories(p).length >= 2),
		);
	if (!second) {
		return undefined;
	}
	said.add(second.name);
	// "in defeat" on its own turned up in most games on a slate, so the tail
	// rotates too. Deliberately ONE shape: the verb and tail together give this
	// sentence more distinct forms than a handful of alternative shapes would,
	// and shapes without that variation collapse to a single frame each.
	return `${second.name} ${lineVerb(second, rng)} ${statPhrase(second, 1)} ${pick(
		rng,
		["in defeat", "in the loss", "in a losing cause", "for the losing side"],
		"loserTail",
	)}.`;
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
				`${cap(theNick(shape.winner))} were supposed to win this one comfortably, and did not.`,
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

// How the two benches / rotations compared in depth terms.
const balanceNote = (
	shape: Shape,
	rng: () => number,
	// Paragraph 2 has its own "N players in double figures" and "piled up N
	// assists" sentences. Restating either as a comparison two sentences later
	// is the same number twice, and it reads as though the writer forgot.
	told: { dblFig: boolean; assists: boolean } = {
		dblFig: false,
		assists: false,
	},
): string | undefined => {
	const w = teamStats(shape.winner);
	const l = teamStats(shape.loser);
	// Five in double figures is the median winner; six against three is a
	// difference.
	if (!told.dblFig && w.dblFig >= 6 && w.dblFig - l.dblFig >= 3) {
		return pick(
			rng,
			[
				`${cap(theNick(shape.winner))} got double figures out of ${numWord(w.dblFig)} players to ${numWord(l.dblFig)} for ${theNick(
					shape.loser,
				)}.`,
				`The scoring was spread around - ${numWord(w.dblFig)} ${nick(shape.winner)} in double figures.`,
			],
			"balance",
		);
	}
	if (!told.assists && w.ast >= 26 && w.ast - l.ast >= 10) {
		return pick(
			rng,
			[
				`${cap(theNick(shape.winner))} assisted on far more of their baskets, ${w.ast} to ${l.ast}.`,
				`${cap(theNick(shape.winner))} moved the ball far better, ${w.ast} assists to ${l.ast}.`,
				`The ball found the open man all night for ${theNick(shape.winner)} - ${w.ast} assists to ${l.ast}.`,
				`${cap(theNick(shape.winner))} out-assisted ${theNick(shape.loser)} ${w.ast} to ${l.ast}.`,
			],
			"balanceAst",
		);
	}
	return undefined;
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

	const headline = as.mvp
		? pick(
				rng,
				[
					`${as.mvp} takes home All-Star Game MVP`,
					`${as.mvp} is the All-Star Game MVP`,
					`${as.mvp} steals the show at the All-Star Game`,
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
	const mvpLine = as.mvp
		? [...winner.players, ...loser.players].find((p) => p.name === as.mvp)
		: undefined;
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
		sentences.push(
			`${pick(
				rng,
				["Over the weekend", "Elsewhere on the weekend", "On the undercard"],
				"allStarExtras",
			)}, ${extras.join("; ")}.`,
		);
	}

	return `**${headline}**\n\n${sentences.join(" ")}`;
};

// --- Entry point: one game -----------------------------------------------------

// --- How it unfolded: the sim's score log, when the game recorded one ---------
//
// Everything above reads the box score and the quarter scores, which can say
// a game was close and not how. These read the flow summary
// (common/gameFlow.ts): who took the lead for good and when, the last tie,
// how many times it changed hands, the run that turned it, the lead the loser
// let go. Each says nothing on a game from before the log existed.

const sideOf = (game: RecapGame, t: RecapTeam): GameFlowSide =>
	game.teams[0].tid === t.tid ? 0 : 1;

const nameOfPid = (
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

// Where a run happened: "in the second", or across the break it spanned -
// a 14-0 run that began with no time left in the first was all second
// quarter, and "in the first" was wrong.
const runSpan = (start: number, end: number, regPeriods: number): string =>
	end === start
		? `in ${periodTag(start, regPeriods)}`
		: end > regPeriods
			? start > regPeriods
				? `across ${periodTag(start, regPeriods)} and ${periodTag(end, regPeriods)}`
				: "across the end of regulation and overtime"
			: start === regPeriods / 2 && end === start + 1
				? "spanning halftime"
				: `across the ${ordinal(start)} and ${ordinal(end)}`;

// How a close game finished. The go-ahead score that held, or failing that
// the last tie, and the count of lead changes when there were a lot of them.
const finishNote = (
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
	// comeback it is the turn of the game, wherever it came after halftime.
	const halfway = Math.ceil(shape.regPeriods / 2);
	if (
		last &&
		last.side === wSide &&
		(inFinal(last.period) || (comeback && last.period > halfway)) &&
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
	if (flow.leadChanges >= 10) {
		out.push(
			flow.ties > 0
				? pick(
						rng,
						[
							`There were ${flow.leadChanges} lead changes and ${flow.ties} ${
								flow.ties === 1 ? "tie" : "ties"
							}.`,
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

// How big the rout got. Every wire recap of a blowout says "led by as many
// as 31"; the engine had the number and only ever used the losers' side of
// it. Worth a sentence when the biggest lead outran the final margin, since
// the final alone says nothing about a lead that was cut in garbage time.
const bigLeadNote = (
	game: RecapGame,
	shape: Shape,
	rng: () => number,
): string | undefined => {
	const flow = game.flow;
	if (!flow) {
		return undefined;
	}
	const lead = flow.maxLead[sideOf(game, shape.winner)];
	const W = cap(theNick(shape.winner));
	// The lead that nearly got away: up 19 at the half, home by six.
	if (shape.margin <= 8 && lead >= 15 && lead >= shape.margin + 9) {
		// A lead that was GONE - tied or lost in the final period and won
		// back - was not held on to. "Led by as many as 15 and had to hold on"
		// closed a game won by a three at the buzzer after a tie.
		const last = flow.lastLead;
		const tie = flow.lastTie;
		// Any tie, or the winners retaking the lead, in the last period: a
		// 25-point lead that was level at 102 with 3:39 left was not "held".
		const lateFinal = (e: { period: number; clock: number } | undefined) =>
			e !== undefined && e.period >= shape.regPeriods;
		if (lateFinal(last) || lateFinal(tie) || shape.ot > 0) {
			return pick(
				rng,
				[
					`${W} let ${aNum(lead)}-point lead get away and still won.`,
					`${W} blew ${aNum(lead)}-point lead and won anyway.`,
				],
				"blownThenWon",
			);
		}
		// "Very nearly let it go" and "had to hold on" are a game that got
		// within a basket or so; "had led by 28, and very nearly let it go"
		// closed a seven-point win.
		return pick(
			rng,
			[
				...(shape.margin <= 4
					? [
							`${W} led by as many as ${lead} and had to hold on.`,
							`${W} had led by ${lead}, and very nearly let it go.`,
						]
					: []),
				`A lead that reached ${lead} was down to ${shape.margin} at the end.`,
			],
			"nearlyBlown",
		);
	}
	if (shape.margin < 12 || lead < 20 || lead < shape.margin + 6) {
		return undefined;
	}
	return pick(
		rng,
		[
			`${W} led by as many as ${lead}.`,
			`The lead reached ${lead} at one point.`,
			`${W} were up by ${lead} at their biggest.`,
		],
		"bigLead",
	);
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

// The lead the loser let go: the biggest they held, or the one they had with
// two minutes left.
const blownLeadNote = (
	game: RecapGame,
	shape: Shape,
	rng: () => number,
	// The comeback line already told the deficit the winner erased, which IS
	// the loser's biggest lead.
	written: string,
): string | undefined => {
	const flow = game.flow;
	if (!flow) {
		return undefined;
	}
	const lSide = sideOf(game, shape.loser);
	const L = cap(theNick(shape.loser));
	// "Fell behind by 15 early and still ran away" in the lede, and then
	// "At one point the Celtics led by 15" three sentences on.
	const comebackTold =
		/deficit|erased|comeback|stormed back|charged home|came from \d|trailed by \d+ and still|fell behind by \d|spotted the [a-z][\w ]* an? \d+-point lead|at the break|at halftime|clawing back|hold on|nearly not enough/i.test(
			written,
		);
	const lead = flow.maxLead[lSide];
	// The story's own first half may have said it already: "The Heat were up
	// by as many as 14 in the second..." and then, after the rebounding
	// note, "The Heat led by as many as 14."
	const peakTold = new RegExp(
		String.raw`${nick(shape.loser)}[^.]*\b(?:as many as|led by|up by) ${lead}\b`,
	).test(written);
	if (lead >= 12 && !comebackTold && !peakTold) {
		return pick(
			rng,
			[
				`${L} led by as many as ${lead}.`,
				`${L} had been up by ${lead} at one stage.`,
				`At one point ${theNick(shape.loser)} led by ${lead}.`,
			],
			"blownLead",
		);
	}
	const two = flow.late?.find((m) => m.clock === 120);
	// Nor when the finish already put them in front later than that.
	const laterLead =
		new RegExp(
			String.raw`${nick(shape.loser)} (?:had )?(?:led|were up|had been up) \d+-\d+ with`,
		).test(written) ||
		new RegExp(
			String.raw`${nick(shape.winner)} (?:had )?trailed \d+-\d+ with`,
		).test(written);
	if (two && !laterLead && !/two minutes/.test(written)) {
		const l = two.pts[lSide];
		const w = two.pts[1 - lSide];
		if (l !== undefined && w !== undefined && l > w) {
			const tail = shape.ot > 0 ? " in regulation" : "";
			return pick(
				rng,
				[
					`${L} were up ${l}-${w} with two minutes to go${tail}.`,
					`${L} led ${l}-${w} with two minutes left${tail} and could not close it out.`,
				],
				"lateLead",
			);
		}
	}
	return undefined;
};

// The run that turned it, or the one that was not enough.
const runNote = (
	game: RecapGame,
	shape: Shape,
	rng: () => number,
): string | undefined => {
	const run = game.flow?.run;
	if (!run) {
		return undefined;
	}
	const isWinner = run.side === sideOf(game, shape.winner);
	// A 10-0 run is most games; see winnersRunIsNews.
	if (isWinner ? !winnersRunIsNews(shape, run) : run.pts < 15) {
		return undefined;
	}
	const team = isWinner ? shape.winner : shape.loser;
	const T = cap(theNick(team));
	const end = run.endPeriod ?? run.period;
	const spans = end !== run.period;
	const at = runSpan(run.period, end, shape.regPeriods);
	// "scored 15 unanswered in the first", but "15 unanswered points
	// across the first and second".
	const unanswered = spans ? "unanswered points" : "unanswered";
	// An overtime run nobody answered is how the game ended.
	const finish = game.flow?.finish ?? [];
	if (
		isWinner &&
		run.period > shape.regPeriods &&
		finish.some((e) => e.period === run.period) &&
		!finish.some(
			(e) =>
				e.period === run.period && e.side !== run.side && e.clock < run.clock,
		)
	) {
		return `${T} scored the last ${run.pts} points.`;
	}
	// "Put them in charge" of a game they won by three is the wrong tone: in
	// a close one the run built a lead that then had to be kept.
	const close = shape.margin <= 6 && shape.ot === 0;
	// "Put them in charge", "settled it", "the game got away" all say the
	// winners led from there. A 14-0 run in the second by a side still down
	// a point at halftime did no such thing, so the quarter-end score decides
	// whether the run gets a verdict or just a count.
	const cum = (q: unknown) =>
		(Array.isArray(q) ? (q as number[]) : [])
			.slice(0, end)
			.reduce((a, b) => a + b, 0);
	const qs = shape.winner.ptsQtrs;
	// And ahead for good: a run in the second does not "settle" a game the
	// winners only went in front of for the last time in the third.
	const lastLead = game.flow?.lastLead;
	const aheadAfter =
		(Array.isArray(qs) && qs.length >= end
			? cum(shape.winner.ptsQtrs) > cum(shape.loser.ptsQtrs)
			: true) &&
		(lastLead === undefined || lastLead.period <= end);
	// A run across a break gets the count and no verdict: where the score
	// stood when it ended is not known, and "An 18-0 burst across the third
	// and fourth settled it" was said of a side still 10 down going into the
	// fourth.
	if (isWinner && (!aheadAfter || spans)) {
		return pick(
			rng,
			[
				`${T} ran off ${run.pts} straight points ${at}.`,
				`${T} scored ${run.pts} ${unanswered} ${at}.`,
			],
			"runWinnerCount",
		);
	}
	// The losers' run gets "and it still was not enough" only when it was a
	// late push that fell short. One in the first half built a lead that was
	// lost later - "The Pistons had a 19-0 run in the first, and it still was
	// not enough" called a game in the first quarter - and one that put them
	// in front going into the next quarter turned it, for a while.
	// Ahead at the end of the quarter it began in or the one it ended in.
	const aheadAt = (n: number) => {
		const sum = (q: unknown) =>
			(Array.isArray(q) ? (q as number[]) : [])
				.slice(0, n)
				.reduce((a, b) => a + b, 0);
		return (
			Array.isArray(qs) &&
			qs.length >= n &&
			sum(shape.loser.ptsQtrs) > sum(shape.winner.ptsQtrs)
		);
	};
	// Or in front at their best inside the run's quarters: an 18-0 run
	// spanning halftime took a 10-point deficit to an 8-point lead, then
	// gave it back by the end of the third.
	const lSide = sideOf(game, shape.loser);
	const lPeakAt = game.flow?.maxLeadAt?.[lSide];
	const ledDuring =
		(game.flow?.maxLead[lSide] ?? 0) > 0 &&
		lPeakAt !== undefined &&
		lPeakAt >= run.period &&
		lPeakAt <= end;
	const losersAheadAfter = aheadAt(run.period) || aheadAt(end) || ledDuring;
	// An early run that never even put them in front is a footnote to a game
	// they lost: "The Knicks ran off 16 straight points in the first. The
	// Jazz were ahead 65-56 at the half."
	if (!isWinner && run.period <= shape.regPeriods / 2 && !losersAheadAfter) {
		return undefined;
	}
	if (!isWinner && (run.period <= shape.regPeriods / 2 || losersAheadAfter)) {
		return pick(
			rng,
			[
				`${T} ran off ${run.pts} straight points ${at}.`,
				`${T} scored ${run.pts} ${unanswered} ${at}.`,
			],
			"runLoserCount",
		);
	}
	return pick(
		rng,
		isWinner
			? close
				? [
						`${T} ran off ${run.pts} straight points ${at}, and needed every one of them.`,
						`${cap(aNum(run.pts))}-0 run ${at} built the lead ${theNick(shape.winner)} spent the rest of the night protecting.`,
					]
				: [
						`${T} ran off ${run.pts} straight points ${at}.`,
						`${cap(aNum(run.pts))}-0 run ${at} put ${theNick(shape.winner)} in charge.`,
						`${T} put together ${aNum(run.pts)}-0 run ${at}.`,
						// Enough shapes that a beat reader following one team does
						// not meet "put the Hawks in charge" three times in a week:
						// per-day rotation cannot see across days, so only pool
						// size spreads a team's own recaps out.
						`${T} scored ${run.pts} ${unanswered} ${at}.`,
						`${cap(aNum(run.pts))}-0 burst ${at} settled it.`,
						`The game got away ${at}, ${run.pts} straight points for ${theNick(shape.winner)}.`,
					]
			: [
					`${T} had ${aNum(run.pts)}-0 run ${at}, and it still was not enough.`,
					`Even ${aNum(run.pts)}-0 run ${at} could not turn it for ${theNick(shape.loser)}.`,
				],
		isWinner ? "runWinner" : "runLoser",
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

	const headline = buildHeadline(game, shape, star, post, rng);
	const tail = ledeTail(game, shape, headline, star, rng);
	// A game decided in its last minute, on the basket that decided it, when
	// nothing else holds the lede's last slot.
	const goAhead =
		tail.text === "" ? lateGoAhead(game, shape, clutchShot(game)) : undefined;
	if (goAhead) {
		tail.text = goAhead.text;
	}

	// Paragraph 1: the result and how it happened. A game-winning shot is the
	// game's defining moment, so it always gets a beat right after the lead -
	// previously the headline could tout a buzzer-beater the body never
	// mentioned. When the shooter IS the lead star, the beat merges into the
	// lead sentence so the name isn't repeated back-to-back ("Hamilton finished
	// with 20... Hamilton won it...").
	const para1: string[] = [];
	const shot = clutchShot(game);
	let flowCovered: string | undefined;
	// Set when the clutch sentence printed the hero's full line, so the
	// supporting cast doesn't introduce him a second time.
	let heroTold: string | undefined;
	// The lede said how many reached double figures.
	let balanceTold = false;

	const monsterLoser = headline.spentLoserStar
		? bestOf(shape.loser.players)
		: undefined;
	if (monsterLoser && monsterLoser.pts >= 40) {
		// A 50-point night in a loss is the story of the game, and the headline
		// already said so - the body opening on the winner's 20-and-15 read like
		// a different story stapled to it. Open on the man himself, then let the
		// winner's star follow.
		const verb = pastTense(pick(rng, verbPool(game, shape)));
		const w = theNick(shape.winner);
		const l = theNick(shape.loser);
		para1.push(
			pick(
				rng,
				[
					`${monsterLoser.name} gave ${l} ${monsterLoser.pts} points, and ${w} beat them anyway, ${scoreTag(shape)}${tail.text}.`,
					`${monsterLoser.name} scored ${monsterLoser.pts} and it still was not enough - ${w} ${verb} ${l} ${scoreTag(shape)}${tail.text}.`,
					`On most nights ${poss(monsterLoser.name)} ${monsterLoser.pts} points win the game. On this one ${w} ${verb} ${l} ${scoreTag(shape)}${tail.text}.`,
				],
				"lede:monster-loss",
			),
			leadSentence(game, shape, star, rng, true),
		);
		if (shot && !shot.tying && shot.name !== monsterLoser.name) {
			const clutch = clutchSentence(shot, shape);
			para1.push(clutch.text);
			heroTold = clutch.told;
		}
	} else if (headline.spentStar) {
		// The headline already told the star's story. Open on the RESULT and let
		// his line follow as its own sentence, rather than writing the headline
		// again with the verbs swapped.
		const opener = resultLead(
			game,
			shape,
			rng,
			headline.spentScore,
			/straight|in a row|skid|losing streak/.test(headline.text),
			tail.text,
			star,
			headline.spentLine,
		);
		flowCovered = opener.covers;
		para1.push(opener.text);
		// The headline may already have printed his whole line ("DerMarr Johnson
		// goes for 22 points as..."), in which case repeating it as the next
		// sentence is the same numbers twice. Keep the sentence whenever it
		// brings anything new - a shooting split, a second category - and drop it
		// only when every figure in it was in the headline.
		const twin = twinOf(shape, star);
		const starSentence = twin
			? `${star.name} and ${twin.name} scored ${star.pts} points apiece.`
			: leadSentence(game, shape, star, rng, true);
		if (twin) {
			heroTold = twin.name;
		}
		const figures = (t: string) => t.match(/\d+(?:\.\d+)?/g) ?? [];
		const inHeadline = new Set(figures(headline.text));
		// He won it, and the winning-shot sentence carries his line: "Walker
		// Carter exploded for 37 points and 8 assists. Walker Carter won it
		// on a layup with 2.5 seconds left and finished with 37 points and 8
		// assists" gave the line twice in two sentences.
		const starWonIt = !!shot && !shot.tying && shot.name === star.name && !twin;
		if (opener.starFolded || starWonIt) {
			// His line is in the opener, or comes with the shot.
		} else if (
			!headline.spentLine ||
			figures(starSentence).some((n) => !inHeadline.has(n))
		) {
			para1.push(starSentence);
		} else if (star.fga >= 10) {
			// Everything in the sentence was already in the headline, so print
			// something the headline did NOT have rather than dropping him: with
			// nothing else to carry his name, the man in the headline disappeared
			// from his own story.
			// The phrasing has to match what the split SAYS. "Vince Dunn scored
			// his 16 on 5-of-15 shooting" states a poor night in the tone of a
			// good one and reads as though nobody looked at it; a bad line is
			// worth writing about, but as a bad line.
			const pct = star.fga > 0 ? star.fg / star.fga : 0;
			para1.push(
				pick(
					rng,
					pct >= 0.55
						? [
								`${star.name} scored his ${star.pts} on ${star.fg}-of-${star.fga} shooting.`,
								`${star.name} needed only ${star.fga} shots for his ${star.pts}.`,
								`${star.name} was ${star.fg}-of-${star.fga} from the floor.`,
							]
						: pct < 0.42
							? // Plainly, the way the wire puts it: the second sentence of
								// the winners' story is not the place to editorialize about
								// the man the headline is about ("The shooting was not
								// pretty for Cade Hayes" under "Heat ride Cade Hayes' 14
								// points and 15 rebounds").
								[
									`${star.name} had ${plural(star.pts, "point")} on ${star.fg}-of-${star.fga} shooting.`,
									`${star.name} got there the hard way, ${star.fg}-of-${star.fga} from the floor.`,
								]
							: [
									`${star.name} scored his ${star.pts} on ${star.fg}-of-${star.fga} shooting.`,
									`${star.name} finished ${star.fg}-of-${star.fga} from the floor.`,
									`${star.name} took ${star.fga} shots and made ${star.fg} of them on the way to ${star.pts}.`,
								],
					pct >= 0.55
						? "starSplitGood"
						: pct < 0.42
							? "starSplitPoor"
							: "starSplit",
				),
			);
		} else if (star.fga > 0) {
			// Fewer shots than that still make a split the headline did not
			// have. With none of this, "Franz Carter goes for 15 points and 10
			// rebounds as the Hornets take down the Bucks" sat over a body that
			// never said his name.
			para1.push(`${star.name} was ${star.fg}-of-${star.fga} from the floor.`);
		}
		// (A headline that printed his whole line, and a line with no split
		// to add: the sentence is not pushed a third time.)
		if (shot && !shot.tying) {
			const clutch =
				starWonIt && opener.starFolded
					? {
							text: `${star.name} won it on ${clutchWhat(shot)}.`,
							told: star.name,
						}
					: clutchSentence(shot, shape);
			para1.push(clutch.text);
			heroTold = clutch.told;
		}
	} else if (shot && !shot.tying && shot.name !== star.name) {
		// The headline is the winning shot (buildHeadline always leads with one
		// when there is one), so the body has to get there immediately. Opening
		// on someone else's scoring line and reaching the shot a sentence later
		// made the headline look like it belonged to a different story: "Metta
		// World Peace's three-point play sinks the Knicks / Ray Allen scored 18
		// points as...". Result, then the shot, then the leading scorer.
		const opener = resultLead(
			game,
			shape,
			rng,
			headline.spentScore,
			/straight|in a row|skid|losing streak/.test(headline.text),
			tail.text,
		);
		flowCovered = opener.covers;
		const clutch = clutchSentence(shot, shape);
		heroTold = clutch.told;
		para1.push(
			opener.text,
			clutch.text,
			leadSentence(game, shape, star, rng, true),
		);
	} else if (shot && !shot.tying) {
		// The man who won it is the man the lede is about, and the shot comes
		// before his line: "Nowell hit a three-pointer with 1.8 seconds left
		// and finished with 20 points" is the order any wire lede puts a game
		// decided at the end. His line used to come first and the shot arrived
		// as a trailing clause.
		const verb = pastTense(pick(rng, verbPool(game, shape)));
		const what = clutchWhat(shot);
		para1.push(
			`${star.name} ${clutchVerb(what)} ${what} and finished with ${statPhrase(
				star,
			)} as ${theNick(shape.winner)} ${verb} ${theNick(shape.loser)}${
				headline.spentScore ? "" : ` ${scoreTag(shape)}`
			}${tail.text}.`,
		);
		heroTold = star.name;
	} else {
		const team = teamLede(
			game,
			shape,
			star,
			rng,
			!!headline.spentScore,
			tail.text,
		);
		if (team) {
			para1.push(team.text);
			if (team.twin) {
				heroTold = team.twin.name;
			}
			if (team.dblFig) {
				balanceTold = true;
			}
		} else {
			para1.push(
				leadSentence(
					game,
					shape,
					star,
					rng,
					false,
					headline.spentScore,
					tail.text,
				),
			);
		}
	}

	// The headline carried the series score, so the body's series sentence is
	// dropped below - and the round goes where a wire lead puts it: "beat the
	// Pistons 104-96 in Game 4 of the First Round".
	if (headline.spentState && post.state && para1.length > 0) {
		const where = ` in Game ${post.state.gameNo} of ${post.state.round}`;
		const first = para1[0]!;
		const tag = scoreTag(shape);
		if (first.includes(tag)) {
			para1[0] = first.replace(tag, `${tag}${where}`);
		} else {
			const loserName = theNick(shape.loser);
			const at = first.indexOf(loserName);
			para1[0] =
				at >= 0
					? `${first.slice(0, at + loserName.length)}${where}${first.slice(at + loserName.length)}`
					: `${first.slice(0, -1)}${where}.`;
		}
	}

	// THE GAME STORY, as its own paragraph after the lede: the half, the
	// runs, the leads and the finish told in the order they happened. When
	// the quarter scores cannot support one, the old single-paragraph path
	// below is used unchanged.
	const story = gameStory(game, shape, rng, {
		comebackTold: flowCovered === "comeback",
		wireTold: flowCovered === "wire",
		lastQuarterTold: new RegExp(
			String.raw`with an? \d+-\d+ ${ordinal(shape.regPeriods)} quarter`,
		).test(para1.join(" ")),
	});
	const storyPara: string[] = [];
	const body = story ? storyPara : para1;
	if (story) {
		storyPara.push(...story.sentences);
		if (!flowCovered) {
			flowCovered = story.covers.has("comeback")
				? "comeback"
				: story.covers.has("run")
					? "run"
					: undefined;
		}
	}
	// The result lead already carried the comeback / wire-to-wire / overtime /
	// decisive-run angle, so the flow sentence would be saying it twice. With
	// a story, only the overtime line is still wanted from it.
	let otFlowAt = -1;
	// ...unless the lead said it: "It took an extra period, but the Suns
	// pulled away" was followed by "Regulation settled nothing, and it took
	// an extra period."
	if (!flowCovered || (story && shape.ot > 0 && flowCovered !== "ot")) {
		const flow = flowSentence(shape, rng);
		if (flow && (!story || flow.covers === "ot")) {
			if (flow.covers === "ot") {
				otFlowAt = body.length;
			}
			body.push(flow.text);
			flowCovered = flow.covers;
		}
	}
	// The late go-ahead basket the lede ended on is his own when he is the
	// lede's subject: "Miles Foster had 23 points as the Pacers beat the
	// Bulls 110-107 on his go-ahead jumper".
	const goAheadAt = goAhead
		? para1.findIndex((t) => t.includes(goAhead.text))
		: -1;
	if (goAhead && goAheadAt >= 0) {
		const t = para1[goAheadAt]!;
		// Not after two names: "Reggie Carter and Scoot Carter scored 17
		// points apiece as... on his go-ahead three-pointer".
		if (
			(t.startsWith(`${goAhead.name} `) || t.startsWith(`${goAhead.name},`)) &&
			!t.startsWith(`${goAhead.name} and `)
		) {
			para1[goAheadAt] = t.replace(
				`${poss(goAhead.name)} go-ahead`,
				"his go-ahead",
			);
		}
	}
	// How it finished, for a game that was still a game at the end: the
	// closing scores in order when the sim kept them, the go-ahead basket
	// and the last tie when it did not.
	const shotTold = (shot !== undefined && !shot.tying) || goAheadAt >= 0;
	const lastNamed = body.at(-1)?.includes(star.name)
		? star.name
		: heroTold && body.at(-1)?.includes(heroTold)
			? heroTold
			: undefined;
	const sequence = finishStory(
		{
			game,
			winner: shape.winner,
			loser: shape.loser,
			regPeriods: shape.regPeriods,
			shotTold,
			justNamed: lastNamed,
			afterItWas: /^It was\b/.test(body.at(-1) ?? ""),
		},
		rng,
	);
	// "Regulation settled nothing, and it took an extra period" right before
	// "Bo Bull's jumper with 4 seconds left sent it to overtime" is the same
	// fact twice, and the second one has the name and the clock.
	if (otFlowAt >= 0 && sequence.some((s) => /overtime|extra period/.test(s))) {
		body.splice(otFlowAt, 1);
	}
	// In a story, what explains the game goes BEFORE the finish, so the
	// paragraph ends on the last basket rather than on a rebounding margin.
	let finishAt = body.length;
	const beforeFinish = (text: string) => {
		if (story) {
			body.splice(finishAt, 0, text);
			finishAt += 1;
		} else {
			body.push(text);
		}
	};
	body.push(
		...sequence,
		...finishNote(game, shape, rng, shotTold, lastNamed, sequence.length > 0),
	);
	const spentFacts = new Set<StatFact>();
	const spentTopics = new Set<StatTopic>();
	// A "jumped out to a 30-20 first quarter" note on top of "led wire to wire"
	// or "broke it open with a 30-20 first quarter" is the same beat twice.
	// And a game story has already decided whether the first quarter was
	// news; its note would land after the second half, out of order.
	const stat = statNote(
		shape,
		rng,
		flowCovered === "wire" || flowCovered === "run" || !!story,
		balanceTold,
	);
	// In a story the team's number goes with the team's men, after the
	// supporting cast: dropped in before the finish it broke the clock -
	// "They were 14 up going into the fourth. They made 25 free throws to 11
	// for the Trail Blazers. The Hornets had led by 18, and very nearly let
	// it go." Without a story it stays with the lede, as before.
	let storyStat: string | undefined;
	if (stat && (story ? storyPara.length < 6 : para1.length < 4)) {
		if (story) {
			storyStat = stat.text;
		} else {
			beforeFinish(stat.text);
		}
		spentFacts.add(stat.fact);
		if (stat.topic) {
			spentTopics.add(stat.topic);
		}
	}

	// THE ORDER A REPORTER WRITES IN. What happened (the lede, above), then
	// how it happened, then who did it - the winner's men, then the losers -
	// and last what it means. The sentences used to be grouped by where they
	// came from (star line, supporting cast, whatever extras fired), which
	// put "an 18-0 run in the first" after the box-score names and a team's
	// three-point shooting next to its bench scorer, and read as sentences
	// thrown in at random.
	const postSentences = headline.spentState
		? // "The Cavaliers are up 2-1 in the First Round" under "Cavaliers take
			// a 2-1 series lead" is the same numbers twice; the stakes sentence
			// that follows it is the one with something to add.
			post.sentences.slice(1)
		: headline.spentClinch
			? [
					// The series score, when the headline said only where the winner
					// is going. Short: the fact, not the clinch sentence again.
					...(post.clinch &&
					!/sweep|Game \d|in (?:three|four|five|six|seven|nine)\b/.test(
						headline.text,
					)
						? [
								`${cap(theNick(shape.winner))} took the series ${post.clinch.wAfter}-${post.clinch.lBefore}.`,
							]
						: []),
					// A decider's headline names the game and often nothing else -
					// "Pacers make Game 7 a rout" - and the body then reported whose
					// season ended without ever saying where the winner goes next.
					...(post.clinch &&
					!post.clinch.title &&
					/Game \d/.test(headline.text) &&
					!/advance|move on|reach/.test(headline.text)
						? [
								`${cap(theNick(shape.winner))} move on to ${post.clinch.nextRound}.`,
							]
						: []),
					...post.sentences.slice(1),
				]
			: post.sentences;

	// --- 1. How it happened: the rest of the game story joins the lede. ---
	//
	// A "neither offense got going" note and a "pulled away after halftime"
	// note are two readings of the same game arguing with each other. The
	// grind is the more distinctive observation, so it wins.
	const combined = combinedNote(shape, rng);
	const written1 = [headline.text, ...para1, ...storyPara].join(" ");
	if (story) {
		// The character of the night opens the story; the notes the story
		// did not already tell go in before the finish.
		if (combined) {
			storyPara.unshift(combined);
			finishAt += 1;
		}
		const told = storyPara.join(" ");
		const run =
			story.covers.has("run") || /straight points|-0 run|unanswered/.test(told)
				? undefined
				: runNote(game, shape, rng);
		// A first-half run goes before the halftime score, not after the
		// second half: "...before the Lakers won the second half 69-59. The
		// Lakers ran off 15 straight points in the first."
		const runPeriod = game.flow?.run?.period ?? 0;
		const halfAt = storyPara.findIndex((t) =>
			/halftime|at the half\b|the break|into the second half/.test(t),
		);
		const runEnd = game.flow?.run?.endPeriod ?? runPeriod;
		// Unless that sentence starts further back: "The Hawks had a 15-0
		// run in the second... The Mavericks took the first quarter 35-20 and
		// were up 49-43 at the break" told the second quarter first.
		const halfStartsEarlier =
			halfAt >= 0 &&
			runPeriod > 1 &&
			/first quarter|in the first\b/.test(storyPara[halfAt]!);
		if (run && runEnd <= shape.regPeriods / 2 && halfAt >= 0) {
			storyPara.splice(halfStartsEarlier ? halfAt + 1 : halfAt, 0, run);
			finishAt += 1;
		} else if (run && runPeriod <= shape.regPeriods / 2 && halfAt >= 0) {
			// One that spanned halftime comes right after the halftime score.
			storyPara.splice(halfAt + 1, 0, run);
			finishAt += 1;
		}
		// A third-quarter run goes before the score going into the fourth,
		// and an overtime one after the finish that got the game there: "They
		// scored 12 unanswered in overtime. Payton Dunn's three-pointer at
		// the buzzer sent it to overtime." ran the clock backwards.
		const enterAt = storyPara
			.slice(0, finishAt)
			// Either case: "Going into the fourth, the Suns were up 8" opens on
			// it, and the run was told after it, in the wrong quarter.
			.findIndex((t) => /going into the|entering the/i.test(t));
		if (
			run &&
			!storyPara.includes(run) &&
			runPeriod > shape.regPeriods / 2 &&
			runPeriod < shape.regPeriods &&
			enterAt >= 0
		) {
			storyPara.splice(enterAt, 0, run);
			finishAt += 1;
		}
		const otRun =
			run && !storyPara.includes(run) && runEnd > shape.regPeriods
				? run
				: undefined;
		// And the losers' biggest lead, when it came in the first half, goes
		// right after the halftime score: "...winning the second half 65-43.
		// They were up by 21 at their biggest. The Celtics led by as many as
		// 15." told the first half last.
		let blown = blownLeadNote(game, shape, rng, written1);
		const loserPeakAt =
			game.flow?.maxLeadAt?.[sideOf(game, shape.loser)] ?? Infinity;
		const halfNow = storyPara.findIndex((t) =>
			/halftime|at the half\b|the break|into the second half/.test(t),
		);
		if (
			blown &&
			!/two minutes/.test(blown) &&
			loserPeakAt <= shape.regPeriods / 2 &&
			halfNow >= 0 &&
			storyPara.length < 7
		) {
			storyPara.splice(halfNow + 1, 0, blown);
			finishAt += 1;
			blown = undefined;
		}
		const big =
			story.covers.has("winnerPeak") || /as many as|lead reached/.test(told)
				? undefined
				: bigLeadNote(game, shape, rng);
		// A lead that peaked after it was taken for good is told after the
		// basket that took it: "They led by as many as 21. They took the lead
		// for good on Scoot Mathis' basket with 6:43 left in the third" ran
		// the fourth quarter before the third.
		const wPeakAt = game.flow?.maxLeadAt?.[sideOf(game, shape.winner)];
		const forGoodAt = game.flow?.lastLead?.period;
		const peakLater =
			wPeakAt !== undefined &&
			forGoodAt !== undefined &&
			forGoodAt < wPeakAt &&
			/for good|for the last time/.test(storyPara.slice(finishAt).join(" "));
		for (const e of [
			run && !storyPara.includes(run) && run !== otRun ? run : undefined,
			big,
			blown,
		]) {
			if (e && storyPara.length < 7) {
				if (e === big && peakLater) {
					storyPara.push(e);
				} else {
					beforeFinish(e);
				}
			}
		}
		if (otRun && storyPara.length < 7) {
			storyPara.push(otRun);
		}
	}
	const howItHappened = story
		? []
		: [
				// A "took over in the fourth, 23-11" flow line and a "40-22 over the
				// last two quarters" note are the same stretch of the game measured
				// twice, so a run already told stands in for the half.
				combined || flowCovered === "run" || flowCovered === "comeback"
					? undefined
					: secondHalfNote(shape, rng),
				combined,
				runNote(game, shape, rng),
				bigLeadNote(game, shape, rng),
				blownLeadNote(game, shape, rng, written1),
			].filter((x): x is string => !!x);
	for (const e of howItHappened) {
		if (para1.length >= 5) {
			break;
		}
		para1.push(e);
	}
	// The halftime score, as every wire recap carries it, unless something
	// above already put the reader at the break.
	if (
		!story &&
		para1.length <= 5 &&
		!/halftime|at the break|the half\b|after the break|second half/.test(
			para1.join(" "),
		)
	) {
		const half = halftimeNote(
			shape,
			rng,
			// The comeback's own words, not "it came from everywhere".
			flowCovered === "comeback" ||
				/trailed by \d|came from \d+ down|erased|-point deficit|-point hole/.test(
					para1.join(" "),
				),
		);
		if (half) {
			// IN ORDER. The break comes before anything that happened after
			// it: "The fourth quarter settled it: 27-16. They took a 57-47 lead
			// into halftime" and "had led by 29 and very nearly let it go. They
			// went into the break ahead 69-45" both run the clock backwards. A
			// first-half run stays ahead of it; the lede is never displaced.
			const later = para1.findIndex(
				(t, i) => i > 0 && SECOND_HALF_OR_FINISH.test(t),
			);
			if (later > 0) {
				// Not between a name and the "His basket..." that leans on it:
				// step back over a sentence that opens on a pronoun.
				const PRONOUN = /^(?:His|He)\b/;
				let at = later;
				while (at > 1 && PRONOUN.test(para1[at]!)) {
					at -= 1;
				}
				para1.splice(PRONOUN.test(para1[at]!) ? at + 1 : at, 0, half);
			} else {
				para1.push(half);
			}
		}
	}
	// A FIRST-HALF RUN AFTER THE FINISH. "Marcus Jackson's three cut it to
	// 107-106 with 0.7 seconds left. A 13-0 run in the second built the
	// lead..." walks the clock backwards the same way the halftime score
	// used to. A sentence pinned to the first or second quarter moves up to
	// sit before the first thing that happened after the break - unless it
	// leans on the sentence before it with a pronoun.
	{
		const FIRST_HALF =
			/\bin the (?:first|second)\b(?! half)|\bafter one\b|\b(?:first|second) quarter\b/;
		// "It was 33-18 after one" is impersonal and moves freely; "They..."
		// and "His..." lean on the sentence before them.
		const LEANS = /^(?:He|His|They|Their)\b/;
		const firstLater = body.findIndex(
			(t, i) => i > 0 && SECOND_HALF_OR_FINISH.test(t),
		);
		if (firstLater > 0) {
			// Collected first and inserted as one block, in their own order.
			// Moving them one at a time left the position of "the first
			// thing after the break" stale, and two first-half sentences could
			// trade places with each other forever.
			const moved: string[] = [];
			const kept: string[] = [];
			body.forEach((t, i) => {
				if (
					i > firstLater &&
					FIRST_HALF.test(t) &&
					!SECOND_HALF_OR_FINISH.test(t) &&
					!LEANS.test(t)
				) {
					moved.push(t);
				} else {
					kept.push(t);
				}
			});
			if (moved.length > 0) {
				// Before the halftime score when it sits right there: the run
				// came before the break. The lede never moves; a story
				// paragraph has no lede in it.
				const at =
					firstLater > (story ? 0 : 1) &&
					/halftime|at the break|at the half\b|into the break|into halftime/.test(
						kept[firstLater - 1]!,
					)
						? firstLater - 1
						: firstLater;
				kept.splice(at, 0, ...moved);
				body.splice(0, body.length, ...kept);
			}
		}
	}
	// Where the series stands is part of what happened, and closes the lede.
	// When the headline already carried it, what is left is the stakes
	// ("The Raptors can still close it out in Game 5"), which is context.
	const seriesState = headline.spentState ? undefined : postSentences[0];
	const postContext = headline.spentState
		? postSentences
		: postSentences.slice(1);
	if (seriesState && (story || para1.length < 6)) {
		body.push(seriesState);
	}
	const spentThrees = () =>
		spentTopics.has("threes") || spentFacts.has("loserThrees");

	// --- 2. Who did it: the winners. The man in the lede first - what his
	// night was against his own season - then the men around him. ---
	const winners: string[] = [];
	let writtenSoFar = [headline.text, ...para1, ...storyPara].join(" ");
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
	// The career arc first: it is the rarest note and the one a season
	// high or an against-his-average line would otherwise crowd out.
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
		() => playerHighBeat(star, rng, writtenSoFar),
		() => vsOpponentBeat(star, nick(shape.loser), rng),
	];
	// Two notes on the star at most; the paragraph is about the team.
	for (const beat of starColour) {
		addWinner(beat, 2);
	}
	let supportText: string | undefined;
	addWinner(() => {
		supportText = supportSentence(
			shape,
			star,
			rng,
			heroTold,
			shape.winner.players.find(
				(p) => p !== star && headline.text.includes(p.name),
			),
		);
		return supportText;
	});
	addWinner(() => storyStat, 6);
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
	});
	addWinner(() =>
		balanceNote(shape, rng, {
			dblFig: /in double figures|double figures/.test(writtenSoFar),
			// "assisted on 31 of their 39 baskets" counts too.
			assists: /assist(?:s|ed)?\b/.test(writtenSoFar),
		}),
	);

	// --- 3. The losers: their best man and why it was not enough, then the
	// rest of what they got, then the individual notes. ---
	const losers: string[] = [];
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
	addLoser(() =>
		loserSentence(
			shape,
			rng,
			spentFacts,
			headline.spentLoserStar || tail.loserStar !== undefined,
		),
	);
	const said = namesIn(writtenSoFar, shape);
	said.add(star.name);
	const loserBest = bestOf(shape.loser.players);
	// The losing side's best man had a season high of his own - straight
	// after his line, and only once the piece has introduced him, so the
	// sentence has a line to refer to.
	addLoser(() =>
		loserBest && loserBest !== star && said.has(loserBest.name)
			? playerHighBeat(loserBest, rng, writtenSoFar)
			: undefined,
	);
	// Their second man right after their first - the two men together, then
	// the team's shooting. Interleaved, the paragraph read "Trey Hayes had 19
	// for the Bucks... The Suns got to the line more often... Obi Hayes
	// tacked on 16 in defeat", the losers' men separated by a whistle count.
	addLoser(() => loserSupportNote(shape, rng, namesIn(writtenSoFar, shape)));
	// The two sides' shooting, told as the reason the losers lost: the
	// three-point line and the free-throw line sit with the side they
	// explain rather than three paragraphs from it.
	for (const beat of shuffle(rng, [
		() => threeNote(shape, rng, spentThrees()),
		() => freeThrowNote(shape, rng, spentTopics.has("freeThrows")),
	])) {
		addLoser(beat, 4);
	}
	// The defensive note can be either side's man, so it waits until the
	// losers' best has had his sentence - named first for his steals, he
	// came back a sentence later as "His 18 points...".
	// It goes in the paragraph of the side it is about: "Jalen Green
	// collected 5 steals for the Clippers" closed the losers' paragraph after
	// their two men and their shooting, a winner's note among the losers.
	// The men written since the set was taken, the losers' second man among
	// them: "Payton Brooks turned in 12 points for the losing side. Foul
	// trouble cost Payton Brooks, who finished with 12 points".
	for (const name of namesIn(writtenSoFar, shape)) {
		said.add(name);
	}
	for (const beat of shuffle(rng, [
		() => defensiveNote(shape, rng, said),
		() => foulOutNote(shape, said, rng),
		() => minutesNote(shape, said),
	])) {
		if (losers.length >= 5) {
			break;
		}
		const text = beat();
		if (!text) {
			continue;
		}
		const aboutWinner =
			shape.winner.players.some((p) => text.startsWith(p.name)) ||
			(text.includes(nick(shape.winner)) && !text.includes(nick(shape.loser)));
		if (aboutWinner) {
			addWinner(() => text, 6);
		} else {
			addLoser(() => text);
		}
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
		addContext(() => text);
	}
	const seriesCount = context.length;
	addContext(() =>
		stakesSentence(
			game,
			shape,
			rng,
			/underdog|-point dogs|the wrong side of the line/.test(headline.text),
			/in a row|straight|skid|losing streak|winning streak/.test(written1) ||
				tail.streak === true,
			new RegExp(
				String.raw`${nick(shape.loser)}'s? [\w-]+-game winning streak`,
			).test(`${headline.text} ${written1}`),
		),
	);
	addContext(() => loserSkidNote(shape, rng, beatCtx.written));
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

	// Paragraphing. The two people paragraphs are one when together they are
	// short; a lone sentence is never a paragraph of its own.
	let para2: string[];
	let para3: string[];
	if (winners.length + losers.length <= 4) {
		para2 = [...winners, ...losers];
		para3 = context;
	} else {
		para2 = winners;
		para3 = losers;
	}
	let para4: string[] = para3 === context ? [] : context;
	if (para4.length === 1 && para3.length > 0 && para3.length <= 4) {
		para3.push(para4.pop()!);
	}
	if (para3.length === 1 && para2.length > 0 && para2.length <= 4) {
		para2.push(para3.shift()!);
		para3 = para4;
		para4 = [];
	}
	// A lede of one bare result sentence happens when the headline already
	// spent the star's line and the game had no comeback or decisive run to
	// describe; pull the next beat up rather than leave the opening paragraph
	// a single clause.
	//
	// With a story paragraph the lede may stand alone - one sentence is how
	// a wire story opens - but a story of one sentence is folded back into
	// it, and a lone sentence about the players joins the next paragraph.
	// A short story is part of the lede, not a paragraph of its own.
	if (
		storyPara.length > 0 &&
		storyPara.length <= 2 &&
		para1.length + storyPara.length <= 4
	) {
		para1.push(...storyPara.splice(0));
	}
	if (storyPara.length > 0) {
		if (para2.length === 1) {
			if (para3.length > 0) {
				para3.unshift(para2.shift()!);
				para2 = para3;
				para3 = para4;
				para4 = [];
			} else {
				storyPara.push(para2.shift()!);
			}
		}
	} else {
		if (para1.length === 1 && para2.length > 1) {
			para1.push(para2.shift()!);
		}
		if (para2.length === 1 && para1.length <= 3) {
			para1.push(para2.shift()!);
			para2 = para3;
			para3 = para4;
			para4 = [];
		}
	}

	// A PARAGRAPH SAYS WHOSE IT IS. The supporting cast opening a paragraph
	// after the game story named two men and no team - "Miles Green added 24
	// points, and Herb Green chipped in 17" - leaving the reader to work out
	// which side they played for. The wire's answer is three words on the
	// first clause.
	for (const para of [para2, para3, para4]) {
		const first = para[0];
		if (
			first !== undefined &&
			first === supportText &&
			!first.includes(nick(shape.winner))
		) {
			// The ", and" that starts the next man's clause, never the one
			// inside a stat list ("18 points, 3 steals, and 2 blocks").
			const cut = first.search(/, and (?=[A-Z])|\.$/);
			const clause = first.slice(0, cut);
			if (cut > 0 && !/ for | of his own/.test(clause)) {
				para[0] = `${clause} for ${theNick(shape.winner)}${first.slice(cut)}`;
			}
		}
	}

	const otherNick = nick(shape.loser);
	const nicks = [nick(shape.winner), otherNick];
	// Longest first, so a "Marcus Jackson Jr." is not read as "Marcus Jackson".
	const playerNames = [...shape.winner.players, ...shape.loser.players]
		.map((p) => p.name)
		.sort((a, b) => b.length - a.length);
	const tidy = (para: string[]) =>
		joinShortPairs(
			varyHalfWords(
				dedupePlayerSubjects(
					dedupeSubjects(para, otherNick, nicks),
					playerNames,
				),
			),
		).join(" ");
	const paragraphs = [tidy(para1)];
	if (storyPara.length > 0) {
		paragraphs.push(tidy(storyPara));
	}
	if (para2.length > 0) {
		paragraphs.push(tidy(para2));
	}
	if (para3.length > 0) {
		paragraphs.push(tidy(para3));
	}
	if (para4.length > 0) {
		paragraphs.push(tidy(para4));
	}
	return `**${headline.text}**\n\n${withRecords(teamTheSwitch(paragraphs, shape), game, shape).join("\n\n")}`;
};

// A MAN WHOSE SIDE THE SENTENCE BEFORE DID NOT SET UP. A sentence that opens
// on a bare name right after one about the other club reads as that club's
// man: "The Grizzlies got to the line more than twice as often, 32 attempts
// to 11. Cade Brooks sprained his shoulder..." - and Cade Brooks plays for
// the Hawks. He gets his club, as the wire writes it: "The Hawks' Cade
// Brooks sprained his shoulder". Across paragraphs too: a notes paragraph
// opens straight after the losers' lines.
export const teamTheSwitch = (paragraphs: string[], shape: Shape): string[] => {
	const sides = [shape.winner, shape.loser];
	const sideOfName = new Map<string, RecapTeam>();
	for (const t of sides) {
		for (const p of t.players) {
			sideOfName.set(p.name, t);
		}
	}
	const names = [...sideOfName.keys()].sort((a, b) => b.length - a.length);
	let prev = "";
	return paragraphs.map((para) => {
		const sentences = para.split(/(?<=[!.?])\s+(?=[A-Z])/);
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

export const getAutoRecap = (game: RecapGame): string =>
	fixArticles(buildGameRecap(withSettledLead(game)));

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
		// only: "0-1" is inside "110-109".
		const stated = new RegExp(String.raw`(?<![\d-])${tag}(?![\d-])`);
		if (out.some((p) => stated.test(p))) {
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
type LeaguePerformance = {
	p: RecapPlayer;
	team: RecapTeam;
	opp: RecapTeam;
	won: boolean;
	game: RecapGame;
};

// How big a line reads in a league wrap. `impact` (above) discounts volume
// shooting, which is right for ordering a recap's supporting cast and wrong
// here: a wrap that said "Franz Ellis had 25 points" about a game in which
// Herb Ellis had 31 and Amen Brooks had 27 and 16 read as though nobody had
// looked at the box score. Points, the all-round numbers, and a bonus for
// each double-double category past the first.
const headlineScore = (p: RecapPlayer): number =>
	p.pts +
	0.5 * p.reb +
	0.7 * p.ast +
	1.4 * p.stl +
	1.4 * p.blk +
	6 * Math.max(0, doubleCategories(p).length - 1);

const collectPerformances = (games: RecapGame[]): LeaguePerformance[] => {
	const out: LeaguePerformance[] = [];
	for (const game of games) {
		if (game.allStar) {
			continue;
		}
		const [home, away] = game.teams;
		for (const [team, opp] of [
			[home, away],
			[away, home],
		] as const) {
			for (const p of team.players) {
				out.push({ p, team, opp, won: team.tid === game.winnerTid, game });
			}
		}
	}
	return out.sort((a, b) => headlineScore(b.p) - headlineScore(a.p));
};

// How much a game deserves to be the day's marquee story.
const notability = (game: RecapGame): number => {
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

// A compact one-sentence account of a game, for the day wrap.
const gameBlurb = (
	game: RecapGame,
	rng: () => number,
	// Lead on the man rather than the scoreline. Used when the headline has
	// already given this exact result, where the default order made the first
	// line of the body an echo of the line above it: "Apollos shock the Bankers
	// 114-104" over "The Apollos shocked the Bankers 114-104 behind ...".
	starFirst = false,
): string => {
	const shape = analyzeShape(game);
	// The same man the game's own recap leads on, so the wrap and the recap
	// under it never disagree about whose night it was.
	const star = storyPlayerOf(game, shape);
	if (shape.margin === 0) {
		return game.shootout
			? `${cap(theNick(shape.winner))} beat ${theNick(shape.loser)} in a shootout, ${shape.winner.pts}-${shape.loser.pts}`
			: `${cap(theNick(shape.winner))} and ${theNick(shape.loser)} played to a ${shape.winner.pts}-${shape.loser.pts} tie`;
	}
	const verb = pastTense(pick(rng, verbPool(game, shape)));
	const shot = clutchShot(game);
	const base = `${cap(theNick(shape.winner))} ${verb} ${theNick(
		shape.loser,
	)} ${scoreTag(shape)}`;
	// A game decided in the last two minutes, on the score that decided it -
	// the sim only calls the very last basket a game-winner. Like the winning
	// shot, it outranks the star-first form: it is new even under a headline
	// that gave the result.
	const last = game.flow?.lastLead;
	if (
		shape.margin <= 3 &&
		shape.ot === 0 &&
		!(shot && !shot.tying) &&
		last &&
		last.side === sideOf(game, shape.winner) &&
		last.period >= shape.regPeriods &&
		last.clock <= 120 &&
		Number.isFinite(last.clock)
	) {
		const who = nameOfPid(game, last.pid);
		const left = clockLeft(last.clock);
		if (who && left) {
			// Two free throws are "go-ahead free throws", not a basket.
			const e = (game.flow?.finish ?? []).find(
				(x) =>
					x.side === last.side &&
					x.period === last.period &&
					Math.abs(x.clock - last.clock) < 0.15,
			);
			const kind = e
				? shotName(e).replace(/^(?:two|three) /, "")
				: last.by === 1
					? "free throw"
					: (winningShotKind(game, game.winnerTid, last.clock) ?? "basket");
			return `${base} on ${poss(who)} go-ahead ${kind} with ${left} left`;
		}
	}
	// A game-winning shot outranks this: it is the story, and it already opens
	// on something the headline's bare result did not have.
	if (starFirst && star && !(shot && !shot.tying)) {
		const ddw = doubleWord(doubleCategories(star).length);
		const line =
			ddw && doubleCategories(star).length >= 3
				? `a ${ddw} (${statPhrase(star)})`
				: statPhrase(star, 2);
		// Not "had": the performance sentences that follow in the same paragraph
		// use it, and "X had 42 as the Roses stunned the Aztecs. Y had 32 as the
		// Snipers beat the Symphony." is the frame twice in a row.
		const leadVerbPhrase = pick(
			rng,
			["led the way with", "went for", "turned in"],
			"dayLeadStar",
		);
		return `${star.name} ${leadVerbPhrase} ${line} as ${theNick(
			shape.winner,
		)} ${verb} ${theNick(shape.loser)} ${scoreTag(shape)}`;
	}
	if (shot && !shot.tying) {
		const timing =
			shot.shot === "buzzer-beater"
				? ""
				: shot.buzzer
					? " at the buzzer"
					: shot.seconds !== undefined
						? ` with ${plural(shot.seconds, "second")} left`
						: "";
		return `${base} on ${poss(shot.name)} ${shot.shot}${timing}`;
	}
	if (star) {
		// Give the marquee star his full line, calling out a triple-double.
		const ddw = doubleWord(doubleCategories(star).length);
		if (ddw && doubleCategories(star).length >= 3) {
			return `${base} behind ${poss(star.name)} ${ddw} (${statPhrase(star)})`;
		}
		return `${base} behind ${poss(star.name)} ${statPhrase(star, 2)}`;
	}
	return base;
};

const conferencePictureSentence = (
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
		// something once a real sample exists.
		if (leader.won + leader.lost < 5) {
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

// The night's injury news, worst first - a real league wrap covers who went
// down, not just who went off. A wrap has room for the injuries that change a
// team for a while: a rotation player out five games or more. "The casualty
// list: Payton Hayes (sore back, out about three games)" was a sentence about
// nothing.
const injuryRoundup = (
	games: RecapGame[],
	rng: () => number,
	// The wrap so far; a man it already named needs no team.
	written: string,
): string | undefined => {
	const hurt: {
		t: RecapTeam;
		p: RecapPlayer;
		n: number;
		left?: number;
		playoffs: boolean;
	}[] = [];
	for (const game of games) {
		if (game.allStar) {
			continue;
		}
		for (const t of game.teams) {
			for (const p of t.players) {
				const n = p.injury?.newThisGame ? (p.injury.gamesRemaining ?? 0) : 0;
				if (n >= 5 && hurtIsNews(p)) {
					hurt.push({
						t,
						p,
						n,
						left: gamesLeft(game, t),
						playoffs: game.playoffs,
					});
				}
			}
		}
	}
	if (hurt.length === 0) {
		return undefined;
	}
	hurt.sort((a, b) => b.n - a.n);
	// "the Celtics' Isaiah King (strained hamstring, out about eleven games)
	// and the Rockets' Anthony Foster (sprained ankle, about five)": whose he
	// is, and "out ... games" said once.
	// One club's men side by side, its name once: "the Warriors' Walker
	// Ellis (...), the Warriors' Corey Ellis (...), and the Warriors' Vince
	// Ellis (...)" named it three times.
	const worst = hurt.slice(0, 3);
	const grouped = worst
		.map((h, i) => ({
			h,
			i,
			g: worst.findIndex((x) => x.t.tid === h.t.tid),
		}))
		.sort((a, b) => a.g - b.g || a.i - b.i)
		.map(({ h }) => h);
	let teamInAir: number | undefined;
	const top = grouped.map(({ t, p, n, left, playoffs }, i) => {
		const named = written.includes(p.name);
		const whose = named || teamInAir === t.tid ? "" : `${poss(theNick(t))} `;
		teamInAir = named ? undefined : t.tid;
		return `${whose}${p.name} (${lowerInjury(p.injury!.type)}, ${
			left !== undefined && n > left
				? playoffs
					? "out for the playoffs"
					: "out for the season"
				: i === 0
					? `out about ${gamesWord(n)}`
					: `about ${numWord(n)}`
		})`;
	});
	return pick(
		rng,
		[
			`On the injury front, ${naturalList(top)} went down.`,
			`The night took its toll: ${naturalList(top)}.`,
			`${cap(naturalList(top))} ${top.length === 1 ? "was" : "were"} hurt along the way.`,
			`The casualty list: ${naturalList(top)}.`,
		],
		"dayInjuries",
	);
};

// A team riding a notable win streak into the night.
const teamStreakSentence = (
	games: RecapGame[],
	rng: () => number,
	// The deck already in lights ("Kings make it 6 straight"), so the body
	// does not say the same streak two paragraphs later.
	deck = "",
): string | undefined => {
	let best: { team: RecapTeam; count: number } | undefined;
	for (const game of games) {
		if (game.allStar) {
			continue;
		}
		const winner =
			game.teams[0].tid === game.winnerTid ? game.teams[0] : game.teams[1];
		const s = winner.streak;
		// An unbeaten team's streak IS its record - the standings line already
		// says "still perfect", so don't repeat it as a streak note.
		if (winner.record && winner.record.lost === 0) {
			continue;
		}
		if (
			s &&
			s.won &&
			s.count >= 6 &&
			(!best || s.count > best.count) &&
			!(deck.includes(nick(winner)) && /straight/.test(deck))
		) {
			best = { team: winner, count: s.count };
		}
	}
	if (!best) {
		return undefined;
	}
	return pickByDay(
		games[0]?.day,
		rng,
		[
			`${cap(theNick(best.team))} ran their win streak to ${numWord(best.count)} games.`,
			`${cap(theNick(best.team))} have now won ${numWord(best.count)} straight.`,
			`Make it ${numWord(best.count)} in a row for ${theNick(best.team)}.`,
			`${cap(theNick(best.team))} stretched their run to ${numWord(best.count)} wins.`,
		],
		"dayStreak",
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
				tails: [`to win the series in Game ${games}`],
				whos: [`who won the series in Game ${games}`],
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

type DaySeriesPhrase = { text: string; round?: string };

const daySeriesPhrase = (
	g: RecapGame,
	rng: () => number,
): DaySeriesPhrase | undefined => {
	const shape = analyzeShape(g);
	const w = theNick(shape.winner);
	const l = theNick(shape.loser);

	if (g.playIn) {
		const p = g.playIn;
		if (p.kind === "seed7v8") {
			return {
				text:
					typeof p.prizeSeed === "number"
						? // The conference matters: both play-in winners take a #7 seed,
							// and side by side "the Monuments grabbed the #7 seed ... the
							// Whalers grabbed the #7 seed" reads like a mistake.
							pick(
								rng,
								[
									`${w} grabbed the No. ${p.prizeSeed} seed`,
									`${w} came through for the No. ${p.prizeSeed} seed`,
									`${w} locked up the No. ${p.prizeSeed} seed`,
								],
								"daySeriesPlayInSeed",
							)
						: `${w} took the higher seed`,
			};
		}
		if (p.kind === "seed9v10") {
			return {
				text: pick(
					rng,
					[
						`${w} ended ${poss(l)} season in the play-in`,
						`${w} knocked ${l} out of the play-in`,
					],
					"daySeriesPlayInOut",
				),
			};
		}
		return { text: `${w} claimed the last playoff spot` };
	}

	const s = g.series;
	if (!s) {
		return undefined;
	}
	const round = roundName(s.round, s.numRounds);
	const winnerIsHome = shape.winner.abbrev === s.homeAbbrev;
	const wBefore = winnerIsHome ? s.homeWon : s.awayWon;
	const lBefore = winnerIsHome ? s.awayWon : s.homeWon;
	const wAfter = wBefore + 1;
	const need =
		typeof s.bestOf === "number" && s.bestOf > 0
			? Math.floor(s.bestOf / 2) + 1
			: undefined;

	// A clinch, phrased so it reads under a shared round ("In the First Round,
	// ... and the Blazers swept the Bucks") and on its own ("the Blazers swept
	// the Bucks in the First Round"). It used to bake the round into its own
	// text, which broke the shared-round fold for the whole sentence: "In the
	// playoffs, the Pistons grabbed a 3-1 edge in the First Round, the Hawks
	// are up 3-1 in the First Round, ..." four times down the line.
	if (need !== undefined && wAfter >= need) {
		if (s.round === s.numRounds) {
			return { text: `${w} won the championship` };
		}
		const games = wAfter + lBefore;
		return {
			round,
			text:
				lBefore === 0
					? `${w} swept ${l}`
					: games === s.bestOf
						? `${w} put ${l} out in Game ${games}`
						: pick(
								rng,
								[
									`${w} put ${l} out in ${numWord(games)}`,
									`${w} closed out ${l} in ${numWord(games)}`,
									`${w} finished off ${l} in ${numWord(games)}`,
								],
								"daySeriesClinch",
							),
		};
	}
	if (wAfter === lBefore) {
		return {
			round,
			text: pick(
				rng,
				[
					`${w} pulled even with ${l} at ${wAfter}-${wAfter}`,
					`${w} evened their series with ${l} at ${wAfter}-${wAfter}`,
					`${w} squared it with ${l} at ${wAfter}-${wAfter}`,
				],
				"daySeriesEven",
			),
		};
	}
	// Always against a named opponent. "The Hawks are up 3-1" in a list of
	// six series left the reader to work out who they were up on.
	if (wAfter > lBefore) {
		return {
			round,
			text: pick(
				rng,
				[
					`${w} lead ${l} ${wAfter}-${lBefore}`,
					`${w} are up ${wAfter}-${lBefore} on ${l}`,
					`${w} grabbed a ${wAfter}-${lBefore} edge over ${l}`,
					`${w} moved in front of ${l} ${wAfter}-${lBefore}`,
				],
				"daySeriesLead",
			),
		};
	}
	return {
		round,
		text: pick(
			rng,
			[
				`${w} won but still trail ${l} ${lBefore}-${wAfter}`,
				`${w} got one back but remain down ${lBefore}-${wAfter} to ${l}`,
			],
			"daySeriesTrail",
		),
	};
};

// What the marquee playoff game sets up, for the day wrap. On a one-game day -
// every Finals game, and most of the later rounds - the wrap's series roundup
// skips the marquee (it is already the story) and so had nothing at all to say:
// the whole wrap was a headline and one sentence of score. This is the beat
// that was missing, and it is phrased apart from the game recap's own stakes
// sentence so the two blocks do not read as one repeated twice.
const dayStakesPhrase = (
	g: RecapGame,
	rng: () => number,
): string | undefined => {
	const s = g.series;
	if (!s || typeof s.bestOf !== "number" || s.bestOf <= 1) {
		return undefined;
	}
	const shape = analyzeShape(g);
	const w = theNick(shape.winner);
	const l = theNick(shape.loser);
	const winnerIsHome = shape.winner.abbrev === s.homeAbbrev;
	const wAfter = (winnerIsHome ? s.homeWon : s.awayWon) + 1;
	const lAfter = winnerIsHome ? s.awayWon : s.homeWon;
	const need = Math.floor(s.bestOf / 2) + 1;
	const nextGame = wAfter + lAfter + 1;
	const prize = s.round === s.numRounds ? "the title" : "the next round";

	// The series is over; the clinch is the story and the headline has it.
	if (wAfter >= need) {
		return undefined;
	}
	const winnerNeeds = need - wAfter;
	const loserNeeds = need - lAfter;

	if (winnerNeeds === 1 && loserNeeds === 1) {
		return `Game ${nextGame} is for ${prize}.`;
	}
	if (winnerNeeds === 1) {
		return pick(
			rng,
			[
				`${cap(l)} face elimination in Game ${nextGame}.`,
				`${cap(l)} have to win out from here, starting in Game ${nextGame}.`,
				`Game ${nextGame} can end it.`,
			],
			"dayStakesBrink",
		);
	}
	if (loserNeeds === 1) {
		return pick(
			rng,
			[
				`${cap(l)} can still finish it in Game ${nextGame}.`,
				`${cap(w)} are not out of it, but ${l} need only one more.`,
				`It is still ${poss(l)} series to close out.`,
			],
			"dayStakesBehind",
		);
	}
	// numWord on BOTH halves: plural() renders the count as a numeral, and
	// "the Blizzard need 3 more wins; the Monuments need four" mixed the two
	// inside one sentence.
	const wins = (n: number) => `${numWord(n)} ${n === 1 ? "win" : "wins"}`;
	return pick(
		rng,
		[
			// "Need three more" of what: "The Warriors need three more; the
			// Grizzlies need four" left the noun out of both.
			`${cap(w)} need ${numWord(winnerNeeds)} more ${winnerNeeds === 1 ? "win" : "wins"}; ${l} need ${numWord(loserNeeds)}.`,
			`${cap(w)} are ${wins(winnerNeeds)} from ${prize}, ${l} ${wins(loserNeeds)}.`,
		],
		"dayStakesOpen",
	);
};

// The day's headline, driven by the single biggest thing that happened -
// a buzzer-beater, a 45-point night, a playoff clinch, an upset, a rout, a
// thriller - rather than a fixed "N-game slate" template. Seeded variation keeps
// two similar days from reading the same.
// The headline, plus WHICH GAME it is about. The caller needs the second half:
// the headline does not always name the marquee (a night of upsets leads with
// the biggest shock, wherever it happened), and a game the headline names has
// to turn up in the body. It did not - the deck would cover it, the roundup
// would then skip it as covered, and the story in the headline appeared nowhere
// underneath it.
const dayHeadline = (
	marquee: RecapGame,
	mShape: Shape,
	mStar: RecapPlayer | undefined,
	games: RecapGame[],
	performers: LeaguePerformance[],
	playoffs: boolean,
	rng: () => number,
): { text: string; game: RecapGame; players?: RecapPlayer[] } => {
	// Nearly every branch is about the marquee; the league-wide upset lead is
	// the exception, and it is the one that went missing from the body.
	const hl = (text: string, game: RecapGame = marquee) => ({ text, game });

	// `w` leads a headline (no article, "Heat stun Lakers"); `tw` sits mid-sentence
	// as an object ("... powers the Heat past ...").
	const w = nick(mShape.winner);
	const tw = theNick(mShape.winner);
	const l = theNick(mShape.loser);

	const post =
		playoffs && marquee.playoffs
			? postseasonContext(marquee, mShape, rng)
			: undefined;

	// A 45-point night on the losing side is the night's story, and "Cade
	// Green's 17 and 13 power the Clippers past the Nets" over a 52-point
	// loss missed it. A clinch or a game-winner still leads.
	const loserBig = bestOf(mShape.loser.players);
	const shotEarly = clutchShot(marquee);
	if (
		loserBig &&
		loserBig.pts >= 45 &&
		loserBig.pts >= (mStar?.pts ?? 0) + 10 &&
		!post?.clinch &&
		!(shotEarly && !shotEarly.tying)
	) {
		const where = post?.state ? ` in Game ${post.state.gameNo}` : "";
		return hl(
			pick(
				rng,
				[
					`${poss(loserBig.name)} ${loserBig.pts} not enough as ${tw} ${pick(rng, verbPool(marquee, mShape))} ${l}${where}`,
					`${loserBig.name} scores ${loserBig.pts} in a losing cause${where ? where : ` against ${tw}`}`,
				],
				"dayHeadlineLoserBig",
			),
		);
	}

	// Postseason storylines lead everything.
	if (playoffs && marquee.playoffs && post) {
		const joined = post.sentences.join(" ");
		const clinch = post.clinch;
		if (clinch?.title) {
			return hl(
				pick(rng, [
					`${w} are champions`,
					`${w} win it all`,
					`${w} capture the title`,
					...(clinch.sweep ? [`${w} sweep ${l} for the title`] : []),
					...(clinch.decider
						? [
								`${w} win the title in ${clinch.deciderTag.replace(/^a decisive/, "the decisive").replace(/^a /, "")}`,
							]
						: []),
				]),
			);
		}
		if (clinch) {
			const next = clinch.nextRound;
			return hl(
				pick(rng, [
					...(clinch.decider
						? []
						: [
								`${w} close out ${l} and reach ${next}`,
								`${w} advance to ${next}`,
								`${w} eliminate ${l}`,
							]),
					...(comebackSize(mShape) > 0
						? [`${w} rally from ${comebackSize(mShape)} down to close out ${l}`]
						: []),
					...(clinch.sweep ? [`${w} sweep ${l}`] : []),
					...(clinch.decider
						? [
								`${w} take ${clinch.deciderTag.replace(/^a decisive/, "the decisive").replace(/^a /, "")} from ${l}`,
								`${w} win ${clinch.deciderTag.replace(/^a decisive/, "the decisive").replace(/^a /, "")} and reach ${next}`,
							]
						: [`${w} put ${l} out in ${numWord(clinch.games)}`]),
				]),
			);
		}
		// Where the series stands after this game, so a headline only claims what
		// actually happened. The generic fallback used to be a two-entry pool
		// containing "X take command against Y" - which it then printed over a
		// play-in game and over Game 1 of a first-round series, neither of which
		// is anyone taking command of anything.
		const ss = (() => {
			const s = marquee.series;
			if (!s || typeof s.bestOf !== "number" || s.bestOf <= 1) {
				return undefined;
			}
			const winnerIsHome = mShape.winner.abbrev === s.homeAbbrev;
			const wBefore = winnerIsHome ? s.homeWon : s.awayWon;
			const lBefore = winnerIsHome ? s.awayWon : s.homeWon;
			return {
				need: Math.floor(s.bestOf / 2) + 1,
				wAfter: wBefore + 1,
				lBefore,
				gameNo: wBefore + lBefore + 1,
				wSeed: winnerIsHome ? s.homeSeed : s.awaySeed,
				lSeed: winnerIsHome ? s.awaySeed : s.homeSeed,
				finals: s.round === s.numRounds,
			};
		})();

		// A series-tying win that forces a winner-take-all next game.
		if (ss && ss.wAfter === ss.need - 1 && ss.lBefore === ss.need - 1) {
			const bestOf = marquee.series!.bestOf!;
			const decider = bestOf === 7 ? "a Game 7" : `a decisive Game ${bestOf}`;
			// A 50-point night that forced it belongs in the headline too.
			return hl(
				mStar && mStar.pts >= 35
					? `${mStar.name} scores ${mStar.pts} as ${tw} force ${decider} with ${l}`
					: `${w} force ${decider} with ${l}`,
			);
		}
		if (/staved off elimination/.test(joined)) {
			return hl(
				pick(
					rng,
					[
						`${w} stave off elimination against ${l}`,
						`${w} keep their season alive`,
						`${w} refuse to go quietly against ${l}`,
					],
					"dayHeadlineSurvive",
				),
			);
		}
		const shot = clutchShot(marquee);
		if (shot && !shot.tying) {
			// Which game, and whose: "Tyrese Ellis' three-pointer decides a
			// playoff thriller" named neither team nor the game, on five
			// nights of a postseason.
			const s = marquee.series;
			const gameNo = s ? s.homeWon + s.awayWon + 1 : undefined;
			return hl(
				gameNo === undefined
					? `${poss(shot.name)} ${shot.shot} decides a playoff thriller`
					: pick(
							rng,
							[
								`${poss(shot.name)} ${shot.shot} wins Game ${gameNo} for ${tw}`,
								`${poss(shot.name)} ${shot.shot} lifts ${tw} past ${l} in Game ${gameNo}`,
							],
							"dayHeadlinePlayoffShot",
						),
			);
		}

		// A play-in game is single elimination, not a series - it has its own
		// stakes and none of the series language applies to it.
		if (marquee.playIn) {
			const kind = marquee.playIn.kind;
			const prize = marquee.playIn.prizeSeed;
			const seedBits =
				typeof prize === "number"
					? [`${w} claim the #${prize} seed`, `${w} lock up the #${prize} seed`]
					: [];
			return hl(
				pick(
					rng,
					kind === "seed9v10"
						? [
								`${w} end ${poss(l)} season in the play-in`,
								`${w} survive and advance in the play-in`,
								`${w} live to play again, and ${l} do not`,
							]
						: kind === "final"
							? [
									`${w} grab the last playoff spot`,
									`${w} take the final berth from ${l}`,
									...seedBits,
								]
							: [
									...seedBits,
									`${w} take the play-in game from ${l}`,
									`${w} come through the play-in against ${l}`,
								],
					`dayHeadlinePlayIn:${kind}`,
				),
			);
		}

		// The generic case, said in terms of where the series now stands.
		const stateBits: string[] = [];
		if (ss) {
			if (ss.gameNo === 1) {
				stateBits.push(
					`${w} take the opener from ${l}`,
					`${w} take Game 1 from ${l}`,
				);
			} else if (ss.wAfter === ss.lBefore) {
				stateBits.push(
					`${w} pull even with ${l}`,
					`${w} answer back against ${l}`,
				);
			} else if (ss.wAfter === ss.need - 1 && ss.wAfter - ss.lBefore >= 2) {
				// Genuinely in command: one win away, and two clear.
				stateBits.push(
					`${w} take command against ${l}`,
					`${w} push ${l} to the brink`,
				);
			} else if (ss.wAfter > ss.lBefore) {
				// The bracket's story when a low seed leads a high one: say the
				// seeds, not just the count.
				if (
					ss.wSeed !== undefined &&
					ss.lSeed !== undefined &&
					ss.wSeed - ss.lSeed >= 3 &&
					// Finals seeds are from different conferences - not comparable.
					!ss.finals
				) {
					stateBits.push(
						`#${ss.wSeed} ${w} have the #${ss.lSeed} seed in trouble`,
						`${w}, seeded #${ss.wSeed}, lead ${l} ${ss.wAfter}-${ss.lBefore}`,
					);
				}
				stateBits.push(
					`${w} take a ${ss.wAfter}-${ss.lBefore} lead on ${l}`,
					// "Edge ahead" is a one-game lead; "Pistons edge ahead of the
					// Trail Blazers" headlined a 2-0.
					ss.wAfter - ss.lBefore === 1
						? `${w} edge ahead of ${l}`
						: `${w} go up ${ss.wAfter}-${ss.lBefore} on ${l}`,
				);
			} else {
				stateBits.push(
					`${w} get one back against ${l}`,
					`${w} cut ${poss(l)} lead to ${ss.lBefore}-${ss.wAfter}`,
					...(ss.wAfter === 1 ? [`${w} get on the board against ${l}`] : []),
				);
			}
		}
		if (mStar) {
			const sh = starHeadline(mStar);
			// A 42-point night AND the series: "Dunn scores 42 as the Cavaliers
			// take a 2-1 lead on the Pacers", not one or the other.
			const starState =
				mStar.pts >= 30 && stateBits.length > 0
					? // Only the bits that open with the bare nickname `w` can take the
						// star prefix; swap in the articled form rather than stripping a
						// word, which mangled a multi-word nickname and once produced "as
						// the take a 2-1 lead". (The seed-first upset bits don't open on
						// the nickname, and slicing them mid-word made hash.)
						stateBits
							.filter((bit) => bit.startsWith(w))
							.map(
								(bit) =>
									`${mStar.name} scores ${mStar.pts} as ${tw}${bit.slice(w.length)}`,
							)
					: [];
			return hl(
				pick(
					rng,
					// A 35-point night is the headline, with the series in it.
					mStar.pts >= 35 && starState.length > 0
						? starState
						: [
								`${poss(mStar.name)} ${sh.text} power${sh.plural ? "" : "s"} ${tw} past ${l}`,
								...stateBits,
								...starState,
							],
					"dayHeadlinePlayoffStar",
				),
			);
		}
		if (stateBits.length > 0) {
			return hl(pick(rng, stateBits, "dayHeadlinePlayoffState"));
		}
		return hl(`${w} ${pick(rng, verbPool(marquee, mShape))} ${l}`);
	}

	// --- Regular season: a LEAGUE-scope headline. The marquee moment is framed
	// against what the whole night looked like (upset count, a monster stat line,
	// the score), so the headline reads like a league wrap, not one game's.
	const facts = games
		.filter((g) => !g.allStar)
		.map((g) => ({ g, shape: analyzeShape(g) }));
	const upsets = facts.filter(({ g, shape }) => isUpset(g, shape));
	const topPts = [...performers].sort((a, b) => b.p.pts - a.p.pts)[0];

	// A walk-off is the day's story - and when favorites fell all over the
	// league, say both.
	const shot = clutchShot(marquee);
	const mSkid = snappedSkid(mShape);
	if (shot && !shot.tying && mSkid !== undefined) {
		return hl(
			pick(rng, [
				`${w} end ${aNum(mSkid)}-game skid on ${poss(shot.name)} ${shot.shot}`,
				`${poss(shot.name)} ${shot.shot} ends ${poss(tw)} ${mSkid}-game losing streak`,
			]),
		);
	}
	if (shot && !shot.tying) {
		if (upsets.length >= 2) {
			return hl(
				pick(rng, [
					`${poss(shot.name)} ${shot.shot} caps a night of upsets`,
					`${shot.name} sinks ${l} at the wire on a night ${numWord(upsets.length)} favorites fell`,
					`${cap(numWord(upsets.length))} favorites fall, and ${shot.name} finishes the night with a ${shot.shot}`,
					`${shot.name} beats ${l} at the wire as the night goes to the underdogs`,
					`${shot.name} settles it late as ${numWord(upsets.length)} favorites go down`,
					`A ${shot.shot} from ${shot.name} decides the pick of an upset-filled night`,
				]),
			);
		}
		return hl(
			pick(rng, [
				`${poss(shot.name)} ${shot.shot} sinks ${l} ${scoreTag(mShape)}`,
				`${shot.name} walks off ${l} ${scoreTag(mShape)}`,
				`${shot.name} wins it for ${tw} with a ${shot.shot}`,
			]),
		);
	}

	// A long skid ending anywhere on the slate is a league story. The marquee
	// game first; otherwise the longest one on the night, if it is long enough
	// to outrank the marquee's own angle.
	if (mSkid !== undefined && mSkid >= 6) {
		return hl(
			pick(rng, [
				`${w} end ${aNum(mSkid)}-game skid against ${l}`,
				`${w} finally win one, snapping ${aNum(mSkid)}-game losing streak`,
				`${mSkid} straight losses, and then ${tw} beat ${l}`,
			]),
		);
	}
	const longestSkid = facts
		.map(({ g, shape }) => ({ g, shape, skid: snappedSkid(shape) ?? 0 }))
		.filter((x) => x.skid >= 10)
		.sort((a, b) => b.skid - a.skid)[0];
	if (longestSkid) {
		const sw = nick(longestSkid.shape.winner);
		const sl = theNick(longestSkid.shape.loser);
		return hl(
			pick(rng, [
				`${sw} end ${aNum(longestSkid.skid)}-game skid against ${sl}`,
				`${longestSkid.skid} straight losses, and then the ${sw} beat ${sl}`,
			]),
			longestSkid.g,
		);
	}

	// Two or more 40-point nights across the league.
	// The bigger night first: "Payton Mathis goes for 42, Jalen Hayes for 44".
	const fortyClub = performers
		.filter((perf) => perf.p.pts >= 40)
		.sort((x, y) => y.p.pts - x.p.pts);
	if (fortyClub.length >= 2) {
		const [a, b] = fortyClub;
		// Both men are the headline, so the body tells both: "Jalen Hayes
		// goes for 44, Payton Mathis for 42" over a body that gave the Jazz's
		// 121-88 win and never said who scored 42 in it.
		return {
			...hl(
				pick(rng, [
					`${poss(a!.p.name)} ${a!.p.pts} and ${poss(b!.p.name)} ${b!.p.pts} light up the night`,
					`${a!.p.name} goes for ${a!.p.pts}, ${b!.p.name} for ${b!.p.pts}`,
					`${a!.p.pts} for ${a!.p.name}, ${b!.p.pts} for ${b!.p.name} on a night the scorers took over`,
				]),
			),
			players: [a!.p, b!.p],
		};
	}

	if (mStar && mStar.pts >= 45) {
		return hl(
			pick(rng, [
				`${mStar.name} erupts for ${mStar.pts} to lead ${tw} past ${l}`,
				`${mStar.name} drops ${mStar.pts} in ${poss(tw)} win`,
				`${mStar.name} goes off for ${mStar.pts} as ${tw} beat ${l}`,
			]),
		);
	}

	// Favorites fell league-wide: lead with the biggest shock and the count.
	if (upsets.length >= 3) {
		const biggest = [...upsets].sort(
			(a, b) => (b.g.spread?.points ?? 0) - (a.g.spread?.points ?? 0),
		)[0]!;
		const bw = nick(biggest.shape.winner);
		const bl = theNick(biggest.shape.loser);
		return hl(
			pick(rng, [
				`${bw} stun ${bl} on a night ${numWord(upsets.length)} favorites fell`,
				// Not "Three favorites fall, Kings over the Cavaliers the biggest
				// of them": a headline with no verb for its subject.
				`${bw} top ${bl} as ${numWord(upsets.length)} favorites fall`,
				`${bw} knock off ${bl} to headline a ${numWord(upsets.length)}-upset night`,
			]),
			biggest.g,
		);
	}

	if (mStar && doubleCategories(mStar).length >= 3) {
		return hl(
			pick(rng, [
				`${mStar.name} triple-doubles to lead ${tw} past ${l}`,
				`${poss(mStar.name)} triple-double carries ${tw} over ${l}`,
				`${mStar.name} triple-doubles and ${tw} handle ${l}`,
			]),
		);
	}

	if (isUpset(marquee, mShape)) {
		const pts = marquee.spread?.points;
		return hl(
			pick(rng, [
				// The seeds and the streaks are a headline's business; the
				// betting line is the body's.
				...(pts !== undefined && pts >= 7
					? [
							`${w} stun ${l} ${scoreTag(mShape)}`,
							`${w} shock ${l} ${scoreTag(mShape)}`,
						]
					: [`${w} upset ${l} ${scoreTag(mShape)}`]),
				`${w} pull the upset over ${l} ${scoreTag(mShape)}`,
				...(mShape.loser.streakBefore?.won &&
				mShape.loser.streakBefore.count >= 5
					? [
							`${w} snap ${poss(l)} ${mShape.loser.streakBefore.count}-game winning streak`,
						]
					: []),
			]),
		);
	}

	if (mShape.ot > 0) {
		return hl(
			pick(rng, [
				`${w} outlast ${l} ${scoreTag(mShape)}`,
				`${w} survive ${l} in ${otPhrase(mShape.ot)}`,
				`${w} need ${
					mShape.ot === 1
						? "an extra period"
						: `${numWord(mShape.ot)} extra periods`
				} to beat ${l}`,
			]),
		);
	}

	if (comebackSize(mShape) >= 15) {
		return hl(
			pick(rng, [
				`${w} storm back from ${comebackSize(mShape)} down to beat ${l}`,
				`${w} erase ${aNum(comebackSize(mShape))}-point hole to beat ${l}`,
				`Down ${comebackSize(mShape)}, ${w} come back to beat ${l}`,
			]),
		);
	}

	if (mShape.margin >= 25) {
		return hl(
			pick(rng, [
				`${w} rout ${l} by ${mShape.margin}`,
				`${w} blow out ${l} ${scoreTag(mShape)}`,
				`${w} bury ${l} by ${mShape.margin}`,
			]),
		);
	}

	if (mShape.margin <= 3) {
		return hl(
			pick(rng, [
				`${w} edge ${l} in ${aFigure(scoreTag(mShape))} thriller`,
				...(heldOn(marquee) ? [`${w} hold off ${l} at the wire`] : []),
				`${w} survive ${l} ${scoreTag(mShape)}`,
			]),
		);
	}

	// Nothing dramatic anywhere: lead with the night's biggest individual line.
	if (topPts && topPts.p.pts >= 32) {
		return hl(
			pick(rng, [
				`${topPts.p.name} pours in ${topPts.p.pts} to headline the night`,
				`${topPts.p.pts} from ${topPts.p.name} tops the night's scoring`,
				`${topPts.p.name} goes for ${topPts.p.pts} on a quiet night around the league`,
			]),
		);
	}
	if (mStar) {
		return hl(
			pick(rng, [
				`${poss(mStar.name)} ${starHeadline(mStar).text} lead${
					starHeadline(mStar).plural ? "" : "s"
				} ${tw} past ${l}`,
				`${w} ${pick(rng, verbPool(marquee, mShape))} ${l} behind ${mStar.name}`,
			]),
		);
	}
	return hl(`${w} ${pick(rng, verbPool(marquee, mShape))} ${l}`);
};

// A headline-style storyline for the day's DECK - the secondary headlines that
// run under the lead. Each is one big, self-contained thing that happened (a
// monster line, a rout, a hot streak, another upset), so the deck reads like a
// sports-page kicker covering the whole night rather than restating the marquee.
type Storyline = {
	score: number;
	kind: string;
	text: string; // headline-cased, no trailing period
	tids: number[];
	pid?: RecapPlayer;
	// The game this storyline IS (for game-result kinds), so the body can skip a
	// result the deck already headlined. Individual-performance storylines leave
	// this undefined - their game's result can still show in the roundup.
	game?: RecapGame;
};

const collectStorylines = (
	games: RecapGame[],
	performers: LeaguePerformance[],
): Storyline[] => {
	const out: Storyline[] = [];

	for (const g of games) {
		if (g.allStar) {
			continue;
		}
		const shape = analyzeShape(g);
		const w = nick(shape.winner);
		const l = theNick(shape.loser);
		const tids = [shape.winner.tid, shape.loser.tid];
		const shot = clutchShot(g);

		if (shot && !shot.tying) {
			out.push({
				score: 130 + notability(g),
				kind: "walkoff",
				text: `${poss(shot.name)} ${shot.shot} sinks ${l}`,
				tids,
				game: g,
			});
		}
		if (isUpset(g, shape)) {
			const pts = g.spread?.points ?? 0;
			out.push({
				score: 90 + pts * 3 + shape.ot * 10,
				kind: "upset",
				text:
					shape.loser.streakBefore?.won && shape.loser.streakBefore.count >= 5
						? `${w} snap ${poss(l)} ${shape.loser.streakBefore.count}-game winning streak`
						: pts >= 7
							? `${w} stun ${l} ${scoreTag(shape)}`
							: `${w} upset ${l} ${scoreTag(shape)}`,
				tids,
				game: g,
			});
		}
		if (comebackSize(shape) >= 15) {
			out.push({
				score: 80 + comebackSize(shape),
				kind: "comeback",
				// "rally from 17 down past the Pandas" split the verb from its
				// object across the whole adverbial. Put the object back next to
				// the verb.
				text: `${w} rally past ${l} from ${comebackSize(shape)} down`,
				tids,
				game: g,
			});
		}
		if (shape.margin >= 25) {
			out.push({
				score: 62 + shape.margin,
				kind: "rout",
				text: `${w} rout ${l} by ${shape.margin}`,
				tids,
				game: g,
			});
		}
		if (shape.ot > 0) {
			out.push({
				score: 70 + shape.ot * 15,
				kind: "ot",
				text: `${w} outlast ${l} in ${shape.ot === 1 ? "OT" : `${shape.ot}OT`}`,
				tids,
				game: g,
			});
		}
		if (shape.margin <= 3 && shape.ot === 0) {
			out.push({
				score: 55,
				kind: "thriller",
				text: `${w} edge ${l} ${scoreTag(shape)}`,
				tids,
				game: g,
			});
		}
		// A team riding a real hot streak.
		if (
			shape.winner.streak?.won &&
			shape.winner.streak.count >= 6 &&
			!(shape.winner.record && shape.winner.record.lost === 0)
		) {
			out.push({
				score: 66 + shape.winner.streak.count * 2,
				kind: "streak",
				text: `${w} make it ${shape.winner.streak.count} straight`,
				tids,
				game: g,
			});
		}
		// A long skid ending. Bigger than a hot streak extending: the win is
		// the first thing to happen to that team in weeks.
		const skid = snappedSkid(shape);
		if (skid !== undefined && skid >= 6) {
			out.push({
				score: 70 + skid * 3,
				kind: "skidEnd",
				text: `${w} end ${aNum(skid)}-game skid`,
				tids,
				game: g,
			});
		}
		// A team still winless deep into the season - a storyline of its own.
		if (
			shape.loser.record &&
			shape.loser.record.won === 0 &&
			shape.loser.record.lost >= 8
		) {
			out.push({
				score: 58 + shape.loser.record.lost,
				kind: "winless",
				text: `${w} drop ${l} to 0-${shape.loser.record.lost}`,
				tids,
				game: g,
			});
		}
	}

	// Individual masterpieces across the league. No `game` set: the deck may
	// headline the player while the body still reports the game result.
	for (const perf of performers) {
		const p = perf.p;
		const cats = doubleCategories(p).length;
		if (p.pts >= 40) {
			out.push({
				score: 100 + p.pts,
				kind: "scorer",
				text: `${p.name} pours in ${p.pts}`,
				tids: [perf.team.tid],
				pid: p,
			});
		} else if (cats >= 3) {
			out.push({
				score: 96,
				kind: "tripdub",
				text: `${p.name} triple-doubles`,
				tids: [perf.team.tid],
				pid: p,
			});
		} else if (p.pts >= 33) {
			out.push({
				score: 60 + p.pts,
				kind: "bigline",
				text: `${p.name} goes for ${starHeadline(p).text}`,
				tids: [perf.team.tid],
				pid: p,
			});
		}
	}

	return out.sort((a, b) => b.score - a.score);
};

// The deck: up to three secondary headlines under the lead, each a DIFFERENT
// kind of story involving DIFFERENT teams than the marquee (and each other), so
// the reader sees the night's biggest handful of stories at a glance instead of
// just the one marquee game. Returns the games it headlined as results, so the
// body can cover OTHER games instead of restating them verbatim.
const dayDeck = (
	storylines: Storyline[],
	marquee: RecapGame,
	mStar: RecapPlayer | undefined,
	// The game the headline is about, which is not always the marquee. Its teams
	// are excluded for the same reason the marquee's are: the deck exists to add
	// stories, not to say the headline again in different words.
	headlineGame?: RecapGame,
	// Returns the PLAYERS it featured as well as the games. Without them the
	// body re-told the same man two lines below the deck that had just named
	// him: "Adrian Murphy triple-doubles" over "Adrian Murphy put together a
	// triple-double (23 points, 11 rebounds, and 10 assists)".
	// And the headline's words: "Payton Mathis goes for 42, Jalen Hayes for
	// 44" over a deck reading "Payton Mathis pours in 42".
	headlineText = "",
): { text: string; games: RecapGame[]; players: RecapPlayer[] } | undefined => {
	const usedTids = new Set<number>([
		marquee.teams[0].tid,
		marquee.teams[1].tid,
		...(headlineGame
			? [headlineGame.teams[0].tid, headlineGame.teams[1].tid]
			: []),
	]);
	const usedKinds = new Set<string>();
	const picks: string[] = [];
	const covered: RecapGame[] = [];
	const featured: RecapPlayer[] = [];
	for (const s of storylines) {
		if (s.tids.some((tid) => usedTids.has(tid))) {
			continue; // don't reuse the marquee's teams or a team already in the deck
		}
		if (usedKinds.has(s.kind)) {
			continue; // force variety - no two clauses of the same kind
		}
		if (mStar && s.pid === mStar) {
			continue;
		}
		if (s.pid && headlineText.includes(s.pid.name)) {
			continue;
		}
		picks.push(s.text);
		usedKinds.add(s.kind);
		for (const tid of s.tids) {
			usedTids.add(tid);
		}
		if (s.game) {
			covered.push(s.game);
		}
		if (s.pid) {
			featured.push(s.pid);
		}
		if (picks.length >= 3) {
			break;
		}
	}
	return picks.length > 0
		? { text: picks.join(" · "), games: covered, players: featured }
		: undefined;
};

// A compact scoreboard-style clause for the "Around the league" sweep, with a
// verb chosen to fit the margin (a blowout "routs", a nail-biter "edges"). `seq`
// rotates the verb DETERMINISTICALLY through the bucket, so a long roundup reads
// "beat ... downed ... got past ..." instead of "beat ... beat ... beat".
const ROUT_VERBS = ["routed", "blew out", "ran away from", "rolled past"];
const BIG_ROUT_VERBS = ["routed", "blew out", "crushed", "trounced"];
const EDGE_VERBS = ["edged", "held off", "slipped past"];

// The first verb of the pool, from `seq` on, that the wrap has not used yet
// (all of them used: the rotation's own). "Elsewhere, the 76ers routed the
// Celtics and the Raptors routed the Heat. Also on the night, the Hornets
// routed the Rockets" was the notable list and the roundup each rotating
// on its own.
const freshVerb = (
	pool: readonly string[],
	seq: number,
	used?: Set<string>,
): string => {
	for (let k = 0; k < pool.length; k++) {
		const verb = pool[(seq + k) % pool.length]!;
		if (!used?.has(verb)) {
			used?.add(verb);
			return verb;
		}
	}
	return pool[seq % pool.length]!;
};

const roundupClause = (
	game: RecapGame,
	shape: Shape,
	seq: number,
	used?: Set<string>,
): string => {
	const w = theNick(shape.winner);
	const l = theNick(shape.loser);
	if (shape.margin === 0) {
		return game.shootout
			? `${w} beat ${l} in a shootout, ${shape.winner.pts}-${shape.loser.pts}`
			: `${w} and ${l} tied ${shape.winner.pts}-${shape.loser.pts}`;
	}
	let pool: string[];
	// A game won at the wire that the deck has already flagged still lands
	// here for its score; "edged" undersells what the deck just said.
	const shot = clutchShot(game);
	if (shot && !shot.tying) {
		return `${w} beat ${l} ${scoreTag(shape)} at the wire`;
	}
	// "The Nets blew out the Bulls" is not how a game they trailed by 17
	// reads, whatever the final margin.
	if (comebackSize(shape) >= 15) {
		return `${w} came from ${comebackSize(shape)} down to beat ${l} ${scoreTag(shape)}`;
	}
	if (shape.ot > 0) {
		pool =
			shape.margin > 6
				? ["outlasted", "pulled away from"]
				: ["outlasted", "survived", "edged"];
	} else if (shape.margin >= 20) {
		pool = shape.margin >= 35 ? BIG_ROUT_VERBS : ROUT_VERBS;
	} else if (shape.margin <= 4) {
		pool = [...EDGE_VERBS, "outlasted"].filter(
			(v) => v !== "held off" || heldOn(game),
		);
	} else if (shape.margin <= 9) {
		pool =
			closeLate(game) && heldOn(game)
				? ["beat", "got past", "held off", "topped", "took down"]
				: ["beat", "got past", "topped", "took down", "handled"];
	} else {
		pool = [
			"beat",
			"downed",
			"took down",
			"handled",
			"topped",
			"knocked off",
			"pulled away from",
		];
	}
	return `${w} ${freshVerb(pool, seq, used)} ${l} ${scoreTag(shape)}`;
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
	if (hottest && hottest.fgp >= 55) {
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
	if (bombs && bombs.tp >= 17) {
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

// How many of the day's games were decided by 5 or fewer.
// WHAT KIND OF NIGHT IT WAS, in one line: the home teams swept, the road
// teams won five of six, every favorite held serve, four favorites fell.
// The notes a wrap writer reaches for first, and the engine had none of
// them - it listed results and let the reader do the counting.
const nightThemeSentence = (
	games: RecapGame[],
	rng: () => number,
): string | undefined => {
	const real = games.filter((g) => !g.allStar && !g.playoffs);
	if (real.length < 5) {
		return undefined;
	}
	let homeWins = 0;
	let favWins = 0;
	let lined = 0;
	let upsets = 0;
	for (const g of real) {
		if (g.winnerTid === g.teams[0].tid) {
			homeWins += 1;
		}
		if (g.spread && g.spread.points > 0) {
			lined += 1;
			if (g.spread.favTid === g.winnerTid) {
				favWins += 1;
			} else {
				upsets += 1;
			}
		}
	}
	const n = real.length;
	const roadWins = n - homeWins;
	const options: string[] = [];
	if (homeWins === n) {
		options.push(
			`The home teams went ${n}-0.`,
			`Every home team won - ${numWord(n)} for ${numWord(n)}.`,
		);
	} else if (roadWins === n) {
		options.push(
			`The road teams went ${n}-0.`,
			`Not one home team won: ${numWord(n)} road wins in ${numWord(n)} games.`,
		);
	} else if (roadWins >= 0.75 * n) {
		options.push(
			`Road teams went ${roadWins}-${homeWins} on the night.`,
			`It was a night for the visitors, who went ${roadWins}-${homeWins}.`,
		);
	} else if (homeWins >= 0.85 * n) {
		options.push(`Home teams went ${homeWins}-${roadWins} on the night.`);
	}
	if (lined >= 5 && upsets === 0) {
		options.push(
			`Chalk held everywhere: all ${numWord(lined)} favorites won.`,
			`The favorites went ${lined}-0.`,
		);
	} else if (lined >= 5 && upsets >= 3 && upsets >= 0.5 * lined) {
		// Half of them, at least: favorites lose a third of the time, and
		// "Three of the seven favorites lost" is a normal night.
		options.push(
			`${cap(numWord(upsets))} of the ${numWord(lined)} favorites lost.`,
			...(favWins >= upsets
				? [`The favorites went just ${favWins}-${upsets}.`]
				: [`Only ${numWord(favWins)} of the ${numWord(lined)} favorites won.`]),
		);
	}
	if (options.length === 0) {
		return undefined;
	}
	return pick(rng, options, "nightTheme");
};

const closeGamesSentence = (
	games: RecapGame[],
	// The sentence before it, so "Four of the ten favorites lost. Four of the
	// ten games were decided..." does not run the same frame twice.
	before = "",
): string | undefined => {
	const nonExhibition = games.filter((g) => !g.allStar);
	if (nonExhibition.length < 4) {
		return undefined;
	}
	let close = 0;
	let ot = 0;
	for (const g of nonExhibition) {
		const shape = analyzeShape(g);
		if (shape.margin <= 5) {
			close += 1;
		}
		if (shape.ot > 0) {
			ot += 1;
		}
	}
	// OT games are already surfaced individually (in "Elsewhere" and the roundup's
	// scores), so only call out an unusually OT-heavy night here; otherwise the
	// close-game count is the more informative league note.
	// A night of them, not the usual share: "Three of the 15 games were
	// decided by five points or fewer" is any night, and ran in a quarter of
	// all wraps.
	if (close >= 3 && close >= 0.4 * nonExhibition.length) {
		return /^\w+ of the /.test(before)
			? `${cap(numWord(close))} games were decided by five points or fewer.`
			: `${cap(numWord(close))} of the ${numWord(nonExhibition.length)} games were decided by five points or fewer.`;
	}
	if (ot >= 3) {
		return `${ot === nonExhibition.length ? "All" : cap(numWord(ot))} games went to overtime.`;
	}
	return undefined;
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
		return fixArticles(
			buildDayRecap({ ...input, games: input.games.map(withSettledLead) }),
		);
	} finally {
		endRecapBatch();
	}
};

const buildDayRecap = (input: AutoDayRecapInput): string => {
	const { games, standings, day, playoffs } = input;

	const rng = rngFromSeed((day + 1) * 40503 + games.length * 97);

	if (games.length === 0) {
		return "";
	}

	// The All-Star showcase gets its own wrap.
	const allStarGame = games.find((g) => g.allStar);
	if (allStarGame && games.length <= 2) {
		return getAutoRecap(allStarGame);
	}

	const ranked = [...games].sort((a, b) => notability(b) - notability(a));
	const marquee = ranked[0]!;
	const performers = collectPerformances(games);
	const topScorer = [...performers].sort((a, b) => b.p.pts - a.p.pts)[0];

	const mShape = analyzeShape(marquee);
	const mStar = storyPlayerOf(marquee, mShape);
	const headline = dayHeadline(
		marquee,
		mShape,
		mStar,
		games,
		performers,
		playoffs,
		rng,
	);

	// The deck: two or three secondary headlines under the lead, so the reader
	// sees the night's biggest handful of stories at a glance, not just the
	// marquee. Skipped in the playoffs, where the marquee series IS the story.
	//
	// The headline's own game is barred from it. On a night of upsets the lead
	// is the biggest shock rather than the marquee, and the deck would then pick
	// that same shock as its first clause - so the page opened "Monuments stun
	// the Riots on a night four favorites fell" over a deck reading "Monuments
	// stun the Riots as 10.5-point underdogs".
	const deckResult = playoffs
		? undefined
		: dayDeck(
				collectStorylines(games, performers),
				marquee,
				mStar,
				headline.game,
				headline.text,
			);
	const deck = deckResult?.text;

	// Games already told in full (the marquee, a game the deck already headlined,
	// a featured performance, a dramatic "Elsewhere" result) so the body doesn't
	// restate them and the "Around the league" sweep mops up only what's left.
	const coveredGames = new Set<RecapGame>([marquee]);
	if (deckResult) {
		for (const g of deckResult.games) {
			coveredGames.add(g);
		}
	}

	// The playoff games whose series state rides on their result; the
	// series list further down tells only the rest.
	const seriesTold = new Set<RecapGame>();
	const tailsUsed = new Map<string, number>();
	// A night of openers: once the lead has said "in Game 1 of the First
	// Round", "in Game 1", "to open the series", "who lead the series 1-0"
	// and "who took Game 1" on every line after it say it four more times.
	const openers =
		playoffs &&
		games.every(
			(g) => !g.series || (g.series.homeWon === 0 && g.series.awayWon === 0),
		);
	// Set once the lead has said it.
	let openerTold = false;
	const seriesWords = (g: RecapGame, which: "tails" | "whos"): string => {
		if (openerTold && g !== marquee) {
			seriesTold.add(g);
			return "";
		}
		const t = playoffs ? seriesTail(g) : undefined;
		// Once per game: the leading scorer's line and a losing line from the
		// same game both carried it - "...win over the Suns to even the series
		// 1-1" and then "...loss to the Heat, who tied the series 1-1".
		if (!t || seriesTold.has(g)) {
			return "";
		}
		seriesTold.add(g);
		const n = tailsUsed.get(t.kind) ?? 0;
		tailsUsed.set(t.kind, n + 1);
		const pool = t[which];
		return pool[n % pool.length]!;
	};
	const tailFor = (g: RecapGame): string => {
		const t = seriesWords(g, "tails");
		return t ? ` ${t}` : "";
	};
	const whoFor = (g: RecapGame): string => {
		const t = seriesWords(g, "whos");
		return t ? `, ${t}` : "";
	};
	// "...beat the Clippers at the wire, 101-100, to even the series": a
	// clause that ends on a set-off score closes it with a comma first.
	const withTail = (g: RecapGame, clause: string): string => {
		const tail = tailFor(g);
		return tail && /, \d+-\d+(?: \(\w+\))?$/.test(clause)
			? `${clause},${tail}`
			: `${clause}${tail}`;
	};

	// One line about one player, in one of several shapes. Every performance
	// sentence used to end "in the X's win over the Y", so a paragraph naming
	// three standouts read as the same sentence three times with the nouns
	// swapped. pick() keeps the same frame from being used twice on a night.
	const perfLine = (
		perf: LeaguePerformance,
		line: string,
		verb: string,
	): string => {
		const team = theNick(perf.team);
		const opp = theNick(perf.opp);
		// With the score. A performer sentence was the only place three games
		// in seven got mentioned, and "in the Warriors' win over the Thunder"
		// left the reader without a final for it.
		const score = scoreTag(analyzeShape(perf.game));
		if (!perf.won) {
			const who = whoFor(perf.game);
			return pick(
				rng,
				[
					`${perf.p.name} ${verb} ${line} in ${poss(team)} ${score} loss to ${opp}${who}.`,
					`${perf.p.name} ${verb} ${line} for ${team} in ${aFigure(score)} loss to ${opp}${who}.`,
					...(who
						? []
						: [
								`${perf.p.name} ${verb} ${line}, but ${team} lost to ${opp} ${score}.`,
							]),
				],
				"dayPerfLoss",
			);
		}
		const tail = tailFor(perf.game);
		return pick(
			rng,
			[
				`${perf.p.name} ${verb} ${line} in ${poss(team)} ${score} win over ${opp}${tail}.`,
				`${perf.p.name} ${verb} ${line} as ${team} beat ${opp} ${score}${tail}.`,
				`${cap(team)} got ${line} from ${perf.p.name} in ${aFigure(score)} win over ${opp}${tail}.`,
			],
			"dayPerfWin",
		);
	};

	// Paragraph 1: the marquee game and the day's best individual nights. The
	// marquee star is already covered in the blurb, so the standouts that follow
	// come from OTHER games and no player is named twice.
	const para1: string[] = [];
	// Does the headline already name both sides of this game? Then it has given
	// the result, and the lead has to arrive at it from somewhere else.
	const headlineHasResult =
		headline.game === marquee &&
		headline.text.includes(nick(mShape.winner)) &&
		headline.text.includes(nick(mShape.loser));
	// The marquee's series, unless the headline already gave it.
	const headlineHasSeries =
		/\d-\d(?!\d)|sweep|close|put .* out|finish|eliminat|advance|move on|through|game \d|force|even|title|champion/i.test(
			headline.text.replaceAll(/\b\d{2,3}-\d{2,3}\b/g, ""),
		);
	para1.push(
		`${cap(gameBlurb(marquee, rng, headlineHasResult))}${headlineHasSeries ? "" : tailFor(marquee)}.`,
	);
	// THE ROUND, ONCE. "Obi King went for 16 points, 13 rebounds, and 3
	// steals as the Suns rallied past the Hornets 105-99" opened the
	// playoffs and never said which round they were in; 60 of 100
	// postseason wraps named none. The lead names it when nothing above it
	// has.
	// A series it ended is told by where the winners go next ("The Raptors
	// move on to the Conference Finals").
	const mSeries = marquee.series;
	const mWinnerHome = mSeries?.homeAbbrev === mShape.winner.abbrev;
	const mEnded =
		!!mSeries &&
		(mWinnerHome ? mSeries.homeWon : mSeries.awayWon) + 1 ===
			Math.floor((mSeries.bestOf ?? 1) / 2) + 1;
	if (
		playoffs &&
		mSeries &&
		(mSeries.bestOf ?? 0) > 1 &&
		!mEnded &&
		!/Round|Semifinals|Finals/.test(`${headline.text} ${para1[0]!}`)
	) {
		const round = roundName(mSeries.round, mSeries.numRounds);
		const gameNo = mSeries.homeWon + mSeries.awayWon + 1;
		const lead = para1[0]!;
		para1[0] = /\bin Game \d+\b/.test(lead)
			? lead.replace(/\b(in Game \d+)\b/, `$1 of ${round}`)
			: /series|Game \d/.test(lead)
				? lead.replace(/\.$/, ` in ${round}.`)
				: lead.replace(/\.$/, ` in Game ${gameNo} of ${round}.`);
	}
	openerTold =
		openers && /\bGame 1\b|\bopener\b/.test(`${headline.text} ${para1[0]!}`);

	// When the headline is about a DIFFERENT game, that game leads the body -
	// a reader should not have to take the headline on trust. Previously the
	// deck covered it, which made the roundup skip it as already told, and the
	// night's stated lead story appeared nowhere in the wrap.
	if (headline.game !== marquee && !coveredGames.has(headline.game)) {
		para1.push(
			`${cap(gameBlurb(headline.game, rng))}${tailFor(headline.game)}.`,
		);
		coveredGames.add(headline.game);
	}

	const marqueeTids = new Set([mShape.winner.tid, mShape.loser.tid]);
	// The game the most recent performance sentence was about, so the next one
	// does not come straight back at it from the other side.
	let lastPerfGame: RecapGame | undefined;
	const named = new Set<RecapPlayer>();
	if (mStar) {
		named.add(mStar);
	}
	// THE DECK IS A TEASE, AND THE BODY TELLS IT. A deck player used to count
	// as named, so the body never told him: in all 73 decks of a 90-day corpus
	// run, "Marcus Green pours in 40" was the last the reader heard of him -
	// for whom, against whom, won or lost - and his game could be told through
	// a teammate's 25 instead. The body now picks its standouts as if there
	// were no deck, and any deck player it did not pick rides on his game's
	// result: "the Cavaliers erased an 18-point deficit to beat the Clippers
	// 108-103 despite Marcus Green's 40 points".
	const deckPerfByGame = new Map<RecapGame, LeaguePerformance[]>();
	for (const p of new Set([
		...(deckResult?.players ?? []),
		...(headline.players ?? []).filter((p) => p !== mStar),
	])) {
		const perf = performers.find((x) => x.p === p);
		if (perf) {
			deckPerfByGame.set(perf.game, [
				...(deckPerfByGame.get(perf.game) ?? []),
				perf,
			]);
		}
	}
	// Points alone, as the deck had them: the clause sits in a list joined
	// by "and", and "40 points and 9 rebounds and the Suns routed the Spurs"
	// lost the reader.
	const deckLine = (perf: LeaguePerformance) =>
		doubleCategories(perf.p).length >= 3
			? "triple-double"
			: plural(perf.p.pts, "point");
	// The deck players of this game the page has not named yet.
	const untold = (g: RecapGame, text: string) =>
		(deckPerfByGame.get(g) ?? []).filter((perf) => !text.includes(perf.p.name));
	const withDeckPerf = (g: RecapGame, clause: string): string => {
		let out = clause;
		for (const perf of untold(g, para1.join(" "))) {
			const sep = / as /.test(out) ? "," : "";
			out = `${out}${sep} ${perf.won ? "behind" : "despite"} ${poss(perf.p.name)} ${deckLine(perf)}`;
		}
		return out;
	};

	// The day's leading scorer, when it isn't the marquee star already described.
	if (topScorer && topScorer.p.pts >= 30 && !named.has(topScorer.p)) {
		const line = withFlourish(
			statPhrase(topScorer.p),
			shootingFlourish(topScorer.p),
			true,
		);
		const topScore = scoreTag(analyzeShape(topScorer.game));
		// The sentence before this one has just given the matchup and score
		// when his game is the marquee, so "in the Nets' 124-114 loss to the
		// Clippers" straight after "beat the Nets 124-114" says it twice.
		para1.push(
			coveredGames.has(topScorer.game) &&
				para1.at(-1)?.includes(nick(topScorer.team))
				? `${topScorer.p.name} led all scorers with ${line} in ${
						topScorer.won ? "the win" : "the loss"
					}.`
				: topScorer.won
					? `${topScorer.p.name} led all scorers with ${line} in ${poss(
							theNick(topScorer.team),
						)} ${topScore} win over ${theNick(topScorer.opp)}${tailFor(topScorer.game)}.`
					: `${topScorer.p.name} led all scorers with ${line} in ${poss(
							theNick(topScorer.team),
						)} ${topScore} loss to ${theNick(topScorer.opp)}${whoFor(topScorer.game)}.`,
		);
		named.add(topScorer.p);
		coveredGames.add(topScorer.game);
		lastPerfGame = topScorer.game;
	}

	// A second standout from a different game entirely. Winners only - "added 31
	// points for the Timberwolves" reads as a win contribution, and a big line on
	// the losing side gets the dedicated "losing effort" sentence below instead.
	const secondPerf = performers.find(
		(perf) =>
			!named.has(perf.p) &&
			!marqueeTids.has(perf.team.tid) &&
			// ...and from a game the wrap has not already told. Without this the
			// leading scorer's game came back around immediately from the other
			// side: "Jared Jones led all scorers with 40 despite the Cheesesteaks'
			// loss to the Monuments. Tyrone Allen put together a triple-double as
			// the Monuments beat the Cheesesteaks."
			!coveredGames.has(perf.game) &&
			perf.won &&
			(perf.p.pts >= 25 || doubleCategories(perf.p).length >= 3),
	);
	if (secondPerf) {
		const ddw = doubleWord(doubleCategories(secondPerf.p).length);
		// Always against a named opponent. "Added ... for the Wizards", tacked onto
		// a sentence about a different game entirely, read as though he'd been on
		// the floor for it.
		para1.push(
			ddw && doubleCategories(secondPerf.p).length >= 3
				? perfLine(
						secondPerf,
						`a ${ddw} (${statPhrase(secondPerf.p)})`,
						"posted",
					)
				: perfLine(secondPerf, statPhrase(secondPerf.p), "had"),
		);
		named.add(secondPerf.p);
		coveredGames.add(secondPerf.game);
		lastPerfGame = secondPerf.game;
	}

	// A league-wide triple-double gets a nod if it wasn't already the story.
	const tdPerf = performers.find(
		(perf) =>
			doubleCategories(perf.p).length >= 3 &&
			!named.has(perf.p) &&
			!marqueeTids.has(perf.team.tid) &&
			// Same reason as the second standout: without this, the leading
			// scorer's game came straight back from the other side.
			!coveredGames.has(perf.game),
	);
	if (tdPerf) {
		para1.push(
			perfLine(
				tdPerf,
				`a triple-double (${statPhrase(tdPerf.p)})`,
				"put together",
			),
		);
		named.add(tdPerf.p);
		coveredGames.add(tdPerf.game);
		lastPerfGame = tdPerf.game;
	}

	// A monster line wasted in a loss is a story of its own (a 23-18-8 night on
	// the losing side shouldn't vanish from the league wrap).
	if (para1.length < 5) {
		// Deliberately NOT filtered on coveredGames. This is the other side of
		// a game the wrap may have told from the winner's view, and a 23-18-8
		// wasted in defeat is its own story rather than a restatement of the
		// result.
		//
		// But not straight back at the sentence before it, which reads as a
		// stutter: "Lonnie Dunn had 25 points as the Mavericks beat the Knicks.
		// Walker Ingram's 20 points, 12 assists and 4 steals came in a losing
		// effort against the Mavericks." So another game is PREFERRED - and
		// only preferred, because when the mirror is the only monster line in
		// defeat on the slate, printing it beats losing it.
		// A MONSTER line, though. Any 20-and-10 in defeat made the cut, so
		// "Bennedict Brooks' 27 points and 11 rebounds came in the Nets'
		// 119-96 loss" ran in more than half of all wraps, about lines no
		// desk would carry past the box score - a 26-and-11 in a 25-point
		// rout among them. The bar is the line's size against how far off
		// the game was.
		const lossCandidates = performers.filter(
			(perf) =>
				!named.has(perf.p) &&
				!perf.won &&
				!marqueeTids.has(perf.team.tid) &&
				((perf.p.pts >= 20 && doubleCategories(perf.p).length >= 2) ||
					perf.p.pts >= 35) &&
				loserLineSize(perf.p) >=
					26 + Math.max(0, analyzeShape(perf.game).margin - 10),
		);
		const lossPerf =
			lossCandidates.find((perf) => !coveredGames.has(perf.game)) ??
			lossCandidates.find((perf) => perf.game !== lastPerfGame) ??
			lossCandidates[0];
		if (lossPerf) {
			// When the mirror is the only monster line in defeat on the slate it
			// is still worth printing, but the sentence before it has just named
			// the matchup - so name it once. "...as the Mavericks beat the
			// Knicks. Walker Ingram's 20 points, 12 assists and 4 steals came in
			// a losing effort against the Mavericks."
			const mirror = lossPerf.game === lastPerfGame;
			// A game a sentence above already scored keeps its score there:
			// "Darius Hayes had 31 points as the Heat beat the Lakers 121-88.
			// ... Jaden Green's 22 points came in the Lakers' 121-88 loss to
			// the Heat" gave it twice.
			const lossShape = analyzeShape(lossPerf.game);
			const scored = para1.some(
				(t) =>
					t.includes(plainScore(lossShape)) &&
					t.includes(nick(lossPerf.team)) &&
					t.includes(nick(lossPerf.opp)),
			)
				? ""
				: `${scoreTag(lossShape)} `;
			para1.push(
				mirror
					? `${poss(lossPerf.p.name)} ${statPhrase(lossPerf.p)} came in a losing effort.`
					: // Whose loss: "Bennedict Brooks' 27 points and 11 rebounds
						// came in a 119-96 loss to the Mavericks" never said he
						// played for the Nets (78 lines in 177 wraps).
						`${poss(lossPerf.p.name)} ${statPhrase(
							lossPerf.p,
						)} came in ${poss(theNick(lossPerf.team))} ${scored}loss to ${theNick(lossPerf.opp)}${whoFor(lossPerf.game)}.`,
			);
			coveredGames.add(lossPerf.game);
		}
	}

	// A deck performer whose game a sentence above already told, from either
	// side, rides in right after it: "Marcus Nowell's 25 points and 11
	// rebounds came in a 120-111 loss to the Bulls. Xavier Carter had 34
	// points for the Bulls."
	for (const g of deckPerfByGame.keys()) {
		const score = plainScore(analyzeShape(g));
		const at = para1.findIndex((t) => t.includes(score));
		const left = untold(g, para1.join(" "));
		if (at >= 0 && left.length > 0) {
			para1.splice(
				at + 1,
				0,
				`${naturalList(
					left.map((perf) => {
						const line = deckLine(perf);
						// "Marcus Green had a triple-double for the Clippers" gave the
						// deck's word and none of the line.
						return `${perf.p.name} had ${line === "triple-double" ? `a triple-double (${statPhrase(perf.p)})` : line} for ${theNick(perf.team)}`;
					}),
				)}.`,
			);
		}
	}

	// A deck's game-winner whose game a sentence above told without him:
	// "Keegan Lowry's free throw sinks the Trail Blazers" over "Yuta King's
	// 26 points came in the Trail Blazers' 99-97 loss to the Kings", and the
	// shot was never mentioned again.
	for (const g of deckResult?.games ?? []) {
		const shot = clutchShot(g);
		const at = para1.findIndex((t) => t.includes(plainScore(analyzeShape(g))));
		if (
			shot &&
			!shot.tying &&
			at >= 0 &&
			deck?.includes(shot.name) &&
			!para1.join(" ").includes(shot.name)
		) {
			const when =
				shot.seconds !== undefined && !shot.buzzer
					? ` with ${shot.seconds} seconds left`
					: shot.buzzer
						? " at the buzzer"
						: "";
			para1.splice(
				at + 1,
				0,
				`${cap(poss(shot.name))} ${shot.shot}${when} won it for ${theNick(analyzeShape(g).winner)}.`,
			);
		}
	}

	// Paragraph 2: the rest of the night's RESULTS. First the dramatic ones
	// ("Elsewhere, ..." - upsets, comebacks, walk-offs, blowouts, thrillers) with
	// rotating verbs so three upsets don't read "upset... upset... upset", then an
	// "Around the league" sweep of every remaining game with its score, so you can
	// actually feel caught up on the whole slate instead of just the top stories.
	const para2: string[] = [];
	const others = ranked.slice(1);
	const notableBlurbs: string[] = [];
	// Which opener the notable-results sentence used, so the roundup below can
	// avoid echoing it.
	let notableOpener: string | undefined;
	// "Stunned" and "shocked" belong to a real number. A 3.5-point dog winning
	// gets the flat verbs, so the strong ones still mean something when the
	// 13.5-point dog turns up two clauses later.
	// Seeded with the verbs the opening paragraph already spent: "as the
	// Pistons routed the Magic... In the other games, the Lakers routed the
	// Bulls".
	const usedVerbs = new Set<string>(
		[...ROUT_VERBS, ...BIG_ROUT_VERBS, ...EDGE_VERBS].filter((verb) =>
			para1.some((t) => t.includes(` ${verb} `)),
		),
	);
	const bigUpsetVerbs = ["stunned", "shocked", "toppled", "upset"];
	const smallUpsetVerbs = ["upset", "got past", "took down", "knocked off"];
	let upsetIdx = 0;
	for (const g of others) {
		// A game the deck headlined is still the body's to tell, unless the
		// body has given its final already: "Wizards stun the Thunder
		// 102-100" in the deck and not a word of it underneath left the
		// body short a result - the night's upset, often.
		const deckOnly =
			deckResult?.games.includes(g) &&
			![headline.text, ...para1]
				.join(" ")
				.includes(plainScore(analyzeShape(g)));
		if (g.allStar || (coveredGames.has(g) && !deckOnly)) {
			continue;
		}
		const shape = analyzeShape(g);
		// A tie is told in the roundup, plainly.
		if (shape.margin === 0) {
			continue;
		}
		const shot2 = clutchShot(g);
		let blurb: string | undefined;
		if (isUpset(g, shape)) {
			const spreadPts = g.spread?.points;
			const verbs =
				spreadPts !== undefined && spreadPts >= 7
					? bigUpsetVerbs
					: smallUpsetVerbs;
			const verb = verbs[upsetIdx % verbs.length]!;
			// Every blurb carries its score: these are the only place many of
			// the night's games get told, and the reader wants the final.
			blurb =
				upsetIdx === 0 &&
				spreadPts !== undefined &&
				spreadPts >= 5 &&
				!(playoffs && seriesTail(g))
					? `${theNick(shape.winner)} ${verb} ${theNick(
							shape.loser,
						)} ${scoreTag(shape)} as ${spreadPts}-point underdogs`
					: `${theNick(shape.winner)} ${verb} ${theNick(shape.loser)} ${scoreTag(shape)}`;
			upsetIdx += 1;
		} else if (comebackSize(shape) >= 15) {
			blurb = `${theNick(shape.winner)} erased ${aNum(
				comebackSize(shape),
			)}-point deficit to beat ${theNick(shape.loser)} ${scoreTag(shape)}`;
		} else if (shot2 && !shot2.tying) {
			// No comma inside: these sit in comma-separated lists, and "the
			// Suns survived the 76ers in overtime, 103-97, the Rockets..."
			// read as two items.
			// The team, then the man: "Xavier Hayes beat the Clippers 101-100
			// at the wire" never said whose side he was on.
			blurb = `${theNick(shape.winner)} beat ${theNick(shape.loser)} ${scoreTag(shape)} on ${poss(shot2.name)} ${shot2.shot}`;
		} else if (shape.ot > 0) {
			// "Survived" is a close one, and said once: "the Heat survived the
			// Kings 124-121 in overtime and the Jazz survived the Suns 133-126
			// in overtime".
			blurb = `${theNick(shape.winner)} ${
				shape.ot === 1 && shape.margin <= 3
					? freshVerb(["survived", "outlasted"], 0, usedVerbs)
					: "outlasted"
			} ${theNick(shape.loser)} ${plainScore(shape)} in ${otPhrase(shape.ot)}`;
		} else if (shape.margin >= 25) {
			blurb = `${theNick(shape.winner)} ${freshVerb(shape.margin >= 35 ? BIG_ROUT_VERBS : ROUT_VERBS, 0, usedVerbs)} ${theNick(shape.loser)} ${scoreTag(shape)}`;
		} else if (shape.margin <= 3) {
			blurb = `${theNick(shape.winner)} ${freshVerb(EDGE_VERBS, 0, usedVerbs)} ${theNick(
				shape.loser,
			)} ${scoreTag(shape)}`;
		}
		if (blurb) {
			notableBlurbs.push(withDeckPerf(g, withTail(g, blurb)));
			coveredGames.add(g);
			if (notableBlurbs.length >= 5) {
				break;
			}
		}
	}
	if (notableBlurbs.length > 0) {
		// "There was drama elsewhere too: the Lakers routed the Thunder by 30."
		// A rout is notable and it is not drama. The opener has to match what
		// it introduces, so the excitable ones are kept for a list that
		// actually contains an upset, a comeback, a walk-off or a tight one.
		const anyDrama = notableBlurbs.some((blurb) =>
			/stun|shock|upset|took down|knocked off|got past|erased|overtime|at the wire|edged/.test(
				blurb,
			),
		);
		notableOpener = pick(
			rng,
			anyDrama
				? ["Elsewhere", "There was drama elsewhere too", "Also worth the watch"]
				: ["Elsewhere", "Also on the night", "There was more besides"],
			anyDrama ? "notableOpener" : "notableOpenerFlat",
		);
		para2.push(
			notableOpener === "Elsewhere"
				? `Elsewhere, ${naturalList(notableBlurbs)}.`
				: `${notableOpener}: ${naturalList(notableBlurbs)}.`,
		);
	}
	const notableAt = para2.length - 1;

	// Around the league: sweep up every game not already told, with its score, so
	// nothing on the slate goes unmentioned. Ordered by notability so the more
	// interesting leftovers lead; capped so a huge slate rolls the tail into "and
	// N others" rather than an endless run-on.
	// A WRAP IS NOT A SCOREBOARD. This used to sweep up to ten leftovers and
	// chunk them into groups of four, so a twenty-game night ended with four
	// consecutive sentences of nothing but scores - "Elsewhere... Around the
	// league... Also on the night... In the rest of the schedule..." - which is
	// where the whole thing stopped reading like writing. The full slate is on
	// the scoreboard already; prose should cover a handful and stop.
	const ROUNDUP_CAP = 5;
	const roundupClauses: string[] = [];
	let roundupExtra = 0;
	// A game counts as told when its FINAL is on the page, not when it has
	// been alluded to. The deck's "Spurs drop the Rockets to 0-8" is a tease,
	// not a result, and three games in seven were reaching the reader with no
	// score at all.
	// Not the deck: it is a row of headlines over the body, and a final in it
	// is not yet told underneath.
	const soFar = [headline.text, ...para1, ...para2].join(" ");
	for (const g of ranked) {
		if (g.allStar) {
			continue;
		}
		const gShape = analyzeShape(g);
		if (coveredGames.has(g) && soFar.includes(plainScore(gShape))) {
			continue;
		}
		if (roundupClauses.length < ROUNDUP_CAP) {
			roundupClauses.push(
				withDeckPerf(
					g,
					withTail(
						g,
						roundupClause(g, gShape, roundupClauses.length, usedVerbs),
					),
				),
			);
		} else {
			roundupExtra += 1;
		}
	}
	if (roundupClauses.length > 0) {
		// No "and 5 other games" tail: it dangles off a list of result clauses as a
		// bare noun phrase, and the scoreboard has them anyway.
		const items = roundupClauses;
		// Never the same opener the notable-results sentence just used, and never
		// one that STARTS THE SAME WAY - matching the whole string still allowed
		// "Also worth the watch: ..." to be followed by "Also on the night, ...".
		const notableFirstWord = notableOpener?.split(" ")[0];
		const candidates = [
			// Not over one or two results: "Around the league, the Bulls blew
			// out the Grizzlies 101-80" was the whole league on a two-game night.
			...(items.length >= 3 ? ["Around the league"] : []),
			"Also on the night",
			// "In the other games" over a single clause read as a miscount on a
			// two-game night ("In the other games, the Gold Club beat the Turtles").
			// And only when these ARE the other games, none left off the list.
			...(roundupExtra === 0
				? [items.length === 1 ? "In the other game" : "In the other games"]
				: []),
		].filter((o) => o.split(" ")[0] !== notableFirstWord);
		if (notableBlurbs.length > 0 && notableBlurbs.length + items.length <= 3) {
			// Three results or fewer are one list, not two sentences of one
			// game each: "Elsewhere, the Warriors edged the Pacers 101-98.
			// Around the league, the Heat beat the Spurs 101-83."
			para2[notableAt] =
				`Elsewhere, ${naturalList([...notableBlurbs, ...items])}.`;
		} else {
			const opener = pick(
				rng,
				candidates.length > 0 ? candidates : ["Elsewhere"],
				"roundupOpener",
			);
			para2.push(`${opener}, ${naturalList(items)}.`);
		}
	}

	// The night's own context, shared by the beats below so no team or player
	// gets two sentences of it.
	const dayCtx: DayBeatContext = {
		games,
		standings,
		saidTids: new Set([mShape.winner.tid, mShape.loser.tid]),
		saidPlayers: new Set(
			[mStar, ...(deckResult?.players ?? [])]
				.filter((p): p is RecapPlayer => !!p)
				.map((p) => p.name),
		),
	};

	// Paragraph 3: the league picture - close-game count, injury news, and then
	// either the playoff series state or the streak/standings context.
	const para3: string[] = [];

	const theme = nightThemeSentence(games, rng);
	if (theme) {
		para3.push(theme);
	}
	const close = closeGamesSentence(games, theme);
	if (close) {
		para3.push(close);
	}

	// What kind of night it was, league-wide: the blowout, the shootout, the best
	// shooting performance. Two notes about the night at most, counting the
	// theme and the close games above, so the wrap stays a wrap: "Home teams
	// went 7-1. Three of the eight games were decided by five or fewer. The
	// night's most one-sided result... Nothing on the slate outscored..." was
	// four in a row.
	for (const note of leagueNotes(
		games,
		rng,
		// One in the playoffs, where the series are the news: "Nobody shot it
		// better than the Pistons. No team hit more threes than the Heat"
		// closed a wrap of four elimination games.
		Math.max(0, (playoffs ? 1 : 2) - para3.length),
		[headline.text, deck ?? "", ...para1, ...para2].join(" "),
	)) {
		para3.push(note);
	}

	// The night's injury news - who went down matters to the league picture.
	const injuries = injuryRoundup(
		games,
		rng,
		[headline.text, deck ?? "", ...para1, ...para2, ...para3].join(" "),
	);
	if (injuries) {
		para3.push(injuries);
	}

	if (playoffs) {
		// Series developments across the day, skipping the marquee game (already the
		// story) and any duplicate phrasing.
		const seriesBits: DaySeriesPhrase[] = [];
		const seen = new Set<string>();
		for (const g of games) {
			if (
				seriesTold.has(g) ||
				(marqueeTids.has(g.teams[0].tid) && marqueeTids.has(g.teams[1].tid))
			) {
				continue;
			}
			const phrase = daySeriesPhrase(g, rng);
			if (phrase && !seen.has(phrase.text)) {
				seen.add(phrase.text);
				seriesBits.push(phrase);
			}
			if (seriesBits.length >= 4) {
				break;
			}
		}
		// Nothing left after the marquee - a one-game day. Say what the marquee
		// series now hinges on instead of falling silent.
		// Only on a night of one series: with others told beside it, "the
		// Suns need three more; the Hornets need four" is arithmetic on a
		// state the lead already gave.
		const stakes = games.some((g) => g !== marquee && g.series)
			? undefined
			: dayStakesPhrase(marquee, rng);
		if (stakes) {
			para3.push(stakes);
		} else {
			// A clinch in the marquee game: whose season ended, and how, is the
			// sentence the wrap was missing - on a one-game night it had the
			// result and nothing else, and on a two-game night the series line
			// below skips the marquee, so "the #8 seed puts out the #1 seed" was
			// never said at all.
			const post = postseasonContext(marquee, mShape, rng);
			if (post.clinch) {
				// The marquee's ending goes first, straight after the results:
				// behind "The Pistons made the most threes on the slate" it read
				// as an afterthought to somebody else's night.
				const ending: string[] = [];
				// Where the winner goes next, unless the headline said so.
				// "Wizards take Game 7 from the Warriors" over "The Warriors are
				// done for the year" never said the Wizards were through.
				const toldNext =
					!post.clinch.title &&
					!/advance|move on|reach|through to|champion/i.test(headline.text);
				if (toldNext) {
					ending.push(
						pick(
							rng,
							[
								`${cap(theNick(mShape.winner))} move on to ${post.clinch.nextRound}.`,
								`${cap(theNick(mShape.winner))} are through to ${post.clinch.nextRound}.`,
							],
							"dayClinchNext",
						),
					);
				}
				const { seedUpset, wSeed, lSeed } = post.clinch;
				if (
					seedUpset &&
					!toldNext &&
					!post.clinch.title &&
					wSeed !== undefined &&
					lSeed !== undefined
				) {
					// On a night of several series, "beaten by the No. 8 seed"
					// could be anyone's: the wrap names them.
					ending.push(
						`The ${seeded(lSeed)} ${nick(mShape.loser)} are out, beaten by the No. ${wSeed} ${nick(mShape.winner)}.`,
					);
				} else if (post.sentences[1]) {
					ending.push(post.sentences[1]);
				}
				para3.unshift(...ending);
			}
		}
		if (seriesBits.length > 0) {
			// When every clause belongs to the same round - which is most nights,
			// since rounds run in lockstep - say the round once at the front
			// instead of four times down the sentence.
			const rounds = new Set(seriesBits.map((b) => b.round));
			const shared = rounds.size === 1 ? [...rounds][0] : undefined;
			para3.push(
				shared
					? `In ${shared}, ${naturalList(seriesBits.map((b) => b.text))}.`
					: `In the playoffs, ${naturalList(
							seriesBits.map((b) =>
								b.round ? `${b.text} in ${b.round}` : b.text,
							),
						)}.`,
			);
		}
	} else {
		const streak = teamStreakSentence(games, rng, deck);
		if (streak) {
			para3.push(streak);
		}
		const picture = conferencePictureSentence(
			standings,
			rng,
			dayCtx.saidTids,
			games[0]?.day,
			{
				played: new Set(games.flatMap((g) => g.teams.map((t) => t.tid))),
				won: new Set(games.map((g) => g.winnerTid)),
				rankBefore: new Map(
					games.flatMap((g) =>
						g.teams.flatMap((t) =>
							t.standing?.rankBefore !== undefined
								? [[t.tid, t.standing.rankBefore] as const]
								: [],
						),
					),
				),
			},
		);
		if (picture) {
			para3.push(picture);
		}
	}

	// Paragraph 4: what the night did to the season. Who moved in the table,
	// where the race stands at the cut line, the team that cannot win, the
	// round numbers passed, the season bests, and what is next on the schedule. Every
	// one reads the context the games carry and says nothing when the night
	// gives it nothing, so a quiet Tuesday still ends after paragraph three.
	const para4 = playoffs
		? [dayBracketWatch(dayCtx, rng)].filter((x): x is string => !!x)
		: [
				dayStandingsMovers(dayCtx, rng),
				dayRaceSentence(dayCtx, rng),
				...shuffle(rng, [
					dayColdStreak(dayCtx, rng),
					dayMilestones(dayCtx, rng),
					daySeasonHighs(dayCtx, rng),
				]),
			]
				.filter((x): x is string => !!x)
				.slice(0, 3);
	if (!playoffs) {
		const nextSlate = dayNextSlate(dayCtx, rng);
		if (nextSlate) {
			para4.push(nextSlate);
		}
	}
	// A lone closing sentence is not a paragraph; fold it up rather than let
	// it hang under three full ones.
	if (para4.length === 1 && para3.length > 0 && para3.length <= 4) {
		para3.push(para4.pop()!);
	}

	const paragraphs = [dedupeSubjects(para1).join(" ")];
	if (para2.length > 0) {
		paragraphs.push(dedupeSubjects(para2).join(" "));
	}
	if (para3.length > 0) {
		paragraphs.push(dedupeSubjects(para3).join(" "));
	}
	if (para4.length > 0) {
		paragraphs.push(dedupeSubjects(para4).join(" "));
	}

	const head = deck
		? `**${headline.text}**\n\n*${deck}*`
		: `**${headline.text}**`;
	return `${head}\n\n${paragraphs.join("\n\n")}`;
};

export default getAutoRecap;
