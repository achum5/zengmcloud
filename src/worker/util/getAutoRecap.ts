import type {
	RecapAverages,
	RecapDayStandings,
	RecapGame,
	RecapPlayer,
	RecapTeam,
} from "./getDayGamesForRecap.ts";

// A procedural, no-AI recap engine. getAutoRecap turns one RecapGame into a
// bold headline plus a couple of tight paragraphs; getAutoDayRecap turns a whole
// day's slate into a league-wide wrap. Every clause is anchored to a real number,
// name, or event from the box score - nothing is invented - and wording is varied
// by a per-game seed so a slate never reads from one template. These are the
// always-on recaps; the "Copy AI Prompt" flow stays the on-demand upgrade.

// --- Seeded RNG (mulberry32), so the same game always reads the same way -------

const rngFromSeed = (seed: number): (() => number) => {
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
const shuffle = <T>(rng: () => number, arr: T[]): T[] => {
	const out = [...arr];
	for (let i = out.length - 1; i > 0; i--) {
		const j = Math.floor(rng() * (i + 1));
		[out[i], out[j]] = [out[j]!, out[i]!];
	}
	return out;
};

// --- Small text helpers --------------------------------------------------------

const naturalList = (items: string[]): string => {
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

const stripHtml = (s: string): string =>
	s
		.replace(/<[^>]*>/g, "")
		.replace(/\s+/g, " ")
		.trim();

const cap = (s: string): string =>
	s ? s.charAt(0).toUpperCase() + s.slice(1) : s;

// Soften back-to-back sentences that open with the same "The <Team>" subject by
// turning the second one's subject into "They" - so "The Spurs led wire to wire.
// The Spurs shot 54.7%." becomes "...The Spurs led wire to wire. They shot
// 54.7%." The `[a-z]` guard means a two-word nickname ("The Trail Blazers ...")
// is left alone rather than mangled.
const dedupeSubjects = (sentences: string[], otherNick?: string): string[] => {
	const out = [...sentences];
	// A sentence with two subjects leaves nothing for "They" to attach to (the
	// two-team injury line "The Hawks were without X; the Timberwolves were
	// without Y."). Merely naming the other team as the OBJECT ("The Celtics
	// topped the Grizzlies 115-105.") is still a clean antecedent.
	const ambiguous = (sentence: string) =>
		sentence.includes(";") || sentence.includes(" were without ");

	const subjectOf = (sentence: string) =>
		/^The (\w+) [a-z]/.exec(sentence)?.[1];

	for (let i = 1; i < out.length; i++) {
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
				// Only reach past a sentence that introduces no competing subject - a
				// player's stat line is fine, another team's is not.
				const between = out[i - 1]!;
				if (
					ambiguous(between) ||
					subjectOf(between) ||
					(!!otherNick && between.includes(otherNick))
				) {
					break;
				}
			}
			if (subjectOf(candidate) === cur) {
				out[i] = out[i]!.replace(/^The \w+ /, "They ");
				break;
			}
		}
	}
	return out;
};

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
];
const ordinal = (n: number): string => ORDINALS[n] ?? `${n}th`;

const plural = (n: number, word: string): string =>
	`${n} ${word}${n === 1 ? "" : "s"}`;

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
const numWord = (n: number): string => NUM_WORDS[n] ?? String(n);

const aNum = (n: number): string =>
	n === 8 || n === 11 || n === 18 ? `an ${n}` : `a ${n}`;

// "a sprained ankle" but "an Achilles injury".
const aWord = (s: string): string =>
	/^[aeiou]/i.test(s) ? `an ${s}` : `a ${s}`;

// Injury types come capitalized ("Sprained Ankle"); prose wants "sprained
// ankle" - but acronyms keep their caps ("Torn ACL" -> "torn ACL").
const lowerInjury = (type: string): string =>
	type
		.split(" ")
		.map((w) => (w.length >= 2 && w === w.toUpperCase() ? w : w.toLowerCase()))
		.join(" ");

// --- Player performance --------------------------------------------------------

// The five double-double-eligible stats; a category counts only at 10+.
const doubleCategories = (p: RecapPlayer): string[] => {
	const cats: [string, number][] = [
		["points", p.pts],
		["rebounds", p.reb],
		["assists", p.ast],
		["steals", p.stl],
		["blocks", p.blk],
	];
	return cats.filter(([, v]) => (v ?? 0) >= 10).map(([k]) => k);
};

const doubleWord = (count: number): string | undefined => {
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
const storyScore = (p: RecapPlayer): number =>
	0.5 * p.pts + 0.5 * p.reb + 0.5 * p.ast + 1.7 * p.blk + 1.7 * p.stl;

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

// The winner's supporting cast: everyone but the story player, best first.
const supportingCast = (
	players: RecapPlayer[],
	star: RecapPlayer,
): RecapPlayer[] =>
	players.filter((p) => p !== star).sort((a, b) => impact(b) - impact(a));

// "34 points, 12 rebounds and 9 assists" - points first, then up to two more
// categories worth mentioning (double-double stats always make the cut).
const statPhrase = (p: RecapPlayer, maxExtras = 2): string => {
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

// A shooting flourish for a big scorer, when the line is efficient or three-heavy.
const shootingFlourish = (p: RecapPlayer): string | undefined => {
	if (p.tp >= 6) {
		return `on ${p.tp} three-pointers`;
	}
	if (p.fga >= 10 && p.fg / p.fga >= 0.6) {
		return `on ${p.fg}-of-${p.fga} shooting`;
	}
	if (p.tp >= 4) {
		return `with ${p.tp} threes`;
	}
	return undefined;
};

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
		pool = ["poured in", "erupted for", "exploded for", "piled up"];
	} else if (p.pts >= 25) {
		pool = hasExtras
			? ["posted", "put up", "finished with", "racked up"]
			: ["scored", "posted", "put up", "racked up"];
	} else {
		pool = hasExtras
			? ["posted", "put up", "finished with", "had"]
			: ["scored", "finished with", "put up", "had"];
	}
	return pick(rng, pool);
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

// --- Team aggregates -----------------------------------------------------------

type TeamStats = {
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

const teamStats = (t: RecapTeam): TeamStats => {
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

// --- Team-name labels ----------------------------------------------------------

const nick = (t: RecapTeam): string => t.name || t.region || "the home team";
const theNick = (t: RecapTeam): string => `the ${nick(t)}`;
// Possessive that reads right for plural team nicknames ("the Kings'", "the
// 76ers'") and singular names ("Kobe Bryant's").
const poss = (s: string): string => (s.endsWith("s") ? `${s}'` : `${s}'s`);

// --- Game-shape detection ------------------------------------------------------

type Shape = {
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
	// Margin the winner led/trailed by entering the final regulation period.
	marginEnteringLast: number;
	wireToWire: boolean;
	// The single period where the winner most outscored the loser (a decisive run).
	bigRun?: { period: number; wpts: number; lpts: number; margin: number };
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
			if (!bigRun || runMargin > bigRun.margin) {
				bigRun = {
					period: i + 1,
					wpts: wq[i] ?? 0,
					lpts: lq[i] ?? 0,
					margin: runMargin,
				};
			}
			wCum += wq[i] ?? 0;
			lCum += lq[i] ?? 0;
			const diff = wCum - lCum;
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

	return {
		winner,
		loser,
		margin,
		ot,
		wq,
		lq,
		regPeriods,
		comebackFrom,
		comebackPeriod,
		marginEnteringLast,
		wireToWire,
		bigRun,
	};
};

const isUpset = (game: RecapGame, shape: Shape): boolean => {
	const s = game.spread;
	if (!s || s.points < 4) {
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
			const shot =
				rawShot === "basket"
					? buzzer && !tying
						? "buzzer-beater"
						: tying
							? "game-tying basket"
							: "game-winner"
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
	return undefined;
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
			? ` with ${shot.seconds} seconds left`
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
const clutchSentence = (
	shot: NonNullable<ReturnType<typeof clutchShot>>,
): string => `${shot.name} won it with ${clutchWhat(shot)}.`;

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
	// A short stakes phrase for the headline ("a Game 7", "an elimination game").
	headlineTag?: string;
	// Full sentences for the body.
	sentences: string[];
};

const postseasonContext = (
	game: RecapGame,
	shape: Shape,
): PostseasonContext => {
	const out: PostseasonContext = { sentences: [] };
	const w = nick(shape.winner);
	const l = nick(shape.loser);

	if (game.playIn) {
		const p = game.playIn;
		if (p.kind === "seed7v8") {
			out.headlineTag = "a play-in game";
			out.sentences.push(
				`With the win in the 7-vs-8 play-in game, ${theNick(
					shape.winner,
				)} claimed the #${p.prizeSeed ?? ""} seed, while ${theNick(
					shape.loser,
				)} dropped into a win-or-go-home game for the final spot.`.replace(
					"#  seed",
					"higher seed",
				),
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
					typeof p.prizeSeed === "number" ? ` as the #${p.prizeSeed} seed` : ""
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

	// The clinching case: series won. (When it's the decider, the headline tag
	// already carries the winner-take-all framing, so the body stays clean.)
	if (need !== undefined && wAfter >= need) {
		if (s.round === s.numRounds) {
			out.sentences.push(
				`${cap(w)} are champions, taking the title series ${wAfter}-${lBefore}.`,
			);
		} else {
			const how =
				lBefore === 0
					? `a ${wAfter}-0 sweep`
					: `the series ${wAfter}-${lBefore}`;
			out.sentences.push(
				`${cap(w)} closed out ${rnd} with ${how} and advanced.`,
			);
		}
		return out;
	}

	// The series continues - describe the new state and the stakes met.
	if (wAfter === lBefore) {
		out.sentences.push(
			`${cap(w)} evened ${rnd} at ${wAfter}-${wAfter} with a Game ${gameNo} win.`,
		);
	} else if (wAfter > lBefore) {
		out.sentences.push(
			facingElimination
				? `${cap(w)} staved off elimination to pull within ${lBefore}-${wAfter} in ${rnd}.`
				: wBefore === 0 && lBefore === 0
					? `${cap(w)} drew first blood in ${rnd}, ${wAfter}-${lBefore}.`
					: `${cap(w)} took a ${wAfter}-${lBefore} lead in ${rnd}.`,
		);
	} else {
		// Winner still trails the series even after this win.
		out.sentences.push(
			facingElimination
				? `${cap(w)} staved off elimination but still trail ${rnd} ${lBefore}-${wAfter}.`
				: `${cap(w)} cut their ${rnd} deficit to ${lBefore}-${wAfter}.`,
		);
	}

	void l;
	return out;
};

// --- Headline ------------------------------------------------------------------

const verbPool = (game: RecapGame, shape: Shape): string[] => {
	if (isUpset(game, shape)) {
		return ["stun", "upset", "shock", "knock off"];
	}
	if (shape.comebackFrom >= 12) {
		return ["rally past", "storm back to beat", "come back to top"];
	}
	if (shape.ot > 0 || shape.margin <= 4) {
		return ["hold off", "edge", "outlast", "slip past", "survive"];
	}
	if (shape.margin >= 20) {
		return ["rout", "cruise past", "run away from", "roll past", "blow out"];
	}
	// "Get past" a team you beat by 15 reads as a scare it wasn't. Comfortable
	// wins get their own band so the verb matches the scoreboard.
	if (shape.margin <= 9) {
		return ["beat", "top", "get past", "hold off", "take down"];
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
};

const h = (
	text: string,
	spentStar: boolean,
	spentLoserStar = false,
	spentLine = false,
): Headline => ({ text, spentStar, spentLoserStar, spentLine });

const buildHeadline = (
	game: RecapGame,
	shape: Shape,
	star: RecapPlayer,
	post: PostseasonContext,
	rng: () => number,
): Headline => {
	const winnerN = nick(shape.winner);
	const loserN = nick(shape.loser);
	const shot = clutchShot(game);
	const tag = post.headlineTag ? ` in ${post.headlineTag}` : "";

	if (shot && !shot.tying) {
		return h(
			pick(
				rng,
				[
					`${poss(shot.name)} ${shot.shot} sinks the ${loserN}${tag}`,
					`${poss(shot.name)} ${shot.shot} lifts the ${winnerN} past the ${loserN}${tag}`,
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
					`${star.name}'s ${word} carries the ${winnerN} past the ${loserN}${tag}`,
					`${star.name} posts a ${word} as the ${winnerN} ${verb} the ${loserN}${tag}`,
				],
				"headline:multi-double",
			),
			true,
		);
	}

	if (isUpset(game, shape)) {
		return h(
			`${winnerN} ${verb} the ${loserN}, ${scoreTag(shape)}${tag}`,
			false,
		);
	}

	if (shape.comebackFrom >= 12) {
		// Either way the comeback is now SAID. The body must not say it a second
		// and third time ("rallies" / "stormed back to beat" / "trailed by 14 and
		// stormed back" all landed in one recap).
		return h(
			pick(
				rng,
				[
					`${winnerN} erase ${aNum(shape.comebackFrom)}-point hole to ${verb} the ${loserN}${tag}`,
					`${star.name} rallies the ${winnerN} past the ${loserN}${tag}`,
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
					`${winnerN} hold off a feisty ${loserN} squad`,
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
					`${poss(star.name)} ${star.pts}-point, ${star.reb}-rebound night carries the ${winnerN} past the ${loserN}${tag}`,
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
					`${star.name}'s ${star.pts} edges ${loserStar.name}'s ${loserStar.pts} in ${poss(
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
					`${star.name}'s ${star.pts} sink the ${loserN}${tag}`,
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
					`${star.name} and ${partner.name} lead the ${winnerN} past the ${loserN}${tag}`,
					`Double-doubles from ${star.name} and ${partner.name} carry the ${winnerN} past the ${loserN}${tag}`,
				],
				"headline:two-doubles",
			),
			true,
		);
	}

	// A defensive showcase.
	if (star.blk >= 5) {
		return h(
			`${poss(star.name)} ${star.blk} blocks anchor the ${winnerN} past the ${loserN}${tag}`,
			true,
		);
	}
	if (star.stl >= 5 && star.pts >= 15) {
		return h(
			`${star.name} takes over with ${plural(star.pts, "point")} and ${plural(
				star.stl,
				"steal",
			)} as the ${winnerN} ${verb} the ${loserN}${tag}`,
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
		star.pts < 25
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
	const starTemplates = [
		`${poss(star.name)} ${sh.text} ${shVerb}${
			sh.plural ? "" : "s"
		} the ${winnerN} past the ${loserN}${tag}`,
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
		`${loserN} fall to the ${winnerN} ${scoreTag(shape)}${tag}`,
		`${winnerN} take the ${loserN} ${scoreTag(shape)}${tag}`,
		shape.margin >= 15
			? `${winnerN} pull away from the ${loserN} for ${aNum(shape.margin)}-point win${tag}`
			: `${winnerN} come out on top of the ${loserN} ${scoreTag(shape)}${tag}`,
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
	const useResult =
		(!starHasDouble && star.pts < 17) ||
		(!starHasDouble && star.pts < 20 && shape.margin >= 15) ||
		outscoredByLoser ||
		(shape.margin >= 18 && rng() < 0.5);
	const text = pick(
		rng,
		useResult ? resultTemplates : starTemplates,
		useResult ? "headline:result" : "headline:star",
	);
	// Only the "goes for <full stat phrase>" shape spends the whole line; the
	// others name him or quote a single number the body doesn't repeat verbatim.
	return h(
		text,
		!useResult,
		false,
		text.includes(` goes for ${statPhrase(star, 1)}`),
	);
};

// --- Body sentence builders ----------------------------------------------------

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
): { text: string; covers?: "comeback" | "wire" | "ot" | "run" } => {
	const verb = pastTense(pick(rng, verbPool(game, shape)));
	const w = theNick(shape.winner);
	const l = theNick(shape.loser);
	const score = scoreTag(shape);

	if (shape.comebackFrom >= 12 && shape.comebackPeriod > 0) {
		return {
			text: `${cap(w)} erased ${aNum(shape.comebackFrom)}-point deficit to ${verb} ${l} ${score}.`,
			covers: "comeback",
		};
	}
	if (shape.ot > 0) {
		return {
			text: `It took ${
				shape.ot === 1 ? "an extra period" : `${shape.ot} extra periods`
			}, but ${w} ${verb} ${l} ${score}.`,
			covers: "ot",
		};
	}
	if (shape.wireToWire && shape.margin >= 10) {
		return {
			text: pick(rng, [
				`${cap(w)} led wire to wire and ${verb} ${l} ${score}.`,
				`${cap(w)} ${verb} ${l} ${score} without ever trailing.`,
				`${cap(w)} were in front from the opening tip and ${verb} ${l} ${score}.`,
				`${cap(w)} ${verb} ${l} ${score}, leading start to finish.`,
			]),
			covers: "wire",
		};
	}
	if (
		shape.bigRun &&
		shape.bigRun.margin >= 9 &&
		shape.margin >= 8 &&
		shape.bigRun.period > 1
	) {
		return {
			text: `${cap(w)} ${verb} ${l} ${score}, breaking it open with a ${
				shape.bigRun.wpts
			}-${shape.bigRun.lpts} ${ordinal(shape.bigRun.period)} quarter.`,
			covers: "run",
		};
	}
	return { text: `${cap(w)} ${verb} ${l} ${score}.` };
};

const leadSentence = (
	game: RecapGame,
	shape: Shape,
	star: RecapPlayer,
	rng: () => number,
	// The result has already been stated (by the headline and the result lead),
	// so give the star's line on its own rather than tacking the score on again.
	omitResult = false,
): string => {
	const verb = pastTense(pick(rng, verbPool(game, shape)));

	// A big blowout with no standout line is a TEAM story - "Brad Miller had 13
	// points as the Clippers won by 31" makes 13 points sound like the reason.
	if (star.pts < 15 && shape.margin >= 20) {
		if (omitResult) {
			return `${star.name} led the way with ${statPhrase(star)}.`;
		}
		return `${cap(theNick(shape.winner))} ${verb} ${theNick(
			shape.loser,
		)} ${scoreTag(shape)}, led by ${poss(star.name)} ${statPhrase(star)}.`;
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
	}

	const statText = statPhrase(star);
	const flourishText = flourish ? ` ${flourish}` : "";
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
		return `${subject} ${actionVerb} ${statText}${flourishText}.`;
	}
	return `${subject} ${actionVerb} ${statText}${flourishText} as ${theNick(
		shape.winner,
	)} ${verb} ${theNick(shape.loser)} ${scoreTag(shape)}.`;
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
	if (
		shape.bigRun &&
		shape.bigRun.margin >= 9 &&
		shape.margin >= 8 &&
		shape.regPeriods >= 3 &&
		shape.bigRun.period > 1
	) {
		const run = `${shape.bigRun.wpts}-${shape.bigRun.lpts}`;
		const per = ordinal(shape.bigRun.period);
		return {
			covers: "run",
			text: pick(rng, [
				`A ${run} ${per} quarter broke it open.`,
				`The ${per} quarter settled it: ${run}.`,
				`${cap(theNick(shape.winner))} took over in the ${per}, ${run}.`,
				`A ${run} edge in the ${per} was the difference.`,
				`The game turned on a ${run} ${per} quarter.`,
			]),
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
		return {
			covers: "ot",
			text: `Neither side could pull away in regulation, and it took ${
				shape.ot === 1 ? "an extra period" : `${shape.ot} extra periods`
			} to settle it.`,
		};
	}
	if (
		Math.abs(shape.marginEnteringLast) <= 4 &&
		shape.margin <= 10 &&
		shape.regPeriods >= 4
	) {
		const m = shape.marginEnteringLast;
		if (m === 0) {
			return {
				text: `The game was tied entering the ${ordinal(shape.regPeriods)}.`,
			};
		}
		const leaderIsWinner = m > 0;
		return {
			text: `${cap(
				theNick(leaderIsWinner ? shape.winner : shape.loser),
			)} led by ${Math.abs(m)} entering the ${ordinal(shape.regPeriods)}.`,
		};
	}
	return undefined;
};

// A team-level statistical note: shooting, rebounding, turnovers, or balance.
// Which team-level fact a note is built on, so two sentences can't both spend
// it ("The Nets turned 19 Magic turnovers into offense. ... The Magic were
// undone by 19 turnovers.").
type StatFact = "loserTov" | "loserFgp" | "other";

const statNote = (
	shape: Shape,
	rng: () => number,
	// The opening surge has already been described, so don't offer it again.
	skipHotStart = false,
): { text: string; fact: StatFact } | undefined => {
	const w = teamStats(shape.winner);
	const l = teamStats(shape.loser);
	const options: { text: string; fact: StatFact }[] = [];

	const add = (text: string, fact: StatFact = "other") => {
		options.push({ text, fact });
	};

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
	if (w.tp >= 14) {
		add(`${cap(theNick(shape.winner))} knocked down ${w.tp} threes.`);
	}
	// Realistic team totals only (a full box score), so partial data can't yield
	// an absurd "won the glass 22-0".
	if (w.reb >= 30 && l.reb >= 20 && w.reb - l.reb >= 10) {
		add(`${cap(theNick(shape.winner))} won the glass ${w.reb}-${l.reb}.`);
	}
	// Six-plus is a genuinely balanced night; five is routine and was showing up
	// in half the recaps on a given day.
	if (w.dblFig >= 6) {
		add(
			`${cap(theNick(shape.winner))} had ${w.dblFig} players score in double figures.`,
		);
	}
	if (w.stl >= 10 && l.tov >= 16) {
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
	if (w.ast >= 28) {
		add(
			pick(
				rng,
				[
					`${cap(theNick(shape.winner))} piled up ${w.ast} assists on the night.`,
					`${cap(theNick(shape.winner))} finished with ${w.ast} assists.`,
					`${cap(theNick(shape.winner))} shared it, ${w.ast} assists on the night.`,
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
		add(
			`${cap(theNick(shape.winner))} jumped out to a ${shape.bigRun.wpts}-${shape.bigRun.lpts} first quarter.`,
		);
	}
	// A big edge at the free-throw line.
	if (w.ft >= 24 && w.ft - l.ft >= 10) {
		add(
			`${cap(theNick(shape.winner))} made ${w.ft} free throws to ${l.ft} for ${theNick(
				shape.loser,
			)}.`,
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
	if (halfMargin <= -4 && shape.margin > 0 && shape.comebackFrom < 12) {
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
				`A ${-halfMargin}-point halftime deficit turned into a ${wSecond}-${lSecond} second half.`,
			],
			"halfComeback",
		);
	}
	if (secondMargin >= 12) {
		return pick(
			rng,
			[
				`${cap(theNick(shape.winner))} pulled away after halftime, taking the second half ${wSecond}-${lSecond}.`,
				`The game got away from ${theNick(
					shape.loser,
				)} after the break - ${wSecond}-${lSecond} over the last two quarters.`,
				`${cap(theNick(shape.winner))} won the second half ${wSecond}-${lSecond} and were never troubled again.`,
			],
			"secondHalfPull",
		);
	}
	if (halfMargin >= 15) {
		return pick(
			rng,
			[
				`${cap(theNick(shape.winner))} led ${wFirst}-${lFirst} at halftime and never looked back.`,
				`It was ${wFirst}-${lFirst} at the break and the outcome was already clear.`,
				`${cap(theNick(shape.winner))} built a ${halfMargin}-point halftime lead and sat on it.`,
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
): string | undefined => {
	let best: RecapPlayer | undefined;
	for (const p of shape.winner.players) {
		if (typeof p.pm === "number" && (!best || p.pm > (best.pm ?? -Infinity))) {
			best = p;
		}
	}
	// +25 is a genuinely dominant shift; the old +18 bar fired in nearly every
	// blowout and turned the note into filler.
	if (!best || best === star || (best.pm ?? 0) < 25) {
		return undefined;
	}
	return pick(
		rng,
		[
			`${best.name} was a team-best +${best.pm} in ${best.min} minutes.`,
			`Nobody swung it further than ${best.name}, +${best.pm} across ${best.min} minutes.`,
			`${best.name} finished +${best.pm}, the best mark on the floor.`,
			`${best.name} was on the court for ${best.min} minutes and came out +${best.pm}.`,
		],
		"plusMinus",
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
				`Neither offense could find a rhythm, the two sides combining for just ${total} points.`,
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
): string | undefined => {
	const cast = supportingCast(shape.winner.players, star).filter(
		(p) => p.pts >= 12 || doubleCategories(p).length >= 2 || p.reb >= 12,
	);
	if (cast.length === 0) {
		return undefined;
	}
	const second = cast[0]!;
	const ddw = doubleWord(doubleCategories(second).length);
	const secondText = ddw
		? `${second.name} added a ${ddw} with ${statPhrase(second)}`
		: `${second.name} ${pick(rng, [
				"added",
				"chipped in",
				"contributed",
				"kicked in",
				"pitched in with",
				"tacked on",
				"was good for",
				"supplied",
				"came up with",
				"finished with",
			])} ${statPhrase(second)}`;

	const third = cast[1];
	if (third && (third.pts >= 14 || doubleCategories(third).length >= 2)) {
		return `${secondText}, and ${third.name} ${pick(rng, [
			"had",
			"put up",
			"posted",
			"went for",
			"finished with",
		])} ${statPhrase(third, 1)}.`;
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
	const leader = skipLeader ? undefined : bestOf(shape.loser.players);
	const stats = teamStats(shape.loser);
	// "reason" is appended after "...led the Loser", so it uses a pronoun rather
	// than repeating the team name.
	let reason = "";
	if (stats.tov >= 18 && !spent.has("loserTov")) {
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
	} else if (stats.fga >= 20 && stats.fgp <= 40 && !spent.has("loserFgp")) {
		reason = pick(
			rng,
			[
				`, but they shot just ${stats.fgp}% as a team`,
				`, though nothing else fell - ${stats.fgp}% for the game`,
				`, on a ${stats.fgp}% shooting night for the team`,
			],
			"loserFgp",
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
			? `${leader.name}'s ${ddw} (${statPhrase(leader)})`
			: `${leader.name}'s ${statPhrase(leader)}`;
		return `${leaderLine} ${verb} ${theNick(shape.loser)}${reason}.`;
	}
	// No standout to hang it on - name the team directly.
	if (stats.tov >= 18 && !spent.has("loserTov")) {
		return `${cap(theNick(shape.loser))} were undone by ${stats.tov} turnovers.`;
	}
	if (stats.fga >= 20 && stats.fgp <= 40 && !spent.has("loserFgp")) {
		return `${cap(theNick(shape.loser))} shot just ${stats.fgp}% as a team.`;
	}
	return undefined;
};

// Stakes and context: streaks, record, standings implication, the spread.
const stakesSentence = (
	game: RecapGame,
	shape: Shape,
	rng: () => number,
): string | undefined => {
	const options: string[] = [];

	const streak = shape.winner.streak;
	if (streak && streak.won && streak.count >= 4) {
		options.push(
			pick(
				rng,
				[
					`The win was ${poss(theNick(shape.winner))} ${ordinal(streak.count)} in a row.`,
					`${cap(theNick(shape.winner))} have now won ${plural(streak.count, "straight game")}.`,
					`That is ${plural(streak.count, "win")} in a row for ${theNick(shape.winner)}.`,
					`${cap(theNick(shape.winner))} ran their streak to ${streak.count}.`,
				],
				"streak",
			),
		);
	}

	// A losing streak the winner snapped, from the loser's last-10 log (index 0 is
	// this game).
	const l10 = shape.loser.last10;
	if (Array.isArray(l10) && l10.length >= 5) {
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
				`It snapped ${poss(theNick(shape.loser))} ${run}-game winning streak.`,
			);
		}
	}

	if (isUpset(game, shape) && game.spread) {
		const dog = game.spread.points;
		// A 3.5-point dog winning is a Tuesday. Reserve the language of a genuine
		// shock for a number that earns it, and let the small ones be stated
		// plainly rather than breathlessly.
		const big = dog >= 7;
		options.push(
			pick(
				rng,
				big
					? [
							`${cap(theNick(shape.winner))} were given no chance, ${dog} points the wrong side of the line.`,
							`Nobody had ${theNick(shape.winner)} winning - they were ${dog}-point dogs.`,
							`${cap(theNick(shape.winner))} entered ${dog}-point underdogs.`,
							`The books had ${theNick(shape.winner)} ${dog} points short of this one.`,
						]
					: [
							`${cap(theNick(shape.winner))} entered ${dog}-point underdogs.`,
							`${cap(theNick(shape.winner))} were getting ${dog} and did not need them.`,
							`The line had ${theNick(shape.winner)} ${dog} points short.`,
						],
				big ? "underdogBig" : "underdogSmall",
			),
		);
	}

	// The record note earns its sentence only when the record is actually good
	// ("The Warriors improved to 2-8" is not news), and even then only sometimes
	// - it was showing up in nearly every recap on a full slate.
	const rec = shape.winner.record;
	if (
		rec &&
		rec.won + rec.lost >= 10 &&
		!game.playoffs &&
		rec.won > rec.lost &&
		rng() < 0.5
	) {
		options.push(
			pick(
				rng,
				[
					`${cap(theNick(shape.winner))} improved to ${rec.won}-${rec.lost}.`,
					`${cap(theNick(shape.winner))} moved to ${rec.won}-${rec.lost}.`,
					`The win pushed ${theNick(shape.winner)} to ${rec.won}-${rec.lost}.`,
				],
				"record",
			),
		);
	}

	if (options.length === 0) {
		return undefined;
	}
	return pick(rng, options);
};

// Injury color: returns, playing through, new injuries, and notable inactives.
const injurySentence = (
	shape: Shape,
	rng: () => number,
): string | undefined => {
	const bits: string[] = [];
	for (const t of [shape.winner, shape.loser]) {
		for (const p of t.players) {
			if (p.injury?.playingThrough && p.pts >= 18) {
				bits.push(
					`${p.name} played through ${aWord(lowerInjury(p.injury.type))} for ${p.pts}`,
				);
			} else if (p.injury?.newThisGame && p.injury.gamesRemaining > 0) {
				bits.push(
					pick(
						rng,
						[
							`${p.name} left with ${aWord(lowerInjury(p.injury.type))} (out ~${plural(
								p.injury.gamesRemaining,
								"game",
							)})`,
							`${p.name} went down with ${aWord(lowerInjury(p.injury.type))} and is out around ${plural(
								p.injury.gamesRemaining,
								"game",
							)}`,
							`${p.name} picked up ${aWord(lowerInjury(p.injury.type))} that will cost him about ${plural(
								p.injury.gamesRemaining,
								"game",
							)}`,
						],
						"injuryNew",
					),
				);
			}
		}
	}
	// A key player held out entirely.
	for (const t of [shape.winner, shape.loser]) {
		for (const out of t.injuries ?? []) {
			bits.push(
				pick(
					rng,
					[
						`${theNick(t)} were without ${out.name} (${lowerInjury(out.type)})`,
						`${out.name} sat out for ${theNick(t)} with ${aWord(lowerInjury(out.type))}`,
						`${theNick(t)} were missing ${out.name} (${lowerInjury(out.type)})`,
						// NOT pre-capitalized: these bits get joined with "; " and only
						// the whole string is capped, so a capital here reads as "a
						// Torn achilles tendon kept..." in the middle of a sentence.
						`${aWord(lowerInjury(out.type))} kept ${out.name} out for ${theNick(t)}`,
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
): string | undefined => {
	const avg = enteringLine(star, playoffs);
	if (!avg || avg.gp < 5 || alreadySaidAverage) {
		return undefined;
	}
	const over = star.pts - avg.pts;
	// A big night ONLY counts as a departure if it's both a large absolute jump
	// and a large relative one - 28 from a 24-point scorer is Tuesday.
	if (star.pts >= 20 && over >= 11 && star.pts >= avg.pts * 1.5) {
		return pick(
			rng,
			[
				`${star.name} came into the night averaging ${avg.pts.toFixed(1)} points a game.`,
				`That is ${Math.round(over)} clear of ${poss(star.name)} ${avg.pts.toFixed(1)} season average.`,
				`${star.name} had been averaging ${avg.pts.toFixed(1)} points a game to this point.`,
				`It was a long way past the ${avg.pts.toFixed(1)} a night ${star.name} had been putting up.`,
			],
			"vsAvgHigh",
		);
	}
	// The reverse: a star who was held down, but only when he actually is one.
	if (avg.pts >= 18 && star.pts <= avg.pts - 10 && star.pts <= 12) {
		return pick(
			rng,
			[
				`It was a quiet night by ${poss(star.name)} standards - he came in averaging ${avg.pts.toFixed(1)} points a game.`,
				`The ${avg.pts.toFixed(1)} a night ${star.name} averages never showed up.`,
			],
			"vsAvgLow",
		);
	}
	// An efficient volume night against a middling shooting season.
	if (
		star.fga >= 12 &&
		avg.fgp > 0 &&
		star.fg / star.fga >= 0.6 &&
		star.fg / star.fga - avg.fgp / 100 >= 0.12
	) {
		return `${star.name} shot it far better than the ${avg.fgp.toFixed(1)}% he had managed on the season.`;
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
	const past = star.career;
	if (playoffs || !avg || avg.gp < 12 || !past || past.length < 2) {
		return undefined;
	}
	const bestPast = Math.max(...past.map((c) => c.pts));
	const seasons = past.length + 1;
	if (avg.pts > bestPast + 1.5 && avg.pts >= 15) {
		return pick(
			rng,
			[
				`At ${avg.pts.toFixed(1)} a night, ${star.name} is having the best scoring season of his career.`,
				`${poss(star.name)} ${avg.pts.toFixed(1)} per game is a career best in year ${seasons}.`,
				`No season ${star.name} has played has scored like this one.`,
			],
			"careerBest",
		);
	}
	// A veteran well past his peak is its own story.
	if (seasons >= 8 && bestPast >= avg.pts + 6 && avg.pts >= 8) {
		return `${star.name} is ${seasons} seasons in, some way down from the ${bestPast.toFixed(
			1,
		)} he once averaged.`;
	}
	return undefined;
};

// The three-point battle, when one side clearly won it. Volume from deep is the
// single biggest swing factor in a modern box score and nothing was reading it
// except a bare "knocked down 14 threes".
const threeNote = (shape: Shape, rng: () => number): string | undefined => {
	const w = teamStats(shape.winner);
	const l = teamStats(shape.loser);
	if (w.tpa < 10 || l.tpa < 10) {
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
		return `${cap(theNick(shape.loser))} went ${l.tp}-of-${l.tpa} from three.`;
	}
	return undefined;
};

// Who got to the line. A lopsided free-throw night is a real explanation for a
// close result and it was only ever mentioned from the winner's side.
const freeThrowNote = (shape: Shape, rng: () => number): string | undefined => {
	const w = teamStats(shape.winner);
	const l = teamStats(shape.loser);
	if (w.fta + l.fta < 20) {
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
			],
			"ftGap",
		);
	}
	if (l.fta - w.fta >= 14 && shape.margin <= 10) {
		return `${cap(theNick(shape.loser))} had the better of the whistle - ${l.fta} free throws to ${w.fta} - and still lost.`;
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
	const options: string[] = [];
	if (w.blk >= 9) {
		options.push(
			`${cap(theNick(shape.winner))} blocked ${w.blk} shots at the rim.`,
		);
	}
	if (bigBlocker) {
		options.push(
			`${bigBlocker.name} protected the rim with ${plural(bigBlocker.blk, "block")}.`,
		);
	}
	if (bigThief) {
		options.push(
			`${bigThief.name} picked off ${plural(bigThief.stl, "pass")}.`,
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
const foulOutNote = (shape: Shape, said: Set<string>): string | undefined => {
	for (const t of [shape.winner, shape.loser]) {
		for (const p of t.players) {
			if (p.pf >= 6 && !said.has(p.name)) {
				said.add(p.name);
				return `${p.name} fouled out with ${plural(p.pts, "point")} in ${p.min} minutes.`;
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
	// Set when a streak sentence has already run. "The Celtics have now won 6
	// straight" followed by "the Celtics have won 5 of their last 6" is the same
	// fact told twice, and worse, the two numbers look like they disagree.
	skipWinner = false,
): string | undefined => {
	const describe = (t: RecapTeam): string | undefined => {
		const l10 = t.last10;
		// Index 0 is this game, so the FORM entering it is everything after.
		if (!Array.isArray(l10) || l10.length < 7) {
			return undefined;
		}
		const prior = l10.slice(1);
		const won = prior.filter((x) => x.won).length;
		// EVERY sentence here has to be past tense, describing the form the team
		// carried INTO this game, because that is the window `prior` measures.
		// Present tense reads as a claim about right now and is then flatly
		// wrong for the team that just lost: a 9-0 run into a defeat became
		// "the Celtics are 9-0 over their last 9" underneath a box score of
		// them losing. It is wrong for the winner too, just less visibly - they
		// are 10-0 including tonight, not the 9-0 the sentence "now" asserts.
		if (won >= prior.length - 1 && prior.length >= 6) {
			const of = won === prior.length ? "every one of" : `${won} of`;
			return pick(
				rng,
				[
					`${theNick(t)} came in having won ${of} their last ${prior.length}`,
					`that was ${won} wins in ${prior.length} games for ${theNick(t)} coming in`,
					`${theNick(t)} entered the night ${won}-${prior.length - won} over their previous ${prior.length}`,
				],
				"formHot",
			);
		}
		if (won <= 1 && prior.length >= 6) {
			return pick(
				rng,
				[
					`${theNick(t)} had lost ${prior.length - won} of ${prior.length} coming in`,
					`${theNick(t)} arrived having dropped ${prior.length - won} of their last ${prior.length}`,
					`it had been a rough stretch for ${theNick(t)}, ${won}-${prior.length - won} in their previous ${prior.length}`,
				],
				"formCold",
			);
		}
		return undefined;
	};
	const options = [
		skipWinner ? undefined : describe(shape.winner),
		describe(shape.loser),
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
	// rotates too.
	return `${second.name} ${pick(rng, [
		"also had",
		"finished with",
		"put up",
		"was good for",
		"added",
	])} ${statPhrase(second, 1)} ${pick(
		rng,
		["in defeat", "in the loss", "in a losing cause", "for the losing side"],
		"loserTail",
	)}.`;
};

// Whether the favorite covered. The spread is already known to the recap and
// was only ever used to flag outright upsets.
const spreadNote = (
	game: RecapGame,
	shape: Shape,
	rng: () => number,
): string | undefined => {
	const s = game.spread;
	if (!s || s.points < 3 || game.playoffs) {
		return undefined;
	}
	// An outright upset has its own sentence elsewhere.
	if (s.favTid === shape.loser.tid) {
		return undefined;
	}
	const covered = shape.margin > s.points;
	if (covered && shape.margin - s.points >= 10) {
		return pick(
			rng,
			[
				`Favored by ${s.points}, ${theNick(shape.winner)} won by ${shape.margin}.`,
				`The ${s.points}-point line never looked like mattering.`,
			],
			"spreadCovered",
		);
	}
	if (!covered && s.points >= 7) {
		// "Had to sweat this one out" needs the game to have actually been in
		// doubt. A 22.5-point favorite winning by 10 never sweated anything - it
		// just never got near the number.
		if (shape.margin <= 5) {
			return pick(
				rng,
				[
					`The ${s.points}-point favorites had to sweat this one out.`,
					`${cap(theNick(shape.winner))} were favored by ${s.points} and got out with ${shape.margin}.`,
				],
				"spreadScare",
			);
		}
		return pick(
			rng,
			[
				`${cap(theNick(shape.winner))} were ${s.points}-point favorites and won by ${shape.margin}.`,
				`A ${shape.margin}-point win was nowhere near the ${s.points} ${theNick(shape.winner)} were giving.`,
				`${cap(theNick(shape.winner))} won comfortably enough but fell ${s.points - shape.margin} short of the number.`,
			],
			"spreadNarrow",
		);
	}
	return undefined;
};

// Heavy minutes - a starter who barely came off the floor.
const minutesNote = (shape: Shape, said: Set<string>): string | undefined => {
	const iron = [...shape.winner.players, ...shape.loser.players].find(
		(p) => p.min >= 46 && !said.has(p.name),
	);
	if (!iron) {
		return undefined;
	}
	said.add(iron.name);
	return `${iron.name} logged ${iron.min} minutes.`;
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
	if (!told.dblFig && w.dblFig >= 5 && w.dblFig - l.dblFig >= 3) {
		return pick(
			rng,
			[
				`${cap(theNick(shape.winner))} got double figures out of ${w.dblFig} players to ${l.dblFig} for ${theNick(
					shape.loser,
				)}.`,
				`The scoring was spread around - ${w.dblFig} ${nick(shape.winner)} in double figures.`,
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

	const headline = as.mvp
		? `${as.mvp} takes home All-Star Game MVP`
		: `The stars come out for the All-Star Game`;

	const sentences: string[] = [];
	sentences.push(
		`${nick(winner)} beat ${nick(loser)} ${winner.pts}-${loser.pts} in the All-Star Game${
			star ? `, with ${star.name} pouring in ${star.pts}` : ""
		}.`,
	);
	const extras: string[] = [];
	if (as.dunk?.winner) {
		extras.push(`${as.dunk.winner} won the dunk contest`);
	}
	if (as.three?.winner) {
		extras.push(`${as.three.winner} took the three-point shootout`);
	}
	if (extras.length > 0) {
		sentences.push(`Over the weekend, ${naturalList(extras)}.`);
	}

	return `**${headline}**\n\n${sentences.join(" ")}`;
};

// --- Entry point: one game -----------------------------------------------------

export const getAutoRecap = (game: RecapGame): string => {
	if (!inBatch) {
		phraseMemory.clear();
	}
	const rng = rngFromSeed((game.gid + 1) * 2654435761);

	if (game.allStar) {
		return buildAllStar(game, rng);
	}

	const shape = analyzeShape(game);
	let star = bestOf(shape.winner.players) ?? bestOf(shape.loser.players);

	// The impact score occasionally crowns a low-scoring stat-stuffer (a 9-point,
	// 4-steal night) whose line reads absurd as the LEAD ("Nene chipped in 9
	// points... as the Lakers won"). When the pick barely scored, hand the lead to
	// the winner's best real scoring line instead - the impact pick still shows up
	// in the supporting-cast sentence.
	if (star && star.pts < 12 && shape.winner.players.includes(star)) {
		const alt = supportingCast(shape.winner.players, star).find(
			(p) => p.pts >= 15 || (p.pts >= 12 && doubleCategories(p).length >= 2),
		);
		if (alt) {
			star = alt;
		}
	}

	if (!star) {
		const verb = pastTense(pick(rng, verbPool(game, shape)));
		return `**${nick(shape.winner)} ${verb} the ${nick(shape.loser)}, ${scoreTag(
			shape,
		)}**\n\n${cap(theNick(shape.winner))} ${verb} ${theNick(
			shape.loser,
		)} ${scoreTag(shape)}.`;
	}

	const post = game.playoffs
		? postseasonContext(game, shape)
		: { sentences: [] as string[] };

	const headline = buildHeadline(game, shape, star, post, rng);

	// Paragraph 1: the result and how it happened. A game-winning shot is the
	// game's defining moment, so it always gets a beat right after the lead -
	// previously the headline could tout a buzzer-beater the body never
	// mentioned. When the shooter IS the lead star, the beat merges into the
	// lead sentence so the name isn't repeated back-to-back ("Hamilton finished
	// with 20... Hamilton won it...").
	const para1: string[] = [];
	const shot = clutchShot(game);
	let flowCovered: string | undefined;

	if (headline.spentStar) {
		// The headline already told the star's story. Open on the RESULT and let
		// his line follow as its own sentence, rather than writing the headline
		// again with the verbs swapped.
		const opener = resultLead(game, shape, rng);
		flowCovered = opener.covers;
		para1.push(opener.text);
		// The headline may already have printed his whole line ("DerMarr Johnson
		// goes for 22 points as..."), in which case repeating it as the next
		// sentence is the same numbers twice. Keep the sentence whenever it
		// brings anything new - a shooting split, a second category - and drop it
		// only when every figure in it was in the headline.
		const starSentence = leadSentence(game, shape, star, rng, true);
		const figures = (t: string) => t.match(/\d+(?:\.\d+)?/g) ?? [];
		const inHeadline = new Set(figures(headline.text));
		if (
			!headline.spentLine ||
			figures(starSentence).some((n) => !inHeadline.has(n))
		) {
			para1.push(starSentence);
		}
		if (shot && !shot.tying) {
			para1.push(clutchSentence(shot));
		}
	} else if (shot && !shot.tying && shot.name !== star.name) {
		// The headline is the winning shot (buildHeadline always leads with one
		// when there is one), so the body has to get there immediately. Opening
		// on someone else's scoring line and reaching the shot a sentence later
		// made the headline look like it belonged to a different story: "Metta
		// World Peace's three-point play sinks the Knicks / Ray Allen scored 18
		// points as...". Result, then the shot, then the leading scorer.
		const opener = resultLead(game, shape, rng);
		flowCovered = opener.covers;
		para1.push(
			opener.text,
			clutchSentence(shot),
			leadSentence(game, shape, star, rng, true),
		);
	} else {
		let lead = leadSentence(game, shape, star, rng);
		const shooterIsStar = shot && !shot.tying;
		if (shooterIsStar) {
			lead = `${lead.slice(0, -1)}, winning it with ${clutchWhat(shot)}.`;
		}
		para1.push(lead);
	}

	// The result lead already carried the comeback / wire-to-wire / overtime /
	// decisive-run angle, so the flow sentence would be saying it twice.
	if (!flowCovered) {
		const flow = flowSentence(shape, rng);
		if (flow) {
			para1.push(flow.text);
			flowCovered = flow.covers;
		}
	}
	const spentFacts = new Set<StatFact>();
	// A "jumped out to a 30-20 first quarter" note on top of "led wire to wire"
	// or "broke it open with a 30-20 first quarter" is the same beat twice.
	const stat =
		flowCovered === "wire" || flowCovered === "run"
			? statNote(shape, rng, true)
			: statNote(shape, rng);
	if (stat && para1.length < 4) {
		para1.push(stat.text);
		spentFacts.add(stat.fact);
	}

	// Paragraph 2: supporting cast, the losing side, stakes, and injuries. The
	// postseason context leads it when this is a playoff game.
	const para2: string[] = [];
	if (post.sentences.length > 0) {
		para2.push(post.sentences[0]!);
	}
	const support = supportSentence(shape, star, rng);
	if (support) {
		para2.push(support);
	}
	const loser = loserSentence(shape, rng, spentFacts, headline.spentLoserStar);
	if (loser) {
		para2.push(loser);
	}
	// Fill out with the remaining angles, seed-ordered for variety.
	// A "neither offense got going" note and a "pulled away after halftime" note
	// are two readings of the same game arguing with each other. The grind is the
	// more distinctive observation, so it wins.
	const combined = combinedNote(shape, rng);
	const extras = shuffle(rng, [
		post.sentences[1],
		combined ? undefined : secondHalfNote(shape, rng),
		stakesSentence(game, shape, rng),
		combined,
		plusMinusNote(shape, star, rng),
		injurySentence(shape, rng),
	]).filter((s): s is string => !!s);
	for (const e of extras) {
		if (para2.length >= 6) {
			break;
		}
		para2.push(e);
	}

	// Paragraph 3: the detail a beat writer fills a column with once the result
	// has been told - what the star's night was against his own season, how the
	// two sides shot it, who got to the line, how either club has been playing.
	// All optional, so a forgettable game still gets a short recap and only a
	// game with things to say runs long.
	//
	// Everyone already named, so this paragraph introduces new faces instead of
	// re-describing the same two men a third time.
	const said = new Set<string>([star.name]);
	// The HEADLINE counts too. A losing star named up top had his line skipped by
	// the loser sentence, then the support note printed it again down here.
	const alreadyWritten = [headline.text, ...para1, ...para2].join(" ");
	for (const t of [shape.winner, shape.loser]) {
		for (const p of t.players) {
			if (alreadyWritten.includes(p.name)) {
				said.add(p.name);
			}
		}
	}
	// A streak sentence in paragraph 2 already covered the winner's recent form.
	const streakTold =
		/in a row|straight game|ran their streak|winning streak/.test(
			alreadyWritten,
		);
	// Same idea for the two team totals paragraph 2 can hand out on its own.
	const toldAlready = {
		dblFig: /in double figures|double figures/.test(alreadyWritten),
		assists: /assists?\b/.test(alreadyWritten),
	};

	// Ordered, not shuffled: the man who decided the game comes before how the
	// two sides shot it, which comes before the bookkeeping. Within each tier the
	// seed still varies which angles appear at all, so no two recaps line up.
	const para3 = [
		vsAverageNote(
			shape,
			star,
			game.playoffs,
			rng,
			/averaging|average/.test(alreadyWritten),
		),
		careerArcNote(star, game.playoffs, rng),
		...shuffle(rng, [
			threeNote(shape, rng),
			freeThrowNote(shape, rng),
			balanceNote(shape, rng, toldAlready),
		]),
		loserSupportNote(shape, rng, said),
		...shuffle(rng, [
			defensiveNote(shape, rng, said),
			foulOutNote(shape, said),
			minutesNote(shape, said),
		]),
		formNote(shape, rng, streakTold),
		spreadNote(game, shape, rng),
	]
		.filter((x): x is string => !!x)
		.slice(0, 5);

	// A lone extra sentence isn't a paragraph - it reads as an orphan under two
	// full ones. Fold it into the second and let the subject dedupe treat it as
	// part of that paragraph, so a repeated name collapses to a pronoun.
	if (para3.length === 1 && para2.length > 0) {
		para2.push(para3.pop()!);
	}
	// Same at the top. A lede of one bare result sentence happens when the
	// headline already spent the star's line and the game had no comeback or
	// decisive run to describe; pull the next beat up rather than leave the
	// opening paragraph a single clause.
	if (para1.length === 1 && para2.length > 1) {
		para1.push(para2.shift()!);
	}

	const otherNick = nick(shape.loser);
	const paragraphs = [dedupeSubjects(para1, otherNick).join(" ")];
	if (para2.length > 0) {
		paragraphs.push(dedupeSubjects(para2, otherNick).join(" "));
	}
	if (para3.length > 0) {
		paragraphs.push(dedupeSubjects(para3, otherNick).join(" "));
	}
	return `**${headline.text}**\n\n${paragraphs.join("\n\n")}`;
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
	return out.sort((a, b) => impact(b.p) - impact(a.p));
};

// How much a game deserves to be the day's marquee story.
const notability = (game: RecapGame): number => {
	if (game.allStar) {
		return 1000;
	}
	const shape = analyzeShape(game);
	const star = bestOf(shape.winner.players) ?? bestOf(shape.loser.players);
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
		// A clinch is the biggest story.
		if (post.sentences.some((s) => /champions|closed out|advanced/.test(s))) {
			n += 80;
		}
	}
	if (isUpset(game, shape)) {
		n += 35 + (game.spread?.points ?? 0);
	}
	n += shape.ot * 25;
	if (shape.margin <= 3) {
		n += 20;
	}
	if (shape.comebackFrom >= 15) {
		n += 25;
	}
	if (star && doubleCategories(star).length >= 3) {
		n += 35;
	}
	if (star && star.pts >= 40) {
		n += star.pts;
	}
	return n;
};

// A compact one-sentence account of a game, for the day wrap.
const gameBlurb = (game: RecapGame, rng: () => number): string => {
	const shape = analyzeShape(game);
	const star = bestOf(shape.winner.players) ?? bestOf(shape.loser.players);
	const verb = pastTense(pick(rng, verbPool(game, shape)));
	const shot = clutchShot(game);
	const base = `${cap(theNick(shape.winner))} ${verb} ${theNick(
		shape.loser,
	)} ${scoreTag(shape)}`;
	if (shot && !shot.tying) {
		const timing =
			shot.shot === "buzzer-beater"
				? ""
				: shot.buzzer
					? " at the buzzer"
					: shot.seconds !== undefined
						? ` with ${shot.seconds} seconds left`
						: "";
		return `${base} on ${poss(shot.name)} ${shot.shot}${timing}`;
	}
	if (star) {
		// Give the marquee star his full line, calling out a triple-double.
		const ddw = doubleWord(doubleCategories(star).length);
		if (ddw && doubleCategories(star).length >= 3) {
			return `${base} behind ${star.name}'s ${ddw} (${statPhrase(star)})`;
		}
		return `${base} behind ${star.name}'s ${statPhrase(star, 2)}`;
	}
	return base;
};

const gbText = (gb: number): string =>
	gb === 0.5 ? "half a game" : `${gb} game${gb === 1 ? "" : "s"}`;

const conferencePictureSentence = (
	standings: RecapDayStandings | undefined,
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
		// Nicknames, like every other team reference in the piece. "Cleveland
		// Cavaliers (47-4) lead the Eastern Conference" in a paragraph that has
		// said "the Cavaliers" four times reads like it was pasted in.
		const who = `the ${leader.name} (${leader.won}-${leader.lost})`;
		// An unbeaten leader is the story itself, not a "narrow lead".
		if (leader.lost === 0) {
			bits.push(
				`the ${leader.name} are still perfect at ${leader.won}-0 atop the ${conf.name}`,
			);
		} else if (second && second.gb >= 1) {
			bits.push(`${who} lead the ${conf.name} by ${gbText(second.gb)}`);
		} else if (second) {
			bits.push(`${who} hold a narrow lead in the ${conf.name}`);
		} else {
			bits.push(`${who} sit atop the ${conf.name}`);
		}
	}
	if (bits.length === 0) {
		return undefined;
	}
	return `In the standings, ${naturalList(bits)}.`;
};

// The night's injury news, worst first - a real league wrap covers who went
// down, not just who went off.
const injuryRoundup = (games: RecapGame[]): string | undefined => {
	const bits: { text: string; severity: number }[] = [];
	for (const game of games) {
		if (game.allStar) {
			continue;
		}
		for (const t of game.teams) {
			for (const p of t.players) {
				if (p.injury?.newThisGame && (p.injury.gamesRemaining ?? 0) >= 2) {
					bits.push({
						text: `${p.name} (${lowerInjury(p.injury.type)}, out ~${plural(
							p.injury.gamesRemaining,
							"game",
						)})`,
						severity: p.injury.gamesRemaining,
					});
				}
			}
		}
	}
	if (bits.length === 0) {
		return undefined;
	}
	bits.sort((a, b) => b.severity - a.severity);
	const top = bits.slice(0, 3).map((b) => b.text);
	return `On the injury front, ${naturalList(top)} went down.`;
};

// A team riding a notable win streak into the night.
const teamStreakSentence = (games: RecapGame[]): string | undefined => {
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
		if (s && s.won && s.count >= 6 && (!best || s.count > best.count)) {
			best = { team: winner, count: s.count };
		}
	}
	if (!best) {
		return undefined;
	}
	return `${cap(theNick(best.team))} ran their win streak to ${best.count} games.`;
};

// A compact, varied series-state clause for one playoff/play-in game, for the day
// wrap's postseason roundup (lower-cased, no trailing period).
const daySeriesPhrase = (
	g: RecapGame,
	rng: () => number,
): string | undefined => {
	const shape = analyzeShape(g);
	const w = theNick(shape.winner);
	const l = theNick(shape.loser);

	if (g.playIn) {
		const p = g.playIn;
		if (p.kind === "seed7v8") {
			return typeof p.prizeSeed === "number"
				? `${w} grabbed the #${p.prizeSeed} seed`
				: `${w} took the higher seed`;
		}
		if (p.kind === "seed9v10") {
			return `${w} ended ${poss(l)} season in the play-in`;
		}
		return `${w} claimed the last playoff spot`;
	}

	const s = g.series;
	if (!s) {
		return undefined;
	}
	const rnd = roundName(s.round, s.numRounds);
	const winnerIsHome = shape.winner.abbrev === s.homeAbbrev;
	const wBefore = winnerIsHome ? s.homeWon : s.awayWon;
	const lBefore = winnerIsHome ? s.awayWon : s.homeWon;
	const wAfter = wBefore + 1;
	const need =
		typeof s.bestOf === "number" && s.bestOf > 0
			? Math.floor(s.bestOf / 2) + 1
			: undefined;

	if (need !== undefined && wAfter >= need) {
		return s.round === s.numRounds
			? `${w} won the championship`
			: `${w} advanced past ${l}`;
	}
	if (wAfter === lBefore) {
		return `${w} pulled even with ${l} at ${wAfter}-${wAfter} in ${rnd}`;
	}
	if (wAfter > lBefore) {
		return pick(rng, [
			`${w} lead ${rnd} ${wAfter}-${lBefore}`,
			`${w} are up ${wAfter}-${lBefore} in ${rnd}`,
			`${w} grabbed a ${wAfter}-${lBefore} edge in ${rnd}`,
		]);
	}
	return `${w} trail ${rnd} ${lBefore}-${wAfter} despite the win`;
};

// The day's headline, driven by the single biggest thing that happened -
// a buzzer-beater, a 45-point night, a playoff clinch, an upset, a rout, a
// thriller - rather than a fixed "N-game slate" template. Seeded variation keeps
// two similar days from reading the same.
const dayHeadline = (
	marquee: RecapGame,
	mShape: Shape,
	mStar: RecapPlayer | undefined,
	games: RecapGame[],
	performers: LeaguePerformance[],
	playoffs: boolean,
	rng: () => number,
): string => {
	// `w` leads a headline (no article, "Heat stun Lakers"); `tw` sits mid-sentence
	// as an object ("... powers the Heat past ...").
	const w = nick(mShape.winner);
	const tw = theNick(mShape.winner);
	const l = theNick(mShape.loser);

	// Postseason storylines lead everything.
	if (playoffs && marquee.playoffs) {
		const post = postseasonContext(marquee, mShape);
		const joined = post.sentences.join(" ");
		if (/are champions|win the title/.test(joined)) {
			return pick(rng, [
				`${w} are champions`,
				`${w} win it all`,
				`${w} capture the title`,
			]);
		}
		if (/closed out|advanced/.test(joined)) {
			const s = marquee.series;
			const next = s
				? roundName(Math.min(s.round + 1, s.numRounds), s.numRounds)
				: "the next round";
			return pick(rng, [
				`${w} close out ${l} and reach ${next}`,
				`${w} advance to ${next}`,
				`${w} eliminate ${l}`,
			]);
		}
		// A series-tying win that forces a winner-take-all next game.
		const s = marquee.series;
		if (s && typeof s.bestOf === "number" && s.bestOf > 1) {
			const need = Math.floor(s.bestOf / 2) + 1;
			const winnerIsHome = mShape.winner.abbrev === s.homeAbbrev;
			const wBefore = winnerIsHome ? s.homeWon : s.awayWon;
			const lBefore = winnerIsHome ? s.awayWon : s.homeWon;
			if (wBefore + 1 === need - 1 && lBefore === need - 1) {
				const decider =
					s.bestOf === 7 ? "a Game 7" : `a decisive Game ${s.bestOf}`;
				return `${w} force ${decider} with ${l}`;
			}
		}
		if (/staved off elimination/.test(joined)) {
			return pick(rng, [
				`${w} stave off elimination against ${l}`,
				`${w} keep their season alive`,
			]);
		}
		const shot = clutchShot(marquee);
		if (shot && !shot.tying) {
			return `${poss(shot.name)} ${shot.shot} decides a playoff thriller`;
		}
		if (mStar) {
			return pick(rng, [
				`${mStar.name}'s ${starHeadline(mStar).text} power${
					starHeadline(mStar).plural ? "" : "s"
				} ${tw} past ${l}`,
				`${w} take command against ${l}`,
			]);
		}
		return `${w} ${pick(rng, verbPool(marquee, mShape))} ${l}`;
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
	if (shot && !shot.tying) {
		if (upsets.length >= 2) {
			return pick(rng, [
				`${poss(shot.name)} ${shot.shot} caps a night of upsets`,
				`${shot.name} sinks ${l} at the wire on a night ${numWord(upsets.length)} favorites fell`,
			]);
		}
		return pick(rng, [
			`${poss(shot.name)} ${shot.shot} sinks ${l} ${scoreTag(mShape)}`,
			`${shot.name} walks off ${l} ${scoreTag(mShape)}`,
		]);
	}

	// Two or more 40-point nights across the league.
	const fortyClub = performers.filter((perf) => perf.p.pts >= 40);
	if (fortyClub.length >= 2) {
		const [a, b] = fortyClub;
		return `${poss(a!.p.name)} ${a!.p.pts} and ${poss(b!.p.name)} ${b!.p.pts} light up the night`;
	}

	if (mStar && mStar.pts >= 45) {
		return pick(rng, [
			`${mStar.name} erupts for ${mStar.pts} to lead ${tw} past ${l}`,
			`${mStar.name} drops ${mStar.pts} in ${poss(tw)} win`,
		]);
	}

	// Favorites fell league-wide: lead with the biggest shock and the count.
	if (upsets.length >= 3) {
		const biggest = [...upsets].sort(
			(a, b) => (b.g.spread?.points ?? 0) - (a.g.spread?.points ?? 0),
		)[0]!;
		return `${nick(biggest.shape.winner)} stun ${theNick(
			biggest.shape.loser,
		)} on a night ${numWord(upsets.length)} favorites fell`;
	}

	if (mStar && doubleCategories(mStar).length >= 3) {
		return pick(rng, [
			`${mStar.name} triple-doubles to lead ${tw} past ${l}`,
			`${poss(mStar.name)} triple-double carries ${tw} over ${l}`,
		]);
	}

	if (isUpset(marquee, mShape)) {
		const pts = marquee.spread?.points;
		return pick(rng, [
			pts !== undefined && pts >= 6
				? `${w} stun ${l} as ${pts}-point underdogs`
				: `${w} stun ${l} ${scoreTag(mShape)}`,
			`${w} shock ${l} ${scoreTag(mShape)}`,
		]);
	}

	if (mShape.ot > 0) {
		return pick(rng, [
			`${w} outlast ${l} ${scoreTag(mShape)}`,
			`${w} survive ${l} in ${mShape.ot === 1 ? "overtime" : `${mShape.ot} overtimes`}`,
		]);
	}

	if (mShape.comebackFrom >= 15) {
		return `${w} storm back from ${mShape.comebackFrom} down to beat ${l}`;
	}

	if (mShape.margin >= 25) {
		return pick(rng, [
			`${w} rout ${l} by ${mShape.margin}`,
			`${w} blow out ${l} ${scoreTag(mShape)}`,
		]);
	}

	if (mShape.margin <= 3) {
		return pick(rng, [
			`${w} edge ${l} in a ${scoreTag(mShape)} thriller`,
			`${w} hold off ${l} at the wire`,
		]);
	}

	// Nothing dramatic anywhere: lead with the night's biggest individual line.
	if (topPts && topPts.p.pts >= 32) {
		return `${topPts.p.name} pours in ${topPts.p.pts} to headline the night`;
	}
	if (mStar) {
		return pick(rng, [
			`${poss(mStar.name)} ${starHeadline(mStar).text} lead${
				starHeadline(mStar).plural ? "" : "s"
			} ${tw} past ${l}`,
			`${w} ${pick(rng, verbPool(marquee, mShape))} ${l} behind ${mStar.name}`,
		]);
	}
	return `${w} ${pick(rng, verbPool(marquee, mShape))} ${l}`;
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
					pts >= 6
						? `${w} stun ${l} as ${pts}-point underdogs`
						: `${w} stun ${l}`,
				tids,
				game: g,
			});
		}
		if (shape.comebackFrom >= 15) {
			out.push({
				score: 80 + shape.comebackFrom,
				kind: "comeback",
				text: `${w} rally from ${shape.comebackFrom} down past ${l}`,
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
): { text: string; games: RecapGame[] } | undefined => {
	const usedTids = new Set<number>([
		marquee.teams[0].tid,
		marquee.teams[1].tid,
	]);
	const usedKinds = new Set<string>();
	const picks: string[] = [];
	const covered: RecapGame[] = [];
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
		picks.push(s.text);
		usedKinds.add(s.kind);
		for (const tid of s.tids) {
			usedTids.add(tid);
		}
		if (s.game) {
			covered.push(s.game);
		}
		if (picks.length >= 3) {
			break;
		}
	}
	return picks.length > 0
		? { text: picks.join(" · "), games: covered }
		: undefined;
};

// A compact scoreboard-style clause for the "Around the league" sweep, with a
// verb chosen to fit the margin (a blowout "routs", a nail-biter "edges"). `seq`
// rotates the verb DETERMINISTICALLY through the bucket, so a long roundup reads
// "beat ... downed ... got past ..." instead of "beat ... beat ... beat".
const roundupClause = (shape: Shape, seq: number): string => {
	const w = theNick(shape.winner);
	const l = theNick(shape.loser);
	let pool: string[];
	if (shape.ot > 0) {
		pool = ["outlasted", "survived", "edged"];
	} else if (shape.margin >= 20) {
		pool = ["routed", "blew out", "ran away from", "rolled past"];
	} else if (shape.margin <= 4) {
		pool = ["edged", "held off", "slipped past", "outlasted"];
	} else if (shape.margin <= 9) {
		pool = ["beat", "got past", "held off", "topped", "took down"];
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
	return `${w} ${pool[seq % pool.length]} ${l} ${scoreTag(shape)}`;
};

// League-wide notes read off the whole slate: the night's blowout, its
// shootout, the best shooting performance. A day wrap that only lists results
// never tells you what KIND of night it was.
const leagueNotes = (
	games: RecapGame[],
	rng: () => number,
	limit: number,
): string[] => {
	const real = games.filter((g) => !g.allStar);
	if (real.length < 4) {
		return [];
	}

	type Cand = { sort: number; text: string };
	const cands: Cand[] = [];

	let biggest: { shape: Shape; margin: number } | undefined;
	let highest: { shape: Shape; total: number } | undefined;
	let hottest: { shape: Shape; fgp: number } | undefined;
	let bombs: { shape: Shape; tp: number } | undefined;
	for (const g of real) {
		const shape = analyzeShape(g);
		const total = shape.winner.pts + shape.loser.pts;
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

	if (biggest && biggest.margin >= 25) {
		cands.push({
			sort: 0,
			text: `The night's most one-sided result was ${poss(
				theNick(biggest.shape.winner),
			)} ${biggest.margin}-point win over ${theNick(biggest.shape.loser)}.`,
		});
	}
	if (highest && highest.total >= 235) {
		cands.push({
			sort: 1,
			text: `${cap(theNick(highest.shape.winner))} and ${theNick(
				highest.shape.loser,
			)} combined for ${highest.total} points, the most of any game on the slate.`,
		});
	}
	if (hottest && hottest.fgp >= 53) {
		cands.push({
			sort: 2,
			text: `${cap(theNick(hottest.shape.winner))} shot the ball best of anyone, ${hottest.fgp}% from the field.`,
		});
	}
	if (bombs && bombs.tp >= 17) {
		cands.push({
			sort: 3,
			text: `${cap(theNick(bombs.shape.winner))} led the league in threes with ${bombs.tp}.`,
		});
	}

	// Home teams' night, when it is lopsided enough to be worth saying.
	let homeWins = 0;
	let counted = 0;
	for (const g of real) {
		const home = g.teams[0];
		if (home) {
			counted += 1;
			if (g.winnerTid === home.tid) {
				homeWins += 1;
			}
		}
	}
	if (
		counted >= 8 &&
		(homeWins <= counted * 0.3 || homeWins >= counted * 0.8)
	) {
		cands.push({
			sort: 4,
			text:
				homeWins >= counted * 0.8
					? `Home teams went ${homeWins}-${counted - homeWins}.`
					: `It was a night for the road: visitors won ${counted - homeWins} of ${counted}.`,
		});
	}

	return shuffle(rng, cands)
		.sort((a, b) => a.sort - b.sort)
		.slice(0, limit)
		.map((c) => c.text);
};

// How many of the day's games were decided by 5 or fewer.
const closeGamesSentence = (games: RecapGame[]): string | undefined => {
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
	if (close >= 3) {
		return `${cap(numWord(close))} of the ${nonExhibition.length} games were decided by five points or fewer.`;
	}
	if (ot >= 3) {
		return `${ot === nonExhibition.length ? "All" : ot} games went to overtime.`;
	}
	return undefined;
};

export const getAutoDayRecap = (input: AutoDayRecapInput): string => {
	const { games, standings, day, playoffs } = input;

	// One night, one pool of phrasing - see pick().
	beginRecapBatch();

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
	const mStar = bestOf(mShape.winner.players) ?? bestOf(mShape.loser.players);
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
	const deckResult = playoffs
		? undefined
		: dayDeck(collectStorylines(games, performers), marquee, mStar);
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

	// Paragraph 1: the marquee game and the day's best individual nights. The
	// marquee star is already covered in the blurb, so the standouts that follow
	// come from OTHER games and no player is named twice.
	const para1: string[] = [];
	para1.push(`${cap(gameBlurb(marquee, rng))}.`);

	const marqueeTids = new Set([mShape.winner.tid, mShape.loser.tid]);
	const named = new Set<RecapPlayer>();
	if (mStar) {
		named.add(mStar);
	}

	// The day's leading scorer, when it isn't the marquee star already described.
	if (topScorer && topScorer.p.pts >= 30 && !named.has(topScorer.p)) {
		const flourish = shootingFlourish(topScorer.p);
		const line = `${statPhrase(topScorer.p)}${flourish ? ` ${flourish}` : ""}`;
		para1.push(
			topScorer.won
				? `${topScorer.p.name} led all scorers with ${line} in ${poss(
						theNick(topScorer.team),
					)} win over ${theNick(topScorer.opp)}.`
				: `${topScorer.p.name} led all scorers with ${line} despite ${poss(
						theNick(topScorer.team),
					)} loss to ${theNick(topScorer.opp)}.`,
		);
		named.add(topScorer.p);
		coveredGames.add(topScorer.game);
	}

	// A second standout from a different game entirely. Winners only - "added 31
	// points for the Timberwolves" reads as a win contribution, and a big line on
	// the losing side gets the dedicated "losing effort" sentence below instead.
	const secondPerf = performers.find(
		(perf) =>
			!named.has(perf.p) &&
			!marqueeTids.has(perf.team.tid) &&
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
				? `${secondPerf.p.name} posted a ${ddw} (${statPhrase(
						secondPerf.p,
					)}) in ${poss(theNick(secondPerf.team))} win over ${theNick(
						secondPerf.opp,
					)}.`
				: `${secondPerf.p.name} had ${statPhrase(secondPerf.p)} in ${poss(
						theNick(secondPerf.team),
					)} win over ${theNick(secondPerf.opp)}.`,
		);
		named.add(secondPerf.p);
		coveredGames.add(secondPerf.game);
	}

	// A league-wide triple-double gets a nod if it wasn't already the story.
	const tdPerf = performers.find(
		(perf) =>
			doubleCategories(perf.p).length >= 3 &&
			!named.has(perf.p) &&
			!marqueeTids.has(perf.team.tid),
	);
	if (tdPerf) {
		para1.push(
			tdPerf.won
				? `${tdPerf.p.name} put together a triple-double (${statPhrase(
						tdPerf.p,
					)}) in ${poss(theNick(tdPerf.team))} win over ${theNick(tdPerf.opp)}.`
				: `${tdPerf.p.name} put together a triple-double (${statPhrase(
						tdPerf.p,
					)}) in ${poss(theNick(tdPerf.team))} loss to ${theNick(tdPerf.opp)}.`,
		);
		named.add(tdPerf.p);
		coveredGames.add(tdPerf.game);
	}

	// A monster line wasted in a loss is a story of its own (a 23-18-8 night on
	// the losing side shouldn't vanish from the league wrap).
	if (para1.length < 5) {
		const lossPerf = performers.find(
			(perf) =>
				!named.has(perf.p) &&
				!perf.won &&
				!marqueeTids.has(perf.team.tid) &&
				((perf.p.pts >= 20 && doubleCategories(perf.p).length >= 2) ||
					perf.p.pts >= 35),
		);
		if (lossPerf) {
			para1.push(
				`${poss(lossPerf.p.name)} ${statPhrase(
					lossPerf.p,
				)} came in a losing effort against ${theNick(lossPerf.opp)}.`,
			);
			coveredGames.add(lossPerf.game);
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
	// "Stunned" and "shocked" belong to a real number. A 3.5-point dog winning
	// gets the flat verbs, so the strong ones still mean something when the
	// 13.5-point dog turns up two clauses later.
	const bigUpsetVerbs = ["stunned", "shocked", "toppled", "upset"];
	const smallUpsetVerbs = ["upset", "got past", "took down", "knocked off"];
	let upsetIdx = 0;
	for (const g of others) {
		if (g.allStar || coveredGames.has(g)) {
			continue;
		}
		const shape = analyzeShape(g);
		const shot2 = clutchShot(g);
		let blurb: string | undefined;
		if (isUpset(g, shape)) {
			const spreadPts = g.spread?.points;
			const verbs =
				spreadPts !== undefined && spreadPts >= 7
					? bigUpsetVerbs
					: smallUpsetVerbs;
			const verb = verbs[upsetIdx % verbs.length]!;
			blurb =
				upsetIdx === 0 && spreadPts !== undefined && spreadPts >= 5
					? `${theNick(shape.winner)} ${verb} ${theNick(
							shape.loser,
						)} as ${spreadPts}-point underdogs`
					: `${theNick(shape.winner)} ${verb} ${theNick(shape.loser)}`;
			upsetIdx += 1;
		} else if (shape.comebackFrom >= 15) {
			blurb = `${theNick(shape.winner)} erased ${aNum(
				shape.comebackFrom,
			)}-point deficit to beat ${theNick(shape.loser)}`;
		} else if (shot2 && !shot2.tying) {
			blurb = `${shot2.name} beat ${theNick(shape.loser)} at the wire`;
		} else if (shape.ot > 0) {
			blurb = `${theNick(shape.winner)} outlasted ${theNick(
				shape.loser,
			)} in overtime`;
		} else if (shape.margin >= 25) {
			blurb = `${theNick(shape.winner)} routed ${theNick(shape.loser)} by ${
				shape.margin
			}`;
		} else if (shape.margin <= 3) {
			blurb = `${theNick(shape.winner)} edged ${theNick(shape.loser)} ${scoreTag(
				shape,
			)}`;
		}
		if (blurb) {
			notableBlurbs.push(blurb);
			coveredGames.add(g);
			if (notableBlurbs.length >= 5) {
				break;
			}
		}
	}
	if (notableBlurbs.length > 0) {
		para2.push(`Elsewhere, ${naturalList(notableBlurbs)}.`);
	}

	// Around the league: sweep up every game not already told, with its score, so
	// nothing on the slate goes unmentioned. Ordered by notability so the more
	// interesting leftovers lead; capped so a huge slate rolls the tail into "and
	// N others" rather than an endless run-on.
	const ROUNDUP_CAP = 10;
	const roundupClauses: string[] = [];
	let roundupExtra = 0;
	for (const g of ranked) {
		if (g.allStar || coveredGames.has(g)) {
			continue;
		}
		if (roundupClauses.length < ROUNDUP_CAP) {
			roundupClauses.push(
				roundupClause(analyzeShape(g), roundupClauses.length),
			);
		} else {
			roundupExtra += 1;
		}
	}
	if (roundupClauses.length > 0) {
		const items =
			roundupExtra > 0
				? [...roundupClauses, `${roundupExtra} other games`]
				: roundupClauses;
		// Broken into sentences of three or four rather than one enormous comma
		// list. Eight results strung together with commas is technically complete
		// and completely unreadable; nobody gets to the end of it.
		const OPENERS = [
			"Around the league",
			"Also on the night",
			"In the rest of the schedule",
			"Rounding out the slate",
		];
		const CHUNK = 4;
		for (let i = 0, n = 0; i < items.length; i += CHUNK, n += 1) {
			// A trailing chunk of one reads as an afterthought ("Also on the night,
			// the Hornets topped the Magic."), so absorb it into the one before.
			const slice =
				items.length - i === CHUNK + 1
					? items.slice(i)
					: items.slice(i, i + CHUNK);
			para2.push(`${OPENERS[n % OPENERS.length]}, ${naturalList(slice)}.`);
			i += slice.length - CHUNK;
		}
	}

	// Paragraph 3: the league picture - close-game count, injury news, and then
	// either the playoff series state or the streak/standings context.
	const para3: string[] = [];

	const close = closeGamesSentence(games);
	if (close) {
		para3.push(close);
	}

	// What kind of night it was, league-wide: the blowout, the shootout, the best
	// shooting performance. Two at most, so the wrap stays a wrap.
	for (const note of leagueNotes(games, rng, 2)) {
		para3.push(note);
	}

	// The night's injury news - who went down matters to the league picture.
	const injuries = injuryRoundup(games);
	if (injuries) {
		para3.push(injuries);
	}

	if (playoffs) {
		// Series developments across the day, skipping the marquee game (already the
		// story) and any duplicate phrasing.
		const seriesBits: string[] = [];
		const seen = new Set<string>();
		for (const g of games) {
			if (marqueeTids.has(g.teams[0].tid) && marqueeTids.has(g.teams[1].tid)) {
				continue;
			}
			const phrase = daySeriesPhrase(g, rng);
			if (phrase && !seen.has(phrase)) {
				seen.add(phrase);
				seriesBits.push(phrase);
			}
			if (seriesBits.length >= 4) {
				break;
			}
		}
		if (seriesBits.length > 0) {
			para3.push(`In the playoffs, ${naturalList(seriesBits)}.`);
		}
	} else {
		const streak = teamStreakSentence(games);
		if (streak) {
			para3.push(streak);
		}
		const picture = conferencePictureSentence(standings);
		if (picture) {
			para3.push(picture);
		}
	}

	const paragraphs = [dedupeSubjects(para1).join(" ")];
	if (para2.length > 0) {
		paragraphs.push(dedupeSubjects(para2).join(" "));
	}
	if (para3.length > 0) {
		paragraphs.push(dedupeSubjects(para3).join(" "));
	}

	const head = deck ? `**${headline}**\n\n*${deck}*` : `**${headline}**`;
	return `${head}\n\n${paragraphs.join("\n\n")}`;
};

export default getAutoRecap;
