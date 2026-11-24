// THE NIGHT IN THE LEAGUE.
//
// How the wire services and the league's own site write a night's roundup,
// read for this file (AP and Field Level Media "NBA roundup" pieces, NBA.com's
// nightly "Starting 5", Rotowire's "Around the Association"):
//
//   - The headline is the night's top story, written as a headline: present
//     tense, no articles in front of team names, figures for numbers, the
//     score when it is a result. "Upstart Bulls snap Timberwolves' 5-game win
//     streak". "Poole's 3-pointer with 2.1 seconds left gives Wizards a
//     126-123 win". A team gets a word that says where it stands - "East-
//     leading", "winless", "short-handed" - when one fits.
//   - Under it, one line of the night's other stories. NBA.com's: "Buzzer-
//     beaters and thrilling finishes aplenty marked Monday's slate".
//   - The top story leads, as a wire lede: the man, what he did, the result
//     and the score in one sentence. Then the detail that makes it his night,
//     what it meant, and - when the night had a shape, three games won at the
//     buzzer or five favorites beaten - a line that turns from it to the rest.
//   - Every other game gets its own item, led by its final in bold, winner
//     first: "Nuggets 116, Pelicans 108:". The final is in the lead-in, so the
//     sentences under it never say it again; they say who won it and how, and
//     one thing it meant - a streak, a skid, first place, a man hurt, where a
//     series stands. One or two sentences, never a list of box-score lines.
//   - Streaks, records and injuries ride inside the item they belong to, not
//     in a pile at the bottom. What is left - the standings picture and what
//     is next on the schedule - closes the piece.
//   - Two stories of a kind are told as one: "Five teams took 2-0 series
//     leads", "the Clippers and the Hawks pull off upsets".
//
// The per-game recaps under each card tell each game in full; this tells the
// night, so it never borrows their sentences.

import {
	analyzeShape,
	bestOf,
	bigQuarterRole,
	closeLate,
	clutchShot,
	comebackSize,
	conferencePictureSentence,
	gamesLeft,
	gamesOut,
	heldOn,
	hurtInGame,
	hurtIsNews,
	isUpset,
	lateGoAhead,
	notability,
	pastTense,
	postseasonContext,
	snappedSkid,
	storyPlayerOf,
	verbPool,
	type AutoDayRecapInput,
	type ClutchShot,
	type PostseasonContext,
	type Shape,
} from "./getAutoRecap.ts";
import type {
	RecapGame,
	RecapPlayer,
	RecapTeam,
} from "./getDayGamesForRecap.ts";
import { decisiveRuns, type RunFact } from "./recapStory.ts";
import { clockLeft } from "../../common/gameFlow.ts";
import {
	dayBracketWatch,
	dayNextSlate,
	highOf,
	type DayBeatContext,
} from "./recapBeats.ts";
import { surnameOf } from "./recapNames.ts";
import {
	aFigure,
	aNumWord,
	cap,
	doubleCategories,
	doubleWord,
	lowerInjury,
	naturalList,
	nick,
	numWord,
	ordinal,
	otPhrase,
	pick,
	pickKeyed,
	plural,
	poss,
	spelledNum,
	statPhrase,
	theNick,
} from "./recapText.ts";

type Rng = () => number;

// --- What each game was ------------------------------------------------------

// The thing a game is remembered for, in the order a desk ranks them when a
// game has more than one.
type Kind =
	| "title"
	| "clinch"
	| "playIn"
	| "series"
	| "shot"
	| "lateGoAhead"
	| "monsterLoss"
	| "big"
	| "comeback"
	| "upset"
	| "snap"
	| "ended"
	| "ot"
	| "streak"
	| "close"
	| "rout"
	| "tie"
	| "plain";

type Hurt = {
	t: RecapTeam;
	p: RecapPlayer;
	n: number;
	left?: number;
};

// Where a playoff series stands after tonight.
type SeriesState =
	| "opener"
	| "lead"
	| "brink"
	| "even"
	| "force"
	| "alive"
	| "cut"
	| "over";

export type Story = {
	game: RecapGame;
	shape: Shape;
	star?: RecapPlayer;
	W: RecapTeam;
	L: RecapTeam;
	// "the Kings", and "Kings" as a headline names them.
	w: string;
	l: string;
	wn: string;
	ln: string;
	// "113-111", "119-112 (OT)".
	score: string;
	// "Kings 113, Pacers 111", winner first, as every roundup leads an item.
	// (The score as prose says it, "129-123 in overtime", is `score`.)
	header: string;
	kind: Kind;
	// The losing side's best, when he outplayed everyone in a close game.
	outplayed?: RecapPlayer;
	// The kind of game it was, series aside: a night of eight openers says
	// "Game 1" twice and tells the rest as games.
	gameKind: Kind;
	shot?: ClutchShot;
	late?: { text: string; name: string; what: string; left: string };
	// The run the game ended on, from a game still in the balance, and the
	// winners' run that broke a close game open (recapStory.decisiveRuns).
	closer?: RunFact;
	breaker?: RunFact;
	comeback: number;
	// Points the winners were getting from the books, when it was an upset.
	dog?: number;
	// The losers' man with a line that is a story of its own, and their
	// leading scorer when the game was close enough for him to matter.
	loserStar?: RecapPlayer;
	loserLead?: RecapPlayer;
	// The winners' streak after tonight, a skid tonight ended, and the
	// losers' winning run it ended, and their skid after it.
	streak?: number;
	skid?: number;
	ended?: number;
	lSkid?: number;
	unbeaten?: number;
	winless?: number;
	// Moved into first place in its conference tonight.
	firstIn?: string;
	hurt: Hurt[];
	post?: PostseasonContext;
	series?: {
		gameNo: number;
		wAfter: number;
		lAfter: number;
		need: number;
		bestOf: number;
		round: string;
		state: SeriesState;
	};
	weight: number;
};

const roundOf = (round: number, numRounds: number): string =>
	round === numRounds
		? "the Finals"
		: round === numRounds - 1
			? "the Conference Finals"
			: round === numRounds - 2
				? "the Conference Semifinals"
				: round === 1
					? "the First Round"
					: `Round ${round}`;

const stateOf = (
	gameNo: number,
	a: number,
	b: number,
	need: number,
): SeriesState =>
	a >= need
		? "over"
		: a === b
			? a === need - 1
				? "force"
				: "even"
			: gameNo === 1
				? "opener"
				: a > b
					? a === need - 1
						? "brink"
						: "lead"
					: b === need - 1
						? "alive"
						: "cut";

// A win streak this long ended is one of the night's stories.
const LONG_RUN = 10;

const storyOf = (game: RecapGame): Story => {
	const shape = analyzeShape(game);
	const W = shape.winner;
	const L = shape.loser;
	const star = storyPlayerOf(game, shape);
	const shotRaw = clutchShot(game);
	const shot = shotRaw && !shotRaw.tying ? shotRaw : undefined;
	const late = shot ? undefined : lateGoAhead(game, shape, shotRaw);
	const ot =
		shape.ot > 0 ? ` (${shape.ot === 1 ? "OT" : `${shape.ot}OT`})` : "";
	const comeback = comebackSize(shape);
	const dog =
		!game.playoffs && isUpset(game, shape) && game.spread
			? game.spread.points
			: undefined;
	const lBest = bestOf(L.players);
	const lTop = [...L.players].sort((a, b) => b.pts - a.pts)[0];
	const loserStar =
		lBest &&
		(lBest.pts >= 35 ||
			doubleCategories(lBest).length >= 3 ||
			(lBest.pts >= 28 && lBest.pts >= (star?.pts ?? 0) + 6) ||
			(lBest.pts >= 18 &&
				doubleCategories(lBest).length >= 2 &&
				(lBest.reb >= 15 || lBest.ast >= 12)))
			? lBest
			: undefined;
	const loserLead =
		!loserStar && lTop && lTop.pts >= 25 && shape.margin <= 10
			? lTop
			: undefined;
	// The best man on the floor lost, in a game close enough to be his.
	const outplayed =
		lBest &&
		shape.margin <= 10 &&
		lBest.pts >= 20 &&
		lBest.pts >= (star?.pts ?? 0) + 6 &&
		(lBest.pts >= 24 || doubleCategories(lBest).length >= 2)
			? lBest
			: undefined;
	const wStreak = W.streak?.won ? W.streak.count : undefined;
	const lStreak = L.streak && !L.streak.won ? L.streak.count : undefined;
	const lRun = L.streakBefore?.won ? L.streakBefore.count : undefined;
	const st = W.standing;
	const firstIn =
		!game.playoffs &&
		st &&
		st.rank === 1 &&
		st.rankBefore !== undefined &&
		st.rankBefore > 1 &&
		st.won + st.lost >= 10
			? st.conf
			: undefined;
	const hurt: Hurt[] = [];
	for (const t of game.teams) {
		for (const p of t.players) {
			const n = p.injury?.newThisGame ? (p.injury.gamesRemaining ?? 0) : 0;
			if (n >= 5 && hurtIsNews(p)) {
				hurt.push({ t, p, n, left: gamesLeft(game, t) });
			}
		}
	}
	hurt.sort((a, b) => b.n - a.n);

	let series: Story["series"];
	const s = game.series;
	if (game.playoffs && s && typeof s.bestOf === "number" && s.bestOf > 1) {
		const winnerHome = W.abbrev === s.homeAbbrev;
		const wAfter = (winnerHome ? s.homeWon : s.awayWon) + 1;
		const lAfter = winnerHome ? s.awayWon : s.homeWon;
		const need = Math.floor(s.bestOf / 2) + 1;
		const gameNo = s.homeWon + s.awayWon + 1;
		series = {
			gameNo,
			wAfter,
			lAfter,
			need,
			bestOf: s.bestOf,
			round: roundOf(s.round, s.numRounds),
			state: stateOf(gameNo, wAfter, lAfter, need),
		};
	}
	const post = game.playoffs ? postseasonContext(game, shape) : undefined;
	const wRec = W.record;
	const lRec = L.record;
	const unbeaten =
		!game.playoffs && wRec && wRec.lost === 0 && wRec.won >= 4
			? wRec.won
			: undefined;
	const winless =
		!game.playoffs && lRec && lRec.won === 0 && lRec.lost >= 4
			? lRec.lost
			: undefined;

	const big =
		!!star &&
		(star.pts >= 40 ||
			doubleCategories(star).length >= 3 ||
			(star.pts >= 20 && star.reb >= 20));
	// What the game itself was, series aside.
	const gameKind: Kind = shot
		? "shot"
		: late
			? "lateGoAhead"
			: comeback >= 15
				? "comeback"
				: loserStar &&
					  loserStar.pts >= 35 &&
					  loserStar.pts >= (star?.pts ?? 0) + 8
					? "monsterLoss"
					: big
						? "big"
						: comeback >= 12
							? "comeback"
							: dog !== undefined
								? "upset"
								: !game.playoffs && (snappedSkid(shape) ?? 0) >= 5
									? "snap"
									: !game.playoffs && (lRun ?? 0) >= 5
										? "ended"
										: shape.ot > 0
											? "ot"
											: !game.playoffs && (wStreak ?? 0) >= 5
												? "streak"
												: shape.margin <= 5 && closeLate(game)
													? "close"
													: shape.margin >= 25
														? "rout"
														: "plain";
	const kind: Kind =
		shape.margin === 0
			? "tie"
			: post?.clinch?.title
				? "title"
				: post?.clinch
					? "clinch"
					: game.playIn
						? "playIn"
						: series
							? "series"
							: gameKind;
	return {
		game,
		shape,
		star,
		W,
		L,
		w: theNick(W),
		l: theNick(L),
		wn: nick(W),
		ln: nick(L),
		score: `${W.pts}-${L.pts}${shape.ot > 0 ? ` in ${otPhrase(shape.ot)}` : ""}`,
		header: `${nick(W)} ${W.pts}, ${nick(L)} ${L.pts}${ot}${game.shootout ? `, ${nick(W)} win shootout` : ""}`,
		kind,
		gameKind,
		shot,
		late,
		...decisiveRuns(game, shape),
		comeback,
		dog,
		loserStar,
		loserLead,
		outplayed,
		streak:
			!game.playoffs && wStreak !== undefined && wStreak >= 4
				? wStreak
				: undefined,
		skid: game.playoffs ? undefined : snappedSkid(shape),
		ended: !game.playoffs && lRun !== undefined && lRun >= 5 ? lRun : undefined,
		lSkid:
			!game.playoffs && lStreak !== undefined && lStreak >= 5
				? lStreak
				: undefined,
		unbeaten,
		winless,
		firstIn,
		hurt,
		post,
		series,
		// (A long win streak ended is news on a night, not in its game's own
		// recap.)
		weight:
			notability(game) +
			(!game.playoffs && (lRun ?? 0) >= LONG_RUN ? 40 + 2 * lRun! : 0),
	};
};

// --- Words -------------------------------------------------------------------

// "a 129-123 overtime win".
const aWin = (s: Story): string =>
	`${aFigure(`${s.W.pts}-${s.L.pts}`)} ${s.shape.ot > 0 ? "overtime " : ""}win`;

// "123-121 2OT win", as a headline has it.
const headWin = (s: Story): string =>
	`${s.W.pts}-${s.L.pts}${s.shape.ot > 0 ? ` ${s.shape.ot === 1 ? "OT" : `${s.shape.ot}OT`}` : ""} win`;

// The score as a headline has it: "129-123 in OT", "118-115 in 2OT".
const headScore = (s: Story): string =>
	`${s.W.pts}-${s.L.pts}${s.shape.ot > 0 ? ` in ${s.shape.ot === 1 ? "OT" : `${s.shape.ot}OT`}` : ""}`;

// A man's night as a short line: "49 points", "25 points and 19 rebounds",
// "15 points, 12 rebounds and 11 assists".
const lineOf = (p: RecapPlayer): string => statPhrase(p);
const onlyPoints = (p: RecapPlayer): boolean =>
	lineOf(p) === plural(p.pts, "point");

// His line with a season high called one: "a season-high 41 points".
// After a possessive it takes no article: "Marcus Brooks' season-high 45".
const lineWithHigh = (p: RecapPlayer, possessive = false): string => {
	const line = lineOf(p);
	const high = highOf(p);
	if (!high) {
		return line;
	}
	const figure = `${high.n} ${high.unit}`;
	return line.includes(figure)
		? line.replace(figure, `${possessive ? "" : "a "}${high.label} ${figure}`)
		: line;
};

// "had 25 points and 19 rebounds", "scored 49 points", "poured in 41", with
// a triple-double called one.
const did = (p: RecapPlayer, rng: Rng): string => {
	const line = lineWithHigh(p);
	const dd = doubleCategories(p).length;
	if (dd >= 3) {
		return `recorded a ${doubleWord(dd)!} with ${line}`;
	}
	if (!onlyPoints(p) && p.pts >= 35) {
		const pts = `${line.split(/, | and /)[0]}`;
		const extras = naturalList(line.split(/, | and /).slice(1));
		return pick(
			rng,
			[
				`scored ${pts} to go with ${extras}`,
				`poured in ${pts} and added ${extras}`,
			],
			"day:didBigLine",
		);
	}
	if (onlyPoints(p)) {
		return pick(
			rng,
			p.pts >= 35
				? [`scored ${line}`, `poured in ${line}`, `went for ${line}`]
				: [`scored ${line}`, `had ${line}`],
			p.pts >= 35 ? "day:didBig" : "day:didPts",
		);
	}
	return pick(
		rng,
		[`had ${line}`, `finished with ${line}`, `put up ${line}`],
		"day:didLine",
	);
};

// "Walker Young had 25 points to lead the Pistons" - and not "scored 37
// points to go with 13 rebounds to lead", two infinitives running.
const leadLine = (p: RecapPlayer, w: string, rng: Rng): string => {
	const d = did(p, rng);
	return d.includes(" to go with ")
		? `${p.name} ${d} for ${w}.`
		: `${p.name} ${d} to lead ${w}.`;
};

// "a three-pointer at the buzzer", "a layup with 1.3 seconds left".
const shotWhat = (shot: ClutchShot): string => {
	const what =
		shot.shot === "game-winner"
			? "the go-ahead basket"
			: shot.shot === "buzzer-beater"
				? "a buzzer-beater"
				: /^(?:free throw|two free throws)$/.test(shot.shot)
					? shot.shot === "free throw"
						? "a free throw"
						: "two free throws"
					: `a ${shot.shot}`;
	const when =
		shot.shot === "buzzer-beater"
			? ""
			: shot.buzzer
				? " at the buzzer"
				: shot.seconds !== undefined
					? ` with ${plural(shot.seconds, "second")} left`
					: "";
	return `${what}${when}`;
};

const shotVerb = (what: string): string =>
	/three-point play|four-point play/.test(what)
		? "converted"
		: /dunk/.test(what)
			? "threw down"
			: /tip|putback/.test(what)
				? "had"
				: /free throw|layup|hook/.test(what)
					? "made"
					: "hit";

const COMEBACK = /rally|come back|storm back|upset|stun|shock|knock off/;

// The result verb for the score: "beat", "edged", "routed".
const beatVerb = (s: Story, rng: Rng): string => {
	const pool = verbPool(s.game, s.shape).filter((v) => !COMEBACK.test(v));
	return pastTense(pick(rng, pool.length > 0 ? pool : ["beat"]));
};

// How the game was won, as a predicate for the winners, when its shape says
// something: "pulled away with a 34-25 third quarter", "held on late".
const howWon = (s: Story, rng: Rng): string | undefined => {
	const sh = s.shape;
	// The run that settled it, when the score log had one: more exact than
	// the quarter it came in.
	const c = s.closer;
	if (c && !s.shot && !s.late) {
		return c.ended
			? pick(
					rng,
					[
						`closed on ${aFigure(`${c.pts}-${c.opp}`)} run`,
						c.opp === 0
							? `scored the last ${c.pts} points`
							: `scored ${c.pts} of the last ${c.pts + c.opp} points`,
					],
					"day:how:closer",
				)
			: `pulled away late with ${aFigure(`${c.pts}-${c.opp}`)} run`;
	}
	const b = s.breaker;
	// A quarter that did more than the run inside it is told as the quarter.
	const q = sh.bigRun;
	const quarterBigger =
		!!q && !!b && q.margin >= b.pts - b.opp + 5 && q.period === b.period;
	if (b && b.period <= 4 && !quarterBigger) {
		const when =
			b.period === b.endPeriod
				? ` in the ${ordinal(b.period)} quarter`
				: b.endPeriod === b.period + 1 && b.period < 4
					? ` spanning the ${ordinal(b.period)} and ${ordinal(b.endPeriod)} quarters`
					: "";
		const run = `${aFigure(`${b.pts}-${b.opp}`)} run${when}`;
		return b.period <= 2
			? pick(
					rng,
					[`took control with ${run}`, `took over with ${run}`],
					"day:how:breakEarly",
				)
			: pick(
					rng,
					[`pulled away with ${run}`, `broke it open with ${run}`],
					"day:how:breakLate",
				);
	}
	const r = sh.bigRun;
	const role = bigQuarterRole(sh);
	const quarter = r
		? `${aFigure(`${r.wpts}-${r.lpts}`)} ${ordinal(r.period)} quarter`
		: "";
	if (r && role === "broke" && r.margin >= 9 && sh.margin >= 8) {
		// A first or second quarter builds a lead; a later one pulls away.
		return r.period <= 2
			? pick(
					rng,
					[
						`took control early with ${quarter}`,
						`built a big lead with ${quarter}`,
					],
					"day:how:early",
				)
			: pick(
					rng,
					[
						`pulled away with ${quarter}`,
						`broke it open with ${quarter}`,
						`put it away with ${quarter}`,
					],
					"day:how:late",
				);
	}
	if (sh.wireToWire && sh.margin >= 10) {
		return "led from start to finish";
	}
	if (r && role === "turned" && r.margin >= 8) {
		return pick(
			rng,
			[`took control with ${quarter}`, `turned it around with ${quarter}`],
			"day:how:turned",
		);
	}
	if (sh.margin <= 5 && closeLate(s.game)) {
		return heldOn(s.game) ? "held on late" : "pulled it out late";
	}
	if (sh.margin >= 20) {
		return `won by ${sh.margin}`;
	}
	return undefined;
};

// --- The headline ------------------------------------------------------------
//
// As a desk writes one: the subject does something to somebody, in the
// present tense, with no "the" in front of a team, figures for numbers and
// the score where it is a result.

const headOrd = (n: number): string => {
	const teen = n % 100 >= 11 && n % 100 <= 13;
	const suffix = teen
		? "th"
		: n % 10 === 1
			? "st"
			: n % 10 === 2
				? "nd"
				: n % 10 === 3
					? "rd"
					: "th";
	return `${n}${suffix}`;
};

// "3-pointer", "3-point play": the wire's figures.
export const headShot = (shot: string): string =>
	shot
		.replace(/three-pointer/, "3-pointer")
		.replace(/three-point play/, "3-point play")
		.replace(/four-point play/, "4-point play");

// A word for where a team stands, the one a desk would put in front of its
// name: "East-leading Knicks", "winless Pistons", "short-handed Nuggets".
// Only one, and only when it is true and says something.
export const tagOf = (
	t: RecapTeam,
	won: boolean,
	game: RecapGame,
): string | undefined => {
	if (game.playoffs) {
		return undefined;
	}
	const rec = t.record;
	const before = rec
		? {
				won: rec.won - (won ? 1 : 0),
				lost: rec.lost - (won ? 0 : 1),
			}
		: undefined;
	if (before && before.lost === 0 && before.won >= 5) {
		return "unbeaten";
	}
	if (before && before.won === 0 && before.lost >= 5) {
		return "winless";
	}
	const st = t.standing;
	const conf = /^(east|west)/i.exec(st?.conf ?? "")?.[1];
	if (
		st &&
		conf &&
		st.rankBefore === 1 &&
		before &&
		before.won + before.lost >= 12
	) {
		return `${conf[0]!.toUpperCase()}${conf.slice(1).toLowerCase()}-leading`;
	}
	const out = (t.injuries ?? []).filter((x) => (x.mpg ?? 0) >= 24).length;
	if (out >= 2) {
		return "short-handed";
	}
	return undefined;
};

const HEAD_MAX = 80;

// What a series result does, as a headline predicate: "take 3-2 series
// lead", "force Game 7".
const headState = (sr: NonNullable<Story["series"]>): string => {
	const { wAfter: a, lAfter: b } = sr;
	switch (sr.state) {
		case "force":
			return `force Game ${sr.bestOf}`;
		case "even":
			return `even series ${a}-${b}`;
		case "opener":
			return "take Game 1";
		case "lead":
		case "brink":
			return `take ${a}-${b} series lead`;
		case "alive":
			return "stay alive";
		case "cut":
			return `cut series deficit to ${b}-${a}`;
		case "over":
			return "win series";
	}
};

// The man on the losing side who outplayed everyone, in a game close
// enough that he is the story: "Celtics beat Grizzlies 81-72 despite
// Walker's 27".
const outplayedHead = (
	s: Story,
	Wt: string,
	Lt: string,
	sc: string,
	nm: (name: string) => string,
	options: string[],
): boolean => {
	const lp = s.outplayed;
	if (!lp) {
		return false;
	}
	const dd = doubleCategories(lp).length;
	options.push(
		`${Wt} beat ${Lt} ${sc} despite ${poss(nm(lp.name))} ${lp.pts}`,
		dd >= 2 && lp.reb >= 10
			? `${nm(lp.name)} has ${lp.pts} points, ${lp.reb} rebounds, but ${Wt} win ${sc}`
			: `${nm(lp.name)} scores ${lp.pts}, but ${Wt} beat ${Lt} ${sc}`,
	);
	return true;
};

// A favorite by six or more that won by four or fewer escaped.
const escaped = (s: Story): boolean =>
	!!s.game.spread &&
	s.game.spread.favTid === s.W.tid &&
	s.game.spread.points >= 6 &&
	s.shape.margin <= 4;

// The wire's ways of heading a win with nothing more to it than who won and
// who carried them: "Young leads Pistons past 76ers 110-107", "Jackson
// scores 31, Bulls beat Hawks 116-95", "Bulls beat Hawks 116-95 behind
// Jackson's 31", "Flagg has 26 points, 22 rebounds as Nets top Kings".
const plainHeads = (
	s: Story,
	Wt: string,
	Lt: string,
	sc: string,
	nm: (name: string) => string,
): string[] => {
	const out: string[] = [];
	const st = s.star;
	const margin = s.shape.margin;
	const beat = margin >= 15 ? "roll past" : margin <= 3 ? "edge" : "beat";
	if (st) {
		const n = nm(st.name);
		const dd = doubleCategories(st);
		// A night of steals or blocks is headlined as one.
		if (st.stl >= 5) {
			out.push(`${n} has ${st.stl} steals as ${Wt} ${beat} ${Lt} ${sc}`);
		}
		if (st.blk >= 5) {
			out.push(`${n} blocks ${st.blk} shots as ${Wt} ${beat} ${Lt} ${sc}`);
		}
		if (out.length > 0) {
			return out;
		}
		if (st.pts >= 20) {
			out.push(
				`${n} leads ${Wt} past ${Lt} ${sc}`,
				`${n} helps ${Wt} ${beat} ${Lt} ${sc}`,
				`${Wt} ${beat} ${Lt} ${sc} behind ${poss(n)} ${st.pts}`,
			);
		}
		if (st.pts >= 25) {
			out.push(`${n} scores ${st.pts}, ${Wt} ${beat} ${Lt} ${sc}`);
		}
		if (dd.length >= 2 && st.pts >= 15) {
			const other =
				st.reb >= st.ast ? `${st.reb} rebounds` : `${st.ast} assists`;
			out.push(`${n} has ${st.pts} points, ${other} as ${Wt} top ${Lt}`);
		}
		// Two men with big nights share it, the bigger first: "Kidd, Johnson
		// lead Lakers past Mavericks 120-111".
		const two = s.W.players
			.filter(
				(p) =>
					p !== st &&
					(p.pts >= 20 || (p.pts >= 10 && doubleCategories(p).length >= 2)),
			)
			.sort((a, b) => b.pts - a.pts)[0];
		if (two && (st.pts >= 20 || dd.length >= 2)) {
			const [a, b] = two.pts > st.pts ? [two, st] : [st, two];
			out.push(`${nm(a.name)}, ${nm(b.name)} lead ${Wt} past ${Lt} ${sc}`);
		}
	}
	if (escaped(s)) {
		out.push(`${Wt} escape ${Lt} ${sc}`);
	}
	// "Nuggets lose 7th straight, fall to Nets 99-92".
	if (s.lSkid !== undefined && s.lSkid >= 6) {
		out.push(
			`${s.ln} lose ${headOrd(s.lSkid)} straight, fall to ${s.wn} ${sc}`,
			`${Wt} beat ${s.ln} ${sc}, hand them ${headOrd(s.lSkid)} straight loss`,
		);
	}
	if (margin >= 15) {
		out.push(`${Wt} cruise past ${Lt} ${sc}`);
	}
	if (out.length === 0) {
		out.push(`${Wt} beat ${Lt} ${sc}`, `${Wt} top ${Lt} ${sc}`);
	}
	return out;
};

// "Young" for Walker Young, "Walker Young" when a Darius Young played too -
// or when Young is what a team in the game is called.
const headName = (name: string, s: Story): string => {
	const sur = surnameOf(name);
	if (!sur) {
		return name;
	}
	const clash =
		[...s.W.players, ...s.L.players].some(
			(p) => p.name !== name && surnameOf(p.name) === sur,
		) || [s.W, s.L].some((t) => [t.name, nick(t)].includes(sur));
	return clash ? name : sur;
};

// A headline written with whole names, as a desk prints it: each man by his
// surname. The whole names stay with the writer, who checks what the
// headline said against them.
export const surnamesIn = (text: string, game: RecapGame): string => {
	const s = { W: game.teams[0], L: game.teams[1] } as Story;
	let out = text;
	const names = [...game.teams[0].players, ...game.teams[1].players]
		.map((p) => p.name)
		.sort((a, b) => b.length - a.length);
	for (const name of names) {
		if (out.includes(name)) {
			out = out.replaceAll(name, headName(name, s));
		}
	}
	return out;
};

// The seeds, in a playoff headline where they make it an upset: "No. 8
// Suns", "top-seeded Warriors".
const seedsOf = (s: Story): { w: string; l: string } | undefined => {
	const st = s.post?.clinch ?? s.post?.state;
	if (
		!st?.seedUpset ||
		typeof st.wSeed !== "number" ||
		typeof st.lSeed !== "number"
	) {
		return undefined;
	}
	return {
		w: `No. ${st.wSeed} ${s.wn}`,
		l: st.lSeed === 1 ? `top-seeded ${s.ln}` : `No. ${st.lSeed} ${s.ln}`,
	};
};

const headlineOf = (s: Story, rng: Rng, themes: Theme[] = []): string => {
	const seeds = seedsOf(s);
	const W = seeds?.w ?? s.wn;
	const L = seeds?.l ?? s.ln;
	const sc = headScore(s);
	const star = s.star;
	const wTag = tagOf(s.W, true, s.game);
	const lTag = tagOf(s.L, false, s.game);
	// One tag, the loser's first: "Hornets stun East-leading Knicks".
	const Wt = lTag ? W : wTag ? `${wTag} ${W}` : W;
	const Lt = lTag ? `${lTag} ${L}` : L;
	const ser = s.series;
	// Whole names here; `surnamesIn` makes them a headline's.
	const nm = (name: string) => name;
	const options: string[] = [];
	switch (s.kind) {
		case "title": {
			const games = ser ? ser.wAfter + ser.lAfter : 0;
			options.push(
				ser && ser.lAfter === 0
					? `${W} sweep ${L} to win championship`
					: games === ser?.bestOf
						? `${W} win Game ${games} over ${L}, take championship`
						: `${W} beat ${L} ${sc} in Game ${games} to win championship`,
			);
			break;
		}
		case "clinch": {
			const next = s.post?.clinch?.nextRound.replace(/^the /, "") ?? "";
			const games = ser ? ser.wAfter + ser.lAfter : 0;
			options.push(
				ser && ser.lAfter === 0
					? `${W} sweep ${L}, advance to ${next}`
					: games === ser?.bestOf
						? `${W} beat ${L} in Game ${games}, reach ${next}`
						: `${W} eliminate ${L} in ${games}, move on to ${next}`,
			);
			break;
		}
		case "playIn": {
			const pi = s.game.playIn;
			options.push(
				pi?.kind === "seed9v10"
					? `${W} beat ${L} ${sc}, end ${poss(L)} season`
					: pi?.kind === "final"
						? `${W} beat ${L} ${sc} for last playoff spot`
						: pi?.prizeSeed !== undefined
							? `${W} beat ${L} ${sc}, clinch No. ${pi.prizeSeed} seed`
							: `${W} beat ${L} ${sc} in play-in`,
			);
			break;
		}
		case "series": {
			const sr = ser!;
			const st = headState(sr);
			if (s.shot) {
				const { wAfter: a, lAfter: b } = sr;
				const tail =
					sr.state === "force"
						? `to force Game ${sr.bestOf}`
						: sr.state === "even"
							? "to even series"
							: sr.state === "opener"
								? "in Game 1"
								: sr.state === "alive"
									? "to stay alive"
									: sr.state === "cut"
										? `to cut series deficit to ${b}-${a}`
										: `for ${a}-${b} series lead`;
				options.push(
					`${poss(nm(s.shot.name))} ${headShot(s.shot.shot)} lifts ${W} past ${L} ${tail}`,
				);
			}
			// The game's own story comes with the series': "Hawks rally from
			// 13 down to beat Kings 109-101, take Game 1".
			const how =
				s.comeback >= 12
					? `rally from ${s.comeback} down to beat`
					: s.shape.margin >= 20
						? "rout"
						: s.shape.ot > 0
							? "outlast"
							: "beat";
			options.push(
				`${W} ${how} ${L} ${sc}, ${st}`,
				...(star && star.pts >= 30
					? [`${nm(star.name)} scores ${star.pts}, ${W} ${st}`]
					: []),
			);
			break;
		}
		case "shot": {
			const sh = s.shot!;
			const what = headShot(sh.shot);
			const when =
				what === "buzzer-beater"
					? ""
					: sh.buzzer
						? " at buzzer"
						: sh.seconds !== undefined && sh.seconds <= 10
							? ` with ${plural(sh.seconds, "second")} left`
							: "";
			// A teammate's 45 is not left out for the shot: "Ellis scores 45,
			// Dunn's layup lifts Nuggets past Bulls".
			if (
				star &&
				star.name !== sh.name &&
				(star.pts >= 40 ||
					doubleCategories(star).length >= 3 ||
					(star.pts >= 20 && star.reb >= 20))
			) {
				const big =
					doubleCategories(star).length >= 3
						? `${nm(star.name)} has ${doubleWord(doubleCategories(star).length)}`
						: `${nm(star.name)} scores ${star.pts}`;
				const both = `${big}, ${poss(nm(sh.name))} ${what} lifts ${Wt} past ${Lt}`;
				if (both.length <= HEAD_MAX) {
					options.push(both);
					break;
				}
			}
			options.push(
				`${poss(nm(sh.name))} ${what}${when} lifts ${Wt} past ${Lt} ${sc}`,
				`${poss(nm(sh.name))} ${what}${when} gives ${Wt} ${headWin(s)} over ${Lt}`,
			);
			// Shorter, for when the clock does not fit: "Ingram's late 3-point
			// play lifts Cavaliers past Pelicans 103-100".
			if (when && !sh.buzzer) {
				const short = `${poss(nm(sh.name))} late ${what} lifts ${Wt} past ${Lt} ${sc}`;
				if (options.every((o) => o.length > HEAD_MAX)) {
					options.push(short);
				}
			}
			break;
		}
		case "lateGoAhead": {
			const who = s.late!.name;
			const what = headShot(s.late!.what);
			options.push(
				`${poss(nm(who))} late ${what} lifts ${Wt} past ${Lt} ${sc}`,
				`${Wt} edge ${Lt} ${sc} on ${poss(nm(who))} late ${what}`,
			);
			break;
		}
		case "monsterLoss": {
			const p = s.loserStar!;
			options.push(
				`${Wt} beat ${Lt} ${sc} despite ${poss(nm(p.name))} ${p.pts}`,
				`${nm(p.name)} scores ${p.pts}, but ${Wt} beat ${Lt} ${sc}`,
			);
			break;
		}
		case "big": {
			const p = star!;
			const dd = doubleCategories(p).length;
			if (dd >= 3) {
				options.push(
					`${nm(p.name)} has ${doubleWord(dd)} as ${Wt} beat ${Lt} ${sc}`,
					`${poss(nm(p.name))} ${doubleWord(dd)} leads ${Wt} past ${Lt} ${sc}`,
				);
			} else if (p.reb >= 20) {
				options.push(
					`${nm(p.name)} has ${p.pts} points, ${p.reb} rebounds as ${Wt} beat ${Lt}`,
				);
			} else {
				options.push(
					`${nm(p.name)} scores ${p.pts}, ${Wt} beat ${Lt} ${sc}`,
					`${poss(nm(p.name))} ${p.pts} points lead ${Wt} past ${Lt} ${sc}`,
				);
			}
			break;
		}
		case "comeback":
			options.push(
				`${Wt} rally from ${s.comeback} down to beat ${Lt} ${sc}`,
				`${Wt} overcome ${s.comeback}-point deficit, beat ${Lt} ${sc}`,
			);
			break;
		case "upset":
			if (s.lSkid !== undefined && s.lSkid >= 6) {
				options.push(
					`${s.ln} lose ${headOrd(s.lSkid)} straight, fall to ${s.wn} ${sc}`,
				);
			}
			if (s.shape.margin >= 20) {
				options.push(`${Wt} rout ${Lt} ${sc}`, `${Wt} blow out ${Lt} ${sc}`);
				break;
			}
			options.push(
				`${Wt} upset ${Lt} ${sc}`,
				`${Wt} knock off ${Lt} ${sc}`,
				...(s.dog! >= 9 ? [`${Wt} stun ${Lt} ${sc}`] : []),
				...(star && star.pts >= 25
					? [`${nm(star.name)} scores ${star.pts}, ${Wt} upset ${Lt} ${sc}`]
					: star && star.pts >= 18
						? [`${nm(star.name)} leads ${Wt} to ${sc} upset of ${Lt}`]
						: []),
			);
			break;
		case "snap":
			options.push(
				`${Wt} beat ${Lt} ${sc}, snap ${s.skid}-game skid`,
				`${Wt} snap ${s.skid}-game losing streak with ${sc} win over ${Lt}`,
			);
			break;
		case "ended":
			options.push(
				`${Wt} snap ${poss(L)} ${s.ended}-game win streak`,
				`${Wt} end ${poss(L)} ${s.ended}-game win streak, ${sc}`,
			);
			break;
		case "ot":
			options.push(
				`${Wt} outlast ${Lt} ${s.W.pts}-${s.L.pts} in ${s.shape.ot > 1 ? `${s.shape.ot}OT` : "OT"}`,
			);
			break;
		case "streak":
			options.push(
				`${Wt} win ${headOrd(s.streak!)} straight, beat ${Lt} ${sc}`,
				`${Wt} beat ${Lt} ${sc} for ${headOrd(s.streak!)} straight win`,
			);
			break;
		case "close":
			if (outplayedHead(s, Wt, Lt, sc, nm, options)) {
				break;
			}
			options.push(
				heldOn(s.game)
					? `${Wt} hold off ${Lt} ${sc}`
					: `${Wt} pull out ${headWin(s)} over ${Lt}`,
				...(escaped(s) ? [`${Wt} escape ${Lt} ${sc}`] : []),
				...(star && star.pts >= 20
					? [
							`${nm(star.name)} leads ${Wt} past ${Lt} ${sc}`,
							`${nm(star.name)} helps ${Wt} ${heldOn(s.game) ? "hold off" : "edge"} ${Lt} ${sc}`,
						]
					: []),
			);
			break;
		case "rout":
			options.push(
				`${Wt} rout ${Lt} ${sc}`,
				`${Wt} blow out ${Lt} ${sc}`,
				...(star && star.pts >= 25
					? [`${nm(star.name)} scores ${star.pts} as ${Wt} rout ${Lt} ${sc}`]
					: []),
			);
			break;
		case "tie":
			options.push(
				s.game.shootout
					? `${W} beat ${L} in shootout after ${s.W.pts}-${s.L.pts} tie`
					: `${W} and ${L} play to ${s.W.pts}-${s.L.pts} tie`,
			);
			break;
		default:
			if (outplayedHead(s, Wt, Lt, sc, nm, options)) {
				break;
			}
			options.push(...plainHeads(s, Wt, Lt, sc, nm));
	}
	const fits = options.filter((o) => o.length <= HEAD_MAX);
	// Rotated by shape across the page, whatever the names and numbers: a
	// page of "X leads Y past Z" is one headline eight times.
	const names = [...s.W.players, ...s.L.players]
		.map((p) => p.name)
		.sort((a, b) => b.length - a.length);
	const shapeOf = (o: string) => {
		let k = o;
		for (const n of names) {
			k = k.replaceAll(n, "~");
		}
		return k
			.replaceAll(Wt, "W")
			.replaceAll(Lt, "L")
			.replaceAll(W, "W")
			.replaceAll(L, "L")
			.replaceAll(/\d+/g, "#");
	};
	const chosen = pickKeyed(
		rng,
		(fits.length > 0 ? fits : options).map((o) => ({
			key: shapeOf(o),
			value: o,
		})),
		"head",
	);
	const themed = nightHead(s, themes);
	if (themed) {
		return themed;
	}
	// Too long with a tag or the seeds: the plain names; still too long, the
	// shortest it could have been.
	const plain = (o: string) =>
		o
			.replace(`${wTag ?? "~"} `, "")
			.replace(`${lTag ?? "~"} `, "")
			.replace(seeds?.w ?? "~", s.wn)
			.replace(seeds?.l ?? "~", s.ln);
	let out = chosen.length > HEAD_MAX ? plain(chosen) : chosen;
	if (out.length > HEAD_MAX) {
		out = options.map(plain).sort((a, b) => a.length - b.length)[0] ?? out;
	}
	return cap(out);
};

// A headline for the night, when the night is the story: three series
// ended, two Game 7s forced, a slate of upsets. "Bulls, Suns and Hawks all
// finish sweeps". "Kings win at buzzer as 4 favorites fall".
const nightHead = (s: Story, themes: Theme[]): string | undefined => {
	const names = (ss: Story[]) =>
		ss.length === 2
			? `${ss[0]!.wn}, ${ss[1]!.wn}`
			: `${ss
					.slice(0, -1)
					.map((x) => x.wn)
					.join(", ")} and ${ss.at(-1)!.wn}`;
	const clinches = themes.find((t) => t.kind === "clinches");
	if (clinches && clinches.games.length >= 3 && clinches.games.includes(s)) {
		const g = clinches.games;
		const sweeps = g.every((x) => x.series && x.series.lAfter === 0);
		const sevens = g.every(
			(x) => x.series && x.series.wAfter + x.series.lAfter === x.series.bestOf,
		);
		const head = `${names(g)} all ${sweeps ? "finish sweeps" : sevens ? `win Game ${g[0]!.series!.bestOf}s` : "advance"}`;
		if (head.length <= HEAD_MAX) {
			return head;
		}
	}
	const forced = themes.find(
		(t) => t.kind === "state" && t.games[0]!.series!.state === "force",
	);
	if (forced && forced.games.includes(s) && s.kind === "series") {
		const head = `${names(forced.games)} force Game ${s.series!.bestOf}s`;
		if (head.length <= HEAD_MAX) {
			return head;
		}
	}
	const upsets = themes.find((t) => t.kind === "upsets");
	if (
		upsets &&
		upsets.games.includes(s) &&
		(s.kind === "upset" ||
			upsets.games.length >= 5 ||
			upsets.games.length === upsets.of)
	) {
		const n = upsets.games.length;
		const short =
			s.kind === "shot" && s.shot?.buzzer
				? `${s.wn} win at buzzer`
				: s.dog !== undefined
					? `${s.wn} stun ${s.ln}`
					: `${s.wn} beat ${s.ln}`;
		const head = `${short} as ${n} favorites fall`;
		if (head.length <= HEAD_MAX) {
			return head;
		}
	}
	return undefined;
};

// A game's own headline, for its recap: whole names (`text`), and as it is
// printed (`display`).
export const gameHeadline = (
	game: RecapGame,
	rng: Rng,
): { text: string; display: string; story: Story } => {
	const story = storyOf(game);
	const text = headlineOf(story, rng);
	return { text, display: surnamesIn(text, game), story };
};

// --- What kind of night it was ----------------------------------------------

type Theme = {
	kind:
		| "finishes"
		| "upsets"
		| "ot"
		| "forty"
		| "state"
		| "clinches"
		| "comebacks";
	// The games in it.
	games: Story[];
	// Out of how many (for upsets: the games with a favorite).
	of?: number;
};

// Where a series stands, for grouping: "lead 2-0".
const stateKey = (s: Story): string | undefined => {
	const sr = s.series;
	if (!sr || s.kind !== "series") {
		return undefined;
	}
	return sr.state === "lead" || sr.state === "brink"
		? `lead ${sr.wAfter}-${sr.lAfter}`
		: sr.state === "even"
			? `even ${sr.wAfter}-${sr.lAfter}`
			: sr.state === "cut"
				? `cut ${sr.wAfter}-${sr.lAfter}`
				: sr.state;
};

const themesOf = (stories: Story[]): Theme[] => {
	const out: Theme[] = [];
	const finishes = stories.filter(
		(s) =>
			s.shot &&
			(s.shot.buzzer || (s.shot.seconds !== undefined && s.shot.seconds <= 10)),
	);
	if (finishes.length >= 2) {
		out.push({ kind: "finishes", games: finishes });
	}
	const favored = stories.filter(
		(s) => s.game.spread && s.game.spread.points >= 1 && !s.game.playoffs,
	);
	const upsets = favored.filter(
		(s) => s.game.spread!.favTid === s.L.tid && s.game.spread!.points >= 3,
	);
	// Favorites lose a third of the time; a night of upsets is half of them
	// or more, or every one on a small slate.
	if (
		(upsets.length >= 4 && upsets.length / favored.length >= 0.5) ||
		(upsets.length >= 3 && upsets.length === favored.length)
	) {
		out.push({ kind: "upsets", games: upsets, of: favored.length });
	}
	const ot = stories.filter((s) => s.shape.ot > 0);
	if (ot.length >= 2) {
		out.push({ kind: "ot", games: ot });
	}
	const comebacks = stories.filter((s) => s.comeback >= 15);
	if (comebacks.length >= 2) {
		out.push({ kind: "comebacks", games: comebacks });
	}
	const forty = stories.filter((s) =>
		s.game.teams.some((t) => t.players.some((p) => p.pts >= 40)),
	);
	if (forty.length >= 2) {
		out.push({ kind: "forty", games: forty });
	}
	const clinches = stories.filter(
		(s) => s.kind === "clinch" || s.kind === "title",
	);
	if (clinches.length >= 2) {
		out.push({ kind: "clinches", games: clinches });
	}
	const byState = new Map<string, Story[]>();
	for (const s of stories) {
		const k = stateKey(s);
		if (k && k !== "opener") {
			byState.set(k, [...(byState.get(k) ?? []), s]);
		}
	}
	for (const games of byState.values()) {
		if (games.length >= 2) {
			out.push({ kind: "state", games });
		}
	}
	return out;
};

// A group of series with one result, said once: "Five teams took 2-0 series
// leads", "Two series are going to a Game 7".
const stateLine = (games: Story[], lede: Story): string => {
	const sr = games[0]!.series!;
	const n = games.length;
	const inIt = games.includes(lede);
	const N = cap(spelledNum(n));
	switch (sr.state) {
		case "force":
			return inIt
				? `It is one of ${numWord(n)} series going to a Game ${sr.bestOf}.`
				: `${N} series are going to a Game ${sr.bestOf}.`;
		case "lead":
		case "brink":
			return `${N} teams took ${sr.wAfter}-${sr.lAfter} series leads on the night.`;
		case "even":
			return `${N} series are now tied ${sr.wAfter}-${sr.lAfter}.`;
		case "alive":
			return `${N} teams facing elimination stayed alive.`;
		case "cut":
			return `${N} teams cut their series deficits to ${sr.lAfter}-${sr.wAfter}.`;
		default:
			return "";
	}
};

// The line that turns from the lede game to the rest of the night: "It was
// one of three games won on a shot in the final seconds."
const themeLine = (theme: Theme, lede: Story): string => {
	const n = theme.games.length;
	const inIt = theme.games.includes(lede);
	switch (theme.kind) {
		case "finishes":
			return inIt
				? `It was one of ${numWord(n)} games won on a shot in the final seconds.`
				: `${cap(spelledNum(n))} games were won on a shot in the final seconds.`;
		case "upsets":
			return `It was a rough night for favorites: ${numWord(n)} of the ${numWord(theme.of!)} lost.`;
		case "ot":
			return inIt
				? `It was one of ${numWord(n)} games to go to overtime.`
				: `${cap(spelledNum(n))} games went to overtime.`;
		case "comebacks":
			return inIt
				? `It was one of ${numWord(n)} comebacks from 15 or more down on the night.`
				: `${cap(spelledNum(n))} teams came back from 15 or more down.`;
		case "forty": {
			const scorers = theme.games
				.flatMap((s) => s.game.teams.flatMap((t) => t.players))
				.filter((p) => p.pts >= 40);
			return scorers.length === 2
				? `${scorers[0]!.name} and ${scorers[1]!.name} both scored 40 or more.`
				: `${cap(spelledNum(scorers.length))} players scored 40 or more.`;
		}
		case "clinches":
			return inIt
				? `It was one of ${numWord(n)} series to end on the night.`
				: `${cap(spelledNum(n))} series ended on the night.`;
		case "state":
			return stateLine(theme.games, lede);
	}
};

// Which kind of night says the most, when it was more than one.
const themeRank = (t: Theme): number => {
	switch (t.kind) {
		case "clinches":
			return 10;
		case "state":
			return t.games[0]!.series!.state === "force" ? 9 : 4;
		case "finishes":
			return 8;
		case "upsets":
			return 7;
		case "comebacks":
			return 6;
		case "ot":
			return 5;
		case "forty":
			return 3;
	}
};

// --- Series words ------------------------------------------------------------

// What a series result did, as the end of a clause after the team: "went up
// 2-0", "evened the series at 1-1", "forced a Game 7".
const statePast = (s: Story, rng: Rng): string => {
	const sr = s.series!;
	const { wAfter: a, lAfter: b } = sr;
	switch (sr.state) {
		case "opener":
			return pick(
				rng,
				[
					"won the series opener",
					"struck first in the series",
					"won the opener",
				],
				"day:st:opener",
			);
		case "lead":
			return pick(
				rng,
				[
					`went up ${a}-${b}`,
					`took ${aFigure(`${a}-${b}`)} series lead`,
					`won Game ${sr.gameNo} to go up ${a}-${b}`,
				],
				"day:st:lead",
			);
		case "brink":
			return pick(
				rng,
				[
					"moved within a win of advancing",
					`went up ${a}-${b}`,
					`took ${aFigure(`${a}-${b}`)} series lead`,
				],
				"day:st:brink",
			);
		case "even":
			return pick(
				rng,
				[
					`evened the series at ${a}-${b}`,
					`tied the series ${a}-${b}`,
					`won Game ${sr.gameNo} to even the series at ${a}-${b}`,
				],
				"day:st:even",
			);
		case "force":
			return `forced a Game ${sr.bestOf}`;
		case "alive":
			return pick(
				rng,
				[
					"stayed alive",
					"avoided elimination",
					`won Game ${sr.gameNo} to stay alive`,
				],
				"day:st:alive",
			);
		case "cut":
			return `cut the series deficit to ${b}-${a}`;
		case "over":
			return "won the series";
	}
};

// --- The lede ----------------------------------------------------------------

// The top story as a wire lede: the man, what he did, the result and the
// score, in one sentence.
const ledeSentence = (s: Story, rng: Rng, night = ""): string => {
	const { w, l, score } = s;
	const star = s.star;
	const by = star
		? ` behind ${poss(star.name)} ${lineWithHigh(star, true)}`
		: "";
	const verb = beatVerb(s, rng);
	const ser = s.series;
	switch (s.kind) {
		case "title":
		case "clinch": {
			const games = ser ? ser.wAfter + ser.lAfter : 0;
			const end =
				s.kind === "title"
					? "to win the championship"
					: `to reach ${s.post!.clinch!.nextRound}`;
			const how =
				ser && ser.lAfter === 0
					? `finished ${aNumWord(games)}-game sweep of ${l} with ${aWin(s)} ${end}`
					: games === ser?.bestOf
						? `beat ${l} ${score} in Game ${games} ${end}`
						: `beat ${l} ${score} ${end}, closing out the series in ${numWord(games)} games`;
			return star
				? `${star.name} ${did(star, rng)}, and ${w} ${how}.`
				: `${cap(w)} ${how}.`;
		}
		case "playIn":
			return star
				? `${star.name} ${did(star, rng)} as ${w} beat ${l} ${score}${s.post?.playInLede ?? ""}.`
				: `${cap(w)} beat ${l} ${score}${s.post?.playInLede ?? ""}.`;
		case "series": {
			const sr = ser!;
			// A round the night has named already is not named again, nor a
			// night of openers' "Game 1": "... as the Pistons beat the Pelicans
			// 104-96."
			if (night.includes(sr.round)) {
				const tail =
					sr.state === "opener"
						? ""
						: sr.state === "force"
							? ` to force a Game ${sr.bestOf}`
							: sr.state === "even"
								? ` to even the series ${sr.wAfter}-${sr.lAfter}`
								: sr.state === "alive"
									? " to stay alive"
									: sr.state === "cut"
										? ` to cut the deficit to ${sr.lAfter}-${sr.wAfter}`
										: ` for ${aFigure(`${sr.wAfter}-${sr.lAfter}`)} series lead`;
				return star
					? `${star.name} ${did(star, rng)} as ${w} beat ${l} ${score}${tail}.`
					: `${cap(w)} beat ${l} ${score}${tail}.`;
			}
			const tail =
				sr.state === "opener"
					? ""
					: sr.state === "force"
						? ` to force a Game ${sr.bestOf}`
						: sr.state === "even"
							? ` to even the series ${sr.wAfter}-${sr.lAfter}`
							: sr.state === "alive"
								? " to stay alive"
								: sr.state === "cut"
									? ` to cut the deficit to ${sr.lAfter}-${sr.wAfter}`
									: ` for ${aFigure(`${sr.wAfter}-${sr.lAfter}`)} series lead`;
			return star
				? `${star.name} ${did(star, rng)} as ${w} beat ${l} ${score} in Game ${sr.gameNo} of ${sr.round}${tail}.`
				: `${cap(w)} beat ${l} ${score} in Game ${sr.gameNo} of ${sr.round}${tail}.`;
		}
		case "shot": {
			const sh = s.shot!;
			return `${cap(w)} beat ${l} ${score} on ${poss(sh.name)} ${shotWhat(sh).replace(/^an? |^the /, "")}.`;
		}
		case "lateGoAhead":
			return `${cap(w)} ${verb} ${l} ${score}${s.late!.text}.`;
		case "monsterLoss": {
			const p = s.loserStar!;
			return `${p.name} ${did(p, rng)}, but ${w} beat ${l} ${score}${by}.`;
		}
		case "big":
			return pick(
				rng,
				[
					`${star!.name} ${did(star!, rng)} as ${w} ${verb} ${l} ${score}.`,
					`${cap(w)} ${verb} ${l} ${score} behind ${poss(star!.name)} ${lineWithHigh(star!, true)}.`,
				],
				"day:lede:big",
			);
		case "comeback":
			return `${cap(w)} came back from ${s.comeback} points down to beat ${l} ${score}${by}.`;
		case "upset":
			return pick(
				rng,
				[
					`${cap(w)} upset ${l} ${score} as ${s.dog}-point underdogs${by}.`,
					...(star
						? [
								`${star.name} ${did(star, rng)}, and ${w}, ${s.dog}-point underdogs, beat ${l} ${score}.`,
							]
						: []),
				],
				"day:lede:upset",
			);
		case "snap":
			return `${cap(w)} snapped ${aNumWord(s.skid!)}-game losing streak with ${aWin(s)} over ${l}${by}.`;
		case "ended":
			return `${cap(w)} ended ${poss(l)} ${numWord(s.ended!)}-game winning streak with ${aWin(s)}${by}.`;
		case "ot":
			return `${cap(w)} outlasted ${l} ${s.W.pts}-${s.L.pts} in ${otPhrase(s.shape.ot)}${by}.`;
		case "streak":
			return s.unbeaten !== undefined
				? `${cap(w)} stayed unbeaten at ${s.unbeaten}-0, beating ${l} ${score}${by}.`
				: `${cap(w)} won their ${ordinal(s.streak!)} straight, beating ${l} ${score}${by}.`;
		case "tie":
			return s.game.shootout
				? `${cap(w)} and ${l} tied ${s.W.pts}-${s.L.pts}${s.shape.ot > 0 ? ` after ${otPhrase(s.shape.ot)}` : ""}, and ${w} won the shootout ${s.game.shootout.won}-${s.game.shootout.lost}.`
				: `${cap(w)} and ${l} played to ${aFigure(`${s.W.pts}-${s.L.pts}`)} tie.`;
		default: {
			const how = howWon(s, rng);
			return star
				? how
					? /^(?:held on|pulled it out|closed on|scored (?:the|\d+ of the) last)/.test(
							how,
						)
						? `${star.name} ${did(star, rng)} as ${w} ${how} to beat ${l} ${score}.`
						: `${star.name} ${did(star, rng)} as ${w} ${how} on the way to ${aWin(s)} over ${l}.`
					: `${star.name} ${did(star, rng)} as ${w} ${verb} ${l} ${score}.`
				: `${cap(w)} ${verb} ${l} ${score}.`;
		}
	}
};

// The detail that makes the lede game his night, or someone else's: the
// shooter's line, the shot that won it when the sentence before did not say,
// how the big scorer got there, who else carried it. "He" for the man the
// sentence before was about, when he is the only man it named.
const ledeDetail = (s: Story, written: string): string | undefined => {
	const star = s.star;
	const named = [...s.W.players, ...s.L.players].filter((p) =>
		written.includes(p.name),
	);
	const he = (p: RecapPlayer) =>
		named.length === 1 && named[0] === p ? "He" : p.name;
	const sh = s.shot;
	const hero = sh ? s.W.players.find((p) => p.name === sh.name) : undefined;
	// A game still in the balance late that the winners ran away with: where
	// it stood, and the run.
	const c = s.closer;
	if (c && c.from && !sh && !s.late && !/ run\b|of the last/.test(written)) {
		const clock = clockLeft(c.from.clock);
		const left =
			clock && c.period > s.shape.regPeriods
				? `${clock} ${c.period - s.shape.regPeriods === 1 ? "left in overtime" : `left in the ${ordinal(c.period - s.shape.regPeriods)} overtime`}`
				: clock && `${clock} left`;
		const run = c.ended
			? `closed on ${aFigure(`${c.pts}-${c.opp}`)} run`
			: `pulled away with ${aFigure(`${c.pts}-${c.opp}`)} run`;
		const { w, l } = c.from;
		if (left) {
			return w === l
				? `It was tied at ${w} with ${left} before ${s.w} ${run}.`
				: w > l
					? `${cap(s.l)} were within ${w - l === 1 ? "a point" : numWord(w - l)} with ${left} before ${s.w} ${run}.`
					: `${cap(s.l)} led ${l}-${w} with ${left} before ${s.w} ${run}.`;
		}
	}
	// A playoff game won at the end: the lede gave the series, this gives
	// the shot.
	if (sh && hero && !written.includes(sh.name)) {
		const what = shotWhat(sh);
		return hero.pts >= 12 && hero !== star
			? `${sh.name} won it with ${what} and finished with ${lineOf(hero)}.`
			: `${sh.name} won it with ${what}.`;
	}
	// He was named for his line, and the shot that won it was not said.
	if (sh && hero && !/seconds? left|buzzer/.test(written)) {
		return `${he(hero)} won it with ${shotWhat(sh)}.`;
	}
	if (sh && hero && hero.pts >= 12) {
		const other =
			star && star !== hero && star.pts >= hero.pts + 5 ? star : undefined;
		if (other) {
			return `${he(hero)} finished with ${lineOf(hero)}, and ${other.name} led ${s.w} with ${lineOf(other)}.`;
		}
		const second = secondMan(s, `${written} ${hero.name}`);
		return second
			? `${he(hero)} finished with ${lineOf(hero)}. ${second}`
			: `${he(hero)} finished with ${lineOf(hero)}.`;
	}
	// A late go-ahead basket the sentence before did not tell (a playoff
	// lede tells the series instead).
	if (s.late && !/go-ahead/.test(written)) {
		const late = s.late;
		return `${late.name} ${shotVerb(late.what)} the go-ahead ${late.what} with ${late.left} left.`;
	}
	if (s.late) {
		const lateHero = s.W.players.find((p) => p.name === s.late!.name);
		if (star && !written.includes(star.name)) {
			return `${star.name} led ${s.w} with ${lineOf(star)}.`;
		}
		if (lateHero && lateHero.pts >= 12) {
			return `${he(lateHero)} finished with ${lineOf(lateHero)}.`;
		}
	}
	if (star && !written.includes(star.name)) {
		return `${star.name} led ${s.w} with ${lineOf(star)}.`;
	}
	if (star && star.pts >= 30 && star.fga >= 12) {
		const threes =
			star.tp >= 5 ? `, making ${star.tp} of ${star.tpa} three-pointers` : "";
		return `${he(star)} shot ${star.fg}-of-${star.fga} from the field${threes}.`;
	}
	return secondMan(s, written);
};

// The man next to the star, when he had a night too: "Jaden Green added 30
// points and 12 assists."
const secondMan = (s: Story, written: string): string | undefined => {
	const second = s.W.players
		.filter((p) => p !== s.star && !written.includes(p.name))
		.sort((a, b) => b.pts - a.pts)[0];
	if (
		second &&
		(second.pts >= 22 ||
			(second.pts >= 15 && doubleCategories(second).length >= 2))
	) {
		return `${second.name} added ${lineOf(second)}.`;
	}
	return undefined;
};

// The next man in, for a lede with nothing else to say: the winners' second
// scorer, else the losers' best.
const depthLine = (s: Story, written: string): string | undefined => {
	const fresh = (p: RecapPlayer) => !written.includes(p.name);
	const second = s.W.players
		.filter((p) => p !== s.star && fresh(p))
		.sort((a, b) => b.pts - a.pts)[0];
	if (second && second.pts >= 15) {
		return `${second.name} added ${lineOf(second)}.`;
	}
	const top = s.L.players.filter(fresh).sort((a, b) => b.pts - a.pts)[0];
	if (top && top.pts >= 18) {
		return `${top.name} had ${lineOf(top)} for ${s.l}.`;
	}
	return undefined;
};

// --- An item -----------------------------------------------------------------

// The shapes an item can take, each keyed so a night never uses one twice
// running and rotates through them.
type Shaped = { key: string; text: string };

// What a game was, without its score (the item's lead-in has it).
const itemShapes = (s: Story, rng: Rng): Shaped[] => {
	const { w, l } = s;
	const star = s.star;
	const W = cap(w);
	const sx = (key: string, text: string): Shaped => ({ key, text });
	switch (s.kind) {
		case "title":
		case "clinch": {
			const sr = s.series;
			const games = sr ? sr.wAfter + sr.lAfter : 0;
			const how =
				sr && sr.lAfter === 0
					? `swept ${l}`
					: games === sr?.bestOf
						? pick(
								rng,
								[
									`won Game ${games} to eliminate ${l}`,
									`ousted ${l} in Game ${games}`,
									`took Game ${games} and the series`,
								],
								"day:item:decider",
							)
						: pick(
								rng,
								[
									`closed out ${l} in ${numWord(games)}`,
									`finished off ${l} in ${numWord(games)} games`,
								],
								"day:item:close",
							);
			return star
				? [
						sx("as", `${star.name} ${did(star, rng)} as ${w} ${how}.`),
						sx(
							"behind",
							`${W} ${how} behind ${poss(star.name)} ${lineOf(star)}.`,
						),
					]
				: [sx("bare", `${W} ${how}.`)];
		}
		case "playIn":
			return [
				sx(
					"as",
					star
						? `${star.name} ${did(star, rng)} as ${w} won${s.post?.playInLede ?? ""}.`
						: `${W} won${s.post?.playInLede ?? ""}.`,
				),
			];
		case "series": {
			const what = statePast(s, rng);
			return star
				? [
						sx("as", `${star.name} ${did(star, rng)} as ${w} ${what}.`),
						sx(
							"behind",
							`${W} ${what} behind ${poss(star.name)} ${lineOf(star)}.`,
						),
						sx("and", `${star.name} ${did(star, rng)}, and ${w} ${what}.`),
					]
				: [sx("bare", `${W} ${what}.`)];
		}
		case "shot": {
			const sh = s.shot!;
			const what = shotWhat(sh);
			const hero = s.W.players.find((p) => p.name === sh.name);
			const tail =
				star && star !== hero && star.pts >= 20
					? ` ${star.name} led the way with ${lineOf(star)}.`
					: hero && hero.pts >= 15
						? ` ${sh.name} finished with ${lineOf(hero)}.`
						: "";
			return [
				sx(
					"shot",
					`${sh.name} ${shotVerb(what)} ${what} to win it for ${w}.${tail}`,
				),
			];
		}
		case "lateGoAhead": {
			const late = s.late!;
			const hit = `${shotVerb(late.what)} the go-ahead ${late.what} with ${late.left} left`;
			const hero = s.W.players.find((p) => p.name === late.name);
			return [
				sx(
					"late",
					star && star.name === late.name
						? `${late.name} ${hit} and finished with ${lineOf(star)} for ${w}.`
						: star
							? `${late.name} ${hit} for ${w}, and ${star.name} led the way with ${lineOf(star)}.`
							: hero && hero.pts >= 12
								? `${late.name} ${hit} and finished with ${lineOf(hero)} for ${w}.`
								: `${late.name} ${hit} for ${w}.`,
				),
			];
		}
		case "monsterLoss": {
			const p = s.loserStar!;
			return [
				sx(
					"monster",
					`${p.name} ${did(p, rng)} for ${l}, but ${w} won anyway${star ? ` behind ${poss(star.name)} ${lineOf(star)}` : ""}.`,
				),
			];
		}
		case "big":
			return [
				sx("for", `${star!.name} ${did(star!, rng)} for ${w}.`),
				sx("lead", leadLine(star!, w, rng)),
			];
		case "comeback":
			return [
				sx(
					"cameBack",
					`${W} came back from ${s.comeback} points down${star ? `, led by ${poss(star.name)} ${lineOf(star)}` : ""}.`,
				),
				...(star
					? [
							sx(
								"as",
								`${star.name} ${did(star, rng)} as ${w} rallied from ${s.comeback} down.`,
							),
						]
					: []),
			];
		case "upset":
			return [
				sx(
					"dogs",
					`${W} won as ${s.dog}-point underdogs${star ? ` behind ${poss(star.name)} ${lineOf(star)}` : ""}.`,
				),
				...(star
					? [
							sx(
								"and",
								`${star.name} ${did(star, rng)}, and ${w} pulled the upset as ${s.dog}-point underdogs.`,
							),
						]
					: []),
			];
		case "snap":
			return [
				star
					? sx(
							"as",
							`${star.name} ${did(star, rng)} as ${w} snapped ${aNumWord(s.skid!)}-game losing streak.`,
						)
					: sx("bare", `${W} snapped ${aNumWord(s.skid!)}-game losing streak.`),
			];
		case "ended":
			return [
				star
					? sx(
							"as",
							`${star.name} ${did(star, rng)} as ${w} ended ${poss(l)} ${numWord(s.ended!)}-game winning streak.`,
						)
					: sx(
							"bare",
							`${W} ended ${poss(l)} ${numWord(s.ended!)}-game winning streak.`,
						),
			];
		case "ot":
			return [
				sx(
					"ot",
					`${W} won in ${otPhrase(s.shape.ot)}${star ? `, with ${star.name} finishing with ${lineOf(star)}` : ""}.`,
				),
			];
		case "streak": {
			const what =
				s.unbeaten !== undefined
					? `stayed unbeaten at ${s.unbeaten}-0`
					: pick(
							rng,
							[
								`won their ${ordinal(s.streak!)} straight`,
								`extended their winning streak to ${numWord(s.streak!)}`,
								`won for the ${ordinal(s.streak!)} straight time`,
							],
							"day:item:streak",
						);
			return star
				? [
						sx("as", `${star.name} ${did(star, rng)} as ${w} ${what}.`),
						sx(
							"behind",
							`${W} ${what} behind ${poss(star.name)} ${lineOf(star)}.`,
						),
					]
				: [sx("bare", `${W} ${what}.`)];
		}
		case "tie":
			return [
				sx(
					"tie",
					s.game.shootout
						? `${W} won a shootout ${s.game.shootout.won}-${s.game.shootout.lost} after the teams tied${star ? `; ${star.name} ${did(star, rng)}` : ""}.`
						: `${W} and ${l} played to a tie${star ? `; ${star.name} ${did(star, rng)}` : ""}.`,
				),
			];
		default: {
			if (!star) {
				return [sx("bare", `${W} won.`)];
			}
			const how = howWon(s, rng);
			const out: Shaped[] = [];
			if (how) {
				out.push(
					sx("as", `${star.name} ${did(star, rng)} as ${w} ${how}.`),
					sx(
						"behind",
						`${W} ${how} behind ${poss(star.name)} ${lineOf(star)}.`,
					),
				);
			}
			out.push(
				sx("for", `${star.name} ${did(star, rng)} for ${w}.`),
				sx("lead", leadLine(star, w, rng)),
				sx("got", `${W} got ${lineOf(star)} from ${star.name}.`),
			);
			return out;
		}
	}
};

// One more thing the game meant, as its own sentence. What each item could
// add, best first; the item takes the first one (two for the night's biggest
// stories) not already told.
const contextOf = (
	s: Story,
	told: string,
	rng: Rng,
	// A night of one or two games, with room for what the series needs.
	stakes = false,
	// What the night's recap has said before this game.
	night = "",
): string[] => {
	const out: string[] = [];
	const { w, l } = s;
	const W = cap(w);
	const L = cap(l);
	const sr = s.series;
	if (sr) {
		if (s.kind === "title") {
			if (!/champion/.test(told)) {
				out.push(`${W} are champions.`);
			}
		} else if (s.kind === "clinch") {
			const next = s.post!.clinch!.nextRound;
			const over = `${cap(poss(l))} season is over.`;
			// Once a night: on a night four series end, the fourth "advance
			// to the Conference Semifinals" says nothing.
			if (night.includes(next) || told.includes(next)) {
				if (!/season is over/.test(`${night} ${told}`)) {
					out.push(over);
				}
			} else {
				out.push(
					pick(
						rng,
						[`${W} move on to ${next}.`, `${W} advance to ${next}.`, over],
						"day:ctx:clinch",
					),
				);
			}
		} else if (sr.state === "brink") {
			const next = sr.gameNo + 1;
			const lp = s.loserStar ?? s.loserLead;
			if (lp && !told.includes(lp.name)) {
				// "Payton Harper scored 40 for the Nets, who face elimination
				// in Game 6."
				out.push(
					`${lp.name} ${did(lp, rng)} for ${l}, ${pick(
						rng,
						[
							`who face elimination in Game ${next}`,
							`who must win Game ${next} to stay alive`,
							"who are one loss from elimination",
						],
						"day:ctx:brinkWho",
					)}.`,
				);
			} else {
				out.push(
					pick(
						rng,
						[
							`${L} face elimination in Game ${next}.`,
							`${L} must win Game ${next} to stay alive.`,
							`${L} are one loss from elimination.`,
							`One more loss ends ${poss(l)} season.`,
						],
						"day:ctx:brink",
					),
				);
			}
		}
	}
	// What the series needs now, on a night with room to say it.
	if (sr && s.kind === "series" && stakes) {
		const finals = sr.round === "the Finals";
		const wNeed = sr.need - sr.wAfter;
		const lNeed = sr.need - sr.lAfter;
		const wins = (n: number) =>
			`${numWord(n)} more ${n === 1 ? "win" : "wins"}`;
		const to = finals ? "for the title" : "to advance";
		if (sr.state === "even") {
			out.push(`Both teams need ${wins(wNeed)} ${to}.`);
		} else if (sr.state === "opener" || sr.state === "lead") {
			out.push(
				`${W} are ${numWord(wNeed)} wins from ${finals ? "the title" : "the next round"}; ${l} need ${numWord(lNeed)}.`,
			);
		} else if (sr.state === "brink") {
			out.push(`${W} can finish it in Game ${sr.gameNo + 1}.`);
		} else if (sr.state === "alive" || sr.state === "cut") {
			out.push(`${L} still need ${wins(lNeed)} ${to}.`);
		}
	}
	const p = s.loserStar ?? s.loserLead;
	if (p && !told.includes(p.name) && !out.some((x) => x.includes(p.name))) {
		out.push(
			pick(
				rng,
				[
					`${p.name} ${did(p, rng)} for ${l}.`,
					`${p.name} ${did(p, rng)} in the loss for ${l}.`,
					...(/ got /.test(told)
						? []
						: [`${L} got ${lineOf(p)} from ${p.name}.`]),
				],
				"day:ctx:loserStar",
			),
		);
	}
	const changes = s.game.flow?.leadChanges ?? 0;
	if (changes >= 15) {
		out.push(`The teams traded the lead ${changes} times.`);
	}
	if (s.dog !== undefined && !/underdog|upset/.test(told)) {
		out.push(`${W} were ${s.dog}-point underdogs.`);
	}
	if (s.skid !== undefined && s.skid >= 5 && !/losing streak|skid/.test(told)) {
		out.push(
			`The win snapped ${poss(w)} ${numWord(s.skid)}-game losing streak.`,
		);
	}
	if (s.ended !== undefined && !/winning streak/.test(told)) {
		out.push(
			`The loss ended ${poss(l)} ${numWord(s.ended)}-game winning streak.`,
		);
	}
	if (s.firstIn) {
		out.push(`${W} moved into first place in the ${s.firstIn}.`);
	}
	if (s.unbeaten !== undefined && !/unbeaten/.test(told)) {
		out.push(`${W} are still unbeaten at ${s.unbeaten}-0.`);
	} else if (
		s.unbeaten === undefined &&
		s.streak !== undefined &&
		!/straight|in a row|winning streak/.test(told)
	) {
		out.push(
			pick(
				rng,
				[
					`${W} have won ${numWord(s.streak)} straight.`,
					`${W} have won ${numWord(s.streak)} in a row.`,
					`It was ${poss(w)} ${ordinal(s.streak)} straight win.`,
				],
				"day:ctx:streak",
			),
		);
	}
	if (s.winless !== undefined) {
		out.push(`${L} are still winless at 0-${s.winless}.`);
	} else if (s.lSkid !== undefined && !/lost|skid/.test(told)) {
		out.push(
			pick(
				rng,
				[
					`${L} have lost ${numWord(s.lSkid)} straight.`,
					`${L} have dropped ${numWord(s.lSkid)} in a row.`,
				],
				"day:ctx:lSkid",
			),
		);
	}
	const hurt = hurtSentence(s.hurt, told, s.game.playoffs);
	if (hurt) {
		out.push(hurt);
	}
	return out;
};

// Who got hurt, the way the wire tells it: one man in a sentence of his own,
// two of a club's men together. "The Rockets lost two players to injuries:
// Kenny Thomas (torn ACL, out for the season) and Yao Ming (sore foot, out
// about nine games)."
const hurtSentence = (
	hurt: Hurt[],
	told: string,
	playoffs: boolean,
): string | undefined => {
	const first = hurt[0];
	if (!first) {
		return undefined;
	}
	const mates = hurt.filter((h) => h.t === first.t).slice(0, 3);
	if (mates.length === 1) {
		const whose = told.includes(first.p.name)
			? ""
			: `${cap(poss(theNick(first.t)))} `;
		return `${whose}${first.p.name} ${hurtInGame(first.p.injury!.type)} and is expected to miss ${gamesOut(first.n, first.left, playoffs)}.`;
	}
	const each = mates.map((h) => {
		const out = gamesOut(h.n, h.left, playoffs)
			.replace(/^the rest of the (?:regular )?season$/, "for the season")
			.replace(/^the rest of the playoffs$/, "for the playoffs")
			.replace(/^the next game$/, "for the next game");
		return `${h.p.name} (${lowerInjury(h.p.injury!.type)}, out ${out})`;
	});
	return `${cap(theNick(first.t))} lost ${numWord(mates.length)} players to injuries: ${naturalList(each)}.`;
};

// A club named at the end of one sentence and at the start of the next is
// "they" the second time, when the first named no other club: "Corey Vance
// scored 49 points for the Spurs. They were 10-point underdogs."
const theyNext = (sentences: string[], clubs: string[]): string => {
	const parts = sentences.join(" ").split(/(?<=\.) (?=[A-Z])/);
	for (let i = 1; i < parts.length; i++) {
		const before = parts[i - 1]!;
		for (const club of clubs) {
			const nick = club.replace(/^the /, "");
			const others = clubs.filter((c) => c !== club);
			if (
				before.endsWith(` ${nick}.`) &&
				/\b[Tt]he $/.test(before.slice(0, -nick.length - 1)) &&
				parts[i]!.startsWith(`The ${nick} `) &&
				!others.some((c) => before.includes(c.replace(/^the /, "")))
			) {
				parts[i] = `They ${parts[i]!.slice(`The ${nick} `.length)}`;
			}
		}
	}
	return parts.join(" ");
};

// --- The night ---------------------------------------------------------------
//
// THE NIGHT AS ONE PIECE, EASY TO READ. The headline is the league's night,
// not one game's: its two or three biggest stories, each as a desk heads it
// ("Everett scores 48 as Thunder snap skid; Kings win 5th straight"), one
// game's alone only when nothing else that night was a story. Then each of
// those stories gets a short paragraph of its own, in the order the headline
// has them; every other game a line in a list under "Around the league"; and
// what the night left - the standings, who got hurt, what's next - one short
// paragraph each.

// How many games get a line of their own in the list; past this many the
// rest are told together at the end of it.
const MAX_ITEMS = 12;

// At most this many stories get a paragraph (and the headline).
const MAX_TOP = 3;

// A game that is a story of its own, beyond who won: not just a win, a close
// one or a blowout - unless it moved a team into first, kept one perfect or
// cost one a regular for a while.
const isStory = (s: Story): boolean =>
	!/^(?:plain|close|rout|tie)$/.test(s.kind) ||
	(s.star?.pts ?? 0) >= 35 ||
	s.firstIn !== undefined ||
	s.unbeaten !== undefined ||
	(s.winless ?? 0) >= 6 ||
	s.hurt.some((h) => h.n >= 15 && (h.p.seasonAvg?.min ?? 0) >= 28);

// "East" for "Eastern Conference".
const confShort = (conf: string): string =>
	/^(east|west)/i.test(conf)
		? `${conf[0]!.toUpperCase()}${conf.slice(1, 4).toLowerCase()}`
		: conf;

// A man's night that leads his game's clause: 35 points (30 in the
// playoffs), a triple-double, 20 and 20.
const starLed = (s: Story): boolean => {
	const star = s.star;
	return (
		!!star &&
		(star.pts >= (s.game.playoffs ? 30 : 35) ||
			doubleCategories(star).length >= 3 ||
			(star.pts >= 20 && star.reb >= 20))
	);
};

// A story as one clause of the night's headline, present tense, no score:
// "Thunder snap 6-game skid", "Everett scores 48 as Thunder end skid".
// Short, it leaves out who they beat where the clause says enough without:
// "Heat rally from 15 down". Whole names; `surnamesIn` makes them a
// headline's.
const headClause = (
	s: Story,
	{ withStar = false, short = false }: { withStar?: boolean; short?: boolean },
): string => {
	const W = s.wn;
	const L = s.ln;
	const star = s.star;
	const sr = s.series;
	const ot = s.shape.ot > 1 ? `${s.shape.ot}OT` : "OT";
	const bigLine = starLed(s);
	// What else the game was, for a clause led by its man.
	const otherwise = (): string =>
		s.skid !== undefined && s.skid >= 5
			? `${W} snap ${s.skid}-game skid`
			: s.ended !== undefined
				? `${W} end ${poss(L)} ${s.ended}-game win streak`
				: s.dog !== undefined
					? `${W} upset ${L}`
					: s.comeback >= 12
						? `${W} rally past ${L}`
						: s.shape.ot > 0
							? `${W} outlast ${L} in ${ot}`
							: s.unbeaten !== undefined
								? `${W} improve to ${s.unbeaten}-0`
								: s.streak !== undefined
									? `${W} win ${headOrd(s.streak)} straight`
									: s.firstIn
										? `${W} move into 1st in ${confShort(s.firstIn)}`
										: `${W} beat ${L}`;
	let result: string;
	if (endsLongRun(s) && !(bigLine && s.star!.pts >= 35)) {
		return `${W} end ${poss(L)} ${s.ended}-game ${short ? "" : "win "}streak`;
	}
	switch (s.kind) {
		case "title":
			return sr && sr.lAfter === 0
				? `${W} sweep ${L} for championship`
				: `${W} win championship`;
		case "clinch": {
			const games = sr ? sr.wAfter + sr.lAfter : 0;
			return sr && sr.lAfter === 0
				? `${W} sweep ${L}`
				: games === sr?.bestOf
					? `${W} win Game ${games} over ${L}`
					: `${W} eliminate ${L} in ${games}`;
		}
		case "playIn": {
			const pi = s.game.playIn;
			return pi?.kind === "seed9v10"
				? `${W} end ${poss(L)} season`
				: pi?.kind === "final"
					? `${W} take last playoff spot`
					: pi?.prizeSeed !== undefined
						? `${W} clinch No. ${pi.prizeSeed} seed`
						: `${W} win play-in game`;
		}
		case "series":
			if (s.shot) {
				const { wAfter: a, lAfter: b } = sr!;
				const tail =
					sr!.state === "force"
						? ` to force Game ${sr!.bestOf}`
						: sr!.state === "even"
							? " to even series"
							: sr!.state === "opener"
								? " in Game 1"
								: sr!.state === "alive"
									? " to stay alive"
									: sr!.state === "cut"
										? ` to cut series deficit to ${b}-${a}`
										: ` for ${a}-${b} series lead`;
				return `${poss(s.shot.name)} ${headShot(s.shot.shot)} lifts ${W}${short ? "" : tail}`;
			}
			result = `${W} ${headState(sr!)}`;
			break;
		case "shot": {
			const sh = s.shot!;
			const what = headShot(sh.shot);
			const shot =
				what === "buzzer-beater"
					? what
					: sh.buzzer
						? `${what} at buzzer`
						: `late ${what}`;
			return `${poss(sh.name)} ${shot} lifts ${W}${short ? "" : ` past ${L}`}`;
		}
		case "lateGoAhead":
			return `${W} edge ${L} late`;
		case "monsterLoss":
			return `${s.loserStar!.name} scores ${s.loserStar!.pts} in loss${short ? "" : ` to ${W}`}`;
		case "big": {
			const p = star!;
			const dd = doubleCategories(p).length;
			const his =
				dd >= 3
					? `${p.name} has ${doubleWord(dd)}`
					: p.reb >= 20
						? `${p.name} has ${p.pts} points, ${p.reb} rebounds`
						: `${p.name} scores ${p.pts}`;
			return short ? `${his} for ${W}` : `${his} as ${otherwise()}`;
		}
		case "comeback":
			result = short
				? `${W} rally from ${s.comeback} down`
				: `${W} rally from ${s.comeback} down to beat ${L}`;
			break;
		case "upset":
			result = `${W} ${s.dog! >= 9 ? "stun" : "upset"} ${L}`;
			break;
		case "snap":
			result = `${W} snap ${s.skid}-game skid`;
			break;
		case "ended":
			result = `${W} end ${poss(L)} ${s.ended}-game win streak`;
			break;
		case "ot":
			result = short ? `${W} win in ${ot}` : `${W} outlast ${L} in ${ot}`;
			break;
		case "streak":
			result =
				s.unbeaten !== undefined
					? `${W} improve to ${s.unbeaten}-0`
					: `${W} win ${headOrd(s.streak!)} straight`;
			break;
		case "tie":
			return s.game.shootout
				? `${W} beat ${L} in shootout`
				: `${W}, ${L} play to tie`;
		default:
			// A story for its man's night: his line leads it.
			if (
				star &&
				star.pts >= 35 &&
				!s.firstIn &&
				s.unbeaten === undefined &&
				(s.winless ?? 0) < 6
			) {
				const his = `${star.name} scores ${star.pts}`;
				return short ? `${his} for ${W}` : `${his} as ${otherwise()}`;
			}
			result = s.firstIn
				? `${W} move into 1st in ${confShort(s.firstIn)}`
				: s.unbeaten !== undefined
					? `${W} improve to ${s.unbeaten}-0`
					: (s.winless ?? 0) >= 6
						? `${L} fall to 0-${s.winless}`
						: s.kind === "rout"
							? `${W} rout ${L}`
							: s.kind === "close"
								? heldOn(s.game)
									? `${W} hold off ${L}`
									: `${W} edge ${L}`
								: `${W} beat ${L}`;
	}
	if ((withStar || s.game.playoffs) && star && bigLine) {
		const dd = doubleCategories(star).length;
		const his =
			dd >= 3
				? `${star.name} has ${doubleWord(dd)}`
				: star.reb >= 20
					? `${star.name} has ${star.pts} points, ${star.reb} rebounds`
					: `${star.name} scores ${star.pts}`;
		return `${his} as ${result}`;
	}
	return result;
};

// What a story is, for telling two of a kind as one: a shot that won it and
// a late go-ahead basket are both games won late.
// A long win streak ended outranks how the game went: "Kings end Pelicans'
// 16-game win streak" over "Kings rally from 15 down".
const endsLongRun = (s: Story) =>
	(s.ended ?? 0) >= LONG_RUN &&
	/^(?:comeback|lateGoAhead|upset|snap|ot|streak|close|rout|plain)$/.test(
		s.kind,
	);

const groupKey = (s: Story): string =>
	endsLongRun(s) && !starLed(s)
		? "ended"
		: starLed(s) && /^(?:series|streak|plain|close|rout)$/.test(s.kind)
			? `star ${s.game.gid}`
			: s.kind === "clinch" && s.series
				? s.series.wAfter + s.series.lAfter === s.series.bestOf
					? "clinch decider"
					: s.series.lAfter === 0
						? "clinch sweep"
						: "clinch close"
				: s.kind === "series"
					? // (A series game won at the end is its own clause.)
						s.shot
						? `shot ${s.game.gid}`
						: `series ${stateKey(s) ?? ""}`
					: s.kind === "shot" || s.kind === "lateGoAhead"
						? "late"
						: s.unbeaten !== undefined &&
							  /^(?:streak|plain|close|rout)$/.test(s.kind)
							? "unbeaten"
							: s.kind;

// Two or three stories of one kind are one clause: "Kings, Rockets extend
// win streaks", "Mathis, Murray hit game-winners".
const groupClause = (ss: Story[]): string | undefined => {
	const key = groupKey(ss[0]!);
	if (ss.length < 2 || !ss.every((s) => groupKey(s) === key)) {
		return undefined;
	}
	const list = (xs: string[]) =>
		xs.length === 2
			? `${xs[0]}, ${xs[1]}`
			: `${xs.slice(0, -1).join(", ")} and ${xs.at(-1)}`;
	const names = list(ss.map((s) => s.wn));
	if (key.startsWith("series ")) {
		const sr = ss[0]!.series!;
		const { wAfter: a, lAfter: b } = sr;
		const k = key.slice("series ".length);
		return k === "opener"
			? `${names} win series openers`
			: k === "force"
				? `${names} force Game ${sr.bestOf}s`
				: k === "alive"
					? `${names} stay alive`
					: k.startsWith("lead")
						? `${names} take ${a}-${b} series leads`
						: k.startsWith("even")
							? `${names} even series at ${a}-${b}`
							: k.startsWith("cut")
								? `${names} cut series deficits to ${b}-${a}`
								: undefined;
	}
	switch (key) {
		case "clinch decider":
			return `${names} win Game ${ss[0]!.series!.bestOf}s`;
		case "clinch sweep":
			return `${names} complete sweeps`;
		case "clinch close":
			return `${names} close out their series`;
		case "late":
			return ss.every((s) => s.shot)
				? `${list(ss.map((s) => surnamesIn(s.shot!.name, s.game)))} hit game-winners`
				: `${names} win it late`;
		case "upset":
			return `${names} pull upsets`;
		case "unbeaten":
			return `${names} stay unbeaten`;
		case "streak":
			return `${names} extend win streaks`;
		case "snap":
			return `${names} end losing streaks`;
		case "ended":
			return `${names} snap win streaks`;
		case "comeback":
			return `${names} win from double digits down`;
		case "ot":
			return `${names} win in overtime`;
		default:
			return undefined;
	}
};

// The top stories as the headline tells them: stories of a kind together,
// where the first of them stood.
const unitsOf = (top: Story[]): Story[][] => {
	const units: Story[][] = [];
	for (const s of top) {
		const unit = units.find((u) => groupClause([...u, s]) !== undefined);
		if (unit) {
			unit.push(s);
		} else {
			units.push([s]);
		}
	}
	return units;
};

// The night's headline: its top stories, biggest first, each as a clause.
// One story alone is headed as its game is, score and all.
const NIGHT_HEAD_MAX = 85;
const nightHeadline = (units: Story[][], themes: Theme[], rng: Rng): string => {
	const top = units.flat();
	const lede = top[0]!;
	const single = () => surnamesIn(headlineOf(lede, rng, themes), lede.game);
	if (top.length === 1) {
		return single();
	}
	const clause = (s: Story, o: { withStar?: boolean; short?: boolean }) =>
		surnamesIn(headClause(s, o), s.game);
	const themed = nightHead(lede, themes);
	if (themed) {
		// "Hawks stun Grizzlies as 4 favorites fall; Kings win 5th straight".
		const theme = themes.find((t) => t.games.includes(lede));
		const other = top.find((s) => !theme?.games.includes(s));
		// ("Timberwolves, Bulls, Warriors advance" to fit a second story.)
		const compact = themed
			.replace(/ and ([^;]*?) all /, ", $1 ")
			.replace(" all ", " ");
		for (const head of [themed, compact]) {
			for (const short of [false, true]) {
				const both = other
					? `${surnamesIn(head, lede.game)}; ${clause(other, { short })}`
					: undefined;
				if (both && both.length <= NIGHT_HEAD_MAX) {
					return cap(both);
				}
			}
		}
		return cap(themed);
	}
	const unitClause = (
		u: Story[],
		o: { withStar?: boolean; short?: boolean },
	): string => (u.length > 1 ? groupClause(u)! : clause(u[0]!, o));
	const tries: string[][] = [];
	for (const n of [units.length, 2]) {
		const us = units.slice(0, n);
		tries.push(
			us.map((u, i) => unitClause(u, { withStar: i === 0 })),
			us.map((u) => unitClause(u, {})),
			us.map((u, i) => unitClause(u, { withStar: i === 0, short: true })),
			us.map((u) => unitClause(u, { short: true })),
		);
	}
	for (const parts of tries) {
		const head = parts.join("; ");
		if (parts.length >= 2 && head.length <= NIGHT_HEAD_MAX) {
			return cap(head);
		}
	}
	// One story told in full, when a second will not fit beside it.
	if (units[0]!.length > 1) {
		const head = unitClause(units[0]!, {});
		if (head.length <= NIGHT_HEAD_MAX) {
			return cap(head);
		}
	}
	return single();
};

// A top story as a paragraph of its own: the wire lede, the detail that made
// it, and what it meant.
const storyParagraph = (
	s: Story,
	rng: Rng,
	first: boolean,
	alone: boolean,
	night: string,
): string => {
	// Told as the headline has it.
	if (
		endsLongRun(s) &&
		s.kind !== "ended" &&
		headClause(s, {}).includes(" end ")
	) {
		s = { ...s, kind: "ended" };
	}
	const para: string[] = [ledeSentence(s, rng, night)];
	const detail = ledeDetail(s, para.join(" "));
	if (detail) {
		para.push(detail);
	}
	const ctx = contextOf(s, para.join(" "), rng, alone, night);
	para.push(...ctx.slice(0, detail ? 1 : first ? 2 : 1));
	if (para.length === 1) {
		const extra = depthLine(s, para[0]!);
		if (extra) {
			para.push(extra);
		}
	}
	return theyNext(para, [s.w, s.l]);
};

export const writeDayRecap = (input: AutoDayRecapInput, rng: Rng): string => {
	const { games, standings, playoffs } = input;
	const stories = games
		.filter((g) => !g.allStar)
		.map(storyOf)
		.sort((a, b) => b.weight - a.weight);
	if (stories.length === 0) {
		return "";
	}
	const themes = themesOf(stories);

	// The night's top stories: its biggest that are stories of their own -
	// or, on a night with none, its biggest game. They head it and get a
	// paragraph each, in the headline's order.
	let top = stories.filter(isStory).slice(0, MAX_TOP);
	if (top.length === 0) {
		top = [stories[0]!];
	}
	const units = unitsOf(top);
	const headline = nightHeadline(units, themes, rng);
	top = units.flat();
	const rest = stories.filter((s) => !top.includes(s));

	// The night's shape (three games won at the buzzer, five favorites
	// beaten): after the first story when it was one of them, else over the
	// list of the rest. Not when the paragraphs tell every game in it anyway.
	const theme = [...themes].sort(
		(a, b) =>
			themeRank(b) +
			(b.games.includes(top[0]!) ? 1 : 0) -
			(themeRank(a) + (a.games.includes(top[0]!) ? 1 : 0)),
	)[0];
	const themeTold =
		theme !== undefined &&
		theme.kind !== "upsets" &&
		theme.games.every((x) => top.includes(x));
	const themeText =
		theme && !themeTold ? themeLine(theme, top[0]!) || undefined : undefined;
	const themeInLede = theme !== undefined && theme.games.includes(top[0]!);

	// A paragraph for each top story, in the headline's order.
	const paragraphs: string[] = [];
	let night = "";
	for (const [i, s] of top.entries()) {
		let text = storyParagraph(s, rng, i === 0, stories.length === 1, night);
		if (i === 0 && themeText && (themeInLede || rest.length === 0)) {
			text += ` ${themeText}`;
		}
		night += ` ${text}`;
		paragraphs.push(text);
	}

	// Every other game, a line in a list, biggest story first.
	const itemStories = rest.slice(0, MAX_ITEMS);
	const quiet = rest.slice(MAX_ITEMS);
	let lastKey = "";
	let openers = top.filter((s) => s.series?.state === "opener").length;
	const items = itemStories.map((s0) => {
		let s = s0;
		if (s.kind === "series" && s.series!.state === "opener") {
			openers += 1;
			if (openers > 2) {
				s = { ...s, kind: s.gameKind };
			}
		}
		const shapes = itemShapes(s, rng);
		const fresh = shapes.filter((x) => x.key !== lastKey);
		const main = pickKeyed(
			rng,
			(fresh.length > 0 ? fresh : shapes).map((x) => ({
				key: `${s.kind}:${x.key}`,
				value: x,
			})),
			"day:item",
		);
		lastKey = main.key;
		// One more thing it meant, when it meant something; who got hurt is
		// told at the end with the rest.
		const ctx = contextOf({ ...s, hurt: [] }, main.text, rng, false, night);
		const text = theyNext([main.text, ...ctx.slice(0, 1)], [s.w, s.l]);
		night += ` ${text}`;
		return `- **${s.header}:** ${text}`;
	});
	if (quiet.length > 0) {
		const clauses = quiet.map(
			(s) => `${s.w} ${beatVerb(s, rng)} ${s.l} ${s.score}`,
		);
		items.push(`- ${cap(`${naturalList(clauses)}.`)}`);
	}
	const intro = themeText && !themeInLede ? themeText : undefined;
	if (items.length === 1 && !intro) {
		// (One game left: a paragraph of its own, not a list of one.)
		paragraphs.push(items[0]!.replace(/^- /, ""));
	} else if (items.length > 0) {
		paragraphs.push(
			[`**Around the league**`, ...(intro ? [intro] : []), ...items].join("\n"),
		);
	}

	// What the night left: the standings, who got hurt in the games the list
	// told, what's next.
	const said = [headline, ...paragraphs].join(" ");
	const dayCtx: DayBeatContext = {
		games,
		standings,
		saidTids: new Set(),
		saidPlayers: new Set(),
	};
	if (playoffs) {
		const bracket = dayBracketWatch(dayCtx, rng);
		if (
			bracket &&
			!/face elimination|stay alive|from elimination|more loss ends/.test(said)
		) {
			paragraphs.push(bracket);
		}
	} else {
		const picture = conferencePictureSentence(
			standings,
			rng,
			dayCtx.saidTids,
			input.day,
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
		if (picture && !/first place|unbeaten/.test(said)) {
			paragraphs.push(picture);
		}
	}
	const hurt: string[] = [];
	for (const s of rest.filter((x) => x.hurt.length > 0)) {
		const text = hurtSentence(s.hurt, "", s.game.playoffs);
		if (text && hurt.length < 3) {
			hurt.push(text);
		}
	}
	if (hurt.length > 0) {
		paragraphs.push(hurt.join(" "));
	}
	if (!playoffs) {
		const next = dayNextSlate(dayCtx, rng);
		if (next) {
			paragraphs.push(next);
		}
	}

	return `**${headline}**\n\n${paragraphs.join("\n\n")}`;
};
