// THE STORY OF A GAME.
//
// getAutoRecap used to write a game as a list: the result, the star's line,
// the halftime score, the score going into the fourth, each late basket with
// its clock, then two men "chipping in" for the winners and two for the
// losers. Every sentence was true and every one stood alone, so a page of
// recaps read as one template printed fourteen times - "X led 52-46 at
// halftime. The lead had grown to 7 going into the fourth. A chipped in 23
// points for the Suns, and B contributed 19."
//
// A reporter writes the same facts as a story. One sentence up top carries
// the result, the man of the night and the thing that made the game what it
// was. The game itself is told as an arc - who had it, when it turned, how
// it ended - in sentences joined by what actually connects them ("but",
// "then", "before"). The box score comes after, each side in a sentence or
// two with the reason it won or lost hung on it.
//
// So this file plans before it writes. The lede, the game story and the two
// casts are each composed from the game's facts as whole thoughts, never
// assembled from independent one-fact sentences and stitched up afterwards.
// Everything it prints is a number from the box score or the score log.

import {
	clockLeft,
	periodTopOf,
	type GameFlowSide,
} from "../../common/gameFlow.ts";
import type {
	RecapGame,
	RecapPlayer,
	RecapTeam,
} from "./getDayGamesForRecap.ts";
import {
	bestOf,
	bigQuarterRole,
	closeLate,
	clutchVerb,
	clutchWhat,
	comebackSize,
	finishNote,
	heldOn,
	leadVerb,
	nameOfPid,
	pastTense,
	scoreTag,
	shootingFlourish,
	sideOf,
	twinOf,
	verbPool,
	type ClutchShot,
	type Headline,
	type LedeTail,
	type Shape,
} from "./getAutoRecap.ts";
import { finishStory } from "./recapFinish.ts";
import {
	aFigure,
	aNum,
	aNumWord,
	cap,
	doubleCategories,
	doubleWord,
	nick,
	numWord,
	ordinal,
	otPhrase,
	naturalList,
	pick,
	pickKeyed,
	placeName,
	plural,
	poss,
	statPhrase,
	teamStats,
	theNick,
	claimVerb,
	verbFree,
} from "./recapText.ts";

type Rng = () => number;

// What the piece has said so far, so nothing is said twice. Kept as facts,
// not read back out of the prose.
export type Told = {
	// Men whose scoring line is on the page.
	lines: Set<string>;
	// The star's line is in the lede.
	starLine: boolean;
	// How far back the winners came from.
	comeback: boolean;
	// That the winners never trailed.
	wire: boolean;
	// The winners' biggest lead, and the losers'.
	wPeak: boolean;
	lPeak: boolean;
	// Periods whose score has been given.
	quarters: Set<number>;
	// Men named with only part of their night ("9 of his 18 points in the
	// quarter"), who can still be given the rest.
	partial: Set<string>;
	// The second half's score.
	secondHalf: boolean;
	// The game's longest run.
	run: boolean;
	// The basket that won it.
	shot: boolean;
	// That it went to overtime.
	ot: boolean;
	// A streak the result kept going or ended.
	streak: boolean;
	// How many reached double figures.
	balance: boolean;
	// The man the last sentence was about.
	hero?: string;
	// The star's line as the lede printed it, when the lede had room for
	// only part of it.
	starPrinted?: string;
	// That the losers took the lead late and the winners took it back.
	retake?: boolean;
};

export type StoryInput = {
	game: RecapGame;
	shape: Shape;
	star: RecapPlayer;
	headline: Headline;
	tail: LedeTail;
	// The winning shot, when the game had one.
	shot?: ClutchShot;
	// A go-ahead basket in the last minute that was not the last score.
	goAhead?: { text: string; name: string };
	// What a playoff lede adds after the score: " in Game 4 of the First
	// Round", " to close out the series 4-1".
	where?: string;
	rng: Rng;
};

// --- The game in numbers ---------------------------------------------------

type Facts = {
	game: RecapGame;
	shape: Shape;
	W: string;
	L: string;
	// Regulation periods, and what one is called.
	n: number;
	unit: string;
	// Running scores and the winners' margin after each period.
	ws: number[];
	ls: number[];
	cum: number[];
	// Periods in the first half, when the game has halves.
	halfAt?: number;
	// The winners' margin at halftime and going into the last period.
	mh?: number;
	m3?: number;
	// The margin regulation ended on: the final, or level before overtime.
	reg: number;
	wSide: GameFlowSide;
	lSide: GameFlowSide;
	// Each side's biggest lead and the period it came in (0 without a log).
	wp: number;
	lp: number;
	wpAt?: number;
	lpAt?: number;
	// The longest unanswered run.
	run?: {
		mine: boolean;
		pts: number;
		period: number;
		endPeriod: number;
		top?: { p: RecapPlayer; pts: number };
	};
	close: boolean;
};

const factsOf = (game: RecapGame, shape: Shape): Facts => {
	const n = shape.regPeriods;
	const ws: number[] = [];
	const ls: number[] = [];
	const cum: number[] = [];
	let w = 0;
	let l = 0;
	// Quarters that do not add up to the final are no quarters at all: a
	// story told from them contradicts the score above it.
	const adds =
		shape.wq.reduce((a, b) => a + b, 0) === shape.winner.pts &&
		shape.lq.reduce((a, b) => a + b, 0) === shape.loser.pts;
	const len =
		shape.wq.length === shape.lq.length && shape.wq.length >= n && adds
			? shape.wq.length
			: 0;
	for (let i = 0; i < len; i++) {
		w += shape.wq[i] ?? 0;
		l += shape.lq[i] ?? 0;
		ws.push(w);
		ls.push(l);
		cum.push(w - l);
	}
	const halfAt = n % 2 === 0 && n >= 2 && len > 0 ? n / 2 : undefined;
	const wSide = sideOf(game, shape.winner);
	const lSide = sideOf(game, shape.loser);
	const flow = game.flow;
	const r = flow?.run;
	const runSide = r
		? r.side === wSide
			? shape.winner
			: shape.loser
		: undefined;
	const runTop =
		r && runSide && r.topPid !== undefined && r.topPts !== undefined
			? runSide.players.find((p) => p.pid === r.topPid)
			: undefined;
	return {
		game,
		shape,
		W: theNick(shape.winner),
		L: theNick(shape.loser),
		n,
		unit: n === 4 ? "quarter" : n === 2 ? "half" : "period",
		ws,
		ls,
		cum,
		halfAt,
		mh: halfAt === undefined ? undefined : cum[halfAt - 1],
		m3: len > 0 && n >= 2 ? cum[n - 2] : undefined,
		reg: shape.ot > 0 ? 0 : shape.margin,
		wSide,
		lSide,
		wp: flow ? flow.maxLead[wSide] : 0,
		lp: flow ? flow.maxLead[lSide] : 0,
		wpAt: flow?.maxLeadAt?.[wSide] || undefined,
		lpAt: flow?.maxLeadAt?.[lSide] || undefined,
		run:
			r && r.pts >= 10
				? {
						mine: r.side === wSide,
						pts: r.pts,
						period: r.period,
						endPeriod: r.endPeriod ?? r.period,
						top:
							runTop && r.topPts !== undefined && r.topPts >= 6
								? { p: runTop, pts: r.topPts }
								: undefined,
					}
				: undefined,
		close: shape.margin <= 5 || shape.ot > 0 || closeLate(game),
	};
};

// IS THE WINNERS' RUN NEWS? "Ran off 11 straight points in the second" was
// in most recaps. It is told when it is 15 or more, when it came in a
// quarter that turned the game or broke it open (12 or more), or in a last
// quarter the game was won in from behind or level.
const runIsNews = (f: Facts): boolean => {
	const r = f.run;
	if (!r || !r.mine) {
		return false;
	}
	const after = (n: number) => (n <= 0 ? 0 : (f.cum[n - 1] ?? 0));
	return (
		r.pts >= 15 ||
		(r.pts >= 12 &&
			((after(r.period - 1) <= 4 && after(r.period) >= 6) ||
				(after(r.period - 1) < 0 && after(r.period) > 0))) ||
		(r.pts >= 10 && r.period === f.n && f.shape.ot === 0 && after(f.n - 1) <= 0)
	);
};

// "third quarter", "second half", "overtime", "second overtime".
const pName = (f: Facts, period: number): string =>
	period <= f.n
		? `${ordinal(period)} ${f.unit}`
		: period === f.n + 1
			? "overtime"
			: `${ordinal(period - f.n)} overtime`;

// "in the third quarter", "in overtime".
const inP = (f: Facts, period: number): string =>
	period === f.n + 1 ? "in overtime" : `in the ${pName(f, period)}`;

// A pick that can never choose a phrasing the headline already used: "Down
// 13, the Lakers come back to beat the Pacers" over "The Lakers erased a
// 13-point deficit" is fine, over "The Lakers came back from 13 down" it is
// the headline read out twice.
const STEMS = [
	"eras",
	"rall",
	"storm",
	"came from|come from|came back|come back|comeback",
	"hole",
	"deficit",
	"never trail|without ever trailing|wire to wire|start to finish|opening tip",
	"rout",
	"roll",
	"cruis",
	"blew|blow",
	"pull(?:ed)? away",
	"ran away|run away",
	"h[eo]ld off",
	"outlast",
	"edge",
	"surviv",
	"slip",
	"stun",
	"upset",
	"knock",
	"handl",
	"br(?:oke|eak) it open",
	"took control|take control|took over|take over",
	"overtime|extra period",
	"lift",
	"sink|sank",
	"too much",
	"take care|took care",
	"lean",
	"ride|rode",
].map((s) => new RegExp(String.raw`\b(?:${s})`, "i"));

const freshOf = <T extends string>(headline: string, options: T[]): T[] => {
	const spent = STEMS.filter((re) => re.test(headline));
	const fresh = options.filter((o) => !spent.some((re) => re.test(o)));
	return fresh.length > 0 ? fresh : options;
};

// --- The lede --------------------------------------------------------------

// The star as the lede names him, and his line.
const starBits = (
	input: StoryInput,
	// The rest of the sentence is long: his line keeps to its numbers.
	tight: boolean,
) => {
	const { game, shape, star, rng } = input;
	const flourish = tight
		? undefined
		: ((star.pts >= 22 ? shootingFlourish(star) : undefined) ??
			(shape.margin >= 20 && star.pts >= 20 && star.min > 0 && star.min <= 30
				? `in just ${star.min} minutes`
				: undefined));
	const topPts = Math.max(...shape.winner.players.map((p) => p.pts));
	const plain = statPhrase(star, tight ? 1 : 2);
	const doubles = doubleCategories(star).length;
	// A line that is only points for a man somebody outscored reads as the
	// wrong pick: let the rest of what he did in at a lower bar.
	const numbers =
		plain === plural(star.pts, "point") && star.pts < topPts
			? statPhrase(star, 2, true)
			: doubles >= 3
				? statPhrase(star, 2)
				: plain;
	const hasExtras = numbers !== plural(star.pts, "point");
	const tripleTag = doubles >= 3 ? ` for a ${doubleWord(doubles)!}` : "";
	const verb =
		doubles >= 3
			? pick(rng, ["posted", "recorded", "produced"], "ledeTripleVerb")
			: leadVerb(star, hasExtras, rng);
	// "Rookie Jalen Green", "Vince Carter, 36,": only where the night earns
	// the word, and not every time.
	const entering =
		game.playoffs && star.playoffAvg && star.playoffAvg.gp >= 2
			? star.playoffAvg
			: star.seasonAvg && star.seasonAvg.gp >= 3
				? star.seasonAvg
				: undefined;
	let subject = star.name;
	if (
		!tight &&
		flourish === undefined &&
		entering &&
		star.pts >= entering.pts + 12 &&
		star.pts >= 24 &&
		rng() < 0.5
	) {
		subject = `${star.name}, who came in averaging ${entering.pts} points a game,`;
	} else if (star.rookie && star.pts >= 20 && rng() < 0.5) {
		subject = `Rookie ${star.name}`;
	} else if (
		star.age !== undefined &&
		star.age >= 34 &&
		star.pts >= 24 &&
		rng() < 0.35
	) {
		subject = `${star.name}, ${star.age},`;
	}
	const participial =
		flourish !== undefined && /^(?:hitting|making) /.test(flourish);
	const withF = (closing: boolean) =>
		flourish === undefined
			? `${numbers}${tripleTag}`
			: participial
				? `${numbers}${tripleTag}, ${flourish}${closing ? "," : ""}`
				: `${numbers}${tripleTag} ${flourish}`;
	return {
		subject,
		verb,
		// His line before "as the Suns..." and before ", and the Suns...".
		lineAs: withF(true),
		lineAnd: withF(false),
		// His line after "behind" or "from": numbers only - and a
		// triple-double called one.
		numbers,
		fromLine:
			doubles >= 3 ? `a ${doubleWord(doubles)!} of ${numbers}` : numbers,
		pointsOnly: !hasExtras,
		// His name after "from": "from rookie Jalen Green", "from
		// 36-year-old Vince Carter".
		named: subject.startsWith("Rookie ")
			? `rookie ${star.name}`
			: subject === `${star.name}, ${star.age},`
				? `${star.age}-year-old ${star.name}`
				: star.name,
	};
};

// What the winners did, as the predicate of a sentence about them: "rallied
// from 23 down to beat the Heat 115-111". `noun` marks the shapes that end
// on "win over the Heat", which cannot carry a playoff "to close out the
// series" after them.
type Predicate = {
	text: string;
	// The clause ends in an infinitive ("...to beat the Heat 115-111"), so
	// whatever follows it cannot open on another.
	inf?: boolean;
	noun?: boolean;
	// It used the streak the lede's tail would have told.
	tailSpent?: boolean;
	// It cannot take a clause after it.
	closed?: boolean;
	// It says nothing but who beat whom.
	bare?: boolean;
	// It ends on the score after a comma ("...to get past the Bulls,
	// 112-108"), so only a clause with its own comma can follow.
	comma?: boolean;
	tell?: (t: Told) => void;
};

const WHERE_ING: Record<string, string> = {
	claim: "claiming",
	end: "ending",
	close: "closing",
	win: "winning",
	finish: "finishing",
	reach: "reaching",
};

const predicates = (input: StoryInput, f: Facts): Predicate[] => {
	const { game, shape, headline, tail, rng } = input;
	const { L } = f;
	const where = input.where ?? "";
	const stakes = /^ to \w/.test(where);
	const sc = headline.spentScore ? "" : ` ${scoreTag(shape)}`;
	const plainSc = headline.spentScore
		? ""
		: ` ${shape.winner.pts}-${shape.loser.pts}`;
	// The losers, with the score and the round after them. After an
	// infinitive a second one becomes a participle: "to beat the Heat
	// 115-111, claiming the No. 7 seed".
	const R = (inf = false, score = sc, o = "") =>
		`${L}${score}${o}${
			inf && stakes
				? where.replace(/^ to (\w+)/, (m, v: string) =>
						WHERE_ING[v] ? `, ${WHERE_ING[v]}` : m,
					)
				: where
		}`;
	const verbs = freshOf(headline.text, verbPool(game, shape));
	const beat = () => pastTense(pick(rng, verbs));
	const plainVerbs = freshOf(headline.text, [
		"beat",
		"top",
		"take down",
		"knock off",
	]);
	const beatInf = () => pick(rng, plainVerbs, "comebackVerb");
	// A win described as a thing: "a 100-81 win over the 76ers".
	const aWin = headline.spentScore
		? `a win over ${L}`
		: `${aFigure(scoreTag(shape))} win over ${L}`;
	// The same after a clause that has just named the losers: "outscored the
	// Grizzlies 46-34 after halftime on the way to an 81-72 win".
	const theWin = headline.spentScore
		? "the win"
		: `${aFigure(scoreTag(shape))} win`;
	const out: Predicate[] = [];
	const add = (p: Predicate) => {
		if (!(p.noun && stakes)) {
			out.push(p);
		}
	};

	// A streak kept going or ended can carry the sentence itself.
	const streakForms = (): Predicate[] => {
		if (game.playoffs || stakes) {
			return [];
		}
		if (tail.streak === "skid") {
			const m = /snapping (.+) losing streak/.exec(tail.text);
			return m
				? [
						{
							text: `snapped ${m[1]} losing streak with ${aWin}${where}`,
							noun: true,
							tailSpent: true,
						},
						{
							text: `ended ${m[1]} losing streak, beating ${R()}`,
							tailSpent: true,
						},
					]
				: [];
		}
		if (tail.streak === "win") {
			const s = shape.winner.streak;
			return s
				? [
						{
							text: `won their ${ordinal(s.count)} straight, beating ${R()}`,
							tailSpent: true,
						},
						{
							text: `ran their winning streak to ${numWord(s.count)} with ${aWin}${where}`,
							noun: true,
							tailSpent: true,
						},
					]
				: [];
		}
		return [];
	};

	const down = comebackSize(shape);
	if (down > 0) {
		const at =
			shape.loserPeak >= shape.comebackFrom ? f.lpAt : shape.comebackPeriod;
		const half = f.halfAt !== undefined && at !== undefined && at <= f.halfAt;
		const when = at === undefined || at > f.n ? "" : ` ${inP(f, at)}`;
		const tell = (t: Told) => {
			t.comeback = true;
			t.lPeak = true;
		};
		if (shape.margin >= 15) {
			const early = half ? " early" : "";
			add({
				text: `fell behind by ${down}${early} and still ran away from ${R()}`,
				tell,
			});
			add({
				text: `spotted ${L} ${aNum(down)}-point lead${early} and won going away${sc ? `,${sc}` : ""}${where}`,
				tell,
			});
			add({
				text: `trailed by ${down}${when} before running away from ${R()}`,
				tell,
			});
			return freshOf(
				headline.text,
				out as (Predicate & string)[],
			) as Predicate[];
		}
		const o = shape.ot > 0 ? ` in ${otPhrase(shape.ot)}` : "";
		const score = shape.ot > 0 ? plainSc : sc;
		const tellOt = (t: Told) => {
			tell(t);
			if (shape.ot > 0) {
				t.ot = true;
			}
		};
		add({
			text: `rallied from ${down} down${when} to ${beatInf()} ${R(true, score, o)}`,
			inf: true,
			tell: tellOt,
		});
		add({
			text: `erased ${aNum(down)}-point deficit and ${pastTense(beatInf())} ${R(false, score, o)}`,
			tell: tellOt,
		});
		add({
			text: `trailed by ${down}${when} before storming back to ${beatInf()} ${R(true, score, o)}`,
			inf: true,
			tell: tellOt,
		});
		add({
			text: `came from ${down} down to ${beatInf()} ${R(true, score, o)}`,
			inf: true,
			tell: tellOt,
		});
		add({
			text: `overcame ${aNum(down)}-point deficit${when} to ${beatInf()} ${R(true, score, o)}`,
			inf: true,
			tell: tellOt,
		});
		add({
			text: `climbed out of ${aNum(down)}-point hole and ${pastTense(beatInf())} ${R(false, score, o)}`,
			tell: tellOt,
		});
		return out;
	}

	if (shape.ot > 0) {
		const tell = (t: Told) => {
			t.ot = true;
		};
		const extra =
			shape.ot === 1 ? "overtime" : `${numWord(shape.ot)} overtimes`;
		add({
			text: `${beat()} ${R(false, plainSc, ` in ${otPhrase(shape.ot)}`)}`,
			tell,
		});
		add({
			text: `needed ${extra} to ${pick(rng, ["get past", "put away", "beat"], "otInf")} ${L}${plainSc ? `,${plainSc}` : ""}${where}`,
			inf: !plainSc,
			comma: !!plainSc,
			tell,
		});
		add({
			text: `outlasted ${R(false, plainSc, ` in ${otPhrase(shape.ot)}`)}`,
			tell,
		});
		return out;
	}

	out.push(...streakForms());

	if (shape.wireToWire && shape.margin >= 10) {
		const tell = (t: Told) => {
			t.wire = true;
		};
		add({ text: `never trailed in ${aWin}${where}`, noun: true, tell });
		add({ text: `led from start to finish and ${beat()} ${R()}`, tell });
		add({ text: `led wire to wire and ${beat()} ${R()}`, tell });
		add({ text: `never trailed and ${beat()} ${R()}`, tell });
		add({
			text: `led from start to finish in ${aWin}${where}`,
			noun: true,
			tell,
		});
		add({
			text: `were in front from the opening tip and ${beat()} ${R()}`,
			tell,
		});
		if (tail.text === "") {
			add({
				text: `${beat()} ${R()} without ever trailing`,
				closed: true,
				tell,
			});
		}
		return out;
	}

	// A big lead that very nearly got away.
	const lastLead = game.flow?.lastLead;
	if (
		shape.margin <= 8 &&
		f.wp >= 15 &&
		f.wp >= shape.margin + 9 &&
		closeLate(game) &&
		heldOn(game) &&
		(f.mh ?? 1) > 0 &&
		(f.m3 ?? 1) > 0 &&
		!(lastLead && lastLead.side === f.wSide && lastLead.period >= f.n)
	) {
		const tell = (t: Told) => {
			t.wPeak = true;
		};
		add({
			text: `led by as many as ${f.wp} and had to hold off ${R()}`,
			tell,
		});
		add({
			text: `nearly let ${aNum(f.wp)}-point lead get away before holding off ${R()}`,
			tell,
		});
		add({
			text: `held off ${R()} after leading by as many as ${f.wp}`,
			tell,
			closed: true,
		});
		return out;
	}

	if (shape.margin >= 20) {
		for (let i = 0; i < 2; i++) {
			add({ text: `${beat()} ${R()}`, bare: true });
		}
		if (f.wp >= shape.margin + 4) {
			add({
				text: `led by as many as ${f.wp} and ${beat()} ${R()}`,
				tell: (t) => {
					t.wPeak = true;
				},
			});
			add({
				text: `led by as many as ${f.wp} in ${aWin}${where}`,
				noun: true,
				tell: (t) => {
					t.wPeak = true;
				},
			});
		}
		if (f.mh !== undefined && f.mh >= 15) {
			add({
				text: `built ${aNum(f.mh)}-point halftime lead and ${beat()} ${R()}`,
			});
		}
		return out;
	}

	const role = bigQuarterRole(shape);
	const r = shape.bigRun;
	if (
		r &&
		role &&
		r.margin >= 9 &&
		shape.margin >= 8 &&
		r.period > 1 &&
		r.period <= f.n &&
		f.cum.length > 0 &&
		!closeLate(game)
	) {
		const q = `${r.wpts}-${r.lpts} ${pName(f, r.period)}`;
		const tell = (t: Told) => {
			t.quarters.add(r.period);
		};
		add({
			text: `${role === "broke" ? "broke it open" : "took control"} with ${aFigure(q)} and ${beat()} ${R()}`,
			tell,
		});
		add({
			text: `used ${aFigure(q)} to ${pick(rng, shape.margin >= 15 ? ["pull away from", "put away", "run away from"] : ["get past", "pull away from", "put away"], "quarterInf")} ${L}${sc ? `,${sc}` : ""}${where}`,
			inf: !sc,
			comma: !!sc,
			tell,
		});
		add({
			text: `outscored ${L} ${r.wpts}-${r.lpts} ${inP(f, r.period)} on the way to ${theWin}${where}`,
			noun: true,
			tell,
		});
		if (tail.text === "") {
			add({
				text: `${beat()} ${R()}, ${role === "broke" ? "breaking it open" : "taking control"} with ${aFigure(q)}`,
				closed: true,
				tell,
			});
		}
		return out;
	}

	// Decided in the last period, from behind or level.
	if (
		f.n === 4 &&
		f.m3 !== undefined &&
		f.m3 <= 0 &&
		f.m3 > -12 &&
		shape.margin >= 6 &&
		shape.wq[f.n - 1]! > shape.lq[f.n - 1]! &&
		!closeLate(game)
	) {
		const a = shape.wq[f.n - 1]!;
		const b = shape.lq[f.n - 1]!;
		const tell = (t: Told) => {
			t.quarters.add(f.n);
		};
		add({
			text:
				f.m3 === 0
					? `broke a tie with ${aFigure(`${a}-${b}`)} fourth quarter and ${beat()} ${R()}`
					: `trailed after three quarters, then won the fourth ${a}-${b} to ${beatInf()} ${R(true)}`,
			inf: f.m3 !== 0,
			tell,
		});
		add({
			text: `outscored ${L} ${a}-${b} in the fourth quarter on the way to ${theWin}${where}`,
			noun: true,
			tell,
		});
		add({
			text: `pulled away in the fourth quarter to ${beatInf()} ${R(true)}`,
			inf: true,
		});
		return out;
	}

	// What else the numbers say about how it was won, for a game with no one
	// headline fact.
	if (f.n === 4 && f.mh !== undefined && f.m3 !== undefined && !f.close) {
		const a4 = shape.wq[3]!;
		const b4 = shape.lq[3]!;
		const sa = shape.wq[2]! + a4;
		const sb = shape.lq[2]! + b4;
		if (f.m3 <= 5 && a4 - b4 >= 7 && shape.margin >= 8) {
			add({
				text: `pulled away in the fourth quarter to ${beatInf()} ${R(true)}`,
				inf: true,
			});
			add({
				text: `won the fourth quarter ${a4}-${b4} and ${beat()} ${R()}`,
				tell: (t) => {
					t.quarters.add(4);
				},
			});
		} else if (f.mh <= 4 && sa - sb >= 10) {
			add({
				text: `outscored ${L} ${sa}-${sb} after halftime on the way to ${theWin}${where}`,
				noun: true,
				tell: (t) => {
					t.secondHalf = true;
				},
			});
			add({
				text: `pulled away after halftime to ${beatInf()} ${R(true)}`,
				inf: true,
			});
		}
	}
	if (f.wp >= shape.margin + 6 && f.wp >= 14 && !f.close) {
		add({
			text: `led by as many as ${f.wp} and ${beat()} ${R()}`,
			tell: (t) => {
				t.wPeak = true;
			},
		});
	}
	for (let i = 0; i < (out.length > 0 ? 2 : 3); i++) {
		add({ text: `${beat()} ${R()}`, bare: true });
	}
	// Where it was won, when nothing else is in the way.
	if (!game.playoffs && where === "" && tail.text === "") {
		const winnerHome = game.teams[0].tid === shape.winner.tid;
		const place = placeName(shape.loser.region);
		if (winnerHome) {
			add({
				text: `${beat()} the visiting ${nick(shape.loser)}${sc}`,
				bare: true,
			});
		} else if (place && sc) {
			add({ text: `${beat()} ${L}${sc} in ${place}`, closed: true });
		} else {
			add({ text: `went on the road and ${beat()} ${L}${sc}` });
		}
	}
	return out;
};

// Six or seven in double figures behind a quiet leader is a lede about the
// team.
const balancedLede = (input: StoryInput): boolean => {
	const { shape, star } = input;
	return (
		teamStats(shape.winner).dblFig >= 6 &&
		star.pts < 18 &&
		doubleCategories(star).length < 3
	);
};

// The scoring verbs a stretch of finished prose has spent, entered in the
// recap's ledger so that nothing written after it says one again.
const CAST_VERBS = [
	"had",
	"scored",
	"finished with",
	"posted",
	"put up",
	"added",
	"chipped in",
	"contributed",
	"went for",
	"tacked on",
	"turned in",
];
const claimIn = (text: string) => {
	for (const v of CAST_VERBS) {
		if (text.includes(` ${v} `)) {
			claimVerb(v);
		}
	}
};

export const composeLede = (
	input: StoryInput,
): { sentences: string[]; told: Told } => {
	const out = ledeOf(input);
	claimIn(out.sentences.join(" "));
	return out;
};

const ledeOf = (given: StoryInput): { sentences: string[]; told: Told } => {
	// "...in Game 7 to reach the Conference Semifinals despite 33 points and
	// 9 rebounds from Evan Everett" is two sentences' worth: with the round
	// on the result, the losers' big night waits for their paragraph.
	const input: StoryInput =
		(given.where ?? "").length > 20 && given.tail.loserStar
			? { ...given, tail: { text: "" } }
			: given;
	const { game, shape, star, headline, tail, shot, goAhead, rng } = input;
	const f = factsOf(game, shape);
	const { W, L } = f;
	const told: Told = {
		lines: new Set(),
		quarters: new Set(),
		partial: new Set(),
		starLine: false,
		comeback: false,
		wire: false,
		wPeak: false,
		lPeak: false,
		secondHalf: false,
		run: false,
		shot: false,
		ot: false,
		streak: false,
		balance: false,
	};
	const sc = headline.spentScore ? "" : ` ${scoreTag(shape)}`;
	const where = input.where ?? "";
	if (tail.loserStar) {
		told.lines.add(tail.loserStar.name);
	}

	// A 40-point night in a loss, which the headline was about: the lede is
	// about it too, and the winners' man follows in the cast.
	const monster = headline.spentLoserStar
		? bestOf(shape.loser.players)
		: undefined;
	if (monster && monster.pts >= 40) {
		const verb = pastTense(pick(rng, verbPool(game, shape)));
		told.lines.add(monster.name);
		return {
			sentences: [
				pick(
					rng,
					[
						`${monster.name} gave ${L} ${monster.pts} points, and ${W} beat them anyway, ${scoreTag(shape)}${where}${tail.text}.`,
						`${monster.name} scored ${monster.pts} and it still was not enough - ${W} ${verb} ${L} ${scoreTag(shape)}${where}${tail.text}.`,
						`On most nights ${poss(monster.name)} ${monster.pts} points win the game. On this one ${W} ${verb} ${L} ${scoreTag(shape)}${where}${tail.text}.`,
					],
					"lede:monster-loss",
				),
			],
			told,
		};
	}

	const pool = predicates(input, f);
	const tailOf = (p: Predicate) => (p.tailSpent ? "" : tail.text);
	const spendTail = (p: Predicate) => {
		p.tell?.(told);
		if (tail.streak) {
			told.streak = true;
		}
	};
	// The shape of a predicate, with its names and numbers out: what the
	// night's rotation remembers, so "never trailed in a 100-81 win over the
	// 76ers" waits its turn however the pool around it changes.
	const shapeOf = (text: string) =>
		text.replaceAll(nick(shape.loser), "~").replaceAll(/\d+(?:\.\d+)?/g, "#");
	const choose = (ok: (p: Predicate) => boolean = () => true): Predicate => {
		const fits = pool.filter(ok);
		const from = fits.length > 0 ? fits : pool;
		const fresh = new Set(
			freshOf(
				headline.text,
				from.map((p) => p.text),
			),
		);
		return pickKeyed(
			rng,
			from
				.filter((p) => fresh.has(p.text))
				.map((p) => ({ key: shapeOf(p.text), value: p })),
			"lede:predicate",
		);
	};

	// --- The basket that won it leads, whoever made it. ---
	if (shot && !shot.tying) {
		const hero = shape.winner.players.find((p) => p.name === shot.name);
		const what = clutchWhat(shot);
		const did = `${clutchVerb(what)} ${what}`;
		const down = comebackSize(shape);
		const o = shape.ot > 0 ? ` in ${otPhrase(shape.ot)}` : "";
		const plain = headline.spentScore
			? ""
			: ` ${shape.winner.pts}-${shape.loser.pts}`;
		const score = shape.ot > 0 ? plain : sc;
		const closeVerbs = freshOf(headline.text, [
			"beat",
			"edged",
			"slipped past",
			"got past",
			"topped",
		]);
		const result =
			down > 0
				? pick(
						rng,
						freshOf(headline.text, [
							`completed a comeback from ${down} down to beat ${L}${score}${o}${where}`,
							`finished off a rally from ${down} down, beating ${L}${score}${o}${where}`,
							`came all the way back from ${down} down to beat ${L}${score}${o}${where}`,
						]),
						"shotComeback",
					)
				: `${pick(rng, closeVerbs, "shotVerb")} ${L}${score}${o}${where}`;
		told.shot = true;
		told.hero = shot.name;
		if (down > 0) {
			told.comeback = true;
			told.lPeak = true;
		}
		if (shape.ot > 0) {
			told.ot = true;
		}
		if (tail.streak) {
			told.streak = true;
		}
		const heroLine = hero && (hero === star || hero.pts >= 15);
		if (hero && heroLine) {
			told.lines.add(hero.name);
			if (hero === star) {
				told.starLine = true;
			}
			const line = statPhrase(hero, hero === star ? 2 : 1);
			return {
				sentences: [
					pick(
						rng,
						[
							`${shot.name} ${did} and finished with ${line} as ${W} ${result}${tail.text}.`,
							`${shot.name} ${did}, and ${W} ${result}. He finished with ${line}.`,
							`${shot.name} scored ${hero.pts === 1 ? "one point" : `${hero.pts} points`}, none bigger than ${what.replace(/^an? /, "the ")}, and ${W} ${result}${tail.text}.`,
						].filter(
							(_, i) =>
								i === 0 ||
								// "He" after a clause that named another man is that man.
								(i === 1 && tail.text === "") ||
								// "None bigger" is a line that is only points.
								(i === 2 &&
									tail.loserStar === undefined &&
									line === plural(hero.pts, "point")),
						),
						"lede:shot-hero",
					),
				],
				told,
			};
		}
		// A big night beside the shot is in the same sentence: "Marcus Ellis
		// scored 45 points and Yuta Dunn made a layup with 1.5 seconds left as
		// the Nuggets beat the Bulls 112-110."
		if (star.pts >= 28 && star.name !== shot.name && tail.text === "") {
			told.lines.add(star.name);
			told.starLine = true;
			const line = statPhrase(star, 1);
			if (line !== statPhrase(star, 2)) {
				told.starPrinted = line;
			}
			return {
				sentences: [
					`${star.name} had ${line} and ${shot.name} ${did} as ${W} ${result}.`,
				],
				told,
			};
		}
		return {
			sentences: [
				pick(
					rng,
					[
						`${shot.name} ${did}, and ${W} ${result}${tail.text}.`,
						`${shot.name} ${did} as ${W} ${result}${tail.text}.`,
						`${cap(W)} ${result} when ${shot.name} ${did}${tail.text}.`,
					].filter((_, i) => i < 2 || tail.text === ""),
					"lede:shot",
				),
			],
			told,
		};
	}

	// --- Two men with the same number, told together. ---
	const twin = twinOf(shape, star);
	if (twin && !headline.spentLine) {
		const p = choose((x) => !x.closed);
		spendTail(p);
		told.lines.add(star.name).add(twin.name);
		told.starLine = true;
		const late = goAhead && tailOf(p) === "" && !p.noun ? goAhead.text : "";
		if (late) {
			told.shot = true;
			told.hero = goAhead!.name;
		}
		return {
			sentences: [
				`${star.name} and ${twin.name} scored ${star.pts} points apiece as ${W} ${p.text}${tailOf(p)}${late}.`,
			],
			told,
		};
	}

	// --- A night that belonged to the whole team. ---
	if (
		balancedLede(input) &&
		!headline.spentLine &&
		// "put seven players in double figures and trailed by 16 before
		// storming back" is two stories in one sentence.
		pool.some((x) => x.bare)
	) {
		const n = numWord(teamStats(shape.winner).dblFig);
		const p = choose((x) => !!x.bare && !x.closed);
		spendTail(p);
		told.balance = true;
		told.lines.add(star.name);
		told.starLine = true;
		const led = !shape.winner.players.some((x) => x.pts > star.pts);
		const line = statPhrase(star);
		const late = goAhead && tailOf(p) === "" && !p.noun ? goAhead.text : "";
		if (late) {
			told.shot = true;
			told.hero = goAhead!.name;
		}
		return {
			sentences: [
				pick(
					rng,
					(led
						? [
								`${poss(star.name)} ${line} led ${n} players in double figures as ${W} ${p.text}${tailOf(p)}${late}.`,
								`${cap(W)} put ${n} players in double figures, led by ${poss(star.name)} ${line}, and ${p.text}${tailOf(p)}${late}.`,
							]
						: [
								`${star.name} had ${line}, and ${n} players reached double figures as ${W} ${p.text}${tailOf(p)}${late}.`,
								`${cap(W)} put ${n} players in double figures and ${p.text}${tailOf(p)}, with ${star.name} finishing with ${line}.`,
							]
					).filter((_, i) => late === "" || i === 0),
					"teamLedeBalance",
				),
			],
			told,
		};
	}

	// --- The headline already printed his whole line: the lede is the game,
	// and the cast tells what the headline did not. ---
	// Or was about him, in a game with a story of its own: "Bagaric goes for
	// 26 points as the Celtics beat the Grizzlies" over "Bagaric scored 26 as
	// the Celtics topped the Grizzlies 115-105" is one sentence printed twice.
	// The lede tells the game, and he opens the paragraph about who did it.
	if (
		headline.spentLine ||
		(headline.spentStar && pool.some((x) => !x.bare) && !goAhead)
	) {
		// The sentence has only the game to tell, so it takes the shape that
		// tells the most of it.
		const p = choose((x) => !x.bare);
		spendTail(p);
		const late = goAhead && !p.closed && tailOf(p) === "" ? goAhead.text : "";
		if (late) {
			told.shot = true;
			told.hero = goAhead!.name;
		}
		// With nothing but the result to say, it at least says whose night it
		// was; his numbers are the headline's, and the cast has the rest.
		const led = !shape.winner.players.some((x) => x.pts > star.pts);
		const whose =
			p.bare && late === "" && !p.closed && tailOf(p) === "" && led
				? pick(
						rng,
						[
							`, with ${star.name} leading the way`,
							` behind ${star.name}`,
							`, led by ${star.name}`,
						],
						"lede:whose",
					)
				: "";
		return {
			sentences: [`${cap(W)} ${p.text}${tailOf(p)}${late}${whose}.`],
			told,
		};
	}

	// A blowout with no standout line is the team's story: "Brad Miller had
	// 13 points as the Clippers won by 31" makes 13 points the reason.
	const quiet =
		star.pts < 15 && shape.margin >= 20 && doubleCategories(star).length < 3;

	// How long the rest of the sentence will run decides how much of his line
	// it can hold.
	const sample = pool[0]?.text ?? "";
	const tight = sample.length + tail.text.length > 78 || where.length > 20;
	const bits = starBits(input, tight);
	told.lines.add(star.name);
	told.starLine = true;
	// A line cut down to fit is finished in the cast.
	if (bits.numbers !== statPhrase(star, 2)) {
		told.starPrinted = bits.numbers;
	}

	// A go-ahead basket in the last minute closes the sentence.
	if (goAhead) {
		const p = choose((x) => !x.closed && !x.noun);
		spendTail(p);
		told.shot = true;
		told.hero = goAhead.name;
		const own = goAhead.name === star.name;
		return {
			sentences: [
				own
					? `${bits.subject} ${bits.verb} ${bits.lineAs} as ${W} ${p.text}${tailOf(p)}${goAhead.text.replace(`${poss(goAhead.name)} go-ahead`, "his go-ahead")}.`
					: `${bits.subject} ${bits.verb} ${bits.lineAnd}, and ${W} ${p.text}${tailOf(p)}${goAhead.text}.`,
			],
			told,
		};
	}

	if (quiet) {
		const p = choose((x) => !x.closed);
		spendTail(p);
		const led = !shape.winner.players.some((x) => x.pts > star.pts);
		return {
			sentences: [
				`${cap(W)} ${p.text}${tailOf(p)}, ${led ? `led by ${poss(star.name)}` : `with ${star.name} putting up`} ${statPhrase(star)}.`,
			],
			told,
		};
	}

	// The headline was about him and had nothing to say about the game: the
	// sentence opens on the game, and his line rides on it.
	if (headline.spentStar) {
		const p = choose();
		spendTail(p);
		const t = tailOf(p);
		const from = `${bits.fromLine} from ${bits.named}`;
		const forms: string[] = [];
		// A clause hung on the end, when the sentence can take one.
		if (!p.closed && (t === "" || /^ for their /.test(t))) {
			forms.push(`${cap(W)} ${p.text}${t}, getting ${from}.`);
			if (!p.comma) {
				forms.push(`${cap(W)} ${p.text}${t} behind ${from}.`);
			}
		}
		// The one shape that says how he shot: "...as Obi King scored 29
		// points, hitting 4 of 7 threes."
		if (!p.closed && !p.comma && t === "") {
			const as = `${cap(W)} ${p.text} as ${bits.subject} ${bits.verb} ${bits.lineAnd}.`;
			if (bits.lineAnd !== bits.numbers) {
				return { sentences: [as], told };
			}
			forms.push(as);
		}
		// "got 26 points from X and led from start to finish and beat" is
		// one "and" too many.
		if (!/ and /.test(p.text)) {
			forms.push(`${cap(W)} got ${from} and ${p.text}${t}.`);
		}
		if (
			forms.length === 0 ||
			!/^(?:snapped|ended|won their|ran their)/.test(p.text)
		) {
			forms.push(`Behind ${from}, ${W} ${p.text}${t}.`);
		}
		return { sentences: [pick(rng, forms, "lede:team-first")], told };
	}
	// A headline about the result: the sentence opens on him, as the wire
	// does.
	const p = choose((x) => !x.closed || tail.text === "");
	spendTail(p);
	const t = tailOf(p);
	const forms = [
		`${bits.subject} ${bits.verb} ${bits.lineAs} as ${W} ${p.text}${t}.`,
		`${bits.subject} ${bits.verb} ${bits.lineAnd}, and ${W} ${p.text}${t}.`,
	];
	// "...to lead the Suns past the Lakers 126-113": a plain result only.
	if (
		/^(?:beat|topped|took down|handled|got past) /.test(p.text) &&
		!p.closed
	) {
		const rest = p.text.replace(
			/^(?:beat|topped|took down|handled|got past) /,
			"",
		);
		forms.push(
			`${bits.subject} ${bits.verb} ${bits.lineAnd} to lead ${W} past ${rest}${t}.`,
		);
	}
	if (!/^(?:snapped|ended|won their|ran their)/.test(p.text)) {
		forms.push(
			`Behind ${bits.fromLine} from ${bits.named}, ${W} ${p.text}${t}.`,
		);
	}
	return { sentences: [pick(rng, forms, "lede:star-first")], told };
};

// --- The game story ----------------------------------------------------------

// "59-38", leader first.
const sc2 = (a: number, b: number): string =>
	a >= b ? `${a}-${b}` : `${b}-${a}`;

// The man whose quarter it was: ", with Zeke Foster scoring 14 of his 25
// points in the quarter". Only a real share of a real night.
const quarterMan = (
	f: Facts,
	period: number,
	team: RecapTeam,
	told: Told,
	// The clause it hangs on has just named the quarter.
	named = false,
): string => {
	const top = periodTopOf(f.game.flow, period, sideOf(f.game, team));
	if (!top || top.pts < 9) {
		return "";
	}
	const p = team.players.find((x) => x.pid === top.pid);
	if (!p || p.pts < top.pts) {
		return "";
	}
	const known = told.lines.has(p.name);
	told.lines.add(p.name);
	told.partial.add(p.name);
	const where = named
		? ""
		: period > f.n
			? " in the extra period"
			: ` in the ${f.unit}`;
	if (top.pts === p.pts) {
		return `, with ${p.name} scoring all ${p.pts} of his points${named ? " in it" : where}`;
	}
	return `, with ${p.name} scoring ${top.pts} of his ${p.pts}${known ? "" : " points"}${where}`;
};

// A run, as the predicate of a sentence about the side that made it. With
// the man who scored most of it when one man did: "scored 15 straight
// points, eight of them from Devin Nash,".
const runDid = (
	f: Facts,
	told: Told,
	rng: Rng,
	// " in the fourth quarter", or nothing when the sentence has already
	// said where it is.
	span: string,
): string => {
	const r = f.run!;
	told.run = true;
	const top = r.top;
	const sp = span === "" ? "" : ` ${span}`;
	if (top && top.pts >= 6 && top.pts >= r.pts * 0.45) {
		told.hero = top.p.name;
		const share =
			top.pts === r.pts
				? `all of them from ${top.p.name}`
				: `${numWord(top.pts)} of them from ${top.p.name}`;
		return pick(
			rng,
			[
				`scored ${r.pts} straight points${sp}, ${share},`,
				`reeled off ${r.pts} straight points${sp}, ${share},`,
			],
			"runDid:who",
		);
	}
	return pick(
		rng,
		[
			`ran off ${r.pts} straight points${sp}`,
			`went on ${aNum(r.pts)}-0 run${sp}`,
			`scored ${r.pts} unanswered points${sp}`,
			`reeled off ${r.pts} straight points${sp}`,
		],
		"runDid",
	);
};

// Where a run happened, when it can be said in a word.
const runSpanOf = (f: Facts): string => {
	const r = f.run!;
	const h = f.halfAt;
	return r.period === r.endPeriod
		? inP(f, r.period)
		: h !== undefined && r.period <= h && r.endPeriod > h
			? "either side of halftime"
			: r.endPeriod > f.n
				? "late"
				: f.n === 4 && r.period === 3
					? "late in the third quarter and early in the fourth"
					: f.n === 4 && r.period === 1
						? "late in the first quarter and early in the second"
						: "";
};

// One half of the game, written as a clause that can stand alone or lean on
// the other half.
type Half = {
	// A whole sentence, when the half had a story of its own.
	sentence?: string;
	// Otherwise the bare state at the break, to be folded into what follows:
	// "led 59-38 at halftime" / "Up 59-38 at halftime".
	state?: {
		who: "w" | "l" | "tie";
		led: string;
		fronted: string;
		// Its verb can carry a second number: "led 52-46 at halftime and
		// 83-75 after three quarters".
		bare?: boolean;
	};
	// The side a "they" right after the sentence would mean.
	subject?: "w" | "l";
};

const firstHalf = (f: Facts, told: Told, rng: Rng): Half => {
	const { W, L, shape } = f;
	const h = f.halfAt;
	if (h === undefined || f.mh === undefined) {
		return {};
	}
	const a = f.ws[h - 1]!;
	const b = f.ls[h - 1]!;
	const hs = sc2(a, b);
	const mh = f.mh;
	const X = mh >= 0 ? W : L;
	const Y = mh >= 0 ? L : W;
	const xw: "w" | "l" = mh >= 0 ? "w" : "l";
	// Each side's biggest lead, when it came before the break.
	const xPeak = mh >= 0 ? f.wp : f.lp;
	const xAt = mh >= 0 ? f.wpAt : f.lpAt;
	const yPeak = mh >= 0 ? f.lp : f.wp;
	const yAt = mh >= 0 ? f.lpAt : f.wpAt;
	const xToldPeak = mh >= 0 ? told.wPeak : told.lPeak;
	const yToldPeak = mh >= 0 ? told.lPeak : told.wPeak;
	const tellPeak = (x: boolean) => {
		if (x === mh >= 0) {
			told.wPeak = true;
		} else {
			told.lPeak = true;
		}
	};
	// A first-half run: the winners' when it is news, the losers' when it
	// was a big one.
	const run =
		f.run &&
		f.run.endPeriod <= h &&
		!told.run &&
		(f.run.mine ? runIsNews(f) : f.run.pts >= 12 && f.lp > 0)
			? f.run
			: undefined;
	const runIsX = run ? run.mine === mh >= 0 : false;
	const q1 =
		h >= 2 && f.cum[0] !== undefined
			? { m: f.cum[0], s: sc2(f.ws[0]!, f.ls[0]!) }
			: undefined;

	if (mh === 0) {
		// Level at the break, after somebody had been well in front.
		const peak = Math.max(f.wp, f.lp);
		const who = f.wp >= f.lp ? W : L;
		const at = f.wp >= f.lp ? f.wpAt : f.lpAt;
		if (peak >= 10 && at !== undefined && at <= h) {
			if (f.wp >= f.lp) {
				told.wPeak = true;
			} else {
				told.lPeak = true;
			}
			return {
				sentence: `${cap(who)} led by as many as ${peak} ${inP(f, at)}, but it was tied at ${a} at halftime.`,
			};
		}
		return {
			state: {
				who: "tie",
				led: `were tied at ${a} at halftime`,
				fronted: `Tied at ${a} at halftime`,
			},
		};
	}

	// The side behind at the break had been well in front before it.
	if (yPeak >= 9 && yAt !== undefined && yAt <= h) {
		const said = yToldPeak;
		tellPeak(false);
		return {
			sentence: said
				? pick(
						rng,
						[
							`${cap(X)} had wiped out all of that by halftime, when they led ${hs}.`,
							`By halftime ${X} had turned it around and led ${hs}.`,
							`${cap(X)} were back in front by halftime, ${hs}.`,
						],
						"fh:flipTold",
					)
				: pick(
						rng,
						[
							`${cap(Y)} led by as many as ${yPeak} ${inP(f, yAt)}, but ${X} were in front ${hs} by halftime.`,
							`${cap(X)} trailed by as many as ${yPeak} ${inP(f, yAt)} and still led ${hs} at halftime.`,
							`${cap(Y)} built ${aNum(yPeak)}-point lead ${inP(f, yAt)}, only for ${X} to take ${aFigure(hs)} lead into the break.`,
						],
						"fh:flip",
					),
			subject: xw,
		};
	}

	// A run that made the half.
	if (run && runIsX && Math.abs(mh) >= 6 && runSpanOf(f) !== "") {
		const span = runSpanOf(f);
		return {
			sentence: pick(
				rng,
				[
					`${cap(X)} ${runDid(f, told, rng, span)} and led ${hs} at halftime.`,
					`${cap(X)} ${runDid(f, told, rng, span)} on the way to ${aFigure(hs)} halftime lead.`,
				],
				"fh:runX",
			).replace(/,, /, ", "),
			subject: xw,
		};
	}
	if (
		run &&
		!runIsX &&
		run.period === run.endPeriod &&
		run.period === 2 &&
		q1 &&
		Math.sign(q1.m) === Math.sign(mh) &&
		Math.abs(q1.m) >= 10 &&
		f.n === 4
	) {
		// A second-quarter run into a lead built in the first: in order.
		told.run = true;
		return {
			sentence: `${cap(X)} led ${q1.s} after one quarter, and though ${Y} ran off ${run.pts} straight points in the second, ${X} still led ${hs} at halftime.`,
			subject: xw,
		};
	}
	if (run && !runIsX && run.period === run.endPeriod) {
		told.run = true;
		const span = inP(f, run.period);
		return {
			sentence: pick(
				rng,
				[
					`${cap(Y)} ran off ${run.pts} straight points ${span}, but ${X} led ${hs} at halftime.`,
					`${cap(X)} absorbed ${aNum(run.pts)}-0 run ${span} and still led ${hs} at halftime.`,
					`${cap(Y)} put together ${aNum(run.pts)}-0 run ${span} and still trailed ${hs} at halftime.`,
				],
				"fh:runY",
			),
			subject: xw,
		};
	}

	// A lead that was bigger before the break than at it.
	if (
		xPeak >= 12 &&
		xPeak >= Math.abs(mh) + 6 &&
		xAt !== undefined &&
		xAt <= h &&
		!xToldPeak
	) {
		tellPeak(true);
		return {
			sentence: pick(
				rng,
				[
					`${cap(X)} led by as many as ${xPeak} ${inP(f, xAt)}, but ${Y} had it down to ${hs} at halftime.`,
					`${cap(X)} were up by as many as ${xPeak} ${inP(f, xAt)} before ${Y} cut it to ${hs} at the half.`,
				],
				"fh:shrunk",
			),
			subject: xw,
		};
	}

	// A first quarter that set the tone, or one the second undid.
	if (q1 && f.n === 4 && !told.quarters.has(1)) {
		const sameLeader = Math.sign(q1.m) === Math.sign(mh);
		if (Math.abs(q1.m) >= 12 && sameLeader) {
			return {
				sentence: pick(
					rng,
					[
						`${cap(X)} led ${q1.s} after one quarter and ${hs} at halftime.`,
						`${cap(X)} jumped out to ${aFigure(q1.s)} lead after one quarter and were up ${hs} at the half.`,
						`${cap(X)} were ahead ${q1.s} after one quarter and ${hs} at the half.`,
						`It was ${q1.s} after one quarter and ${hs} at halftime, ${X} in front both times.`,
						`${cap(X)} took ${aFigure(q1.s)} lead out of the first quarter and led ${hs} at halftime.`,
					],
					"fh:q1",
				),
				subject: xw,
			};
		}
		if (
			Math.abs(q1.m) >= 7 &&
			!sameLeader &&
			q1.m !== 0 &&
			!told.quarters.has(2)
		) {
			const q2x = mh >= 0 ? shape.wq[1]! : shape.lq[1]!;
			const q2y = mh >= 0 ? shape.lq[1]! : shape.wq[1]!;
			told.quarters.add(2);
			return {
				sentence: pick(
					rng,
					[
						`${cap(Y)} led ${q1.s} after one quarter, but ${X} won the second ${q2x}-${q2y} and led ${hs} at halftime.`,
						`${cap(X)} trailed ${q1.s} after one quarter, then took the second ${q2x}-${q2y} to go into halftime up ${hs}.`,
					],
					"fh:q1flip",
				),
				subject: xw,
			};
		}
	}

	// The plain state at the break, said more than one way across a night.
	// Only "led" and "were up" can carry a second number on the same verb
	// ("...and 83-75 after three quarters").
	const led = pick(
		rng,
		[
			{ text: `led ${hs} at halftime`, bare: true },
			{ text: `were up ${hs} at halftime`, bare: true },
			{ text: `took ${aFigure(hs)} lead into halftime`, bare: false },
			{ text: `led ${hs} at the half`, bare: true },
			{ text: `went into halftime ahead ${hs}`, bare: false },
		],
		"fh:state",
	);
	return {
		state: {
			who: xw,
			led: led.text,
			bare: led.bare,
			fronted: mh >= 0 ? `Up ${hs} at halftime` : `Down ${hs} at halftime`,
		},
	};
};

// The winners' biggest lead, when it came after the break and says more
// than the final does.
const latePeak = (
	f: Facts,
	told: Told,
): { n: number; at: number } | undefined =>
	!told.wPeak &&
	f.wp >= f.shape.margin + 4 &&
	f.wp >= 12 &&
	f.wpAt !== undefined &&
	f.halfAt !== undefined &&
	f.wpAt > f.halfAt &&
	f.wpAt <= f.n
		? { n: f.wp, at: f.wpAt }
		: undefined;

// The second half: what the break led to. Written against the first half's
// clause, so the two read as one thought where they are one.
const secondHalf = (f: Facts, half: Half, told: Told, rng: Rng): string[] => {
	const { W, L, shape, n } = f;
	const h = f.halfAt;
	const st = half.state;
	if (h === undefined || f.mh === undefined || f.m3 === undefined || n !== 4) {
		// A game of halves, or of some other number of periods: the break and
		// what came after it, plainly.
		if (half.sentence) {
			return [half.sentence];
		}
		if (st && h !== undefined) {
			const a = f.ws[n - 1]! - f.ws[h - 1]!;
			const b = f.ls[n - 1]! - f.ls[h - 1]!;
			const X = st.who === "l" ? L : W;
			// The lede may have given the second half's score already.
			const said = told.secondHalf || (n === 2 && told.quarters.has(2));
			if (st.who === "tie") {
				return [
					a > b && !said
						? `The teams ${st.led}, and ${W} won the second half ${a}-${b}.`
						: `The teams ${st.led}.`,
				];
			}
			return [
				said || a <= b
					? `${cap(X)} ${st.led}.`
					: st.who === "l"
						? `${cap(X)} ${st.led}, but ${W} won the second half ${a}-${b}.`
						: `${cap(X)} ${st.led} and won the second half ${a}-${b}.`,
			];
		}
		return [];
	}
	const out: string[] = [];
	const mh = f.mh;
	const m3 = f.m3;
	const a3 = shape.wq[2]!;
	const b3 = shape.lq[2]!;
	const a4 = shape.wq[3]!;
	const b4 = shape.lq[3]!;
	const s3 = sc2(f.ws[2]!, f.ls[2]!);
	const sa = a3 + a4;
	const sb = b3 + b4;
	const d3 = a3 - b3;
	const d4 = a4 - b4;
	const told3 = told.quarters.has(3);
	const told4 = told.quarters.has(4);
	// The winners' run, when it came after the break.
	const runW =
		f.run &&
		f.run.mine &&
		f.run.endPeriod > h &&
		f.run.period <= n &&
		!told.run &&
		runIsNews(f)
			? f.run
			: undefined;
	// One in the fourth quarter alone decided something.
	const run4 =
		runW && runW.period === 4 && runW.endPeriod === 4 ? runW : undefined;
	const peak = latePeak(f, told);
	// Never headed after three: the log's last lead change came before the
	// fourth.
	const last = f.game.flow?.lastLead;
	const neverHeaded4 =
		!!last && last.side === f.wSide && last.period <= 3 && f.reg > 0;

	// Where it stood after three, said more than one way across a night:
	// after "led 52-46 at halftime and", and as a clause of its own.
	const afterThree = pick(
		rng,
		[
			{
				bare: `${s3} after three quarters`,
				full: `led ${s3} after three quarters`,
			},
			{
				bare: `${s3} through three quarters`,
				full: `were up ${s3} through three quarters`,
			},
			{
				bare: `${s3} going into the fourth`,
				full: `led ${s3} going into the fourth`,
			},
			{
				bare: `${s3} with a quarter to play`,
				full: `were in front ${s3} with a quarter to play`,
			},
		],
		"sh:afterThree",
	);

	if (half.sentence) {
		out.push(half.sentence);
	}
	// The subject of a new sentence about one side: "They" when the sentence
	// before left the reader on that side.
	const subj = (who: "w" | "l") =>
		half.sentence && half.subject === who && out.length === 1
			? "They"
			: cap(who === "w" ? W : L);
	// The state at the break joined to what the same side did next ("led
	// 52-46 at halftime and by as many as 20 in the third quarter"), or
	// turned against by the other side.
	const join = (
		who: "w" | "l",
		// After "...at halftime and": may lean on the verb "led".
		bare: string,
		// As a sentence of its own, with its verb.
		full: string,
	): string => {
		if (st && st.who === who) {
			return `${cap(who === "w" ? W : L)} ${st.led} and ${st.bare ? bare : full}.`;
		}
		if (st && st.who === "tie") {
			return `The teams ${st.led}, and ${who === "w" ? W : L} ${full}.`;
		}
		if (st) {
			return `${cap(st.who === "w" ? W : L)} ${st.led}, but ${who === "w" ? W : L} ${full}.`;
		}
		return `${subj(who)} ${full}.`;
	};
	const stateFirst = () => {
		if (st) {
			out.push(
				st.who === "tie"
					? `The teams ${st.led}.`
					: `${cap(st.who === "w" ? W : L)} ${st.led}.`,
			);
		}
	};

	// --- Overtime: how regulation came to end level. ---
	if (shape.ot > 0) {
		const tied = f.ws[n - 1]!;
		if (m3 >= 6) {
			out.push(
				join(
					"w",
					`by ${m3} going into the fourth`,
					`were up ${m3} going into the fourth`,
				),
				pick(
					rng,
					[
						`${cap(L)} won the fourth quarter ${b4}-${a4} to wipe all of it out.`,
						`${cap(L)} erased it with ${aFigure(`${b4}-${a4}`)} fourth quarter, and regulation ended tied at ${tied}.`,
					],
					"sh:otBlown",
				),
			);
			told.quarters.add(4);
		} else if (m3 <= -6) {
			out.push(
				join(
					"l",
					`by ${-m3} going into the fourth`,
					`were up ${-m3} going into the fourth`,
				),
				pick(
					rng,
					[
						`${cap(W)} won the fourth quarter ${a4}-${b4} to pull level.`,
						`${cap(W)} took the fourth quarter ${a4}-${b4}, and regulation ended tied at ${tied}.`,
					],
					"sh:otRally",
				),
			);
			told.quarters.add(4);
		} else {
			stateFirst();
			out.push(
				pick(
					rng,
					[
						`Neither team could shake the other after the break, and regulation ended tied at ${tied}.`,
						`It was ${m3 === 0 ? `tied at ${f.ws[2]}` : `${aNumWord(Math.abs(m3))}-point game`} going into the fourth, and still level when regulation ran out.`,
					],
					"sh:otTight",
				),
			);
		}
		return out;
	}

	// --- The winners trailed going into the fourth: the last quarter is the
	// game. ---
	if (m3 < 0) {
		const man = told4 ? "" : quarterMan(f, 4, shape.winner, told, true);
		// What the winners did with it.
		const did = run4
			? told4
				? `${runDid(f, told, rng, "")} in the fourth`
				: `${runDid(f, told, rng, "")} and won the fourth quarter ${a4}-${b4}`
			: told4
				? "took over in the fourth"
				: pick(
						rng,
						[
							`won the final quarter ${a4}-${b4}${man}`,
							`outscored them ${a4}-${b4} the rest of the way${man}`,
							`had the last quarter, ${a4}-${b4}${man}`,
						],
						"sh:trailedAfter3",
					);
		// After "...going into the fourth, where": the quarter is named.
		const didThere = did
			.replace(/ in the fourth$/, "")
			.replace(/ and won the fourth quarter /, " and won the quarter ")
			.replace(/, with (.+?) in the quarter$/, ", with $1")
			.replace(
				/^won the final quarter|^had the last quarter,/,
				"won the quarter",
			)
			.replace(
				/^outscored them (\d+-\d+) the rest of the way/,
				"won the quarter $1",
			);
		if (st && st.who === "l") {
			out.push(
				pick(
					rng,
					[
						`${cap(L)} ${st.led} and ${st.bare ? "" : "led "}${s3} after three quarters, but ${W} ${did}.`,
						`${cap(L)} ${st.led} and were still up ${s3} going into the fourth, where ${W} ${didThere}.`,
						`${st.fronted.replace(/^Up/, "Down")} and ${s3} after three quarters, ${W} ${did}.`,
					],
					"sh:lateComeback",
				),
			);
		} else if (mh >= 0) {
			// Ahead or level at the break, behind after three: the third
			// quarter was the losers', and the fourth the answer.
			// Not "took a 64-61 lead into halftime, but the Pacers took...".
			const third = `${st && /took /.test(st.led) ? "won" : "took"} the third quarter ${b3}-${a3} to ${mh === 0 ? "lead" : "go ahead"} ${s3}`;
			const answer = did.replace("outscored them", `outscored ${L}`);
			if (st) {
				out.push(
					st.who === "tie"
						? `The teams ${st.led}, and ${L} ${third}.`
						: `${cap(W)} ${st.led}, but ${L} ${third}.`,
					`${cap(W)} ${answer}.`,
				);
			} else {
				out.push(`${cap(L)} ${third}, but ${W} ${answer}.`);
			}
			told.quarters.add(3);
		} else {
			out.push(
				pick(
					rng,
					[
						`${subj("l")} still led ${s3} going into the fourth, but ${W} ${didThere}.`,
						`${subj("l")} were still up ${s3} after three quarters before ${W} ${did}.`,
					],
					"sh:lateComebackAfter",
				),
			);
		}
		told.quarters.add(4);
		return out.map((t) => t.replace(/,\./, "."));
	}

	// --- Level going into the fourth. ---
	if (m3 === 0) {
		stateFirst();
		const man = told4 ? "" : quarterMan(f, 4, shape.winner, told, true);
		const tied = f.ws[2]!;
		out.push(
			run4
				? `It was tied at ${tied} going into the fourth, where ${W} ${runDid(f, told, rng, "")}${told4 ? "" : ` and won the quarter ${a4}-${b4}`}.`
				: told4
					? `It was tied at ${tied} going into the fourth.`
					: pick(
							rng,
							[
								`It was tied at ${tied} going into the fourth, and ${W} won the final quarter ${a4}-${b4}${man}.`,
								`Tied at ${tied} going into the fourth, ${W} took the final quarter ${a4}-${b4}${man}.`,
							],
							"sh:tied3",
						),
		);
		told.quarters.add(4);
		return out;
	}

	// From here the winners led after three.

	// What the fourth added to a lead taken in the third.
	const fourthTail = (): string => {
		if (f.close) {
			return "";
		}
		if (!told4 && d4 >= 8) {
			told.quarters.add(4);
			return `, then won the fourth ${a4}-${b4}`;
		}
		if (peak && peak.at === 4) {
			told.wPeak = true;
			return ` and led by as many as ${peak.n} in the fourth`;
		}
		return neverHeaded4 ? " and stayed there" : "";
	};

	// --- Behind or level at the break: the third quarter turned it. ---
	if (mh <= 0) {
		if (told3) {
			// The lede gave the quarter; this says where it left them.
			const after = fourthTail();
			out.push(
				st && st.who === "l"
					? `${cap(L)} ${st.led} and trailed by ${m3} when the third quarter ended${after.replace(/^, then won/, `. ${cap(W)} then won`).replace(/^ and (led|stayed)/, `. ${cap(W)} $1`)}.`
					: st
						? `The teams ${st.led}, and ${W} were up ${m3} after three quarters${after}.`
						: `${cap(W)} were up ${m3} after three quarters${after}.`,
			);
			return out;
		}
		told.quarters.add(3);
		const tail4 = fourthTail();
		// The man of the quarter stays next to the quarter: with the fourth's
		// score after it, "scoring 9 of his 20 in the quarter" reads as the
		// fourth. So it is the fourth's score or the man, not both.
		// The run that turned it, when there was one in the quarter.
		const run3 =
			runW && runW.period === 3 && runW.endPeriod === 3 ? runW : undefined;
		const manT =
			run3 || /won the fourth/.test(tail4)
				? ""
				: quarterMan(f, 3, shape.winner, told, true);
		const then = manT === "" ? tail4 : "";
		// "...and the lead with it and stayed there" is one "and" too many.
		const thenPlain = /^ and /.test(then) ? "" : then;
		if (run3) {
			const did3 = `${runDid(f, told, rng, "in the third quarter")} and won the quarter ${a3}-${b3} to go in front${thenPlain}`;
			out.push(
				st && st.who === "l"
					? `${cap(L)} ${st.led}, but ${W} ${did3}.`
					: st
						? `The teams ${st.led}, and ${W} ${did3}.`
						: `${cap(W)} ${did3}.`,
			);
		} else if (st && st.who === "l") {
			out.push(
				pick(
					rng,
					[
						`${cap(L)} ${st.led}, but ${W} won the third quarter ${a3}-${b3} to go in front${then}${manT}.`,
						`${cap(L)} ${st.led} before ${W} took the third quarter ${a3}-${b3} and the lead with it${thenPlain}${manT}.`,
						`${st.fronted.replace(/^Up/, "Down")}, ${W} outscored ${L} ${a3}-${b3} in the third quarter to take the lead${then}${manT}.`,
					],
					"sh:turned3",
				),
			);
		} else if (st) {
			out.push(
				`The teams ${st.led}, and ${W} won the third quarter ${a3}-${b3}${then}${manT}.`,
			);
		} else {
			out.push(
				pick(
					rng,
					[
						`${cap(W)} went in front with ${aFigure(`${a3}-${b3}`)} third quarter${then}${manT}.`,
						`${cap(W)} won the third quarter ${a3}-${b3} to take the lead${then}${manT}.`,
					],
					"sh:turned3After",
				),
			);
		}
		return out;
	}

	// From here the winners led at the break and after three.

	// --- The lead all but went in the third, and the fourth won it. ---
	if (m3 <= Math.min(mh - 5, 4) && f.reg >= 6 && !f.close) {
		const man = told4 ? "" : quarterMan(f, 4, shape.winner, told, true);
		const shrink = m3 === 1 ? "a single point" : `${numWord(m3)} points`;
		const reassert = run4
			? runDid(f, told, rng, "in the fourth")
			: told4
				? "took over in the fourth"
				: pick(
						rng,
						[
							`won the fourth ${a4}-${b4}${man}`,
							`pulled away with ${aFigure(`${a4}-${b4}`)} fourth quarter${man}`,
							`outscored them ${a4}-${b4} in the fourth${man}`,
						],
						"sh:reassert",
					);
		// ...and went in front in the fourth, when the log says so.
		if (f.lp > 0 && f.lpAt === 4) {
			told.lPeak = true;
			const X = st ? cap(W) : subj("w");
			out.push(
				`${st ? `${X} ${st.led}, but ${L}` : cap(L)} had the lead down to ${shrink} by the end of the third quarter and went ahead by as many as ${f.lp} in the fourth.`,
				told4
					? `${cap(W)} took it back.`
					: `${cap(W)} won the quarter anyway, ${a4}-${b4}.`,
			);
			told.quarters.add(4);
			return out;
		}
		if (st) {
			out.push(
				...pick(
					rng,
					[
						[
							`${cap(W)} ${st.led}, but ${L} had the lead down to ${shrink} by the end of the third quarter.`,
							`${cap(W)} ${reassert.replace("outscored them", `outscored ${L}`)}.`,
						],
						[
							`${cap(W)} ${st.led} and saw the lead shrink to ${shrink} after three quarters, then ${reassert}.`,
						],
						[
							`${st.fronted}, ${W} let ${L} back within ${shrink} by the end of the third quarter before they ${reassert}.`,
						],
					],
					"sh:shrunk3",
				),
			);
		} else {
			out.push(
				`${cap(L)} had the lead down to ${shrink} by the end of the third quarter, but ${W} ${reassert}.`,
			);
		}
		told.quarters.add(4);
		return out.map((t) => t.replace(/,\.$/, "."));
	}

	// --- A narrow lead after three, and the fourth put it away. ---
	if (m3 <= 5 && f.reg >= 9 && !f.close) {
		const man = told4 ? "" : quarterMan(f, 4, shape.winner, told, true);
		const finish = run4
			? `put it away when they ${runDid(f, told, rng, "in the fourth")}`
			: told4
				? ""
				: pick(
						rng,
						[
							`pulled away with ${aFigure(`${a4}-${b4}`)} fourth quarter${man}`,
							`won the fourth ${a4}-${b4}${man}`,
						],
						"sh:put-away",
					);
		const just = m3 === 1 ? "a point" : numWord(m3);
		out.push(
			join(
				"w",
				`by only ${just} after three quarters${finish ? `, then ${finish}` : ""}`,
				`were up only ${just} after three quarters${finish ? `, then ${finish}` : ""}`,
			),
		);
		told.quarters.add(4);
		return out.map((t) => t.replace(/,\.$/, "."));
	}

	// The winners led after three and still had to take the lead back: the
	// log's last lead change is theirs, in the last quarter.
	const retook =
		!!last &&
		last.side === f.wSide &&
		last.period === n &&
		f.lp > 0 &&
		shape.margin <= 9;
	const retakeSentence = (): string | undefined => {
		if (!retook || !last) {
			return undefined;
		}
		const left = clockLeft(last.clock);
		const who = nameOfPid(f.game, last.pid);
		// Inside the last two minutes the finish tells it, basket by basket.
		if (!left || last.clock <= 120) {
			return undefined;
		}
		told.retake = true;
		return pick(
			rng,
			[
				`${cap(L)} came all the way back to lead in the fourth before ${W} went ahead for good with ${left} left${who ? `, on a basket by ${who}` : ""}.`,
				`${cap(L)} took the lead in the fourth, and ${W} did not get it back for good until ${who ? `${poss(who)} basket with ${left} left` : `${left} was left`}.`,
			],
			"sh:retake",
		);
	};

	// --- A game still alive at the end: the story is the finish, and this
	// only sets it up. ---
	if (f.close) {
		const pk =
			!told.wPeak && f.wp >= 12 && f.wp >= f.reg + 7 && f.wpAt !== undefined
				? { n: f.wp, at: f.wpAt }
				: undefined;
		if (pk && pk.at > h) {
			told.wPeak = true;
			out.push(
				join(
					"w",
					`by as many as ${pk.n} ${inP(f, pk.at)}`,
					`were up by as many as ${pk.n} ${inP(f, pk.at)}`,
				),
				retakeSentence() ??
					(!told4 && d4 <= -6
						? pick(
								rng,
								[
									`${cap(L)} took the fourth quarter ${b4}-${a4} to make a game of it.`,
									`${cap(L)} won the fourth quarter ${b4}-${a4} and very nearly caught them.`,
									`A ${b4}-${a4} fourth quarter got ${L} back into it.`,
								],
								"sh:their-fourth",
							)
						: pick(
								rng,
								[
									`${cap(L)} would not go away.`,
									`${cap(L)} kept coming.`,
									`It did not last.`,
									`${cap(L)} were not done.`,
								],
								"sh:kept-coming",
							)),
			);
			if (!told.retake && !told4 && d4 <= -6) {
				told.quarters.add(4);
			}
			return out;
		}
		if (pk && pk.n >= m3 + 5) {
			told.wPeak = true;
			out.push(
				join(
					"w",
					`by ${m3} going into the fourth, having been up by as many as ${pk.n} ${inP(f, pk.at)}`,
					`were up ${m3} going into the fourth, having led by as many as ${pk.n} ${inP(f, pk.at)}`,
				),
			);
			if (run4) {
				out.push(
					`${cap(W)} ${runDid(f, told, rng, "in the fourth")}.`.replace(
						/,\.$/,
						".",
					),
				);
			} else if (!told4 && d4 <= -6 && m3 >= 6) {
				told.quarters.add(4);
				out.push(
					pick(
						rng,
						[
							`${cap(L)} took the fourth quarter ${b4}-${a4} to make a game of it.`,
							`${cap(L)} won the fourth quarter ${b4}-${a4} and very nearly caught them.`,
							`A ${b4}-${a4} fourth quarter got ${L} back into it.`,
						],
						"sh:their-fourth",
					),
				);
			}
			return out;
		}
		// The run that won it, when the last two minutes held nothing.
		const then = run4 ? `, then ${runDid(f, told, rng, "in the fourth")}` : "";
		if (st || then || Math.abs(m3 - mh) >= 5 || (told.wPeak && m3 >= 6)) {
			out.push(
				join(
					"w",
					`${afterThree.bare}${then}`,
					`${
						m3 <= mh - 4
							? `were still in front, ${s3}, going into the fourth`
							: afterThree.full
					}${then}`,
				),
			);
		}
		const back = then === "" ? retakeSentence() : undefined;
		if (back) {
			out.push(back);
		} else if (then === "" && !told4 && d4 <= -6 && m3 >= 6) {
			// A lead that was still real after three and nearly gone at the
			// end: the quarter it went in.
			told.quarters.add(4);
			out.push(
				pick(
					rng,
					[
						`${cap(L)} took the fourth quarter ${b4}-${a4} to make a game of it.`,
						`${cap(L)} won the fourth quarter ${b4}-${a4} and very nearly caught them.`,
						`A ${b4}-${a4} fourth quarter got ${L} back into it.`,
					],
					"sh:their-fourth",
				),
			);
		}
		return out.map((t) => t.replace(/,\.$/, "."));
	}

	// --- In control: how far it got. ---
	if (runW && runSpanOf(f) !== "") {
		const span = runSpanOf(f);
		// A run across the break is its own sentence after the halftime
		// score: "were up 56-47 at halftime and then went on a 17-0 run
		// either side of halftime" says halftime twice and gets the order
		// wrong.
		const acrossBreak = runW.period <= h;
		if (acrossBreak) {
			stateFirst();
		}
		if (acrossBreak || (!st && !runW.top && rng() < 0.6)) {
			told.run = true;
			out.push(
				pick(
					rng,
					acrossBreak
						? [
								`${cap(W)} went on ${aNum(runW.pts)}-0 run on either side of the break.`,
								`${cap(aNum(runW.pts))}-0 ${nick(shape.winner)} run straddled the break.`,
							]
						: shape.margin >= 12
							? [
									`${cap(aNum(runW.pts))}-0 run ${span} put it out of reach.`,
									`${cap(aNum(runW.pts))}-0 run ${span} finished it.`,
								]
							: [
									`${cap(W)} went on ${aNum(runW.pts)}-0 run ${span}.`,
									`${cap(aNum(runW.pts))}-0 run ${span} gave ${W} room.`,
								],
					acrossBreak ? "sh:controlRunBreak" : "sh:controlRunSubject",
				),
			);
		} else {
			out.push(
				join(
					"w",
					`${runDid(f, told, rng, span)} to put it away`.replace(
						/^(ran|went|scored|reeled)/,
						"then $1",
					),
					`${runDid(f, told, rng, span)} to put it away`,
				),
			);
		}
	} else if (!told3 && d3 >= 8) {
		told.quarters.add(3);
		// One draw, said after "...at halftime and" or on its own.
		const third = pick(
			rng,
			[
				() =>
					`won the third quarter ${a3}-${b3} to take ${aNum(m3)}-point lead into the fourth`,
				() =>
					`outscored ${L} ${a3}-${b3} in the third quarter to go up ${m3}${quarterMan(f, 3, shape.winner, told, true)}`,
				() =>
					`stretched the lead to ${m3} with ${aFigure(`${a3}-${b3}`)} third quarter${quarterMan(f, 3, shape.winner, told, true)}`,
			],
			"sh:control3",
		)();
		out.push(join("w", third, third));
	} else if (peak) {
		told.wPeak = true;
		out.push(
			join(
				"w",
				pick(
					rng,
					[
						`by as many as ${peak.n} ${inP(f, peak.at)}`,
						`pushed the lead to ${peak.n} ${inP(f, peak.at)}`,
						`were up by as many as ${peak.n} ${inP(f, peak.at)}`,
					],
					"sh:controlPeak",
				),
				pick(
					rng,
					[
						`led by as many as ${peak.n} ${inP(f, peak.at)}`,
						`pushed the lead to ${peak.n} ${inP(f, peak.at)}`,
						`were up by as many as ${peak.n} ${inP(f, peak.at)}`,
					],
					"sh:controlPeakFull",
				),
			),
		);
	} else if (!told.secondHalf && sa - sb >= 8 && !told3 && !told4) {
		told.secondHalf = true;
		out.push(
			join(
				"w",
				pick(
					rng,
					[
						`outscored ${L} ${sa}-${sb} after the break`,
						`won the second half ${sa}-${sb}`,
					],
					"sh:control2h",
				),
				pick(
					rng,
					[
						`outscored ${L} ${sa}-${sb} after the break`,
						`won the second half ${sa}-${sb}`,
					],
					"sh:control2h",
				),
			),
		);
	} else if (st) {
		out.push(
			join(
				"w",
				m3 === mh ? `by the same ${m3} after three quarters` : afterThree.bare,
				m3 === mh
					? `were still up ${m3} after three quarters`
					: afterThree.full,
			),
		);
	}
	// How big it got, in a game that got big: "led by as many as 38" is in
	// every wire story of a rout.
	if (
		!told.wPeak &&
		shape.margin >= 15 &&
		f.wp >= shape.margin + 4 &&
		!(f.wpAt !== undefined && f.wpAt <= h) &&
		out.length < 3
	) {
		told.wPeak = true;
		const at = f.wpAt !== undefined && f.wpAt <= n ? ` ${inP(f, f.wpAt)}` : "";
		out.push(
			pick(
				rng,
				[
					`The lead reached ${f.wp}${at}.`,
					`They were up by as many as ${f.wp}${at}.`,
				],
				"sh:blowoutPeak",
			),
		);
	}
	// The losers made the final closer than the game was.
	if (mh - f.reg >= 8 && sb - sa >= 8 && !told3 && !told4) {
		told.secondHalf = true;
		out.push(
			pick(
				rng,
				[
					`${cap(L)} won the second half ${sb}-${sa}, which only made the final closer.`,
					`${cap(L)} outscored them ${sb}-${sa} after the break to make the final respectable.`,
				],
				"sh:garbageHalf",
			),
		);
	} else if (
		peak &&
		told.wPeak &&
		peak.n >= shape.margin + 10 &&
		d4 <= -8 &&
		!told4 &&
		out.length < 3
	) {
		out.push(
			pick(
				rng,
				[
					`${cap(L)} won the fourth quarter ${b4}-${a4}, long after it mattered.`,
					`${cap(L)} took the fourth quarter ${b4}-${a4} to make the final closer than the game was.`,
					`A ${b4}-${a4} fourth quarter made the final respectable for ${L}.`,
				],
				"sh:garbage",
			),
		);
		told.quarters.add(4);
	}
	return out.map((t) => t.replace(/,\.$/, "."));
};

// A line to open the story with, said only when the numbers leave no doubt
// about it - and not of every game that qualifies.
const thesis = (f: Facts, told: Told, rng: Rng): string | undefined => {
	const { W, L, shape, n } = f;
	if (n !== 4 || f.mh === undefined || f.m3 === undefined || rng() < 0.35) {
		return undefined;
	}
	const flow = f.game.flow;
	// Three quarters the losers' way.
	if (
		shape.ot === 0 &&
		f.mh <= -8 &&
		f.m3 <= -6 &&
		f.cum[0] !== undefined &&
		f.cum[0] <= 0
	) {
		return pick(
			rng,
			[
				`For three quarters it was ${poss(L)} game.`,
				`${cap(L)} had this one for three quarters.`,
				`This was ${poss(L)} game for most of the night.`,
			],
			"thesis:theirs",
		);
	}
	// Over early.
	if (
		!told.wire &&
		f.cum[0] !== undefined &&
		f.cum[0] >= 10 &&
		f.mh >= 15 &&
		f.m3 >= 15 &&
		f.lp <= 3
	) {
		return pick(
			rng,
			[
				`It was over early.`,
				`This one was decided in the first half.`,
				`${cap(W)} settled it before halftime.`,
			],
			"thesis:early",
		);
	}
	// Nobody could get away.
	const biggest = Math.max(f.wp, f.lp);
	if (flow && f.close && biggest <= 9 && flow.leadChanges >= 8) {
		return pick(
			rng,
			[
				`Neither team led by more than ${biggest}, and the lead changed hands ${flow.leadChanges} times.`,
				`Nobody could get away: the lead changed hands ${flow.leadChanges} times, and neither team led by more than ${biggest}.`,
				`There were ${flow.leadChanges} lead changes, and neither team led by more than ${biggest}.`,
			],
			"thesis:seesaw",
		);
	}
	// Level for three quarters, then not.
	if (
		!f.close &&
		Math.abs(f.mh) <= 3 &&
		Math.abs(f.m3) <= 3 &&
		f.reg >= 9 &&
		biggest > 0 &&
		Math.max(f.lp, 0) <= 9
	) {
		return pick(
			rng,
			[
				`There was nothing in it for three quarters.`,
				`Three quarters settled nothing.`,
				`It was anybody's game going into the fourth.`,
			],
			"thesis:level",
		);
	}
	return undefined;
};

export const composeStory = (input: StoryInput, told: Told): string[] => {
	const { game, shape, rng } = input;
	const f = factsOf(game, shape);
	if (f.cum.length === 0) {
		return [];
	}
	const out: string[] = [];
	const open = thesis(f, told, rng);
	if (open) {
		out.push(open);
	}
	const seesaw = !!open && /lead change|changed hands/.test(open);
	const half = firstHalf(f, told, rng);
	const body = secondHalf(f, half, told, rng);
	// Straight after a line about the losers, they are "they".
	if (open && body[0] && open.includes(nick(shape.loser))) {
		const Lcap = `${cap(f.L)} `;
		if (body[0].startsWith(Lcap) && !open.includes(nick(shape.winner))) {
			body[0] = `They ${body[0].slice(Lcap.length)}`;
		}
	}
	out.push(...body);

	// The finish, for a game that was still one at the end.
	const last = out.at(-1) ?? "";
	const sequence = finishStory(
		{
			game,
			winner: shape.winner,
			loser: shape.loser,
			regPeriods: shape.regPeriods,
			shotTold: told.shot,
			justNamed: undefined,
			afterItWas: /^It was\b/.test(last),
			losersJustNamed: last.includes(nick(shape.loser)),
		},
		rng,
	);
	out.push(...sequence);
	const notes = finishNote(
		game,
		shape,
		rng,
		told.shot,
		undefined,
		sequence.length > 0,
	)
		.filter(
			(t) =>
				!(
					(seesaw || Math.max(f.wp, f.lp) >= 12) &&
					/lead change|changed hands|led by more than|biggest lead/.test(t)
				),
		)
		// The retake has been told, with its clock.
		.filter((t) => !(told.retake && /for good|for the last time/.test(t)))
		// A lead taken for good in the third is the third quarter's story,
		// which the turn has told: after "led by as many as 21 in the fourth"
		// it runs the clock backwards.
		.filter(
			(t) =>
				!(
					/for good|for the last time/.test(t) &&
					/in the (?:first|second|third)\b/.test(t) &&
					/fourth/.test(out.join(" "))
				),
		);
	out.push(...notes);
	// A game close late whose last two minutes held nothing to tell: at
	// least how the fourth quarter went.
	if (
		f.close &&
		f.n === 4 &&
		shape.ot === 0 &&
		sequence.length === 0 &&
		notes.length === 0 &&
		!told.shot &&
		!told.run &&
		!told.quarters.has(4) &&
		f.m3 !== undefined &&
		f.m3 > 0 &&
		shape.wq[3]! - shape.lq[3]! >= 4
	) {
		told.quarters.add(4);
		out.push(
			pick(
				rng,
				[
					`${cap(f.W)} won the fourth quarter ${shape.wq[3]}-${shape.lq[3]} to close it out.`,
					`The fourth quarter went to ${f.W}, ${shape.wq[3]}-${shape.lq[3]}.`,
					`${cap(f.W)} took the fourth quarter ${shape.wq[3]}-${shape.lq[3]} to finish it.`,
				],
				"story:closeFourth",
			),
		);
	}
	// A game close at the end whose last two minutes held nothing to tell:
	// how close it still was.
	const two = game.flow?.late?.find((m) => m.clock === 120);
	if (
		f.close &&
		shape.ot === 0 &&
		sequence.length === 0 &&
		notes.length === 0 &&
		!told.shot &&
		!told.run &&
		!told.quarters.has(4) &&
		two
	) {
		const gap = two.pts[f.wSide]! - two.pts[f.lSide]!;
		if (gap > 0 && gap <= 5 && shape.margin > gap) {
			out.push(
				pick(
					rng,
					[
						`It was still ${gap === 1 ? "a one-point" : `a ${numWord(gap)}-point`} game with two minutes left.`,
						`${cap(f.L)} were still within ${gap === 1 ? "a point" : numWord(gap)} with two minutes to play.`,
					],
					"story:lateGap",
				),
			);
		}
	}

	// A STORY THAT REACHES THE BREAK GOES PAST IT. "The Suns were up 1 at
	// the half, 56-55." and nothing after it, in a game the Suns won by seven.
	// Whatever else there was or was not to tell, where it stood going into
	// the last quarter is always there.
	if (f.n === 4 && f.m3 !== undefined && shape.ot === 0) {
		const at = out.findIndex((t) =>
			/halftime|at the half\b|at the break|into the break/.test(t),
		);
		const AFTER =
			/second half|after the break|after halftime|(?:after|through) three|quarter to play|going into the|fourth|third|with \d|seconds|for good|pulled away|from there|rest of the way/i;
		// What the story says from the halftime score on.
		const fromBreak =
			at < 0
				? ""
				: [
						out[at]!.replace(
							/^.*?(?:halftime|at the half\b|at the break|into the break)/,
							"",
						),
						...out.slice(at + 1),
					].join(" ");
		if (at >= 0 && !AFTER.test(fromBreak)) {
			const s3 = sc2(f.ws[2]!, f.ls[2]!);
			out.push(
				f.m3 === 0
					? `It was tied at ${f.ws[2]} going into the fourth.`
					: pick(
							rng,
							[
								`${cap(f.m3 > 0 ? f.W : f.L)} were up ${s3} going into the fourth.`,
								`It was ${s3} ${nick(f.m3 > 0 ? shape.winner : shape.loser)} going into the fourth.`,
								`${cap(f.m3 > 0 ? f.W : f.L)} led ${s3} after three quarters.`,
								`After three quarters ${f.m3 > 0 ? f.W : f.L} were ahead ${s3}.`,
								`${cap(f.m3 > 0 ? f.W : f.L)} took ${aFigure(s3)} lead into the fourth.`,
								`${cap(f.m3 > 0 ? f.W : f.L)} were in front ${s3} with a quarter to play.`,
							],
							"story:intoFourth",
						),
			);
		}
	}

	// Overtime that nothing above told: who took the extra period.
	if (shape.ot > 0) {
		let a = 0;
		let b = 0;
		for (let i = f.n; i < shape.wq.length; i++) {
			a += shape.wq[i] ?? 0;
			b += shape.lq[i] ?? 0;
		}
		const said = out.join(" ");
		const extra = shape.ot > 1 ? "the overtimes" : "overtime";
		if (!/overtime|extra period/.test(said) && !told.ot) {
			out.push(`It went to overtime, and ${f.W} won it ${a}-${b}.`);
		} else if (
			a - b >= 6 &&
			!/scored the (?:last|next)|pulled away from there/.test(said)
		) {
			out.push(
				pick(
					rng,
					[
						`${cap(f.W)} outscored ${f.L} ${a}-${b} in ${extra}.`,
						`${cap(f.W)} won ${extra} ${a}-${b}.`,
					],
					"story:otScore",
				),
			);
		}
	}
	claimIn(out.join(" "));
	return out;
};

// --- The casts ---------------------------------------------------------------

// What a team did that explains the result, as a clause to hang on it and
// as a sentence of its own.
type Edge = {
	about: "w" | "l";
	// "shot 35.1% from the field"
	did: string;
	// How much it explains: the heaviest one is told.
	weight: number;
	topic: "fg" | "threes" | "tov" | "ft" | "reb" | "ast" | "balance";
};

const edgesOf = (f: Facts, told: Told, rng: Rng): Edge[] => {
	const { shape, L } = f;
	const w = teamStats(shape.winner);
	const l = teamStats(shape.loser);
	const out: Edge[] = [];
	const add = (e: Edge) => out.push(e);

	if (l.fga >= 40 && l.fgp <= 39.5 && w.fgp - l.fgp >= 5) {
		add({
			about: "l",
			did: pick(
				rng,
				[
					`shot ${l.fgp}% from the field`,
					`shot just ${l.fgp}%`,
					`made only ${l.fgp}% of their shots`,
				],
				"edge:lfg",
			),
			weight: 3 + (40 - l.fgp) * 0.4,
			topic: "fg",
		});
	}
	if (
		l.tpa >= 20 &&
		l.tp / l.tpa <= 0.27 &&
		(w.tpa === 0 || w.tp / w.tpa - l.tp / l.tpa >= 0.06)
	) {
		add({
			about: "l",
			did: pick(
				rng,
				[
					`went ${l.tp}-of-${l.tpa} from three-point range`,
					`missed ${l.tpa - l.tp} of their ${l.tpa} threes`,
					`shot ${l.tp}-of-${l.tpa} from beyond the arc`,
				],
				"edge:l3",
			),
			weight: 3 + (0.27 - l.tp / l.tpa) * 20,
			topic: "threes",
		});
	}
	if (l.tov >= 19 && l.tov - w.tov >= 5) {
		add({
			about: "l",
			did: pick(
				rng,
				[`committed ${l.tov} turnovers`, `turned the ball over ${l.tov} times`],
				"edge:ltov",
			),
			weight: 3 + (l.tov - 19) * 0.5,
			topic: "tov",
		});
	}
	if (w.fta - l.fta >= 20 && w.fta >= 30) {
		add({
			about: "w",
			did: pick(
				rng,
				[
					`went to the line ${w.fta} times to ${poss(L)} ${l.fta}`,
					`shot ${w.fta} free throws to ${poss(L)} ${l.fta}`,
				],
				"edge:ft",
			),
			weight: 3 + (w.fta - l.fta - 20) * 0.2,
			topic: "ft",
		});
	}
	if (w.reb >= 30 && l.reb >= 20 && w.reb - l.reb >= 12) {
		add({
			about: "w",
			did: pick(
				rng,
				[
					`outrebounded ${L} ${w.reb}-${l.reb}`,
					`won the boards ${w.reb}-${l.reb}`,
				],
				"edge:reb",
			),
			weight: 3 + (w.reb - l.reb - 12) * 0.25,
			topic: "reb",
		});
	}
	if (w.fga >= 40 && w.fgp >= 52) {
		add({
			about: "w",
			did: pick(
				rng,
				[
					`shot ${w.fgp}% from the field`,
					`made ${w.fgp}% of their shots`,
					`shot ${w.fgp}%`,
				],
				"edge:wfg",
			),
			weight: 2.5 + (w.fgp - 52) * 0.4,
			topic: "fg",
		});
	}
	if (w.tp >= 16 && w.tp - l.tp >= 5) {
		add({
			about: "w",
			did: pick(
				rng,
				[
					`made ${w.tp} three-pointers to ${poss(L)} ${l.tp}`,
					`hit ${w.tp} threes to ${poss(L)} ${l.tp}`,
				],
				"edge:w3gap",
			),
			weight: 2.5 + (w.tp - l.tp - 5) * 0.3,
			topic: "threes",
		});
	} else if (w.tp >= 17) {
		add({
			about: "w",
			did: pick(
				rng,
				[`made ${w.tp} three-pointers`, `went ${w.tp}-of-${w.tpa} from three`],
				"edge:w3",
			),
			weight: 2.5,
			topic: "threes",
		});
	}
	if (w.ast >= 30) {
		add({
			about: "w",
			did: `assisted on ${w.ast} of their ${w.fg} baskets`,
			weight: 2.2 + (w.ast - 30) * 0.2,
			topic: "ast",
		});
	}
	if (w.dblFig >= 7 && !told.balance) {
		add({
			about: "w",
			did: `had ${numWord(w.dblFig)} players in double figures`,
			weight: 2.6,
			topic: "balance",
		});
	}
	return out.sort((a, b) => b.weight - a.weight);
};

const pointsOnly = (p: RecapPlayer): boolean =>
	statPhrase(p) === plural(p.pts, "point");

// How much a man's line deserves the space, for ordering a cast.
const castRank = (p: RecapPlayer): number => {
	const dd = doubleCategories(p).length;
	return (
		p.pts +
		(dd >= 3 ? 15 : dd === 2 ? 4 : 0) +
		(p.reb >= 15 ? 4 : 0) +
		(p.ast >= 12 ? 4 : 0) +
		(p.stl >= 5 || p.blk >= 5 ? 3 : 0)
	);
};

// Who is worth a clause behind a team's best man.
const worthy = (p: RecapPlayer): boolean =>
	p.pts >= 13 ||
	doubleCategories(p).length >= 2 ||
	p.reb >= 12 ||
	p.ast >= 10 ||
	p.stl >= 5 ||
	p.blk >= 5;

// The parts of a line: ["27 points", "15 rebounds", "6 blocks"].
const lineItems = (line: string): string[] => line.split(/, | and /);

// "scored 19", "had 23 points and 7 assists", "added a double-double of 17
// points and 14 rebounds". No scoring verb is used twice in a recap: the
// ledger in recapText.ts holds every one the piece has spent, the lede's
// included, and `used` the ones this sentence has.
const didLine = (
	p: RecapPlayer,
	rng: Rng,
	used: Set<string>,
	// Nobody before him in the sentence for an "added" to add to.
	first: boolean,
	more: {
		// His points are already on the page ("9 of his 18 points in the
		// quarter"): only the rest of his line.
		rest?: boolean;
		// He outscored the man the story is about.
		top?: boolean;
	} = {},
): string => {
	const line = statPhrase(p);
	if (more.rest) {
		const extras = lineItems(line).filter((x) => x !== plural(p.pts, "point"));
		used.add("had");
		claimVerb("had");
		return `also had ${naturalList(extras)}`;
	}
	// A night on the glass is said as one, not listed: "(19 points and 19
	// rebounds)" flattened the biggest number in the box score.
	if (p.reb >= 17) {
		const others = naturalList(
			lineItems(line).filter((x) => x !== plural(p.reb, "rebound")),
		);
		return pick(
			rng,
			[
				`was everywhere on the glass, pulling down ${p.reb} rebounds to go with ${others}`,
				`owned the glass, pulling in ${p.reb} rebounds to go with ${others}`,
				`cleaned the glass for ${p.reb} rebounds to go with ${others}`,
			],
			"cast:glass",
		);
	}
	// The team's top scorer is called that: "Lonnie Green added 28" of a man
	// who outscored everybody.
	if (more.top) {
		const hadFree = !used.has("had") && verbFree("had");
		const text = hadFree
			? pick(
					rng,
					[`had a team-high ${line}`, `led the scoring with ${line}`],
					"cast:top",
				)
			: `led the scoring with ${line}`;
		if (text.startsWith("had ")) {
			used.add("had");
			claimVerb("had");
		}
		return text;
	}
	const only = pointsOnly(p);
	const dd = doubleCategories(p).length;
	if (
		dd === 2 &&
		!used.has("double-double") &&
		lineItems(line).length === 2 &&
		rng() < 0.3
	) {
		used.add("double-double");
		const verb =
			!first && !used.has("added") && verbFree("added") ? "added" : "had";
		if (verb === "added" || (!used.has("had") && verbFree("had"))) {
			used.add(verb);
			claimVerb(verb);
			return `${verb} a double-double of ${line}`;
		}
	}
	// [verb, what follows it]
	const options: [string, string][] = only
		? [
				["scored", String(p.pts)],
				["had", line],
				["finished with", line],
				["went for", String(p.pts)],
				...(first
					? []
					: ([
							["added", String(p.pts)],
							["chipped in", String(p.pts)],
							["contributed", String(p.pts)],
							["tacked on", String(p.pts)],
						] as [string, string][])),
			]
		: [
				["had", line],
				["finished with", line],
				["put up", line],
				["posted", line],
				["turned in", line],
				...(first
					? []
					: ([
							["added", line],
							["contributed", line],
							["chipped in", line],
						] as [string, string][])),
			];
	const free = options.filter(([v]) => !used.has(v) && verbFree(v));
	const local = options.filter(([v]) => !used.has(v));
	const from = free.length > 0 ? free : local.length > 0 ? local : options;
	const [verb, what] = pickKeyed(
		rng,
		from.map((o) => ({ key: o[0], value: o })),
		only ? "cast:verb:only" : "cast:verb:line",
	);
	used.add(verb);
	claimVerb(verb);
	return `${verb} ${what}`;
};

export const composeCast = (
	input: StoryInput,
	told: Told,
	// What the piece has printed so far, so a man whose line the story already
	// gave is not given it again.
	written: string,
): {
	winners: string[];
	losers: string[];
	// The winners' paragraph opens on the star's own sentence.
	starFirst: boolean;
} => {
	const { game, shape, star, headline, rng } = input;
	const f = factsOf(game, shape);
	const { W, L } = f;
	const edges = edgesOf(f, told, rng);
	const winners: string[] = [];
	const losers: string[] = [];
	const hasLine = (p: RecapPlayer) =>
		told.lines.has(p.name) &&
		!told.partial.has(p.name) &&
		written.includes(p.name);
	// A man the story gave part of ("9 of his 18 points in the quarter") is
	// given the rest only when there is a rest worth giving.
	const partialDone = (p: RecapPlayer) =>
		told.partial.has(p.name) && written.includes(p.name) && pointsOnly(p);
	// ...and then only the rest: his points are on the page.
	const isRest = (p: RecapPlayer) =>
		told.partial.has(p.name) && written.includes(p.name);
	// One "double-double" label across the two casts: they are often one
	// paragraph, and a game full of them said it four times.
	let ddSaid = false;
	// A teammate who outscored the man the story is about.
	const isTop = (p: RecapPlayer) =>
		p !== star &&
		shape.winner.players.includes(p) &&
		p.pts > star.pts &&
		!shape.winner.players.some((x) => x.pts > p.pts);
	// Lines only a sentence that names the man first can say.
	const special = (p: RecapPlayer) => isRest(p) || isTop(p) || p.reb >= 17;
	const dl = (p: RecapPlayer, used: Set<string>, first: boolean) => {
		if (ddSaid) {
			used.add("double-double");
		}
		const text = didLine(p, rng, used, first, {
			rest: isRest(p),
			top: isTop(p),
		});
		if (text.includes("double-double")) {
			ddSaid = true;
		}
		return text;
	};

	// --- The winners ---
	const wEdge = edges.find((e) => e.about === "w");
	// The star, when the lede did not carry his line.
	let starSentence: string | undefined;
	// One of several ways to say it, by a verb the recap has not spent.
	const sayStar = (options: [string, string][], poolId: string): string => {
		const free = options.filter(([v]) => v === "" || verbFree(v));
		const [verb, text] = pick(rng, free.length > 0 ? free : options, poolId);
		if (verb !== "") {
			claimVerb(verb);
		}
		return text;
	};
	if (
		!told.starLine &&
		told.partial.has(star.name) &&
		written.slice(headline.text.length).includes(star.name)
	) {
		// "...with Oliver scoring 14 of his 36 points" has his points: only
		// what else he did.
		const rest = lineItems(statPhrase(star)).filter(
			(x) => x !== plural(star.pts, "point"),
		);
		if (rest.length > 0) {
			claimVerb("had");
			starSentence = `${star.name} also had ${naturalList(rest)}.`;
		}
		told.lines.add(star.name);
	} else if (!told.starLine) {
		const line = statPhrase(star);
		const made = `${star.fg}-of-${star.fga}`;
		const split = star.fga >= 8 ? ` on ${made} shooting` : "";
		const led = !shape.winner.players.some((p) => p.pts > star.pts);
		const N = star.name;
		if (headline.spentLine) {
			// The headline has his line; the body has what it did not.
			const pct = star.fga > 0 ? star.fg / star.fga : 0;
			const more = statPhrase(star, 3, true);
			const only = line === plural(star.pts, "point");
			starSentence =
				more !== line
					? sayStar(
							[
								["finished with", `${N} finished with ${more}${split}.`],
								["had", `${N} had ${more}${split}.`],
							],
							"cast:starMore",
						)
					: star.fga < 5
						? sayStar(
								[
									["finished with", `${N} finished with ${line}.`],
									["had", `${N} had ${line}.`],
								],
								"cast:starPlain",
							)
						: !only
							? sayStar(
									[
										[
											"finished with",
											`${N} finished with ${line} on ${made} shooting.`,
										],
										["", `${N} shot ${made} on the way to ${line}.`],
									],
									"cast:starSplitLine",
								)
							: sayStar(
									// The phrasing has to match what the split says: a
									// poor night in the tone of a good one reads as though
									// nobody looked at it.
									star.pts >= star.fga * 1.5
										? [
												[
													"",
													`${N} needed only ${star.fga} shots for his ${line}.`,
												],
												["", `${N} got his ${line} on ${made} shooting.`],
												[
													"",
													`${N} made ${star.fg} of ${star.fga} shots on the way to ${line}.`,
												],
											]
										: pct < 0.42 && star.fga >= 10
											? [
													[
														"",
														`${N} got his ${line} the hard way, on ${made} shooting.`,
													],
													["", `${N} got his ${line} on ${made} shooting.`],
												]
											: [
													["", `${N} got his ${line} on ${made} shooting.`],
													["", `${N} shot ${made} for his ${line}.`],
												],
									"cast:starSplit",
								);
		} else {
			// How he shot is told when it says something - a hot night, or a
			// hard one - and left out of an ordinary one: "on 9-of-19 shooting"
			// on every star's line is a box score read aloud.
			const pct = star.fga > 0 ? star.fg / star.fga : 0;
			const notable = star.fga >= 10 && (pct >= 0.58 || pct <= 0.36);
			const sp = notable ? split : "";
			const only = line === plural(star.pts, "point");
			const options: [string, string][] = led
				? [
						["", `${N} led ${W} with ${line}${sp}.`],
						["finished with", `${N} finished with ${line}${sp} to lead ${W}.`],
						["had", `${N} had ${line}${sp} for ${W}.`],
						["", `${poss(N)} ${line} led ${W}.`],
					]
				: [
						["had", `${N} had ${line}${sp} for ${W}.`],
						["finished with", `${N} finished with ${line}${sp} for ${W}.`],
					];
			// What his night was made of, when it was made of one thing.
			if (star.tp >= 5 && star.tpa >= star.tp) {
				options.push([
					"",
					`${N} made ${star.tp} of ${star.tpa} three-pointers on the way to ${line}.`,
				]);
			}
			if (star.ft >= 12 && star.fta >= star.ft) {
				options.push([
					"",
					`${N} made ${star.ft} of ${star.fta} free throws on the way to ${line}.`,
				]);
			}
			if (
				only &&
				star.pts >= 20 &&
				star.pts >= star.fga * 1.5 &&
				star.fga >= 8
			) {
				options.push([
					"",
					`${N} needed only ${star.fga} shots for his ${line}.`,
				]);
			}
			starSentence = sayStar(options, "cast:star");
		}
		told.lines.add(star.name);
	} else if (told.starPrinted && written.includes(star.name)) {
		// The lede had room for only part of his line: the rest of it.
		const printed = new Set(lineItems(told.starPrinted));
		const rest = lineItems(statPhrase(star, 2)).filter((x) => !printed.has(x));
		if (rest.length > 0) {
			claimVerb("had");
			starSentence = `${star.name} also had ${naturalList(rest)}.`;
		}
	}

	const wOthers = shape.winner.players
		.filter((p) => p !== star && !hasLine(p) && !partialDone(p))
		.sort((x, y) => castRank(y) - castRank(x));
	// A man the headline named is in the story under it.
	const body = written.slice(headline.text.length);
	const headlined = (p: RecapPlayer) =>
		headline.text.includes(p.name) && !body.includes(p.name);
	let support = [
		...wOthers.filter(headlined),
		...wOthers.filter((p) => worthy(p) && !headlined(p)),
	].slice(0, 2);
	// A teammate who outscored the star is named before the other man.
	support.sort((x, y) => Number(y.pts > star.pts) - Number(x.pts > star.pts));
	// Nobody with a line of his own: the next scorer still gets his name.
	if (
		support.length === 0 &&
		starSentence === undefined &&
		wOthers[0] &&
		wOthers[0].pts >= 10
	) {
		support = [wOthers[0]];
	}
	const afterStar = starSentence !== undefined;
	const [a, b] = support;
	if (starSentence) {
		winners.push(starSentence);
	}
	// Each shape says where the team's number can ride on it: as a clause on
	// the team's name at the end, or ahead of the verb.
	type Form = {
		text: (edge: string, lead: string) => string;
		kind: string;
		// A scoring verb written into the shape, which the recap's ledger
		// has to have free.
		verb?: string;
	};
	const wForms: Form[] = [];
	const starOnPage =
		told.starLine && star.pts >= 20 && written.includes(star.name);
	if (a && b) {
		const twins = a.pts === b.pts && pointsOnly(a) && pointsOnly(b);
		const bothOnly = pointsOnly(a) && pointsOnly(b);
		if (twins) {
			wForms.push(
				{
					kind: "apiece",
					verb: "scored",
					text: (who) =>
						`${a.name} and ${b.name} scored ${a.pts} apiece for ${W}${who}.`,
				},
				{
					kind: "got",
					text: (_who, lead) =>
						`${cap(W)}${lead || (afterStar || told.starLine ? " also" : "")} got ${a.pts} points apiece from ${a.name} and ${b.name}.`,
				},
			);
		} else {
			wForms.push(
				{
					kind: "and",
					text: (who) => {
						const used = new Set<string>();
						const first = dl(a, used, true);
						return `${a.name} ${first}${/,|glass/.test(first) ? "," : ""} and ${b.name} ${dl(b, used, false)} for ${W}${who}.`;
					},
				},
				{
					kind: "got",
					text: (_who, lead) => {
						const la = statPhrase(a);
						const lb = statPhrase(b);
						const also = afterStar || told.starLine ? " also" : "";
						return lead
							? `${cap(W)}${lead} got ${la} from ${a.name} and ${lb} from ${b.name}.`
							: pick(
									rng,
									[
										`${cap(W)}${also} got ${la} from ${a.name} and ${lb} from ${b.name}.`,
										`${a.name} gave ${W} ${la}, and ${b.name} ${lb}.`,
										`There were${also} ${la} from ${a.name} and ${lb} from ${b.name}.`,
									],
									"cast:got",
								);
					},
				},
			);
			if (bothOnly) {
				// The wire's own shape: "Owens added 19 points and Young 17".
				wForms.push({
					kind: "gap",
					verb: "added",
					text: (who) =>
						`${a.name} added ${a.pts} points and ${b.name} ${b.pts} for ${W}${who}.`,
				});
			}
			if (starOnPage && a.pts !== star.pts && b.pts !== star.pts) {
				wForms.push(
					{
						kind: "help",
						text: () => {
							const used = new Set(["had"]);
							claimVerb("had");
							const first = dl(a, used, true);
							return `${star.name} had help: ${a.name} ${first}, and ${b.name} ${dl(b, used, false)}.`;
						},
					},
					{
						kind: "behind",
						text: () => {
							const used = new Set<string>();
							const first = dl(a, used, true);
							return `Behind ${star.name}, ${a.name} ${first}${/,|glass/.test(first) ? "," : ""} and ${b.name} ${dl(b, used, false)}.`;
						},
					},
				);
			}
		}
	} else if (a) {
		wForms.push(
			{
				kind: "one",
				text: (who) =>
					`${a.name} ${dl(a, new Set(), !starOnPage && !afterStar)} for ${W}${who}.`,
			},
			{
				kind: "got",
				text: (_who, lead) =>
					`${cap(W)}${lead || (afterStar || told.starLine ? " also" : "")} got ${statPhrase(a)} from ${a.name}.`,
			},
		);
	}
	let wKind: string | undefined;
	// Not the lede's own shape again: "Behind 26 points from Ace Hawk, the
	// Hawks... Behind Ace Hawk, ...", "The Hawks got 26 points from Ace Hawk
	// and... The Hawks also got...".
	{
		const lede = written.slice(headline.text.length);
		const echo = wForms.filter(
			(x) =>
				!(x.kind === "behind" && /\b[Bb]ehind /.test(lede)) &&
				!(x.kind === "got" && / got \d+ points?[^.]* from /.test(lede)),
		);
		if (echo.length > 0) {
			wForms.splice(0, wForms.length, ...echo);
		}
	}
	// A man whose points the story gave takes only the shapes that can give
	// the rest of his line.
	if ((a && special(a)) || (b && special(b))) {
		const plain = wForms.filter(
			(x) =>
				x.kind === "and" ||
				x.kind === "one" ||
				((x.kind === "help" || x.kind === "behind") &&
					!(a && isRest(a)) &&
					!(b && isRest(b))),
		);
		if (plain.length > 0) {
			wForms.splice(0, wForms.length, ...plain);
		}
	}
	{
		const open = wForms.filter((x) => !x.verb || verbFree(x.verb));
		if (open.length > 0) {
			wForms.splice(0, wForms.length, ...open);
		}
	}
	if (a && b && !pointsOnly(a) && !pointsOnly(b)) {
		// "got 13 points and 10 assists from A and 14 points and 9 rebounds
		// from B" is four "and"s in a row.
		const light = wForms.filter((x) => x.kind !== "got");
		if (light.length > 0) {
			wForms.splice(0, wForms.length, ...light);
		}
	}
	if (wForms.length > 0) {
		const kind = pickKeyed(
			rng,
			wForms.map((x) => ({ key: x.kind, value: x.kind })),
			"cast:w",
		);
		const form = wForms.find((x) => x.kind === kind)!;
		wKind = kind;
		if (form.verb) {
			claimVerb(form.verb);
		}
		const carries = kind !== "help" && kind !== "behind";
		const text = form.text(
			wEdge && carries ? `, who ${wEdge.did}` : "",
			wEdge && carries ? ` ${wEdge.did} and` : "",
		);
		const rel = wEdge ? `, who ${wEdge.did}` : "";
		if (rel && text.includes(rel) && text.split(" ").length > 33) {
			// Two men and the team's number is a paragraph in one breath.
			winners.push(text.replace(rel, ""), `${cap(W)} ${wEdge!.did}.`);
		} else {
			winners.push(text);
		}
		// The shapes that cannot carry it leave a real edge its own sentence.
		if (wEdge && !carries && wEdge.weight >= 3) {
			winners.push(`${cap(W)} ${wEdge.did}.`);
		}
	} else if (wEdge && wEdge.weight >= 3) {
		winners.push(`${cap(W)} ${wEdge.did}.`);
	}

	// --- The losers ---
	const lMen = shape.loser.players
		.filter((p) => !hasLine(p) && !partialDone(p))
		.sort(
			(x, y) =>
				Number(headlined(y)) - Number(headlined(x)) ||
				castRank(y) - castRank(x),
		);
	const lTold = shape.loser.players.some(hasLine);
	const lBest = lMen[0];
	const lEdges = edges.filter((e) => e.about === "l");
	const lEdge = lEdges[0];
	// A losing run rides on the losers' own sentence.
	const s = shape.loser.streak;
	const lSkid =
		!game.playoffs &&
		s &&
		!s.won &&
		s.count >= 4 &&
		!told.streak &&
		!/in a row|straight|skid|losing streak/.test(written)
			? pick(
					rng,
					[
						`have lost ${numWord(s.count)} in a row`,
						`have dropped ${numWord(s.count)} straight`,
					],
					"cast:skid",
				)
			: undefined;
	const reasons = [lEdge?.did, lSkid].filter((x): x is string => !!x);
	const reason = reasons.join(" and ");
	let skidUsed = false;
	if (lBest && (worthy(lBest) || !lTold)) {
		const second = lMen[1] && worthy(lMen[1]) ? lMen[1] : undefined;
		const line = statPhrase(lBest);
		const led = !shape.loser.players.some((p) => p.pts > lBest.pts);
		const who = reason ? `, who ${reason}` : "";
		// A line worth calling wasted.
		const big = lBest.pts >= 22 || doubleCategories(lBest).length >= 2;
		const forms: {
			kind: string;
			text: () => string[];
			skid: boolean;
			verb?: string;
		}[] = [];
		// The headline printed his line ("...despite Antoine Walker's 27
		// points and 10 rebounds"): the body says what it did not, how he
		// shot, and never the line again.
		const lineSpent =
			!!headline.spentLoserStar && headline.text.includes(lBest.name);
		const shot =
			lBest.fga >= 5
				? `shot ${lBest.fg}-of-${lBest.fga} for his ${lBest.pts}`
				: `got his ${lBest.pts} in ${lBest.min} minutes`;
		// Nobody stood out: said as that, with the reason as more of the same
		// ("...and they turned the ball over 21 times"), never as a contrast.
		const quiet =
			!lineSpent &&
			!lTold &&
			lBest.pts <= 17 &&
			lBest.pts >= 8 &&
			doubleCategories(lBest).length < 2 &&
			!special(lBest) &&
			// Alone at the top: a shared high is both men's, told as that.
			!shape.loser.players.some((p) => p !== lBest && p.pts >= lBest.pts);
		if (quiet) {
			const more = reason ? `, and they ${reason}` : "";
			forms.push({
				kind: "quiet",
				skid: true,
				text: () => [
					pick(
						rng,
						[
							`${cap(L)} got no more than ${lBest.pts} from anyone, ${lBest.name} leading the way${more}.`,
							`Nobody scored more than ${lBest.pts} for ${L}, ${lBest.name} leading them${more}.`,
							`${cap(L)} had nobody with more than ${poss(lBest.name)} ${lBest.pts}${more}.`,
						],
						"cast:quiet",
					),
				],
			});
		}
		if (quiet) {
			// The shape for it, and no other.
		} else if (lineSpent) {
			forms.push({
				kind: "shot",
				skid: true,
				text: () => [
					second
						? `${lBest.name} ${shot} and ${second.name} ${dl(second, new Set(), false)} for ${L}${who}.`
						: `${lBest.name} ${shot} for ${L}${who}.`,
				],
			});
		} else if (lTold) {
			// Their best man is already on the page: the men behind him.
			forms.push({
				kind: "behind",
				skid: true,
				text: () => {
					const used = new Set<string>();
					return [
						`${lBest.name} ${dl(lBest, used, false)}${second ? ` and ${second.name} ${dl(second, used, false)}` : ""} for ${L}${who}.`,
					];
				},
			});
		} else if (
			second &&
			second.pts === lBest.pts &&
			pointsOnly(lBest) &&
			pointsOnly(second)
		) {
			forms.push({
				kind: "apiece",
				skid: true,
				verb: "scored",
				text: () => [
					`${lBest.name} and ${second.name} scored ${lBest.pts} apiece for ${L}${who}.`,
				],
			});
		} else if (second) {
			forms.push({
				kind: "and",
				skid: true,
				text: () => {
					const used = new Set<string>();
					const first = dl(lBest, used, true);
					return [
						`${lBest.name} ${first}${/,|glass/.test(first) ? "," : ""} and ${second.name} ${dl(second, used, false)} for ${L}${who}.`,
					];
				},
			});
			if (wKind !== "got" && (pointsOnly(lBest) || pointsOnly(second))) {
				forms.push({
					kind: "got",
					skid: true,
					text: () => [
						`${cap(L)} got ${line} from ${lBest.name} and ${statPhrase(second)} from ${second.name}${reason ? ` but ${reason}` : ""}.`,
					],
				});
			}
			if (led) {
				forms.push({
					kind: "led",
					skid: true,
					text: () => [
						`${lBest.name} led ${L} with ${line}, and ${second.name} ${dl(second, new Set(), false)}${reason ? `, but ${L} ${reason}` : ""}.`,
					],
				});
			}
			if (lEdge && big) {
				forms.push({
					kind: "wasted",
					skid: false,
					text: () => [
						`${cap(L)} ${lEdge.did}, wasting ${line} from ${lBest.name} and ${statPhrase(second)} from ${second.name}.`,
					],
				});
			}
			if (pointsOnly(lBest) && pointsOnly(second) && lBest.pts !== second.pts) {
				forms.push({
					kind: "gap",
					skid: true,
					verb: "scored",
					text: () => [
						`${lBest.name} scored ${lBest.pts} points and ${second.name} ${second.pts} for ${L}${who}.`,
					],
				});
			}
			forms.push({
				kind: "for",
				skid: true,
				text: () => {
					const used = new Set<string>();
					const first = dl(lBest, used, true);
					return [
						`For ${L}${who ? `${who},` : ","} ${lBest.name} ${first}${/,|glass/.test(first) ? "," : ""} and ${second.name} ${dl(second, used, false)}.`,
					];
				},
			});
		} else {
			forms.push({
				kind: "one",
				skid: true,
				text: () => [
					special(lBest) || !led || who
						? `${lBest.name} ${dl(lBest, new Set(), true)} for ${L}${who}.`
						: `${lBest.name} led ${L} with ${line}.`,
				],
			});
			if (wKind !== "got") {
				forms.push({
					kind: "got",
					skid: true,
					text: () => [
						`${cap(L)} got ${line} from ${lBest.name}${reason ? ` but ${reason}` : ""}.`,
					],
				});
			}
			if (lEdge && big) {
				forms.push({
					kind: "wasted",
					skid: false,
					text: () => [
						`${cap(L)} ${lEdge.did}, wasting ${line} from ${lBest.name}.`,
					],
				});
			}
		}
		if (special(lBest) || (second && special(second))) {
			const plain = forms.filter((x) =>
				["and", "behind", "one", "for", "shot", "quiet"].includes(x.kind),
			);
			if (plain.length > 0) {
				forms.splice(0, forms.length, ...plain);
			}
		}
		{
			const open = forms.filter((x) => !x.verb || verbFree(x.verb));
			// The apiece shape has no other way to say it.
			if (open.length > 0) {
				forms.splice(0, forms.length, ...open);
			}
		}
		const kind = pickKeyed(
			rng,
			forms.map((x) => ({ key: x.kind, value: x.kind })),
			"cast:l",
		);
		const form = forms.find((x) => x.kind === kind)!;
		if (form.verb) {
			claimVerb(form.verb);
		}
		const tooLong = (text: string) => text.split(" ").length > 33;
		const written2 = form.text();
		// Two men and two reasons is a paragraph in one breath: the reason
		// gets its own sentence.
		const at = written2.findIndex(
			(t) => reason !== "" && tooLong(t) && t.includes(`, who ${reason}`),
		);
		if (at >= 0) {
			written2[at] = written2[at]!.replace(`, who ${reason}`, "");
			written2.splice(at + 1, 0, `${cap(L)} ${reason}.`);
		}
		losers.push(...written2);
		skidUsed = form.skid && !!lSkid;
		told.lines.add(lBest.name);
		if (second) {
			told.lines.add(second.name);
		}
	} else if (reason) {
		losers.push(`${cap(L)} ${reason}.`);
		skidUsed = !!lSkid;
	}
	if (skidUsed) {
		told.streak = true;
	}
	// A second reason, when it is a big one.
	const lEdge2 = lEdges[1];
	if (
		lEdge2 &&
		lEdge2.weight >= 4.5 &&
		losers.length > 0 &&
		losers.length < 3
	) {
		losers.push(`They also ${lEdge2.did}.`);
	}
	return { winners, losers, starFirst: starSentence !== undefined };
};
