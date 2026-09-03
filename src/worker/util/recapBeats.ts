// THE SEASON AROUND THE GAME.
//
// getAutoRecap reads the box score. These beats read what the box score sits
// inside: the standings the result moved, the season series it extended or
// squared, the second night of a back-to-back, the season high, the streak a
// scorer carried in, the round number a veteran went past, the man playing his
// first game back, what the bench gave, and who is up next. All of it comes
// from the context getDayGamesForRecap derives (recapContext.ts), so every
// sentence is anchored to a fact the games already hold.
//
// House rules, same as the rest of the engine: every beat has at least three
// phrasings behind a pool id so a night rotates through them; no sentence
// opens on a numeral; past tense for what happened, present only for where
// things stand; a beat that has nothing true to say returns undefined, so a
// quiet game stays short. And every number is one the accuracy reader
// (recapAccuracy.ts) can hold against the game - a previous best is quoted
// bare rather than as "31 points", because that reader takes "N points" next
// to a name as tonight's line.

import type {
	RecapGame,
	RecapPlayer,
	RecapTeam,
} from "./getDayGamesForRecap.ts";
import {
	aWord,
	cap,
	doubleCategories,
	gbText,
	injuryPhrase,
	lowerInjury,
	naturalList,
	nick,
	numWord,
	ordinal,
	pick,
	plural,
	poss,
	scoredVerb,
	statPhrase,
	theNick,
} from "./recapText.ts";

export type BeatContext = {
	game: RecapGame;
	winner: RecapTeam;
	loser: RecapTeam;
	margin: number;
	// Everyone the piece has named so far. A beat that introduces a man adds
	// him, so the next one does not introduce him again.
	said: Set<string>;
	// The prose so far, for a beat that must not repeat a fact it finds there.
	written: string;
};

type Rng = () => number;

type StandingInfo = NonNullable<RecapTeam["standing"]>;

const standingOf = (t: RecapTeam): StandingInfo | undefined => t.standing;

// Ranks mean nothing until the sample does.
const MIN_GAMES_FOR_STANDINGS = 15;

// ---------------------------------------------------------------- THE TABLE

export const standingsBeat = (
	ctx: BeatContext,
	rng: Rng,
): string | undefined => {
	if (ctx.game.playoffs) {
		return undefined;
	}
	const options: string[] = [];
	const w = standingOf(ctx.winner);
	const l = standingOf(ctx.loser);
	const played = (s: StandingInfo) => s.won + s.lost;

	// A move is news inside the playoff picture - the top half of the
	// conference - and noise below it: "climbed to thirteenth" is nobody's
	// headline.
	const picture = (x: StandingInfo) => Math.ceil(x.teams / 2);

	if (w && played(w) >= MIN_GAMES_FOR_STANDINGS) {
		const W = cap(theNick(ctx.winner));
		const wn = theNick(ctx.winner);
		if (
			w.rankBefore !== undefined &&
			w.rank < w.rankBefore &&
			w.rank <= picture(w)
		) {
			options.push(
				pick(
					rng,
					[
						`The win moved ${wn} up to ${ordinal(w.rank)} in the ${w.conf}.`,
						`${W} climbed to ${ordinal(w.rank)} in the ${w.conf} with the win.`,
						`That lifted ${wn} into ${ordinal(w.rank)} place in the ${w.conf}.`,
					],
					"standingsUp",
				),
			);
		} else if (w.rank === 1 && w.lead !== undefined && w.lead >= 1) {
			options.push(
				pick(
					rng,
					[
						`${W} stayed top of the ${w.conf}, ${gbText(w.lead)} clear of the field.`,
						`The lead at the top of the ${w.conf} is ${gbText(w.lead)}, and it belongs to ${wn}.`,
						`${W} remain first in the ${w.conf}, ${gbText(w.lead)} ahead of anyone else.`,
					],
					"standingsTop",
				),
			);
		} else if (w.rank >= 2 && w.rank <= 4 && w.gb <= 3 && w.leader) {
			options.push(
				pick(
					rng,
					[
						`${W} sit ${gbText(w.gb)} behind the ${w.leader} in the ${w.conf}.`,
						`That keeps ${wn} within ${gbText(w.gb)} of the ${w.leader} at the top of the ${w.conf}.`,
						`${W} are ${ordinal(w.rank)} in the ${w.conf}, ${gbText(w.gb)} back of the ${w.leader}.`,
					],
					"standingsChase",
				),
			);
		}
	}

	if (
		l &&
		played(l) >= MIN_GAMES_FOR_STANDINGS &&
		l.rankBefore !== undefined &&
		l.rank > l.rankBefore &&
		l.rankBefore <= picture(l)
	) {
		const L = cap(theNick(ctx.loser));
		const ln = theNick(ctx.loser);
		options.push(
			pick(
				rng,
				[
					`The loss dropped ${ln} to ${ordinal(l.rank)} in the ${l.conf}.`,
					`${L} slipped to ${ordinal(l.rank)} in the ${l.conf}.`,
					`It left ${ln} ${ordinal(l.rank)} in the ${l.conf}, ${gbText(l.gb)} off the top.`,
				],
				"standingsDown",
			),
		);
	}

	return options.length > 0 ? pick(rng, options) : undefined;
};

// ---------------------------------------------------------------- THE SERIES

export const seriesBeat = (ctx: BeatContext, rng: Rng): string | undefined => {
	if (ctx.game.playoffs) {
		return undefined;
	}
	const s = ctx.winner.seasonSeries;
	if (!s || s.won + s.lost === 0) {
		return undefined;
	}
	const W = cap(theNick(ctx.winner));
	const wn = theNick(ctx.winner);
	const L = cap(theNick(ctx.loser));
	const ln = theNick(ctx.loser);
	const wonNow = s.won + 1;
	const total = s.won + s.lost + 1;
	const options: string[] = [];

	if (s.lost === 0 && total === 2 && rng() < 0.5) {
		return undefined;
	}
	if (s.lost === 0) {
		options.push(
			pick(
				rng,
				total === 2
					? [
							`${W} have won both meetings with ${ln} this season.`,
							`That is two for two against ${ln} this season for ${wn}.`,
							`${W} have yet to lose to ${ln} this season.`,
						]
					: [
							`${W} have won all ${numWord(total)} meetings with ${ln} this season.`,
							`That is ${numWord(total)} for ${numWord(total)} against ${ln} this season for ${wn}.`,
							`${W} have yet to lose to ${ln} this season, ${numWord(total)} meetings in.`,
						],
				"seriesSweep",
			),
		);
	} else if (wonNow === s.lost) {
		options.push(
			pick(
				rng,
				[
					`That squared the season series with ${ln} at ${wonNow}-${wonNow}.`,
					`The season series with ${ln} is level at ${wonNow}-${wonNow}.`,
					`${W} pulled level with ${ln} at ${wonNow}-${wonNow} for the season.`,
				],
				"seriesLevel",
			),
		);
	} else if (wonNow > s.lost) {
		options.push(
			pick(
				rng,
				[
					`${W} lead the season series with ${ln} ${wonNow}-${s.lost}.`,
					`That put ${wn} up ${wonNow}-${s.lost} in the season series.`,
					`It was ${poss(wn)} ${ordinal(wonNow)} win in ${numWord(total)} meetings with ${ln} this season.`,
				],
				"seriesLead",
			),
		);
	} else if (wonNow === 1) {
		options.push(
			pick(
				rng,
				[
					`${L} had taken the first ${numWord(s.lost)} meetings this season; this was ${poss(wn)} first.`,
					`It was ${poss(wn)} first win in ${numWord(total)} meetings with ${ln} this season.`,
					`${W} still trail the season series ${s.lost}-${wonNow}, but they are on the board.`,
				],
				"seriesFirstWin",
			),
		);
	} else {
		options.push(
			pick(
				rng,
				[
					`${W} still trail the season series ${s.lost}-${wonNow}.`,
					`${L} lead the season series ${s.lost}-${wonNow} even so.`,
					`That made it ${wonNow}-${s.lost} to ${ln} in the season series.`,
				],
				"seriesTrail",
			),
		);
	}

	// The last meeting, when it went the other way - a revenge angle a beat
	// writer never leaves on the table.
	if (s.last && !s.last.won && s.last.oppPts - s.last.pts >= 10) {
		options.push(
			pick(
				rng,
				[
					`It avenged a ${s.last.oppPts}-${s.last.pts} loss to ${ln} in the teams' previous meeting.`,
					`The previous meeting had gone to ${ln}, ${s.last.oppPts}-${s.last.pts}.`,
					`${W} had lost the last meeting by ${s.last.oppPts - s.last.pts}.`,
				],
				"seriesRevenge",
			),
		);
	}

	let text = pick(rng, options);
	if (s.left === 1 && text.includes("season series")) {
		text = `${text.slice(0, -1)}, with one meeting to go.`;
	}
	return text;
};

// ---------------------------------------------------------------- THE VENUE

export const homeRoadBeat = (
	ctx: BeatContext,
	rng: Rng,
): string | undefined => {
	if (ctx.game.playoffs) {
		return undefined;
	}
	// A record has already been given ("improved to 30-8"); a venue record on
	// top of it is the same kind of number twice.
	if (
		/\b(?:improved|moved|pushed \w+ \w+|dropped|fell) to \d+-\d+/.test(
			ctx.written,
		)
	) {
		return undefined;
	}
	const options: string[] = [];
	const winnerHome = ctx.game.teams[0].tid === ctx.winner.tid;
	const W = cap(theNick(ctx.winner));
	const wn = theNick(ctx.winner);
	const L = cap(theNick(ctx.loser));
	const ln = theNick(ctx.loser);

	const wa = ctx.winner.awayRecord;
	const wh = ctx.winner.homeRecord;
	const la = ctx.loser.awayRecord;
	const lh = ctx.loser.homeRecord;
	const games = (r: { won: number; lost: number }) => r.won + r.lost;

	if (
		!winnerHome &&
		wa &&
		games(wa) >= 8 &&
		wa.won >= 5 &&
		wa.won >= wa.lost * 1.5
	) {
		options.push(
			pick(
				rng,
				[
					`${W} improved to ${wa.won}-${wa.lost} on the road.`,
					`That is ${wa.won} road wins in ${games(wa)} tries for ${wn}.`,
					`${W} are ${wa.won}-${wa.lost} away from home.`,
				],
				"roadRecord",
			),
		);
	}
	if (
		winnerHome &&
		wh &&
		games(wh) >= 8 &&
		wh.won >= 6 &&
		wh.won >= games(wh) * 0.75
	) {
		options.push(
			pick(
				rng,
				[
					`${W} improved to ${wh.won}-${wh.lost} at home.`,
					`${W} have now won ${wh.won} of ${games(wh)} at home.`,
					`Home has been kind to ${wn}: ${wh.won}-${wh.lost} there this season.`,
				],
				"homeRecord",
			),
		);
	}
	if (!winnerHome && lh && games(lh) >= 8 && lh.lost >= 6 && lh.lost > lh.won) {
		options.push(
			pick(
				rng,
				[
					`${L} fell to ${lh.won}-${lh.lost} at home.`,
					`That is ${lh.lost} home losses in ${games(lh)} for ${ln}.`,
					`${L} are ${lh.won}-${lh.lost} in their own building.`,
				],
				"homeStruggles",
			),
		);
	}
	if (
		winnerHome &&
		la &&
		games(la) >= 8 &&
		la.lost >= 6 &&
		la.lost >= la.won * 2
	) {
		options.push(
			pick(
				rng,
				[
					`${L} dropped to ${la.won}-${la.lost} on the road.`,
					`${L} have won just ${la.won} of ${games(la)} away from home.`,
					`It is ${la.won}-${la.lost} on the road now for ${ln}.`,
				],
				"roadStruggles",
			),
		);
	}
	return options.length > 0 ? pick(rng, options) : undefined;
};

// ---------------------------------------------------------------- THE REST

export const restBeat = (ctx: BeatContext, rng: Rng): string | undefined => {
	const options: string[] = [];
	const winnerHome = ctx.game.teams[0].tid === ctx.winner.tid;
	for (const t of [ctx.loser, ctx.winner]) {
		const r = t.rest;
		if (!r) {
			continue;
		}
		const lost = t === ctx.loser;
		const T = cap(theNick(t));
		const tn = theNick(t);
		if (r.daysSince === 1) {
			// A back-to-back is worth a sentence when it plausibly showed: the
			// tired side lost by something, or won on the road anyway. Even then
			// not every time - in a real league a third of games have a team on
			// one, and the reader does not need telling on every page.
			if (lost && ctx.margin >= 6 && rng() < 0.6) {
				options.push(
					pick(
						rng,
						[
							`${T} were playing the second night of a back-to-back.`,
							`It was the second night of a back-to-back for ${tn}.`,
							`${T} were on the back end of a back-to-back.`,
							`${T} had played the night before.`,
							`This was ${poss(tn)} second game in two nights.`,
						],
						"backToBackLoser",
					),
				);
			} else if (!lost && !winnerHome && rng() < 0.5) {
				options.push(
					pick(
						rng,
						[
							`${T} won it on the second night of a back-to-back.`,
							`${T} had played the night before and won on the road anyway.`,
							`That was the back end of a back-to-back for ${tn}, and a road game at that.`,
							`${T} were on a back-to-back, and it did not show.`,
						],
						"backToBackWinner",
					),
				);
			}
		} else if (r.daysSince >= 4 && rng() < 0.7) {
			options.push(
				pick(
					rng,
					[
						`${T} had not played in ${numWord(r.daysSince)} days.`,
						`${T} came in off ${numWord(r.daysSince)} days' rest.`,
						`It was ${poss(tn)} first game in ${numWord(r.daysSince)} days.`,
						`${T} had been idle for ${numWord(r.daysSince)} days.`,
					],
					"longRest",
				),
			);
		}
	}
	return options.length > 0 ? pick(rng, options) : undefined;
};

// ---------------------------------------------------------------- SEASON HIGHS

export const teamHighBeat = (
	ctx: BeatContext,
	rng: Rng,
): string | undefined => {
	if (ctx.game.playoffs) {
		return undefined;
	}
	const W = cap(theNick(ctx.winner));
	const wn = theNick(ctx.winner);
	const ln = theNick(ctx.loser);
	const h = ctx.winner.seasonHighs;
	if (h && h.priorGames >= 10) {
		if (h.leaguePts && h.priorGames >= 15) {
			return pick(
				rng,
				[
					`No team had scored ${ctx.winner.pts} in a game this season.`,
					`The ${ctx.winner.pts} points were the most any team has managed this season.`,
					`It was the highest-scoring night any team has had this season.`,
				],
				"leagueHighPts",
			);
		}
		if (h.pts) {
			return pick(
				rng,
				[
					`The ${ctx.winner.pts} points were a season high for ${wn}.`,
					`${W} had not scored that many in a game all season.`,
					`It was the most ${wn} have scored in a game this season.`,
					`No ${nick(ctx.winner)} game this season had produced more points.`,
					`${W} put up more than they had in any game this season.`,
				],
				"teamHighPts",
			);
		}
		if (h.margin && ctx.margin >= 15) {
			return pick(
				rng,
				[
					`The ${ctx.margin}-point margin was ${poss(wn)} biggest win of the season.`,
					`${W} had not won by that much all season.`,
					`It was ${poss(wn)} most lopsided win of the season.`,
				],
				"teamHighMargin",
			);
		}
	}
	const lh = ctx.loser.seasonHighs;
	if (lh && lh.priorGames >= 10 && lh.pts && ctx.loser.pts >= 110) {
		return pick(
			rng,
			[
				`The ${ctx.loser.pts} points were a season high for ${ln}, and it still was not enough.`,
				`${cap(ln)} scored more than they had in any game this season and lost anyway.`,
				`It was a season-high night for ${ln} on the scoreboard, for all the good it did.`,
			],
			"loserHighPts",
		);
	}
	return undefined;
};

export const playerHighBeat = (
	p: RecapPlayer,
	rng: Rng,
): string | undefined => {
	const e = p.entering;
	if (!e || e.gp < 10) {
		return undefined;
	}
	// A high by a point in a 22-point night is a footnote; clearing the old
	// mark by a few, or a genuinely big night, is a sentence.
	if (
		p.pts >= 22 &&
		p.pts > e.high.pts &&
		(p.pts - e.high.pts >= 2 || p.pts >= 30)
	) {
		return pick(
			rng,
			[
				`It was a season high for ${p.name}, whose previous best was ${e.high.pts}.`,
				`${p.name} had not scored more than ${e.high.pts} in a game this season.`,
				`${p.name} topped his season high of ${e.high.pts}.`,
				`${p.name} had never gone past ${e.high.pts} in a game this season.`,
				`No night of ${poss(p.name)} season had gone better than ${e.high.pts}.`,
			],
			"seasonHighPts",
		);
	}
	if (p.reb >= 12 && p.reb > e.high.reb) {
		return pick(
			rng,
			[
				`The ${p.reb} rebounds were a season high for ${p.name}.`,
				`${p.name} had not grabbed more than ${e.high.reb} in a game this season.`,
				`It was ${poss(p.name)} best night on the glass this season.`,
			],
			"seasonHighReb",
		);
	}
	if (p.ast >= 10 && p.ast > e.high.ast) {
		return pick(
			rng,
			[
				`The ${p.ast} assists were a season high for ${p.name}.`,
				`${p.name} had not handed out more than ${e.high.ast} in a game this season.`,
				`It was ${poss(p.name)} best passing night of the season.`,
			],
			"seasonHighAst",
		);
	}
	return undefined;
};

// ---------------------------------------------------------------- STREAKS

export const playerStreakBeat = (
	p: RecapPlayer,
	rng: Rng,
): string | undefined => {
	const e = p.entering;
	if (!e) {
		return undefined;
	}
	if (p.pts >= 30 && e.streaks.thirty >= 2) {
		const n = e.streaks.thirty + 1;
		return pick(
			rng,
			[
				`That is ${numWord(n)} straight games of 30 or more for ${p.name}.`,
				`${p.name} has scored 30-plus in ${numWord(n)} straight.`,
				`It was the ${ordinal(n)} game in a row ${p.name} has reached 30.`,
			],
			"streak30",
		);
	}
	if (p.pts >= 20 && e.streaks.twenty >= 4) {
		const n = e.streaks.twenty + 1;
		return pick(
			rng,
			[
				`That is ${numWord(n)} straight games of 20 or more for ${p.name}.`,
				`${p.name} has scored 20-plus in ${numWord(n)} straight.`,
				`It was the ${ordinal(n)} game in a row ${p.name} has reached 20.`,
				`${p.name} has not been held under 20 in ${numWord(n)} games.`,
				`Make it ${numWord(n)} in a row over 20 for ${p.name}.`,
			],
			"streak20",
		);
	}
	if (doubleCategories(p).length >= 2 && e.streaks.doubleDouble >= 3) {
		const n = e.streaks.doubleDouble + 1;
		return pick(
			rng,
			[
				`${p.name} has a double-double in ${numWord(n)} straight games.`,
				`That is ${numWord(n)} double-doubles in a row for ${p.name}.`,
				`It was ${poss(p.name)} ${ordinal(n)} straight double-double.`,
			],
			"streakDoubleDouble",
		);
	}
	return undefined;
};

// ---------------------------------------------------------------- MILESTONES

const STAT_WORD = {
	pts: "points",
	reb: "rebounds",
	ast: "assists",
	tp: "three-pointers",
} as const;

const STAT_ONE = {
	pts: "point",
	reb: "rebound",
	ast: "assist",
	tp: "three",
} as const;

const fmtNum = (n: number) => n.toLocaleString("en-US");

// One milestone per recap: the biggest one on the floor, career over season,
// points over the rest.
export const milestoneBeat = (
	ctx: BeatContext,
	rng: Rng,
): string | undefined => {
	const rank = (p: RecapPlayer) => {
		const m = p.milestone!;
		return (
			(m.scope === "career" ? 1000 : 0) +
			(m.stat === "pts" ? 100 : 0) +
			m.mark / 1000
		);
	};
	const p = [...ctx.winner.players, ...ctx.loser.players]
		.filter((x) => x.milestone)
		.sort((a, b) => rank(b) - rank(a))[0];
	if (!p || !p.milestone) {
		return undefined;
	}
	const m = p.milestone;
	const word = STAT_WORD[m.stat];
	const one = STAT_ONE[m.stat];
	ctx.said.add(p.name);
	if (m.scope === "career") {
		return pick(
			rng,
			[
				`${p.name} passed ${fmtNum(m.mark)} career ${word} along the way.`,
				`Somewhere in there, ${p.name} went past ${fmtNum(m.mark)} ${word} for his career.`,
				`${p.name} now has ${fmtNum(m.total)} career ${word}, having crossed ${fmtNum(m.mark)} tonight.`,
				`The night also took ${p.name} past ${fmtNum(m.mark)} career ${word}.`,
			],
			"milestoneCareer",
		);
	}
	return pick(
		rng,
		[
			`${p.name} went past the ${fmtNum(m.mark)}-${one} mark for the season.`,
			`That took ${p.name} past ${fmtNum(m.mark)} ${word} on the season.`,
			`${p.name} now has ${fmtNum(m.total)} ${word} on the season.`,
		],
		"milestoneSeason",
	);
};

// ---------------------------------------------------------------- THE RETURN

export const returnBeat = (ctx: BeatContext, rng: Rng): string | undefined => {
	const p = [...ctx.winner.players, ...ctx.loser.players]
		.filter((x) => x.returnFrom && x.min >= 12)
		.sort((a, b) => b.pts - a.pts)[0];
	if (!p || !p.returnFrom) {
		return undefined;
	}
	const r = p.returnFrom;
	ctx.said.add(p.name);
	return pick(
		rng,
		[
			`${p.name} was back after missing ${numWord(r.games)} games with ${injuryPhrase(r.type)}, and had ${statPhrase(p)}.`,
			`In his first game since ${injuryPhrase(r.type)} cost him ${numWord(r.games)} games, ${p.name} ${scoredVerb(rng)} ${statPhrase(p)}.`,
			`${p.name} returned from ${aWord(`${numWord(r.games)}-game`)} absence (${lowerInjury(r.type)}) with ${statPhrase(p)}.`,
		],
		"returnFromInjury",
	);
};

// ---------------------------------------------------------------- THE BENCH

const benchPoints = (t: RecapTeam) =>
	t.players
		.filter((p) => p.starter === false)
		.reduce((acc, p) => acc + p.pts, 0);

const startersKnown = (t: RecapTeam) =>
	t.players.filter((p) => p.starter === true).length >= 5;

export const benchBeat = (ctx: BeatContext, rng: Rng): string | undefined => {
	if (!startersKnown(ctx.winner) || !startersKnown(ctx.loser)) {
		return undefined;
	}
	const W = cap(theNick(ctx.winner));
	const wn = theNick(ctx.winner);
	const L = cap(theNick(ctx.loser));
	const ln = theNick(ctx.loser);
	const wb = benchPoints(ctx.winner);
	const lb = benchPoints(ctx.loser);
	if (wb >= 45 && wb - lb >= 20) {
		return pick(
			rng,
			[
				`${W} got ${wb} points from the bench, to ${lb} for ${ln}.`,
				`The ${nick(ctx.winner)} bench outscored ${poss(ln)} ${wb}-${lb}.`,
				`${poss(W)} reserves chipped in ${wb} points, ${wb - lb} more than ${poss(ln)} did.`,
				`The bench was the difference: ${wb} points from ${poss(wn)} reserves, ${lb} from ${poss(ln)}.`,
				`${W} went deeper: ${wb} bench points to ${lb}.`,
			],
			"benchEdge",
		);
	}
	if (lb >= 50 && lb - wb >= 20) {
		return pick(
			rng,
			[
				`${L} got ${lb} points from their bench, and it still was not enough.`,
				`${poss(L)} reserves outscored ${poss(wn)} ${lb}-${wb} in a losing cause.`,
				`The ${nick(ctx.loser)} bench had ${lb} points, ${lb - wb} more than ${poss(wn)}.`,
			],
			"benchLoser",
		);
	}
	const spark = ctx.winner.players
		.filter((p) => p.starter === false && p.pts >= 22 && !ctx.said.has(p.name))
		.sort((a, b) => b.pts - a.pts)[0];
	if (spark) {
		ctx.said.add(spark.name);
		return pick(
			rng,
			[
				`${spark.name} gave ${wn} ${plural(spark.pts, "point")} off the bench.`,
				`${spark.name} ${scoredVerb(rng)} ${statPhrase(spark)} in a reserve role.`,
				`Off the bench, ${spark.name} ${scoredVerb(rng)} ${statPhrase(spark)}.`,
			],
			"benchSpark",
		);
	}
	return undefined;
};

// ---------------------------------------------------------------- UP NEXT

export const nextGameBeat = (
	ctx: BeatContext,
	rng: Rng,
): string | undefined => {
	if (ctx.game.playoffs) {
		return undefined;
	}
	// Most recaps close on it, not all - a page where every piece ends the
	// same way reads as a form.
	if (rng() < 0.4) {
		return undefined;
	}
	const usable = (t: RecapTeam) => {
		const n = t.nextGame;
		return n && n.oppName && n.daysAway >= 1 ? n : undefined;
	};
	const wNext = usable(ctx.winner);
	const lNext = usable(ctx.loser);
	if (!wNext && !lNext) {
		return undefined;
	}
	const when = (daysAway: number) =>
		daysAway === 1 ? "tomorrow" : `in ${numWord(daysAway)} days`;
	const line = (t: RecapTeam, n: NonNullable<RecapTeam["nextGame"]>) => {
		const T = cap(theNick(t));
		const tn = theNick(t);
		const opp = `the ${n.oppName}`;
		const options = [
			`${T} are next in action ${when(n.daysAway)}, ${n.home ? "at home against" : "on the road against"} ${opp}.`,
			`Up next for ${tn}: ${opp}, ${n.home ? "at home" : "away"}, ${when(n.daysAway)}.`,
			`${T} ${n.home ? "host" : "visit"} ${opp} ${when(n.daysAway)}.`,
			`Next up for ${tn} is ${opp} ${when(n.daysAway)}, ${n.home ? "at home" : "on the road"}.`,
			`${T} get ${opp} ${when(n.daysAway)}.`,
		];
		if (n.daysAway === 1) {
			options.push(
				`${T} turn around and ${n.home ? "host" : "visit"} ${opp} tomorrow.`,
			);
		}
		return pick(rng, options, "nextGame");
	};
	if (wNext && lNext && rng() < 0.3) {
		const W = cap(theNick(ctx.winner));
		const ln = theNick(ctx.loser);
		return `${W} ${wNext.home ? "host" : "visit"} the ${wNext.oppName} ${when(wNext.daysAway)}; ${ln} ${lNext.home ? "host" : "visit"} the ${lNext.oppName} ${when(lNext.daysAway)}.`;
	}
	if (wNext && (!lNext || rng() < 0.75)) {
		return line(ctx.winner, wNext);
	}
	return lNext ? line(ctx.loser, lNext) : undefined;
};

// ---------------------------------------------------------------- THE NORM

// The scoreboard against what the two clubs had been doing all season. A
// 128-point night means one thing from a team averaging 96 and another from
// one averaging 124, and nothing in the recap could tell them apart.
export const scoringNormBeat = (
	ctx: BeatContext,
	rng: Rng,
): string | undefined => {
	if (ctx.game.playoffs) {
		return undefined;
	}
	const options: string[] = [];
	const W = cap(theNick(ctx.winner));
	const wn = theNick(ctx.winner);
	const L = cap(theNick(ctx.loser));
	const ln = theNick(ctx.loser);

	// The season-high beat has already described this team's scoring; a second
	// sentence measuring the same points against the same season is the same
	// observation with different arithmetic.
	const highTold =
		/season high|had not scored|most .* have scored|any game this season|more than they had in any/.test(
			ctx.written,
		);

	const wNorm = ctx.winner.norm;
	if (wNorm && wNorm.gp >= 12 && !highTold) {
		const over = Math.round(ctx.winner.pts - wNorm.pts);
		if (over >= 14) {
			options.push(
				pick(
					rng,
					[
						`${W} had been averaging ${wNorm.pts.toFixed(1)} a game coming in.`,
						// Every shape names the team: these land in a paragraph with
						// other clubs in it, and "That is 17 more" two sentences after
						// the last mention of them attaches to the wrong one.
						`The ${ctx.winner.pts} were ${numWord(over)} more than ${poss(wn)} season average of ${wNorm.pts.toFixed(1)}.`,
						`${W} scored ${numWord(over)} more than they had been managing on the season.`,
						`${W} came in averaging ${wNorm.pts.toFixed(1)} and beat it by ${numWord(over)}.`,
					],
					"normWinnerHigh",
				),
			);
		}
	}

	const lNorm = ctx.loser.norm;
	if (lNorm && lNorm.gp >= 12) {
		const under = Math.round(lNorm.pts - ctx.loser.pts);
		if (under >= 14) {
			options.push(
				pick(
					rng,
					[
						`${L} came in averaging ${lNorm.pts.toFixed(1)} and never got near it.`,
						`It left ${ln} ${numWord(under)} short of their season average.`,
						`${L} had been scoring ${lNorm.pts.toFixed(1)} a game; tonight they managed ${ctx.loser.pts}.`,
					],
					"normLoserLow",
				),
			);
		}
	}

	// A defensive night measured against what the other side usually gets.
	if (
		lNorm &&
		lNorm.gp >= 12 &&
		lNorm.pts - ctx.loser.pts >= 10 &&
		ctx.margin >= 8
	) {
		options.push(
			pick(
				rng,
				[
					`${W} held an offense averaging ${lNorm.pts.toFixed(1)} to ${ctx.loser.pts}.`,
					`Holding ${ln} to ${ctx.loser.pts} was ${numWord(Math.round(lNorm.pts - ctx.loser.pts))} below what they usually get.`,
					`${cap(ln)} were kept well under their ${lNorm.pts.toFixed(1)} a night.`,
				],
				"normDefense",
			),
		);
	}

	return options.length > 0 ? pick(rng, options) : undefined;
};

// ---------------------------------------------------------------- THE MATCHUP

// What the star had done to this opponent before tonight. Two shapes of
// story: he keeps doing it, or he had never done it until now.
export const vsOpponentBeat = (
	p: RecapPlayer,
	oppName: string,
	rng: Rng,
): string | undefined => {
	const v = p.vsOpponent;
	if (!v || v.games < 2 || p.pts < 20) {
		return undefined;
	}
	const opp = `the ${oppName}`;
	if (v.avgPts >= 24 && p.pts >= 24) {
		return pick(
			rng,
			[
				`${p.name} has made a habit of this against ${opp}, ${v.avgPts.toFixed(1)} a game in ${numWord(v.games)} earlier meetings.`,
				`${cap(opp)} have no answer for him: ${v.avgPts.toFixed(1)} a game from ${p.name} across their ${numWord(v.games)} previous meetings.`,
				`${p.name} had already been averaging ${v.avgPts.toFixed(1)} against ${opp} this season.`,
			],
			"vsOppHabit",
		);
	}
	if (p.pts >= v.bestPts + 8 && v.bestPts <= 18) {
		return pick(
			rng,
			[
				`${p.name} had not managed more than ${v.bestPts} against ${opp} this season.`,
				`His previous best against ${opp} was ${v.bestPts}.`,
				`${cap(opp)} had held ${p.name} to ${v.bestPts} or fewer in every earlier meeting.`,
			],
			"vsOppBreakout",
		);
	}
	return undefined;
};

// ---------------------------------------------------------------- THE NIGHT
//
// The day wrap's own beats. A league wrap that lists results tells you what
// happened; these tell you what it meant to the season - who moved in the
// table, where the race stands at the cut line, who is hot and who cannot buy
// a win, the round numbers passed, the season highs set, and what is on
// tomorrow. Same rules as the game beats: pooled phrasings, nothing said
// twice, undefined when the night gives them nothing.

export type DayBeatContext = {
	games: RecapGame[];
	standings?: {
		playoffSpots?: number;
		confs: {
			name: string;
			teams: {
				tid?: number;
				name?: string;
				abbrev: string;
				rank: number;
				won: number;
				lost: number;
				gb: number;
			}[];
		}[];
	};
	// Teams and players the wrap has already given a sentence to.
	saidTids: Set<number>;
	saidPlayers: Set<string>;
};

const realGames = (games: RecapGame[]) => games.filter((g) => !g.allStar);

const sidesOf = (game: RecapGame) => {
	const [home, away] = game.teams;
	const winner = game.winnerTid === home.tid ? home : away;
	const loser = winner === home ? away : home;
	return { winner, loser };
};

// Enough of a sample for a rank to mean anything.
const MIN_GAMES_FOR_RACE = 15;

// Who moved in the playoff picture tonight. Only inside it: a climb to
// thirteenth is not news, and neither is a slide between two lottery places.
export const dayStandingsMovers = (
	ctx: DayBeatContext,
	rng: Rng,
): string | undefined => {
	type Move = { text: string; tid: number; size: number };
	const moves: Move[] = [];
	for (const game of realGames(ctx.games)) {
		if (game.playoffs) {
			continue;
		}
		for (const t of game.teams) {
			const st = t.standing;
			if (
				!st ||
				st.rankBefore === undefined ||
				st.rank === st.rankBefore ||
				st.won + st.lost < MIN_GAMES_FOR_RACE ||
				ctx.saidTids.has(t.tid)
			) {
				continue;
			}
			const spots = ctx.standings?.playoffSpots ?? Math.ceil(st.teams / 2);
			const up = st.rank < st.rankBefore;
			const intoThePicture = up && st.rank <= spots && st.rankBefore > spots;
			const outOfIt = !up && st.rank > spots && st.rankBefore <= spots;
			// No commas inside a clause: these are joined into one list, and a
			// clause carrying its own comma runs the list together.
			if (intoThePicture) {
				moves.push({
					tid: t.tid,
					size: 3,
					text: `${theNick(t)} moved into the ${st.conf} places at ${ordinal(st.rank)}`,
				});
			} else if (outOfIt) {
				moves.push({
					tid: t.tid,
					size: 3,
					text: `${theNick(t)} slid out of the ${st.conf} places to ${ordinal(st.rank)}`,
				});
			} else if (st.rank <= spots && up) {
				moves.push({
					tid: t.tid,
					size: st.rank <= 3 ? 2 : 1,
					text: `${theNick(t)} climbed to ${ordinal(st.rank)} in the ${st.conf}`,
				});
			}
		}
	}
	if (moves.length === 0) {
		return undefined;
	}
	moves.sort((a, b) => b.size - a.size);
	const top = moves.slice(0, 3);
	for (const m of top) {
		ctx.saidTids.add(m.tid);
	}
	const list = naturalList(top.map((m) => m.text));
	return pick(
		rng,
		[
			`${cap(list)}.`,
			`The table moved with them: ${list}.`,
			`In the standings, ${list}.`,
			`It shuffled the order too - ${list}.`,
		],
		"dayMovers",
	);
};

// The cut line: who is holding the last playoff place and by how much.
export const dayRaceSentence = (
	ctx: DayBeatContext,
	rng: Rng,
): string | undefined => {
	const standings = ctx.standings;
	const spots = standings?.playoffSpots;
	if (!standings || !spots) {
		return undefined;
	}
	const bits: string[] = [];
	for (const conf of standings.confs) {
		const last = conf.teams.find((t) => t.rank === spots);
		const first = conf.teams.find((t) => t.rank === spots + 1);
		if (!last || !first || last.won + last.lost < MIN_GAMES_FOR_RACE) {
			continue;
		}
		const gap = Math.round((first.gb - last.gb) * 2) / 2;
		const lastName = last.name ? `the ${last.name}` : last.abbrev;
		const firstName = first.name ? `the ${first.name}` : first.abbrev;
		if (gap <= 0) {
			bits.push(
				`${lastName} and ${firstName} are level for the last ${conf.name} place`,
			);
		} else if (gap <= 3) {
			bits.push(
				`${lastName} hold the last ${conf.name} place by ${gbText(gap)} over ${firstName}`,
			);
		}
	}
	if (bits.length === 0) {
		return undefined;
	}
	return pick(
		rng,
		[
			`On the cut line, ${naturalList(bits)}.`,
			`The race for the last places: ${naturalList(bits)}.`,
			`At the bottom of the bracket, ${naturalList(bits)}.`,
			`Down at the cut line, ${naturalList(bits)}.`,
		],
		"dayRace",
	);
};

// The team nobody can beat, and the team that cannot win. The hot side is
// already covered by the wrap's own streak sentence; this is the cold one,
// which had no voice at all.
export const dayColdStreak = (
	ctx: DayBeatContext,
	rng: Rng,
): string | undefined => {
	let worst: { team: RecapTeam; count: number } | undefined;
	for (const game of realGames(ctx.games)) {
		const { loser } = sidesOf(game);
		const s = loser.streak;
		if (
			s &&
			!s.won &&
			s.count >= 6 &&
			!ctx.saidTids.has(loser.tid) &&
			(!worst || s.count > worst.count)
		) {
			worst = { team: loser, count: s.count };
		}
	}
	if (!worst) {
		return undefined;
	}
	ctx.saidTids.add(worst.team.tid);
	const t = theNick(worst.team);
	const T = cap(t);
	return pick(
		rng,
		[
			`${T} have now lost ${plural(worst.count, "straight")}.`,
			`That is ${plural(worst.count, "loss")} in a row for ${t}.`,
			`${T} have not won in ${numWord(worst.count)} games.`,
			`The skid reached ${numWord(worst.count)} for ${t}.`,
		],
		"dayCold",
	);
};

// Round numbers passed across the league tonight.
export const dayMilestones = (
	ctx: DayBeatContext,
	rng: Rng,
): string | undefined => {
	type Hit = { text: string; name: string; weight: number };
	const hits: Hit[] = [];
	for (const game of realGames(ctx.games)) {
		for (const t of game.teams) {
			for (const p of t.players) {
				const m = p.milestone;
				if (!m || ctx.saidPlayers.has(p.name)) {
					continue;
				}
				const word = STAT_WORD[m.stat];
				hits.push({
					name: p.name,
					weight: (m.scope === "career" ? 100 : 0) + m.mark / 100,
					text:
						m.scope === "career"
							? `${p.name} went past ${fmtNum(m.mark)} career ${word}`
							: `${p.name} went past ${fmtNum(m.mark)} ${word} for the season`,
				});
			}
		}
	}
	if (hits.length === 0) {
		return undefined;
	}
	hits.sort((a, b) => b.weight - a.weight);
	const top = hits.slice(0, 2);
	for (const h of top) {
		ctx.saidPlayers.add(h.name);
	}
	const list = naturalList(top.map((h) => h.text));
	return pick(
		rng,
		[
			`${cap(list)}.`,
			`Milestones on the night: ${list}.`,
			`Round numbers fell too - ${list}.`,
			`Somewhere in it all, ${list}.`,
		],
		"dayMilestones",
	);
};

// Season highs set across the league tonight.
export const daySeasonHighs = (
	ctx: DayBeatContext,
	rng: Rng,
): string | undefined => {
	const bits: string[] = [];
	const names: string[] = [];
	for (const game of realGames(ctx.games)) {
		if (game.playoffs) {
			continue;
		}
		const { winner } = sidesOf(game);
		const h = winner.seasonHighs;
		if (
			h?.leaguePts &&
			h.priorGames >= MIN_GAMES_FOR_RACE &&
			!ctx.saidTids.has(winner.tid)
		) {
			ctx.saidTids.add(winner.tid);
			bits.push(
				`${poss(theNick(winner))} ${winner.pts} were the most any team has scored this season`,
			);
		}
		for (const t of game.teams) {
			for (const p of t.players) {
				const e = p.entering;
				if (
					!e ||
					e.gp < 15 ||
					ctx.saidPlayers.has(p.name) ||
					names.length >= 2 ||
					!(p.pts >= 30 && p.pts > e.high.pts && p.pts - e.high.pts >= 3)
				) {
					continue;
				}
				ctx.saidPlayers.add(p.name);
				names.push(
					`${p.name} set a season high with ${plural(p.pts, "point")}`,
				);
			}
		}
	}
	const all = [...bits, ...names];
	if (all.length === 0) {
		return undefined;
	}
	return pick(
		rng,
		[
			`${cap(naturalList(all))}.`,
			`Season bests on the night: ${naturalList(all)}.`,
			`It was a night for career-best sort of numbers - ${naturalList(all)}.`,
		],
		"dayHighs",
	);
};

// What is on tomorrow, from the teams that played tonight. The pick is the
// matchup between the two best records, because that is the one a reader
// would circle.
export const dayTomorrow = (
	ctx: DayBeatContext,
	rng: Rng,
): string | undefined => {
	type Fixture = { home: RecapTeam; awayName: string; weight: number };
	const fixtures: Fixture[] = [];
	for (const game of realGames(ctx.games)) {
		if (game.playoffs) {
			continue;
		}
		for (const t of game.teams) {
			const n = t.nextGame;
			if (!n || n.daysAway !== 1 || !n.home || !n.oppName) {
				continue;
			}
			const rec = t.record;
			const wpct =
				rec && rec.won + rec.lost > 0 ? rec.won / (rec.won + rec.lost) : 0;
			fixtures.push({ home: t, awayName: n.oppName, weight: wpct });
		}
	}
	if (fixtures.length === 0) {
		return undefined;
	}
	fixtures.sort((a, b) => b.weight - a.weight);
	const best = fixtures[0]!;
	const rest = fixtures.length - 1;
	const matchup = `the ${best.awayName} at ${theNick(best.home)}`;
	const restWord = `${numWord(rest)} ${rest === 1 ? "other" : "others"}`;
	const tail =
		rest > 0
			? pick(
					rng,
					[
						` and ${numWord(rest)} ${rest === 1 ? "other game" : "other games"}`,
						`, with ${restWord} on the slate`,
					],
					"dayTomorrowTail",
				)
			: "";
	return pick(
		rng,
		[
			`Tomorrow brings ${matchup}${tail}.`,
			`Up tomorrow: ${matchup}${tail}.`,
			`Tomorrow night, ${matchup}${tail}.`,
			`Next up on the schedule is ${matchup}${tail}.`,
		],
		"dayTomorrow",
	);
};

// ---------------------------------------------------------------- THE BRACKET

// The postseason day wrap's closing context. Which series are on the brink,
// which are level, and who plays for their season next. The wrap already
// narrates the night's series results; this is what they set up.
export const dayBracketWatch = (
	ctx: DayBeatContext,
	rng: Rng,
): string | undefined => {
	type Watch = { text: string; weight: number };
	const out: Watch[] = [];
	for (const game of ctx.games) {
		const s = game.series;
		if (!s || typeof s.bestOf !== "number" || s.bestOf <= 1) {
			continue;
		}
		const { winner, loser } = sidesOf(game);
		if (ctx.saidTids.has(winner.tid) && ctx.saidTids.has(loser.tid)) {
			continue;
		}
		const winnerIsHome = winner.abbrev === s.homeAbbrev;
		const wAfter = (winnerIsHome ? s.homeWon : s.awayWon) + 1;
		const lAfter = winnerIsHome ? s.awayWon : s.homeWon;
		const need = Math.floor(s.bestOf / 2) + 1;
		const nextGame = wAfter + lAfter + 1;
		if (wAfter >= need) {
			continue; // the series is over; the wrap has it as a result
		}
		const w = theNick(winner);
		const l = theNick(loser);
		if (wAfter === need - 1 && lAfter === need - 1) {
			out.push({
				weight: 4,
				text: `${w} and ${l} play a decider in Game ${nextGame}`,
			});
		} else if (wAfter === need - 1) {
			out.push({
				weight: 3,
				text: `${l} face elimination in Game ${nextGame}`,
			});
		} else if (wAfter === lAfter) {
			out.push({
				weight: 1,
				text: `${w} and ${l} are level at ${wAfter}-${wAfter}`,
			});
		}
		ctx.saidTids.add(winner.tid);
		ctx.saidTids.add(loser.tid);
	}
	if (out.length === 0) {
		return undefined;
	}
	out.sort((a, b) => b.weight - a.weight);
	const list = naturalList(out.slice(0, 3).map((x) => x.text));
	return pick(
		rng,
		[
			`Looking ahead, ${list}.`,
			`What it sets up: ${list}.`,
			`Next time out, ${list}.`,
			`The bracket now: ${list}.`,
		],
		"dayBracket",
	);
};
