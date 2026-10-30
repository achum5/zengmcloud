// HOW A CLOSE GAME ENDED, IN THE ORDER IT HAPPENED.
//
// The one paragraph a reader of a tight game actually wants - who tied it,
// who put the winner in front, what the losers got back, who closed it out
// at the line - could not be written from a box score, and the flow summary
// alone gave it one sentence: "took the lead for good with 47.7 seconds
// left". The sim now keeps every score from the last two minutes of
// regulation and all of overtime (common/gameFlow.ts, `finish`), and this
// module reads that list the way a reporter reads his notebook: the score
// with two minutes to go, the last tie, the go-ahead shot, the answer that
// fell short, the free throws that sealed it.
//
// Every sentence names a score the list contains, at a clock it contains, by
// a man it contains. Nothing is inferred about what was missed in between -
// the log has makes only - so a possession that came up empty is never
// described, and "cut it to 111-110" is said only when that is the score the
// basket left.

import {
	clockLeft,
	type FinishEvent,
	type GameFlowSide,
} from "../../common/gameFlow.ts";
import type { RecapGame, RecapTeam } from "./getDayGamesForRecap.ts";
import { aOrdinal, cap, pick, poss, stripHtml, theNick } from "./recapText.ts";

type Rng = () => number;

export type FinishInput = {
	game: RecapGame;
	winner: RecapTeam;
	loser: RecapTeam;
	regPeriods: number;
	// The lede already described the winning shot, by name and clock, so the
	// sequence refers back to it rather than describing it a second time.
	shotTold: boolean;
	// The man the sentence before this paragraph was about, when there is
	// one, so a sentence opening on his name can open on "his" instead.
	justNamed?: string;
	// The sentence before opens on "It was", so the finish does not open on
	// it again: "It was an 11-point game going into the fourth... It was
	// 117-117 with two minutes to play."
	afterItWas?: boolean;
	// The sentence before ends on the losers ("...but the Nuggets took the
	// fourth quarter 37-21"), so their man needs no club.
	losersJustNamed?: boolean;
};

// "layup", "jumper", "three-pointer"; an and-one is the play it made.
export const shotName = (e: Pick<FinishEvent, "kind" | "andOne" | "pts">) => {
	if (e.kind === "ft") {
		return e.pts === 1
			? "free throw"
			: e.pts === 2
				? "two free throws"
				: e.pts === 3
					? "three free throws"
					: `${e.pts} free throws`;
	}
	if (e.andOne) {
		return e.kind === "tp" ? "four-point play" : "three-point play";
	}
	switch (e.kind) {
		case "rim":
			return "layup";
		case "post":
			return "hook shot";
		case "mid":
			return "jumper";
		case "tp":
			return "three-pointer";
	}
};

// "with 47.7 seconds left", "with 1:12 to go", "at the buzzer".
const when = (e: FinishEvent, rng: Rng): string => {
	if (e.clock < 0.5) {
		return e.kind === "ft" ? "with no time left" : "at the buzzer";
	}
	const left = clockLeft(e.clock);
	if (!left) {
		return "";
	}
	return pick(
		rng,
		[`with ${left} left`, `with ${left} to go`, `with ${left} to play`],
		"finishWhen",
	);
};

// The shot as a noun phrase with its owner: "Cade King's layup", or "his
// layup" when the man was the previous sentence's subject.
const shotBy = (name: string, e: FinishEvent, justNamed?: string) =>
	name === justNamed ? `his ${shotName(e)}` : `${poss(name)} ${shotName(e)}`;

const nameOf = (game: RecapGame, pid: number | undefined) => {
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

// The two sides' scores from an event, winner first.
const scoreOf = (e: FinishEvent, wSide: GameFlowSide): [number, number] =>
	wSide === 0 ? [e.score[0], e.score[1]] : [e.score[1], e.score[0]];

// Whether a winner's free throws decided anything: the lead they were shot
// with was one possession or less. Two free throws with 3.7 seconds left
// did not "finish" a game already up nine. And in the last minute: "Quentin
// Ingram sealed it with two free throws with 3:44 left" was said of a game
// with eleven more Pelicans points still to come.
const mattered = (e: FinishEvent, wSide: GameFlowSide): boolean => {
	const [w, l] = scoreOf(e, wSide);
	return w - e.pts - l <= 3 && e.clock <= 60;
};

// The winners' lead at the end of the window.
const finalLead = (moments: { e: FinishEvent }[], wSide: GameFlowSide) => {
	const last = moments.at(-1);
	if (!last) {
		return 0;
	}
	const [w, l] = scoreOf(last.e, wSide);
	return w - l;
};

// The same, leader first - "cut it to 121-117", never "made it 117-121".
const shown = (e: FinishEvent, wSide: GameFlowSide): [number, number] => {
	const [w, l] = scoreOf(e, wSide);
	return w >= l ? [w, l] : [l, w];
};

// "a layup", "two free throws", "a free throw".
const aShot = (e: FinishEvent): string => {
	const name = shotName(e);
	return /free throws$/.test(name) ? name : `a ${name}`;
};

// "double" in the sim's "to force double overtime": which overtime it was.
const OT_COUNTER: Record<string, number> = {
	"": 1,
	double: 2,
	triple: 3,
	quadruple: 4,
	quintuple: 5,
	sextuple: 6,
	septuple: 7,
};

type ForcingTie = {
	name: string;
	shot: string;
	clock?: string;
	buzzer: boolean;
	// The log's row for it, when the log reaches back that far.
	e?: FinishEvent;
};

// The game-tying shot that forced the nth overtime (1 = the first).
//
// From the sim's own note - "X made a three-pointer with 4.2 seconds
// remaining to force overtime", "...to force double overtime" - matched
// to the overtime it names. Taking the first "to force" note there was
// for the first overtime put the second overtime's tie at the end of
// regulation.
//
// The sim writes that note only for a shot in the last seconds. A tie with
// 38 seconds left that nobody broke forced overtime just the same, and
// without it the story went from "up 4 going into the fourth" to "Bennedict
// King's layup with 1:40 to play made it 108-108" - an overtime basket read
// as one in regulation. The log has the period's last score, which is that
// tie. (It has every overtime score, up to its cap, which can leave out the
// first of three; the note covers that one.)
const forcingTie = (
	game: RecapGame,
	regPeriods: number,
	n: number,
): ForcingTie | undefined => {
	const period = regPeriods + n - 1;
	const fromLog = (): ForcingTie | undefined => {
		const e = endingTie(game, period);
		const name = e ? nameOf(game, e.pid) : undefined;
		return e && name
			? {
					name,
					shot: shotName(e),
					clock: clockLeft(e.clock),
					buzzer: e.clock < 0.5,
					e,
				}
			: undefined;
	};
	for (const raw of game.clutchPlays) {
		const text = stripHtml(raw);
		const m =
			/^(.+?) made (a [ a-z-]+?|two free throws|three free throws)(?: with ([\d.:]+) seconds remaining| at the buzzer| with no time on the clock)? to force\s+([a-z]*)\s*overtime/.exec(
				text,
			);
		if (!m || OT_COUNTER[m[4]!] !== n) {
			continue;
		}
		// Past the first overtime the log's row says it with the score.
		const logged = n > 1 ? fromLog() : undefined;
		if (logged) {
			return logged;
		}
		let shot = m[2]!.replace(/^a /, "");
		const secs = m[3] ? Number.parseFloat(m[3].replace(":", ".")) : undefined;
		// The sim's note says "basket"; the log knows what it was.
		if (shot === "basket" && secs !== undefined) {
			const e = (game.flow?.finish ?? []).find(
				(x) =>
					x.period === period &&
					x.kind !== "ft" &&
					Math.abs(x.clock - secs) < 0.15,
			);
			if (e) {
				shot = shotName(e);
			}
		}
		return {
			name: m[1]!.trim(),
			shot,
			clock:
				secs !== undefined && Number.isFinite(secs)
					? clockLeft(secs)
					: undefined,
			buzzer: /at the buzzer|no time on the clock/.test(text),
		};
	}
	return fromLog();
};

// The score a period ended on, when it was a tie: its last basket. Only
// for a period that went to overtime.
const endingTie = (
	game: RecapGame,
	period: number,
): FinishEvent | undefined => {
	const last = (game.flow?.finish ?? [])
		.filter((e) => e.period === period && e.pts > 0)
		.at(-1);
	return last && last.score[0] === last.score[1] ? last : undefined;
};

// tie: level. goAhead: the winner took the lead. extend: the winner added to
// a lead. cut: the losers closed on a winner's lead. loserLead: the losers
// took the lead. loserExtend: the losers added to theirs. winnerCut: the
// winner closed on a losers' lead.
type Role =
	| "tie"
	| "loserLead"
	| "loserExtend"
	| "goAhead"
	| "cut"
	| "extend"
	| "winnerCut";

type Moment = { e: FinishEvent; role: Role; name?: string; margin: number };

// Each score in the window, read for what it did to the game.
const classify = (
	events: readonly FinishEvent[],
	wSide: GameFlowSide,
	game: RecapGame,
): Moment[] => {
	const out: Moment[] = [];
	for (const e of events) {
		// The log holds makes only; a zero is a malformed row, not a play.
		if (e.pts <= 0) {
			continue;
		}
		const [w, l] = scoreOf(e, wSide);
		const after = w - l;
		const before = after - (e.side === wSide ? e.pts : -e.pts);
		let role: Role;
		if (after === 0) {
			role = "tie";
		} else if (e.side === wSide) {
			role = after < 0 ? "winnerCut" : before <= 0 ? "goAhead" : "extend";
		} else {
			role = after > 0 ? "cut" : before >= 0 ? "loserLead" : "loserExtend";
		}
		out.push({ e, role, name: nameOf(game, e.pid), margin: after });
	}
	return out;
};

// The losers' biggest lead in the window before the go-ahead score, at the
// last moment it was that big, when it was four or more inside the last
// minute and a half; whether the winners' points from there to the go-ahead
// came unanswered; and the winners' last basket that cut it short of the lead.
const lateRally = (
	moments: Moment[],
	goAheadIdx: number,
	wSide: GameFlowSide,
):
	| { peak: Moment; unanswered: boolean; pts: number; lastCut?: Moment }
	| undefined => {
	let peakIdx = -1;
	for (let i = 0; i < goAheadIdx; i++) {
		const m = moments[i]!;
		if (
			m.e.side !== wSide &&
			m.e.clock <= 90 &&
			(peakIdx < 0 || m.margin <= moments[peakIdx]!.margin)
		) {
			peakIdx = i;
		}
	}
	const peak = peakIdx >= 0 ? moments[peakIdx] : undefined;
	if (!peak || -peak.margin < 4) {
		return undefined;
	}
	const after = moments.slice(peakIdx + 1, goAheadIdx + 1);
	return {
		peak,
		unanswered: after.every((m) => m.e.side === wSide),
		pts: after.reduce((a, m) => a + m.e.pts, 0),
		lastCut: after
			.slice(0, -1)
			.filter((m) => m.role === "winnerCut" && m.margin >= -3)
			.at(-1),
	};
};

// The sequence, as up to three sentences. Empty when the closing scores
// hold no drama - a six-point win where the losers' last basket came with
// the game decided is not a finish worth narrating.
export const finishStory = (input: FinishInput, rng: Rng): string[] => {
	const { game, winner, loser, regPeriods, shotTold } = input;
	const finish = game.flow?.finish;
	if (!finish || finish.length === 0) {
		return [];
	}
	const wSide: GameFlowSide = game.teams[0].tid === winner.tid ? 0 : 1;
	const W = theNick(winner);
	const L = theNick(loser);
	const ot = game.overtimes > 0;

	// In overtime the story is overtime; regulation's last two minutes are
	// summarized by the shot that forced it.
	const window = ot
		? finish.filter((e) => e.period > regPeriods)
		: finish.filter((e) => e.period === regPeriods);
	const moments = classify(window, wSide, game);

	const out: string[] = [];
	let justNamed = input.justNamed;
	let itWas = input.afterItWas ?? false;
	const say = (text: string, name?: string) => {
		out.push(text);
		justNamed = name;
		itWas = /^It was\b/.test(text);
	};

	// The last time the winner took a lead it never gave back, inside the
	// window. Everything before it is the fight; everything after is the
	// hold.
	let goAheadIdx = -1;
	for (let i = moments.length - 1; i >= 0; i--) {
		const m = moments[i]!;
		if (
			m.role === "tie" ||
			m.role === "loserLead" ||
			m.role === "loserExtend" ||
			m.role === "winnerCut"
		) {
			break;
		}
		if (m.role === "goAhead") {
			goAheadIdx = i;
		}
	}
	const goAhead = goAheadIdx >= 0 ? moments[goAheadIdx]! : undefined;
	// What the go-ahead shot answered: the most recent tie or lead the
	// losers held before it.
	const before = goAhead
		? moments
				.slice(0, goAheadIdx)
				.reverse()
				.find((m) => m.role === "tie" || m.role === "loserLead")
		: undefined;

	// " with 4.2 seconds left", " at the buzzer", from a forcing tie.
	const tieClock = (tie: ForcingTie) =>
		tie.buzzer
			? tie.shot.includes("free throw")
				? " with no time left"
				: " at the buzzer"
			: tie.clock
				? ` with ${tie.clock} left`
				: "";

	if (ot) {
		const tie = forcingTie(game, regPeriods, 1);
		if (tie) {
			const clock = tieClock(tie);
			const shot = tie.shot;
			const aShot = /free throws$/.test(shot) ? shot : `a ${shot}`;
			// The losers' man is theirs: "They were still 10 up going into the
			// fourth. Bennedict Dunn forced overtime on a three-pointer at the
			// buzzer" read as one of "they".
			const losersTie =
				!input.losersJustNamed &&
				loser.players.some((p) => p.name === tie.name);
			say(
				pick(
					rng,
					losersTie
						? [
								`${cap(poss(L))} ${tie.name} forced overtime on ${aShot}${clock}.`,
								`It took ${poss(tie.name)} ${shot}${clock} for ${L} to force ${game.overtimes > 1 ? "overtime" : "the extra period"}.`,
							]
						: [
								`${cap(poss(tie.name))} ${shot}${clock} sent it to overtime.`,
								`It took ${poss(tie.name)} ${shot}${clock} to force ${game.overtimes > 1 ? "overtime" : "the extra period"}.`,
								`${tie.name} forced overtime on ${aShot}${clock}.`,
							],
					"finishForcedOt",
				),
				tie.name,
			);
		}
		// Every overtime but the last ended tied too, and each of those ties
		// is told in order. Without them a double-overtime story went from
		// "sent it to overtime" straight to "The Kings led 108-106 on Lonnie
		// Lowry's layup with 4:51 to play" - a basket in the SECOND
		// overtime, with the whole first one, and the free throw that sent
		// it on, never mentioned. The tie the go-ahead basket answered is
		// told below, with that basket.
		for (let n = 2; n <= game.overtimes; n++) {
			const later = forcingTie(game, regPeriods, n);
			if (!later || (later.e && before?.e === later.e)) {
				continue;
			}
			const by =
				later.name === justNamed
					? `his ${later.shot}`
					: `${poss(later.name)} ${later.shot}`;
			say(
				later.e
					? `${cap(by)} ${when(later.e, rng)} tied it at ${later.e.score[0]} and forced ${aOrdinal(n)} overtime.`
					: `${cap(by)}${tieClock(later)} forced ${aOrdinal(n)} overtime.`,
				later.name,
			);
		}
	}

	if (goAhead) {
		if (before?.name) {
			const [w, l] = scoreOf(before.e, wSide);
			const shot = shotBy(before.name, before.e, justNamed);
			// Past perfect when the lede has already jumped ahead to the
			// winning shot: the tie is being told out of order.
			const had = shotTold ? "had " : "";
			if (before.role === "tie" && ot && before.e.period < goAhead.e.period) {
				// The tie that ended an overtime and forced the next. Told as
				// any other tie, "Lonnie Ingram's three-pointer with 0.9 seconds
				// left made it 97-97" sat between the shot that forced the first
				// overtime and the second overtime's go-ahead basket, and the
				// reader could not tell there had been a second.
				say(
					`${cap(shot)} ${when(before.e, rng)} ${had}tied it at ${w} and forced ${aOrdinal(before.e.period - regPeriods + 1)} overtime.`,
					before.name,
				);
			} else if (before.role === "tie") {
				const tied = [
					`${cap(shot)} ${had}tied it at ${w} ${when(before.e, rng)}.`,
					`${cap(shot)} ${when(before.e, rng)} ${had}made it ${w}-${w}.`,
				];
				say(
					itWas
						? pick(rng, tied, "finishTieNoIt")
						: pick(
								rng,
								[
									...tied,
									`It was ${w}-${w} after ${shot} ${when(before.e, rng)}.`,
								],
								"finishTie",
							),
					before.name,
				);
			} else {
				say(
					pick(
						rng,
						[
							`${cap(shot)} ${had}put ${L} up ${l}-${w} ${when(before.e, rng)}.`,
							`${cap(L)} ${shotTold ? "had " : ""}led ${l}-${w} on ${shot} ${when(before.e, rng)}.`,
						],
						"finishLoserLead",
					),
					before.name,
				);
			}
		} else if (!ot && lateRally(moments, goAheadIdx, wSide)) {
			// The losers were in front all through the window until the
			// go-ahead score, and by a real margin late: how far back the
			// winners came, and how late, is the story. "The Jazz were up
			// 100-91 with two minutes left" and then the winning three said
			// nothing of the 103-94 with 56.8 seconds to go.
			const rally = lateRally(moments, goAheadIdx, wSide)!;
			const [w, l] = scoreOf(rally.peak.e, wSide);
			const had = shotTold ? "had " : "";
			const at = when(rally.peak.e, rng);
			if (rally.unanswered) {
				say(
					pick(
						rng,
						[
							`${cap(L)} ${had}led ${l}-${w} ${at} before ${W} scored the next ${rally.pts} points.`,
							`${cap(W)} ${shotTold ? "had trailed" : "trailed"} ${l}-${w} ${at} and scored the next ${rally.pts} points.`,
						],
						"finishRally",
					),
				);
			} else {
				say(`${cap(L)} ${shotTold ? "had been" : "were"} up ${l}-${w} ${at}.`);
				const cut = rally.lastCut;
				if (cut?.name) {
					const [cw, cl] = scoreOf(cut.e, wSide);
					say(
						`${cap(shotBy(cut.name, cut.e, justNamed))} ${when(cut.e, rng)} ${had}cut it to ${cl}-${cw}.`,
						cut.name,
					);
				}
			}
		} else if (!ot) {
			// Nothing in the window before the go-ahead score: the losers were
			// in front or level at the two-minute mark and did not score
			// again before it.
			const two = game.flow?.late?.find((m) => m.clock === 120);
			if (two) {
				const l = two.pts[1 - wSide]!;
				const w = two.pts[wSide]!;
				// The basket that won it is the lede's, so nothing between the
				// two-minute score and the last one says how it turned: "The
				// Spurs were up 120-116 with two minutes left. Darius Webb's
				// free throw with no time left made the final 129-126." The
				// two minutes, in two numbers: "The Nuggets outscored them 13-6
				// from there."
				const closing =
					shotTold && l >= w
						? `${cap(W)} outscored ${l > w ? "them" : L} ${winner.pts - w}-${loser.pts - l} from there.`
						: undefined;
				if (l > w) {
					say(
						pick(
							rng,
							[
								`${cap(L)} led ${l}-${w} with two minutes to go.`,
								`${cap(L)} were up ${l}-${w} with two minutes left.`,
							],
							"finishTwoMinutes",
						),
					);
				} else if (l === w) {
					say(
						itWas
							? `With two minutes to play, it was ${w}-${w}.`
							: `It was ${w}-${w} with two minutes to play.`,
					);
				}
				if (closing) {
					say(closing);
				}
			}
		}

		// The pass that set up the decisive basket, when the log has it and
		// the basket was the one that decided it - in the basket's own
		// sentence. Its own ("Anthony Brooks set it up.") hung between the
		// go-ahead three and the free throws like a caption.
		const setUp = nameOf(game, goAhead.e.ast);
		const feed =
			!shotTold &&
			setUp &&
			setUp !== goAhead.name &&
			goAhead.e.kind !== "ft" &&
			(goAhead.e.clock <= 60 || goAheadIdx === moments.length - 1)
				? ` off a pass from ${setUp}`
				: "";
		if (goAhead.name && !shotTold) {
			const [w, l] = scoreOf(goAhead.e, wSide);
			const shot = `${shotBy(goAhead.name, goAhead.e, justNamed)}${feed}`;
			const isLast = goAheadIdx === moments.length - 1;
			// "Answered" only when the losers had just scored; when the same
			// side tied it and then went ahead, the second basket followed.
			const answered = before !== undefined && before.e.side !== wSide;
			const followed = before !== undefined && before.e.side === wSide;
			say(
				pick(
					rng,
					[
						`${cap(shot)} ${when(goAhead.e, rng)} put ${W} ahead ${w}-${l}${isLast ? "" : " for good"}.`,
						`${cap(W)} went in front ${isLast ? "to stay " : "for good "}on ${shot} ${when(goAhead.e, rng)}, ${w}-${l}.`,
						...(answered
							? [
									`Then ${shot} ${when(goAhead.e, rng)} made it ${w}-${l}.`,
									`${goAhead.name} answered with ${aShot(goAhead.e)}${feed} ${when(goAhead.e, rng)} that put ${W} up ${w}-${l}.`,
								]
							: followed
								? [
										`${cap(shot)} ${when(goAhead.e, rng)} then made it ${w}-${l}.`,
										`${cap(W)} took the lead on ${shot} ${when(goAhead.e, rng)}, ${w}-${l}.`,
									]
								: // "Never trailed again" says they had trailed.
									(game.flow?.maxLead[wSide === 0 ? 1 : 0] ?? 0) > 0
									? [
											`${cap(shot)} ${when(goAhead.e, rng)} made it ${w}-${l}, and ${W} never trailed again.`,
										]
									: []),
					],
					"finishGoAhead",
				),
				// The passer was named last: a "he" next would be his.
				feed ? undefined : goAhead.name,
			);
		}

		// What followed: the losers getting close, and the winner closing it.
		const after = moments.slice(goAheadIdx + 1);
		const cut = after
			.filter((m) => m.role === "cut" && m.margin <= 3 && m.name)
			.at(-1);
		const seal = after
			.filter(
				(m) =>
					m.e.side === wSide &&
					m.name &&
					m.e.kind === "ft" &&
					mattered(m.e, wSide),
			)
			.at(-1);
		const cutText = cut
			? (() => {
					const [w, l] = shown(cut.e, wSide);
					return pick(
						rng,
						[
							`${shotBy(cut.name!, cut.e, justNamed)} ${when(cut.e, rng)} cut it to ${w}-${l}`,
							`${shotBy(cut.name!, cut.e, justNamed)} pulled ${L} within ${cut.margin} ${when(cut.e, rng)}`,
							`${cut.name} got ${L} back to ${w}-${l} with ${aShot(cut.e)} ${when(cut.e, rng)}`,
						],
						"finishCut",
					);
				})()
			: undefined;
		const sealText =
			seal && (!cut || seal.e.clock < cut.e.clock)
				? (() => {
						const fts = aShot(seal.e);
						const bare = shotName(seal.e);
						return pick(
							rng,
							[
								`${seal.name} sealed it with ${fts} ${when(seal.e, rng)}`,
								`${seal.name} made ${fts} ${when(seal.e, rng)} to close it out`,
								`${poss(seal.name!)} ${bare} ${when(seal.e, rng)} finished it`,
							],
							"finishSeal",
						);
					})()
				: undefined;
		if (cutText && sealText) {
			say(`${cap(cutText)}, but ${sealText}.`, seal!.name);
		} else if (cut && cut.e.clock < 3) {
			// A basket at the horn, or with a second or two left, changes the
			// final and nothing else - "and the Pacers got no closer" after a
			// three with 1.1 seconds left is a sentence about nothing.
			const [w, l] = shown(cut.e, wSide);
			say(
				`${cap(shotBy(cut.name!, cut.e, justNamed))} ${when(cut.e, rng)} made the final ${w}-${l}.`,
				cut.name,
			);
		} else if (cutText && cut && finalLead(moments, wSide) >= cut.margin + 4) {
			say(`${cap(cutText)}, and ${W} pulled away from there.`, cut.name);
		} else if (cutText) {
			say(`${cap(cutText)}, and ${L} got no closer.`, cut!.name);
		} else if (sealText) {
			say(`${cap(sealText)}.`, seal!.name);
		}
		return out;
	}

	// The winner led throughout the window: the story is how close the
	// losers came, and who closed the door.
	const closest = moments
		.filter((m) => m.role === "cut" && m.name)
		.sort((a, b) => a.margin - b.margin || a.e.clock - b.e.clock)[0];
	if (!closest || closest.margin > 3) {
		return out;
	}
	const [w, l] = shown(closest.e, wSide);
	const later = moments.filter((m) => m.e.clock < closest.e.clock);
	const seal = later
		.filter(
			(m) =>
				m.e.side === wSide &&
				m.name &&
				m.e.kind === "ft" &&
				mattered(m.e, wSide),
		)
		.at(-1);
	const cutText = pick(
		rng,
		[
			`${shotBy(closest.name!, closest.e, justNamed)} ${when(closest.e, rng)} got ${L} within ${closest.margin} at ${w}-${l}`,
			`${cap(L)} got as close as ${w}-${l} on ${shotBy(closest.name!, closest.e, justNamed)} ${when(closest.e, rng)}`,
			`${shotBy(closest.name!, closest.e, justNamed)} cut it to ${w}-${l} ${when(closest.e, rng)}`,
		],
		"finishClosest",
	);
	if (seal) {
		const fts = aShot(seal.e);
		say(
			`${cap(cutText)}, but ${pick(
				rng,
				[
					`${seal.name} answered with ${fts} ${when(seal.e, rng)}`,
					`${seal.name} made ${fts} ${when(seal.e, rng)} to put it away`,
					`${W} closed it out at the line, ${seal.name} making ${fts} ${when(seal.e, rng)}`,
				],
				"finishHoldSeal",
			)}.`,
			seal.name,
		);
	} else if (closest.e.clock < 0.5) {
		// The last shot of the game. There was no "from there" and no stop
		// left to get: "a three at the buzzer got them within 2, and the
		// Rockets held from there" held through zero seconds.
		say(
			`${cap(cutText)}, ${pick(
				rng,
				// Not "but time had run out" after "at the buzzer" or "with no
				// time left": the clock already said so.
				[`too late to matter`, `and that was the game`],
				"finishHoldBuzzer",
			)}.`,
			closest.name,
		);
	} else {
		// "got the Nets within 3, and the Nets never got the stop they
		// needed" named them twice in one breath - and so did "The Mavericks
		// got as close as 115-112..., and the Mavericks never got the stop",
		// which the lower-case test missed.
		const named = cutText.toLowerCase().includes(L.toLowerCase());
		const stop = `and ${named ? "they" : L} never got the stop they needed`;
		say(
			`${cap(cutText)}, ${
				// "The Nets got as close as 114-112..., but that was as close
				// as it got" said it twice.
				/got as close as/.test(cutText)
					? pick(rng, [stop, `and ${W} held from there`], "finishHoldTailClose")
					: pick(
							rng,
							[
								stop,
								`but that was as close as it got`,
								`and ${W} held from there`,
							],
							"finishHoldTail",
						)
			}.`,
			closest.name,
		);
	}
	return out;
};

// THE SCORE THAT PUT THE WINNERS AHEAD FOR GOOD. The flow's lastLead is the
// last time the lead CHANGED HANDS, and a tie does not change hands: up
// 112-109 on a three with 41.6 seconds left, tied at 119 with 5.9 left, ahead
// again on a layup with 2.5 left - and lastLead still pointed at the three, so
// the recap called it "the go-ahead three-pointer" and said they "went in
// front for good" 39 seconds before the game was tied. When the last tie came
// after it, the score that broke that tie is the one - when the log has it.
// The log keeps only the last minutes: a tie at 14 in the first quarter was
// broken by a basket it never saw, and its first basket was being called
// the go-ahead ("The Thunder took the lead for good on Grant Jackson's
// basket with 1:54 left" of a team up 60-41 at the half), in one recap in
// twenty. Then nobody is credited.
export const withSettledLead = (game: RecapGame): RecapGame => {
	const flow = game.flow;
	const lead = flow?.lastLead;
	const tie = flow?.lastTie;
	if (!flow || !lead || !tie) {
		return game;
	}
	const later = (
		a: { period: number; clock: number },
		b: { period: number; clock: number },
	) => a.period > b.period || (a.period === b.period && a.clock < b.clock);
	if (!later(tie, lead)) {
		return game;
	}
	const finish = flow.finish ?? [];
	const first = finish[0];
	// The log starts two minutes from the end. A tie before that was broken
	// in it only when it still stood at the two-minute mark.
	const two = flow.late?.find((m) => m.clock === 120);
	const stoodAtTwo =
		two !== undefined && two.pts[0] === two.pts[1] && two.pts[0] === tie.pts;
	const next =
		first && (!later(first, tie) || stoodAtTwo)
			? finish.find(
					(e) => e.pts > 0 && later(e, tie) && e.score[0] !== e.score[1],
				)
			: undefined;
	if (!next) {
		return { ...game, flow: { ...flow, lastLead: undefined } };
	}
	return {
		...game,
		flow: {
			...flow,
			lastLead: {
				side: next.side,
				pid: next.pid,
				period: next.period,
				clock: next.clock,
				pts: [next.score[0], next.score[1]],
				by: next.pts,
			},
		},
	};
};

// What the winning shot was, for the headline and the lede, when the sim's
// own note only says "basket": the finish log knows whether it was a
// layup, a jumper or a hook.
export const winningShotKind = (
	game: RecapGame,
	winnerTid: number,
	clock: number,
): string | undefined => {
	const finish = game.flow?.finish;
	if (!finish) {
		return undefined;
	}
	const wSide: GameFlowSide = game.teams[0].tid === winnerTid ? 0 : 1;
	const e = [...finish]
		.reverse()
		.find(
			(x) =>
				x.side === wSide && x.kind !== "ft" && Math.abs(x.clock - clock) < 0.15,
		);
	return e ? shotName(e) : undefined;
};

// For the accuracy reader: every score the closing stretch passed through,
// as "a-b" both ways round, plus each tie as its bare number.
export const finishScores = (
	game: RecapGame,
): { pairs: Set<string>; ties: Set<number> } => {
	const pairs = new Set<string>();
	const ties = new Set<number>();
	for (const e of game.flow?.finish ?? []) {
		pairs.add(`${e.score[0]}-${e.score[1]}`);
		pairs.add(`${e.score[1]}-${e.score[0]}`);
		if (e.score[0] === e.score[1]) {
			ties.add(e.score[0]);
		}
	}
	for (const m of game.flow?.late ?? []) {
		pairs.add(`${m.pts[0]}-${m.pts[1]}`);
		pairs.add(`${m.pts[1]}-${m.pts[0]}`);
		if (m.pts[0] === m.pts[1]) {
			ties.add(m.pts[0]);
		}
	}
	if (game.flow?.lastTie) {
		ties.add(game.flow.lastTie.pts);
	}
	return { pairs, ties };
};
