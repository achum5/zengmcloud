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
import { cap, pick, poss, stripHtml, theNick } from "./recapText.ts";

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
};

// "layup", "jumper", "three-pointer"; an and-one is the play it made.
export const shotName = (e: Pick<FinishEvent, "kind" | "andOne" | "pts">) => {
	if (e.kind === "ft") {
		return e.pts === 1
			? "free throw"
			: e.pts === 2
				? "two free throws"
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

// The game-tying shot that forced overtime, from the sim's own note of it:
// "X made a three-pointer with 4.2 seconds remaining to force overtime".
const regulationTie = (
	game: RecapGame,
):
	| { name: string; shot: string; clock?: string; buzzer: boolean }
	| undefined => {
	for (const raw of game.clutchPlays) {
		const text = stripHtml(raw);
		const m =
			/^(.+?) made (a [ a-z-]+?|two free throws|three free throws)(?: with ([\d.:]+) seconds remaining| at the buzzer| with no time on the clock)? to force/.exec(
				text,
			);
		if (m) {
			const shot = m[2]!.replace(/^a /, "");
			const secs = m[3] ? Number.parseFloat(m[3].replace(":", ".")) : undefined;
			return {
				name: m[1]!.trim(),
				shot: shot === "basket" ? "basket" : shot,
				clock:
					secs !== undefined && Number.isFinite(secs)
						? clockLeft(secs)
						: undefined,
				buzzer: /at the buzzer|no time on the clock/.test(text),
			};
		}
	}
	return undefined;
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
	const say = (text: string, name?: string) => {
		out.push(text);
		justNamed = name;
	};

	if (ot) {
		const tie = regulationTie(game);
		if (tie) {
			const clock = tie.buzzer
				? tie.shot.includes("free throw")
					? " with no time left"
					: " at the buzzer"
				: tie.clock
					? ` with ${tie.clock} left`
					: "";
			const shot = tie.shot;
			const aShot = /free throws$/.test(shot) ? shot : `a ${shot}`;
			say(
				pick(
					rng,
					[
						`${cap(poss(tie.name))} ${shot}${clock} sent it to overtime.`,
						`It took ${poss(tie.name)} ${shot}${clock} to force the extra period.`,
						`${tie.name} forced overtime on ${aShot}${clock}.`,
					],
					"finishForcedOt",
				),
				tie.name,
			);
		}
	}

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

	// The final overtime period only, when there were several: a double-OT
	// game's first extra period is the fight, and it is enough to say the
	// game needed two of them.
	const goAhead = goAheadIdx >= 0 ? moments[goAheadIdx]! : undefined;

	if (goAhead) {
		// What the go-ahead shot answered: the most recent tie or lead the
		// losers held before it.
		const before = moments
			.slice(0, goAheadIdx)
			.reverse()
			.find((m) => m.role === "tie" || m.role === "loserLead");
		if (before?.name) {
			const [w, l] = scoreOf(before.e, wSide);
			const shot = shotBy(before.name, before.e, justNamed);
			// Past perfect when the lede has already jumped ahead to the
			// winning shot: the tie is being told out of order.
			const had = shotTold ? "had " : "";
			if (before.role === "tie") {
				say(
					pick(
						rng,
						[
							`${cap(shot)} ${had}tied it at ${w} ${when(before.e, rng)}.`,
							`${cap(shot)} ${when(before.e, rng)} ${had}made it ${w}-${w}.`,
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
		} else if (!ot) {
			// Nothing in the window before the go-ahead score: the losers were
			// in front or level at the two-minute mark and did not score
			// again before it.
			const two = game.flow?.late?.find((m) => m.clock === 120);
			if (two) {
				const l = two.pts[1 - wSide]!;
				const w = two.pts[wSide]!;
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
					say(`It was ${w}-${w} with two minutes to play.`);
				}
			}
		}

		if (goAhead.name && !shotTold) {
			const [w, l] = scoreOf(goAhead.e, wSide);
			const shot = shotBy(goAhead.name, goAhead.e, justNamed);
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
									`${goAhead.name} answered with ${aShot(goAhead.e)} ${when(goAhead.e, rng)} that put ${W} up ${w}-${l}.`,
								]
							: followed
								? [
										`${cap(shot)} ${when(goAhead.e, rng)} then made it ${w}-${l}.`,
										`${cap(W)} took the lead on ${shot} ${when(goAhead.e, rng)}, ${w}-${l}.`,
									]
								: [
										`${cap(shot)} ${when(goAhead.e, rng)} made it ${w}-${l}, and ${W} never trailed again.`,
									]),
					],
					"finishGoAhead",
				),
				goAhead.name,
			);
		}

		// What followed: the losers getting close, and the winner closing it.
		const after = moments.slice(goAheadIdx + 1);
		const cut = after
			.filter((m) => m.role === "cut" && m.margin <= 3 && m.name)
			.at(-1);
		const seal = after
			.filter((m) => m.e.side === wSide && m.name && m.e.kind === "ft")
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
		} else if (cut && cut.e.clock < 0.5) {
			// A basket at the horn changes the final and nothing else.
			const [w, l] = shown(cut.e, wSide);
			say(
				`${cap(shotBy(cut.name!, cut.e, justNamed))} at the buzzer made the final ${w}-${l}.`,
				cut.name,
			);
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
		.filter((m) => m.e.side === wSide && m.name && m.e.kind === "ft")
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
	} else {
		say(
			`${cap(cutText)}, ${pick(
				rng,
				[
					`and ${L} never got the stop they needed`,
					`but that was as close as it got`,
					`and ${W} held from there`,
				],
				"finishHoldTail",
			)}.`,
			closest.name,
		);
	}
	return out;
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
