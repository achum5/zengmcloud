// SAYING IT.
//
// The last stage: a cast slot (this account, this event) becomes a line of
// text. Everything that decides WHO speaks and WHAT ABOUT already happened, so
// this file only has to answer "how would this particular account put it".
//
// Three rules hold the quality line.
//
// NUMBERS COME FROM FACTS, FULL STOP. A template can only interpolate what the
// event handed it, and verifyPostNumbers reads the finished string back and
// checks every numeral in it against those facts. An account with a low
// accuracy dial gets to be wrong about what a result MEANS - it can call a
// two-point win a statement, or a career night a fluke - and never about what
// the result WAS. A feed that misreports a score is broken, not characterful.
//
// TEMPLATES ARE TAGGED, NOT DUPLICATED. Writing eight tone variants of twelve
// event types would be ninety-six banks nobody could keep consistent, and each
// would be thin. Instead every line declares which tones it suits and what has
// to be true for it to apply, and the pool for a post is whatever survives that
// filter. A tone with wide taste draws from a large bank; a narrow one draws
// from a small, well-chosen one.
//
// VOICE IS A SEPARATE PASS. Casing, emoji, shouting, swearing and catchphrases
// are applied AFTER a line is chosen, so one template serves five hundred
// accounts and reads differently in each. This is the only reason a league of
// this size is writable at all.

import type { PhrasePool } from "./phrasePool.ts";
import type { ResolvedSocialAccount } from "./socialAccounts.ts";
import type { SocialEvent } from "./socialEvents.ts";
import type { SocialPersonality, SocialTone } from "./socialPersonality.ts";

// Where an account sits relative to what happened. Computed once so templates
// can be written from a point of view rather than re-deriving it - and so a
// homer for the losing team can never draw a celebration line.
export type Stance = "for" | "against" | "neutral";

export const stanceOf = (
	account: ResolvedSocialAccount,
	event: SocialEvent,
): Stance => {
	const side = account.personality.loyaltyTid ?? account.tid;
	if (side === undefined) {
		return "neutral";
	}
	const winner = event.facts.winnerTid;
	const loser = event.facts.loserTid;
	if (typeof winner === "number" && side === winner) {
		return "for";
	}
	if (typeof loser === "number" && side === loser) {
		return "against";
	}
	// A performance event names one team; being on it is being for it.
	if (typeof event.facts.tid === "number" && side === event.facts.tid) {
		return event.facts.won === false ? "against" : "for";
	}
	return "neutral";
};

// ---------------------------------------------------------------- FRAMES
//
// The shape of what happened, computed from facts once. Templates read these
// rather than doing arithmetic, so a line can be written about "a blowout"
// without every author re-deciding what counts as one.

export type GameFrame = {
	kind: "game";
	// The franchise itself, which changes what it will say about a loss.
	corporate: boolean;
	// A player on one of these teams speaks in the first person plural. The
	// same result said by a beat writer and by the guy who played in it are
	// different sentences, and nothing else in the pipeline can tell them
	// apart once the line is chosen.
	insider: boolean;
	winner: string;
	loser: string;
	winnerAbbrev: string;
	loserAbbrev: string;
	winnerPts: number;
	loserPts: number;
	margin: number;
	ot: number;
	playoffs: boolean;
	upset: boolean;
	blowout: boolean;
	nailbiter: boolean;
	streak?: number;
	skid?: number;
	stance: Stance;
};

export type PerformanceFrame = {
	kind: "performance";
	// Whose line this is, from the speaker's seat. The first sample day had
	// Chris Bosh's own account posting "chris bosh finished with 36", which is
	// the single most obviously generated thing a feed like this can do.
	viewer: "self" | "teammate" | "other";
	name: string;
	pts: number;
	reb: number;
	ast: number;
	stl: number;
	blk: number;
	tov: number;
	tsp?: number;
	tripleDouble: boolean;
	doubles: number;
	won: boolean;
	opponent: string;
	huge: boolean;
	stance: Stance;
};

export type SummaryFrame = {
	kind: "summary";
	summary: string;
	stance: Stance;
};

export type Frame = GameFrame | PerformanceFrame | SummaryFrame;

const num = (facts: SocialEvent["facts"], key: string): number => {
	const value = facts[key];
	return typeof value === "number" ? value : 0;
};

const str = (facts: SocialEvent["facts"], key: string): string => {
	const value = facts[key];
	return typeof value === "string" ? value : "";
};

export const frameFor = (
	account: ResolvedSocialAccount,
	event: SocialEvent,
): Frame | undefined => {
	const stance = stanceOf(account, event);
	const f = event.facts;

	if (event.type === "gameResult") {
		const margin = num(f, "margin");
		const side = account.personality.loyaltyTid ?? account.tid;
		return {
			kind: "game",
			corporate: account.personality.tone === "corporate",
			insider:
				account.kind === "player" &&
				side !== undefined &&
				event.tids.includes(side),
			winner: str(f, "winnerName"),
			loser: str(f, "loserName"),
			winnerAbbrev: str(f, "winnerAbbrev"),
			loserAbbrev: str(f, "loserAbbrev"),
			winnerPts: num(f, "winnerPts"),
			loserPts: num(f, "loserPts"),
			margin,
			ot: num(f, "overtimes"),
			playoffs: f.playoffs === true,
			upset: f.upset === true,
			blowout: margin >= 20,
			nailbiter: margin <= 3 || num(f, "overtimes") > 0,
			streak: typeof f.winnerStreak === "number" ? f.winnerStreak : undefined,
			skid: typeof f.loserSkid === "number" ? f.loserSkid : undefined,
			stance,
		};
	}

	if (event.type === "performance") {
		const pts = num(f, "pts");
		const side = account.personality.loyaltyTid ?? account.tid;
		const viewer =
			account.pid !== undefined && event.pids.includes(account.pid)
				? "self"
				: account.kind === "player" &&
					  side !== undefined &&
					  event.tids.includes(side)
					? "teammate"
					: "other";
		return {
			kind: "performance",
			viewer,
			name: str(f, "name"),
			pts,
			reb: num(f, "reb"),
			ast: num(f, "ast"),
			stl: num(f, "stl"),
			blk: num(f, "blk"),
			tov: num(f, "tov"),
			tsp: typeof f.tsp === "number" ? f.tsp : undefined,
			tripleDouble: f.tripleDouble === true,
			doubles: num(f, "doubles"),
			won: f.won === true,
			opponent: str(f, "opponentAbbrev"),
			huge: pts >= 40 || f.tripleDouble === true,
			stance,
		};
	}

	const summary = str(f, "summary");
	if (summary === "") {
		// Nothing to say and no numbers to say it with. Better to post nothing
		// than to post a shape with a hole in it.
		return undefined;
	}
	return { kind: "summary", summary, stance };
};

// ---------------------------------------------------------------- TEMPLATES

type Template<F extends Frame> = {
	// Stable across builds: the phrase ledger claims these so one line cannot
	// be used twice in a night, and an index would shift as banks grow.
	id: string;
	// Which tones this line suits. Omitted means any.
	tones?: SocialTone[];
	// What has to be true. Omitted means always.
	when?: (frame: F) => boolean;
	text: (frame: F) => string;
};

const ALL_TONES: SocialTone[] = [
	"wire",
	"beat",
	"hype",
	"snark",
	"doom",
	"wonk",
	"corporate",
	"unhinged",
];

const GAME_TEMPLATES: Template<GameFrame>[] = [
	// Wire and beat: what happened, no adjectives.
	{
		id: "game.final",
		tones: ["wire", "beat", "wonk"],
		text: (f) =>
			`FINAL: ${f.winnerAbbrev} ${f.winnerPts}, ${f.loserAbbrev} ${f.loserPts}.`,
	},
	{
		id: "game.beat.margin",
		tones: ["wire", "beat"],
		when: (f) => !f.nailbiter,
		text: (f) => `${f.winner} beat the ${f.loser} by ${f.margin}.`,
	},
	{
		id: "game.beat.close",
		tones: ["wire", "beat"],
		when: (f) => f.nailbiter && f.ot === 0,
		text: (f) =>
			`${f.winner} held on ${f.winnerPts}-${f.loserPts} over the ${f.loser}.`,
	},
	{
		id: "game.beat.ot",
		tones: ["wire", "beat", "hype"],
		when: (f) => f.ot > 0,
		text: (f) =>
			f.ot === 1
				? `${f.winner} needed overtime to get past the ${f.loser}, ${f.winnerPts}-${f.loserPts}.`
				: `${f.ot} overtimes. ${f.winner} ${f.winnerPts}, ${f.loser} ${f.loserPts}.`,
	},
	{
		id: "game.beat.blowout",
		tones: ["beat", "wonk", "snark"],
		when: (f) => f.blowout,
		text: (f) =>
			`Not close: ${f.winner} ${f.winnerPts}, ${f.loser} ${f.loserPts}.`,
	},
	// Stakes.
	{
		id: "game.playoffs",
		tones: ["wire", "beat", "hype"],
		when: (f) => f.playoffs,
		text: (f) => `Playoffs: ${f.winner} take it ${f.winnerPts}-${f.loserPts}.`,
	},
	{
		id: "game.upset",
		tones: ["beat", "snark", "hype"],
		when: (f) => f.upset,
		text: (f) => `So much for the line. ${f.winner} win by ${f.margin}.`,
	},
	{
		id: "game.streak",
		tones: ["beat", "hype", "wonk"],
		when: (f) => f.streak !== undefined && f.streak >= 4,
		text: (f) => `${f.streak} straight for the ${f.winner}.`,
	},
	{
		id: "game.skid",
		tones: ["beat", "snark", "doom"],
		when: (f) => f.skid !== undefined && f.skid >= 4,
		text: (f) => `${f.loser} have now lost ${f.skid} in a row.`,
	},
	// Supporting a winner.
	{
		id: "game.hype.win",
		tones: ["hype", "corporate", "unhinged"],
		when: (f) => f.stance === "for",
		text: (f) => `${f.winnerPts}-${f.loserPts}. That is how you do it.`,
	},
	{
		id: "game.hype.win.close",
		tones: ["hype", "unhinged"],
		when: (f) => f.stance === "for" && f.nailbiter,
		text: () => `I have aged ten years and I would watch it again right now`,
	},
	{
		id: "game.hype.win.blowout",
		tones: ["hype", "corporate", "unhinged"],
		when: (f) => f.stance === "for" && f.blowout,
		text: (f) => `By ${f.margin}. Not a typo.`,
	},
	{
		id: "game.corp.win",
		tones: ["corporate"],
		when: (f) => f.stance === "for",
		text: (f) => `Your ${f.winner} take it, ${f.winnerPts}-${f.loserPts}.`,
	},
	// Supporting a loser.
	{
		id: "game.doom.loss",
		tones: ["doom", "snark", "unhinged"],
		when: (f) => f.stance === "against",
		text: (f) => `${f.winnerPts}-${f.loserPts}. Same thing every night.`,
	},
	{
		id: "game.doom.loss.close",
		tones: ["doom", "unhinged"],
		when: (f) => f.stance === "against" && f.nailbiter,
		text: (f) =>
			`Lose by ${f.margin}. Somehow worse than getting run off the floor.`,
	},
	{
		id: "game.doom.loss.blowout",
		tones: ["doom", "snark", "unhinged"],
		when: (f) => f.stance === "against" && f.blowout,
		text: (f) => `Down ${f.margin}. Sell the team.`,
	},
	{
		id: "game.doom.streakless",
		tones: ["doom"],
		when: (f) => f.stance === "against" && f.skid !== undefined && f.skid >= 3,
		text: (f) => `${f.skid} in a row now. Nobody is coming to fix this.`,
	},
	// Neutral snark.
	{
		id: "game.snark.neutral",
		tones: ["snark", "unhinged"],
		when: (f) => f.stance === "neutral" && f.blowout,
		text: (f) => `The ${f.loser} were allowed to leave after that one.`,
	},
	{
		id: "game.wonk.margin",
		tones: ["wonk"],
		text: (f) =>
			`${f.winnerAbbrev} ${f.winnerPts}, ${f.loserAbbrev} ${f.loserPts}. Margin ${f.margin}.`,
	},
	{
		id: "game.take",
		tones: ["wire", "beat", "corporate"],
		text: (f) =>
			`${f.winner} take it over the ${f.loser}, ${f.winnerPts}-${f.loserPts}.`,
	},
	{
		id: "game.hold",
		tones: ["beat", "snark"],
		when: (f) => !f.blowout && !f.nailbiter,
		text: (f) => `Never really in doubt: ${f.winner} by ${f.margin}.`,
	},
	{
		id: "game.beat.score",
		tones: ["wire", "beat", "wonk", "corporate"],
		text: (f) =>
			`${f.winnerAbbrev} ${f.winnerPts} — ${f.loserAbbrev} ${f.loserPts}.`,
	},
	{
		id: "game.snark.loser",
		tones: ["snark", "unhinged"],
		when: (f) => f.stance !== "against" && f.blowout,
		text: (f) => `Somebody check on the ${f.loser}.`,
	},
	{
		id: "game.hype.close",
		tones: ["hype", "unhinged"],
		when: (f) => f.stance === "for" && !f.nailbiter,
		text: (f) => `Win is a win. ${f.winnerPts}-${f.loserPts}.`,
	},
	{
		id: "game.doom.again",
		tones: ["doom", "snark"],
		when: (f) => f.stance === "against" && !f.nailbiter,
		text: (f) => `Another one. ${f.winnerPts}-${f.loserPts}.`,
	},
	{
		id: "game.wonk.pace",
		tones: ["wonk"],
		when: (f) => f.winnerPts + f.loserPts >= 220,
		text: (f) =>
			`${f.winnerPts + f.loserPts} combined. Nobody guarded anybody.`,
	},
];

const PERFORMANCE_TEMPLATES: Template<PerformanceFrame>[] = [
	{
		id: "perf.line",
		tones: ["wire", "beat", "wonk"],
		text: (f) =>
			`${f.name}: ${f.pts} points, ${f.reb} rebounds, ${f.ast} assists vs ${f.opponent}.`,
	},
	{
		id: "perf.scoring",
		tones: ["wire", "beat", "hype"],
		when: (f) => f.pts >= 30 && !f.tripleDouble,
		text: (f) => `${f.name} finished with ${f.pts}.`,
	},
	{
		id: "perf.td",
		tones: ALL_TONES,
		when: (f) => f.tripleDouble,
		text: (f) => `Triple-double for ${f.name}: ${f.pts}, ${f.reb}, ${f.ast}.`,
	},
	{
		id: "perf.huge",
		tones: ["hype", "unhinged", "corporate"],
		when: (f) => f.pts >= 40,
		text: (f) => `${f.pts} POINTS. ${f.name}.`,
	},
	{
		id: "perf.efficient",
		tones: ["wonk", "beat"],
		// Upper bound as well as lower: true shooting above 100 is arithmetically
		// impossible, and a generator that prints it once has destroyed its own
		// credibility for the rest of the league.
		when: (f) =>
			f.tsp !== undefined && f.tsp >= 65 && f.tsp <= 100 && f.pts >= 25,
		text: (f) => `${f.name} put up ${f.pts} on ${f.tsp}% true shooting.`,
	},
	{
		id: "perf.boards",
		tones: ["wire", "beat", "wonk", "hype"],
		when: (f) => f.reb >= 15,
		text: (f) => `${f.name} pulled down ${f.reb} boards.`,
	},
	{
		id: "perf.dimes",
		tones: ["wire", "beat", "wonk", "hype"],
		when: (f) => f.ast >= 12,
		text: (f) => `${f.ast} assists for ${f.name}.`,
	},
	{
		id: "perf.defense",
		tones: ["beat", "wonk", "hype"],
		when: (f) => f.stl >= 5 || f.blk >= 5,
		text: (f) => `${f.name}: ${f.stl} steals, ${f.blk} blocks.`,
	},
	{
		id: "perf.wasted",
		tones: ["doom", "snark"],
		when: (f) => !f.won && f.pts >= 30,
		text: (f) => `${f.pts} from ${f.name} in a loss. Wasted.`,
	},
	{
		id: "perf.hype.win",
		tones: ["hype", "unhinged"],
		when: (f) => f.won && f.huge,
		text: (f) => `${f.name} is not human`,
	},
	{
		id: "perf.corp",
		tones: ["corporate"],
		text: (f) => `${f.pts} PTS · ${f.reb} REB · ${f.ast} AST for ${f.name}.`,
	},
	{
		id: "perf.snark.tov",
		tones: ["snark", "wonk"],
		when: (f) => f.tov >= 6,
		text: (f) => `${f.name} also had ${f.tov} turnovers, but sure.`,
	},
	// Everything below exists because the first sample night used "finished
	// with" five times: the eligible set for a common tone on an ordinary
	// 30-point game was one line long, so the ledger had nothing to rotate to.
	{
		id: "perf.led",
		tones: ["wire", "beat", "corporate"],
		when: (f) => f.pts >= 20,
		text: (f) => `${f.name} led the way with ${f.pts}.`,
	},
	{
		id: "perf.night",
		tones: ["beat", "hype", "corporate"],
		when: (f) => f.pts >= 25,
		text: (f) => `${f.pts}-point night for ${f.name}.`,
	},
	{
		id: "perf.allaround",
		tones: ["beat", "wonk", "wire"],
		when: (f) => f.doubles >= 2,
		text: (f) =>
			`${f.name} did a bit of everything: ${f.pts}/${f.reb}/${f.ast}.`,
	},
	{
		id: "perf.hype.cook",
		tones: ["hype", "unhinged"],
		when: (f) => f.pts >= 28,
		text: (f) => `${f.name} cooked. ${f.pts} of them.`,
	},
	{
		id: "perf.hype.short",
		tones: ["hype", "unhinged", "corporate"],
		when: (f) => f.pts >= 25,
		text: (f) => `${f.name}. That is the post.`,
	},
	{
		id: "perf.wonk.usage",
		tones: ["wonk"],
		when: (f) => f.pts >= 20,
		text: (f) => `${f.pts} points against ${f.opponent}. Efficient enough.`,
	},
	{
		id: "perf.snark.empty",
		tones: ["snark", "doom"],
		when: (f) => !f.won && f.pts >= 20,
		text: (f) => `${f.pts} points and a loss. Empty calories.`,
	},
	{
		id: "perf.beat.vs",
		tones: ["wire", "beat"],
		text: (f) => `${f.name} vs ${f.opponent}: ${f.pts} pts, ${f.reb} reb.`,
	},
	{
		id: "perf.doom.only",
		tones: ["doom"],
		when: (f) => f.won && f.pts >= 25,
		text: (f) => `${f.name} was the only one who showed up. Again.`,
	},
	{
		id: "perf.corp.congrats",
		tones: ["corporate"],
		when: (f) => f.pts >= 30 || f.tripleDouble,
		text: (f) => `A career kind of night from ${f.name}. What a performance.`,
	},
	{
		id: "perf.unhinged.caps",
		tones: ["unhinged"],
		when: (f) => f.pts >= 25,
		text: (f) => `${f.name} DID THAT`,
	},
];

// A PLAYER TALKING ABOUT HIS OWN NIGHT.
//
// Separate bank rather than a tone variant, because the grammar changes: this
// is the only speaker in the league who says "I". The first sample day without
// it produced "@ChrisBosh: chris bosh finished with 36", which reads as
// generated the instant anyone sees it.
//
// Deliberately light on numbers. Players do not recite their own box score,
// and the ones who come closest do it obliquely, so most of these lines quote
// nothing and the few that do are the ones a person would actually mention.
const SELF_TEMPLATES: Template<PerformanceFrame>[] = [
	{
		id: "self.win.plain",
		when: (f) => f.won,
		text: () => `Good win. On to the next one.`,
	},
	{
		id: "self.win.team",
		when: (f) => f.won,
		text: () => `Team win. That is all that matters.`,
	},
	{
		id: "self.win.thanks",
		tones: ["hype", "corporate", "beat"],
		when: (f) => f.won,
		text: () => `Appreciate the support tonight. We felt it.`,
	},
	{
		id: "self.win.big",
		tones: ["hype", "unhinged"],
		when: (f) => f.won && f.huge,
		text: () => `They cannot guard me. Say it back.`,
	},
	{
		id: "self.win.humble",
		tones: ["beat", "wire", "wonk"],
		when: (f) => f.won && f.huge,
		text: () => `Shots fell. My teammates found me all night.`,
	},
	{
		id: "self.td",
		when: (f) => f.tripleDouble,
		text: () => `Just trying to fill in wherever they need me.`,
	},
	{
		id: "self.boards",
		when: (f) => f.reb >= 15,
		text: () => `Rebounding is effort. Nothing else to it.`,
	},
	{
		id: "self.dimes",
		when: (f) => f.ast >= 12,
		text: () => `Easier to pass to guys who make shots.`,
	},
	{
		id: "self.defense",
		when: (f) => f.stl >= 4 || f.blk >= 4,
		text: () => `Defense travels. Been saying it.`,
	},
	{
		id: "self.loss.short",
		when: (f) => !f.won,
		text: () => `Not good enough. That is on us.`,
	},
	{
		id: "self.loss.back",
		when: (f) => !f.won,
		text: () => `We will be back at it tomorrow. Nobody is panicking.`,
	},
	{
		id: "self.loss.big",
		tones: ["doom", "snark", "unhinged"],
		when: (f) => !f.won && f.pts >= 30,
		text: () => `Scoring is not the problem.`,
	},
	{
		id: "self.loss.mine",
		tones: ["beat", "wire", "wonk", "doom"],
		when: (f) => !f.won,
		text: () => `I have to be better. Simple as that.`,
	},
	{
		id: "self.tov",
		when: (f) => f.tov >= 6,
		text: () => `Have to take care of the ball. That is on me.`,
	},
	{
		id: "self.quiet",
		text: () => `Long season. Back to work.`,
	},
	{
		id: "self.pts",
		tones: ["hype", "unhinged", "corporate"],
		when: (f) => f.pts >= 30,
		text: (f) => `${f.pts} and we still had more in the tank.`,
	},
];

// A TEAMMATE'S NIGHT. The other reason player accounts are worth having: a
// locker room that reacts to itself.
const TEAMMATE_TEMPLATES: Template<PerformanceFrame>[] = [
	{
		id: "mate.hype",
		when: (f) => f.huge,
		text: (f) => `${f.name} is different. Enjoy watching that man work.`,
	},
	{
		id: "mate.short",
		text: (f) => `My guy ${f.name}.`,
	},
	{
		id: "mate.credit",
		when: (f) => f.won,
		text: (f) => `${f.name} carried us tonight. Give him his flowers.`,
	},
	{
		id: "mate.defense",
		when: (f) => f.stl >= 4 || f.blk >= 4,
		text: (f) => `Nobody talks about what ${f.name} does on defense.`,
	},
	{
		id: "mate.loss",
		when: (f) => !f.won && f.pts >= 25,
		text: (f) => `${f.name} did his part. The rest of us have to help him.`,
	},
	{
		id: "mate.td",
		when: (f) => f.tripleDouble,
		text: (f) => `Triple-double and he is still mad at himself. ${f.name}.`,
	},
];

// A PLAYER ON A RESULT, as opposed to a reporter on a result. First person
// plural, and no box score - the guy who was there does not recite the score.
const INSIDER_GAME_TEMPLATES: Template<GameFrame>[] = [
	{
		id: "ins.win",
		when: (f) => f.stance === "for",
		text: () => `That is a good one. On to the next.`,
	},
	{
		id: "ins.win.hard",
		when: (f) => f.stance === "for" && f.nailbiter,
		text: () => `We found a way. That is what good teams do.`,
	},
	{
		id: "ins.win.big",
		when: (f) => f.stance === "for" && f.blowout,
		text: () => `Everybody ate tonight.`,
	},
	{
		id: "ins.win.road",
		tones: ["hype", "unhinged", "corporate"],
		when: (f) => f.stance === "for",
		text: () => `Business handled.`,
	},
	{
		id: "ins.loss",
		when: (f) => f.stance === "against",
		text: () => `We were not good enough. Back to work.`,
	},
	{
		id: "ins.loss.close",
		when: (f) => f.stance === "against" && f.nailbiter,
		text: () => `Comes down to a couple of possessions. We know it.`,
	},
	{
		id: "ins.loss.big",
		when: (f) => f.stance === "against" && f.blowout,
		text: () => `That one is on all of us. It will not happen again.`,
	},
	{
		id: "ins.streak",
		when: (f) => f.stance === "for" && f.streak !== undefined && f.streak >= 4,
		text: () => `Stacking days. Nothing is finished.`,
	},
];

// League-log events arrive as the game's own prose. An account either passes
// it along or reacts to it; nobody re-derives the facts, because the log line
// already is the fact.
const SUMMARY_TEMPLATES: Template<SummaryFrame>[] = [
	{
		id: "sum.plain",
		tones: ["wire", "beat", "corporate"],
		text: (f) => f.summary,
	},
	{
		id: "sum.breaking",
		tones: ["wire", "beat"],
		text: (f) => `Breaking: ${f.summary}`,
	},
	{
		id: "sum.wonk",
		tones: ["wonk"],
		text: (f) => `${f.summary} Worth watching what that does to the rotation.`,
	},
	{
		id: "sum.hype",
		tones: ["hype", "corporate"],
		text: (f) => `${f.summary} Let's go.`,
	},
	{
		id: "sum.doom",
		tones: ["doom"],
		text: (f) => `${f.summary} This will go badly.`,
	},
	{
		id: "sum.snark",
		tones: ["snark", "unhinged"],
		text: (f) => `${f.summary} Sure. Great. Love it here.`,
	},
	{
		id: "sum.question",
		tones: ["snark", "doom", "unhinged"],
		text: (f) => `${f.summary} Who signed off on this?`,
	},
];

// ---------------------------------------------------------------- VOICE

const POSITIVE_EMOJI = ["🔥", "😤", "🙌", "💪", "👏"];
const NEGATIVE_EMOJI = ["💀", "😭", "🤡", "🥴", "😐"];
const NEUTRAL_EMOJI = ["👀", "📈", "🧠", "📝"];

// Censored on purpose. The dial exists so an angry account reads angry, not so
// the app ships slurs into a screenshot.
const EXPLETIVES = ["damn", "hell", "garbage", "brutal"];

const stripTerminal = (text: string) => text.replace(/[!.]+$/, "");

// Shout one phrase rather than the whole line: an entire post in caps is
// unreadable, and the real thing people do is emphasize a few words.
//
// Consecutive capitalised words are treated as ONE token, so a name is shouted
// whole or not at all. Splitting them produced "Marcin GORTAT led the way" and
// "DERRICK Rose led the way", which read as a bug rather than as emphasis.
const shoutSomething = (rng: () => number, text: string): string => {
	const raw = text.split(" ");
	const words: string[] = [];
	for (const word of raw) {
		const previous = words.at(-1);
		if (
			previous !== undefined &&
			/^[A-Z]/.test(word) &&
			/^[A-Z]/.test(previous)
		) {
			words[words.length - 1] = `${previous} ${word}`;
		} else {
			words.push(word);
		}
	}
	if (words.length < 3) {
		return text.toUpperCase();
	}
	const start = Math.floor(rng() * Math.max(1, words.length - 2));
	// At most half the line. Capping the RUN rather than the chance is what
	// keeps this from occasionally shouting an entire post, which is both
	// unreadable and not what anybody actually does.
	const len = Math.min(
		words.length - start,
		1 + Math.floor(rng() * 3),
		Math.max(1, Math.floor(words.length / 2)),
	);
	for (let i = start; i < start + len; i++) {
		words[i] = words[i]!.toUpperCase();
	}
	return words.join(" ");
};

export const applyVoice = ({
	text,
	personality,
	pool,
	rng,
	positive,
}: {
	text: string;
	personality: SocialPersonality;
	pool: PhrasePool;
	rng: () => number;
	// Whether the news is good from this account's point of view, which is what
	// decides between a fire emoji and a skull.
	positive: boolean | undefined;
}): string => {
	let out = text;

	if (rng() < personality.profanity) {
		const word = pool.pick(rng, EXPLETIVES, "voice:expletive");
		out = `${word}. ${out}`;
	}

	// Casing BEFORE shouting. The other order lowercased the shout away, so an
	// account that types in lowercase - which is most of the fan accounts -
	// could never emphasize anything, however high its dial was set.
	if (personality.formality < 0.3) {
		out = out.toLowerCase();
		out = stripTerminal(out);
	} else if (personality.formality < 0.6 && rng() < 0.5) {
		out = stripTerminal(out);
	}

	if (rng() < personality.caps) {
		out = shoutSomething(rng, out);
	}

	if (personality.catchphrases.length > 0 && rng() < 0.25) {
		const phrase = pool.pick(
			rng,
			personality.catchphrases,
			"voice:catchphrase",
		);
		out = `${phrase} ${out}`;
	}

	if (rng() < personality.emoji) {
		const bank =
			positive === undefined
				? NEUTRAL_EMOJI
				: positive
					? POSITIVE_EMOJI
					: NEGATIVE_EMOJI;
		out = `${out} ${pool.pick(rng, bank, "voice:emoji")}`;
	}

	return out;
};

// ---------------------------------------------------------------- WRITING

// Which bank a speaker draws from. Routing on WHO IS TALKING rather than only
// on what happened is what stops a player narrating his own box score in the
// third person, and it is why the player banks are separate files' worth of
// lines rather than extra tones on the reporter banks.
const bankFor = (frame: Frame): Template<any>[] => {
	if (frame.kind === "game") {
		// A franchise's own account goes quiet after a loss rather than
		// announcing it. The first sample day had the Heat posting their own
		// 130-99 defeat with a crying emoji, which no team account has ever
		// done.
		if (frame.stance === "against" && frame.corporate) {
			return [];
		}
		// REPLACES rather than extends. Concatenating let a player draw a beat
		// writer's line about a game he had just played in, which reads as
		// somebody narrating their own night from the press box.
		return frame.insider ? INSIDER_GAME_TEMPLATES : GAME_TEMPLATES;
	}
	if (frame.kind === "performance") {
		if (frame.viewer === "self") {
			return SELF_TEMPLATES;
		}
		if (frame.viewer === "teammate") {
			return TEAMMATE_TEMPLATES;
		}
		return PERFORMANCE_TEMPLATES;
	}
	return SUMMARY_TEMPLATES;
};

// Whether the event reads as good news from this account's seat, for emoji
// choice. Undefined for a neutral observer, who gets neither.
const positivity = (frame: Frame): boolean | undefined => {
	if (frame.stance === "neutral") {
		return undefined;
	}
	return frame.stance === "for";
};

export const writePost = ({
	account,
	event,
	pool,
	rng,
}: {
	account: ResolvedSocialAccount;
	event: SocialEvent;
	pool: PhrasePool;
	rng: () => number;
}): string | undefined => {
	const frame = frameFor(account, event);
	if (!frame) {
		return undefined;
	}

	const { tone } = account.personality;
	const eligible = bankFor(frame).filter(
		(template) =>
			(template.tones === undefined || template.tones.includes(tone)) &&
			(template.when === undefined || template.when(frame)),
	);
	if (eligible.length === 0) {
		return undefined;
	}

	// Claimed by ID rather than by index, so one line cannot appear twice in a
	// night even though the eligible set differs from post to post - which it
	// always does, because `when` depends on the event.
	const ids = eligible.map((template) => template.id);
	const chosenId = pool.takeUnclaimed(rng, ids, `tmpl:${frame.kind}`);
	const chosen = eligible.find((template) => template.id === chosenId)!;

	return applyVoice({
		text: chosen.text(frame),
		personality: account.personality,
		pool,
		rng,
		positive: positivity(frame),
	});
};

// ---------------------------------------------------------------- CHECKING
//
// Read the finished post back and prove every number in it came from the
// event. This is the guarantee that makes the accuracy dial safe to expose:
// however unreliable an account's OPINIONS are, its numbers are the league's.
//
// The recap engine has the same idea and it earned its keep there, catching a
// generator that quoted a quarter score as a final.

export type PostViolation = { kind: string; detail: string };

export const verifyPostNumbers = (
	text: string,
	facts: SocialEvent["facts"],
): PostViolation[] => {
	const allowed = new Set<string>();
	for (const value of Object.values(facts)) {
		if (typeof value === "number") {
			allowed.add(String(value));
			// A negative fact prints without its sign inside a sentence ("down
			// 14"), and the scan reads digits only - so the magnitude has to be
			// allowed too or every such post fails its own check.
			allowed.add(String(Math.abs(value)));
		} else if (typeof value === "string") {
			for (const found of value.matchAll(/\d+(?:\.\d+)?/g)) {
				allowed.add(found[0]);
			}
		}
	}

	const violations: PostViolation[] = [];
	for (const found of text.matchAll(/\d+(?:\.\d+)?/g)) {
		const literal = found[0]!;
		if (allowed.has(literal)) {
			continue;
		}
		// A percent sign after a number means it was a rate, and rates are
		// rounded for display; allow a stated true-shooting figure to differ in
		// its last place from the stored one rather than failing the post.
		const idx = found.index! + literal.length;
		if (text[idx] === "%") {
			const asNumber = Number(literal);
			if (
				[...allowed].some(
					(candidate) => Math.abs(Number(candidate) - asNumber) < 0.5,
				)
			) {
				continue;
			}
		}
		violations.push({
			kind: "unsourced-number",
			detail: `"${literal}" is not in this event's facts`,
		});
	}
	return violations;
};
