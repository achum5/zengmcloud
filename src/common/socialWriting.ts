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

// Topics that are bad news for everybody involved, whatever the account's
// loyalty. Kept as a set rather than a flag on the event because it is a
// property of the SUBJECT, not of how it was reported.
const BAD_NEWS_TOPICS = new Set(["injury"]);

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
	// Both teams' points added up, which is a real fact on the event rather
	// than arithmetic done in a template - the number checker refuses any
	// numeral it cannot source, and it caught this exact shortcut once already.
	combined: number;
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
	fga: number;
	fta: number;
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
	// Whether this account IS the person the league's line is about. The log
	// writes in the third person, so a player account passing it along says
	// "derek moore had 13 points" about himself - which is the same tell the
	// performance banks already had to fix.
	aboutMe: boolean;
	// A player account, which should REACT to the league's news rather than
	// read it out. Reciting a wire line is what a media account does.
	player: boolean;
	// News nobody celebrates. An injury or a firing still gets posted about,
	// and a doomer still gets to be a doomer about it, but "Let's go." under
	// somebody's torn ankle is the worst line this feed can print.
	bad: boolean;
	// What KIND of news. A trade, an award and a playoff berth all arrive here
	// as one line of the league's prose, and treating them identically is what
	// made every account sound like the same wire service reading the same
	// sentence. Templates specialise on this.
	topic: SocialEvent["topic"];
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
			combined: num(f, "combined"),
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
			fga: num(f, "fga"),
			fta: num(f, "fta"),
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
	return {
		kind: "summary",
		summary,
		aboutMe: account.pid !== undefined && event.pids.includes(account.pid),
		player: account.kind === "player",
		bad: BAD_NEWS_TOPICS.has(event.topic),
		topic: event.topic,
		stance,
	};
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
	// No emoji, no shouting, whatever the account's dials say. A franchise
	// posting about a night it LOST is the one place the enthusiasm machinery
	// has to be switched off entirely.
	quiet?: boolean;
	// Which way THIS LINE leans, when that differs from which way the night
	// went. A fan account answering a defeat with "we are still the best thing
	// going" is defiant, not miserable, and pairing it with a skull - which is
	// what picking the emoji off the result alone did - reads as sarcasm
	// nobody wrote.
	mood?: "up" | "down";
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
		text: (f) =>
			`Never really in doubt: ${f.winner} by ${f.margin} over the ${f.loser}.`,
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
	// ---- FILLING THE MATRIX ------------------------------------------------
	//
	// Measured the same way the reply bank was: how many distinct lines can
	// each tone produce about a win, a loss, and a game it has no stake in?
	// Five cells came back ZERO - a doomer whose team had just WON literally
	// could not post, and neither could an excitable account watching a game
	// it did not care about. A zero cell is not a quiet account; it is an
	// account that disappears from the feed on exactly the nights it should
	// be loudest.

	// DOOM ON A WIN. The whole point of the archetype: it does not enjoy this.
	{
		id: "game.doom.win",
		mood: "down",
		tones: ["doom"],
		when: (f) => f.stance === "for",
		text: (f) =>
			`${f.winner} got the ${f.loser}. Fine. It counts. It changes nothing.`,
	},
	{
		id: "game.doom.win.two",
		mood: "down",
		tones: ["doom", "snark"],
		when: (f) => f.stance === "for",
		text: (f) => `Beat the ${f.loser}. Enjoy tonight, the schedule gets worse.`,
	},
	{
		id: "game.doom.win.close",
		mood: "down",
		tones: ["doom", "snark"],
		when: (f) => f.stance === "for" && f.nailbiter,
		text: (f) =>
			`${f.winner} should not need a finish like that against the ${f.loser}.`,
	},
	{
		id: "game.doom.win.blowout",
		mood: "down",
		tones: ["doom"],
		when: (f) => f.stance === "for" && f.blowout,
		text: (f) =>
			`${f.winner} buried the ${f.loser}. One good night in a season of the other kind.`,
	},
	{
		id: "game.doom.win.streak",
		mood: "down",
		tones: ["doom", "snark"],
		when: (f) => f.stance === "for" && f.streak !== undefined && f.streak >= 3,
		text: (f) =>
			`${f.winner} got the ${f.loser} too. The run ends. It always ends.`,
	},
	{
		id: "game.doom.neutral",
		tones: ["doom", "snark"],
		when: (f) => f.stance === "neutral",
		text: (f) =>
			`${f.winner} beat the ${f.loser}. Everyone above us keeps winning.`,
	},
	{
		id: "game.doom.neutral.two",
		tones: ["doom"],
		when: (f) => f.stance === "neutral",
		text: (f) =>
			`${f.winner} handling the ${f.loser} while we watch. Its own punishment.`,
	},

	// HYPE ON A LOSS AND ON SOMEBODY ELSE'S GAME.
	{
		id: "game.hype.loss",
		mood: "up",
		tones: ["hype", "unhinged"],
		when: (f) => f.stance === "against",
		text: (f) =>
			`${f.winner} got us tonight. We are still the best thing going.`,
	},
	{
		id: "game.hype.loss.close",
		mood: "up",
		tones: ["hype", "unhinged"],
		when: (f) => f.stance === "against" && f.nailbiter,
		text: (f) =>
			`${f.margin} points to the ${f.winner}. That is nothing. We get them next time.`,
	},
	{
		id: "game.hype.loss.blowout",
		mood: "up",
		tones: ["hype", "unhinged"],
		when: (f) => f.stance === "against" && f.blowout,
		text: (f) => `Flush it. Burn the ${f.winner} tape. On to the next one.`,
	},
	{
		id: "game.hype.neutral",
		tones: ["hype", "unhinged"],
		when: (f) => f.stance === "neutral",
		text: (f) =>
			`${f.winner} ${f.winnerPts}, ${f.loser} ${f.loserPts}. Good watch.`,
	},
	{
		id: "game.hype.neutral.close",
		tones: ["hype", "unhinged", "corporate"],
		when: (f) => f.stance === "neutral" && f.nailbiter,
		text: (f) =>
			`${f.winner} and the ${f.loser} went to the wire. That is why you watch.`,
	},
	{
		id: "game.hype.neutral.big",
		tones: ["hype", "unhinged"],
		when: (f) => f.stance === "neutral" && f.blowout,
		text: (f) => `The ${f.winner} did not need most of the fourth quarter.`,
	},

	// UNHINGED ON A GAME IT HAS NO STAKE IN. Loud about nothing is the voice.
	{
		id: "game.unhinged.neutral",
		tones: ["unhinged", "snark"],
		when: (f) => f.stance === "neutral",
		text: (f) => `Somebody check on the ${f.loser}. ${f.winner} did that.`,
	},
	{
		id: "game.unhinged.neutral.two",
		tones: ["unhinged"],
		when: (f) => f.stance === "neutral",
		text: (f) =>
			`not my team not my problem but the ${f.winner} just did that to the ${f.loser}`,
	},

	// SNARK AND WONK, which had one or two lines each in most situations.
	{
		id: "game.snark.win",
		tones: ["snark"],
		when: (f) => f.stance === "for",
		text: (f) =>
			`Beat the ${f.loser}. Against all odds and most of the game plan.`,
	},
	{
		id: "game.snark.neutral",
		tones: ["snark", "wonk"],
		when: (f) => f.stance === "neutral",
		text: (f) =>
			`${f.loser} lost to the ${f.winner} by ${f.margin} and it flattered them.`,
	},
	{
		id: "game.snark.close",
		tones: ["snark", "doom"],
		when: (f) => f.nailbiter,
		text: (f) =>
			`${f.winner} and the ${f.loser} both spent the last two minutes trying to lose it.`,
	},
	{
		id: "game.wonk.margin",
		tones: ["wonk", "wire"],
		text: (f) =>
			`${f.winnerAbbrev} over ${f.loserAbbrev} by ${f.margin}. ${f.combined} combined.`,
	},
	{
		id: "game.wonk.pace",
		tones: ["wonk", "wire"],
		when: (f) => f.combined >= 230,
		text: (f) =>
			`${f.combined} points between ${f.winnerAbbrev} and ${f.loserAbbrev}. Nobody guarded anybody.`,
	},
	{
		id: "game.wonk.low",
		tones: ["wonk", "beat"],
		when: (f) => f.combined <= 190,
		text: (f) =>
			`${f.winnerAbbrev} and ${f.loserAbbrev} combined for ${f.combined}. A grind from the tip.`,
	},

	// CORPORATE, which could say nothing at all about a game it lost or a game
	// somebody else played. A franchise account does post both; it just does
	// not gloat about the second or dwell on the first.
	{
		id: "game.corp.neutral",
		tones: ["corporate", "beat"],
		when: (f) => f.stance === "neutral",
		text: (f) => `${f.winner} ${f.winnerPts}, ${f.loser} ${f.loserPts}.`,
	},

	// WIRE, which is deliberately plain but had only four ways to say it.
	{
		id: "game.wire.short",
		tones: ["wire", "beat"],
		text: (f) =>
			`${f.winnerAbbrev} ${f.winnerPts} — ${f.loserAbbrev} ${f.loserPts}.`,
	},
	{
		id: "game.wire.over",
		tones: ["wire", "beat", "wonk"],
		text: (f) =>
			`${f.winner} over the ${f.loser}, ${f.winnerPts}-${f.loserPts}.`,
	},
	{
		id: "game.wire.playoffs",
		tones: ["wire", "wonk", "corporate"],
		when: (f) => f.playoffs,
		text: (f) =>
			`Postseason: ${f.winnerAbbrev} ${f.winnerPts}, ${f.loserAbbrev} ${f.loserPts}.`,
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
	// ---- FILLING THE MATRIX ------------------------------------------------
	//
	// Snark could not post about a big night at all, and doom could manage one
	// line. Both archetypes have plenty to say about a scorer; they just say
	// it sideways, and nobody had written those lines.
	{
		id: "perf.snark.volume",
		tones: ["snark", "doom"],
		when: (f) => f.fga >= 20,
		text: (f) => `${f.pts} on ${f.fga} shots. Somebody had the green light.`,
	},
	{
		id: "perf.snark.loss",
		tones: ["snark", "doom", "wonk"],
		when: (f) => !f.won && f.pts >= 25,
		text: (f) => `${f.pts} points in a loss. Hope it was worth it.`,
	},
	{
		id: "perf.snark.tov",
		tones: ["snark", "wonk"],
		when: (f) => f.tov >= 5,
		text: (f) => `${f.pts} points and ${f.tov} turnovers. Both are real.`,
	},
	{
		id: "perf.snark.finally",
		tones: ["snark"],
		when: (f) => f.huge,
		text: (f) => `${f.name} decided to show up. Noted for the record.`,
	},
	{
		id: "perf.snark.line",
		tones: ["snark", "doom"],
		text: (f) =>
			`${f.pts}/${f.reb}/${f.ast} from ${f.name}, for whatever that ends up being worth.`,
	},
	{
		id: "perf.doom.alone",
		tones: ["doom"],
		when: (f) => !f.won,
		text: (f) => `${f.name} was the only one who showed up. Again.`,
	},
	{
		id: "perf.doom.waste",
		tones: ["doom", "snark"],
		when: (f) => f.huge && !f.won,
		text: (f) => `A night like that wasted. This is the whole problem.`,
	},
	{
		id: "perf.doom.enjoy",
		tones: ["doom"],
		when: (f) => f.won,
		text: (f) => `${f.name} was good. It will not last and neither will this.`,
	},

	// The plainer voices, which had four or five ways to read a box score.
	{
		id: "perf.wire.slash",
		tones: ["wire", "beat", "wonk"],
		text: (f) => `${f.name}: ${f.pts} pts, ${f.reb} reb, ${f.ast} ast.`,
	},
	{
		id: "perf.wire.vs",
		tones: ["wire", "beat"],
		text: (f) => `${f.name} vs ${f.opponent}: ${f.pts} points.`,
	},
	{
		id: "perf.beat.night",
		tones: ["beat", "wire", "corporate"],
		when: (f) => f.huge,
		text: (f) => `A ${f.pts}-point night for ${f.name}.`,
	},
	{
		id: "perf.wonk.usage",
		tones: ["wonk"],
		when: (f) => f.fga >= 15,
		text: (f) =>
			`${f.pts} points on ${f.fga} attempts and ${f.fta} free throws.`,
	},
	{
		id: "perf.wonk.allaround",
		tones: ["wonk", "beat"],
		when: (f) => f.reb >= 8 && f.ast >= 6,
		text: (f) =>
			`${f.reb} boards and ${f.ast} assists alongside the ${f.pts}. Complete night.`,
	},
	{
		id: "perf.hype.loud",
		tones: ["hype", "unhinged"],
		when: (f) => f.huge,
		text: (f) => `${f.pts}. ${f.name}. That is it. That is the post.`,
	},
	{
		id: "perf.hype.boards",
		tones: ["hype", "unhinged", "corporate"],
		when: (f) => f.reb >= 13,
		text: (f) => `${f.reb} rebounds. ${f.name} owned the glass.`,
	},
	{
		id: "perf.corp.line",
		tones: ["corporate", "hype"],
		text: (f) => `${f.pts} PTS · ${f.reb} REB · ${f.ast} AST — ${f.name}.`,
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
		// Only after a win. A sample day had a player posting "37 and we still
		// had more in the tank" after losing by 29, which is the kind of line
		// that makes a reader stop believing any of it.
		when: (f) => f.won && f.pts >= 30,
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
	{
		id: "mate.work",
		text: (f) => `People do not see the work ${f.name} puts in. I do.`,
	},
	{
		id: "mate.deserve",
		when: (f) => f.huge,
		text: (f) => `Nobody deserves it more than ${f.name}.`,
	},
	{
		id: "mate.brother",
		tones: ["hype", "unhinged"],
		text: (f) => `That is my brother right there. ${f.name}.`,
	},
	{
		id: "mate.easy",
		tones: ["hype", "snark", "unhinged"],
		when: (f) => f.won,
		text: (f) => `${f.name} made that look easy. It was not.`,
	},
	{
		id: "mate.boards",
		when: (f) => f.reb >= 14,
		text: (f) => `${f.name} on the glass all night. Thank you.`,
	},
	{
		id: "mate.passing",
		when: (f) => f.ast >= 10,
		text: (f) => `Easy when ${f.name} is finding you.`,
	},
	{
		id: "mate.quiet",
		tones: ["wire", "beat", "wonk"],
		text: (f) => `Good night for ${f.name}. Deserved.`,
	},
	{
		id: "mate.next",
		when: (f) => f.won,
		text: () => `Team win. On to the next one.`,
	},
	{
		id: "mate.stay",
		mood: "up",
		when: (f) => !f.won,
		text: () => `We stay together. That is the only way through this.`,
	},
	{
		id: "mate.watch",
		when: (f) => f.huge,
		text: (f) => `Watch him. That is all I am going to say. ${f.name}.`,
	},
	{
		id: "mate.everything",
		when: (f) => f.tripleDouble,
		text: (f) => `${f.name} filled every column and still wants more.`,
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
	{
		id: "ins.win.crowd",
		when: (f) => f.stance === "for",
		text: () => `Crowd was different tonight. Appreciate it.`,
	},
	{
		id: "ins.win.defense",
		tones: ["wonk", "beat", "wire"],
		when: (f) => f.stance === "for",
		text: () => `Got stops when we had to. That is the whole game.`,
	},
	{
		id: "ins.win.together",
		when: (f) => f.stance === "for",
		text: () => `Everybody did their job. That is all it takes.`,
	},
	{
		id: "ins.win.loud",
		tones: ["hype", "unhinged"],
		when: (f) => f.stance === "for" && f.blowout,
		text: () => `Not close. Not one minute of it.`,
	},
	{
		id: "ins.win.quiet",
		tones: ["wire", "wonk", "beat"],
		when: (f) => f.stance === "for",
		text: () => `One of eighty-two. On to the next.`,
	},
	{
		id: "ins.loss.own",
		when: (f) => f.stance === "against",
		text: () => `I have to be better. Simple as that.`,
	},
	{
		id: "ins.loss.film",
		tones: ["wonk", "beat", "wire"],
		when: (f) => f.stance === "against",
		text: () => `We will watch it, we will own it, and we will move on.`,
	},
	{
		id: "ins.loss.short",
		tones: ["snark", "doom", "unhinged"],
		when: (f) => f.stance === "against",
		text: () => `Not good enough. Nothing else to say tonight.`,
	},
	{
		id: "ins.loss.effort",
		mood: "up",
		when: (f) => f.stance === "against" && !f.blowout,
		text: () => `Effort was there. Execution was not.`,
	},
	{
		id: "ins.streak.long",
		when: (f) => f.stance === "for" && f.streak !== undefined && f.streak >= 7,
		text: () => `Not thinking about the run. Thinking about the next one.`,
	},
	{
		id: "ins.ot",
		when: (f) => f.ot > 0,
		text: () => `Long night. Would not want it any other way.`,
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
		when: (f) => !f.bad,
		text: (f) => `${f.summary} Let's go.`,
	},
	{
		// The same voices still have to say SOMETHING when the news is bad.
		id: "sum.hype.bad",
		mood: "down",
		tones: ["hype", "corporate"],
		when: (f) => f.bad,
		text: (f) => `${f.summary} Speedy recovery.`,
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
		when: (f) => !f.bad,
		text: (f) => `${f.summary} Who signed off on this?`,
	},

	// ---- BY TOPIC ----------------------------------------------------------
	//
	// Everything above treats the league's news as one undifferentiated wire
	// line, which is how a trade, an award and a torn ankle all ended up
	// reading the same. Below, each kind of news gets the reaction it actually
	// draws. These sit alongside the generic lines rather than replacing them,
	// so a topic with no family here still has something to say.

	// Injuries.
	{
		id: "sum.inj.length",
		tones: ["wire", "beat", "wonk"],
		when: (f) => f.topic === "injury",
		text: (f) => `${f.summary} The rotation changes tonight.`,
	},
	{
		id: "sum.inj.doom",
		tones: ["doom", "snark"],
		when: (f) => f.topic === "injury",
		text: (f) => `${f.summary} Of course. Of course it is him.`,
	},
	{
		id: "sum.inj.hope",
		mood: "up",
		tones: ["hype", "corporate", "beat"],
		when: (f) => f.topic === "injury",
		text: (f) => `${f.summary} Wishing him a fast one.`,
	},
	{
		id: "sum.inj.next",
		tones: ["wonk", "beat", "wire"],
		when: (f) => f.topic === "injury",
		text: (f) => `${f.summary} Somebody is about to get minutes.`,
	},

	// Trades and releases.
	{
		id: "sum.trade.grade",
		tones: ["wonk", "beat", "snark"],
		when: (f) => f.topic === "trade",
		text: (f) => `${f.summary} Grading this one in three years.`,
	},
	{
		id: "sum.trade.why",
		tones: ["snark", "doom", "unhinged"],
		when: (f) => f.topic === "trade",
		text: (f) => `${f.summary} Explain the plan. Slowly.`,
	},
	{
		id: "sum.trade.like",
		tones: ["hype", "corporate", "beat"],
		when: (f) => f.topic === "trade",
		text: (f) => `${f.summary} Like it. Fits what they needed.`,
	},
	{
		id: "sum.trade.more",
		tones: ["wire", "beat"],
		when: (f) => f.topic === "trade",
		text: (f) => `${f.summary} Expect this not to be the last one.`,
	},

	// Free agency and signings.
	{
		id: "sum.fa.fit",
		tones: ["wonk", "beat"],
		when: (f) => f.topic === "freeAgency",
		text: (f) => `${f.summary} Fit matters more than the number here.`,
	},
	{
		id: "sum.fa.price",
		tones: ["snark", "doom", "wonk"],
		when: (f) => f.topic === "freeAgency",
		text: (f) => `${f.summary} They paid for last season, not next one.`,
	},
	{
		id: "sum.fa.win",
		tones: ["hype", "corporate"],
		when: (f) => f.topic === "freeAgency",
		text: (f) => `${f.summary} Welcome aboard.`,
	},

	// Awards and milestones.
	{
		id: "sum.award.earned",
		tones: ["beat", "wire", "corporate", "hype"],
		when: (f) => f.topic === "awards" || f.topic === "milestone",
		text: (f) => `${f.summary} Earned every bit of it.`,
	},
	{
		id: "sum.award.snark",
		tones: ["snark", "doom"],
		when: (f) => f.topic === "awards",
		text: (f) => `${f.summary} Voters got there eventually.`,
	},
	{
		id: "sum.award.history",
		tones: ["wonk", "wire", "beat"],
		when: (f) => f.topic === "milestone",
		text: (f) => `${f.summary} One for the record book.`,
	},

	// Draft.
	{
		id: "sum.draft.board",
		tones: ["wonk", "beat", "wire"],
		when: (f) => f.topic === "draft",
		text: (f) => `${f.summary} That was not where the board had him.`,
	},
	{
		id: "sum.draft.hype",
		tones: ["hype", "corporate"],
		when: (f) => f.topic === "draft",
		text: (f) => `${f.summary} Great day for this franchise.`,
	},
	{
		id: "sum.draft.doom",
		tones: ["doom", "snark"],
		when: (f) => f.topic === "draft",
		text: (f) => `${f.summary} Reaching, as usual.`,
	},

	// Standings and playoff races.
	{
		id: "sum.stand.race",
		tones: ["wire", "beat", "wonk"],
		when: (f) => f.topic === "standings",
		text: (f) => `${f.summary} The race is not settled yet.`,
	},
	{
		id: "sum.stand.hype",
		tones: ["hype", "corporate", "unhinged"],
		when: (f) => f.topic === "standings",
		text: (f) => `${f.summary} We are not done.`,
	},
	{
		id: "sum.stand.doom",
		tones: ["doom", "snark"],
		when: (f) => f.topic === "standings",
		text: (f) => `${f.summary} Setting up the usual disappointment.`,
	},

	// Money.
	{
		id: "sum.money.sheet",
		tones: ["wonk", "wire", "beat"],
		when: (f) => f.topic === "money",
		text: (f) => `${f.summary} The sheet does not lie.`,
	},
	{
		id: "sum.money.snark",
		tones: ["snark", "doom", "unhinged"],
		when: (f) => f.topic === "money",
		text: (f) => `${f.summary} Someone should have read the cap rules.`,
	},
];

// THE LEAGUE'S NEWS, WHEN IT IS ABOUT YOU. Never quotes the log line, because
// the log is written in the third person and reading it aloud about yourself is
// the giveaway this whole design keeps having to design around.
const SELF_SUMMARY_TEMPLATES: Template<SummaryFrame>[] = [
	{
		id: "selfsum.blessed",
		when: (f) => !f.bad,
		text: () => `Blessed. Thank you all.`,
	},
	{ id: "selfsum.work", text: () => `Work is not finished.` },
	{
		id: "selfsum.team",
		when: (f) => !f.bad,
		text: () => `None of this happens without my teammates.`,
	},
	{
		id: "selfsum.hype",
		tones: ["hype", "unhinged", "corporate"],
		when: (f) => !f.bad,
		text: () => `Told you.`,
	},
	{
		id: "selfsum.quiet",
		tones: ["wire", "beat", "wonk", "doom"],
		when: (f) => !f.bad,
		text: () => `Appreciate it. Back to work tomorrow.`,
	},
	{
		id: "selfsum.family",
		when: (f) => !f.bad,
		text: () => `For my family and everyone who backed me.`,
	},
	// The player it happened to.
	{
		id: "selfsum.setback",
		mood: "up",
		when: (f) => f.bad,
		text: () => `Setback, not the end. See you soon.`,
	},
	{
		id: "selfsum.rehab",
		when: (f) => f.bad,
		text: () => `Appreciate the messages. Rehab starts now.`,
	},
	{
		id: "selfsum.grateful",
		when: (f) => !f.bad,
		text: () => `Grateful. That is the only word for it.`,
	},
	{
		id: "selfsum.city",
		when: (f) => !f.bad,
		text: () => `This city has had my back from day one.`,
	},
	{
		id: "selfsum.long",
		when: (f) => !f.bad,
		text: () => `Long road. Would not change a step of it.`,
	},
	{
		id: "selfsum.staff",
		when: (f) => !f.bad,
		tones: ["wire", "beat", "wonk", "corporate"],
		text: () => `Thank you to the staff nobody sees. This is theirs too.`,
	},
	{
		id: "selfsum.short",
		tones: ["snark", "doom"],
		when: (f) => !f.bad,
		text: () => `Appreciated. Moving on.`,
	},
	{
		id: "selfsum.more",
		tones: ["hype", "unhinged"],
		when: (f) => !f.bad,
		text: () => `And I am not close to done.`,
	},
	{
		id: "selfsum.patient",
		mood: "up",
		when: (f) => f.bad,
		text: () => `Been through worse. I will be fine.`,
	},
	{
		id: "selfsum.support",
		mood: "up",
		when: (f) => f.bad,
		text: () => `Still with my guys every night. Just from the bench.`,
	},
];

// THE LEAGUE'S NEWS ABOUT SOMEBODY ELSE, said by a player. Short, and never a
// recital: a player reacting to a team-mate's award does not read the press
// release aloud.
const PLAYER_SUMMARY_TEMPLATES: Template<SummaryFrame>[] = [
	{
		id: "psum.congrats",
		when: (f) => !f.bad,
		text: () => `Congrats bro. Well deserved.`,
	},
	{ id: "psum.earned", when: (f) => !f.bad, text: () => `Earned, not given.` },
	{ id: "psum.happy", when: (f) => !f.bad, text: () => `Happy for him.` },
	{
		id: "psum.hype",
		tones: ["hype", "unhinged"],
		when: (f) => !f.bad,
		text: () => `LETS GOOO`,
	},
	{
		id: "psum.quiet",
		tones: ["wire", "beat", "wonk", "doom", "snark"],
		when: (f) => !f.bad,
		text: () => `Good for him.`,
	},
	{
		id: "psum.next",
		when: (f) => !f.bad,
		text: () => `Now we go get the next one.`,
	},
	// What a team-mate actually posts under an injury.
	{ id: "psum.prayers", when: (f) => f.bad, text: () => `Prayers up. 🙏` },
	{
		id: "psum.speedy",
		when: (f) => f.bad,
		text: () => `Speedy recovery brother.`,
	},
	{
		id: "psum.backsoon",
		when: (f) => f.bad,
		mood: "up",
		text: () => `He'll be back.`,
	},
	{
		id: "psum.tough",
		mood: "up",
		when: (f) => f.bad,
		text: () => `Toughest guy I know. He will be back sooner than they say.`,
	},
	{
		id: "psum.hate",
		when: (f) => f.bad,
		tones: ["snark", "doom", "unhinged"],
		text: () => `Hate to see it. Genuinely.`,
	},
	{
		id: "psum.deserved",
		when: (f) => !f.bad,
		text: () => `Been saying it. Glad everyone else caught up.`,
	},
	{
		id: "psum.watched",
		when: (f) => !f.bad,
		text: () => `Watched him put that work in. Nobody handed him anything.`,
	},
	{
		id: "psum.salute",
		when: (f) => !f.bad,
		text: () => `Salute.`,
	},
	{
		id: "psum.time",
		when: (f) => !f.bad,
		tones: ["hype", "unhinged", "corporate"],
		text: () => `About time.`,
	},
	{
		id: "psum.proud",
		when: (f) => !f.bad,
		text: () => `Proud of him. That is all.`,
	},
	{
		id: "psum.brother",
		when: (f) => !f.bad,
		text: () => `My brother. Well deserved.`,
	},
	{
		id: "psum.next.one",
		when: (f) => !f.bad,
		tones: ["wire", "beat", "wonk"],
		text: () => `Good for the group. Back in tomorrow.`,
	},
];

// ---------------------------------------------------------------- REPLIES
//
// An answer has to be about the POST, not just about the game, or a thread is
// two people talking past each other with the same box score. So a reply frame
// carries who is being answered and whether the replier agrees with them, and
// the banks are organised by that rather than by event type.
//
// Quotes and replies draw from the same banks. The difference is presentational
// - a quote shows the original above it - and writing two sets of lines for one
// relationship would thin both.

export type ReplyFrame = {
	parentName: string;
	parentHandle: string;
	// Whether the two accounts are on the same side of what happened. Not the
	// same as agreeing with each other, which is what makes a thread readable:
	// two fans of the same team can be at each other precisely because they
	// watched the same thing.
	sameSide: boolean;
	// History between them, 0 to 1, from socialFeuds.
	heat: number;
	// The replier reads the post as wrong on the facts.
	correcting: boolean;
	// A QUOTE rather than a reply. Presentationally the quoted post sits below
	// it, but it lands in the timeline as its own post, and that changes what
	// can be said: "Fair." works under someone's post and is nonsense as an
	// entry in a feed. Quotes therefore lose the one-word agreements and gain
	// lines that stand on their own.
	quote: boolean;
	subject: Frame;
};

const REPLY_TEMPLATES: Template<any>[] = [
	// Agreement.
	{
		id: "re.agree.short",
		tones: ["hype", "beat", "corporate", "unhinged"],
		when: (f: ReplyFrame) => !f.quote && f.sameSide && f.heat < 0.4,
		text: () => `Exactly this.`,
	},
	{
		id: "re.agree.name",
		tones: ["hype", "beat", "corporate"],
		when: (f: ReplyFrame) => f.sameSide && f.heat < 0.4,
		text: (f: ReplyFrame) => `${f.parentName} gets it.`,
	},
	{
		id: "re.agree.finally",
		tones: ["hype", "unhinged", "snark"],
		when: (f: ReplyFrame) => !f.quote && f.sameSide,
		text: () => `Finally somebody says it.`,
	},
	// Correction. The one reply that needs a number, and it takes it from the
	// same facts the original post was held to.
	{
		id: "re.correct.score",
		tones: ["wonk", "beat", "wire"],
		when: (f: ReplyFrame) => f.correcting && f.subject.kind === "game",
		text: (f: ReplyFrame) => {
			const game = f.subject as GameFrame;
			return `It was ${game.winnerPts}-${game.loserPts}. The margin was ${game.margin}.`;
		},
	},
	{
		id: "re.correct.line",
		tones: ["wonk", "beat"],
		when: (f: ReplyFrame) => f.correcting && f.subject.kind === "performance",
		text: (f: ReplyFrame) => {
			const perf = f.subject as PerformanceFrame;
			return `${perf.pts} points on ${perf.tov} turnovers. Worth mentioning both.`;
		},
	},
	{
		id: "re.correct.soft",
		tones: ["wonk", "beat", "wire"],
		when: (f: ReplyFrame) => f.correcting,
		text: () => `This is not what the box score says.`,
	},
	// Disagreement without a correction.
	{
		id: "re.disagree.plain",
		tones: ["snark", "doom", "wonk"],
		when: (f: ReplyFrame) => !f.sameSide,
		text: () => `Respectfully, no.`,
	},
	{
		id: "re.disagree.name",
		tones: ["snark", "doom", "unhinged"],
		when: (f: ReplyFrame) => !f.sameSide,
		text: (f: ReplyFrame) => `${f.parentName} says this every week.`,
	},
	{
		id: "re.disagree.wait",
		tones: ["snark", "doom"],
		when: (f: ReplyFrame) => !f.sameSide,
		text: () => `Ask me again in a month.`,
	},
	// Heat. Only available once there is history, which is what makes a feud
	// feel earned rather than declared.
	{
		id: "re.heat.again",
		tones: ["snark", "unhinged", "doom"],
		when: (f: ReplyFrame) => f.heat >= 0.5,
		text: (f: ReplyFrame) => `You again, ${f.parentHandle}.`,
	},
	{
		id: "re.heat.record",
		tones: ["snark", "unhinged", "hype"],
		when: (f: ReplyFrame) => f.heat >= 0.5 && !f.sameSide,
		text: () => `Imagine typing this with your season.`,
	},
	{
		id: "re.heat.blocked",
		tones: ["unhinged", "snark"],
		when: (f: ReplyFrame) => f.heat >= 0.6,
		text: () => `Not reading all that. Wrong anyway.`,
	},
	// Despair and celebration under someone else's post.
	{
		id: "re.doom.same",
		tones: ["doom"],
		when: (f: ReplyFrame) => f.sameSide,
		text: () => `Enjoy it while it lasts.`,
	},
	{
		id: "re.hype.same",
		tones: ["hype", "unhinged"],
		when: (f: ReplyFrame) => f.sameSide,
		text: () => `SAY IT LOUDER`,
	},
	{
		id: "re.doom.knew",
		tones: ["doom", "snark"],
		when: (f: ReplyFrame) => f.sameSide,
		text: () => `We have seen this movie. It does not end well.`,
	},
	{
		id: "re.doom.warn",
		tones: ["doom"],
		text: () => `Check back in April.`,
	},
	{
		id: "re.doom.tired",
		tones: ["doom", "snark", "wonk"],
		when: (f: ReplyFrame) => f.sameSide,
		text: () => `One night does not fix the roster.`,
	},
	{
		id: "re.wire.add",
		tones: ["wire", "beat", "wonk"],
		text: (f: ReplyFrame) =>
			f.subject.kind === "game"
				? `Worth adding: the margin was ${(f.subject as GameFrame).margin}.`
				: `Worth adding: ${(f.subject as PerformanceFrame).reb} rebounds too.`,
	},
	{
		id: "re.beat.context",
		tones: ["beat", "wire", "corporate"],
		text: () => `Some context here, but not an unreasonable read.`,
	},
	{
		id: "re.wonk.sample",
		tones: ["wonk", "beat"],
		when: (f: ReplyFrame) => !f.sameSide,
		text: () => `One game is not a sample.`,
	},
	{
		id: "re.hype.cosign",
		tones: ["hype", "corporate", "unhinged"],
		when: (f: ReplyFrame) => !f.quote,
		text: () => `Co-signed.`,
	},
	{
		id: "re.snark.ok",
		tones: ["snark", "unhinged"],
		when: (f: ReplyFrame) => !f.quote,
		text: () => `Okay.`,
	},
	{
		id: "re.snark.bookmark",
		tones: ["snark", "wonk", "doom"],
		when: (f: ReplyFrame) => !f.sameSide,
		text: () => `Bookmarking this one.`,
	},
	// QUOTES. A quote lands in the timeline on its own, so every line here has
	// to read as a post - it names what it is quoting rather than assuming the
	// reader just saw it.
	{
		id: "qt.agree",
		tones: ["hype", "corporate", "beat"],
		when: (f: ReplyFrame) => f.quote && f.sameSide,
		text: (f: ReplyFrame) => `${f.parentName} has it right here.`,
	},
	{
		id: "qt.agree.loud",
		tones: ["hype", "unhinged"],
		when: (f: ReplyFrame) => f.quote && f.sameSide,
		text: () => `Putting this at the top of the timeline.`,
	},
	{
		id: "qt.disagree",
		tones: ["snark", "doom", "wonk"],
		when: (f: ReplyFrame) => f.quote && !f.sameSide,
		text: (f: ReplyFrame) =>
			`Posting this so you can see what ${f.parentName} thinks is normal.`,
	},
	{
		id: "qt.disagree.soft",
		tones: ["wonk", "beat", "wire"],
		when: (f: ReplyFrame) => f.quote && !f.sameSide,
		text: () => `I do not read it this way at all.`,
	},
	{
		id: "qt.heat",
		tones: ["snark", "unhinged", "doom"],
		when: (f: ReplyFrame) => f.quote && f.heat >= 0.5,
		text: (f: ReplyFrame) => `${f.parentHandle} again. Every single week.`,
	},
	{
		id: "qt.correct",
		tones: ["wonk", "wire", "beat"],
		when: (f: ReplyFrame) =>
			f.quote && f.correcting && f.subject.kind === "game",
		text: (f: ReplyFrame) => {
			const game = f.subject as GameFrame;
			return `Correcting the record: ${game.winnerPts}-${game.loserPts}, margin of ${game.margin}.`;
		},
	},
	{
		id: "qt.doom",
		tones: ["doom", "snark"],
		when: (f: ReplyFrame) => f.quote,
		text: () => `Screenshotting this for April.`,
	},
	{
		id: "qt.wonk",
		tones: ["wonk", "beat", "wire"],
		when: (f: ReplyFrame) => f.quote,
		text: () => `Worth keeping, whichever way it ends up going.`,
	},
	{
		id: "qt.hype.disagree",
		tones: ["hype", "corporate", "unhinged"],
		when: (f: ReplyFrame) => f.quote && !f.sameSide,
		text: () => `Leaving this here for when it ages.`,
	},
	{
		id: "qt.hype.disagree.two",
		tones: ["hype", "unhinged", "corporate"],
		when: (f: ReplyFrame) => f.quote && !f.sameSide,
		text: (f: ReplyFrame) =>
			`${f.parentName} is entitled to be wrong in public.`,
	},
	{
		id: "qt.hype.disagree.three",
		tones: ["hype", "unhinged"],
		when: (f: ReplyFrame) => f.quote && !f.sameSide,
		text: () => `Every year somebody posts this. Every year.`,
	},
	{
		id: "qt.corp.disagree",
		tones: ["corporate", "beat", "wire"],
		when: (f: ReplyFrame) => f.quote && !f.sameSide,
		text: () => `A different view, and worth reading anyway.`,
	},
	{
		id: "qt.any.file",
		when: (f: ReplyFrame) => f.quote,
		text: () => `Filing this one away.`,
	},
	{
		id: "qt.any.thread",
		when: (f: ReplyFrame) => f.quote,
		text: (f: ReplyFrame) =>
			`Everyone should read what ${f.parentName} just posted.`,
	},
	{
		id: "qt.any.month",
		when: (f: ReplyFrame) => f.quote,
		text: () => `Revisiting this in a month either way.`,
	},
	{
		id: "qt.neutral",
		when: (f: ReplyFrame) => f.quote,
		text: () => `Noting this one down.`,
	},
	// ---- FILLING THE MATRIX ------------------------------------------------
	//
	// Measured: for each tone crossed with (same side / other side / heat /
	// correcting), how many distinct lines could this bank produce? Four cells
	// came back with ONE, and a cell with one line is a catch-all that repeats
	// every time it comes up - which is exactly what showed up in a fortnight
	// of output as "Noting this one down." six times. The lines below are the
	// holes, not decoration: enthusiastic and corporate voices had nothing to
	// say when they disagreed, and the wire voices had nothing for agreement.

	// Hype and corporate, disagreeing. They do not sneer; they redirect.
	{
		id: "re.hype.disagree",
		tones: ["hype", "corporate", "unhinged"],
		when: (f: ReplyFrame) => !f.sameSide,
		text: () => `Respect it, but we see this one completely differently.`,
	},
	{
		id: "re.hype.disagree.watch",
		tones: ["hype", "unhinged"],
		when: (f: ReplyFrame) => !f.sameSide,
		text: () => `Keep talking. We are listening.`,
	},
	{
		id: "re.corp.disagree",
		tones: ["corporate", "beat"],
		when: (f: ReplyFrame) => !f.sameSide,
		text: () => `Understandable read. We like where we are.`,
	},
	{
		id: "re.hype.confident",
		tones: ["hype", "corporate", "unhinged"],
		when: (f: ReplyFrame) => !f.sameSide,
		text: () => `Save this one. Genuinely.`,
	},
	{
		id: "re.hype.late",
		tones: ["hype", "unhinged"],
		when: (f: ReplyFrame) => !f.sameSide,
		text: () => `You will come around like everyone else did.`,
	},

	// Wire and beat, agreeing. Measured, not enthusiastic.
	{
		id: "re.wire.confirm",
		tones: ["wire", "beat", "wonk"],
		when: (f: ReplyFrame) => f.sameSide,
		text: () => `This matches what I had.`,
	},
	{
		id: "re.wire.same",
		tones: ["wire", "beat"],
		when: (f: ReplyFrame) => f.sameSide,
		text: () => `Same read here.`,
	},
	{
		id: "re.beat.detail",
		tones: ["beat", "wire", "wonk"],
		when: (f: ReplyFrame) => f.sameSide,
		text: () => `Add that it held up in the second half too.`,
	},
	{
		id: "re.wonk.agree",
		tones: ["wonk", "wire"],
		when: (f: ReplyFrame) => f.sameSide,
		text: () => `The numbers back this up, for once.`,
	},

	// Available to everybody, which is what keeps the thin cells off the
	// catch-all without making any one voice sound like another.
	{
		id: "re.any.honest",
		text: () => `Honest answer: could go either way.`,
	},
	{
		id: "re.any.month",
		text: () => `Talk to me in a month about this one.`,
	},
	{
		id: "re.any.point",
		when: (f: ReplyFrame) => !f.sameSide,
		text: (f: ReplyFrame) => `${f.parentName} is not wrong about all of it.`,
	},
	{
		id: "re.any.strong",
		when: (f: ReplyFrame) => !f.sameSide,
		text: () => `Strong take. Not sure it survives the week.`,
	},
	{
		id: "re.any.watching",
		text: () => `Watching this one closely.`,
	},

	// The catch-all, so a thread is never left dangling for want of a line.
	{
		id: "re.neutral",
		when: (f: ReplyFrame) => !f.quote,
		text: () => `Fair.`,
	},
];

export const writeReply = ({
	account,
	parent,
	event,
	heat,
	quote,
	pool,
	rng,
	avoid,
}: {
	account: ResolvedSocialAccount;
	parent: ResolvedSocialAccount;
	event: SocialEvent;
	heat: number;
	quote?: boolean;
	pool: PhrasePool;
	rng: () => number;
	avoid?: (text: string) => boolean;
}): string | undefined => {
	const subject = frameFor(account, event);
	if (!subject) {
		return undefined;
	}
	const parentStance = stanceOf(parent, event);
	const frame: ReplyFrame = {
		parentName: parent.name,
		parentHandle: `@${parent.handle}`,
		sameSide: parentStance === subject.stance,
		heat,
		// Only an account with a genuinely higher bar reads another as wrong.
		correcting:
			account.personality.accuracy - parent.personality.accuracy >= 0.3,
		quote: quote === true,
		subject,
	};

	const { tone } = account.personality;
	const eligible = REPLY_TEMPLATES.filter(
		(template) =>
			(template.tones === undefined || template.tones.includes(tone)) &&
			(template.when === undefined || template.when(frame)),
	);
	if (eligible.length === 0) {
		return undefined;
	}
	return writeFirstAcceptable({
		eligible,
		frame,
		account,
		pool,
		rng,
		ledgerKey: frame.quote ? "tmpl:quote" : "tmpl:reply",
		avoid,
	});
};

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
// Words nobody emphasizes. Shouting one is the tell that a machine chose the
// span: "in A 105-104 win", "enjoy it WHILE IT lasts", "win IS a win". A run
// has to both start and end on something worth raising your voice about.
const NEVER_SHOUT = new Set([
	"a",
	"an",
	"the",
	"and",
	"or",
	"but",
	"if",
	"so",
	"of",
	"to",
	"in",
	"on",
	"at",
	"by",
	"for",
	"from",
	"with",
	"as",
	"is",
	"was",
	"are",
	"were",
	"be",
	"been",
	"it",
	"its",
	"he",
	"his",
	"him",
	"they",
	"them",
	"we",
	"us",
	"our",
	"you",
	"your",
	"that",
	"this",
	"these",
	"those",
	"there",
	"here",
	"than",
	"then",
	"over",
	"out",
	"up",
	"down",
	"into",
	"about",
	"after",
	"before",
	"while",
	"when",
	"how",
	"who",
	"not",
	"no",
	"do",
	"does",
	"did",
	"has",
	"have",
	"had",
	"will",
	"would",
	"can",
	"could",
	"just",
	"still",
	"very",
	"all",
	"any",
	"both",
	"each",
	"some",
	"such",
	"only",
	"even",
	"also",
	"more",
	"most",
	"much",
	"many",
	"own",
	"off",
	"per",
	"yet",
	"too",
	"nor",
	"one",
	"two",
	"back",
	"get",
	"got",
	"go",
	"goes",
	"went",
	"say",
	"says",
	"said",
	"make",
	"makes",
	"made",
	"take",
	"takes",
	"took",
	"come",
	"came",
	"now",
	"well",
	"what",
	"which",
	"why",
	"where",
	"am",
	"im",
	"ive",
	"its",
]);

// Strips the punctuation a token is wearing before asking what word it is, so
// "lasts." and "it," are judged as "lasts" and "it".
const shoutable = (token: string): boolean => {
	const bare = token.replaceAll(/[^\dA-Za-z]/g, "").toLowerCase();
	return bare.length > 0 && !NEVER_SHOUT.has(bare);
};

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

	// Only start on a real word. If the line is nothing but function words -
	// which happens to the very short reply banks - say it normally.
	const starts = words
		.map((word, i) => (shoutable(word) ? i : -1))
		.filter((i) => i >= 0 && i <= words.length - 1);
	if (starts.length === 0) {
		return text;
	}
	const start = starts[Math.floor(rng() * starts.length)]!;

	// At most half the line. Capping the RUN rather than the chance is what
	// keeps this from occasionally shouting an entire post, which is both
	// unreadable and not what anybody actually does.
	let len = Math.min(
		words.length - start,
		1 + Math.floor(rng() * 3),
		Math.max(1, Math.floor(words.length / 2)),
	);
	// Trim function words off the END of the run for the same reason they
	// cannot begin it.
	while (len > 1 && !shoutable(words[start + len - 1]!)) {
		len -= 1;
	}

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
// A FRANCHISE ON A NIGHT IT LOST. This used to be an empty bank, which meant
// the club account simply vanished from the feed on half of its own games.
// Real club accounts do post - they post four flat words and no emoji, which
// is what these are. Kept out of GAME_TEMPLATES so nothing celebratory can
// ever leak in.
const CORPORATE_LOSS_TEMPLATES: Template<GameFrame>[] = [
	{
		id: "corp.loss.plain",
		quiet: true,
		text: () => `Not our night. Back at it tomorrow.`,
	},
	{
		id: "corp.loss.close",
		quiet: true,
		when: (f) => f.nailbiter,
		text: () => `Came up just short. Thank you to everyone who was there.`,
	},
	{
		id: "corp.loss.final",
		quiet: true,
		text: (f) =>
			`Final from tonight: ${f.winnerAbbrev} ${f.winnerPts}, ${f.loserAbbrev} ${f.loserPts}.`,
	},
	{
		id: "corp.loss.big",
		quiet: true,
		when: (f) => f.blowout,
		text: () => `Nothing to say about that one. We will be better.`,
	},
	{
		id: "corp.loss.thanks",
		quiet: true,
		text: () => `Thank you for the support tonight. On to the next.`,
	},
	{
		id: "corp.loss.back",
		quiet: true,
		when: (f) => !f.blowout,
		text: () => `We will look at it and we will be back out there.`,
	},
];

const bankFor = (frame: Frame): Template<any>[] => {
	if (frame.kind === "game") {
		// A franchise's own account does not announce its own defeat the way
		// it announces a win. The first sample day had the Heat posting their
		// own 130-99 loss with a crying emoji, which no club account has ever
		// done; it gets a flat, emoji-free bank of its own instead.
		if (frame.stance === "against" && frame.corporate) {
			return CORPORATE_LOSS_TEMPLATES;
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
	if (frame.aboutMe) {
		return SELF_SUMMARY_TEMPLATES;
	}
	if (frame.player) {
		return PLAYER_SUMMARY_TEMPLATES;
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

// Walk the eligible templates in the pool's rotation order and return the
// first finished line the caller will accept. The ledger already stops a
// template repeating within a batch; `avoid` is the caller's stronger claim -
// that this exact SENTENCE has already been said today, whoever said it and
// whichever template produced it. Two templates can converge on the same words
// once voice has had its way with them, and a reader does not care which bank
// they came from.
const writeFirstAcceptable = ({
	eligible,
	frame,
	account,
	pool,
	rng,
	ledgerKey,
	avoid,
}: {
	eligible: Template<any>[];
	frame: Frame | ReplyFrame;
	account: ResolvedSocialAccount;
	pool: PhrasePool;
	rng: () => number;
	ledgerKey: string;
	avoid: ((text: string) => boolean) | undefined;
}): string | undefined => {
	const remaining = [...eligible];
	let fallback: string | undefined;

	while (remaining.length > 0) {
		const ids = remaining.map((template) => template.id);
		const chosenId = pool.takeUnclaimed(rng, ids, ledgerKey);
		const index = remaining.findIndex((template) => template.id === chosenId);
		const chosen = remaining[index === -1 ? 0 : index]!;
		remaining.splice(index === -1 ? 0 : index, 1);

		const text = applyVoice({
			text: chosen.text(frame as any),
			personality: chosen.quiet
				? { ...account.personality, emoji: 0, caps: 0, profanity: 0 }
				: account.personality,
			pool,
			rng,
			positive:
				chosen.mood === undefined
					? "kind" in frame
						? positivity(frame as Frame)
						: positivity((frame as ReplyFrame).subject)
					: chosen.mood === "up",
		});
		fallback ??= text;
		if (avoid === undefined || !avoid(text)) {
			return text;
		}
	}

	// Every line this account could say today has already been said today. Say
	// nothing rather than say it twice - a quiet account reads better than a
	// duplicated one, and the day is already over its target anyway.
	return undefined;
};

export const writePost = ({
	account,
	event,
	pool,
	rng,
	avoid,
}: {
	account: ResolvedSocialAccount;
	event: SocialEvent;
	pool: PhrasePool;
	rng: () => number;
	// True when the caller has already used this exact text today.
	avoid?: (text: string) => boolean;
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
	return writeFirstAcceptable({
		eligible,
		frame,
		account,
		pool,
		rng,
		ledgerKey: `tmpl:${frame.kind}`,
		avoid,
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
