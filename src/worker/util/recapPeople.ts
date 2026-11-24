// WHO THEY ARE: careers, debuts, moves, trips and the race, in words.
//
// recapCareer.ts derives the facts; this turns them into the sentences a
// wire story carries - "It was Devin Nash's first career triple-double",
// "Franz Owens made his Nuggets debut after coming over from the Celtics in
// a trade", "The Knicks finished their five-game trip 3-2", "Denver became
// the first team to clinch a playoff spot". The game story uses them as
// notes, the Takeaways as lines, with a subject each passes in: the story
// says "The Nuggets", a takeaway "Denver".

import type { RecapPlayer, RecapTeam } from "./getDayGamesForRecap.ts";
import { doublesOf } from "./recapCareer.ts";
import { numWord, ordinal, pick, poss, statPhrase } from "./recapText.ts";

type Rng = () => number;

// ---------------------------------------------------------------- HIS NIGHT

export type CareerHigh = {
	n: number;
	key: "pts" | "reb" | "ast" | "tp";
	unit: "points" | "rebounds" | "assists" | "3-pointers";
	prev: number;
};

// A career high tonight, by bars that make it news: points first, then the
// glass, the passing and the threes. A man needs a career behind him - a
// rookie's "career high" in his third game is no news.
export const careerHighOf = (p: RecapPlayer): CareerHigh | undefined => {
	const past = p.past;
	if (!past || past.gp < 10) {
		return undefined;
	}
	const h = past.high;
	if (p.pts >= 15 && p.pts > h.pts) {
		return { n: p.pts, key: "pts", unit: "points", prev: h.pts };
	}
	if (p.reb >= 12 && p.reb > h.reb) {
		return { n: p.reb, key: "reb", unit: "rebounds", prev: h.reb };
	}
	if (p.ast >= 10 && p.ast > h.ast) {
		return { n: p.ast, key: "ast", unit: "assists", prev: h.ast };
	}
	if (p.tp >= 5 && p.tp > h.tp) {
		return { n: p.tp, key: "tp", unit: "3-pointers", prev: h.tp };
	}
	return undefined;
};

export type First =
	| "triple-double"
	| "40-point game"
	| "30-point game"
	| "double-double";

// The first of its kind in his career, the rarest first.
export const firstOf = (p: RecapPlayer): First | undefined => {
	const past = p.past;
	if (!past || past.gp < 1) {
		return undefined;
	}
	const doubles = doublesOf(p);
	if (doubles >= 3 && past.td === 0) {
		return "triple-double";
	}
	if (p.pts >= 40 && past.high.pts < 40) {
		return "40-point game";
	}
	if (p.pts >= 30 && past.high.pts < 30) {
		return "30-point game";
	}
	if (doubles >= 2 && past.dd === 0 && past.gp >= 3) {
		return "double-double";
	}
	return undefined;
};

// His first game in the league.
export const isDebut = (p: RecapPlayer): boolean => p.past?.gp === 0;

// "the third pick in the 2025 draft", for his first two seasons.
export const draftTag = (p: RecapPlayer): string | undefined => {
	const d = p.draft;
	if (!d) {
		return undefined;
	}
	// Not "the No. 3 pick": the full stop in "No." ends a sentence to
	// everything that reads the piece back.
	if (d.round === 1) {
		return `the ${d.pick === 1 ? "top" : ordinal(d.pick)} pick in the ${d.year} draft`;
	}
	if (d.round === 2) {
		return `a second-round pick in the ${d.year} draft`;
	}
	return undefined;
};

// Sentences of his that already say one of these things.
const toldOf = (written: string, p: RecapPlayer): string =>
	written
		.split(/(?<=[!.?])\s+/)
		.filter((s) => s.includes(p.name))
		.join(" ");

const CAREER_TOLD = /career|debut|first game|since they traded|since leaving/;

// One note on who he is tonight: a first, a career high (unless his line
// already calls it one), a debut, a new club or an old one. Undefined when
// there is nothing, or it has been said. A man whose line is not on the page
// gets it here - "topped his career high of 15" from a man the piece has
// not given a number is a riddle.
export const careerNote = (
	p: RecapPlayer,
	t: RecapTeam,
	rng: Rng,
	written: string,
): string | undefined => {
	if (CAREER_TOLD.test(toldOf(written, p))) {
		return undefined;
	}
	const n = p.name;
	const lined = new RegExp(
		String.raw`${n}[^.]*?\b${p.pts}\b|\b${p.pts}\b[^.]*?\bfrom ${n}`,
	).test(written);
	const first = firstOf(p);
	if (first === "triple-double" || first === "double-double") {
		return lined
			? `It was ${poss(n)} first career ${first}.`
			: `${n} had ${statPhrase(p)} for his first career ${first}.`;
	}
	if (first === "40-point game" || first === "30-point game") {
		const bar = first === "40-point game" ? 40 : 30;
		return lined
			? pick(
					rng,
					[
						`It was the first ${first} of ${poss(n)} career.`,
						`${n} had never scored ${bar} in a game before.`,
					],
					"firstBigGame",
				)
			: `${n} scored ${p.pts}, the first ${first} of his career.`;
	}
	const high = careerHighOf(p);
	if (high?.key === "pts") {
		return lined
			? pick(
					rng,
					[
						`It was a career high for ${n}, who had never scored more than ${high.prev}.`,
						`${n} topped his career high of ${high.prev}.`,
						`${poss(n)} previous career high was ${high.prev}.`,
					],
					"careerHighPts",
				)
			: `${n} scored a career-high ${p.pts} points.`;
	}
	if (high) {
		return `The ${high.n} ${high.unit} were a career high for ${n}.`;
	}
	return moveNote(p, t, rng, lined);
};

// His debut, his first game for this club, his first against the one he
// left. `lined`: his numbers are already on the page.
export const moveNote = (
	p: RecapPlayer,
	t: RecapTeam,
	rng: Rng,
	lined = true,
): string | undefined => {
	const n = p.name;
	const line = !lined && p.pts > 0 ? statPhrase(p) : undefined;
	if (isDebut(p)) {
		const tag = draftTag(p);
		if (line) {
			return tag
				? `${n}, ${tag}, had ${line} in his debut.`
				: `${n} had ${line} in his debut.`;
		}
		return tag
			? `${n}, ${tag}, was making his debut.`
			: pick(
					rng,
					[
						`It was ${poss(n)} first game in the league.`,
						`${n} was making his debut.`,
					],
					"debut",
				);
	}
	const move = p.newTeam;
	if (move) {
		const club = t.name;
		const had = line ? ` and had ${line}` : "";
		if (move.via === "trade") {
			return `${n} made his ${club} debut after coming over from the ${move.name} in a trade${line ? `, finishing with ${line}` : ""}.`;
		}
		if (move.via === "signing") {
			return `${n} made his ${club} debut after signing as a free agent${line ? `, finishing with ${line}` : ""}.`;
		}
		return `${n} made his ${club} debut${had}.`;
	}
	const former = p.formerTeam;
	if (former) {
		const with_ = line ? `, and had ${line}` : "";
		return former.via === "trade"
			? `${n} was facing the ${former.name} for the first time since they traded him${with_}.`
			: `It was ${poss(n)} first game against the ${former.name} since leaving them${line ? `. He had ${line}` : ""}.`;
	}
	return undefined;
};

// How much a note is worth, for picking among the men of a game: a first
// triple-double, a top pick's debut or a 40-point first over a career-high
// eight assists.
export const careerWeight = (p: RecapPlayer): number => {
	const first = firstOf(p);
	if (first === "triple-double" || first === "40-point game") {
		return 5;
	}
	if (isDebut(p)) {
		return p.draft?.round === 1 && p.draft.pick <= 10 ? 4 : p.pts >= 12 ? 2 : 0;
	}
	const high = careerHighOf(p);
	if (first === "30-point game" || (high?.key === "pts" && high.n >= 25)) {
		return 4;
	}
	if (p.newTeam?.via === "trade" || p.formerTeam?.via === "trade") {
		return 3;
	}
	if (high) {
		return 2;
	}
	if (first === "double-double" || p.newTeam || p.formerTeam) {
		return 1;
	}
	return 0;
};

// ---------------------------------------------------------------- THE CLUB

export type Who = { S: string; one: boolean };

const its = (w: Who) => (w.one ? "its" : "their");
const is = (w: Who) => (w.one ? "is" : "are");
const was = (w: Who) => (w.one ? "was" : "were");

const confShort = (conf: string): string => {
	const m = /^(east|west|north|south)(?:ern)? conference$/i.exec(conf.trim());
	return m ? `${m[1]![0]!.toUpperCase()}${m[1]!.slice(1).toLowerCase()}` : conf;
};

// What the night settled in the standings.
export const raceNote = (
	t: RecapTeam,
	w: Who,
	won: boolean,
): string | undefined => {
	const r = t.race;
	if (!r) {
		return undefined;
	}
	if (r.clinched === "top") {
		const where = r.conf ? `in the ${confShort(r.conf)}` : "in the league";
		return `${w.S} clinched the best record ${where}${won ? "" : " despite the loss"}.`;
	}
	if (r.clinched === "playoffs") {
		return r.firstToClinch
			? `${w.S} became the first team to clinch a playoff spot.`
			: `${w.S} clinched a playoff spot${won ? "" : " despite the loss"}.`;
	}
	if (r.firstTo && won) {
		return `${w.S} became the first team to ${r.firstTo} wins.`;
	}
	if (r.clinched === "playIn") {
		return `${w.S} clinched at least a play-in spot${won ? "" : " despite the loss"}.`;
	}
	if (r.eliminated && !won) {
		return `${w.S} ${was(w)} eliminated from playoff contention.`;
	}
	return undefined;
};

// Where the club is on a road trip or a homestand of four games or more:
// its start, its end, or a clean sweep (or skid) three games in.
export const stretchNote = (t: RecapTeam, w: Who): string | undefined => {
	const s = t.stretch;
	if (!s || s.games < 4) {
		return undefined;
	}
	const kind = `${numWord(s.games)}-game ${s.home ? "homestand" : "trip"}`;
	if (s.game === 1 && s.games >= 5) {
		return `${w.S} opened a ${kind}.`;
	}
	if (s.game === s.games) {
		return `${w.S} finished ${its(w)} ${kind} ${s.won}-${s.lost}.`;
	}
	if (s.game >= 3 && (s.lost === 0 || s.won === 0)) {
		return `${w.S} ${is(w)} ${s.won}-${s.lost} on ${its(w)} ${kind}.`;
	}
	return undefined;
};
