// THE WIRE STORY'S DATELINE AND CLOSING PARTS.
//
// An AP game story opens on where it was played ("DENVER -") and, since
// 2024, closes on four short parts under the story: Takeaways (a line per
// team on what the night means for it), Key moment, Key stat and Up next.
// They are what makes a recap read as a wire story and not a paragraph
// generator, and each is a different cut of the same box score the story is
// written from - so each one leans on a fact the story did not already say
// when it can, and says the decisive one again, briefly, when nothing else
// will do.
//
// Everything here is the game's own numbers. A part with nothing true to say
// is left out rather than filled.

import { weekdayName } from "../../common/calendarDates.ts";
import { clockLeft, periodTopOf } from "../../common/gameFlow.ts";
import {
	analyzeShape,
	clutchShot,
	nameOfPid,
	roundName,
	sideOf,
	type Shape,
} from "./getAutoRecap.ts";
import type {
	RecapGame,
	RecapPlayer,
	RecapTeam,
} from "./getDayGamesForRecap.ts";
import { shotName } from "./recapFinish.ts";
import {
	careerHighOf,
	careerWeight,
	draftTag,
	firstOf,
	isDebut,
	moveNote,
	raceNote,
	stretchNote,
} from "./recapPeople.ts";
import { decisiveRuns, type RunFact } from "./recapStory.ts";
import {
	cap,
	gbText,
	injuryPhrase,
	numWord,
	ordinal,
	pick,
	placeName,
	poss,
	rngFromSeed,
} from "./recapText.ts";

type Rng = () => number;

// ---------------------------------------------------------------- DATELINE

// Where a wire story says a game was played: the city, in capitals, and the
// state for a city the Stylebook does not let stand alone. A club named for
// its state plays in a city the dateline names instead.
const DATELINES: Record<string, string> = {
	"golden state": "SAN FRANCISCO",
	"bay area": "SAN FRANCISCO",
	utah: "SALT LAKE CITY",
	indiana: "INDIANAPOLIS",
	minnesota: "MINNEAPOLIS",
	brooklyn: "NEW YORK",
	la: "LOS ANGELES",
	"new england": "BOSTON",
	"new jersey": "NEWARK, N.J.",
	"tri-cities": "MOLINE, Ill.",
	"st louis": "ST. LOUIS",
	memphis: "MEMPHIS, Tenn.",
	nashville: "NASHVILLE, Tenn.",
	charlotte: "CHARLOTTE, N.C.",
	raleigh: "RALEIGH, N.C.",
	orlando: "ORLANDO, Fla.",
	tampa: "TAMPA, Fla.",
	jacksonville: "JACKSONVILLE, Fla.",
	sacramento: "SACRAMENTO, Calif.",
	"san jose": "SAN JOSE, Calif.",
	anaheim: "ANAHEIM, Calif.",
	portland: "PORTLAND, Ore.",
	"kansas city": "KANSAS CITY, Mo.",
	louisville: "LOUISVILLE, Ky.",
	austin: "AUSTIN, Texas",
	"fort worth": "FORT WORTH, Texas",
	"el paso": "EL PASO, Texas",
	columbus: "COLUMBUS, Ohio",
	buffalo: "BUFFALO, N.Y.",
	rochester: "ROCHESTER, N.Y.",
	syracuse: "SYRACUSE, N.Y.",
	omaha: "OMAHA, Neb.",
	albuquerque: "ALBUQUERQUE, N.M.",
	birmingham: "BIRMINGHAM, Ala.",
	richmond: "RICHMOND, Va.",
	"virginia beach": "VIRGINIA BEACH, Va.",
	hartford: "HARTFORD, Conn.",
	providence: "PROVIDENCE, R.I.",
	tucson: "TUCSON, Ariz.",
	"fort wayne": "FORT WAYNE, Ind.",
	vancouver: "VANCOUVER, British Columbia",
	calgary: "CALGARY, Alberta",
	edmonton: "EDMONTON, Alberta",
	winnipeg: "WINNIPEG, Manitoba",
};

export const datelineOf = (region: string | undefined): string | undefined => {
	const r = (region ?? "").trim();
	const known = DATELINES[r.toLowerCase().replaceAll(".", "")];
	if (known) {
		return known;
	}
	const place = placeName(r);
	return place ? place.toUpperCase() : undefined;
};

// ---------------------------------------------------------------- NAMING

// A team as the subject of a closing line: its city, the wire's way ("Denver
// has won three straight"), or its nickname when the two clubs share a city
// or the region is no city at all.
type Subject = {
	// "Denver", "The Lakers"
	S: string;
	// "Denver", "the Lakers"
	s: string;
	one: boolean;
	// "its", "their"
	its: string;
};

const subjectOf = (t: RecapTeam, other: RecapTeam): Subject => {
	const place = placeName(t.region);
	if (
		place &&
		place.toLowerCase() !== (other.region ?? "").trim().toLowerCase()
	) {
		return { S: place, s: place, one: true, its: "its" };
	}
	return { S: `The ${t.name}`, s: `the ${t.name}`, one: false, its: "their" };
};

// "has"/"have", "leads"/"lead".
const vb = (x: Subject, one: string, many: string) => (x.one ? one : many);

// "Eastern Conference" -> "East", the way a desk shortens it.
const confShort = (conf: string): string => {
	const m = /^(east|west|north|south)(?:ern)? conference$/i.exec(conf.trim());
	return m ? cap(m[1]!.toLowerCase()) : conf;
};

// "an 8-2", "an 11-0", "a 20-1".
const aPair = (a: number, b: number): string =>
	`${String(a).startsWith("8") || a === 11 || a === 18 ? "an" : "a"} ${a}-${b}`;

const aRun = (r: RunFact): string => aPair(r.pts, r.opp);

// "eight of his 21", "all nine of his points" - his total is the story's
// to give; here it is only the share.
const shareOf = (part: number, whole: number): string =>
	part === whole
		? `all ${numWord(whole)} of his points`
		: `${numWord(part)} of his ${numWord(whole)}`;

// ---------------------------------------------------------------- THE BOX SCORE

type Totals = {
	pts: number;
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
	bench: number;
	starters: boolean;
	// When every man's line has them.
	orb?: number;
};

const totalsOf = (t: RecapTeam): Totals => {
	const out: Totals = {
		pts: t.pts,
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
		bench: 0,
		starters: false,
	};
	for (const p of t.players) {
		out.fg += p.fg ?? 0;
		out.fga += p.fga ?? 0;
		out.tp += p.tp ?? 0;
		out.tpa += p.tpa ?? 0;
		out.ft += p.ft ?? 0;
		out.fta += p.fta ?? 0;
		out.reb += p.reb ?? 0;
		out.ast += p.ast ?? 0;
		out.tov += p.tov ?? 0;
		out.stl += p.stl ?? 0;
		out.blk += p.blk ?? 0;
		if (p.starter === true) {
			out.starters = true;
		} else if (p.starter === false) {
			out.bench += p.pts ?? 0;
		}
	}
	if (t.players.length > 0 && t.players.every((p) => p.orb !== undefined)) {
		out.orb = t.players.reduce((sum, p) => sum + (p.orb ?? 0), 0);
	}
	return out;
};

// "48.9%" - one decimal, the way the rest of the recap prints a percentage.
const pctOf = (made: number, att: number): string =>
	`${(Math.round((made / att) * 1000) / 10).toFixed(1)}%`;

// ---------------------------------------------------------------- TOLD

const esc = (s: string) => s.replaceAll(/[$()*+.?[\\\]^{|}]/g, String.raw`\$&`);

const sentencesOf = (text: string): string[] =>
	text.split(/(?<=[!.?])\s+(?=["A-Z])/);

// The sentences of the story about a team: its nickname or its city.
const aboutTeam = (story: string, t: RecapTeam): string =>
	sentencesOf(story)
		.filter(
			(s) =>
				s.includes(t.name) ||
				(placeName(t.region) !== undefined && s.includes(t.region)),
		)
		.join(" ");

// ...and about a man: his whole name or his surname.
const aboutPlayer = (story: string, p: RecapPlayer): string => {
	const surname = p.name.split(" ").at(-1) ?? p.name;
	return sentencesOf(story)
		.filter((s) => s.includes(p.name) || s.includes(surname))
		.join(" ");
};

// Form and streaks, however the story put them.
const FORM_TOLD =
	/in a row|straight|winning streak|losing streak|skid|first win in|-game run|of (?:its|their) (?:last|previous|first)|over their previous|coming in/;

// ---------------------------------------------------------------- TAKEAWAYS

type Line = { text: string; kind: string };

const seriesLines = (
	game: RecapGame,
	t: RecapTeam,
	x: Subject,
	won: boolean,
	opp: RecapTeam,
): Line[] => {
	const s = game.series;
	if (!s) {
		return [];
	}
	const before = (team: RecapTeam) =>
		team.abbrev === s.homeAbbrev ? s.homeWon : s.awayWon;
	const mine = before(t) + (won ? 1 : 0);
	const theirs = before(opp) + (won ? 0 : 1);
	const need = Math.ceil((s.bestOf ?? 7) / 2);
	const finals = s.round === s.numRounds;
	const next = finals ? undefined : roundName(s.round + 1, s.numRounds);
	const line = (text: string): Line[] => [{ kind: "series", text }];
	if (mine === need) {
		return line(
			finals
				? `${x.S} won the championship in ${numWord(mine + theirs)} games.`
				: `${x.S} ${vb(x, "advances", "advance")} to ${next}.`,
		);
	}
	if (theirs === need) {
		return line(`${poss(x.S)} season is over.`);
	}
	if (won) {
		if (mine === need - 1) {
			return line(
				`${x.S} ${vb(x, "is", "are")} one win from ${finals ? "the championship" : next}.`,
			);
		}
		if (mine === theirs) {
			return line(`${x.S} evened the series at ${mine}.`);
		}
		if (mine > theirs) {
			return line(
				`${x.S} ${vb(x, "leads", "lead")} the series ${mine}-${theirs}.`,
			);
		}
		return line(`${x.S} cut ${x.its} series deficit to ${theirs}-${mine}.`);
	}
	if (theirs === need - 1) {
		return line(
			`${x.S} ${vb(x, "faces", "face")} elimination in Game ${mine + theirs + 1}.`,
		);
	}
	if (mine > theirs) {
		return line(
			`${x.S} still ${vb(x, "leads", "lead")} the series ${mine}-${theirs}.`,
		);
	}
	return [];
};

const playInLines = (game: RecapGame, x: Subject, won: boolean): Line[] => {
	const p = game.playIn;
	if (!p) {
		return [];
	}
	const line = (text: string): Line[] => [{ kind: "series", text }];
	if (won) {
		return p.kind === "seed9v10"
			? line(
					`${x.S} ${vb(x, "stays", "stay")} alive in the play-in tournament.`,
				)
			: p.prizeSeed !== undefined
				? line(`${x.S} ${vb(x, "is", "are")} the No. ${p.prizeSeed} seed.`)
				: [];
	}
	return p.kind === "seed7v8"
		? line(
				`${x.S} ${vb(x, "gets", "get")} one more chance in the play-in tournament.`,
			)
		: line(`${poss(x.S)} season is over.`);
};

const streakLines = (
	t: RecapTeam,
	x: Subject,
	won: boolean,
	told: string,
	rng: Rng,
): Line[] => {
	const st = t.streak;
	const sb = t.streakBefore;
	if (FORM_TOLD.test(told)) {
		return [];
	}
	// The other side of a streak that just ended.
	// A losing streak is news from four (a winning one from three).
	if (sb && sb.won !== won && sb.count >= (sb.won ? 3 : 4)) {
		const n = sb.count;
		const a = n === 8 || n === 11 || n === 18 ? "an" : "a";
		return [
			{
				kind: "streak",
				text: won
					? pick(
							rng,
							[
								`${x.S} snapped ${a} ${numWord(n)}-game losing streak.`,
								`${x.S} ended ${a} ${numWord(n)}-game skid.`,
							],
							"wire:snapLosing",
						)
					: `${poss(x.S)} ${numWord(n)}-game winning streak ended.`,
			},
		];
	}
	if (!st || st.won !== won || st.count < (won ? 3 : 4)) {
		return [];
	}
	const n = st.count;
	const has = vb(x, "has", "have");
	return [
		{
			kind: "streak",
			text: won
				? pick(
						rng,
						[
							`${x.S} ${has} won ${numWord(n)} straight.`,
							`${x.S} ${has} won ${numWord(n)} in a row.`,
							`${x.S} ${has} won ${numWord(n)} straight games.`,
						],
						"wire:winStreak",
					)
				: pick(
						rng,
						[
							`${x.S} ${has} lost ${numWord(n)} straight.`,
							`${x.S} ${has} lost ${numWord(n)} in a row.`,
							`${x.S} ${has} dropped ${numWord(n)} straight.`,
						],
						"wire:loseStreak",
					),
		},
	];
};

// Ranks mean nothing until the sample does (as in recapBeats).
const MIN_GAMES_FOR_STANDINGS = 15;

const standingLines = (
	game: RecapGame,
	t: RecapTeam,
	x: Subject,
	won: boolean,
	story: string,
	rng: Rng,
): Line[] => {
	const st = t.standing;
	if (game.playoffs || !st || st.won + st.lost < MIN_GAMES_FOR_STANDINGS) {
		return [];
	}
	const conf = confShort(st.conf);
	// A story that already placed the team in its conference.
	if (new RegExp(esc(st.conf)).test(story)) {
		return [];
	}
	const line = (text: string): Line[] => [{ kind: "standing", text }];
	if (
		won &&
		st.rank === 1 &&
		st.rankBefore !== undefined &&
		st.rankBefore > 1
	) {
		return line(`${x.S} moved into first place in the ${conf}.`);
	}
	if (!won && st.rankBefore === 1 && st.rank > 1) {
		return line(`${x.S} fell out of first place in the ${conf}.`);
	}
	if (st.rank === 1 && st.lead !== undefined && st.lead >= 1) {
		const by = st.lead === 1 ? "a game" : gbText(st.lead);
		return line(
			pick(
				rng,
				[
					`${x.S} ${vb(x, "leads", "lead")} the ${conf} by ${by}.`,
					`${x.S} ${vb(x, "is", "are")} ${by} clear at the top of the ${conf}.`,
				],
				"wire:leads",
			),
		);
	}
	// The worst record in a conference, after a loss, late enough to mean it.
	if (!won && st.rank === st.teams && st.teams >= 5 && st.won + st.lost >= 30) {
		return line(
			pick(
				rng,
				[
					`${x.S} ${vb(x, "has", "have")} the worst record in the ${conf}.`,
					`${x.S} ${vb(x, "is", "are")} last in the ${conf}.`,
					`${x.S} ${vb(x, "sits", "sit")} at the bottom of the ${conf}.`,
				],
				"wire:worst",
			),
		);
	}
	return [];
};

// Recent form, counted with tonight: "has won five of six", "has lost
// seven of its first eight".
const formLines = (
	game: RecapGame,
	t: RecapTeam,
	x: Subject,
	won: boolean,
	told: string,
): Line[] => {
	const last = t.last10;
	if (game.playoffs || !last || last.length < 5 || FORM_TOLD.test(told)) {
		return [];
	}
	// Index 0 is this game.
	const winsIn = (k: number) => last.slice(0, k).filter((g) => g.won).length;
	const record = t.record ? t.record.won + t.record.lost : undefined;
	const has = vb(x, "has", "have");
	for (let k = last.length; k >= 5; k--) {
		const w = winsIn(k);
		const first = record === k;
		const span = first
			? `of ${x.its} first ${numWord(k)}`
			: k === 10
				? `of ${x.its} last 10`
				: `of ${numWord(k)}`;
		if (won && w === k - 1) {
			return [
				{ kind: "form", text: `${x.S} ${has} won ${numWord(w)} ${span}.` },
			];
		}
		if (!won && w === 1) {
			return [
				{ kind: "form", text: `${x.S} ${has} lost ${numWord(k - 1)} ${span}.` },
			];
		}
	}
	return [];
};

// The night's own numbers that say most about one side: a hot or cold
// shooting night, the ball given away.
const nightLines = (
	t: RecapTeam,
	opp: RecapTeam,
	x: Subject,
	won: boolean,
	story: string,
): Line[] => {
	const a = totalsOf(t);
	// A losing side's bad number is a reason only when it was worse than the
	// other side's: 39% shooting against 38% lost nothing.
	const b = totalsOf(opp);
	const pct = (m: number, n: number) => (n > 0 ? m / n : 0);
	const has = (n: number) => new RegExp(String.raw`\b${n}\b`).test(story);
	const line = (text: string): Line[] => [{ kind: "night", text }];
	if (won) {
		if (a.tp >= 17 && a.tp > b.tp && !has(a.tp)) {
			return line(`${x.S} made ${a.tp} 3-pointers.`);
		}
		if (
			a.fga >= 70 &&
			pct(a.fg, a.fga) >= 0.54 &&
			pct(a.fg, a.fga) - pct(b.fg, b.fga) >= 0.05
		) {
			const p = pctOf(a.fg, a.fga);
			if (!story.includes(p)) {
				return line(`${x.S} shot ${p} from the field.`);
			}
		}
		if (a.ast >= 30 && !has(a.ast)) {
			return line(`${x.S} had ${a.ast} assists on ${a.fg} baskets.`);
		}
		if (a.tov <= 8 && b.tov - a.tov >= 5 && !has(a.tov)) {
			return line(
				`${x.S} turned it over just ${numWord(a.tov)} ${a.tov === 1 ? "time" : "times"}.`,
			);
		}
		return [];
	}
	if (
		a.tpa >= 25 &&
		pct(a.tp, a.tpa) < 0.28 &&
		pct(b.tp, b.tpa) - pct(a.tp, a.tpa) >= 0.08 &&
		!has(a.tpa)
	) {
		return line(`${x.S} went ${a.tp} for ${a.tpa} from 3-point range.`);
	}
	if (
		a.fga >= 70 &&
		pct(a.fg, a.fga) < 0.41 &&
		pct(b.fg, b.fga) - pct(a.fg, a.fga) >= 0.05
	) {
		const p = pctOf(a.fg, a.fga);
		if (!story.includes(p)) {
			return line(`${x.S} shot ${p} from the field.`);
		}
	}
	if (a.tov >= 17 && a.tov - b.tov >= 5 && !has(a.tov)) {
		return line(`${x.S} turned it over ${a.tov} times.`);
	}
	if (
		a.fta >= 16 &&
		pct(a.ft, a.fta) < 0.68 &&
		b.ft - a.ft >= 4 &&
		!has(a.fta)
	) {
		return line(`${x.S} went ${a.ft} for ${a.fta} at the free-throw line.`);
	}
	return [];
};

// One man's night against what he brought into it: a streak kept going, a
// count of big games, a season high, a night far over his average. In the
// words the checker holds against his entering line.
const playerLines = (t: RecapTeam, story: string): Line[] => {
	const men = [...t.players]
		.filter((p) => p.min >= 10)
		.sort((a, b) => b.pts - a.pts)
		.slice(0, 3);
	for (const p of men) {
		const e = p.entering;
		if (!e) {
			continue;
		}
		const told = aboutPlayer(story, p);
		const line = (text: string): Line[] => [{ kind: "player", text }];
		const n = p.name;
		const dd =
			[p.pts, p.reb, p.ast, p.stl, p.blk].filter((v) => (v ?? 0) >= 10)
				.length >= 2;
		const streakTold = /straight|in a row|consecutive|held under/.test(told);
		if (!streakTold && p.pts >= 30 && e.streaks.thirty >= 2) {
			return line(
				`${n} has scored 30-plus in ${numWord(e.streaks.thirty + 1)} straight games.`,
			);
		}
		if (!streakTold && p.pts >= 20 && e.streaks.twenty >= 4) {
			return line(
				`${n} has ${numWord(e.streaks.twenty + 1)} straight games of 20 or more points.`,
			);
		}
		if (!streakTold && dd && e.streaks.doubleDouble >= 2) {
			return line(
				`${n} has ${numWord(e.streaks.doubleDouble + 1)} double-doubles in a row.`,
			);
		}
		const counted = /-point games?|games of (?:30|40)|or more \w+ times/.test(
			told,
		);
		if (!counted && e.counts) {
			if (p.pts >= 40 && e.counts.forty >= 1) {
				return line(
					`${n} has ${numWord(e.counts.forty + 1)} 40-point games this season.`,
				);
			}
			if (p.pts >= 30 && e.counts.thirty >= 2) {
				return line(
					`${n} has ${numWord(e.counts.thirty + 1)} 30-point games this season.`,
				);
			}
		}
		if (
			e.gp >= 8 &&
			p.pts > e.high.pts &&
			p.pts >= 20 &&
			!/season high|season-high|career/.test(told)
		) {
			return line(`${n} scored a season-high ${p.pts} points.`);
		}
		const avg = p.seasonAvg;
		if (
			avg &&
			e.gp >= 4 &&
			avg.pts > 0 &&
			p.pts >= avg.pts + 12 &&
			!/averag/.test(told)
		) {
			return line(
				`${n} came in averaging ${Math.round(avg.pts * 10) / 10} points a game.`,
			);
		}
	}
	return [];
};

// Who a man was tonight, past his season: a first, a career high, a debut,
// a new club or an old one - for the men who mattered, at `min` weight and
// up (see careerWeight), and not when the story said it.
const careerLines = (
	t: RecapTeam,
	story: string,
	rng: Rng,
	min: number,
	max = Infinity,
): Line[] => {
	const men = [...t.players]
		.filter((p) => careerWeight(p) >= min && careerWeight(p) <= max)
		.sort((a, b) => careerWeight(b) - careerWeight(a) || b.pts - a.pts);
	for (const p of men) {
		if (
			/career|debut|first game|since they traded|since leaving/.test(
				aboutPlayer(story, p),
			)
		) {
			continue;
		}
		const n = p.name;
		const line = (text: string): Line[] => [{ kind: "career", text }];
		const first = firstOf(p);
		if (first === "triple-double" || first === "double-double") {
			return line(`${n} had his first career ${first}.`);
		}
		if (first === "40-point game" || first === "30-point game") {
			return line(`${n} had his first ${first}.`);
		}
		const high = careerHighOf(p);
		if (high) {
			return line(`${n} set a career high with ${high.n} ${high.unit}.`);
		}
		if (isDebut(p)) {
			const tag = draftTag(p);
			return line(
				tag ? `${n}, ${tag}, made his debut.` : `${n} made his debut.`,
			);
		}
		const move = moveNote(p, t, rng);
		if (move) {
			return line(move);
		}
	}
	return [];
};

// What the night settled in the race, and where the club is on a trip -
// unless the story said it.
const raceLines = (
	t: RecapTeam,
	x: Subject,
	won: boolean,
	story: string,
): Line[] => {
	const text = raceNote(t, x, won);
	const said = raceNote(t, { S: "", one: x.one }, won);
	return text && said && !story.includes(said.trim().replace(/\.$/, ""))
		? [{ kind: "race", text }]
		: [];
};

const tripLines = (t: RecapTeam, x: Subject, story: string): Line[] => {
	const text = stretchNote(t, x);
	const plural = stretchNote(t, { S: "", one: false });
	const single = stretchNote(t, { S: "", one: true });
	const told = [plural, single].some(
		(said) => said && story.includes(said.trim().replace(/\.$/, "")),
	);
	return text && !told ? [{ kind: "split", text }] : [];
};

// A team's own season marks: its most points, its biggest win.
const teamHighLines = (
	game: RecapGame,
	t: RecapTeam,
	x: Subject,
	won: boolean,
	margin: number,
	story: string,
): Line[] => {
	const h = t.seasonHighs;
	if (game.playoffs || !h || h.priorGames < 5) {
		return [];
	}
	if (/season high|this season|all season|of the season/.test(story)) {
		return [];
	}
	if (won && h.margin && margin >= 15) {
		return [
			{
				kind: "high",
				text: `${poss(x.S)} ${margin}-point win was ${x.its} biggest of the season.`,
			},
		];
	}
	if (h.pts) {
		return [
			{
				kind: "high",
				text: `The ${t.pts} points were a season high for ${x.s}.`,
			},
		];
	}
	return [];
};

// The season series, once it says something: who has it, or how it ended.
const seasonSeriesLines = (
	game: RecapGame,
	t: RecapTeam,
	x: Subject,
	won: boolean,
	story: string,
): Line[] => {
	const ss = t.seasonSeries;
	if (game.playoffs || !ss || /season series|meetings?\b/.test(story)) {
		return [];
	}
	const w = ss.won + (won ? 1 : 0);
	const l = ss.lost + (won ? 0 : 1);
	if (w + l < 2) {
		return [];
	}
	const line = (text: string): Line[] => [{ kind: "series", text }];
	if (ss.left === 0) {
		return w === l
			? line(`${x.S} split the season series ${w}-${l}.`)
			: line(`${x.S} ${w > l ? "won" : "lost"} the season series ${w}-${l}.`);
	}
	if (w > l && ss.left !== undefined) {
		return line(
			`${x.S} ${vb(x, "leads", "lead")} the season series ${w}-${l}.`,
		);
	}
	return [];
};

const homeRoadLines = (
	game: RecapGame,
	t: RecapTeam,
	x: Subject,
	won: boolean,
	story: string,
	rng: Rng,
): Line[] => {
	if (game.playoffs || game.neutral) {
		return [];
	}
	const home = game.teams[0].tid === t.tid;
	const r = home ? t.homeRecord : t.awayRecord;
	if (!r || r.won + r.lost < 4) {
		return [];
	}
	const where = home ? "at home" : "on the road";
	if (new RegExp(String.raw`\b${r.won}-${r.lost}\b`).test(story)) {
		return [];
	}
	const line = (text: string): Line[] => [{ kind: "split", text }];
	const is = vb(x, "is", "are");
	if (home && !won && r.lost >= r.won) {
		return line(
			pick(
				rng,
				[
					`${x.S} fell to ${r.won}-${r.lost} ${where}.`,
					`${x.S} dropped to ${r.won}-${r.lost} ${where}.`,
				],
				"wire:homeLoss",
			),
		);
	}
	if (!home && won && r.won > r.lost) {
		return line(
			pick(
				rng,
				[
					`${x.S} ${is} ${r.won}-${r.lost} ${where}.`,
					`${x.S} improved to ${r.won}-${r.lost} ${where}.`,
					`${x.S} ${is} ${r.won}-${r.lost} away from home.`,
				],
				"wire:roadWin",
			),
		);
	}
	if (home && won && r.lost === 0) {
		return line(
			pick(
				rng,
				[
					`${x.S} ${is} ${r.won}-0 ${where}.`,
					`${x.S} ${vb(x, "remains", "remain")} unbeaten at home at ${r.won}-0.`,
				],
				"wire:homeUnbeaten",
			),
		);
	}
	if (!home && !won && r.won === 0) {
		return line(`${x.S} ${is} 0-${r.lost} ${where}.`);
	}
	return [];
};

// A regular who sat this one out, when the story did not say so.
const missingLines = (t: RecapTeam, x: Subject, story: string): Line[] => {
	const out = (t.injuries ?? [])
		.filter((p) => (p.mpg ?? 0) >= 24 && !story.includes(p.name))
		.sort((a, b) => (b.mpg ?? 0) - (a.mpg ?? 0))[0];
	if (!out) {
		return [];
	}
	return [
		{
			kind: "missing",
			text: `${x.S} played without ${out.name}, out with ${injuryPhrase(out.type)}.`,
		},
	];
};

const recordLine = (
	game: RecapGame,
	t: RecapTeam,
	x: Subject,
	won: boolean,
	story: string,
	rng: Rng,
): Line[] => {
	const r = t.record;
	if (!r) {
		return [];
	}
	if (r.won + r.lost === 1) {
		return [
			{
				kind: "record",
				text: `${x.S} opened ${x.its} season with ${won ? "a win" : "a loss"}.`,
			},
		];
	}
	const home = game.teams[0].tid === t.tid;
	const split = home ? t.homeRecord : t.awayRecord;
	const where =
		split && !game.neutral && split.won + split.lost >= 2
			? `${split.won}-${split.lost} ${home ? "at home" : "on the road"}`
			: undefined;
	// The story gave the record already ("the Pandas (5-0)"): only the split
	// is new.
	if (where && story.includes(`(${r.won}-${r.lost})`)) {
		return [
			{
				kind: "record",
				text: pick(
					rng,
					[
						`${x.S} ${vb(x, "is", "are")} ${where}.`,
						`${x.S} ${vb(x, "has", "have")} gone ${where}.`,
						`${x.S} ${vb(x, "is", "are")} ${where} this season.`,
					],
					"wire:split",
				),
			},
		];
	}
	return [
		{
			kind: "record",
			text: `${x.S} ${won ? "improved" : "fell"} to ${r.won}-${r.lost}${
				where ? `, ${where}` : ""
			}.`,
		},
	];
};

const takeawayOf = (
	game: RecapGame,
	t: RecapTeam,
	opp: RecapTeam,
	won: boolean,
	story: string,
	rng: Rng,
): string | undefined => {
	const x = subjectOf(t, opp);
	const told = aboutTeam(story, t);
	const lines: Line[] = [
		...(game.series
			? seriesLines(game, t, x, won, opp)
			: playInLines(game, x, won)),
		...(game.playoffs ? [] : raceLines(t, x, won, story)),
		...(game.playoffs ? [] : careerLines(t, story, rng, 4)),
		...streakLines(t, x, won, told, rng),
		...standingLines(game, t, x, won, story, rng),
		...playerLines(t, story),
		...(game.playoffs ? [] : careerLines(t, story, rng, 2, 3)),
		...formLines(game, t, x, won, told),
		...teamHighLines(game, t, x, won, t.pts - opp.pts, story),
		...nightLines(t, opp, x, won, story),
		...(game.playoffs ? [] : tripLines(t, x, story)),
		...homeRoadLines(game, t, x, won, story, rng),
		...seasonSeriesLines(game, t, x, won, story),
		...missingLines(t, x, story),
	];
	const chosen: Line[] = [];
	for (const line of lines) {
		if (chosen.length >= 2) {
			break;
		}
		if (
			chosen.some(
				(c) =>
					c.kind === line.kind ||
					// A streak and the form around it are one fact.
					(c.kind === "streak" && line.kind === "form"),
			)
		) {
			continue;
		}
		// Two lines when the first is short; one is plenty after a long one.
		if (chosen.length === 1 && chosen[0]!.text.length > 75) {
			break;
		}
		chosen.push(line);
	}
	if (chosen.length === 0) {
		chosen.push(...recordLine(game, t, x, won, story, rng));
	}
	if (chosen.length === 0) {
		return undefined;
	}
	return chosen.map((c) => c.text).join(" ");
};

// ---------------------------------------------------------------- KEY MOMENT

const quarterName = (period: number, regPeriods: number): string =>
	period <= regPeriods
		? `${ordinal(period)} quarter`
		: period === regPeriods + 1
			? "overtime"
			: `${ordinal(period - regPeriods)} overtime`;

const inQuarter = (period: number, regPeriods: number): string =>
	period === regPeriods + 1
		? "in overtime"
		: `in the ${quarterName(period, regPeriods)}`;

// "3:45 left", "3:45 left in overtime".
const leftIn = (
	clock: number,
	period: number,
	regPeriods: number,
): string | undefined => {
	const left = clockLeft(clock);
	return !left
		? undefined
		: period > regPeriods
			? `${left} left ${inQuarter(period, regPeriods)}`
			: `${left} left`;
};

const keyMomentOf = (
	game: RecapGame,
	shape: Shape,
	W: Subject,
	L: Subject,
	story: string,
): string | undefined => {
	const reg = shape.regPeriods;
	const wSide = sideOf(game, shape.winner);
	// A game won at the end: the shot.
	const shot = clutchShot(game);
	if (shot && !shot.tying) {
		// The clock as the score log has it, which the shot's note can round
		// differently.
		const last = game.flow?.lastLead;
		const left =
			!shot.buzzer && last && last.side === wSide
				? leftIn(last.clock, last.period, reg)
				: undefined;
		const what =
			shot.shot === "game-winner"
				? "go-ahead basket"
				: shot.shot === "buzzer-beater"
					? "shot at the buzzer"
					: shot.shot;
		if (shot.buzzer) {
			return `${poss(shot.name)} ${what} won it.`;
		}
		if (left) {
			return `${poss(shot.name)} ${what} with ${left} put ${W.s} ahead for good.`;
		}
	}
	const { closer, breaker } = decisiveRuns(game, shape);
	const credit = (r: RunFact, label: string): string | undefined => {
		const top = r.top;
		if (!top || top.pts < 6) {
			return undefined;
		}
		return `${top.p.name} scored ${shareOf(top.pts, top.p.pts)} during ${poss(W.s)} ${r.pts}-${r.opp} ${label}.`;
	};
	if (closer?.from) {
		const c = credit(closer, "closing run");
		if (c) {
			return c;
		}
		const left = leftIn(closer.from.clock, closer.period, reg);
		if (left) {
			const { w, l } = closer.from;
			// "Of the last" only when nobody scored after it.
			const did = !closer.ended
				? `went on ${aRun(closer)} run`
				: closer.opp === 0
					? `scored the last ${closer.pts} points`
					: `scored ${closer.pts} of the last ${closer.pts + closer.opp} points`;
			return w === l
				? `With the score tied at ${w} and ${left}, ${W.s} ${did}.`
				: w > l
					? `Up ${w}-${l} with ${left}, ${W.s} ${did}.`
					: `Down ${l}-${w} with ${left}, ${W.s} ${did}.`;
		}
	}
	const where = (r: RunFact) =>
		r.period === r.endPeriod
			? inQuarter(r.period, reg)
			: r.endPeriod <= reg
				? `spanning the ${ordinal(r.period)} and ${ordinal(r.endPeriod)} quarters`
				: `spanning the ${quarterName(r.period, reg)} and ${quarterName(r.endPeriod, reg)}`;
	// A run that took a game in the balance and made it a lead: where it
	// stood, the run, where it left it. (Never "in the first quarter turned
	// a 6-4 lead": the quarter word would read as that quarter's score.)
	const ranTo = (r: RunFact): string | undefined => {
		if (!r.from || !r.to) {
			return undefined;
		}
		const { w, l } = r.from;
		const ran = `${W.S} went on ${aRun(r)} run ${where(r)} to lead ${r.to.w}-${r.to.l}.`;
		const lower = ran.replace(/^The /, "the ");
		return w + l <= 10
			? ran
			: w === l
				? `With the score tied at ${w}, ${lower}`
				: w > l
					? `Up ${w}-${l}, ${lower}`
					: `Trailing ${l}-${w}, ${lower}`;
	};
	if (breaker && (breaker.opp > 0 || breaker.pts >= 15)) {
		const c = credit(breaker, "run");
		const ran = ranTo(breaker);
		if (c || ran) {
			return c && (!ran || rngFromSeed(game.gid)() < 0.5) ? c : ran;
		}
	}
	// One man's quarter, when it was the quarter that won it.
	let best: { period: number; pid: number; pts: number } | undefined;
	for (let q = 1; q <= shape.wq.length; q++) {
		const top = periodTopOf(game.flow, q, wSide);
		const d = (shape.wq[q - 1] ?? 0) - (shape.lq[q - 1] ?? 0);
		if (top && top.pts >= 13 && d >= 6 && (!best || top.pts > best.pts)) {
			best = { period: q, pid: top.pid, pts: top.pts };
		}
	}
	if (best) {
		const name = nameOfPid(game, best.pid);
		const p = shape.winner.players.find((x) => x.pid === best.pid);
		const a = shape.wq[best.period - 1]!;
		const b = shape.lq[best.period - 1]!;
		if (name && p) {
			return `${name} scored ${shareOf(best.pts, p.pts)} ${inQuarter(best.period, reg)}, which ${W.s} won ${a}-${b}.`;
		}
	}
	// The basket that put the winners ahead for good, late in a game that
	// stayed close.
	const last = game.flow?.lastLead;
	if (
		last &&
		last.side === wSide &&
		last.period >= reg &&
		shape.margin <= 8 &&
		!/for good/.test(story)
	) {
		const name = nameOfPid(game, last.pid);
		const left = leftIn(last.clock, last.period, reg);
		const e = (game.flow?.finish ?? []).find(
			(x) =>
				x.side === wSide &&
				x.period === last.period &&
				Math.abs(x.clock - last.clock) < 0.15,
		);
		const kind = e
			? shotName(e).replace(/^(?:two|three) /, "")
			: last.by === 1
				? "free throw"
				: last.by === 3
					? "3-pointer"
					: "basket";
		if (name && left) {
			return `${poss(name)} ${kind} with ${left} put ${W.s} ahead for good.`;
		}
	}
	// The winners' biggest run, and the quarter that won it: what the story
	// has not told first.
	const runs = (game.runs?.runs ?? []).filter(
		(r) =>
			r.side === wSide &&
			r.pts - r.opp >= 12 &&
			// "14 straight" is a run worth a line from 15.
			(r.opp > 0 || r.pts >= 15),
	);
	let ranLine: string | undefined;
	let ranTold = false;
	if (runs.length > 0) {
		const r = runs.reduce((a, b) => (b.pts - b.opp > a.pts - a.opp ? b : a));
		const w = wSide === 0 ? 0 : 1;
		ranLine = ranTo({
			mine: true,
			pts: r.pts,
			opp: r.opp,
			period: r.from.period,
			endPeriod: r.to.period,
			from: {
				clock: r.from.clock,
				w: r.from.score[w],
				l: r.from.score[1 - w]!,
			},
			to: { clock: r.to.clock, w: r.to.score[w], l: r.to.score[1 - w]! },
			key: `${r.from.t}:${r.side}`,
		});
		ranTold = story.includes(`${r.pts}-${r.opp} run`);
	}
	let quarterLine: string | undefined;
	let quarterTold = false;
	{
		let q = -1;
		let d = 0;
		for (let i = 0; i < shape.wq.length; i++) {
			const di = (shape.wq[i] ?? 0) - (shape.lq[i] ?? 0);
			if (di > d) {
				d = di;
				q = i;
			}
		}
		if (q >= 0 && d >= 8) {
			const a = shape.wq[q]!;
			const b = shape.lq[q]!;
			quarterTold = story.includes(`${a}-${b}`);
			quarterLine = quarterTold
				? `${W.S} won the ${quarterName(q + 1, reg)} ${a}-${b}.`
				: `${W.S} outscored ${L.s} ${a}-${b} ${inQuarter(q + 1, reg)}.`;
		}
	}
	if (ranLine && !ranTold) {
		return ranLine;
	}
	if (quarterLine && !quarterTold) {
		return quarterLine;
	}
	return ranLine ?? quarterLine;
};

// ---------------------------------------------------------------- KEY STAT

const keyStatOf = (
	shape: Shape,
	W: Subject,
	L: Subject,
	story: string,
	// The parts above it: a fact a takeaway just gave is never the key stat.
	above: string,
	rng: Rng,
): string | undefined => {
	const say = (key: string, texts: string[]) =>
		pick(rng, texts, `wire:keyStat:${key}`);
	const w = totalsOf(shape.winner);
	const l = totalsOf(shape.loser);
	type Cand = { value: number; min: number; text: string; sig: RegExp };
	const cands: Cand[] = [];
	const tov = l.tov - w.tov;
	cands.push({
		value: tov * 1.1,
		min: 6,
		text: say("tov", [
			`${L.S} turned it over ${l.tov} times to ${poss(W.s)} ${w.tov}.`,
			`${L.S} committed ${l.tov} turnovers, ${W.s} ${w.tov}.`,
		]),
		sig: new RegExp(String.raw`\b${l.tov} (?:times|turnovers)`),
	});
	const tp = w.tp - l.tp;
	cands.push({
		value: tp * 1.6,
		min: 9,
		text: say("tp", [
			`${W.S} made ${w.tp} 3-pointers to ${poss(L.s)} ${l.tp}.`,
			`${W.S} hit ${w.tp} 3-pointers, ${L.s} ${l.tp}.`,
		]),
		sig: new RegExp(
			String.raw`\b${w.tp}\b[^.]*\b${l.tp}\b|\b${w.tp} (?:3|three)|\b(?:${w.tp}(?: of | for |-of-)${w.tpa}|${l.tp}(?: of | for |-of-)${l.tpa})\b`,
		),
	});
	const ft = w.ft - l.ft;
	cands.push({
		value: ft,
		min: 8,
		text: say("ft", [
			`${W.S} went ${w.ft} for ${w.fta} at the free-throw line, ${L.s} ${l.ft} for ${l.fta}.`,
			`${W.S} made ${w.ft} of ${w.fta} free throws, ${L.s} ${l.ft} of ${l.fta}.`,
		]),
		sig: new RegExp(
			String.raw`\b(?:${w.ft}(?: of | for |-of-)${w.fta}|${l.ft}(?: of | for |-of-)${l.fta})\b`,
		),
	});
	const reb = w.reb - l.reb;
	cands.push({
		value: reb * 0.8,
		min: 8,
		text: say("reb", [
			`${W.S} outrebounded ${L.s} ${w.reb}-${l.reb}.`,
			`${W.S} won the rebounding battle ${w.reb}-${l.reb}.`,
		]),
		sig: new RegExp(String.raw`\b${w.reb}-${l.reb}\b`),
	});
	if (w.fga > 0 && l.fga > 0) {
		const dp = w.fg / w.fga - l.fg / l.fga;
		const wp = pctOf(w.fg, w.fga);
		const lp = pctOf(l.fg, l.fga);
		cands.push({
			value: dp * 100 * 1.1,
			min: 8,
			text: `${W.S} shot ${wp} from the field, ${L.s} ${lp}.`,
			sig: new RegExp(`${esc(wp)}|${esc(lp)}`),
		});
	}
	if (w.orb !== undefined && l.orb !== undefined) {
		cands.push({
			value: (w.orb - l.orb) * 1.1,
			min: 8,
			text: `${W.S} had ${w.orb} offensive rebounds to ${poss(L.s)} ${l.orb}.`,
			sig: new RegExp(String.raw`\b${w.orb} offensive`),
		});
	}
	if (w.starters && l.starters) {
		const bench = w.bench - l.bench;
		cands.push({
			value: bench * 0.5,
			min: 9,
			text: say("bench", [
				`${cap(poss(W.s))} bench outscored ${poss(L.s)} ${w.bench}-${l.bench}.`,
				`${cap(poss(W.s))} reserves outscored ${poss(L.s)} ${w.bench}-${l.bench}.`,
			]),
			sig: new RegExp(String.raw`\b${w.bench}-${l.bench}\b`),
		});
	}
	const open = cands.filter((c) => !c.sig.test(above));
	const ok = open
		.filter((c) => c.value >= c.min)
		.sort((a, b) => b.value - a.value);
	const fresh = ok.find((c) => !c.sig.test(story));
	if (fresh ?? ok[0]) {
		return (fresh ?? ok[0])!.text;
	}
	// Nothing decisive: the edge the winners did have, if any was real.
	const small = open
		.filter((c) => c.value >= c.min / 2)
		.sort((a, b) => b.value - a.value);
	return (small.find((c) => !c.sig.test(story)) ?? small[0])?.text;
};

// ---------------------------------------------------------------- UP NEXT

const upNextOf = (game: RecapGame, shape: Shape): string[] => {
	const on = (date: string | undefined) =>
		date ? ` on ${weekdayName(date)}` : "";
	const w = shape.winner.nextGame;
	const l = shape.loser.nextGame;
	// The same two again: one line.
	if (
		w &&
		l &&
		w.day === l.day &&
		w.oppTid === shape.loser.tid &&
		l.oppTid === shape.winner.tid
	) {
		const host = w.home ? shape.winner : shape.loser;
		const city = placeName(host.region);
		const at = city ? ` in ${city}` : `, at ${poss(`the ${host.name}`)} place`;
		if (game.series) {
			const s = game.series;
			return [`Game ${s.homeWon + s.awayWon + 2} is${on(w.date)}${at}.`];
		}
		return [`The teams meet again${on(w.date)}${at}.`];
	}
	const line = (
		t: RecapTeam,
		n: NonNullable<RecapTeam["nextGame"]>,
	): string | undefined => {
		if (!n.oppName) {
			return undefined;
		}
		const opp =
			n.oppRegion && placeName(n.oppRegion)
				? `the ${n.oppRegion} ${n.oppName}`
				: `the ${n.oppName}`;
		return `${t.name}: ${n.home ? "Host" : "Visit"} ${opp}${on(n.date)}.`;
	};
	return [
		...(w ? [line(shape.winner, w)] : []),
		...(l ? [line(shape.loser, l)] : []),
	].filter((x): x is string => x !== undefined);
};

// ---------------------------------------------------------------- ASSEMBLY

export type Wire = {
	dateline?: string;
	// The closing parts, as Markdown: each a heading and its lines.
	closing: string;
};

export const wireOf = (game: RecapGame, story: string): Wire | undefined => {
	if (game.allStar || game.teams.length !== 2) {
		return undefined;
	}
	const shape = analyzeShape(game);
	if (shape.margin === 0) {
		return undefined;
	}
	const rng = rngFromSeed((game.gid + 1) * 40503 + 7);
	const W = subjectOf(shape.winner, shape.loser);
	const L = subjectOf(shape.loser, shape.winner);
	const parts: string[] = [];
	const takeaways = (
		[
			[shape.winner, shape.loser, true],
			[shape.loser, shape.winner, false],
		] as const
	)
		.map(([t, opp, won]) => {
			const text = takeawayOf(game, t, opp, won, story, rng);
			return text ? `${t.name}: ${text}` : undefined;
		})
		.filter((x): x is string => x !== undefined);
	if (takeaways.length > 0) {
		parts.push(["### Takeaways", ...takeaways].join("\n\n"));
	}
	const moment = keyMomentOf(game, shape, W, L, story);
	if (moment) {
		parts.push(`### Key moment\n\n${moment}`);
	}
	const stat = keyStatOf(shape, W, L, story, parts.join("\n\n"), rng);
	if (stat) {
		parts.push(`### Key stat\n\n${stat}`);
	}
	const next = upNextOf(game, shape);
	if (next.length > 0) {
		parts.push(["### Up next", ...next].join("\n\n"));
	}
	return {
		dateline: game.neutral ? undefined : datelineOf(game.teams[0].region),
		closing: parts.join("\n\n"),
	};
};

// The story with its dateline and closing parts: "DENVER - " before the
// first paragraph under the headline, the parts after the last.
export const withWire = (text: string, wire: Wire): string => {
	const cut = text.indexOf("\n\n");
	let out = text;
	if (wire.dateline && cut >= 0) {
		out = `${text.slice(0, cut + 2)}${wire.dateline} — ${text.slice(cut + 2)}`;
	}
	return wire.closing === "" ? out : `${out}\n\n${wire.closing}`;
};

// The story above the closing parts, for anything that reads only the story.
export const storyOf = (recap: string): string => {
	const at = recap.indexOf("\n\n### ");
	return at < 0 ? recap : recap.slice(0, at);
};
