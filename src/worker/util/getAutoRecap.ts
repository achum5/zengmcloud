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

const pick = <T>(rng: () => number, arr: T[]): T =>
	arr[Math.floor(rng() * arr.length)]!;

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

const bestOf = (players: RecapPlayer[]): RecapPlayer | undefined => {
	let best: RecapPlayer | undefined;
	let bestScore = -Infinity;
	for (const p of players) {
		const s = impact(p);
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
	const extras: [number, string][] = [];
	if (p.reb >= 8 || dd.has("rebounds")) {
		extras.push([p.reb, plural(p.reb, "rebound")]);
	}
	if (p.ast >= 6 || dd.has("assists")) {
		extras.push([p.ast, plural(p.ast, "assist")]);
	}
	if (p.stl >= 4 || dd.has("steals")) {
		extras.push([p.stl, plural(p.stl, "steal")]);
	}
	if (p.blk >= 4 || dd.has("blocks")) {
		extras.push([p.blk, plural(p.blk, "block")]);
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

// A verb for how a player scored, scaled to how big the night was and varied by
// seed so the same word doesn't lead every recap.
const scoredVerb = (p: RecapPlayer, rng: () => number): string => {
	let pool: string[];
	if (p.pts >= 35) {
		pool = ["poured in", "erupted for", "exploded for", "piled up"];
	} else if (p.pts >= 25) {
		pool = ["scored", "posted", "put up", "finished with", "racked up"];
	} else {
		pool = ["finished with", "chipped in", "contributed", "added"];
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
// player and shot type so it can lead a headline.
const clutchShot = (
	game: RecapGame,
): { name: string; shot: string; tying: boolean } | undefined => {
	for (const raw of game.clutchPlays) {
		const text = stripHtml(raw);
		const m =
			/^(.+?) made a (game-winning|game-tying) ([ a-z-]+?)(?: with| at| to|\.|$)/.exec(
				text,
			);
		if (m) {
			return {
				name: m[1]!.trim(),
				shot: m[3]!.trim(),
				tying: m[2] === "game-tying",
			};
		}
	}
	return undefined;
};

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
				)} alive and sent them to the final play-in game; ${theNick(
					shape.loser,
				)}' season is over.`,
			);
		} else {
			out.headlineTag = "a win-or-go-home game";
			out.sentences.push(
				`${cap(w)} grabbed the last playoff berth${
					typeof p.prizeSeed === "number" ? ` as the #${p.prizeSeed} seed` : ""
				}, ending ${theNick(shape.loser)}' season in the final play-in game.`,
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
	const gameLabel = `Game ${gameNo} of ${rnd}`;
	if (wAfter === lBefore) {
		out.sentences.push(
			`${cap(w)} evened ${rnd} at ${wAfter}-${wAfter} with the ${gameLabel} win.`,
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
	return ["beat", "down", "top", "take down", "get past"];
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

const starHeadline = (p: RecapPlayer): string => {
	const dd = doubleWord(doubleCategories(p).length);
	if (dd === "triple-double" || dd === "quadruple-double") {
		return dd;
	}
	if (p.pts >= 38) {
		return `${p.pts}`;
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
		return `${p.pts} points and ${otherVal} ${other}`;
	}
	return `${p.pts}`;
};

const buildHeadline = (
	game: RecapGame,
	shape: Shape,
	star: RecapPlayer,
	post: PostseasonContext,
	rng: () => number,
): string => {
	const winnerN = nick(shape.winner);
	const loserN = nick(shape.loser);
	const shot = clutchShot(game);
	const tag = post.headlineTag ? ` in ${post.headlineTag}` : "";

	if (shot && !shot.tying) {
		return pick(rng, [
			`${shot.name}'s ${shot.shot} sinks the ${loserN}${tag}`,
			`${shot.name}'s ${shot.shot} lifts the ${winnerN} past the ${loserN}${tag}`,
			`${shot.name} beats the ${loserN} with a ${shot.shot}${tag}`,
		]);
	}

	const verb = pick(rng, verbPool(game, shape));
	const ddCount = doubleCategories(star).length;

	if (ddCount >= 3) {
		const word = doubleWord(ddCount)!;
		return pick(rng, [
			`${star.name}'s ${word} carries the ${winnerN} past the ${loserN}${tag}`,
			`${star.name} posts a ${word} as the ${winnerN} ${verb} the ${loserN}${tag}`,
		]);
	}

	if (isUpset(game, shape)) {
		return `${winnerN} ${verb} the ${loserN}, ${scoreTag(shape)}${tag}`;
	}

	if (shape.comebackFrom >= 12) {
		return pick(rng, [
			`${winnerN} erase a ${shape.comebackFrom}-point hole to ${verb} the ${loserN}${tag}`,
			`${star.name} rallies the ${winnerN} past the ${loserN}${tag}`,
		]);
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
		return pick(rng, [
			`${star.name} outduels ${loserStar.name} as the ${winnerN} ${verb} the ${loserN}${tag}`,
			`${star.name}'s ${star.pts} edges ${loserStar.name}'s ${loserStar.pts} in the ${winnerN}' win${tag}`,
		]);
	}

	if (star.pts >= 40) {
		return pick(rng, [
			`${star.name} drops ${star.pts} as the ${winnerN} ${verb} the ${loserN}${tag}`,
			`${star.name}'s ${star.pts} sink the ${loserN}${tag}`,
			`${star.name} pours in ${star.pts} in the ${winnerN}' win${tag}`,
		]);
	}

	// Two double-doubles from the winner, when no single scorer dominates.
	const winnerDoubles = shape.winner.players
		.filter((p) => doubleCategories(p).length >= 2 && p.pts >= 12)
		.sort((a, b) => impact(b) - impact(a));
	if (winnerDoubles.length >= 2 && star.pts < 30) {
		const [a, b] = winnerDoubles;
		return pick(rng, [
			`${a!.name} and ${b!.name} lead the ${winnerN} past the ${loserN}${tag}`,
			`Double-doubles from ${a!.name} and ${b!.name} carry the ${winnerN} past the ${loserN}${tag}`,
		]);
	}

	// A defensive showcase.
	if (star.blk >= 5) {
		return `${star.name}'s ${star.blk} blocks anchor the ${winnerN} past the ${loserN}${tag}`;
	}
	if (star.stl >= 5 && star.pts >= 15) {
		return `${star.name} takes over with ${star.pts} and ${star.stl} steals as the ${winnerN} ${verb} the ${loserN}${tag}`;
	}

	const starTemplates = [
		`${star.name}'s ${starHeadline(star)} ${
			star.pts >= 25 ? "powers" : "leads"
		} the ${winnerN} past the ${loserN}${tag}`,
		`${star.name} scores ${star.pts} as the ${winnerN} ${verb} the ${loserN}${tag}`,
		`${winnerN} ${verb} the ${loserN} behind ${star.name}'s ${starHeadline(
			star,
		)}${tag}`,
		`${star.name} leads the ${winnerN} over the ${loserN}${tag}`,
	];
	const resultTemplates = [
		`${winnerN} ${verb} the ${loserN}, ${scoreTag(shape)}${tag}`,
		`${winnerN} ${verb} the ${loserN} ${scoreTag(shape)}${tag}`,
	];
	const useResult = shape.margin >= 18 && rng() < 0.5;
	return pick(rng, useResult ? resultTemplates : starTemplates);
};

// --- Body sentence builders ----------------------------------------------------

// The lead: the result carried by the winner's best player, with the line he
// brought in when it makes the night pop.
const leadSentence = (
	game: RecapGame,
	shape: Shape,
	star: RecapPlayer,
	rng: () => number,
): string => {
	const verb = pastTense(pick(rng, verbPool(game, shape)));
	const flourish = star.pts >= 25 ? shootingFlourish(star) : undefined;
	const line = enteringLine(star, game.playoffs);

	let subject = star.name;
	// Occasionally frame the star against the average he came in with.
	if (line && star.pts >= line.pts + 12 && star.pts >= 24 && rng() < 0.6) {
		subject = `${star.name}, who came in averaging ${line.pts} points a game,`;
	}

	const statText = statPhrase(star);
	const flourishText = flourish ? ` ${flourish}` : "";
	return `${subject} ${scoredVerb(star, rng)} ${statText}${flourishText} as ${theNick(
		shape.winner,
	)} ${verb} ${theNick(shape.loser)} ${scoreTag(shape)}.`;
};

// How the game unfolded, from the quarter-by-quarter scoring.
const flowSentence = (shape: Shape, rng: () => number): string | undefined => {
	if (shape.comebackFrom >= 12 && shape.comebackPeriod > 0) {
		return pick(rng, [
			`The ${nick(shape.winner)} had trailed by ${
				shape.comebackFrom
			} after the ${ordinal(shape.comebackPeriod)} before storming back.`,
			`It was a ${shape.comebackFrom}-point comeback, with ${theNick(
				shape.winner,
			)} down that much after the ${ordinal(shape.comebackPeriod)}.`,
		]);
	}
	if (shape.wireToWire && shape.margin >= 8) {
		return `${cap(theNick(shape.winner))} led wire to wire.`;
	}
	if (
		shape.bigRun &&
		shape.bigRun.margin >= 9 &&
		shape.margin >= 8 &&
		shape.regPeriods >= 3
	) {
		return `A ${shape.bigRun.wpts}-${shape.bigRun.lpts} ${ordinal(
			shape.bigRun.period,
		)} quarter broke it open.`;
	}
	if (shape.ot > 0) {
		return `Neither side could pull away in regulation, and it took ${
			shape.ot === 1 ? "an extra period" : `${shape.ot} extra periods`
		} to settle it.`;
	}
	if (
		Math.abs(shape.marginEnteringLast) <= 4 &&
		shape.margin <= 10 &&
		shape.regPeriods >= 4
	) {
		const m = shape.marginEnteringLast;
		if (m === 0) {
			return `The game was tied entering the ${ordinal(shape.regPeriods)}.`;
		}
		const leaderIsWinner = m > 0;
		return `${cap(theNick(leaderIsWinner ? shape.winner : shape.loser))} led by ${Math.abs(
			m,
		)} entering the ${ordinal(shape.regPeriods)}.`;
	}
	return undefined;
};

// A team-level statistical note: shooting, rebounding, turnovers, or balance.
const statNote = (shape: Shape, rng: () => number): string | undefined => {
	const w = teamStats(shape.winner);
	const l = teamStats(shape.loser);
	const options: string[] = [];

	if (w.fga >= 20 && w.fgp >= 52) {
		options.push(
			`${cap(theNick(shape.winner))} shot ${w.fgp}% from the field.`,
		);
	}
	if (w.tp >= 14) {
		options.push(`${cap(theNick(shape.winner))} knocked down ${w.tp} threes.`);
	}
	// Realistic team totals only (a full box score), so partial data can't yield
	// an absurd "won the glass 22-0".
	if (w.reb >= 30 && l.reb >= 20 && w.reb - l.reb >= 10) {
		options.push(
			`${cap(theNick(shape.winner))} won the glass ${w.reb}-${l.reb}.`,
		);
	}
	if (w.dblFig >= 5) {
		options.push(
			`${cap(theNick(shape.winner))} had ${w.dblFig} players score in double figures.`,
		);
	}
	if (w.stl >= 10 && l.tov >= 16) {
		options.push(
			`${cap(theNick(shape.winner))} forced ${l.tov} turnovers and turned them into points.`,
		);
	}
	if (w.ast >= 28) {
		options.push(
			`${cap(theNick(shape.winner))} piled up ${w.ast} assists on the night.`,
		);
	}
	// A hot start: the winner's first-quarter margin.
	if (
		shape.bigRun &&
		shape.bigRun.period === 1 &&
		shape.bigRun.margin >= 10 &&
		shape.wq.length > 0
	) {
		options.push(
			`${cap(theNick(shape.winner))} jumped out to a ${shape.bigRun.wpts}-${shape.bigRun.lpts} first quarter.`,
		);
	}
	// A big edge at the free-throw line.
	if (w.ft >= 24 && w.ft - l.ft >= 10) {
		options.push(
			`${cap(theNick(shape.winner))} made ${w.ft} free throws to ${l.ft} for ${theNick(
				shape.loser,
			)}.`,
		);
	}
	if (options.length === 0) {
		return undefined;
	}
	return pick(rng, options);
};

// The halftime / second-half story, from the quarter scores.
const secondHalfNote = (shape: Shape): string | undefined => {
	const { wq, lq, regPeriods } = shape;
	if (regPeriods < 4 || wq.length < regPeriods || lq.length < regPeriods) {
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
	// lead with a bigger comeback).
	if (halfMargin < 0 && shape.margin > 0 && shape.comebackFrom < 12) {
		return `Down ${-halfMargin} at the break, ${theNick(
			shape.winner,
		)} outscored ${theNick(shape.loser)} ${wSecond}-${lSecond} in the second half.`;
	}
	if (secondMargin >= 12) {
		return `${cap(theNick(shape.winner))} pulled away after halftime, taking the second half ${wSecond}-${lSecond}.`;
	}
	if (halfMargin >= 15) {
		return `${cap(theNick(shape.winner))} led ${wFirst}-${lFirst} at halftime and never looked back.`;
	}
	return undefined;
};

// A player who controlled the game by plus-minus (when it's tracked and big).
const plusMinusNote = (shape: Shape, star: RecapPlayer): string | undefined => {
	let best: RecapPlayer | undefined;
	for (const p of shape.winner.players) {
		if (typeof p.pm === "number" && (!best || p.pm > (best.pm ?? -Infinity))) {
			best = p;
		}
	}
	if (!best || best === star || (best.pm ?? 0) < 18) {
		return undefined;
	}
	return `${best.name} was a game-best +${best.pm} in ${best.min} minutes.`;
};

// The scoreboard's overall character: a shootout or a defensive grind.
const combinedNote = (shape: Shape): string | undefined => {
	const total = shape.winner.pts + shape.loser.pts;
	if (shape.ot === 0 && shape.regPeriods >= 4) {
		if (total >= 240) {
			return `The teams combined for ${total} points in an up-and-down affair.`;
		}
		if (total <= 165) {
			return `Neither offense got going in a ${total}-point defensive grind.`;
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
		: `${second.name} ${pick(rng, ["added", "chipped in", "backed him with"])} ${statPhrase(
				second,
			)}`;

	const third = cast[1];
	if (third && (third.pts >= 14 || doubleCategories(third).length >= 2)) {
		return `${secondText}, and ${third.name} had ${statPhrase(third, 1)}.`;
	}
	return `${secondText}.`;
};

// The losing side: their leader, and (when there's a clear culprit) why it wasn't
// enough.
const loserSentence = (shape: Shape, rng: () => number): string | undefined => {
	const leader = bestOf(shape.loser.players);
	if (!leader) {
		return undefined;
	}
	const stats = teamStats(shape.loser);
	let reason = "";
	if (stats.tov >= 18) {
		reason = `, but ${stats.tov} turnovers doomed ${theNick(shape.loser)}`;
	} else if (stats.fga >= 20 && stats.fgp <= 40) {
		reason = `, but ${theNick(shape.loser)} shot just ${stats.fgp}% as a team`;
	}

	if (leader.pts >= 18 || doubleCategories(leader).length >= 2) {
		const verb = pick(rng, ["led", "paced", "topped"]);
		const ddw = doubleWord(doubleCategories(leader).length);
		const leaderLine = ddw
			? `${leader.name}'s ${ddw} (${statPhrase(leader)})`
			: `${leader.name}'s ${statPhrase(leader)}`;
		return `${leaderLine} ${verb} ${theNick(shape.loser)}${reason}.`;
	}
	if (reason) {
		return `${cap(reason.replace(/^, but /, ""))}.`;
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
			`The win was ${theNick(shape.winner)}' ${ordinal(
				streak.count,
			)} in a row.`,
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
				`It snapped ${theNick(shape.loser)}' ${run}-game winning streak.`,
			);
		}
	}

	if (isUpset(game, shape) && game.spread) {
		options.push(
			`${cap(theNick(shape.winner))} entered ${game.spread.points}-point underdogs.`,
		);
	}

	const rec = shape.winner.record;
	if (rec && rec.won + rec.lost >= 10 && !game.playoffs) {
		options.push(
			`${cap(theNick(shape.winner))} improved to ${rec.won}-${rec.lost}.`,
		);
	}

	if (options.length === 0) {
		return undefined;
	}
	return pick(rng, options);
};

// Injury color: returns, playing through, new injuries, and notable inactives.
const injurySentence = (shape: Shape): string | undefined => {
	const bits: string[] = [];
	for (const t of [shape.winner, shape.loser]) {
		for (const p of t.players) {
			if (p.injury?.playingThrough && p.pts >= 18) {
				bits.push(`${p.name} played through a ${p.injury.type} for ${p.pts}`);
			} else if (p.injury?.newThisGame && p.injury.gamesRemaining > 0) {
				bits.push(
					`${p.name} left with a ${p.injury.type} (out ~${p.injury.gamesRemaining})`,
				);
			}
		}
	}
	// A key player held out entirely.
	for (const t of [shape.winner, shape.loser]) {
		for (const out of t.injuries ?? []) {
			bits.push(`${theNick(t)} were without ${out.name} (${out.type})`);
			break; // one per team is enough
		}
	}
	if (bits.length === 0) {
		return undefined;
	}
	return `${cap(bits.slice(0, 2).join("; "))}.`;
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
	const rng = rngFromSeed((game.gid + 1) * 2654435761);

	if (game.allStar) {
		return buildAllStar(game, rng);
	}

	const shape = analyzeShape(game);
	const star = bestOf(shape.winner.players) ?? bestOf(shape.loser.players);

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

	// Paragraph 1: the result and how it happened.
	const para1: string[] = [leadSentence(game, shape, star, rng)];
	const flow = flowSentence(shape, rng);
	if (flow) {
		para1.push(flow);
	}
	const stat = statNote(shape, rng);
	if (stat && para1.length < 3) {
		para1.push(stat);
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
	const loser = loserSentence(shape, rng);
	if (loser) {
		para2.push(loser);
	}
	// Fill out with the remaining angles, seed-ordered for variety.
	const extras = shuffle(rng, [
		post.sentences[1],
		secondHalfNote(shape),
		stakesSentence(game, shape, rng),
		combinedNote(shape),
		plusMinusNote(shape, star),
		injurySentence(shape),
	]).filter((s): s is string => !!s);
	for (const e of extras) {
		if (para2.length >= 5) {
			break;
		}
		para2.push(e);
	}

	const paragraphs = [para1.join(" ")];
	if (para2.length > 0) {
		paragraphs.push(para2.join(" "));
	}
	return `**${headline}**\n\n${paragraphs.join("\n\n")}`;
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
				out.push({ p, team, opp, won: team.tid === game.winnerTid });
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
		return `${base} on ${shot.name}'s ${shot.shot}`;
	}
	if (star) {
		return `${base} behind ${star.name}'s ${statPhrase(star, 1)}`;
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
		const who = `${leader.region} ${leader.name} (${leader.won}-${leader.lost})`;
		if (second && second.gb >= 1) {
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
		if (s && s.won && s.count >= 6 && (!best || s.count > best.count)) {
			best = { team: winner, count: s.count };
		}
	}
	if (!best) {
		return undefined;
	}
	return `${cap(theNick(best.team))} ran their win streak to ${best.count} games.`;
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
	const w = nick(mShape.winner);
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
			return `${shot.name}'s ${shot.shot} decides a playoff thriller`;
		}
		if (mStar) {
			return pick(rng, [
				`${mStar.name}'s ${starHeadline(mStar)} powers ${w} past ${l}`,
				`${w} take command against ${l}`,
			]);
		}
		return `${w} ${pick(rng, verbPool(marquee, mShape))} ${l}`;
	}

	// A walk-off is the day's story.
	const shot = clutchShot(marquee);
	if (shot && !shot.tying) {
		return pick(rng, [
			`${shot.name} beats the buzzer to sink ${l}`,
			`${shot.name}'s ${shot.shot} stuns ${l}`,
			`${shot.name} walks it off against ${l}`,
		]);
	}

	// Two or more 40-point nights across the league.
	const fortyClub = performers.filter((perf) => perf.p.pts >= 40);
	if (fortyClub.length >= 2) {
		const [a, b] = fortyClub;
		return `${a!.p.name}'s ${a!.p.pts} and ${b!.p.name}'s ${b!.p.pts} light up the night`;
	}

	if (mStar && mStar.pts >= 45) {
		return pick(rng, [
			`${mStar.name} erupts for ${mStar.pts} to lead ${w} past ${l}`,
			`${mStar.name} drops ${mStar.pts} in ${poss(w)} win`,
		]);
	}

	if (mStar && doubleCategories(mStar).length >= 3) {
		return pick(rng, [
			`${mStar.name} triple-doubles to lead ${w} past ${l}`,
			`${mStar.name}'s triple-double carries ${w} over ${l}`,
		]);
	}

	if (isUpset(marquee, mShape)) {
		return pick(rng, [
			`${w} stun ${l}`,
			`${w} pull off the upset over ${l}`,
			`${w} shock ${l}`,
		]);
	}

	if (mShape.ot > 0) {
		return pick(rng, [
			`${w} outlast ${l} in overtime`,
			`${w} survive ${l} in ${mShape.ot === 1 ? "OT" : `${mShape.ot} OTs`}`,
		]);
	}

	if (mShape.comebackFrom >= 15) {
		return `${w} storm back to beat ${l}`;
	}

	if (mShape.margin >= 25) {
		return pick(rng, [
			`${w} rout ${l} by ${mShape.margin}`,
			`${w} blow out ${l}`,
		]);
	}

	if (mShape.margin <= 3) {
		return pick(rng, [
			`${w} edge ${l} in a ${scoreTag(mShape)} thriller`,
			`${w} hold off ${l} at the wire`,
		]);
	}

	if (mStar) {
		return pick(rng, [
			`${mStar.name}'s ${starHeadline(mStar)} leads ${w} past ${l}`,
			`${w} ${pick(rng, verbPool(marquee, mShape))} ${l} behind ${mStar.name}`,
		]);
	}
	return `${w} ${pick(rng, verbPool(marquee, mShape))} ${l}`;
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
	if (ot >= 2) {
		return `${ot === nonExhibition.length ? "All" : ot} games went to overtime.`;
	}
	if (close >= 3) {
		return `${close} of the ${nonExhibition.length} games were decided by five points or fewer.`;
	}
	return undefined;
};

export const getAutoDayRecap = (input: AutoDayRecapInput): string => {
	const { games, standings, day, playoffs } = input;
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

	// Paragraph 1: the marquee game and the day's best individual nights.
	const para1: string[] = [];
	para1.push(`${cap(gameBlurb(marquee, rng))}.`);
	if (topScorer && topScorer.p.pts >= 30) {
		para1.push(
			topScorer.won
				? `${topScorer.p.name} led all scorers with ${statPhrase(
						topScorer.p,
					)} in ${poss(theNick(topScorer.team))} win over ${theNick(
						topScorer.opp,
					)}.`
				: `${topScorer.p.name} led all scorers with ${statPhrase(
						topScorer.p,
					)} despite ${poss(theNick(topScorer.team))} loss to ${theNick(
						topScorer.opp,
					)}.`,
		);
	}
	// A second standout from a different game.
	const secondPerf = performers.find(
		(perf) =>
			perf.team.tid !== topScorer?.team.tid &&
			perf.p !== topScorer?.p &&
			(perf.p.pts >= 25 || doubleCategories(perf.p).length >= 3),
	);
	if (secondPerf) {
		const ddw = doubleWord(doubleCategories(secondPerf.p).length);
		para1.push(
			ddw && doubleCategories(secondPerf.p).length >= 3
				? `${secondPerf.p.name} posted a ${ddw} (${statPhrase(
						secondPerf.p,
					)}) for ${theNick(secondPerf.team)}.`
				: `${secondPerf.p.name} added ${statPhrase(secondPerf.p)} for ${theNick(
						secondPerf.team,
					)}.`,
		);
	}

	// A league-wide triple-double gets a nod if it wasn't already the story.
	const mentioned = new Set([topScorer?.p, secondPerf?.p, mStar]);
	const tdPerf = performers.find(
		(perf) => doubleCategories(perf.p).length >= 3 && !mentioned.has(perf.p),
	);
	if (tdPerf) {
		para1.push(
			`${tdPerf.p.name} put together a triple-double (${statPhrase(
				tdPerf.p,
			)}) for ${theNick(tdPerf.team)}.`,
		);
	}

	// Paragraph 2: other notable results, then the league picture.
	const para2: string[] = [];
	const others = ranked.slice(1);
	const notableBlurbs: string[] = [];
	for (const g of others) {
		const shape = analyzeShape(g);
		if (isUpset(g, shape)) {
			notableBlurbs.push(
				`${theNick(shape.winner)} upset ${theNick(shape.loser)}`,
			);
		} else if (shape.ot > 0) {
			notableBlurbs.push(
				`${theNick(shape.winner)} outlasted ${theNick(shape.loser)} in overtime`,
			);
		} else if (shape.margin >= 25) {
			notableBlurbs.push(
				`${theNick(shape.winner)} routed ${theNick(shape.loser)} by ${shape.margin}`,
			);
		} else if (clutchShot(g) && !clutchShot(g)!.tying) {
			notableBlurbs.push(
				`${clutchShot(g)!.name} beat ${theNick(shape.loser)} at the wire`,
			);
		}
		if (notableBlurbs.length >= 3) {
			break;
		}
	}
	if (notableBlurbs.length > 0) {
		para2.push(`Elsewhere, ${naturalList(notableBlurbs)}.`);
	}

	const close = closeGamesSentence(games);
	if (close) {
		para2.push(close);
	}

	if (playoffs) {
		// Series developments across the day.
		const seriesBits: string[] = [];
		for (const g of games) {
			const shape = analyzeShape(g);
			const post = postseasonContext(g, shape);
			if (post.sentences.length > 0) {
				seriesBits.push(post.sentences[0]!);
			}
			if (seriesBits.length >= 3) {
				break;
			}
		}
		if (seriesBits.length > 0) {
			para2.push(seriesBits.join(" "));
		}
	} else {
		const streak = teamStreakSentence(games);
		if (streak) {
			para2.push(streak);
		}
		const picture = conferencePictureSentence(standings);
		if (picture) {
			para2.push(picture);
		}
	}

	const paragraphs = [para1.join(" ")];
	if (para2.length > 0) {
		paragraphs.push(para2.join(" "));
	}
	return `**${headline}**\n\n${paragraphs.join("\n\n")}`;
};

export default getAutoRecap;
