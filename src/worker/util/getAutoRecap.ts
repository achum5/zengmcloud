import type {
	RecapGame,
	RecapPlayer,
	RecapTeam,
} from "./getDayGamesForRecap.ts";

// A procedural, no-AI game recap: a bold headline plus one or two tight,
// fact-anchored sentences, generated from a RecapGame. Every clause is tied to a
// real number, name, or event in the box score - nothing is invented. Wording is
// varied by a per-game seed so a slate of recaps doesn't read from one template,
// but the facts are what make each one distinct.
//
// This is the automatic, always-on recap. The richer "Copy AI Prompt" flow stays
// available as an on-demand upgrade for any day the user wants more than this.

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

// "34 points, 12 rebounds and 9 assists" - points first, then up to two more
// categories worth mentioning (double-double stats always make the cut).
const statPhrase = (p: RecapPlayer): string => {
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
	const chosen = extras.slice(0, 2).map((e) => e[1]);
	return naturalList([plural(p.pts, "point"), ...chosen]);
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
		pool = ["led the way with", "finished with", "chipped in", "contributed"];
	}
	return pick(rng, pool);
};

const pick = <T>(rng: () => number, arr: T[]): T =>
	arr[Math.floor(rng() * arr.length)]!;

// --- Team-name labels ----------------------------------------------------------

// "the Pistons" - the nickname is the default, natural subject in prose. Region
// ("Detroit") gets an occasional turn for variety.
const nick = (t: RecapTeam): string => t.name || t.region || "the home team";
const theNick = (t: RecapTeam): string => `the ${nick(t)}`;

// --- Game-shape detection ------------------------------------------------------

type Shape = {
	winner: RecapTeam;
	loser: RecapTeam;
	margin: number;
	ot: number;
	// Winner's largest deficit at any period boundary (0 if it never trailed).
	comebackFrom: number;
	comebackPeriod: number;
	wireToWire: boolean;
};

const analyzeShape = (game: RecapGame): Shape => {
	const [home, away] = game.teams;
	const winner = game.winnerTid === home.tid ? home : away;
	const loser = winner === home ? away : home;
	const margin = winner.pts - loser.pts;

	// Reconstruct the game's flow from quarter scoring, in the WINNER's favor.
	let comebackFrom = 0;
	let comebackPeriod = 0;
	let wireToWire = false;
	const wq = winner.ptsQtrs;
	const lq = loser.ptsQtrs;
	if (Array.isArray(wq) && Array.isArray(lq) && wq.length === lq.length) {
		let wCum = 0;
		let lCum = 0;
		let ledEveryBoundary = true;
		for (let i = 0; i < wq.length; i++) {
			wCum += wq[i] ?? 0;
			lCum += lq[i] ?? 0;
			const diff = wCum - lCum; // winner minus loser
			// Only the boundaries BEFORE the last one matter for "led wire to wire".
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
		ot: game.overtimes ?? 0,
		comebackFrom,
		comebackPeriod,
		wireToWire,
	};
};

// The pregame favorite lost, and not by a hair - a genuine upset.
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

// --- Series / play-in context -------------------------------------------------

// A short clause on where the playoff series stands AFTER this game, or the
// play-in stakes that were settled. Undefined for regular-season games.
const postseasonClause = (
	game: RecapGame,
	shape: Shape,
): string | undefined => {
	if (game.playIn) {
		const p = game.playIn;
		const winnerAbbrev = shape.winner.abbrev;
		if (p.kind === "seed9v10") {
			return `${winnerAbbrev} advance to the final play-in game`;
		}
		if (typeof p.prizeSeed === "number") {
			return `${winnerAbbrev} lock up the #${p.prizeSeed} seed`;
		}
		return `${winnerAbbrev} punch their playoff ticket`;
	}

	const s = game.series;
	if (!s) {
		return undefined;
	}
	// Series wins were the count ENTERING this game; add this result.
	const winnerIsHome = shape.winner.abbrev === s.homeAbbrev;
	const winnerWon = (winnerIsHome ? s.homeWon : s.awayWon) + 1;
	const loserWon = winnerIsHome ? s.awayWon : s.homeWon;
	const need =
		typeof s.bestOf === "number" && s.bestOf > 0
			? Math.floor(s.bestOf / 2) + 1
			: undefined;

	if (need !== undefined && winnerWon >= need) {
		if (s.round === s.numRounds) {
			return `${shape.winner.abbrev} win the title`;
		}
		return `${shape.winner.abbrev} take the series ${winnerWon}-${loserWon}`;
	}
	if (winnerWon === loserWon) {
		return `the series is even at ${winnerWon}-${winnerWon}`;
	}
	return `${shape.winner.abbrev} lead the series ${Math.max(
		winnerWon,
		loserWon,
	)}-${Math.min(winnerWon, loserWon)}`;
};

// --- Headline ------------------------------------------------------------------

const scoreTag = (shape: Shape): string => {
	const ot =
		shape.ot > 0 ? ` (${shape.ot === 1 ? "OT" : `${shape.ot}OT`})` : "";
	return `${shape.winner.pts}-${shape.loser.pts}${ot}`;
};

// A compact description of the star's night for a headline: "triple-double",
// "38", or "27 points and 13 boards".
const starHeadline = (p: RecapPlayer): string => {
	const dd = doubleWord(doubleCategories(p).length);
	if (dd === "triple-double" || dd === "quadruple-double") {
		return dd;
	}
	if (p.pts >= 38) {
		return `${p.pts}`;
	}
	if (dd === "double-double" && p.pts < 25) {
		// A quieter scoring night whose story is the two-way line.
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
		return ["rout", "cruise past", "run away from", "roll past", "bury"];
	}
	return ["beat", "down", "top", "take down", "get past"];
};

const buildHeadline = (
	game: RecapGame,
	shape: Shape,
	star: RecapPlayer,
	rng: () => number,
): string => {
	const winnerN = nick(shape.winner);
	const loserN = nick(shape.loser);
	const shot = clutchShot(game);

	// A buzzer-beater or go-ahead shot is the single most concrete thing that
	// happened - lead with it.
	if (shot && !shot.tying) {
		const templates = [
			`${shot.name}'s ${shot.shot} sinks the ${loserN}`,
			`${shot.name}'s ${shot.shot} lifts the ${winnerN} past the ${loserN}`,
			`${shot.name} beats the ${loserN} with a ${shot.shot}`,
		];
		return pick(rng, templates);
	}

	const verb = pick(rng, verbPool(game, shape));
	const ddCount = doubleCategories(star).length;

	// A triple-double (or better) headlines itself.
	if (ddCount >= 3) {
		const word = doubleWord(ddCount)!;
		const templates = [
			`${star.name}'s ${word} carries the ${winnerN} past the ${loserN}`,
			`${star.name} posts a ${word} as the ${winnerN} ${verb} the ${loserN}`,
		];
		return pick(rng, templates);
	}

	if (isUpset(game, shape)) {
		return `${winnerN} ${verb} the ${loserN}, ${scoreTag(shape)}`;
	}

	if (shape.comebackFrom >= 12) {
		const templates = [
			`${winnerN} erase a ${shape.comebackFrom}-point hole to ${verb} the ${loserN}`,
			`${star.name}, ${winnerN} rally past the ${loserN}`,
		];
		return pick(rng, templates);
	}

	// Otherwise lead with the star's line or the result, chosen by seed.
	const starTemplates = [
		`${star.name}'s ${starHeadline(star)} ${
			star.pts >= 25 ? "powers" : "leads"
		} the ${winnerN} past the ${loserN}`,
		`${star.name} scores ${star.pts} as the ${winnerN} ${verb} the ${loserN}`,
		`${winnerN} ${verb} the ${loserN} behind ${star.name}'s ${starHeadline(
			star,
		)}`,
	];
	const resultTemplates = [
		`${winnerN} ${verb} the ${loserN}, ${scoreTag(shape)}`,
		`${winnerN} ${verb} the ${loserN} ${scoreTag(shape)}`,
	];
	const useResult = shape.margin >= 18 && rng() < 0.5;
	return pick(rng, useResult ? resultTemplates : starTemplates);
};

// --- Body ----------------------------------------------------------------------

const buildBody = (
	game: RecapGame,
	shape: Shape,
	star: RecapPlayer,
	rng: () => number,
): string => {
	const sentences: string[] = [];
	const verb = pick(rng, verbPool(game, shape));

	// Sentence 1: the result, carried by the winner's best player. A road win in
	// a named city is worth a nod now and then; "at home" is the default and goes
	// unsaid.
	const home = game.teams[0];
	const onRoad = shape.winner.tid !== home.tid && !!home.region;
	const venue = onRoad && rng() < 0.5 ? ` on the road in ${home.region}` : "";
	sentences.push(
		`${star.name} ${scoredVerb(star, rng)} ${statPhrase(star)} as ${theNick(
			shape.winner,
		)} ${pastTense(verb)} ${theNick(shape.loser)} ${scoreTag(shape)}${venue}.`,
	);

	// Sentence 2: the best supporting angle available.
	const second = secondSentence(game, shape, star, rng);
	if (second) {
		sentences.push(second);
	}

	return sentences.join(" ");
};

// Past-tense-ify the first verb of the pool so the body reads as reportage.
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
	bury: "buried",
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

const secondSentence = (
	game: RecapGame,
	shape: Shape,
	star: RecapPlayer,
	rng: () => number,
): string | undefined => {
	// Playoff/play-in stakes come first when they exist - that's the story.
	const post = postseasonClause(game, shape);
	if (post) {
		const loserStar = bestOf(shape.loser.players);
		const loserBit =
			loserStar && loserStar.pts >= 20
				? ` ${loserStar.name}'s ${statPhrase(loserStar)} wasn't enough.`
				: "";
		const cap = post.charAt(0).toUpperCase() + post.slice(1);
		return `${cap}.${loserBit}`.trim();
	}

	// A real comeback is worth its own beat.
	if (shape.comebackFrom >= 12 && shape.comebackPeriod > 0) {
		return `The ${nick(shape.winner)} had trailed by ${
			shape.comebackFrom
		} after the ${ordinal(shape.comebackPeriod)}.`;
	}

	// A game-tying shot that forced overtime.
	const shot = clutchShot(game);
	if (shot && shot.tying && shape.ot > 0) {
		return `${shot.name}'s ${shot.shot} forced overtime.`;
	}

	// Rotate among the remaining honest angles by seed: a second big night from
	// the winner, the losing team's leader, a win/loss streak, or the record.
	const options: string[] = [];

	// A second winner with a double-double or a real supporting line - the kind
	// of detail that fleshes out a blowout.
	const winnerSupport = shape.winner.players
		.filter((p) => p !== star)
		.find((p) => doubleCategories(p).length >= 2 || p.pts >= 18 || p.reb >= 12);
	if (winnerSupport) {
		const ddw = doubleWord(doubleCategories(winnerSupport).length);
		options.push(
			ddw
				? `${winnerSupport.name} added a ${ddw} with ${statPhrase(winnerSupport)}.`
				: `${winnerSupport.name} chipped in ${statPhrase(winnerSupport)}.`,
		);
	}

	const loserStar = bestOf(shape.loser.players);
	if (loserStar && loserStar.pts >= 22) {
		const leadVerb = pick(rng, [
			`led ${theNick(shape.loser)} in the loss`,
			`paced ${theNick(shape.loser)} in a losing effort`,
			`topped ${theNick(shape.loser)} in defeat`,
		]);
		options.push(`${loserStar.name}'s ${statPhrase(loserStar)} ${leadVerb}.`);
	}

	const streak = shape.winner.streak;
	if (streak && streak.won && streak.count >= 4) {
		options.push(
			`It was ${theNick(shape.winner)}' ${ordinal(streak.count)} straight win.`,
		);
	}

	// A losing streak the winner snapped, read from the loser's last-10 log
	// (index 0 is this game).
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
				`The loss snapped ${theNick(shape.loser)}' ${run}-game winning streak.`,
			);
		}
	}

	const rec = shape.winner.record;
	if (rec && shape.wireToWire) {
		options.push(
			`${theNick(shape.winner).replace(/^the /, "The ")} led wire-to-wire and improved to ${
				rec.won
			}-${rec.lost}.`,
		);
	} else if (rec && rec.won + rec.lost >= 10) {
		options.push(`The win moved them to ${rec.won}-${rec.lost}.`);
	}

	if (options.length === 0) {
		return undefined;
	}
	return pick(rng, options);
};

// --- All-Star Game -------------------------------------------------------------

const buildAllStar = (game: RecapGame, rng: () => number): string => {
	const [home, away] = game.teams;
	const winner = game.winnerTid === home.tid ? home : away;
	const loser = winner === home ? away : home;
	const as = game.allStar ?? {};
	const star = bestOf(winner.players) ?? bestOf(loser.players);

	const headline = as.mvp
		? `${as.mvp} takes All-Star Game MVP`
		: `The All-Stars light up the exhibition`;

	const sentences: string[] = [];
	sentences.push(
		`${nick(winner)} beat ${nick(loser)} ${winner.pts}-${loser.pts} in the All-Star Game${
			star ? `, with ${star.name} scoring ${star.pts}` : ""
		}.`,
	);
	const extras: string[] = [];
	if (as.dunk?.winner) {
		extras.push(`${as.dunk.winner} won the dunk contest`);
	}
	if (as.three?.winner) {
		extras.push(`${as.three.winner} took the three-point contest`);
	}
	if (extras.length > 0) {
		sentences.push(`${naturalList(extras)} over the weekend.`);
	}

	return `**${headline}**\n\n${sentences.join(" ")}`;
};

// --- Entry point ---------------------------------------------------------------

export const getAutoRecap = (game: RecapGame): string => {
	const rng = rngFromSeed(game.gid * 2654435761);

	if (game.allStar) {
		return buildAllStar(game, rng);
	}

	const shape = analyzeShape(game);
	const star =
		bestOf(shape.winner.players) ?? bestOf(shape.loser.players) ?? undefined;

	// A game with no box-score lines (shouldn't happen for a completed game) still
	// gets a plain, correct one-liner rather than throwing.
	if (!star) {
		return `**${nick(shape.winner)} ${pastTense(
			pick(rng, verbPool(game, shape)),
		)} the ${nick(shape.loser)}, ${scoreTag(shape)}**\n\n${nick(
			shape.winner,
		)} beat ${nick(shape.loser)} ${scoreTag(shape)}.`;
	}

	const headline = buildHeadline(game, shape, star, rng);
	const body = buildBody(game, shape, star, rng);
	return `**${headline}**\n\n${body}`;
};

export default getAutoRecap;
