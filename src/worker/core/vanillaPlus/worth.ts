import { PHASE, PLAYER } from "../../../common/constants.ts";
import type { Player } from "../../../common/types.ts";
import { idb } from "../../db/index.ts";
import { g, local } from "../../util/index.ts";
import getPickValues from "../trade/getPickValues.ts";
import { projectRatings } from "../realisticFrontOffice/projection.ts";
import type { Lane } from "./lanes.ts";
import { nextSeason, thisSeason, type Calendar } from "./calendar.ts";

// WORTH (docs/vanilla-plus.md, "Worth, season by season"): what a player is
// worth in a trade, on vanilla's scale, counted one season at a time.
//
// Vanilla prices a player with one number - his rating and potential blended
// by age - raised to the 7th power, so one star is worth many good players.
// But it blends before it raises: a star who is great now and fading later
// is priced as his average, and a teenager by a potential rating that sits
// about five points above how careers actually turn out. Here each season is
// priced on its own (the player he is that season, by vanilla's 7th power)
// and the seasons are added up, each counting less than the one before.
//
// His seasons are played out with the game's own development code
// (realisticFrontOffice/projection.ts), many careers each, so a chance at
// stardom counts for what it's worth.
//
// How much each season counts is the team's horizon. The market's is a
// steady decline, set so that players in their prime cost what vanilla says.
// A lane's is its window: an all-in team counts this season and next (and a
// little of the one after) and nothing past it - a 17-year-old's chance of
// stardom in year eight is no help to it, however big - while a rebuilding
// team counts the seasons it means to win in above this one.
const geometric = (d: number, n: number) =>
	Array.from({ length: n }, (_, i) => d ** i);
export const worthSettings = {
	// Seasons counted, the coming one first.
	years: 10,
	// A pick in a later draft is worth this much of the coming draft's at the
	// same slot, for each year more it waits (pickValues).
	pickWait: 0.85,
	// Simulated careers per player.
	samples: 40,
	// No career counts above his potential by more than this: the 7th power
	// on the spread of a teenager's careers made a 20-year-old 46/65 worth
	// three times a 22-year-old 59/73 (and a team's 18-year-old 30/60 a main
	// piece for its 21-year-old 62/74) - a long shot's tail, far past the
	// ceiling the game shows, priced as a star.
	ceiling: 3,
	// Each lane's weight on each season, the coming one first.
	weights: {
		allIn: [1, 0.6, 0.25],
		playoff: [1, 0.75, 0.5, 0.3, 0.15],
		retool: geometric(0.75, 10),
		rebuild: [0.5, 0.7, 0.9, 1, 1, 1, 0.9, 0.8, 0.7, 0.6],
	} as Record<Lane, number[]>,
	market: geometric(0.65, 10),
	// A team trying to win now (all-in or playoff) without a real shot at
	// the title (calendar.ts titleShot, under `realShot` of the league's
	// chances) doesn't spend its future on this season: its window moves from
	// its lane's toward a patient one, where this season counts less - and
	// the next two most if its core is young (it means to strike once it's
	// improved) - the whole way with no shot at all (owner, 2026-11-12: "you don't want teams
	// mortgaging their potential future really good years in the current
	// year if they don't have a real shot at it").
	realShot: 0.08,
	// This season counts less, and a young team's next two most - the
	// seasons it means to strike in - but not the far ones: a player at his
	// peak now is worth as much to it as to anyone.
	patientYoung: [0.5, 0.9, 1, 0.8, 0.5, 0.3, 0.15],
	patientOld: [0.5, 0.75, 0.5, 0.3, 0.15],
	youngCore: 25,
	oldCore: 28,
};

// The window a team prices players by: its lane's, but a team trying to win
// now by its title shot and core age (worthSettings.realShot).
export const windowFor = (
	lane: Lane,
	titleShot: number | undefined,
	coreAge: number | undefined,
): number[] => {
	const s = worthSettings;
	const own = s.weights[lane];
	if (
		(lane !== "playoff" && lane !== "allIn") ||
		titleShot === undefined ||
		coreAge === undefined ||
		s.realShot <= 0
	) {
		return own;
	}
	const patient = Math.max(0, 1 - titleShot / s.realShot);
	if (patient === 0) {
		return own;
	}
	const old = Math.min(
		1,
		Math.max(0, (coreAge - s.youngCore) / (s.oldCore - s.youngCore)),
	);
	const n = Math.max(own.length, s.patientYoung.length, s.patientOld.length);
	return Array.from({ length: n }, (_, i) => {
		const young = s.patientYoung[i] ?? 0;
		const wait = young + ((s.patientOld[i] ?? 0) - young) * old;
		return (own[i] ?? 0) + (wait - (own[i] ?? 0)) * patient;
	});
};

// Vanilla's (team/ValueChangeCalculator.ts): a value's z-score, and its trade
// value - the z-score raised to the 7th power above 1.
const zscore = (value: number) =>
	(value - local.playerOvrMean) / local.playerOvrStd;
const tradeValue = (z: number) => (z > 1 ? z ** 7 : z);
// Back from a trade value to the z-score that gives it.
export const zOf = (tv: number) => (tv > 1 ? tv ** (1 / 7) : tv);

// The season "now" is: in the offseason, the coming one.
const nowIndex = () => (g.get("phase") > PHASE.PLAYOFFS ? 1 : 0);

// Gauss-Hermite nodes and weights for an expectation over a standard normal
// (10 points, each used either side of zero).
const HERMITE = [
	[0.342901327223705, 0.610862633735326],
	[1.036610829789514, 0.240138611082315],
	[1.756683649299882, 0.033874394455481],
	[2.53273167423279, 0.001343645746781],
	[3.436159118837738, 0.000007640432855],
].map(([x, w]) => [x! * Math.SQRT2, w! / Math.sqrt(Math.PI)] as const);

const cache = new Map<string, number[]>();
let cacheFor = "";

// Each season's expected trade value from this one on, by the game's own
// development; cached for the season.
export const seasonValues = (p: Player, years: number) => {
	const season = g.get("season");
	const key = `${g.get("lid")}:${season}`;
	if (key !== cacheFor) {
		cache.clear();
		cacheFor = key;
	}
	const r = p.ratings.at(-1)!;
	const id = `${p.pid}:${r.season}:${r.ovr}:${r.pot}:${years}:${worthSettings.ceiling}`;
	let out = cache.get(id);
	if (!out) {
		const proj = projectRatings({
			ratings: r as any,
			age: season - p.born.year,
			years: years - 1,
			samples: worthSettings.samples,
			// The same player the same season projects the same on every device.
			seed: p.pid * 7919 + season,
		});
		// valueNoPot is what he's worth now (ratings and box score, on vanilla's
		// value scale); each season moves it by how his rating moves from the
		// projection's own start. Below average stays below zero, as vanilla's
		// does, so a fringe player is still worth less than a better one.
		//
		// Each season's spread of careers, not the careers themselves: a few
		// dozen careers put a long shot's value at the mercy of one or two
		// superstar draws, which the 7th power magnifies and sorting a draft
		// class by it would select for. So the careers give each season's
		// average and spread, and the worth is the exact expectation over that
		// spread (Gauss-Hermite).
		const scale = 10 / local.playerOvrStd;
		const start = proj.ovr[0]!;
		const top = r.pot + worthSettings.ceiling;
		const at = (ovr: number) =>
			tradeValue(zscore(p.valueNoPot + (Math.min(ovr, top) - start) * scale));
		out = proj.ovr.map((mean, t) => {
			const sd = proj.ovrSd[t]!;
			let sum = 0;
			for (const [x, w] of HERMITE) {
				sum += w * (at(mean + sd * x) + at(mean - sd * x));
			}
			return sum;
		});
		cache.set(id, out);
	}
	return out;
};

// His worth with these weights, as a trade value: a weighted average of his
// seasons from the coming one, counting only those from `first` on (a draft
// prospect plays only once drafted) - so seasons he can't play count as
// nothing.
//
// With the calendar (calendar.ts): the coming season counts `thisSeason` of a
// full one - the games left and the playoffs, for this team, and only those
// he'll be back from injury for - the one after it what an injury leaves of
// it, and seasons past his contract by the chance he re-signs.
export type WorthTimes = {
	thisSeason: number;
	nextSeason: number;
	exp: number;
	retention: number;
};

export const worth = (
	p: Player,
	weights: number[],
	first = 0,
	times?: WorthTimes,
) => {
	const now = nowIndex();
	const seasons = seasonValues(p, now + worthSettings.years);
	const season = g.get("season");
	let sum = 0;
	let weight = 0;
	for (const [i, w] of weights.entries()) {
		const t = now + i;
		weight += w;
		if (t >= first) {
			let x = seasons[t] ?? 0;
			if (times) {
				// By its size, as stock's applyFactor: less of a season is
				// always worth less, below average or not - a fringe player is
				// never more attractive for being hurt.
				const scale = (f: number) => {
					x += Math.abs(x) * (f - 1);
				};
				if (i === 0) {
					scale(times.thisSeason);
				} else if (i === 1) {
					scale(times.nextSeason);
				}
				if (season + t > times.exp) {
					scale(times.retention);
				}
			}
			sum += w * x;
		}
	}
	return sum / weight;
};

// The calendar's part for one player and team (`playoffs`: the team's
// relative playoffs, 1 for the market).
export const timesFor = (
	c: Calendar | undefined,
	p: Player,
	playoffs: number,
): WorthTimes | undefined =>
	c
		? {
				thisSeason: thisSeason(c, playoffs, p.injury.gamesRemaining),
				nextSeason: nextSeason(c, p.injury.gamesRemaining),
				exp: c.exp.get(p.pid) ?? Infinity,
				retention: c.retention.get(p.pid) ?? 1,
			}
		: undefined;

// A team's window, from the calendar if there is one.
const weightsOf = (lane: Lane, c: Calendar | undefined, tid?: number) =>
	tid === undefined || !c
		? worthSettings.weights[lane]
		: windowFor(lane, c.titleShot.get(tid), c.coreAge.get(tid));

// His worth to a team (tid) in this lane, as a trade value.
export const worthFor = (
	p: Player,
	lane: Lane,
	c?: Calendar,
	playoffs = 1,
	tid?: number,
) => worth(p, weightsOf(lane, c, tid), 0, timesFor(c, p, playoffs));

// What a team in this lane would pay for him, and what the market would, as
// vanilla z-scores (team/ValueChangeCalculator.ts playerWorth).
export const playerWorth = (
	p: Player,
	lane: Lane | undefined,
	c?: Calendar,
	playoffs = 1,
	tid?: number,
) => {
	const market = zOf(worth(p, worthSettings.market, 0, timesFor(c, p, 1)));
	const times = timesFor(c, p, playoffs);
	const weights = lane ? weightsOf(lane, c, tid) : worthSettings.market;
	const own = lane ? zOf(worth(p, weights, 0, times)) : market;
	// After this season alone, as the team would price him (its own or the
	// market's, whichever more): what the tank effect can't pay for.
	const later =
		lane && times
			? zOf(
					Math.max(
						worth(p, weights, 0, {
							...times,
							thisSeason: 0,
						}),
						worth(p, worthSettings.market, 0, {
							...timesFor(c, p, 1)!,
							thisSeason: 0,
						}),
					),
				)
			: undefined;
	return { own, market, later };
};

// A draft prospect's market worth, on vanilla's value scale (for the pick
// table, trade/getPickValues.ts): he plays from the season after his draft.
export const prospectValue = (p: Player, draftYear = p.draft.year) => {
	const first = draftYear + 1 - g.get("season");
	const z = zOf(worth(p, worthSettings.market, first));
	return z * local.playerOvrStd + local.playerOvrMean;
};

// A pick in a draft `delay` years after the coming one, priced by the coming
// draft's class: its prospects as they are on their draft day, worth
// `pickWait` less for each year more of waiting (not the market's discount
// on a player's seasons, which would put a far pick's whole career past its
// horizon). Not the prospects already generated for that draft: they're
// years from it, and the expected worth of a 7th-power value over years of
// development blows every prospect up - most of all the ones a second-round
// pick would get, as if the uncertainty were still there once they'd been
// drafted (it isn't: a pick gets who's left then).
const delayedProspectValue = (p: Player, delay: number) => {
	const first = p.draft.year + 1 - g.get("season");
	const z = zOf(
		worth(p, worthSettings.market, first) * worthSettings.pickWait ** delay,
	);
	return z * local.playerOvrStd + local.playerOvrMean;
};

// The pick table on the same scale (trade/getPickValues.ts, which values the
// prospects already in the league): the coming draft by its class, every
// draft after it by the same class, later (delayedProspectValue).
export const pickValues = async () => {
	const table = await getPickValues(prospectValue);
	const phase = g.get("phase");
	const coming = phase < PHASE.DRAFT ? g.get("season") : g.get("season") + 1;
	const prospects = (
		await idb.cache.players.indexGetAll("playersByTid", PLAYER.UNDRAFTED)
	).filter((p) => p.draft.year === coming);
	if (prospects.length === 0) {
		return table;
	}
	const seasons = new Set(
		(await idb.cache.draftPicks.getAll())
			.map((dp) => dp.season)
			.filter((season): season is number => typeof season === "number"),
	);
	for (const season of seasons) {
		if (season > coming) {
			table[season] = prospects
				.map((p) => delayedProspectValue(p, season - coming))
				.sort((a, b) => b - a);
		}
	}
	table.default = prospects
		.map((p) => delayedProspectValue(p, 1))
		.sort((a, b) => b - a);
	return table;
};
