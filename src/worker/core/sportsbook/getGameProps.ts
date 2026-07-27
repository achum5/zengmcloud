import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import getSchedule from "../season/getSchedule.ts";
import { getGameSpread } from "../../../common/getGameSpread.ts";
import { getTeamOvrs } from "./getLines.ts";
import { bySport, isSport } from "../../../common/sportFunctions.ts";
import { probToAmerican } from "../../../common/sportsbook.ts";
import {
	combineIndependentSigmas,
	expectedGameTotal,
	MARGIN_SIGMA,
	marginToWinProb,
	milestoneProb,
	overProb,
	overProbFromSigma,
	probNear,
	toHalfPointLine,
} from "../../../common/sportsbookOdds.ts";

// Per-game player/team prop odds - the "click into a game" deep board. Kept
// SEPARATE from getLines()'s whole-league board on purpose: computing this
// for every upcoming game (dozens of players × a dozen stats each) on every
// board load would be far too expensive. This is computed on demand, for one
// game at a time, exactly when the UI needs it (the game detail page, and
// server-side re-validation of a placed prop bet).
//
// Basketball only: the stat set (trb/ast/stl/blk/tp) and the All-Star-style
// selection formulas below don't translate cleanly to the other sports this
// engine also supports, and guessing a cross-sport formula risks a
// mispriced/exploitable line - the opposite of "no freebies". See the same
// scoping decision for All-Star/All-Defensive futures in getLines.ts.

const priceOdds = (prob: number) => probToAmerican(prob);

// Game-to-game coefficient of variation per stat (relative spread around the
// projected mean) and an absolute floor so a low-volume stat (e.g. a bench
// big averaging 0.4 blocks) never collapses to a near-zero sigma. Points are
// the most stable game-to-game; low-count defensive/shooting stats
// (steals/blocks/threes) are the spikiest - this mirrors real-world NBA
// prop-market variance ordering.
const STAT_CV = {
	pts: 0.32,
	trb: 0.38,
	ast: 0.42,
	stl: 0.6,
	blk: 0.65,
	tp: 0.55,
	tov: 0.42,
} as const;
const STAT_SIGMA_FLOOR = {
	pts: 3,
	trb: 1.2,
	ast: 1.2,
	stl: 0.6,
	blk: 0.6,
	tp: 0.6,
	tov: 0.8,
} as const;
type CountingStat = keyof typeof STAT_CV;

const projectStat = (mean: number, stat: CountingStat) => ({
	mean,
	sigma: Math.max(STAT_SIGMA_FLOOR[stat], mean * STAT_CV[stat]),
});

// How many of the current season's games it takes to fully trust the
// current-season average over the player's established (prior-season) rate -
// matches the spirit of getLines.ts's team-scoring regression.
const FULL_TRUST_GP = 10;

type PlayerBase = {
	pid: number;
	name: string;
	tid: number;
	abbrev: string;
	min: number;
} & Record<CountingStat, number>;

// A player's projected per-game mean for each counting stat, blending the
// CURRENT season's per-game average (trusted more as its sample grows) with
// their most recent PRIOR season's average (their established track record,
// used as the prior instead of a generic league/position average - a
// player's own history is the more defensible baseline). A player with
// NEITHER current nor prior per-game data (a true rookie who hasn't played a
// game yet) gets no projection at all - see buildPlayerField below, which
// drops such players rather than invent a ratings-based guess that could be
// an exploitable bad line.
const blend = (
	curMean: number,
	curGp: number,
	priorMean: number | undefined,
): number => {
	if (curGp <= 0) {
		return priorMean ?? 0;
	}
	const w = Math.min(1, curGp / FULL_TRUST_GP);
	const base = priorMean ?? curMean;
	return curMean * w + base * (1 - w);
};

// Fetch + project every player likely to see meaningful minutes for one team,
// dropping anyone with zero games of real data (current OR prior season) to
// play with. Capped to the top players by projected minutes so the board
// doesn't fill up with noise props for the end of a 15-man bench.
const MAX_PLAYERS_PER_TEAM = 10;
const MIN_PROJECTED_MINUTES = 8;

const buildPlayerField = async (
	tid: number,
	abbrev: string,
	season: number,
): Promise<PlayerBase[]> => {
	const rawPlayers = await idb.cache.players.indexGetAll("playersByTid", tid);
	const statKeys = [
		"gp",
		"min",
		"pts",
		"trb",
		"ast",
		"stl",
		"blk",
		"tp",
		"tov",
	];

	const current = await idb.getCopies.playersPlus(rawPlayers, {
		attrs: ["pid", "name", "tid"],
		stats: statKeys,
		season,
		showNoStats: true,
		tid,
	});

	// No tid filter here, on purpose: the prior season is the player's track
	// record WHEREVER it happened. Filtering by current team silently zeroed
	// out every offseason acquisition's history, leaving their early-season
	// lines priced off a tiny current sample.
	const prior = await idb.getCopies.playersPlus(rawPlayers, {
		attrs: ["pid"],
		stats: statKeys,
		season: season - 1,
		showNoStats: true,
	});
	const priorByPid = new Map(prior.map((p: any) => [p.pid, p]));

	const out: PlayerBase[] = [];
	for (const p of current as any[]) {
		const curGp = p.stats.gp ?? 0;
		const priorRow = priorByPid.get(p.pid);
		const priorGp = priorRow?.stats.gp ?? 0;
		if (curGp <= 0 && priorGp <= 0) {
			// No real data to project from at all - skip rather than guess.
			continue;
		}
		const projMin = blend(p.stats.min ?? 0, curGp, priorRow?.stats.min);
		if (projMin < MIN_PROJECTED_MINUTES) {
			continue;
		}
		out.push({
			pid: p.pid,
			name: p.name,
			tid,
			abbrev,
			min: projMin,
			pts: blend(p.stats.pts ?? 0, curGp, priorRow?.stats.pts),
			trb: blend(p.stats.trb ?? 0, curGp, priorRow?.stats.trb),
			ast: blend(p.stats.ast ?? 0, curGp, priorRow?.stats.ast),
			stl: blend(p.stats.stl ?? 0, curGp, priorRow?.stats.stl),
			blk: blend(p.stats.blk ?? 0, curGp, priorRow?.stats.blk),
			tp: blend(p.stats.tp ?? 0, curGp, priorRow?.stats.tp),
			tov: blend(p.stats.tov ?? 0, curGp, priorRow?.stats.tov),
		});
	}

	out.sort((a, b) => b.min - a.min);
	return out.slice(0, MAX_PLAYERS_PER_TEAM);
};

const ouRow = (stat: string, mean: number, sigma: number) => {
	const line = toHalfPointLine(mean);
	const pOver = overProbFromSigma(mean, line, sigma);
	return {
		stat,
		line,
		over: priceOdds(pOver),
		under: priceOdds(1 - pOver),
	};
};

export type GamePropsBoard = Awaited<ReturnType<typeof getGameProps>>;

// The full prop board for exactly one upcoming game. Returns undefined if the
// game isn't a currently-schedulable one (already played, or an invalid gid) -
// the caller (the UI page, and validateAgainstBoard) treats that as "no props
// available".
export const getGameProps = async (gid: number) => {
	if (!isSport("basketball")) {
		return undefined;
	}

	const season = g.get("season");
	const homeCourtAdvantage = g.get("homeCourtAdvantage");
	const numPeriods = g.get("numPeriods");
	const quarterLength = g.get("quarterLength");

	const schedule = await getSchedule();
	const matchup = schedule.find((gm) => gm.gid === gid);
	if (!matchup || matchup.homeTid < 0 || matchup.awayTid < 0) {
		return undefined;
	}

	// Teams don't have a derived "trb" stat the way players do (see
	// playersPlus.ts) - fetch orb/drb raw and sum them ourselves below.
	// oppPts is for the main total line (same additive model as getLines).
	const teams = await idb.getCopies.teamsPlus(
		{
			attrs: ["tid", "abbrev", "region", "name", "disabled"],
			stats: ["pts", "oppPts", "orb", "drb", "ast", "tp", "gp"],
			season,
			showNoStats: true,
		},
		"noCopyCache",
	);
	const home = teams.find((t) => t.tid === matchup.homeTid);
	const away = teams.find((t) => t.tid === matchup.awayTid);
	if (!home || !away || home.disabled || away.disabled) {
		return undefined;
	}

	// League-average per-team stat line, for regressing team props toward
	// early in the season (same rationale as getLines.ts's leagueAvgTotal).
	const activeTeams = teams.filter((t) => !t.disabled);
	// Teams don't have a derived "trb" - read it as orb+drb everywhere below.
	const teamStatValue = (s: any, stat: "pts" | "trb" | "ast" | "tp"): number =>
		stat === "trb" ? (s?.orb ?? 0) + (s?.drb ?? 0) : (s?.[stat] ?? 0);

	const leagueAvg = { pts: 0, trb: 0, ast: 0, tp: 0 };
	let teamsWithGames = 0;
	for (const t of activeTeams) {
		const s = t.stats as any;
		if ((s?.gp ?? 0) > 0) {
			leagueAvg.pts += teamStatValue(s, "pts");
			leagueAvg.trb += teamStatValue(s, "trb");
			leagueAvg.ast += teamStatValue(s, "ast");
			leagueAvg.tp += teamStatValue(s, "tp");
			teamsWithGames += 1;
		}
	}
	if (teamsWithGames > 0) {
		leagueAvg.pts /= teamsWithGames;
		leagueAvg.trb /= teamsWithGames;
		leagueAvg.ast /= teamsWithGames;
		leagueAvg.tp /= teamsWithGames;
	} else {
		// No games played anywhere yet - fall back to a generic baseline so
		// early-season team props aren't all pinned at exactly 0.
		leagueAvg.pts = 110;
		leagueAvg.trb = 44;
		leagueAvg.ast = 25;
		leagueAvg.tp = 12;
	}

	const teamMean = (
		t: (typeof teams)[number],
		stat: "pts" | "trb" | "ast" | "tp",
	) => {
		const s = t.stats as any;
		const gp = s?.gp ?? 0;
		const perGame = teamStatValue(s, stat);
		const w = Math.min(1, gp / FULL_TRUST_GP);
		return perGame * w + leagueAvg[stat] * (1 - w);
	};

	const teamPropRows = (t: (typeof teams)[number]) => {
		const stats: ("pts" | "trb" | "ast" | "tp")[] = ["pts", "trb", "ast", "tp"];
		return stats.map((stat) => {
			const mean = teamMean(t, stat);
			// Team totals run a bit hotter/spikier than a single player's, but the
			// same CV-based model applies fine.
			const sigma = Math.max(
				stat === "pts" ? 6 : 2.5,
				mean * (stat === "pts" ? 0.12 : 0.16),
			);
			const line = toHalfPointLine(mean);
			const pOver = overProbFromSigma(mean, line, sigma);
			return {
				tid: t.tid,
				stat,
				line,
				over: priceOdds(pOver),
				under: priceOdds(1 - pOver),
			};
		});
	};

	// --- Overtime prop -------------------------------------------------------
	// Reuses the SAME margin model as the moneyline/spread (getGameSpread +
	// MARGIN_SIGMA), so it can never disagree with them about which team is
	// favored or by how much.
	const ovrByTid = await getTeamOvrs([home, away], season);
	const margin = getGameSpread({
		ovr0: ovrByTid.get(home.tid),
		ovr1: ovrByTid.get(away.tid),
		homeCourtAdvantage,
		neutralSite: false,
		numPeriods,
		quarterLength,
	});
	// A team ending regulation within ~2 points of tied is the population that
	// can go to OT; calibrated so an even matchup lands near a realistic ~7-8%
	// real-world OT rate, and a blowout-favorite matchup is correctly much
	// lower.
	const otBand = 2;
	const pOvertime =
		margin === undefined
			? undefined
			: Math.min(0.35, probNear(margin, MARGIN_SIGMA, otBand) * 1.8);

	// --- Main lines (moneyline / spread / total) ---------------------------
	// The exact same formulas AND inputs as getLines.ts's game-lines section,
	// deliberately: a spread/ML/total bet placed from this page is validated by
	// placeBet against the whole-league board, so any drift between the two
	// computations would spuriously reject the bet as a moved line.
	let main:
		| {
				moneyline: { home: number; away: number };
				spread: { line: number; home: number; away: number };
				total: { line: number; over: number; under: number };
		  }
		| undefined;
	if (margin !== undefined) {
		const ptsStats = (t: (typeof teams)[number]) => {
			const s = t.stats as any;
			return {
				gp: s?.gp ?? 0,
				pts: s?.pts ?? 0,
				oppPts: s?.oppPts ?? 0,
			};
		};
		let totalPtsPerGame = 0;
		let n = 0;
		for (const t of activeTeams) {
			const s = ptsStats(t);
			if (s.gp > 0) {
				totalPtsPerGame += s.pts;
				n += 1;
			}
		}
		const leagueAvgTotal =
			n > 0
				? (2 * totalPtsPerGame) / n
				: bySport({ basketball: 220, football: 45, baseball: 9, hockey: 6 });
		const halfLeague = leagueAvgTotal / 2;
		const regress = (perGame: number, gp: number) => {
			const w = Math.min(1, gp / 8);
			return halfLeague + (perGame - halfLeague) * w;
		};
		const scoringFor = (t: (typeof teams)[number]) => {
			const s = ptsStats(t);
			return s.gp > 0 ? regress(s.pts, s.gp) : undefined;
		};
		const scoringAgainst = (t: (typeof teams)[number]) => {
			const s = ptsStats(t);
			return s.gp > 0 ? regress(s.oppPts, s.gp) : undefined;
		};

		const pHome = marginToWinProb(margin);
		const expectedTotal = expectedGameTotal({
			homeFor: scoringFor(home),
			homeAgainst: scoringAgainst(home),
			awayFor: scoringFor(away),
			awayAgainst: scoringAgainst(away),
			leagueAvgTotal,
		});
		const totalLine = toHalfPointLine(expectedTotal);
		const pOver = overProb(expectedTotal, totalLine);
		const spreadLine =
			toHalfPointLine(Math.abs(margin)) * (margin >= 0 ? -1 : 1);
		main = {
			moneyline: {
				home: priceOdds(pHome),
				away: priceOdds(1 - pHome),
			},
			spread: {
				line: spreadLine,
				home: priceOdds(0.5),
				away: priceOdds(0.5),
			},
			total: {
				line: totalLine,
				over: priceOdds(pOver),
				under: priceOdds(1 - pOver),
			},
		};
	}

	// --- Player props ----------------------------------------------------
	const homeField = await buildPlayerField(home.tid, home.abbrev, season);
	const awayField = await buildPlayerField(away.tid, away.abbrev, season);

	const playerRow = (p: PlayerBase) => {
		const cats: CountingStat[] = [
			"pts",
			"trb",
			"ast",
			"stl",
			"blk",
			"tp",
			"tov",
		];
		const props = cats.map((stat) => {
			const proj = projectStat(p[stat], stat);
			return { ...ouRow(stat, proj.mean, proj.sigma) };
		});

		const projFor = (stat: CountingStat) => projectStat(p[stat], stat);
		const pts = projFor("pts");
		const trb = projFor("trb");
		const ast = projFor("ast");
		const combo = (label: "pra" | "pr" | "pa", parts: (typeof pts)[]) => {
			const mean = parts.reduce((s, x) => s + x.mean, 0);
			const sigma = combineIndependentSigmas(parts.map((x) => x.sigma));
			return ouRow(label, mean, sigma);
		};
		props.push(combo("pra", [pts, trb, ast]));
		props.push(combo("pr", [pts, trb]));
		props.push(combo("pa", [pts, ast]));

		const ddCats = (["pts", "trb", "ast", "stl", "blk"] as CountingStat[]).map(
			(stat) => projectStat(p[stat], stat),
		);
		// Deterministic per-player seed so the odds are stable between board
		// loads (and the server re-derives the same board when validating a
		// bet) without depending on wall-clock time.
		const seed = (p.pid * 7919 + gid * 104729) % 2147483647;
		const ddProb = milestoneProb(ddCats, 10, 2, { seed });
		const tdProb = milestoneProb(ddCats, 10, 3, { seed: seed + 1 });

		return {
			pid: p.pid,
			name: p.name,
			tid: p.tid,
			abbrev: p.abbrev,
			props,
			doubleDouble: priceOdds(ddProb),
			tripleDouble: priceOdds(tdProb),
		};
	};

	return {
		gid,
		home: {
			tid: home.tid,
			abbrev: home.abbrev,
			region: home.region,
			name: home.name,
			players: homeField.map(playerRow),
			teamProps: teamPropRows(home),
		},
		away: {
			tid: away.tid,
			abbrev: away.abbrev,
			region: away.region,
			name: away.name,
			players: awayField.map(playerRow),
			teamProps: teamPropRows(away),
		},
		overtime: pOvertime === undefined ? undefined : priceOdds(pOvertime),
		main,
	};
};
