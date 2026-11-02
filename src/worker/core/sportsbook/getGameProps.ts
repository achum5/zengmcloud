import { g } from "../../util/index.ts";
import getSchedule from "../season/getSchedule.ts";
import { buildGameLinePricer } from "./gameLines.ts";
import {
	simGameOutcomes,
	type SimmedGame,
	type SimmedPlayer,
	type SimPlayerStat,
	type SimmedTeam,
	type SimTeamStat,
} from "./simGameOutcomes.ts";
import {
	probToAmerican,
	SPORTSBOOK_MAX_AMERICAN,
} from "../../../common/sportsbook.ts";
import {
	eventProb,
	sampleMean,
	smoothedOverProb,
	toHalfPointLine,
} from "../../../common/sportsbookOdds.ts";
import { team } from "../index.ts";

// Per-game player/team prop odds - the "click into a game" deep board. Kept
// SEPARATE from getLines()'s whole-league board on purpose: this prices ONE
// game by simulating it a few hundred times (see simGameOutcomes.ts), which is
// a couple of seconds of work and could never be done for the two dozen games
// the main board carries. It's computed on demand, exactly when the UI needs it
// (the game detail page, and server-side re-validation of a placed prop bet),
// and cached against the league state it was computed from.
//
// Every prop here is read off the simulated box scores rather than modeled:
//
//   - A player's line is the average of what he actually did across those
//     games, so it already accounts for the opponent's defense, the pace both
//     teams play at, who's hurt, and how the rotation shakes out - none of
//     which a season average can see.
//   - The odds on going over are how often he actually went over, so the shape
//     of the distribution is the real one. A big man's rebounding is not a bell
//     curve, and nothing here pretends it is.
//   - Combined props (points+rebounds+assists) are summed WITHIN each simulated
//     game before being counted, so they carry the real correlation between the
//     categories. Adding independent variances, as the old model did,
//     understated the spread badly: those categories all rise and fall together
//     with minutes.
//   - Double-doubles and triple-doubles are simply counted, using the same
//     definition the game itself writes to a box score.
//
// The moneyline/spread/total on this page are NOT simulated - they come from
// the shared pricer in gameLines.ts, the same code the main board uses, because
// a bet placed here is validated against that board and the two have to agree
// to the dollar.
//
// Basketball only: the stat set (trb/ast/stl/blk/tp) and the double-double
// definition don't translate to the other sports this engine also supports, and
// guessing a cross-sport formula risks a mispriced/exploitable line - the
// opposite of "no freebies". See the same scoping decision for
// All-Star/All-Defensive futures in getLines.ts.

// (The longest price the book offers anywhere - a triple-double for a man who
// never got one in five hundred runs was going up at +49900.)
const priceOdds = (prob: number) =>
	probToAmerican(prob, { maxAmerican: SPORTSBOOK_MAX_AMERICAN });

// A side priced past this is as good as decided - "over 0.5 points" for a
// starter - and its market is left off the board, as a book would: priced at
// the clamp it loses its hold, and a sure thing at -9900 is free money.
const DECIDED_PRICE = -9000;
const decided = (row: { over: number; under: number }) =>
	row.over <= DECIDED_PRICE || row.under <= DECIDED_PRICE;

// Keep the board to players who are actually going to play. The sim answers
// this directly - a player who's hurt, or buried on the bench, simply doesn't
// get minutes in it - so these are read off simulated playing time rather than
// off a projection.
const MAX_PLAYERS_PER_TEAM = 10;
const MIN_SIMULATED_MINUTES = 8;

const ouRow = (stat: string, samples: number[]) => {
	const line = toHalfPointLine(sampleMean(samples));
	const pOver = smoothedOverProb(samples, line);
	return {
		stat,
		line,
		over: priceOdds(pOver),
		under: priceOdds(1 - pOver),
	};
};

// Sum a set of stats WITHIN each simulated game, so the combined prop keeps the
// correlation between them instead of assuming it away.
const combinedSamples = (p: SimmedPlayer, stats: SimPlayerStat[]) => {
	const n = p.samples[stats[0]!]!.length;
	const out = Array<number>(n).fill(0);
	for (const stat of stats) {
		const samples = p.samples[stat];
		for (let i = 0; i < n; i++) {
			out[i]! += samples[i] ?? 0;
		}
	}
	return out;
};

const PLAYER_PROP_STATS: SimPlayerStat[] = [
	"pts",
	"trb",
	"ast",
	"stl",
	"blk",
	"tp",
	"tov",
];

const TEAM_PROP_STATS: SimTeamStat[] = ["pts", "trb", "ast", "tp"];

const playerRow = (p: SimmedPlayer, abbrev: string, numSims: number) => ({
	pid: p.pid,
	name: p.name,
	tid: p.tid,
	abbrev,
	props: [
		...PLAYER_PROP_STATS.map((stat) => ouRow(stat, p.samples[stat])),
		ouRow("pra", combinedSamples(p, ["pts", "trb", "ast"])),
		ouRow("pr", combinedSamples(p, ["pts", "trb"])),
		ouRow("pa", combinedSamples(p, ["pts", "ast"])),
	].filter((row) => !decided(row)),
	doubleDouble: priceOdds(eventProb(p.dd, numSims)),
	tripleDouble: priceOdds(eventProb(p.td, numSims)),
});

// THE PAGE TELLS ONE STORY. The game total and spread come from the shared
// pricer (gameLines.ts) - the main board's numbers, which a bet placed here is
// validated against - while the team and player props come from simulating
// the game. Left alone the two disagreed on the same page (team points of
// 95.5 and 109.5 under a 213.5 total, the away side by 14 under a spread with
// the home team favoured), and the gap between them was a bet against the
// book's own numbers. So each team's points are centred on what the main
// lines imply for it - half the total, plus or minus half the margin - and
// its players' points scaled with it; the sim keeps the shape: how scoring
// spreads, who gets it, how it moves with the rest of the line.
const centredOn = (t: SimmedTeam, target: number): SimmedTeam => {
	const simMean = sampleMean(t.samples.pts);
	if (!(simMean > 0) || !Number.isFinite(target) || target <= 0) {
		return t;
	}
	const shift = target - simMean;
	const k = target / simMean;
	return {
		...t,
		samples: { ...t.samples, pts: t.samples.pts.map((x) => x + shift) },
		players: t.players.map((p) => ({
			...p,
			samples: { ...p.samples, pts: p.samples.pts.map((x) => x * k) },
		})),
	};
};

const playerField = (t: SimmedTeam) =>
	t.players
		.map((p) => ({ p, min: sampleMean(p.samples.min) }))
		.filter(({ min }) => min >= MIN_SIMULATED_MINUTES)
		.sort((a, b) => b.min - a.min)
		.slice(0, MAX_PLAYERS_PER_TEAM)
		.map(({ p }) => p);

const teamPropRows = (t: SimmedTeam) =>
	TEAM_PROP_STATS.map((stat) => ({
		tid: t.tid,
		...ouRow(stat, t.samples[stat]),
	})).filter((row) => !decided(row));

export type GamePropsBoard = Awaited<ReturnType<typeof getGameProps>>;

// The full prop board for exactly one upcoming game. Returns undefined if the
// game isn't a currently-schedulable one (already played, or an invalid gid) -
// the caller (the UI page, and validateAgainstBoard) treats that as "no props
// available".
export const getGameProps = async (gid: number) => {
	if (__SPORT !== "basketball") {
		return undefined;
	}

	const season = g.get("season");

	const schedule = await getSchedule();
	const matchup = schedule.find((gm) => gm.gid === gid);
	if (!matchup || matchup.homeTid < 0 || matchup.awayTid < 0) {
		return undefined;
	}

	const teams = await team.getFormattedAll({
		attrs: [
			"tid",
			"abbrev",
			"region",
			"name",
			"disabled",
			"playThroughInjuries",
		],
		stats: ["pts", "oppPts", "gp"],
		season,
		showNoStats: true,
	});
	const home = teams.find((t) => t.tid === matchup.homeTid);
	const away = teams.find((t) => t.tid === matchup.awayTid);
	if (!home || !away || home.disabled || away.disabled) {
		return undefined;
	}

	// The exact same code path the main board's game lines take, so a
	// spread/moneyline/total bet placed from this page validates against the
	// board it was quoted from.
	const todayDay = schedule[0]?.day ?? 0;
	const pricer = await buildGameLinePricer({
		activeTeams: teams.filter((t) => !t.disabled) as any,
		season,
		todayDay,
	});
	const main = pricer.priceGame(matchup);

	let simmed: SimmedGame | undefined;
	try {
		simmed = await simGameOutcomes({
			gid,
			homeTid: home.tid,
			awayTid: away.tid,
			neutralSite: main?.neutralSite ?? false,
			daysInFuture: Math.max(0, matchup.day - todayDay),
		});
	} catch (error) {
		console.error("Failed to simulate game for props", error);
	}
	if (!simmed) {
		return undefined;
	}

	const numSims = simmed.numSims;
	const [homeSim, awaySim] = main
		? [
				centredOn(simmed.teams[0], (main.expectedTotal + main.margin) / 2),
				centredOn(simmed.teams[1], (main.expectedTotal - main.margin) / 2),
			]
		: simmed.teams;
	const side = (t: typeof home, simTeam: SimmedTeam) => ({
		tid: t.tid,
		abbrev: t.abbrev,
		region: t.region,
		name: t.name,
		players: playerField(simTeam).map((p) => playerRow(p, t.abbrev, numSims)),
		teamProps: teamPropRows(simTeam),
	});

	return {
		gid,
		home: side(home, homeSim),
		away: side(away, awaySim),
		// How often the simulated game actually needed an extra period, rather
		// than a guess at how often a projected margin lands near zero.
		overtime: priceOdds(eventProb(simmed.overtimes, simmed.numSims)),
		main: main
			? {
					moneyline: main.moneyline,
					spread: main.spread,
					total: main.total,
				}
			: undefined,
	};
};
