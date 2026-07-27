import { idb } from "../../db/index.ts";
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
import { isSport } from "../../../common/sportFunctions.ts";
import { probToAmerican } from "../../../common/sportsbook.ts";
import {
	eventProb,
	sampleMean,
	smoothedOverProb,
	toHalfPointLine,
} from "../../../common/sportsbookOdds.ts";

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

const priceOdds = (prob: number) => probToAmerican(prob);

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
	],
	doubleDouble: priceOdds(eventProb(p.dd, numSims)),
	tripleDouble: priceOdds(eventProb(p.td, numSims)),
});

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
	}));

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

	const schedule = await getSchedule();
	const matchup = schedule.find((gm) => gm.gid === gid);
	if (!matchup || matchup.homeTid < 0 || matchup.awayTid < 0) {
		return undefined;
	}

	const teams = await idb.getCopies.teamsPlus(
		{
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
		},
		"noCopyCache",
	);
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
		home: side(home, simmed.teams[0]),
		away: side(away, simmed.teams[1]),
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
