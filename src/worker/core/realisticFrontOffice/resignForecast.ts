import type { Player } from "../../../common/types.ts";
import { g } from "../../util/index.ts";
import moodInfo from "../player/moodInfo.ts";
import {
	finishedComponent,
	hypeRaw,
	loyaltyRaw,
	teamPerformanceRaw,
} from "../player/moodComponents.ts";
import { MOOD_SLOPE } from "./tradePenalty.ts";

// THE RE-SIGNING FORECAST, on vanilla's rules (docs/smart-cpu.md, League 12).
// When his contract runs out, a player re-signs with his team only if he is
// willing, and the game decides that from his mood toward it then
// (moodInfo): its record that season, its hype after it, his seasons with it,
// and the rest. Read off his mood now, a bad team's young players were
// forecast to leave at the end of deals with years still to run - as if the
// team would lose as it is losing now until then - so they were worth little
// to it and it sold them: in League 12 Andrew Wiggins (22, 70/79), D'Angelo
// Russell, Michael Carter-Williams and Giannis went for spare parts, and on
// the 2018 draft's file John Collins (21, 59/71) stood 0% to re-sign with
// WAS, Zach LaVine 1% with PHO, Jayson Tatum 22% with IND. In League 7
// rookie-deal players on teams under 40% wins were forecast 49% and 83%
// re-signed. And a team's own draft picks were forecast certain (the game
// can't let a pick refuse his rookie deal) until the draft was over.
// So his mood is forecast for the season his contract runs out: the team's
// record then, as the outlook expects it (drifting back toward the middle,
// as finishes do) and as far off as it may land, its hype built up from its
// seasons till then, and his seasons with it by then - the rest of his mood as
// it is now. A move that makes the team better by then makes him likelier to
// stay, and one that makes it worse less (losingShifts).

// What his mood is made of now (moodInfo), with the parts that move with the
// team's seasons.
export type MoodNow = {
	// moodInfo's sumAndStuff.
	sum: number;
	// The parts counted in it now, as counted (finishedComponent).
	teamPerformance: number;
	hype: number;
	loyalty: number;
	// Seasons with the team so far, and whether he is its player (loyalty's
	// re-signing part).
	seasons: number;
	own: boolean;
	// Willing whatever his mood by the league's rules: rookies finishing their
	// deals can't refuse, or no player refuses.
	sure: boolean;
	moodTraits: Player["moodTraits"];
};

// His mood toward team tid now, as if he were its player (as his team's
// offer would find him), and his chance of being willing on it now - by his
// mood alone, not the rule that a pick just drafted can't refuse.
export const moodNow = async (
	p: Player,
	tid: number,
): Promise<{ mood: MoodNow; now: number }> => {
	const own = p.tid === tid ? p : { ...p, tid };
	const info = await moodInfo(own, tid);
	const c = info.components;
	const sure =
		!g.get("playersRefuseToNegotiate") ||
		(c.rookieContract > 0 && !g.get("rookiesCanRefuse"));
	const mood: MoodNow = {
		sum: info.sumAndStuff,
		teamPerformance: c.teamPerformance,
		hype: c.hype,
		loyalty: c.loyalty,
		seasons: own.stats.filter((row) => row.tid === tid).length,
		own: true,
		sure,
		moodTraits: own.moodTraits,
	};
	return {
		mood,
		now: sure ? 1 : 1 / (1 + Math.exp(-MOOD_SLOPE * info.sumAndStuff)),
	};
};

// The team's seasons from the outlook's first to the one his contract runs
// out in.
export type SeasonsAhead = {
	// Its expected share of games won each season.
	winp: readonly number[];
	// How far from that it may land, typically (a standard deviation).
	spread: readonly number[];
	// Games each season that still move its hype (the game moves it from a
	// team's sixth game on).
	games: readonly number[];
	// Its hype now, and its share of games won in its last two seasons
	// played, older first: hype moves by a season against those before it.
	hypeNow: number;
	past: readonly [number, number];
	// The share of games won that makes the playoffs, about (for hype).
	playoffLine?: number;
};

// Five points of a standard normal and their weights (Gauss-Hermite).
const NODES = [
	-2.856970013872806, -1.355626179974266, 0, 1.355626179974266,
	2.856970013872806,
];
const WEIGHTS = [
	0.011257411327721, 0.222075922005613, 0.533333333333333, 0.222075922005613,
	0.011257411327721,
];

const clamp01 = (x: number) => Math.min(1, Math.max(0, x));

// Hype after a season at this share of games won, from hype before it: the
// game's per-game rule (writeTeamStats) summed over the season, and 0.05 for
// making the playoffs (newSchedulePlayoffsDay), by how likely that is at
// this share - the line `playoffLine` (none, no playoffs counted). In League
// 12 teams that made the playoffs ended their seasons 0.065 above the
// per-game rule alone.
export const hypeAfter = (
	hype: number,
	winp: number,
	winpOld: number,
	games: number,
	playoffLine?: number,
) =>
	clamp01(
		hype +
			games * (0.01 * (winp - 0.55) + 0.015 * (winp - winpOld)) +
			(playoffLine === undefined
				? 0
				: 0.05 / (1 + Math.exp(-(winp - playoffLine) / PLAYOFF_LINE_WIDTH))),
	);

// How sharp the playoff line is, in share of games won (League 12: of 270
// team-seasons, by most likelihood, 0.012).
const PLAYOFF_LINE_WIDTH = 0.012;

// His chance of being willing to re-sign with team tid when his contract
// runs out at the end of season e of `ahead` (0 for the outlook's first),
// `added` seasons with it from now.
export const willingAtExpiry = (
	mood: MoodNow,
	tid: number,
	e: number,
	ahead: SeasonsAhead,
	added: number,
) => {
	if (mood.sure) {
		return 1;
	}
	const p = { moodTraits: mood.moodTraits };
	// Everything but the parts that move with the team's seasons.
	const rest = mood.sum - mood.teamPerformance - mood.hype - mood.loyalty;
	const loyalty = finishedComponent(
		"loyalty",
		loyaltyRaw(mood.seasons + Math.max(0, added), mood.own),
		p,
		tid,
	);
	// Hype going into his last season: built up season by season.
	let hype = ahead.hypeNow;
	let older = ahead.past[0];
	let old = ahead.past[1];
	for (let y = 0; y < e; y++) {
		const w = ahead.winp[y]!;
		hype = hypeAfter(
			hype,
			w,
			(older + old) / 2,
			ahead.games[y]!,
			ahead.playoffLine,
		);
		older = old;
		old = w;
	}
	const mean = ahead.winp[e]!;
	const spread = ahead.spread[e]!;
	let willing = 0;
	for (let k = 0; k < NODES.length; k++) {
		const w = clamp01(mean + NODES[k]! * spread);
		const h = hypeAfter(
			hype,
			w,
			(older + old) / 2,
			ahead.games[e]!,
			ahead.playoffLine,
		);
		const sum =
			rest +
			loyalty +
			finishedComponent("teamPerformance", teamPerformanceRaw(w), p, tid) +
			finishedComponent("hype", hypeRaw(h), p, tid);
		willing += WEIGHTS[k]! / (1 + Math.exp(-MOOD_SLOPE * sum));
	}
	return willing;
};
