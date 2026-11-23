import { spreadLine } from "../../components/LeagueHub/spreadLine.ts";
import { recordText, seriesLine, seriesWins } from "./gamecast/seriesLine.ts";
import type { NationalTag } from "../../../common/types.ts";
import type { GamecastInfo } from "../../../worker/views/liveGameInfo.ts";

export type MatchupTeam = {
	tid: number;
	abbrev: string;
	region?: string;
	name?: string;
	imgURL?: string;
	imgURLSmall?: string;
	won?: number;
	lost?: number;
	tied?: number;
	otl?: number;
	playoffs?: { seed: number };
};

export type MatchupBoxScore = {
	gid: number;
	playoffs?: boolean;
	exhibition?: boolean;
	// Box score order: home first.
	teams: [MatchupTeam, MatchupTeam];
	// The pregame line, home's side (Game.spread).
	spread?: number;
	gamecast?: GamecastInfo;
	national?: NationalTag;
};

// The regular season's meetings so far: "CLE leads season series 2-1",
// "Season series tied 1-1" - nothing before they've met.
export const seasonSeriesText = (
	info: GamecastInfo | undefined,
	// Away first.
	teams: [MatchupTeam, MatchupTeam],
): string | undefined => {
	if (!info || info.series.length === 0) {
		return undefined;
	}
	const w = seriesWins(info, teams[0].tid, teams[1].tid, true);
	if (w[0] + w[1] === 0) {
		return undefined;
	}
	return w[0] === w[1]
		? `Season series tied ${w[0]}-${w[1]}`
		: recordText(w, teams).replace(" leads ", " leads season series ");
};

// THE LIVE GAME HEADER'S FACTS (see MatchupLine), under the teams: the
// spread, the over/under, and the series - a playoff game's, or the season's.
export const matchupFacts = (boxScore: MatchupBoxScore): string[] => {
	const [home, away] = boxScore.teams;
	const facts: string[] = [];
	const spread = spreadLine(boxScore.spread, home.abbrev, away.abbrev);
	if (spread) {
		facts.push(spread);
	}
	const total = boxScore.gamecast?.total;
	if (total !== undefined) {
		facts.push(`O/U ${total}`);
	}
	// (A play-in game: the seeds are by the teams already.)
	const series = !boxScore.playoffs
		? seasonSeriesText(boxScore.gamecast, [away, home])
		: boxScore.gamecast?.playIn
			? "Play-In"
			: seriesLine(boxScore.gamecast, true, [away, home]);
	if (series) {
		facts.push(series);
	}
	return facts;
};
