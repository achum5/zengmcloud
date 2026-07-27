import { bySport } from "../../../common/sportFunctions.ts";
import getAwardCandidatesBaseball from "./getAwardCandidates.baseball.ts";
import getAwardCandidatesBasketball from "./getAwardCandidates.basketball.ts";
import getAwardCandidatesFootball from "./getAwardCandidates.football.ts";
import getAwardCandidatesHockey from "./getAwardCandidates.hockey.ts";

const getAwardCandidates = (
	season: number,
	// Pass PROJECTED players to rank the field by where the season is heading
	// rather than by partial cumulative totals. See getAwardRaceOdds.
	playersOverride?: any[],
): Promise<
	{
		asterisk?: string;
		name: string;
		players: any[];
		stats: string[];
	}[]
> => {
	return bySport({
		baseball: getAwardCandidatesBaseball(season, playersOverride),
		basketball: getAwardCandidatesBasketball(season, playersOverride),
		football: getAwardCandidatesFootball(season, playersOverride),
		hockey: getAwardCandidatesHockey(season, playersOverride),
	});
};

export default getAwardCandidates;
