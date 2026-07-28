import { g } from "../util/index.ts";
import type { UpdateEvents } from "../../common/types.ts";
import {
	buildMatchups,
	getPoolAndTeams,
	type EightyTwoZeroEra,
	type EightyTwoZeroMatchup,
	type EightyTwoZeroPosition,
} from "../core/trivia/eightyTwoZero.ts";

// 82-0: everything the game needs to deal a round, sent once.
//
// The board is small - a few hundred franchise-era pairs - so the slot machine
// runs in the UI off this list and a roll is instant. The heavy part, the list
// of players a given round can actually offer, is fetched per round via the
// trivia82Options API rather than shipped up front: every combination at once
// would be most of the league's history serialized into one payload.

export type Trivia82Team = {
	tid: number;
	abbrev: string;
	region: string;
	name: string;
	imgURL?: string;
};

export type Trivia82Data = {
	teams: Trivia82Team[];
	eras: EightyTwoZeroEra[];
	matchups: Record<EightyTwoZeroPosition, EightyTwoZeroMatchup[]>;
};

const updateTrivia82 = async (inputs: unknown, updateEvents: UpdateEvents) => {
	// A draft in progress shouldn't be reshuffled by a game simming in the
	// background, so this is built once and left alone.
	if (updateEvents.includes("firstRun")) {
		let data: Trivia82Data | undefined;
		try {
			const { pool, tids, eras } = await getPoolAndTeams();
			const teamInfoCache = g.get("teamInfoCache");
			data = {
				teams: tids.map((tid) => {
					const info = teamInfoCache[tid];
					return {
						tid,
						abbrev: info?.abbrev ?? `T${tid}`,
						region: info?.region ?? "",
						name: info?.name ?? "",
						imgURL: info?.imgURL,
					};
				}),
				eras,
				matchups: buildMatchups(pool, tids, eras),
			};
		} catch (error) {
			console.error("82-0 setup failed", error);
			data = undefined;
		}

		return {
			data,
			season: g.get("season"),
			godMode: g.get("godMode"),
		};
	}
};

export default updateTrivia82;
