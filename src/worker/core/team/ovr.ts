import { bySport } from "../../../common/sportFunctions.ts";
import {
	injuryAtGame,
	type PlayThroughDays,
} from "../../../common/playThroughInjuriesFactor.ts";
import type { PlayerInjury } from "../../../common/types.ts";
import * as ovrBaseball from "./ovr.baseball.ts";
import * as ovrBasketball from "./ovr.basketball.ts";
import * as ovrFootball from "./ovr.football.ts";
import * as ovrHockey from "./ovr.hockey.ts";

type Player = {
	injury: PlayerInjury;
	ratings: {
		ovr: number;
		ovrs?: Record<string, number> | undefined;
		pos: string;
	};
	value: number;
};

type Options = {
	// When defined, only healthy players and players playing through injury are included
	accountForInjuredPlayers?: {
		// Classic days (injury ticks) before the game.
		numDaysInFuture: number;
		playThroughInjuries: [number, number];
		// On the NBA calendar, the user's setting in days, and the calendar
		// days counted down before the game (see injuryAtGame).
		byDays?: PlayThroughDays;
		calendarDaysAhead?: number;
	};
	onlyPos?: string;
	playoffs?: boolean;
	rating?: string;
	wholeRoster?: boolean;
};

// pos is used for position-specific rankings
// wholeRoster=true is used for computing team value of the whole roster, like for determining who to draft or sign
const ovr = (players: Player[], options: Options = {}) => {
	let players2;
	if (options.accountForInjuredPlayers) {
		const { numDaysInFuture, playThroughInjuries, byDays, calendarDaysAhead } =
			options.accountForInjuredPlayers;
		const currentPlayThroughInjuries =
			playThroughInjuries[options.playoffs ? 1 : 0];
		const ahead = { games: numDaysInFuture, days: calendarDaysAhead ?? 0 };

		players2 = [];
		for (const p of players) {
			const { healthy, plays, factor } = injuryAtGame(
				p.injury,
				currentPlayThroughInjuries,
				byDays,
				ahead,
			);
			if (healthy) {
				players2.push(p);
			} else if (plays) {
				// Adjust for playing through injury
				const p2 = { ...p };
				p2.ratings = { ...p2.ratings };
				p2.ratings.ovr *= factor;
				if (p2.ratings.ovrs) {
					for (const key of Object.keys(p2.ratings.ovrs)) {
						p2.ratings.ovrs[key]! *= factor;
					}
				}
				players2.push(p2);
			}
		}
	} else {
		players2 = players;
	}

	return bySport<(players: Player[], options: Options) => number>({
		baseball: ovrBaseball.ovr,
		basketball: ovrBasketball.ovr,
		football: ovrFootball.ovr,
		hockey: ovrHockey.ovr,
	})(players2, options);
};

export default ovr;

export const prepareWholeRoster = bySport({
	baseball: ovrBaseball.prepareWholeRoster,
	basketball: undefined,
	football: ovrFootball.prepareWholeRoster,
	hockey: ovrHockey.prepareWholeRoster,
});
