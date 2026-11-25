import type {
	RecapDayStandings,
	RecapGame,
	RecapPlayer,
} from "../util/getDayGamesForRecap.ts";
import type { GamesOut } from "../../common/types.ts";

// THE LEAGUE HUB ON THE DAILY SCHEDULE: a night's best stat lines, who got
// hurt and where the standings stood - all read off the same recap data the
// night's written recaps come from, so the page and the prose tell one story. Nothing new is decided here; it only picks what to show.

export type HubPerformer = {
	pid: number;
	name: string;
	tid: number;
	oppTid: number;
	gid: number;
	home: boolean;
	won: boolean;
	pts: number;
	trb: number;
	ast: number;
	stl: number;
	blk: number;
	tp: number;
	// The final, his team's score first.
	score: [number, number];
	overtimes: number;
};

export type HubInjury = {
	pid: number;
	name: string;
	tid: number;
	type: string;
	gamesRemaining: number;
	// On the NBA calendar: its days and the games they take in.
	daysRemaining?: number;
	gamesOut?: GamesOut;
};

export type DailyScheduleHub = {
	performers: HubPerformer[];
	injuries: HubInjury[];
	standings?: RecapDayStandings;
};

// Hollinger's game score, from what a recap line keeps (total rebounds, so
// offensive and defensive are weighed alike): the usual way a box score picks
// its best line.
const gameScore = (p: RecapPlayer) =>
	p.pts +
	0.4 * p.fg -
	0.7 * p.fga -
	0.4 * (p.fta - p.ft) +
	0.5 * p.reb +
	p.stl +
	0.7 * p.ast +
	0.7 * p.blk -
	0.4 * p.pf -
	p.tov;

const NUM_PERFORMERS = 5;
const NUM_INJURIES = 6;

export const buildHub = ({
	games,
	standings,
}: {
	games: RecapGame[];
	standings?: RecapDayStandings;
}): DailyScheduleHub => {
	const performers: (HubPerformer & { gmsc: number })[] = [];
	const injuries: HubInjury[] = [];
	for (const game of games) {
		if (game.allStar) {
			continue;
		}
		for (const [i, t] of game.teams.entries()) {
			const opp = game.teams[i === 0 ? 1 : 0];
			for (const p of t.players) {
				if (p.min > 0) {
					performers.push({
						pid: p.pid,
						name: p.name,
						tid: t.tid,
						oppTid: opp.tid,
						gid: game.gid,
						home: i === 0,
						won: game.winnerTid === t.tid,
						pts: p.pts,
						trb: p.reb,
						ast: p.ast,
						stl: p.stl,
						blk: p.blk,
						tp: p.tp,
						score: [t.pts, opp.pts],
						overtimes: game.overtimes,
						gmsc: gameScore(p),
					});
				}
				if (p.injury?.newThisGame && p.injury.gamesRemaining > 0) {
					injuries.push({
						pid: p.pid,
						name: p.name,
						tid: t.tid,
						type: p.injury.type,
						gamesRemaining: p.injury.gamesRemaining,
						...(p.injury.daysRemaining !== undefined
							? { daysRemaining: p.injury.daysRemaining }
							: {}),
						...(p.injury.gamesOut ? { gamesOut: p.injury.gamesOut } : {}),
					});
				}
			}
		}
	}

	performers.sort((a, b) => b.gmsc - a.gmsc);
	injuries.sort((a, b) => b.gamesRemaining - a.gamesRemaining);

	return {
		performers: performers.slice(0, NUM_PERFORMERS).map(({ gmsc, ...p }) => p),
		injuries: injuries.slice(0, NUM_INJURIES),
		standings,
	};
};
