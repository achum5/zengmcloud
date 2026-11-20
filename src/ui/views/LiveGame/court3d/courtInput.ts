import type { ReplayLooks } from "../../../../common/types.ts";
import type { CourtPlayer, Stakes } from "./director.ts";
import { outHurt } from "./outHurt.ts";

// WHAT THE 3D COURT STAGES A GAME FROM, read off its box score - the roster,
// a playoff game's stakes, the lineups' introduction. Court3D reads them
// here, and so does the staging started ahead of it while a league-mate's
// game is offered (see prestage.ts): the two have to come out the same, to
// the bit, for that staging to be the one the court uses.

// The starting lineups called out before the opening tip (see intro.ts):
// off for now, kept to come back to.
export const LINEUP_INTROS = false;

export const introKindOf = (
	playoffs: unknown,
): "playoffs" | "regular" | undefined =>
	LINEUP_INTROS ? (playoffs ? "playoffs" : "regular") : undefined;

// The box score's two teams, with how they looked that night laid over them
// on a saved replay that kept it.
export const courtTeamsOf = (
	teamsNow: unknown,
	looksThen: ReplayLooks | undefined,
): any[] => {
	const teams: any[] = Array.isArray(teamsNow) ? teamsNow : [];
	return teams.map((t) => {
		const then = looksThen?.teams[t?.tid];
		return then ? { ...t, ...then, court: then.court } : t;
	});
};

export type RosterPlayer = CourtPlayer & {
	name: string;
	jerseyNumber?: string;
};

// Everyone dressed, in display order: [away (attacks left), home].
export const courtRosterOf = (away: any, home: any): RosterPlayer[] => {
	const out: RosterPlayer[] = [];
	const teams: [any, any] = [away, home];
	for (const t of [0, 1] as const) {
		for (const p of teams[t]?.players ?? []) {
			if (typeof p.pid === "number") {
				out.push({
					pid: p.pid,
					team: t,
					pos: p.pos,
					skills: Array.isArray(p.skills) ? p.skills : undefined,
					ratings:
						p.courtRatings && typeof p.courtRatings === "object"
							? p.courtRatings
							: undefined,
					injury:
						p.injury?.newThisGame && typeof p.injury.type === "string"
							? {
									type: p.injury.type,
									games: p.injury.gamesRemaining ?? 0,
								}
							: undefined,
					...(outHurt(p) ? { out: true } : {}),
					name: p.name,
					jerseyNumber: p.jerseyNumber,
				});
			}
		}
	}
	return out;
};

// A playoff game's stakes, as they stood going into it - the box score's
// series records take this game in once it is over, so one won by now is
// taken back out. (Only what it reads of the box score.)
export type SeriesBox = {
	playoffs?: boolean;
	finals?: boolean;
	numGamesToWinSeries?: number;
	gameOver?: boolean;
	won?: { tid?: number };
	teams?: { tid?: number; playoffs?: { won?: number } }[];
};

export const stakesOf = (bs: SeriesBox): Stakes | undefined => {
	if (!bs.playoffs || typeof bs.numGamesToWinSeries !== "number") {
		return undefined;
	}
	const won = (raw: 0 | 1) => {
		const t = bs.teams?.[raw];
		const w = Number(t?.playoffs?.won ?? 0);
		return bs.gameOver && bs.won?.tid === t?.tid ? w - 1 : w;
	};
	// (Raw team 0 is home, side 1.)
	return {
		won: [won(1), won(0)] as [number, number],
		toWin: bs.numGamesToWinSeries,
		finals: !!bs.finals,
	};
};
