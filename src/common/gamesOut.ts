import type { GamesOut } from "./types.ts";

// An injury's games on the NBA calendar, as the game writes them beside its
// days: "5 games", "1 game", "12+ games" (more may come), "~19 games" (an
// offseason estimate).
export const gamesOutText = ({ n, more, about }: GamesOut) =>
	`${about ? "~" : ""}${n}${more ? "+" : ""} ${n === 1 && !more ? "game" : "games"}`;

// "9 days (5 games)"; just "9 days" without a count.
export const daysAndGamesText = (days: number, gamesOut?: GamesOut) =>
	`${days} ${days === 1 ? "day" : "days"}${
		gamesOut ? ` (${gamesOutText(gamesOut)})` : ""
	}`;

// For a table cell: "5", "12+", "~19".
export const gamesOutCell = ({ n, more, about }: GamesOut) =>
	`${about ? "~" : ""}${n}${more ? "+" : ""}`;

export const sameGamesOut = (a?: GamesOut, b?: GamesOut) =>
	a?.n === b?.n && !!a?.more === !!b?.more && !!a?.about === !!b?.about;
