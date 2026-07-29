// In-progress trivia games, kept across a reload or a trip to another page.
//
// A half-solved grid is real work. Losing it because you tapped a player's page
// - or because iOS reclaimed the tab while you were in another app - is the
// kind of thing that stops people playing at all. So every game writes its live
// state here on each move and reads it back on mount.
//
// Two guards keep a stale save from being applied to the wrong thing:
//
//   - `lid`, because pids and tids mean different players and teams in a
//     different league, so a save from league 2 must never restore into league 3.
//   - `VERSION`, bumped whenever a game's saved shape changes, so an old save
//     is discarded rather than half-read into a new format.
//
// This is per-device UI state, not league data: it doesn't belong in a league
// export and it isn't synced to anyone else.

export type TriviaProgressGame =
	| "grids"
	| "team"
	| "higherLower"
	| "eightyTwoZero";

const VERSION = 1;

type Saved = {
	v: number;
	lid?: number;
	state: unknown;
};

const keyFor = (game: TriviaProgressGame) => `triviaProgress:${game}`;

export const saveProgress = (
	game: TriviaProgressGame,
	lid: number | undefined,
	state: unknown,
) => {
	try {
		localStorage.setItem(
			keyFor(game),
			JSON.stringify({ v: VERSION, lid, state } satisfies Saved),
		);
	} catch {
		// A full or disabled localStorage costs the resume, not the game.
	}
};

export const loadProgress = <T>(
	game: TriviaProgressGame,
	lid: number | undefined,
): T | undefined => {
	try {
		const raw = localStorage.getItem(keyFor(game));
		if (!raw) {
			return undefined;
		}
		const parsed = JSON.parse(raw) as Saved;
		if (
			!parsed ||
			parsed.v !== VERSION ||
			parsed.lid !== lid ||
			parsed.state === null ||
			typeof parsed.state !== "object"
		) {
			return undefined;
		}
		return parsed.state as T;
	} catch {
		return undefined;
	}
};

export const clearProgress = (game: TriviaProgressGame) => {
	try {
		localStorage.removeItem(keyFor(game));
	} catch {}
};
