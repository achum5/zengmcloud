// RESULTS NOT TO SHOW YET.
//
// The score bar and the ticker are on every page, so a game playing out live -
// this device's own, or a league-mate's broadcast - must not have its final
// in either until it has played out. That used to rest entirely on one flag
// (liveGameInProgress) freezing both bars, and the flag has many hands on it:
// a live page closing, a reconnect, a watch walked out of, a result arriving
// before its broadcast. Any of them dropping it for a moment let the bars
// repaint with the final, and the bar then froze WITH the spoiler in it.
//
// So the data feeding the bars leaves those games out as well: a game in this
// set reads as not played yet, whatever the flag is doing. The sync layer says
// which games those are (see connect.ts - its own module, like liveWatchGate,
// to keep the bars' code free of the sync imports).

import type { LocalStateUI } from "../../common/types.ts";

//
// Two kinds, because they end differently (see util/liveGameGate.ts):
// `watching` - this device's own live game, the one it is watching or being
// asked about, a result held for a broadcast on its way - stays out until this
// screen has played it out; `room` - every other game a league mate is
// playing live - until it is over on theirs, or (regular season) a day has
// been simmed past it.
export type LiveSpoilerSets = {
	watching: ReadonlySet<number>;
	room: ReadonlySet<number>;
};

let provider: (() => LiveSpoilerSets) | undefined;

export const setLiveSpoilerProvider = (
	fn: (() => LiveSpoilerSets) | undefined,
) => {
	provider = fn;
};

const NONE: ReadonlySet<number> = new Set();
const NO_SETS: LiveSpoilerSets = { watching: NONE, room: NONE };

export const liveSpoilerSets = (): LiveSpoilerSets => {
	try {
		return provider?.() ?? NO_SETS;
	} catch {
		return NO_SETS;
	}
};

export const liveSpoilerGids = (): ReadonlySet<number> => {
	const { watching, room } = liveSpoilerSets();
	if (room.size === 0) {
		return watching;
	}
	if (watching.size === 0) {
		return room;
	}
	return new Set([...watching, ...room]);
};

type BarGame = LocalStateUI["games"][number];

// A game still playing out, as the bar shows one not played yet: no score, no
// overtime, and the series standing as it was going in (a game row stores it
// as it stood after - this game's win already counted).
const unplayed = (game: BarGame): BarGame => {
	const [a, b] = game.teams;
	const winner =
		a.pts === undefined || b.pts === undefined || a.pts === b.pts
			? undefined
			: a.pts > b.pts
				? 0
				: 1;
	return {
		...game,
		overtimes: 0,
		teams: game.teams.map((t, i) => ({
			...t,
			pts: undefined,
			sPts: undefined,
			playoffs: t.playoffs
				? {
						...t.playoffs,
						won: Math.max(0, t.playoffs.won - (winner === i ? 1 : 0)),
						lost: Math.max(
							0,
							t.playoffs.lost - (winner !== undefined && winner !== i ? 1 : 0),
						),
					}
				: undefined,
		})) as BarGame["teams"],
	};
};

// The bar's games with any still playing out shown as not played. When one is,
// nothing after it goes on the bar either: the next game's series record would
// give away who won.
export const hideLiveResults = (
	games: BarGame[],
	hidden: ReadonlySet<number> = liveSpoilerGids(),
): BarGame[] => {
	if (hidden.size === 0) {
		return games;
	}
	const out: BarGame[] = [];
	for (const game of games) {
		if (hidden.has(game.gid)) {
			out.push(unplayed(game));
			break;
		}
		out.push(game);
	}
	return out;
};

// A news story about one of these games: game stories link its box score.
const GAME_LINK = /game_log\/[^"/]+\/\d+\/(\d+)/g;

export const mentionsHiddenGame = (
	text: string | undefined,
	hidden: ReadonlySet<number> = liveSpoilerGids(),
): boolean => {
	if (!text || hidden.size === 0) {
		return false;
	}
	for (const m of text.matchAll(GAME_LINK)) {
		if (hidden.has(Number(m[1]))) {
			return true;
		}
	}
	return false;
};
