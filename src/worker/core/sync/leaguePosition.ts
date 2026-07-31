import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";

// How far along the league is, read from the DATA rather than from the sync
// engine's own bookkeeping.
//
// This exists because the engine's answer to "am I caught up?" is only as good
// as what it saw: it compares its watermark against the highest entry it has
// been handed, so an entry that never arrived leaves it confidently, silently
// behind. A device in that state looks completely normal - it just shows the
// next day as upcoming and waits, forever.
//
// The person in charge of simming stamps this on the authority doc, which every
// device already watches for the busy lease. That gives every follower a second
// opinion that does not come from its own change log, so "the room is on day 45
// and I am on day 44" is answerable without trusting the thing that is broken.
export type LeaguePosition = {
	season: number;
	phase: number;
	// The highest day with a played game this season. 0 before any are played.
	day: number;
};

export const getLeaguePosition = async (): Promise<LeaguePosition> => {
	// Current season only, which is what the cache holds.
	const games = await idb.cache.games.getAll();
	let day = 0;
	for (const game of games) {
		// Older games predate the day field.
		if (game.day !== undefined && game.day > day) {
			day = game.day;
		}
	}
	return {
		season: g.get("season"),
		phase: g.get("phase"),
		day,
	};
};

// Is `local` strictly behind `other`? (season, phase, day) is monotonic - phase
// only moves forward inside a season, and both phase and day reset when the
// season does - so comparing it lexicographically is well defined.
export const isBehindPosition = (
	local: LeaguePosition,
	other: LeaguePosition,
): boolean => {
	if (local.season !== other.season) {
		return local.season < other.season;
	}
	if (local.phase !== other.phase) {
		return local.phase < other.phase;
	}
	return local.day < other.day;
};

export const describePosition = (position: LeaguePosition): string =>
	`${position.season} phase ${position.phase} day ${position.day}`;

// Firestore rejects undefined and we never want a half-filled position, so
// parse defensively - an older client writes no position at all.
export const parseLeaguePosition = (
	value: unknown,
): LeaguePosition | undefined => {
	if (!value || typeof value !== "object") {
		return undefined;
	}
	const { season, phase, day } = value as Record<string, unknown>;
	if (
		typeof season !== "number" ||
		typeof phase !== "number" ||
		typeof day !== "number"
	) {
		return undefined;
	}
	return { season, phase, day };
};
