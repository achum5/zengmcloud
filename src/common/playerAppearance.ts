// A PLAYER'S LOOK, SEASON BY SEASON.
//
// `p.face` and `p.imgURL` are what a player looks like NOW. Once faces age
// (see worker/util/realisticFaces.ts) "now" stops being the whole story: the
// 34-year-old with a receding hairline and a full beard was a clean-shaven
// rookie, and a 2011 box score showing his 2026 head is wrong in the same way
// showing his 2026 ratings would be.
//
// STORED ON CHANGE, NOT PER SEASON, and that distinction is the difference
// between a feature and a bloated save file. Storing a face every season would
// be ~20 copies per player per career - on a 500-player league, megabytes of
// duplicated JSON re-uploaded to every device every season, almost all of it
// identical to the season before. Aging only changes a face in about 8% of
// seasons, so instead each entry records "this is the look FROM this season
// onward" and a season resolves to the most recent entry at or before it.
// Same answer for every season, a couple of entries per career instead of
// twenty.
//
// An empty history is the normal state and means "always looked like p.face",
// so every existing player and every single-player league carries no extra
// weight at all.

import type { FaceConfig } from "facesjs";

export type PlayerAppearance = {
	// The first season this look applies to. It stays in effect until the next
	// entry's season, or forever if it is the last one.
	season: number;
	face?: FaceConfig;
	imgURL?: string;
};

type PlayerLike = {
	face?: FaceConfig;
	imgURL?: string;
	appearances?: PlayerAppearance[];
};

// What this player looked like in the given season: the newest entry that had
// already taken effect by then. Falls back to the player's current look, which
// is what every player without a history has.
export const appearanceForSeason = (
	p: PlayerLike,
	season: number | undefined,
): { face?: FaceConfig; imgURL?: string } => {
	const current = { face: p.face, imgURL: p.imgURL };
	const history = p.appearances;
	if (season === undefined || !Array.isArray(history) || history.length === 0) {
		return current;
	}

	let match: PlayerAppearance | undefined;
	for (const entry of history) {
		if (entry.season <= season && (!match || entry.season > match.season)) {
			match = entry;
		}
	}

	if (!match) {
		// Before the first recorded look - the earliest one is the closest thing
		// to the truth, and certainly closer than today's face.
		let earliest = history[0]!;
		for (const entry of history) {
			if (entry.season < earliest.season) {
				earliest = entry;
			}
		}
		return { face: earliest.face, imgURL: earliest.imgURL };
	}

	return { face: match.face, imgURL: match.imgURL };
};

// Do two looks differ in any way that would show on screen? Compared by value
// because a face is a plain data object; this is what keeps an unchanged
// season from being written at all.
export const appearancesDiffer = (
	a: { face?: FaceConfig; imgURL?: string },
	b: { face?: FaceConfig; imgURL?: string },
): boolean =>
	(a.imgURL ?? "") !== (b.imgURL ?? "") ||
	JSON.stringify(a.face ?? null) !== JSON.stringify(b.face ?? null);

// Record what the player looks like from `season` onward, if that is not
// already what the history says. Returns the new history, or undefined when
// nothing needed writing - so a caller can skip saving the player entirely.
//
// `firstSeason` seeds the history the first time a look changes: without it
// the record would claim the player always looked the way he does today,
// which is exactly the thing being fixed.
export const recordAppearance = ({
	appearances,
	season,
	firstSeason,
	look,
	previous,
}: {
	appearances: PlayerAppearance[] | undefined;
	season: number;
	firstSeason: number;
	look: { face?: FaceConfig; imgURL?: string };
	// The look being replaced, used only to seed an empty history.
	previous?: { face?: FaceConfig; imgURL?: string };
}): PlayerAppearance[] | undefined => {
	const history = Array.isArray(appearances) ? [...appearances] : [];

	if (history.length === 0) {
		if (previous === undefined || !appearancesDiffer(look, previous)) {
			// Nothing has changed yet, so there is no history worth keeping.
			return undefined;
		}
		if (firstSeason < season) {
			history.push({
				season: firstSeason,
				face: previous.face,
				imgURL: previous.imgURL,
			});
		}
	} else if (
		!appearancesDiffer(
			look,
			appearanceForSeason({ appearances: history }, season),
		)
	) {
		return undefined;
	}

	const existing = history.findIndex((entry) => entry.season === season);
	const entry = { season, face: look.face, imgURL: look.imgURL };
	if (existing >= 0) {
		history[existing] = entry;
	} else {
		history.push(entry);
	}

	history.sort((a, b) => a.season - b.season);
	return history;
};

// UNDO ONE SEASON'S CHANGE, putting the season (and everything after it, until
// the next recorded change) back to the way the season before it looked.
//
// Faces age on their own now, and aging is a roll: most years it changes
// nothing, and the year it does is occasionally a year you would rather it
// had not - the report that prompted this is a 23-year-old who grew a mustache
// and a pair of flared chops. Everything needed to undo that is already in the
// history, since an entry is exactly "the look from this season onward", so
// reverting is deleting the entry and letting the season fall through to the
// one before.
//
// The LIVE look moves too when the entry being dropped is the one still in
// effect. A history that says a player looks one way while p.face says another
// is the same bug as showing his 2026 head on a 2011 box score, pointed the
// other way - so the current look is recomputed from the history that remains.
//
// Returns undefined when there is nothing to undo: no history, the first
// season of a career (nothing before it), or a season that never changed.
export const revertAppearance = ({
	appearances,
	season,
	current,
	latestSeason,
}: {
	appearances: PlayerAppearance[] | undefined;
	// The season to put back.
	season: number;
	// p.face / p.imgURL - what the player looks like right now.
	current: { face?: FaceConfig; imgURL?: string };
	// The newest season the player has, which is the one the live look belongs
	// to. Anything earlier being reverted leaves the live look alone.
	latestSeason: number;
}):
	| {
			appearances: PlayerAppearance[] | undefined;
			current: { face?: FaceConfig; imgURL?: string };
	  }
	| undefined => {
	const history = Array.isArray(appearances) ? appearances : [];
	if (history.length === 0) {
		return undefined;
	}

	const withCurrent = { ...current, appearances: history };
	const before = appearanceForSeason(withCurrent, season - 1);
	const at = appearanceForSeason(withCurrent, season);
	if (!appearancesDiffer(at, before)) {
		return undefined;
	}


	// Dropping the entry AT this season is what makes it resolve to the one
	// before. A season that changed without an entry of its own cannot happen
	// through recordAppearance, but if it ever did, writing the earlier look in
	// says the same thing.
	const remaining = history.filter((entry) => entry.season !== season);
	if (remaining.length === history.length) {
		remaining.push({ season, face: before.face, imgURL: before.imgURL });
		remaining.sort((a, b) => a.season - b.season);
	}

	const nextCurrent =
		remaining.length > 0
			? appearanceForSeason({ appearances: remaining }, latestSeason)
			: before;

	// A history whose every entry says what the player already looks like is
	// saying nothing - which is the state every player starts in, and the one
	// undoing the only change should get back to. Dropping it keeps a save
	// that has been tidied up as small as one that never changed.
	const stillSaysSomething = remaining.some((entry) =>
		appearancesDiffer(entry, nextCurrent),
	);

	return {
		appearances: stillSaysSomething ? remaining : undefined,
		current: nextCurrent,
	};
};
