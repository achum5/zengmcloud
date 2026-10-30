import { PLAYER } from "../../../common/constants.ts";
import {
	recordSeasonLook,
	type PlayerAppearance,
} from "../../../common/playerAppearance.ts";
import type { PlayerWithoutKey } from "../../../common/types.ts";
import { g } from "../../util/index.ts";
import {
	ageFace,
	familySeed,
	fixBrownHairOnBlackFace,
} from "../../util/realisticFaces.ts";
import { playerInScope } from "./applyFaceAgingToLeague.ts";

// ONE PRESEASON FOR ONE FACE.
//
// In this order, which is what keeps one answer per season (see
// common/playerAppearance.ts):
//
//  1. The season that just ended goes into the history exactly as it looked -
//     p.face, which was that season's face all season, including anything an
//     editor did to it along the way.
//  2. Then the face moves on a year, and p.face is the new season's face.
//
// A retired player is never aged: once he is done, that is how he stays. He
// is not on the preseason list in the first place; this says so out loud for
// anyone who calls it from somewhere else.
//
// Returns whether the player needs writing.
export const agePlayerFace = (
	p: PlayerWithoutKey,
	newSeason: number,
): boolean => {
	if (
		__SPORT !== "basketball" ||
		!g.get("faceAging") ||
		!p.face ||
		p.tid === PLAYER.RETIRED ||
		!playerInScope(p, g.get("faceAgingPlayers"))
	) {
		return false;
	}

	const lastSeason = newSeason - 1;
	const appearances = recordSeasonLook({
		appearances: p.appearances,
		season: lastSeason,
		look: { face: p.face, imgURL: p.imgURL },
		// Where an empty history starts: he has looked like this since he came
		// into the league, as far as anyone knows.
		firstSeason:
			p.draft.year > 0 && p.draft.year <= lastSeason
				? p.draft.year
				: lastSeason,
	});
	if (appearances) {
		p.appearances = appearances;
	}

	// Baldness runs in families, so a player with a father, a son or a brother
	// in the league is judged against the family's seed rather than his own.
	ageFace(
		p.face,
		newSeason - p.born.year,
		p.pid,
		undefined,
		familySeed(p.pid, p.relatives),
	);
	return true;
};

// THE BROWN HAIR, TAKEN BACK OUT OF LEAGUES THAT ALREADY HAVE IT.
//
// Realistic faces used to give a third of Black players a dark brown that
// rendered as plainly brown hair (see HAIR_TONES). New faces no longer get
// it; this corrects the ones already made, every season of them, so the
// gallery does not show a man's hair going black one year. Only players the
// game made up - a real player's face may have been picked by hand from his
// photo, and is left exactly as it is.
//
// Returns whether anything changed.
export const fixBrownHairOnBlackPlayer = (p: PlayerWithoutKey): boolean => {
	if (
		__SPORT !== "basketball" ||
		!g.get("realisticFaces") ||
		!p.face ||
		!playerInScope(p, "fictional")
	) {
		return false;
	}

	let changed = fixBrownHairOnBlackFace(p.face);

	if (Array.isArray(p.appearances)) {
		const fixed = p.appearances.map((entry): PlayerAppearance => {
			if (entry.face && fixBrownHairOnBlackFace(entry.face)) {
				changed = true;
			}
			return entry;
		});
		p.appearances = fixed;
	}

	return changed;
};
