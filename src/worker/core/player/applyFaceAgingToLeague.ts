// Retroactively age every existing player's face.
//
// Face aging only affects faces generated after the setting goes on, so an
// established league keeps a roster of players who were drawn at whatever age
// they happened to be created and never changed since - 34-year-olds with
// rookie faces, and prospects still wearing the mutton chops the old uniform
// generator handed them. This replays each career that already happened, so a
// league that turns the setting on years in looks the way it would have if it
// had been on from the start.
//
// The result is stable: the balding and beard traits come from the player's
// id, so running this twice does not produce two different men.

import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import {
	applyFaceAgingHistory,
	inferRaceFromFace,
} from "../../util/realisticFaces.ts";
import { recordAppearance } from "../../../common/playerAppearance.ts";
import { helpers } from "../../util/index.ts";

export type FaceAgingScope = "all" | "fictional" | "real";

export const playerInScope = (
	p: { real?: boolean; srID?: string },
	scope: FaceAgingScope,
): boolean => {
	if (scope === "all") {
		return true;
	}
	// Real players carry either the flag or a Sports Reference id; anything
	// else was made up by the game.
	const real = p.real === true || typeof p.srID === "string";
	return scope === "real" ? real : !real;
};

// The age this player was in his first season, which is where the replay
// starts. Draft year is the reliable marker; when it is missing or nonsense
// (an imported roster, a God Mode creation) fall back to a normal draft age,
// clamped so it can never exceed how old he is now.
export const replayStartAge = ({
	draftYear,
	bornYear,
	currentAge,
}: {
	draftYear: number | undefined;
	bornYear: number;
	currentAge: number;
}): number => {
	const fromDraft =
		draftYear !== undefined && draftYear > 0 ? draftYear - bornYear : undefined;
	const start =
		fromDraft !== undefined && fromDraft >= 15 && fromDraft <= 40
			? fromDraft
			: 19;
	return Math.min(start, currentAge);
};

export const applyFaceAgingToLeague = async (
	scope: FaceAgingScope,
): Promise<number> => {
	const season = g.get("season");
	const players = await idb.cache.players.getAll();

	let changed = 0;
	for (const p of players) {
		if (!p.face || !playerInScope(p, scope)) {
			continue;
		}

		const currentAge = season - p.born.year;
		if (!Number.isFinite(currentAge) || currentAge < 0) {
			continue;
		}

		const before = { face: helpers.deepCopy(p.face), imgURL: p.imgURL };
		const rookieAge = replayStartAge({
			draftYear: p.draft?.year,
			bornYear: p.born.year,
			currentAge,
		});

		// Every season the look changed, so the career reads as a history rather
		// than one jump to today's face.
		const changes: { age: number; face: any }[] = [];
		applyFaceAgingHistory({
			face: p.face,
			rookieAge,
			currentAge,
			pid: p.pid,
			race: inferRaceFromFace(p.face),
			onChange: (age) => {
				changes.push({ age, face: helpers.deepCopy(p.face) });
			},
		});

		let appearances = p.appearances;
		for (const change of changes) {
			appearances =
				recordAppearance({
					appearances,
					season: p.born.year + change.age,
					firstSeason: p.born.year + rookieAge,
					look: { face: change.face, imgURL: p.imgURL },
					previous: before,
				}) ?? appearances;
		}
		if (appearances) {
			p.appearances = appearances;
		}

		await idb.cache.players.put(p);
		changed += 1;
	}

	return changed;
};
