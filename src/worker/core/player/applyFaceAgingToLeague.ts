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
// The traits that decide a career - whether and when a man balds, whether he
// grows a beard and how far he takes it, when his lines come in - come from
// his id, so running this twice gives the same kind of man. The exact styles
// drawn along the way are rolled fresh each run.
//
// Retired players are left alone: a face stops at retirement.

import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import {
	applyFaceAgingHistory,
	familySeed,
	inferRaceFromFace,
} from "../../util/realisticFaces.ts";
import {
	recordSeasonLook,
	type PlayerAppearance,
} from "../../../common/playerAppearance.ts";
import { PLAYER } from "../../../common/constants.ts";
import type { FaceAgingScope } from "../../../common/types.ts";

export type { FaceAgingScope } from "../../../common/types.ts";

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
	{
		// Keep the seasons the look changed as appearance history. Off at
		// league creation, where the years being replayed were never played.
		record = true,
		// Leave alone anyone with no years to replay. A face generated under the
		// mode is already right for its age; re-rolling it moves the man for
		// nothing.
		skipCurrent = false,
	}: { record?: boolean; skipCurrent?: boolean } = {},
): Promise<number> => {
	const season = g.get("season");
	const players = await idb.cache.players.getAll();

	let changed = 0;
	for (const p of players) {
		// Once a player retires, his face is done. Nothing replays it.
		if (!p.face || p.tid === PLAYER.RETIRED || !playerInScope(p, scope)) {
			continue;
		}

		const currentAge = season - p.born.year;
		if (!Number.isFinite(currentAge) || currentAge < 0) {
			continue;
		}

		const rookieAge = replayStartAge({
			draftYear: p.draft?.year,
			bornYear: p.born.year,
			currentAge,
		});
		if (skipCurrent && currentAge <= rookieAge) {
			continue;
		}

		// The face every season of the replayed career wore, so the gallery
		// has one for each of them (see common/playerAppearance.ts). The
		// replay is a new career from the draft on, so the history it leaves
		// is rebuilt from scratch rather than laid over the old one.
		let appearances: PlayerAppearance[] | undefined;
		applyFaceAgingHistory({
			face: p.face,
			rookieAge,
			currentAge,
			pid: p.pid,
			familyPid: familySeed(p.pid, p.relatives),
			race: inferRaceFromFace(p.face),
			onYear: (age) => {
				const playedSeason = p.born.year + age;
				if (!record || playedSeason >= season) {
					return;
				}
				appearances =
					recordSeasonLook({
						appearances,
						season: playedSeason,
						look: { face: p.face, imgURL: p.imgURL },
					}) ?? appearances;
			},
		});

		if (record) {
			if (appearances) {
				p.appearances = appearances;
			} else {
				delete p.appearances;
			}
		}

		await idb.cache.players.put(p);
		changed += 1;
	}

	return changed;
};
