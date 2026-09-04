// RENAMING AN AWARD RENAMES ITS HISTORY.
//
// Every season's awards row stores the name and the abbrev its awards were
// decided under, and so does every player's own copy of the award. That is what
// makes a league which renames All-League to All-NBA go on showing All-League
// in every season it has already played: the label was written down at the
// time, and nothing reads the settings again afterwards.
//
// Storing it is right - an award can be edited into something else entirely,
// and a season should keep what it actually awarded. But a name is a label, not
// a result. A league that decides its All-League team is called All-NBA has
// always called it that, so a rename rewrites the label everywhere it was
// stored, and changes nothing else: not who won, not the rank he finished, not
// the formula it was decided by.
//
// The abbrev is what identifies an award in history, because the settings
// enforce that abbrevs are unique within the award list. So the rename is
// keyed on the OLD abbrev, whether or not the abbrev itself is what changed.
//
// This is reversible: renaming back matches the new abbrev and puts the old
// label back.

import fastDeepEqual from "fast-deep-equal";
import { idb } from "../../db/index.ts";
import { normalizeAwardsRow } from "../../db/normalizeAwardsRow.ts";
import type { AwardSettings, PlayerAward } from "../../../common/types.ts";

export type AwardRename = {
	// What the award was called in the seasons already played.
	fromShortName: string;
	toName: string;
	toShortName: string;
	// A team award and an individual award are different things even under the
	// same abbrev, so a rename never crosses between them.
	isTeam: boolean;
};

export type RenameAwardsResult = {
	// Seasons whose awards were relabeled.
	seasons: number;
	// Players whose own copies were relabeled.
	players: number;
};

const withoutNames = ({ name, shortName, ...rest }: AwardSettings[number]) =>
	rest;

// What changed between two versions of the award settings, as renames.
//
// An award keeping its abbrev is the same award by definition, so a changed
// name is a rename. An award whose abbrev changed has to be recognized by its
// slot instead: it is a rename when the award that used to sit there is gone
// from the list and everything about it except the name and the abbrev is
// untouched. Anything less certain than that is left alone - a new award, a
// deleted one, or a slot that was edited into something else keeps its own
// history.
export const awardRenames = (
	before: AwardSettings,
	after: AwardSettings,
): AwardRename[] => {
	const beforeByShortName = new Map(
		before.map((award) => [award.shortName, award]),
	);
	const afterShortNames = new Set(after.map((award) => award.shortName));

	const renames: AwardRename[] = [];

	for (const [i, award] of after.entries()) {
		const isTeam = award.numTeams !== undefined;

		const keptAbbrev = beforeByShortName.get(award.shortName);
		if (keptAbbrev) {
			if (keptAbbrev.name !== award.name) {
				renames.push({
					fromShortName: award.shortName,
					toName: award.name,
					toShortName: award.shortName,
					isTeam,
				});
			}
			continue;
		}

		const old = before[i];
		if (
			!old ||
			// Still in use, so it is not this award's old abbrev.
			afterShortNames.has(old.shortName) ||
			!fastDeepEqual(withoutNames(old), withoutNames(award))
		) {
			continue;
		}

		renames.push({
			fromShortName: old.shortName,
			toName: award.name,
			toShortName: award.shortName,
			isTeam,
		});
	}

	return renames;
};

// Does this stored award answer to the old label? The shape is half the
// question: an abbrev handed from a team award to an individual one is a new
// award wearing an old badge, and it must not relabel what the old one won.
export const renameMatches = (
	award: { shortName: string; numTeams?: number | undefined },
	rename: AwardRename,
) =>
	award.shortName === rename.fromShortName &&
	(award.numTeams !== undefined) === rename.isTeam;

// WHAT THE SETTINGS SAY EVERY AWARD IS CALLED, RIGHT NOW.
//
// The diff above only catches a rename as it happens. It cannot help a league
// that renamed its awards before any of this existed, or one whose history was
// half-relabeled and then interrupted - and it never will, because by then
// there is no "before" left to compare against. Both settings say All-NBA;
// only the seasons still say All-League.
//
// So the same machinery is also pointed at the settings themselves. Every
// award becomes a rename from its own abbrev to its own current label, which
// relabels any season still carrying an older one and does nothing at all to a
// season that already agrees. The abbrev is what identifies an award, so this
// says exactly what the diff says: an award keeping its abbrev is the same
// award, and its name follows the settings.
export const awardRenamesFromSettings = (
	awards: AwardSettings,
): AwardRename[] =>
	awards.map((award) => ({
		fromShortName: award.shortName,
		toName: award.name,
		toShortName: award.shortName,
		isTeam: award.numTeams !== undefined,
	}));

// Is any season carrying a label the settings have moved on from? Cheap enough
// to ask on every league load: the awards rows are one small record per season,
// and the sweep that answers it is only paid when the answer is yes.
export const awardLabelsOutOfDate = (
	rows: readonly {
		awards?: {
			shortName: string;
			name: string;
			numTeams?: number | undefined;
		}[];
	}[],
	settings: AwardSettings,
) => {
	const byShortName = new Map(
		settings.map((award) => [award.shortName, award]),
	);

	for (const row of rows) {
		for (const award of row.awards ?? []) {
			const setting = byShortName.get(award.shortName);
			if (
				setting &&
				(setting.numTeams !== undefined) === (award.numTeams !== undefined) &&
				setting.name !== award.name
			) {
				return true;
			}
		}
	}

	return false;
};

export const applyAwardRenames = async (
	renames: AwardRename[],
): Promise<RenameAwardsResult> => {
	const result: RenameAwardsResult = { seasons: 0, players: 0 };
	if (renames.length === 0) {
		return result;
	}

	// Straight to the database, like every other sweep over a league's whole
	// history: most of the players holding these awards are retired and none of
	// them are in the cache. Flush first so what is read here is current, and
	// refill at the end so this season's in-memory copies carry the new labels.
	await idb.cache.flush();

	try {
		return await rename(renames, result);
	} finally {
		// Whatever happened, the cache has to be put back: everything reads
		// through it, and leaving it holding rows this just rewrote (or, worse,
		// stuck mid-fill) breaks the rest of the session.
		await idb.cache.fill();
	}
};

const rename = async (
	renames: AwardRename[],
	result: RenameAwardsResult,
): Promise<RenameAwardsResult> => {
	const byFromShortName = new Map(
		renames.map((rename) => [rename.fromShortName, rename]),
	);

	// pid -> the relabelings that player's own award list needs.
	const playerEdits = new Map<
		number,
		{ season: number; rename: AwardRename }[]
	>();

	for (const raw of await idb.league.getAll("awards")) {
		// A season synced from an older build is still in the pre-upgrade shape.
		const awards = normalizeAwardsRow(raw);
		let changed = false;

		for (const award of awards.awards) {
			const rename = byFromShortName.get(award.shortName);
			if (!rename || !renameMatches(award, rename)) {
				continue;
			}

			// Another award that season already answers to the new abbrev. Two
			// awards sharing one abbrev in a season is exactly what the settings
			// forbid, so leave this one as it is rather than create it.
			if (
				rename.toShortName !== rename.fromShortName &&
				awards.awards.some((other) => other.shortName === rename.toShortName)
			) {
				continue;
			}

			if (
				award.name !== rename.toName ||
				award.shortName !== rename.toShortName
			) {
				award.name = rename.toName;
				award.shortName = rename.toShortName;
				changed = true;
			}

			// Every player who holds it, taken from the award's own ballot -
			// including when the season's own row was already right, because a
			// player's copy can be stale on its own.
			const winners =
				award.numTeams === undefined ? award.winner : award.winner.flat();
			for (const winner of winners) {
				if (winner.pid === undefined) {
					continue;
				}
				const edits = playerEdits.get(winner.pid) ?? [];
				edits.push({ season: awards.season, rename });
				playerEdits.set(winner.pid, edits);
			}
		}

		if (changed) {
			await idb.league.put("awards", awards);
			result.seasons += 1;
		}
	}

	for (const [pid, edits] of playerEdits) {
		const p = await idb.league.get("players", pid);
		if (!p) {
			continue;
		}

		let changed = false;
		for (const award of p.awards) {
			if (award.type !== undefined) {
				continue;
			}
			for (const { season, rename } of edits) {
				if (
					award.season === season &&
					renameMatches(award, rename) &&
					(award.name !== rename.toName ||
						award.shortName !== rename.toShortName)
				) {
					award.name = rename.toName;
					award.shortName = rename.toShortName;
					changed = true;
				}
			}
		}

		if (changed) {
			// A relabel can land one award on top of another the player already
			// held - two awards he won the same season, one renamed into the
			// other's old name. Keep the first of each.
			const seen: PlayerAward[] = [];
			p.awards = p.awards.filter((award) => {
				if (seen.some((other) => fastDeepEqual(other, award))) {
					return false;
				}
				seen.push(award);
				return true;
			});

			await idb.league.put("players", p);
			result.players += 1;
		}
	}

	return result;
};
