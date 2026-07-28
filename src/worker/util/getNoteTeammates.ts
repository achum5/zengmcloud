import { idb } from "../db/index.ts";
import { parseSeasonNote } from "../../common/seasonNote.ts";
import type { Player } from "../../common/types.ts";

// Who a player's note might be talking about, by season.
//
// A career note is a stack of "[YYYY]" sections written about that year, and
// the names in them are overwhelmingly the people he played WITH - the man he
// was deferring to, the rookie who took his minutes, whoever went down in
// February. Linking those names to their pages is what turns a note from a
// block of text into part of the league.
//
// The names have to be SCOPED, not looked up league-wide, or a common name
// links to whichever player happened to be found first. Scoping them to the
// team-seasons the player actually shared makes a mislink close to impossible,
// and it is also what keeps this cheap: it reads the statsTids index for the
// handful of franchises he played for, rather than walking every player who has
// ever been in the league.
//
// Computed only for players who HAVE a note, and only for the seasons that note
// actually has sections for, so an ordinary player page pays nothing.

export type NoteTeammates = {
	season: number;
	players: { pid: number; name: string }[];
}[];

// Cap per season. A rotation is a dozen names; anything past that is deep-bench
// players a recap never mentions, and each one is another regex pass over the
// note at render time.
const MAX_PER_SEASON = 20;

export const getNoteTeammates = async (
	p: Player,
): Promise<NoteTeammates | undefined> => {
	if (!p.note) {
		return undefined;
	}

	// Only the years the note is actually written about.
	const seasons = new Set<number>();
	for (const section of parseSeasonNote(p.note)) {
		if (section.season !== undefined) {
			seasons.add(section.season);
		}
	}
	if (seasons.size === 0) {
		return undefined;
	}

	// The team-seasons he played, restricted to those years.
	const tidsBySeason = new Map<number, Set<number>>();
	const tids = new Set<number>();
	for (const row of p.stats) {
		if (row.playoffs || !seasons.has(row.season) || row.tid < 0) {
			continue;
		}
		const existing = tidsBySeason.get(row.season) ?? new Set<number>();
		existing.add(row.tid);
		tidsBySeason.set(row.season, existing);
		tids.add(row.tid);
	}
	if (tids.size === 0) {
		return undefined;
	}

	// One indexed read per franchise, covering everyone who ever suited up for
	// it - active or retired.
	const candidates = new Map<number, Player>();
	for (const tid of tids) {
		let squad: Player[];
		try {
			squad = await idb.getCopies.players({ statsTid: tid }, "noCopyCache");
		} catch {
			// A link is a nicety; never break the page over one.
			continue;
		}
		for (const other of squad) {
			if (other.pid !== p.pid) {
				candidates.set(other.pid, other);
			}
		}
	}

	const out: NoteTeammates = [];
	for (const [season, seasonTids] of tidsBySeason) {
		const players: { pid: number; name: string; min: number }[] = [];
		for (const other of candidates.values()) {
			let minutes = 0;
			let shared = false;
			for (const row of other.stats) {
				if (
					!row.playoffs &&
					row.season === season &&
					seasonTids.has(row.tid) &&
					(row.gp ?? 0) > 0
				) {
					shared = true;
					minutes += (row as any).min ?? 0;
				}
			}
			if (shared) {
				players.push({
					pid: other.pid,
					name: `${other.firstName} ${other.lastName}`,
					min: minutes,
				});
			}
		}
		if (players.length > 0) {
			out.push({
				season,
				// Most-used players first, so the cap drops the end of the bench.
				players: players
					.sort((a, b) => b.min - a.min)
					.slice(0, MAX_PER_SEASON)
					.map(({ pid, name }) => ({ pid, name })),
			});
		}
	}

	return out.length > 0 ? out : undefined;
};
