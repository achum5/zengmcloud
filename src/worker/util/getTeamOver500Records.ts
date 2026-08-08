import type { Game } from "../../common/types.ts";
import { idb } from "../db/index.ts";

export type Over500Record = { won: number; lost: number; tied: number };

// A team's record against opponents with a winning record - the "quality wins"
// column every standings page has.
//
// Built from the games themselves rather than from stored splits, for the same
// reason the ATS record is: nothing has to be recorded at sim time, and it
// works retroactively on a league that is already mid-season.
//
// WHO COUNTS AS ABOVE .500 IS A MOVING TARGET, deliberately. Opponents are
// judged on the record they hold right now, not the one they held on the night
// of the game - so beating a team that later collapses stops counting, and the
// column re-reads itself as the season sorts itself out. That is how every real
// standings page does it, and the alternative (freezing each game against a
// snapshot) would make the column disagree with the records printed beside it.
//
// The classification uses each team's OVERALL record, including the games
// against the team being measured. Also conventional, and the alternative -
// removing the head-to-head first - would give every team its own private
// ranking of who is good.
export const getTeamOver500Records = async (
	season: number,
	// The standings page already loads a season of games for the ATS column.
	// Passing them in avoids pulling every box score a second time.
	preloadedGames?: Game[],
): Promise<Map<number, Over500Record>> => {
	const games =
		preloadedGames ?? (await idb.getCopies.games({ season }, "noCopyCache"));

	const blank = (): Over500Record => ({ won: 0, lost: 0, tied: 0 });

	// Regular season only, so this spans exactly the games in the W-L beside it,
	// and no All-Star or other special game (tid < 0) muddies either pass.
	const counted = games.filter(
		(game) =>
			!game.playoffs &&
			game.teams[0].tid >= 0 &&
			game.teams[1].tid >= 0 &&
			game.teams[0].tid !== game.teams[1].tid,
	);

	// Pass 1: everyone's overall record, so we know who is above .500.
	const overall = new Map<number, Over500Record>();
	const record = (map: Map<number, Over500Record>, tid: number) => {
		let rec = map.get(tid);
		if (!rec) {
			rec = blank();
			map.set(tid, rec);
		}
		return rec;
	};

	for (const game of counted) {
		const [home, away] = game.teams;
		const homeRec = record(overall, home.tid);
		const awayRec = record(overall, away.tid);
		if (home.pts > away.pts) {
			homeRec.won += 1;
			awayRec.lost += 1;
		} else if (away.pts > home.pts) {
			awayRec.won += 1;
			homeRec.lost += 1;
		} else {
			homeRec.tied += 1;
			awayRec.tied += 1;
		}
	}

	const above500 = new Set<number>();
	for (const [tid, rec] of overall) {
		const played = rec.won + rec.lost + rec.tied;
		if (played > 0 && (rec.won + 0.5 * rec.tied) / played > 0.5) {
			above500.add(tid);
		}
	}

	// Pass 2: the same games again, keeping only those against that set.
	const records = new Map<number, Over500Record>();
	for (const game of counted) {
		const [home, away] = game.teams;
		const sides = [
			{ self: home, opp: away },
			{ self: away, opp: home },
		];
		for (const { self, opp } of sides) {
			if (!above500.has(opp.tid)) {
				continue;
			}
			const rec = record(records, self.tid);
			if (self.pts > opp.pts) {
				rec.won += 1;
			} else if (opp.pts > self.pts) {
				rec.lost += 1;
			} else {
				rec.tied += 1;
			}
		}
	}

	// Every team that played gets an entry, so a team with no games yet against a
	// winning opponent reads "0-0" rather than dropping out of the column.
	for (const tid of overall.keys()) {
		if (!records.has(tid)) {
			records.set(tid, blank());
		}
	}

	return records;
};

export const formatOver500Record = (rec: Over500Record | undefined): string => {
	if (!rec) {
		return "";
	}
	return rec.tied > 0
		? `${rec.won}-${rec.lost}-${rec.tied}`
		: `${rec.won}-${rec.lost}`;
};
