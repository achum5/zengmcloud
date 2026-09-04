// THE REST OF THE BALLOT, FOR SEASONS THAT ONLY EVER RECORDED THE WINNER.
//
// Awards now store the top five of every individual award, so a player page
// shows "MVP-3" for the man who finished third - the way Basketball Reference
// writes it. Seasons played before that only ever stored the winner: the old
// award format had one slot per award and nowhere to put anybody else, so
// after the upgrade those years show the winners and nothing behind them.
//
// The voting order is recoverable, because the award itself is: every season's
// awards row carries the name, the formula and the stat range each of its
// awards was decided by, and the box scores those formulas read are still
// there. Re-running a season's own awards against its own stats gives the same
// ordering it was decided on.
//
// So this fills in the players behind the winner, and touches nothing else:
//
//   - The winner stays the winner. He is never re-picked, re-ranked or moved,
//     whatever the recomputation thinks of him now.
//   - An award whose recomputation does not even contain the stored winner is
//     skipped entirely. Something about that season no longer reproduces -
//     edited stats, a hand-picked winner, a deleted box score - and the order
//     behind a winner the formula does not agree with is not worth writing
//     into a player's history.
//   - Team awards (All-League, All-Rookie) are left alone. They already store
//     every member of every team; there is no ballot behind them.
//
// It is additive and idempotent: a season already carrying a full ballot is
// skipped, and a player who already has the award is left as he is.

import { idb } from "../../db/index.ts";
import addAward from "../player/addAward.ts";
import { getAwardCandidates } from "./getAwardCandidates.ts";
import { NUM_PLAYERS_TO_STORE_PER_INDIVIDUAL_AWARD } from "./doAwards.ts";
import type { Award, AwardPlayer, PlayerAward } from "../../../common/types.ts";

export type BackfillVotingRanksResult = {
	// Seasons that gained at least one name.
	seasons: number;
	// Names added, across every season and award.
	ranks: number;
};

// A candidate as the recomputation ranks them, best first.
export type BallotCandidate = {
	pid: number;
	// The team he played for in the stat range the award was decided over.
	tid: number | undefined;
	statOverrides?: AwardPlayer["statOverrides"];
};

// Who goes behind the winner on one award's ballot, and where each of them
// finished. The decisions all live here:
//
//   - Nothing at all unless the recomputation still has the stored winner in
//     it, because the order behind a winner the formula disagrees with is not
//     worth writing into anybody's history.
//   - The stored entries keep their places; the first name added is the one
//     after them, so a ballot holding only a winner starts adding at 2.
//   - Anybody already on the ballot is skipped rather than listed twice.
export const ballotAdditions = ({
	winner,
	candidates,
	depth = NUM_PLAYERS_TO_STORE_PER_INDIVIDUAL_AWARD,
}: {
	winner: readonly { pid?: number | undefined }[];
	candidates: readonly BallotCandidate[];
	depth?: number;
}): (AwardPlayer & { rank: number })[] => {
	const winnerPid = winner[0]?.pid;
	if (
		winnerPid === undefined ||
		winner.length >= depth ||
		!candidates.some((candidate) => candidate.pid === winnerPid)
	) {
		return [];
	}

	const seen = new Set<number>();
	for (const player of winner) {
		if (player.pid !== undefined) {
			seen.add(player.pid);
		}
	}

	const additions: (AwardPlayer & { rank: number })[] = [];
	let rank = winner.length;
	for (const candidate of candidates) {
		if (rank >= depth) {
			break;
		}
		if (candidate.tid === undefined || seen.has(candidate.pid)) {
			continue;
		}
		seen.add(candidate.pid);
		rank += 1;

		const addition: AwardPlayer & { rank: number } = {
			pid: candidate.pid,
			tid: candidate.tid,
			rank,
		};
		if (candidate.statOverrides) {
			addition.statOverrides = candidate.statOverrides;
		}
		additions.push(addition);
	}

	return additions;
};

// Two awards are the same award when they are the same award realized on the
// same group - which is what makes a conference MVP different from the league
// one, and one semifinal MVP different from the other.
const groupKey = (group: Award["group"]) =>
	group === undefined
		? ""
		: group.type === "conf"
			? `conf${group.cid}`
			: group.type === "div"
				? `div${group.did}`
				: `series${[...group.tids].sort((a, b) => a - b).join("-")}`;

const fullKey = (award: Award) =>
	`${award.shortName}|${award.name}|${award.statRange ?? "regularSeason"}|${groupKey(award.group)}`;

export const backfillVotingRanks =
	async (): Promise<BackfillVotingRanksResult> => {
		// Straight to the database from here on, because this walks every season
		// and most of the players it writes are retired - none of them are in the
		// cache, and pulling a league's whole history through it would be worse
		// than pointless. Flush what is pending first so the rows read here are
		// current, and refill at the end so the in-memory copies of this season's
		// players carry the awards just written.
		await idb.cache.flush();

		const result: BackfillVotingRanksResult = { seasons: 0, ranks: 0 };

		try {
			return await backfill(result);
		} finally {
			// Whatever happened, the cache has to be put back: everything reads
			// through it, and leaving it holding rows this just rewrote (or,
			// worse, stuck mid-fill) breaks the rest of the session.
			await idb.cache.fill();
		}
	};

const backfill = async (
	result: BackfillVotingRanksResult,
): Promise<BackfillVotingRanksResult> => {
	{
		const allAwards = await idb.league.getAll("awards");

		for (const awards of allAwards) {
			const needsFill = (award: Award) =>
				award.numTeams === undefined &&
				award.winner.length < NUM_PLAYERS_TO_STORE_PER_INDIVIDUAL_AWARD &&
				award.winner[0]?.pid !== undefined;

			if (!awards.awards.some((award) => needsFill(award))) {
				continue;
			}

			// The season's own awards, re-decided by their own formulas. This reads
			// the awards row we are about to update, so it uses the definitions that
			// were in force that year rather than whatever the league runs today.
			const { awardCandidates } = await getAwardCandidates(awards.season);

			// Keyed by award, and queued, so a league with two identically named
			// awards in different groups still lines each one up with its own
			// candidates.
			const byKey = new Map<string, (typeof awardCandidates)[number][number]>();
			for (const candidate of awardCandidates.flat()) {
				if (candidate.numTeams !== undefined) {
					continue;
				}
				const key = fullKey(candidate);
				if (!byKey.has(key)) {
					byKey.set(key, candidate);
				}
			}

			const toSave: { pid: number; award: PlayerAward }[] = [];
			let changed = false;

			for (const [index, award] of awards.awards.entries()) {
				if (!needsFill(award) || award.numTeams !== undefined) {
					continue;
				}

				const candidate = byKey.get(fullKey(award));
				if (!candidate) {
					continue;
				}

				const additions = ballotAdditions({
					winner: award.winner,
					candidates: candidate.players.map((p) => ({
						pid: p.pid,
						tid: p.currentStats.tid,
						statOverrides: p.statOverrides,
					})),
				});

				for (const { rank, ...entry } of additions) {
					award.winner.push(entry);
					changed = true;
					result.ranks += 1;

					const playerAward: PlayerAward = {
						season: awards.season,
						name: award.name,
						shortName: award.shortName,
						index,
						rank,
					};
					if (award.group && award.group.type !== "playoffSeries") {
						playerAward.group = award.group;
					}
					if (award.actAs !== undefined) {
						playerAward.actAs = award.actAs;
					}
					toSave.push({ pid: entry.pid, award: playerAward });
				}
			}

			if (!changed) {
				continue;
			}

			await idb.league.put("awards", awards);

			for (const [pid, rows] of Map.groupBy(toSave, (row) => row.pid)) {
				const p = await idb.league.get("players", pid);
				if (!p) {
					continue;
				}
				for (const { award } of rows) {
					addAward(p, award);
				}
				await idb.league.put("players", p);
			}

			result.seasons += 1;
		}

		return result;
	}
};
