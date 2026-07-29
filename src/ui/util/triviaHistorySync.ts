import { toWorker } from "./toWorker.ts";
import {
	loadHistory,
	mergeHistory,
	type TriviaGame,
	type TriviaHistoryEntry,
} from "./triviaHistory.ts";

// The network half of the play history. Kept apart from the store itself so the
// store stays a pure key/value module - and so a device with no room, or a
// publish that fails, can never cost you the game you just finished.

// Only this many of your most recent games go to the room; the whole scoreboard
// lives in a single document that every device writes into.
const SHARED_LIMIT = 20;

// Fire and forget. A failed publish is not worth telling anyone about: the game
// is already saved locally, and the next one will re-publish the whole list.
export const shareHistory = (
	game: TriviaGame,
	entries: TriviaHistoryEntry[],
) => {
	void toWorker("main", "triviaPublishScores", {
		entries: entries.slice(0, SHARED_LIMIT).map((e) => ({
			id: e.id,
			game,
			ts: e.ts,
			score: e.score,
			label: e.label,
			detail: e.detail,
			progress: e.progress,
			cells: e.cells,
			replay: e.replay,
			tid: e.tid,
			season: e.season,
		})),
	}).catch(() => {});
};

// Your games plus everyone else's in the room.
export const loadSharedHistory = async (
	game: TriviaGame,
): Promise<TriviaHistoryEntry[]> => {
	let remote: unknown[] = [];
	try {
		remote = ((await toWorker("main", "triviaRemoteScores", { game })) ??
			[]) as unknown[];
	} catch {
		remote = [];
	}
	return mergeHistory(loadHistory(game), remote);
};
