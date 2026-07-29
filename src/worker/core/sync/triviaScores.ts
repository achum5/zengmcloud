import { g } from "../../util/index.ts";
import { getSyncEngine } from "./engineHolder.ts";
import type { SyncTransport, TriviaScoreEntry } from "./types.ts";

// The room's trivia scoreboard: every finished grid and roster quiz, from every
// device in the league, so a board someone else played shows up in your history
// with their score and their squares - and can be replayed on the spot.
//
// Stored the same way the free-agency board is: ONE control doc, keyed by
// client id, each device writing only its own bucket. That makes concurrent
// publishes merge instead of race, with no read-modify-write on a shared array.
// The cost is a per-device cap, since the whole doc has to stay well inside
// Firestore's 1 MB limit.
const PER_DEVICE_LIMIT = 20;

let currentTransport: SyncTransport | undefined;
let unsubscribe: (() => void) | undefined;
let latest: Record<string, TriviaScoreEntry[] | null> | undefined;

export const setupTriviaScores = (transport: SyncTransport) => {
	teardownTriviaScores();
	currentTransport = transport;
	unsubscribe = transport.subscribeTriviaScores?.((scores) => {
		latest = scores;
	});
};

export const teardownTriviaScores = () => {
	unsubscribe?.();
	unsubscribe = undefined;
	currentTransport = undefined;
	latest = undefined;
};

// Publish this device's recent results. The caller passes its whole local list;
// stamping who played happens here, where the room identity actually lives, so
// the UI never has to know or guess it.
export const publishTriviaScores = async (
	entries: Omit<TriviaScoreEntry, "byName" | "byTid">[],
): Promise<boolean> => {
	const engine = getSyncEngine();
	if (!engine || !currentTransport?.publishTriviaScores) {
		return false;
	}
	const stamped = entries.slice(0, PER_DEVICE_LIMIT).map((entry) => ({
		...entry,
		byName: engine.localName,
		byTid: g.get("userTid"),
	}));
	await currentTransport.publishTriviaScores(stamped);
	return true;
};

// Everyone else's results for one game. This device's own are deliberately
// excluded: the local history is the authority for your own games (it exists
// offline, and it survives the room going away).
export const getRemoteTriviaScores = (game: string): TriviaScoreEntry[] => {
	const mine = currentTransport?.clientId;
	const out: TriviaScoreEntry[] = [];
	for (const [clientId, entries] of Object.entries(latest ?? {})) {
		if (!entries || clientId === mine) {
			continue;
		}
		for (const entry of entries) {
			if (entry?.game === game) {
				out.push(entry);
			}
		}
	}
	return out;
};
