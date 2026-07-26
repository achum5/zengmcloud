import { toWorker } from "./toWorker.ts";

// Shared face/jersey cache for the trivia games. Every game shows the same
// players over and over (a search list is re-filtered on every keystroke, a
// grid re-renders on every guess), so without a cache the same pid would be
// re-fetched from the worker dozens of times per session.
//
// `inFlight` dedupes concurrent requests for the same pid too, which matters
// for the search box: eight rows can ask for their face in the same tick.

export type TriviaPlayerCard = {
	pid: number;
	face?: any;
	imgURL?: string;
	colors?: [string, string, string];
	jersey?: string;
};

const cache = new Map<number, TriviaPlayerCard>();
const inFlight = new Map<number, Promise<TriviaPlayerCard | undefined>>();

// A card already in memory, for a synchronous first paint with no flash.
export const getCachedCard = (pid: number) => cache.get(pid);

export const fetchTriviaCard = (
	pid: number,
	tid?: number,
): Promise<TriviaPlayerCard | undefined> => {
	const cached = cache.get(pid);
	if (cached) {
		return Promise.resolve(cached);
	}
	const existing = inFlight.get(pid);
	if (existing) {
		return existing;
	}
	const p = toWorker("main", "triviaPlayerCard", { pid, tid })
		.then((card) => {
			if (card) {
				cache.set(pid, card as TriviaPlayerCard);
			}
			inFlight.delete(pid);
			return card as TriviaPlayerCard | undefined;
		})
		.catch(() => {
			inFlight.delete(pid);
			return undefined;
		});
	inFlight.set(pid, p);
	return p;
};
