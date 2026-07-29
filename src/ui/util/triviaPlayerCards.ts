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

// Prime the cache for a whole roster in one worker call, applying a single set
// of team colors to all of them. Team Trivia paints its card grid the instant a
// round arrives; fetching each face separately staggers visibly on a phone.
//
// Already-cached pids are skipped, so re-picking a team-season you've seen
// before costs nothing.
export const primeTriviaFaces = async (
	pids: number[],
	team: { colors?: [string, string, string]; jersey?: string },
): Promise<Record<number, TriviaPlayerCard>> => {
	const missing = pids.filter((pid) => !cache.has(pid));
	if (missing.length > 0) {
		const faces = await toWorker("main", "triviaFaces", { pids: missing });
		for (const face of faces ?? []) {
			cache.set(face.pid, {
				pid: face.pid,
				face: face.face,
				imgURL: face.imgURL,
				colors: team.colors,
				jersey: team.jersey,
			});
		}
	}

	const out: Record<number, TriviaPlayerCard> = {};
	for (const pid of pids) {
		const card = cache.get(pid);
		if (card) {
			// A pid already cached from another game carries that game's colors, so
			// re-dress it in this round's jersey without disturbing the cache.
			out[pid] = { ...card, colors: team.colors, jersey: team.jersey };
		}
	}
	return out;
};
