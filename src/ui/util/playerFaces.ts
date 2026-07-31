import { useEffect, useReducer } from "react";
import type { FaceConfig } from "facesjs";
import { toWorker } from "./toWorker.ts";

// A tiny DataLoader for player faces: PlayerNameLabels asks for a (pid, season)'s
// face, we coalesce every one requested in the same tick into ONE worker call,
// and cache the result. This is what lets a small, correctly-uniformed face
// appear next to a name in EVERY player table without baking face data into
// every view's row payload.
//
// Keyed by "pid:season" (season picks the team the player wore that year), and
// scoped to a league - the cache MUST be cleared when the league (lid) changes,
// or a new league's player #5 would show the old league's player #5 face.

export type PlayerFace = {
	face?: FaceConfig;
	imgURL?: string;
	colors?: [string, string, string];
	jersey?: string; // uniform STYLE id (for facesjs), not the number
	jerseyNumber?: string; // the player's actual number, e.g. "3", "44"
	// Real height (inches) and weight (lbs), for sizing a player by his build.
	hgt?: number;
	weight?: number;
};

const keyOf = (pid: number, season: number | undefined) =>
	`${pid}:${season ?? ""}`;

let cacheLid: number | undefined;
const cache = new Map<string, PlayerFace | null>();
const subscribers = new Map<string, Set<() => void>>();
let pending = new Map<string, { pid: number; season?: number }>();
let flushTimer: ReturnType<typeof setTimeout> | undefined;

const notify = (key: string) => {
	const set = subscribers.get(key);
	if (set) {
		for (const cb of set) {
			cb();
		}
	}
};

const flush = async () => {
	flushTimer = undefined;
	const items = [...pending.values()];
	const keys = [...pending.keys()];
	pending = new Map();
	if (items.length === 0) {
		return;
	}

	const lidAtStart = cacheLid;
	let result: Record<string, PlayerFace> | undefined;
	try {
		result = await toWorker("main", "getPlayerFaces", items);
	} catch {
		// Best-effort - a failed fetch just leaves those keys uncached, so they'll
		// be retried the next time a row asks for them.
		return;
	}
	if (!result) {
		// The worker refused the call (e.g. a sync guard) - leave uncached to retry.
		return;
	}

	// If the league changed while we were fetching, these results are for the old
	// league - drop them rather than poison the new league's cache.
	if (cacheLid !== lidAtStart) {
		return;
	}
	for (const key of keys) {
		cache.set(key, result[key] ?? null);
		notify(key);
	}
};

const ensureLid = (lid: number | undefined) => {
	if (lid !== cacheLid) {
		cacheLid = lid;
		cache.clear();
		pending = new Map();
	}
};

const request = (key: string, pid: number, season: number | undefined) => {
	if (cache.has(key) || pending.has(key)) {
		return;
	}
	pending.set(key, { pid, season });
	if (flushTimer === undefined) {
		flushTimer = setTimeout(flush, 0);
	}
};

export const updatePlayerFaceImage = (pid: number, imgURL: string) => {
	const prefix = `${pid}:`;
	const keys = new Set([...cache.keys(), ...subscribers.keys()]);

	for (const key of keys) {
		if (!key.startsWith(prefix)) {
			continue;
		}

		const previous = cache.get(key);
		cache.set(key, {
			...(previous ?? {}),
			face: undefined,
			imgURL,
		});
		notify(key);
	}
};

// The mirror of the above, for saving a cartoon face: the image URL goes away
// (the worker deletes it too, since imgURL wins wherever a player is drawn) and
// every cached season for this player picks up the new face right away, so the
// row you just edited updates without a reload.
export const updatePlayerFaceData = (pid: number, face: FaceConfig) => {
	const prefix = `${pid}:`;
	const keys = new Set([...cache.keys(), ...subscribers.keys()]);

	for (const key of keys) {
		if (!key.startsWith(prefix)) {
			continue;
		}

		const previous = cache.get(key);
		cache.set(key, {
			...(previous ?? {}),
			face,
			imgURL: undefined,
		});
		notify(key);
	}
};

// Returns the player's face data once loaded (null = loaded, no face; undefined =
// still loading or no pid). Re-renders the caller when the data arrives.
export const usePlayerFace = (
	pid: number | undefined,
	season: number | undefined,
	lid: number | undefined,
): PlayerFace | null | undefined => {
	const [, forceRender] = useReducer((x: number) => x + 1, 0);

	useEffect(() => {
		if (pid === undefined) {
			return;
		}
		ensureLid(lid);
		const key = keyOf(pid, season);
		request(key, pid, season);

		let set = subscribers.get(key);
		if (!set) {
			set = new Set();
			subscribers.set(key, set);
		}
		const cb = () => forceRender();
		set.add(cb);

		return () => {
			set.delete(cb);
			if (set.size === 0) {
				subscribers.delete(key);
			}
		};
	}, [pid, season, lid]);

	if (pid === undefined) {
		return undefined;
	}
	return cache.get(keyOf(pid, season));
};
