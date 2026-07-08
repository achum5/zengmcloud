import { useEffect, useReducer } from "react";
import type { FaceConfig } from "facesjs";
import { toWorker } from "./toWorker.ts";

// A tiny DataLoader for player faces: PlayerNameLabels asks for a pid's face, we
// coalesce every pid requested in the same tick into ONE worker call, and cache
// the result. This is what lets a small face appear next to a name in EVERY
// player table without baking face data into every view's row payload.
//
// The cache is keyed by pid, which is league-scoped, so it MUST be cleared when
// the league (lid) changes - otherwise a new league's player #5 would show the
// old league's player #5 face.

export type PlayerFace = {
	face?: FaceConfig;
	imgURL?: string;
	tid?: number;
};

let cacheLid: number | undefined;
const cache = new Map<number, PlayerFace | null>();
const subscribers = new Map<number, Set<() => void>>();
let pending = new Set<number>();
let flushTimer: ReturnType<typeof setTimeout> | undefined;

const notify = (pid: number) => {
	const set = subscribers.get(pid);
	if (set) {
		for (const cb of set) {
			cb();
		}
	}
};

const flush = async () => {
	flushTimer = undefined;
	const pids = [...pending];
	pending = new Set();
	if (pids.length === 0) {
		return;
	}

	const lidAtStart = cacheLid;
	let result: Record<number, PlayerFace> = {};
	try {
		result = await toWorker("main", "getPlayerFaces", pids);
	} catch {
		// Best-effort - a failed fetch just leaves those pids uncached, so they'll
		// be retried the next time a row asks for them.
		return;
	}

	// If the league changed while we were fetching, these results are for the old
	// league - drop them rather than poison the new league's cache.
	if (cacheLid !== lidAtStart) {
		return;
	}
	for (const pid of pids) {
		cache.set(pid, result[pid] ?? null);
		notify(pid);
	}
};

const ensureLid = (lid: number | undefined) => {
	if (lid !== cacheLid) {
		cacheLid = lid;
		cache.clear();
		pending = new Set();
	}
};

const request = (pid: number) => {
	if (cache.has(pid) || pending.has(pid)) {
		return;
	}
	pending.add(pid);
	if (flushTimer === undefined) {
		flushTimer = setTimeout(flush, 0);
	}
};

// Returns the player's face data once loaded (null = loaded, no face; undefined =
// still loading or no pid). Re-renders the caller when the data arrives.
export const usePlayerFace = (
	pid: number | undefined,
	lid: number | undefined,
): PlayerFace | null | undefined => {
	const [, forceRender] = useReducer((x: number) => x + 1, 0);

	useEffect(() => {
		if (pid === undefined) {
			return;
		}
		ensureLid(lid);
		request(pid);

		let set = subscribers.get(pid);
		if (!set) {
			set = new Set();
			subscribers.set(pid, set);
		}
		const cb = () => forceRender();
		set.add(cb);

		return () => {
			set.delete(cb);
			if (set.size === 0) {
				subscribers.delete(pid);
			}
		};
	}, [pid, lid]);

	if (pid === undefined) {
		return undefined;
	}
	return cache.get(pid);
};
