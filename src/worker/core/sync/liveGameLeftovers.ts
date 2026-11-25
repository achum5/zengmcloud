import { quietGameToCheck, quietGameVerdict } from "./liveBroadcastFollow.ts";
import type { LiveBroadcastMeta } from "./types.ts";

// Keeping a dropped connection from leaving a live game stuck on everybody
// else's screen. The rules are in liveBroadcastFollow.ts (quietGameVerdict);
// this is the machinery, apart from connect.ts so it can be tested with a
// fake transport.

// THE SWEEP. A live game gone quiet is looked up on the server (one small
// read, at most once a minute a game) and, by what the server has, called
// over on this device or deleted for everyone.
export type SweepTransport = {
	clientId: string;
	fetchLiveGameFromServer?(gid: number): Promise<{
		meta: LiveBroadcastMeta | undefined;
		updatedAt: number | undefined;
	}>;
	clearLiveGame?(gid: number, chunkCount: number): Promise<void>;
};

export const QUIET_GAME_RECHECK_MS = 60_000;
// Docs looked into in one go (a room never has many live at once).
const SWEEP_MAX = 4;

export const createLiveGameSweep = ({
	transport,
	metas,
	playingHere,
	giveUp,
	onCalledOver,
	log,
	now = Date.now,
}: {
	// The current connection; a sweep stops if it changes under it.
	transport: () => SweepTransport | undefined;
	// Every live game doc last delivered, this device's own included.
	metas: () => readonly LiveBroadcastMeta[];
	// The game this device is playing, or starting to.
	playingHere: (gid: number) => boolean;
	// Over on this device. True if it was not already.
	giveUp: (meta: LiveBroadcastMeta) => boolean;
	// Something was called over: the pages redo what they show.
	onCalledOver: () => void;
	log?: (event: string, data: Record<string, unknown>) => void;
	now?: () => number;
}) => {
	const checkedAt = new Map<string, number>();
	let sweeping = false;
	const keyOf = (meta: LiveBroadcastMeta) =>
		`${meta.gid}:${meta.startedAt}:${meta.expiresAt}`;

	return async () => {
		const t = transport();
		if (sweeping || !t || !t.fetchLiveGameFromServer || !t.clearLiveGame) {
			return;
		}
		const all = metas();
		const keys = new Set(all.map(keyOf));
		for (const key of checkedAt.keys()) {
			if (!keys.has(key)) {
				checkedAt.delete(key);
			}
		}
		const started = now();
		const due = all.filter(
			(meta) =>
				!meta.legacy &&
				started - (checkedAt.get(keyOf(meta)) ?? -Infinity) >=
					QUIET_GAME_RECHECK_MS &&
				quietGameToCheck(meta, {
					clientId: t.clientId,
					playingHere,
					now: started,
				}),
		);
		if (due.length === 0) {
			return;
		}
		sweeping = true;
		let calledOver = false;
		try {
			for (const heard of due.slice(0, SWEEP_MAX)) {
				checkedAt.set(keyOf(heard), now());
				let server;
				try {
					server = await t.fetchLiveGameFromServer(heard.gid);
				} catch {
					// Not now; next time round.
					continue;
				}
				if (transport() !== t) {
					return;
				}
				const verdict = quietGameVerdict({
					heard,
					server: server.meta,
					serverUpdatedAt: server.updatedAt,
					clientId: t.clientId,
					now: now(),
				});
				if (verdict.over && giveUp(heard)) {
					calledOver = true;
					log?.("live:left-behind-over", { gid: heard.gid });
				}
				// (Never one this device has since started playing.)
				if (verdict.clear && !playingHere(heard.gid)) {
					try {
						await t.clearLiveGame(heard.gid, heard.chunkCount);
						log?.("live:left-behind-cleared", {
							gid: heard.gid,
							own: heard.holderId === t.clientId,
						});
					} catch {
						// Somebody else's to clear next time.
					}
				}
			}
		} finally {
			sweeping = false;
			if (calledOver) {
				onCalledOver();
			}
		}
	};
};

// THE SIMMER'S HEARTBEATS. One on its way at a time: a newer one waits for
// it, and only the latest waits (the position it carries is all that
// matters). Firestore takes about one write a second to a doc; heartbeats
// sent faster than the server answers queued up in the device's writes, and
// everything else it wrote waited behind them - its readiness ping (the dot
// red) included - while league mates saw the game later and later. One the
// server hasn't answered in waitMs stops holding the next one up.
export const BEAT_WAIT_MS = 10_000;
export const createLatestOnlyQueue = <T>(waitMs: number = BEAT_WAIT_MS) => {
	type Job = { item: T; send: (item: T) => Promise<unknown> };
	let inFlight = false;
	let waiting: Job | undefined;
	const settle = async (promise: Promise<unknown>) => {
		let timer: ReturnType<typeof setTimeout> | undefined;
		await Promise.race([
			promise.catch(() => {}),
			new Promise<void>((resolve) => {
				timer = setTimeout(resolve, waitMs);
			}),
		]);
		clearTimeout(timer);
	};
	return {
		// `send` is the item's own: what it goes out through (the connection
		// it was made on).
		push: async (item: T, send: (item: T) => Promise<unknown>) => {
			if (inFlight) {
				waiting = { item, send };
				return;
			}
			inFlight = true;
			let next: Job | undefined = { item, send };
			try {
				while (next) {
					waiting = undefined;
					let sent: Promise<unknown>;
					try {
						sent = next.send(next.item);
					} catch (error) {
						sent = Promise.reject(error);
					}
					await settle(sent);
					next = waiting;
				}
			} finally {
				inFlight = false;
			}
		},
		// Nothing more goes out (the broadcast is over).
		clear: () => {
			waiting = undefined;
		},
	};
};
