// The pre-action guard (worker/index.ts) used to turn an action away the
// moment this device was behind the room: "Still catching up to the cloud,
// so this wasn't done. Try again in a moment." Behind is ordinary, though -
// every change a league mate makes puts every other device behind for the
// second or two it takes to fetch and apply it - so a tap that landed in that
// second (a press conference's play button, a signing, a roster drag) was
// refused for nothing. Now the action waits here for the device to catch up
// and then goes ahead. It is refused only if the device still can't catch up
// after a long wait. (The header's Simming… pill shows meanwhile.)

export const CATCH_UP_WAIT_MS = 30_000;

// How often the wait looks again. Catch-up runs on its own (the listener
// starts it the moment a version is announced); this only notices it ended.
const LOOK_AGAIN_MS = 100;

// How long the wait lets a behind device sit with nothing working on the gap
// before it starts a pass itself.
const KICK_EVERY_MS = 2_000;

export type CatchUpEngine = {
	isCaughtUp(): boolean;
	// A pass to the room's head; true only if this pass reached it. Serialized
	// and backoff-aware in the engine (false at once while backing off).
	catchUp(): Promise<boolean>;
	// Starts a pass only if none is running.
	kickCatchUp(): void;
};

const sleep = (ms: number) =>
	new Promise<void>((resolve) => {
		setTimeout(resolve, ms);
	});

// Resolves the promise's value, or `fallback` once `ms` has passed.
const within = <T>(promise: Promise<T>, ms: number, fallback: T) =>
	new Promise<T>((resolve) => {
		const timer = setTimeout(() => {
			resolve(fallback);
		}, ms);
		promise.then(
			(value) => {
				clearTimeout(timer);
				resolve(value);
			},
			() => {
				clearTimeout(timer);
				resolve(fallback);
			},
		);
	});

// Waits until this device has caught up to the room. True once it has; false
// if it hasn't by the deadline, or `current` says the engine was replaced
// (a reconnect) - the caller refuses then.
//
// `drain`: for a timeline advance (a sim, a phase change), "caught up" by the
// device's own count isn't enough - a listener that quietly stopped
// delivering leaves that count looking current (see the guard). It needs a
// pass that read the room's head and reached it, so one is run, and run
// again after a failure, until one does.
export const waitForCatchUp = async (
	engine: CatchUpEngine,
	{
		drain = false,
		timeoutMs = CATCH_UP_WAIT_MS,
		current = () => true,
	}: {
		drain?: boolean;
		timeoutMs?: number;
		current?: () => boolean;
	} = {},
): Promise<boolean> => {
	const deadline = Date.now() + timeoutMs;

	if (drain) {
		while (current()) {
			const left = deadline - Date.now();
			if (left <= 0) {
				return false;
			}
			const reached = await within(engine.catchUp(), left, false);
			if (!current()) {
				return false;
			}
			if (reached && engine.isCaughtUp()) {
				return true;
			}
			// A failed pass backs the engine off; asking again at once would
			// only be answered false at once.
			await sleep(Math.min(250, Math.max(0, deadline - Date.now())));
		}
		return false;
	}

	let kickedAt = Date.now();
	if (!engine.isCaughtUp()) {
		engine.kickCatchUp();
	}
	while (current()) {
		if (engine.isCaughtUp()) {
			return true;
		}
		const now = Date.now();
		if (now >= deadline) {
			return false;
		}
		if (now - kickedAt >= KICK_EVERY_MS) {
			kickedAt = now;
			engine.kickCatchUp();
		}
		await sleep(Math.min(LOOK_AGAIN_MS, deadline - now));
	}
	return false;
};
