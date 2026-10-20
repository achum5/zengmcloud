import { syncDebugLog } from "./debugLog.ts";

// A connect read on the SDK's channel, with a rescue (see v2/restDelta.ts):
// still out after `afterMs`, the same document is read over plain HTTPS as
// well, and whichever answers first wins.
//
// Field capture: a league switch whose very first read - the room's head, one
// tiny document - hung for the whole 20s step timeout on a channel that had
// gone quiet. The connect failed, went round again, and the second try was
// through in a second: a minute from switching to playing, a third of it that
// one read.
//
// The rescue only ever answers with a document that is there. A missing
// document, a refused request or a failed fetch is "no rescue", and the SDK
// read answers (or fails) exactly as it always did - so a rescue can make a
// connect quicker, never different. An SDK read that FAILS while the rescue is
// out waits for it rather than failing over it.
//
// `stalled` hears of the SDK read each time a rescue goes out for it: the
// caller can tell from it whether the channel is still sitting on it later.
export const CONNECT_RESCUE_AFTER_MS = 3000;

export const withRescue = <T>(
	read: Promise<T>,
	direct: ((signal: AbortSignal) => Promise<T | undefined>) | undefined,
	what: string,
	stalled?: (read: Promise<unknown>) => void,
	afterMs = CONNECT_RESCUE_AFTER_MS,
): Promise<T> => {
	if (!direct) {
		return read;
	}
	return new Promise<T>((resolve, reject) => {
		let done = false;
		let rescuing = false;
		let failure: { error: unknown } | undefined;
		const controller = new AbortController();
		const startedAt = Date.now();
		const finish = (settle: () => void) => {
			if (done) {
				return;
			}
			done = true;
			clearTimeout(timer);
			controller.abort();
			settle();
		};
		const timer = setTimeout(() => {
			if (done) {
				return;
			}
			rescuing = true;
			stalled?.(read);
			syncDebugLog("connect:read-rescue", { what });
			const giveUp = () => {
				rescuing = false;
				if (failure) {
					const { error } = failure;
					finish(() => reject(error));
				}
			};
			let rescue: Promise<T | undefined>;
			try {
				rescue = direct(controller.signal);
			} catch {
				giveUp();
				return;
			}
			rescue.then((value) => {
				if (value === undefined) {
					giveUp();
					return;
				}
				syncDebugLog("connect:read-rescued", {
					what,
					ms: Date.now() - startedAt,
				});
				finish(() => resolve(value));
			}, giveUp);
		}, afterMs);
		read.then(
			(value) => finish(() => resolve(value)),
			(error) => {
				failure = { error };
				if (!rescuing) {
					finish(() => reject(error));
				}
			},
		);
	});
};

// Whether any read handed to `stalled` is still out: the channel quiet, not
// merely slow.
export const stallTracker = () => {
	const out = new Set<Promise<unknown>>();
	return {
		stalled: (read: Promise<unknown>) => {
			if (out.has(read)) {
				return;
			}
			out.add(read);
			const clear = () => {
				out.delete(read);
			};
			read.then(clear, clear);
		},
		stillOut: () => out.size > 0,
	};
};
