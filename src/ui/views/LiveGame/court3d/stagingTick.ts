// A WORKER STAGING A GAME SAYS, EVERY SECOND OR SO, THAT IT IS STILL AT IT
// (see stager.ts) - so the page waiting on it can tell a phone slowly
// working through a whole game from a worker the phone has put down for
// the memory it took (which says nothing as it goes: the page was left on
// the warm-ups for good). Called through the work; nothing comes of it but
// the word, and it changes nothing staged.
let hook: (() => void) | undefined;
let last = 0;
let n = 0;
const EVERY_MS = 1000;

export const setStagingTick = (f: (() => void) | undefined) => {
	hook = f;
	last = 0;
	n = 0;
};

// From a loop gone round many times: the clock looked at only so often.
export const stagingTick = () => {
	if (hook && (++n & 255) === 0) {
		stagingBeat();
	}
};

// Between longer pieces of the work.
export const stagingBeat = () => {
	if (!hook) {
		return;
	}
	const now = Date.now();
	if (now - last >= EVERY_MS) {
		last = now;
		hook();
	}
};
