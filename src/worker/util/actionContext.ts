// Which API action is currently executing in the worker, e.g.
// "playMenu.untilFreeAgency". Set by the dispatcher in worker/index.ts around
// each call, read by forensics that need to attribute a deep side effect (a
// phase change several calls down the stack) to the click that caused it.
//
// Calls overlap - a view load or a lock toggle while a sim is running - so
// each one in flight is kept, and the one credited is the one that started
// first: a short call coming and going neither takes the credit for the
// sim's work nor, once done, wipes the sim's label out.
//
// A leaf module with no imports, same pattern as engineHolder / the action
// hooks, so core code can read it without a cycle back into worker/index.ts.

const running = new Map<number, string>();
let next = 0;

// Starts an action; returns the function that ends it.
export const beginAction = (label: string): (() => void) => {
	const id = next++;
	running.set(id, label);
	return () => {
		running.delete(id);
	};
};

export const getCurrentAction = (): string | undefined =>
	running.values().next().value;
