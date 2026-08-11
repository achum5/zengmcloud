// Which API action is currently executing in the worker, e.g.
// "playMenu.untilFreeAgency". Set by the dispatcher in worker/index.ts around
// each call, read by forensics that need to attribute a deep side effect (a
// phase change several calls down the stack) to the click that caused it.
//
// A leaf module with no imports, same pattern as engineHolder / the action
// hooks, so core code can read it without a cycle back into worker/index.ts.

let currentAction: string | undefined;

export const setCurrentAction = (label: string | undefined) => {
	currentAction = label;
};

export const getCurrentAction = (): string | undefined => currentAction;
