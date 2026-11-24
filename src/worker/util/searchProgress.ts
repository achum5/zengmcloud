import type { Conditions } from "../../common/types.ts";
import { toUI } from "./toUI.ts";

export type SearchStep = (done: number, total: number) => void;

// A long search's progress, to the screen that asked - the trading block's
// button fills as the search goes.
export type SearchProgress = {
	// The smart CPU's read of the league before any team answers.
	setup: SearchStep;
	// Each team answering.
	step: SearchStep;
};

// The setup, when there is one, is the bar's first quarter: on a 30-team
// league's first search about 5 of 18 seconds (2026-11-24). It's kept until
// the league changes, so a search after it jumps past.
const SETUP_SHARE = 0.25;

export const searchProgress = (conditions?: Conditions) => {
	let last: number | undefined;
	let teamsFrom = 0;
	const send = (percent: number | undefined) => {
		if (percent === last) {
			return;
		}
		last = percent;
		void toUI("updateLocal", [{ searchProgress: percent }], conditions);
	};
	const at = (share: number) => {
		send(Math.min(100, Math.floor(100 * share)));
	};
	const progress: SearchProgress = {
		setup: (done, total) => {
			teamsFrom = SETUP_SHARE;
			at(SETUP_SHARE * (total > 0 ? done / total : 1));
		},
		step: (done, total) => {
			at(teamsFrom + (1 - teamsFrom) * (total > 0 ? done / total : 0));
		},
	};
	return { ...progress, end: () => send(undefined) };
};
