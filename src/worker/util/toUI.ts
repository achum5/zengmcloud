import { promiseWorker } from "./promiseWorker.ts";
import type api from "../../ui/api/index.ts";
import type { Conditions } from "../../common/types.ts";

// What the screen was last told about a live game being on - the flag that
// freezes the score bar, the ticker and the header (see checkStrandedLiveFlag
// in core/sync/connect.ts, which takes it down if nothing owns it).
let liveFlag = false;
export const liveFlagShown = () => liveFlag;

const toUI = <Name extends keyof typeof api>(
	name: Name,
	args: Parameters<(typeof api)[Name]>,
	conditions: Conditions = {},
): Promise<ReturnType<(typeof api)[Name]>> => {
	if (name === "updateLocal") {
		const v = (args[0] as { liveGameInProgress?: unknown } | undefined)
			?.liveGameInProgress;
		if (typeof v === "boolean") {
			liveFlag = v;
		}
	} else if (name === "resetLeague") {
		liveFlag = false;
	}
	if (__NODE_ENV === "test") {
		// @ts-expect-error
		return Promise.resolve();
	}

	return promiseWorker.postMessage([name, ...args], conditions.hostID) as any;
};

export default toUI;
