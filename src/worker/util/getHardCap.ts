import g from "./g.ts";
import { hardCapForTid } from "../../common/getHardCap.ts";

// The hard-cap ceiling (thousands of dollars) for a team, or Infinity if the
// team isn't bound / the feature is off. See src/common/getHardCap.ts.
export const getHardCap = (tid: number): number =>
	hardCapForTid(tid, g.get("hardCapAmount"), g.get("hardCapTids"));

// Whether the secondary hard cap is enabled at all for the league.
export const hardCapEnabled = (): boolean => g.get("hardCapAmount") > 0;
