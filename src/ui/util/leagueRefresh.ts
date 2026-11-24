import { create } from "zustand";
import type { UpdateEvents } from "../../common/types.ts";

// A count of the worker's word that the league moved on - a day simmed (here
// or on a league mate's device), a phase change, a trade, a setting. A page's
// own view is refreshed by the view manager; this is for the few parts of a
// page that ask the worker for their data themselves (the team hub's
// sections, the auto play preview), so they can ask again rather than keep
// showing the night that was already played.
const MOVES = new Set([
	"gameAttributes",
	"gameSim",
	"newPhase",
	"playerMovement",
	"playoffs",
	"team",
	"teamFinances",
]);

const useLeagueRefreshStore = create<{ count: number }>(() => ({ count: 0 }));

export const noteLeagueRefresh = (updateEvents: UpdateEvents) => {
	if (updateEvents.some((event) => MOVES.has(event))) {
		useLeagueRefreshStore.setState((state) => ({ count: state.count + 1 }));
	}
};

export const useLeagueRefresh = () =>
	useLeagueRefreshStore((state) => state.count);
