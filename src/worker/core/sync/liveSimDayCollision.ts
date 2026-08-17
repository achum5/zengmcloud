import getSchedule from "../season/getSchedule.ts";

// Would a live sim playing in the room actually stand in the way of an
// automatic day sim on THIS device?
//
// Only while the watched game is still PENDING in this device's own schedule.
// A day is claimed as a (day, gids) slice, and a slice overlapping a gid
// somebody else already claimed is refused WHOLE - so an auto sim fired inside
// that window costs the room its tick and sims nothing. Once their result has
// landed here, the game is out of our schedule, the rest of the day is a
// disjoint slice, and that is precisely the case simDayClaimPolicy exists to
// allow ("live-sim one game, then sim the rest of the day").
//
// Which is the usual case by a wide margin. A game is published the moment it
// is simmed, and the playback the room then sits and watches runs for minutes
// afterwards - so by the time a scheduled fire lands mid-playback, the result
// it was supposedly waiting for arrived long ago. Treating the whole playback
// as a blocker is what let a watch party silently eat a scheduled sim.
//
// Deliberately reads the same one-day slice game.play claims (getSchedule(true)),
// so this answers the question the fence will actually be asked.
export const liveSimBlocksDaySim = async (gid: number): Promise<boolean> => {
	if (typeof gid !== "number" || Number.isNaN(gid)) {
		return false;
	}
	const schedule = await getSchedule(true);
	return schedule.some((game) => game.gid === gid);
};
