// AN ORPHANED SCHEDULE ROW: a game that is scheduled to be played and has
// already been played.
//
// The two facts contradict, and the state is real - a field incident left two
// playoff games with final box scores, updated series, even the "Wizards
// advance" headline, while their schedule rows survived. It came out of
// concurrent sims in a shared league (one device simming games one at a time
// while another ran its own live sim), where the results synced to everyone
// but two of the row deletions did not.
//
// What made it a WEDGE rather than a curiosity is the sim-day fence: it
// remembers those gids as already-simmed (correctly - re-simming them would
// double-apply every aggregate stat), so any sim attempt that includes an
// orphan is refused, forever. The day can never be simmed and the league is
// stuck.
//
// So orphans are swept, not worked around: before simming, any scheduled gid
// that already has a saved result is deleted from the schedule like the sim
// that produced the result would have done. The deletions ride the normal
// changeset, so one sweep heals every device in the room.
export const orphanedScheduleGids = <T extends { gid: number }>(
	schedule: T[],
	hasResult: (gid: number) => boolean,
): number[] => schedule.map((game) => game.gid).filter((gid) => hasResult(gid));
