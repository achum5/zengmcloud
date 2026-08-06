// Which of today's games a sim should actually play.
//
// Pulled out of play.ts because getting it wrong is not a cosmetic bug: the
// caller's next move, if this says "sim these", is to simulate them and advance
// the league for everybody in the room.
//
// A "single game" sim - Watch game, or Sim game - names one gid out of the
// day's slate. The subtlety is what happens when that gid is NOT on the slate,
// because it has already been played (by this device moments ago, by another
// device, by a double-tap). The answer has to be "nothing", and it has to be
// said explicitly, because the code downstream reads an empty playoff schedule
// as "the next playoff day hasn't been generated yet" and generates one - then
// sims all of it, unfiltered. Watch a game that is already over and the entire
// next day of the playoffs runs behind you.

export type SimSchedule<T extends { gid: number }> = {
	// The games to simulate. Empty means simulate nothing.
	games: T[];
	// Whether these games finish the day. False when a single-game sim leaves
	// other games on the slate unplayed.
	dayOver: boolean;
	// The caller asked for one specific game and it is not here. Distinct from
	// "the slate is empty" - which legitimately means a new day is due - and the
	// caller must stop rather than treat it as one.
	requestedGameMissing: boolean;
};

export const scheduleForSim = <T extends { gid: number }>(
	daySchedule: T[],
	gidOneGame: number | undefined,
): SimSchedule<T> => {
	if (gidOneGame === undefined) {
		return {
			games: daySchedule,
			dayOver: true,
			requestedGameMissing: false,
		};
	}

	const games = daySchedule.filter((game) => game.gid === gidOneGame);
	return {
		games,
		// Only the games left BESIDE this one keep the day open. If it was the
		// last one on the slate, playing it does end the day.
		dayOver: daySchedule.length - games.length === 0,
		requestedGameMissing: games.length === 0,
	};
};
