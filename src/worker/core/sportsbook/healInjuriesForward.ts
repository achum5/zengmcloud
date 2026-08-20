// The roster as it WILL be N days from now - without touching the roster as it
// is today.
//
// The sportsbook prices games days ahead of the one being played, so a line has
// to be made against the team that will actually take the floor. That means
// healing everyone forward by however many days away the game is, which is a
// hypothesis about the future and nothing more.
//
// Both line-makers used to do it by writing the healed injury straight onto the
// player objects they had loaded. Those come from
//
//     idb.getCopies.players({ tid }, "noCopyCache")
//
// and "noCopyCache" is the caller PROMISING not to mutate - it is what lets the
// db layer hand back the live `idb.cache.players` rows instead of copies. So
// pricing a game eight days out reached into the actual league and set every
// injury with eight or fewer days left to `gamesRemaining: 0`, while leaving
// the injury TYPE in place. Which is what made it so hard to see:
//
//   - the player was instantly available, because everything that asks "is he
//     hurt" asks gamesRemaining, not type;
//   - the next day's countdown found a non-Healthy type sitting at zero games
//     and "healed" him for good, erasing the injury entirely;
//   - and the mutated row went out to every device in the room on the next
//     published sim, as `N(Sprained Ankle) > 0(Sprained Ankle)`.
//
// Which players it hit was decided by how far ahead the board was pricing:
// short injuries vanished, long ones survived untouched. That is why it looked
// random.
//
// The fix is not to mutate. A player who would be healed by then is returned as
// a COPY that says so, and the row in the league is left exactly as it is.
export const healedForward = <
	T extends { injury: { type: string; gamesRemaining: number } },
>(
	players: readonly T[],
	daysInFuture: number,
): T[] => {
	if (!(daysInFuture > 0)) {
		return [...players];
	}

	return players.map((p) => {
		if (p.injury.gamesRemaining <= 0) {
			return p;
		}
		const gamesRemaining = Math.max(0, p.injury.gamesRemaining - daysInFuture);
		return {
			...p,
			injury:
				gamesRemaining > 0
					? { ...p.injury, gamesRemaining }
					: // Fully recovered by then, so say so the way the real countdown
						// does. A type left sitting on a zeroed counter is the exact
						// zombie state this whole file exists to keep out of the league.
						{ ...p.injury, type: "Healthy", gamesRemaining: 0 },
		};
	});
};
