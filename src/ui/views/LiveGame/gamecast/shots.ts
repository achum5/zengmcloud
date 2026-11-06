// WHERE EACH SHOT WAS TAKEN FROM, as the 3D court staged it (see court3d/
// shotSpots): kept with a saved replay as a flat list of whole numbers, a
// few to a shot - small enough to keep for every game watched.

export type ShotSpot = {
	// The game's events in by the time its result was shown.
	cursor: number;
	pid: number;
	// The side, as the court has them: 0 the visitors (shooting at the left
	// rim), 1 the home team (the right).
	side: 0 | 1;
	// Feet along the floor (0-94, left baseline to right) and across it
	// (0 the far sideline, 50 the near one), as the court measures.
	x: number;
	y: number;
	made: boolean;
	three: boolean;
};

const FIELDS = 7;

export const packShots = (spots: readonly ShotSpot[]): number[] =>
	spots.flatMap((s) => [
		s.cursor,
		s.pid,
		s.side,
		Math.round(s.x * 10),
		Math.round(s.y * 10),
		s.made ? 1 : 0,
		s.three ? 1 : 0,
	]);

export const unpackShots = (
	flat: readonly number[] | undefined,
): ShotSpot[] => {
	const out: ShotSpot[] = [];
	if (!Array.isArray(flat)) {
		return out;
	}
	for (let i = 0; i + FIELDS <= flat.length; i += FIELDS) {
		out.push({
			cursor: flat[i]!,
			pid: flat[i + 1]!,
			side: flat[i + 2] === 1 ? 1 : 0,
			x: flat[i + 3]! / 10,
			y: flat[i + 4]! / 10,
			made: flat[i + 5] === 1,
			three: flat[i + 6] === 1,
		});
	}
	return out;
};
