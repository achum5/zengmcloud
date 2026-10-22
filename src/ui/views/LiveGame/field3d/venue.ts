import { makeCourtRng } from "../courtRng.ts";
import type { FieldTeam } from "./fieldPaint.ts";

// EACH TEAM'S HOME, the same every game it plays there: open to the night
// sky or under a roof (about a third of the NFL's teams play indoors), real
// grass mowed in stripes or artificial turf (always, under a roof), and its
// end zones painted solid in its color, left green with its name in its
// colors, or painted and striped corner to corner. Picked for it from who
// it is - for a team's own settings to take over later, the way a
// basketball team's court can be edited.

export type Venue = {
	roof: "open" | "dome";
	turf: "grass" | "artificial";
	endZones: "solid" | "green" | "striped";
};

const DOMES = 0.3;
const ARTIFICIAL_OUTDOORS = 0.4;

export const venueFor = (home: FieldTeam | undefined): Venue => {
	const rng = makeCourtRng(
		`venue|${home?.region ?? ""}|${home?.name ?? ""}|${home?.abbrev ?? ""}`,
	);
	const roof = rng() < DOMES ? "dome" : "open";
	const turf =
		roof === "dome" || rng() < ARTIFICIAL_OUTDOORS ? "artificial" : "grass";
	const r = rng();
	const endZones = r < 0.6 ? "solid" : r < 0.85 ? "green" : "striped";
	return { roof, turf, endZones };
};
