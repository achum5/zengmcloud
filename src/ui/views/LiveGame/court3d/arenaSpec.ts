import { makeCourtRng } from "../courtRng.ts";
import type { ArenaTeam } from "./arena.ts";
import { shade } from "./figure.ts";

// THE BUILDING ROUND THE FLOOR, AS DATA. What one arena does differently
// from another - its seats and who is in them, their colors, its boards - is
// read off one of these by whatever draws it (see arena.ts). It is made from
// the home team (arenaSpecFor), the same every game in that building; one
// day an arena editor hands one over instead.

export type ArenaSpec = {
	// The seats on the floor (see paintCourtside): rows of padded chairs
	// behind each baseline, the back row up a step, and in the far corners
	// past the benches.
	courtside: {
		endRows: number;
		cornerRows: number;
		chair: string;
		// Of the people in them, how many wear the home team's colors (the
		// rest dressed for a night out), and how many of those a jersey.
		homeShare: number;
		jerseyShare: number;
	};
	// The stands: the seats' color; how many of the home fans down low hold
	// up a sign (the D and the fence); a tunnel in under the lower deck at
	// every how many aisles (none, 0).
	stands: {
		seat: string;
		signShare: number;
		tunnelEvery: number;
	};
	// How dark the bowl is round the lit floor, the upper deck darkest (1 as
	// it is, 0 lit right up).
	lighting: {
		bowl: number;
	};
	// The lit panels on the baskets' bases: what they say.
	stanchion: {
		panel: string;
	};
	// The officials' shirts: NBA gray with black sleeves and sides, or the
	// old black and white stripes.
	officials: "gray" | "stripes";
	// At the far end of each bench: the drinks cooler (its color), a stack
	// of cups on it, and a pile of towels.
	bench: {
		cooler: string;
		towels: string;
	};
};

const DEFAULT_MAIN = "#8c1d40";

// (Decided by the building - the same seats every game in it.)
export const arenaSpecFor = (home: ArenaTeam | undefined): ArenaSpec => {
	const r = makeCourtRng(`arena|${home?.abbrev ?? home?.name ?? ""}`);
	const main = home?.colors?.[0] || DEFAULT_MAIN;
	return {
		courtside: {
			endRows: 2,
			cornerRows: 2,
			chair: r() < 0.5 ? "#16171b" : shade(main, -0.38),
			homeShare: 0.25,
			jerseyShare: 0.35,
		},
		stands: {
			seat: r() < 0.55 ? shade(main, -0.28) : "#4b505b",
			signShare: 0.03,
			tunnelEvery: 2,
		},
		lighting: {
			bowl: 1,
		},
		stanchion: {
			panel: (home?.name || "Home").toUpperCase(),
		},
		officials: "gray",
		bench: {
			cooler: r() < 0.7 ? "#f07c1c" : "#3d9a46",
			towels: "#f4f4f2",
		},
	};
};
