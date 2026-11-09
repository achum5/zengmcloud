import {
	compileCourt,
	type CourtInput,
	type CourtTimeline,
} from "./director.ts";
import { makeStager } from "./stager.ts";

// The game staged in a worker of its own (src/court) - see stager.ts.

// Another instance of the worker (see farm.ts).
export const spawnCourtWorker = () =>
	new Worker(
		__NODE_ENV === "production"
			? `/gen/court-${window.bbgmVersion}.js`
			: "/gen/court.js",
		{ type: "module" },
	);

export const stageCourt: (input: CourtInput) => Promise<CourtTimeline> =
	makeStager({ compile: compileCourt, spawn: spawnCourtWorker, what: "court" });
