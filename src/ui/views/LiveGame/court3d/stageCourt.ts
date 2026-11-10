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

const staged: (input: CourtInput) => Promise<CourtTimeline> = makeStager({
	compile: compileCourt,
	spawn: spawnCourtWorker,
	what: "court",
});

// Every man stepped (see stepper.ts) - unless this device says not to:
// court3dStep = "0" in its local storage.
const stepHere = () => {
	try {
		return localStorage.getItem("court3dStep") !== "0";
	} catch {
		return true;
	}
};

export const stageCourt = (input: CourtInput): Promise<CourtTimeline> =>
	staged({ ...input, step: input.step ?? stepHere() });
