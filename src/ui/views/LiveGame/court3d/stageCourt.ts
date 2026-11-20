import {
	compileCourt,
	type CourtInput,
	type CourtTimeline,
} from "./director.ts";
import { makeStager, type Stager } from "./stager.ts";

// The game staged in a worker of its own (src/court) - see stager.ts.

// Another instance of the worker (see farm.ts).
export const spawnCourtWorker = () =>
	new Worker(
		__NODE_ENV === "production"
			? `/gen/court-${window.bbgmVersion}.js`
			: "/gen/court.js",
		{ type: "module" },
	);

const staged: Stager<CourtInput, CourtTimeline> = makeStager({
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

// THE LAST GAME STAGED, KEPT A WHILE. A whole game takes a phone the better
// part of a minute to stage, and it was thrown away whenever the page let go
// of it: off to another page and straight back, and the court started over
// from nothing - a league-mate watching someone's live game sat on the
// warm-ups for minutes while the game went on without them. And a game can
// now be staged before its page is there at all (see prestage.ts), while
// this device is still asked whether to watch it. So the last one is kept -
// staging or staged - and the same game asked for again gets it.
//
// One game, not more: a staged game is tens of megabytes. Let go a while
// after it was last asked for, or the moment it is not wanted (see
// dropStagedCourt).
const KEEP_MS = 5 * 60_000;
let kept:
	| {
			key: string;
			gid: number | undefined;
			tl: Promise<CourtTimeline>;
			timer: ReturnType<typeof setTimeout> | undefined;
	  }
	| undefined;

// Everything it is staged from. The events go in by how many there are and
// the last of them - a game's play-by-play is the game's, and this is read
// on every staging, so it stays cheap.
const keyOf = (input: CourtInput) =>
	JSON.stringify([
		input.gid ?? null,
		input.gender ?? null,
		input.intro ?? null,
		input.stakes ?? null,
		input.step ?? null,
		input.players,
		input.events.length,
		input.events.at(-1) ?? null,
	]);

const keepFor = (key: string) => {
	if (kept?.key !== key) {
		return;
	}
	clearTimeout(kept.timer);
	kept.timer = setTimeout(() => {
		if (kept?.key === key) {
			kept = undefined;
		}
	}, KEEP_MS);
};

export const stageCourt = (input: CourtInput): Promise<CourtTimeline> => {
	const full = { ...input, step: input.step ?? stepHere() };
	let key: string;
	try {
		key = keyOf(full);
	} catch {
		return staged(full);
	}
	if (kept?.key === key) {
		keepFor(key);
		return kept.tl;
	}
	if (kept) {
		clearTimeout(kept.timer);
	}
	const tl = staged(full);
	kept = { key, gid: input.gid, tl, timer: undefined };
	keepFor(key);
	// (One that failed is not kept: the next ask tries again.)
	tl.catch(() => {
		if (kept?.key === key) {
			clearTimeout(kept.timer);
			kept = undefined;
		}
	});
	return tl;
};

// Not wanted after all (a league-mate's game, turned down): let it go now -
// and if it is still being staged, stop that (see stager.ts's abandon).
export const dropStagedCourt = (gid: number) => {
	if (kept?.gid === gid) {
		const { tl, timer } = kept;
		clearTimeout(timer);
		kept = undefined;
		staged.abandon(tl);
	}
};
