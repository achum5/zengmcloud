import { compileStaged, type CourtInput, type StageInput } from "./director.ts";
import { mergeCourt, partsOf, type Staged } from "./parts.ts";
import { makeStager, type Stager } from "./stager.ts";
import { possessionSimHere } from "./possession/setting.ts";

// The game staged in a worker of its own (src/court) - see stager.ts - a
// part of it at a time (see parts.ts).

// Another instance of the worker (see farm.ts).
export const spawnCourtWorker = () =>
	new Worker(
		__NODE_ENV === "production"
			? `/gen/court-${window.bbgmVersion}.js`
			: "/gen/court.js",
		{ type: "module" },
	);

const staged: Stager<StageInput, Staged> = makeStager({
	compile: compileStaged,
	spawn: spawnCourtWorker,
	what: "court",
});

// Every man stepped (see stepper.ts) - unless this device says not to:
// court3dStep = "0" in its local storage.
export const stepHere = () => {
	try {
		return localStorage.getItem("court3dStep") !== "0";
	} catch {
		return true;
	}
};

// Whoever is watching the game as it is staged: told of the game so far
// each time a part goes on the end of it, or that the rest would not stage
// - and, once `signal` goes, no longer.
export type StagingWatch = {
	more: (tl: Staged) => void;
	fail?: (error: unknown) => void;
	signal?: AbortSignal;
};

// A GAME BEING STAGED, A PART AT A TIME (see parts.ts): the first part asked
// for, then each after it as the one before is in - and the game so far,
// each part put on the end of it as it comes.
type Staging = {
	key: string | undefined;
	gid: number | undefined;
	sofar: Staged | undefined;
	// The first part in (or not to be had).
	first: Promise<Staged>;
	watching: Set<StagingWatch>;
	// The part the worker is on (see dropStagedCourt).
	now: Promise<Staged> | undefined;
	// Not wanted any more: nothing more asked for.
	off: boolean;
	timer: ReturnType<typeof setTimeout> | undefined;
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
// dropStagedCourt). (Still staged on while a page watches it, kept or not.)
const KEEP_MS = 5 * 60_000;
let kept: Staging | undefined;
let stagings = 0;

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
		input.possession ?? null,
		input.players,
		input.events.length,
		input.events.at(-1) ?? null,
	]);

const abandoned = () => new Error("3D court staging abandoned");

// Not wanted: the part the worker is on put down (see stager.ts's abandon),
// nothing more asked for.
const giveUp = (st: Staging) => {
	st.off = true;
	clearTimeout(st.timer);
	st.watching.clear();
	if (st.now) {
		staged.abandon(st.now);
	}
};
const unwanted = (st: Staging) => kept !== st && st.watching.size === 0;

const keepFor = (st: Staging) => {
	clearTimeout(st.timer);
	st.timer = setTimeout(() => {
		if (kept === st) {
			kept = undefined;
		}
		if (unwanted(st)) {
			giveUp(st);
		}
	}, KEEP_MS);
};

const watch = (st: Staging, w: StagingWatch | undefined) => {
	if (!w || st.off) {
		return;
	}
	if (w.signal?.aborted) {
		return;
	}
	st.watching.add(w);
	w.signal?.addEventListener(
		"abort",
		() => {
			st.watching.delete(w);
			if (unwanted(st)) {
				giveUp(st);
			}
		},
		{ once: true },
	);
};

// Each part asked for once the one before is in. A phone can put the worker
// down part way through one, for the memory it takes (see stager.ts's
// QUIET_MS): that part staged again then, and the rest, without every man
// stepped (see stepper.ts) - much the lighter work - so the game is still
// there to watch. (No worker to be had, and staged on the page itself: the
// rest of the game at once - a part at a time, the game would stop dead for
// each.)
const run = async (
	st: Staging,
	full: CourtInput,
	firstIn: (tl: Staged) => void,
) => {
	const parts = partsOf(full.events);
	const id = ++stagings;
	const end = parts.at(-1)!.to;
	let step = full.step;
	for (let k = 0; k < parts.length; k++) {
		if (st.off) {
			throw abandoned();
		}
		const part = staged.onPage()
			? { from: parts[k]!.from, to: end }
			: parts[k]!;
		const last = part.to === end;
		const ask = (stepped: boolean | undefined) =>
			staged({ ...full, step: stepped, part, last });
		let tl: Staged;
		try {
			st.now = ask(step);
			tl = await st.now;
		} catch (error) {
			if (st.off || !step || !String(error).includes("worker died")) {
				throw error;
			}
			step = false;
			st.now = ask(false);
			tl = await st.now;
		}
		st.now = undefined;
		const sofar = st.sofar ? mergeCourt(st.sofar, tl) : tl;
		sofar.staging = id;
		st.sofar = sofar;
		if (k === 0) {
			firstIn(sofar);
		} else {
			for (const w of st.watching) {
				w.more(sofar);
			}
		}
		if (last) {
			return;
		}
	}
};

const begin = (full: CourtInput, key: string | undefined): Staging => {
	let resolve!: (tl: Staged) => void;
	let reject!: (error: unknown) => void;
	const first = new Promise<Staged>((res, rej) => {
		resolve = res;
		reject = rej;
	});
	const st: Staging = {
		key,
		gid: full.gid,
		sofar: undefined,
		first,
		watching: new Set(),
		now: undefined,
		off: false,
		timer: undefined,
	};
	let settled = false;
	run(st, full, (tl) => {
		settled = true;
		resolve(tl);
	}).catch((error: unknown) => {
		// (One that failed is not kept: the next ask tries again.)
		if (kept === st) {
			clearTimeout(st.timer);
			kept = undefined;
		}
		if (!settled) {
			reject(error);
			return;
		}
		if (!st.off) {
			console.error("3D court failed to stage the rest of the game", error);
			for (const w of st.watching) {
				w.fail?.(error);
			}
		}
		st.watching.clear();
	});
	return st;
};

// The game: the first part of it once it is in - or, already staged on, as
// far as it is staged - and the rest to `w` as each part comes.
export const stageCourt = (
	input: CourtInput,
	w?: StagingWatch,
): Promise<Staged> => {
	const full = {
		...input,
		step: input.step ?? stepHere(),
		possession: input.possession ?? possessionSimHere(),
	};
	let key: string | undefined;
	try {
		key = keyOf(full);
	} catch {
		key = undefined;
	}
	if (key !== undefined && kept?.key === key) {
		keepFor(kept);
		watch(kept, w);
		return kept.sofar ? Promise.resolve(kept.sofar) : kept.first;
	}
	const st = begin(full, key);
	if (key !== undefined) {
		const was = kept;
		kept = st;
		keepFor(st);
		if (was) {
			clearTimeout(was.timer);
			if (unwanted(was)) {
				giveUp(was);
			}
		}
	}
	watch(st, w);
	return st.first;
};

// Not wanted after all (a league-mate's game, turned down): let it go now -
// and if it is still being staged, stop that (see stager.ts's abandon).
export const dropStagedCourt = (gid: number) => {
	if (kept?.gid === gid) {
		const st = kept;
		kept = undefined;
		giveUp(st);
	}
};
