import {
	compileField,
	type FieldInput,
	type FieldTimeline,
} from "./director.ts";
import { makeStager } from "../court3d/stager.ts";
import { spawnFieldWorker } from "./farm.ts";

// The game staged in a worker of its own (src/field), as the court's is -
// see court3d/stager.ts.
export const stageField: (input: FieldInput) => Promise<FieldTimeline> =
	makeStager({ compile: compileField, spawn: spawnFieldWorker, what: "field" });

// THE OPENING, in the meantime: the game's first play - the kickoff -
// staged right here, in no time at all, to be
// shown while the rest is. The director stages a game in order, never
// looking further on than the next play, so it is the whole game's first
// play exactly, and the whole game takes up where it leaves off. Shown to
// the end of that play, the page given no line past it (`upTo`).
export type FieldOpening = { tl: FieldTimeline; upTo: number };

// (Through the next play too, for whatever the first looks on to.)
const OPENING_PLAYS = 2;

export const stageOpening = (input: FieldInput): FieldOpening | undefined => {
	const { events } = input;
	let n = 0;
	let plays = 0;
	while (
		n < events.length &&
		!(events[n]!.type === "clock" && ++plays > OPENING_PLAYS)
	) {
		n += 1;
	}
	if (n >= events.length) {
		return undefined;
	}
	const part = compileField({ ...input, events: events.slice(0, n) });
	const first = part.plays[0];
	if (!first) {
		return undefined;
	}
	const end = first.t1;
	return {
		tl: {
			plays: part.plays.filter((p) => p.t0 < end),
			beats: part.beats.filter((b) => b.actionStart < end),
			cuts: part.cuts.filter((c) => c < end),
			end,
		},
		upTo: part.beats.find((b) => b.actionStart >= end)?.i ?? n,
	};
};
