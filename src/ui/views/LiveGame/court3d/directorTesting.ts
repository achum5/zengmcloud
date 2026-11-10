// Shared by the 3D director's tests (director*.test.ts), split across
// files so they run side by side.
import { type CourtTimeline } from "./director.ts";
import { bodyOf } from "./poses.ts";

export const body = bodyOf();
export const bodyFor = () => body;
// What a man is doing as he lets a shot go.
export const SHOOTING = new Set<string>([
	"shoot",
	"fade",
	"hook",
	"layup",
	"fingerRoll",
	"layupClutch",
	"powerLayup",
	"scoop",
	"dunk",
	"dunk1",
	"dunkPump",
	"dunkPump1",
	"tomahawk",
	"windmill",
	"block",
]);

// Whether the picture cut between two moments (anyone may be anywhere after).
export const cutBetween = (tl: CourtTimeline, a: number, b: number) =>
	tl.cuts.some((c) => c > a && c <= b);

export const sampleTimes = (tl: CourtTimeline, step: number) => {
	const out: number[] = [];
	for (let t = 0; t <= tl.end; t += step) {
		out.push(t);
	}
	return out;
};

// Of long passes out to a man left open, how many his man is all but
// stood still through. (Before: one in two.)
export const OPEN_STOOD_MOST = 0.45;
