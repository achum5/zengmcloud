import { assert, describe, test } from "vitest";
import { compile } from "./testGame.ts";

// A MAN ON A RUN LETS IT OUT: his third field goal in a row, and every one
// after while it lasts, he celebrates - a fist pump or a roar where he
// stands with play stopped, a fist or a flex on his way back with it on.

// What a man does letting it out, all of him (play stopped) or an arm (on
// his way back).
const ALL_OF_HIM = new Set([
	"fistPump",
	"roar",
	"roarUp",
	"flex",
	"armsWide",
	"skyPoint",
	"tooSmall",
	"raiseRoof",
	"chestPound",
	"chestBump",
	"jerseyTug",
	"celebrate",
	"celebrateBoth",
	"threeUp",
	"goggles",
	"shush",
	"iceVeins",
	"shimmy",
	"nightNight",
	"thanks",
	"point",
]);
const ARM = new Set(["fist", "flex", "pound", "three", "point"]);

const isMake = (type: string) =>
	type === "fgPutBack" ||
	/^(fg(AtRim|LowPost|MidRange)|tp)(AndOne)?$/.test(type);
const isMiss = (type: string) =>
	type === "missPutBack" || /^miss(AtRim|LowPost|MidRange|Tp)$/.test(type);

describe("hot hand", () => {
	test("a man's third make in a row, and on, he celebrates", () => {
		let runs = 0;
		const quiet: string[] = [];
		for (const seed of ["hot-a", "hot-b", "hot-c", "hot-d"]) {
			const { events, tl } = compile(seed, 160);
			const inARow = new Map<number, number>();
			let shooter: number | undefined;
			for (let i = 0; i < events.length; i++) {
				const e = events[i] as any;
				const type = e?.type;
				if (typeof type !== "string") {
					continue;
				}
				if (/^fga/.test(type)) {
					shooter = e.pid;
					continue;
				}
				if (/^blk/.test(type)) {
					if (shooter !== undefined) {
						inARow.set(shooter, 0);
					}
					continue;
				}
				if (isMiss(type)) {
					inARow.set(e.pid, 0);
					continue;
				}
				if (!isMake(type)) {
					continue;
				}
				const run = (inARow.get(e.pid) ?? 0) + 1;
				inARow.set(e.pid, run);
				if (run < 3) {
					continue;
				}
				const beat = tl.beats.find((b) => b.i === i);
				const tr = tl.tracks.get(e.pid);
				if (!beat || !tr) {
					continue;
				}
				runs += 1;
				// (From the basket to a few seconds after it.)
				const from = beat.actionStart;
				const to = beat.end + 3000;
				const did =
					tr.acts.some(
						(a) => ALL_OF_HIM.has(a.anim) && a.t0 >= from && a.t0 <= to,
					) ||
					tr.arms.some((g) => ARM.has(g.kind) && g.t0 >= from && g.t0 <= to);
				if (!did) {
					quiet.push(`${seed}: ${e.pid} make ${run} in a row (${type})`);
				}
			}
		}
		assert.isAbove(runs, 3, "games with men on a run");
		assert.deepEqual(quiet, []);
	}, 480_000);
});
