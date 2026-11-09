import { assert, describe, test } from "vitest";
import { clipOfPlay, isLine } from "../../../util/filterPlayerHighlights.ts";
import { withShotSpots } from "../gamecast/HighlightClip.tsx";
import { compileCourt } from "./director.ts";
import { COURT_H, rimX } from "./geometry.ts";
import { shotSpots } from "./shotSpots.ts";
import { compile } from "./testGame.ts";

describe("the shot chart's spots", () => {
	test("every shot where the court had it let go from, at his own end", () => {
		const { tl } = compile("a", 80);
		const spots = shotSpots(tl);
		const attempts = tl.beats.filter((b) => b.type.startsWith("fga")).length;
		assert.isAbove(spots.length, attempts * 0.9);
		assert.isAtMost(spots.length, attempts);
		const near = spots.filter(
			(s) => Math.hypot(s.x - rimX(s.side), s.y - COURT_H / 2) < 30,
		);
		// (Bar a heave from his own end at the buzzer.)
		assert.isAbove(near.length, spots.length * 0.97);
		assert.isTrue(spots.some((s) => s.made));
		assert.isTrue(spots.some((s) => s.three));
	}, 300_000);

	// A shot on the chart opened on its own - its clip staged by itself -
	// is taken where the chart has it: not wherever the clip's own staging
	// of the trip would have had him.
	test("a shot's clip is taken where the chart has it", () => {
		const { tl, events, players } = compile("chart", 90);
		const spots = shotSpots(tl);
		const marked = withShotSpots(events, spots);
		let checked = 0;
		let off = 0;
		for (const s of spots.filter((_, k) => k % 3 === 0).slice(0, 20)) {
			// (The shot's row: from the line before its result to the result.)
			const last = s.cursor;
			let first = last - 2;
			while (first > 0 && !isLine(marked[first])) {
				first -= 1;
			}
			const clip = clipOfPlay(marked, first, last);
			const ctl = compileCourt({ events: clip.events, players, gid: 1 });
			const got = shotSpots(ctl).find((x) => x.pid === s.pid);
			if (!got) {
				continue;
			}
			checked += 1;
			// (A jumper right there; one in close, from the spot he got to,
			// carried on in by his move a step or two.)
			const close = Math.hypot(s.x - rimX(s.side), s.y - COURT_H / 2) < 10;
			if (Math.hypot(got.x - s.x, got.y - s.y) > (close ? 6 : 1.5)) {
				off += 1;
			}
		}
		assert.isAbove(checked, 10);
		assert.strictEqual(off, 0);
	}, 300_000);
});
