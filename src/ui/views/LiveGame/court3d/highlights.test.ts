import { assert, describe, test } from "vitest";
import {
	clipOfPlay,
	countPlayerHighlights,
	filterPlayerHighlights,
	isLine,
} from "../../../util/filterPlayerHighlights.ts";
import { clipDipAt, compileCourt, snapForCursor } from "./director.ts";
import { clipStopAt } from "./playback.ts";
import { evalBall, evalPlayer, fastAt } from "./evaluate.ts";
import { COURT_W } from "./geometry.ts";
import { bodyOf } from "./poses.ts";
import { fakeGame, gidOf } from "./testGame.ts";

const body = bodyOf();

describe("3D highlight reels", () => {
	test("each clip is cut to, under way, with a dip to black", () => {
		for (const seed of ["hl1", "hl2"]) {
			const { events, players } = fakeGame(seed, 120);
			const pid = [1, 2, 3, 4, 5]
				.map((p) => ({ p, n: countPlayerHighlights(events, p) }))
				.sort((a, b) => b.n - a.n)[0]!.p;
			const reel = filterPlayerHighlights(events, pid);
			const starts = reel.filter((e) => e.clipStart === true).length;
			assert.isAbove(starts, 5);
			const tl = compileCourt({ events: reel, players, gid: gidOf(seed) });
			assert.strictEqual(tl.clips?.length, starts);
			for (const c of tl.clips!) {
				assert.strictEqual(clipDipAt(tl, c), 1);
				// Just after the cut: the ball up top in somebody's hands, in the
				// half court, everybody in the picture's half of the floor.
				const t = c + 50;
				const b = evalBall(tl, t, () => body);
				assert.isDefined(b.holder, `${seed} @${c}`);
				const at = evalPlayer(tl, b.holder!, t);
				const side = Math.sign(at.x - COURT_W / 2);
				for (const p of players) {
					const st = evalPlayer(tl, p.pid, t);
					if (st.shown && st.y > 0 && st.y < 50) {
						assert.isAbove(
							(st.x - COURT_W / 2) * side,
							-4,
							`${seed} @${c}: ${p.pid}`,
						);
					}
				}
			}
			// No long fast-forwards between clips: each clip's first line comes
			// soon after the cut.
			for (const c of tl.clips!) {
				const beat = tl.beats.find((x) => x.actionStart > c)!;
				assert.isBelow(beat.actionStart - c, 20_000, `${seed} @${c}`);
			}
		}
	}, 120_000);

	// A reel cuts the game between his plays out - so a man can come into a
	// clip already on the move, with nothing of his before it. The 3D court
	// plays every player's reel (it used to throw on some, and the page fell
	// back to the 2D court for them).
	test("every player's reel plays on the 3D court", () => {
		let reels = 0;
		for (const seed of ["e", "h"]) {
			const { events, players } = fakeGame(seed, 150);
			for (const p of players) {
				if (countPlayerHighlights(events, p.pid) === 0) {
					continue;
				}
				const reel = filterPlayerHighlights(events, p.pid);
				const tl = compileCourt({ events: reel, players, gid: 1 });
				assert.isAbove(tl.end, 0, `${seed}: ${p.pid}`);
				reels += 1;
			}
		}
		assert.isAbove(reels, 20);
	}, 120_000);
	// One play opened on its box score: just it, cut to - its possession's
	// lead-up and the play - staged on its own in a moment.
	test("a highlight on its own plays from a cut to the play, quickly", () => {
		const { events, players } = fakeGame("hl1", 120);
		let clips = 0;
		for (let i = 0; i < events.length; i++) {
			const e = events[i];
			if (e?.type !== "tp" && e?.type !== "fgAtRim" && e?.type !== "blkAtRim") {
				continue;
			}
			// (The row: from the line before - the shot - to this one.)
			let first = i - 1;
			while (first > 0 && !isLine(events[first])) {
				first -= 1;
			}
			const clip = clipOfPlay(events, first, i + 1);
			const at0 = performance.now();
			const tl = compileCourt({ events: clip.events, players, gid: 1 });
			const ms = performance.now() - at0;
			assert.isBelow(ms, 1500);
			assert.isAtMost(tl.clips?.length ?? 0, 1);
			// From where it starts to the play: the cut on the way, and no long
			// way to go.
			const from = snapForCursor(tl, clip.start);
			const to = clipStopAt(tl, clip.end);
			const play = tl.beats.find((b) => b.i === clip.end - 1);
			assert.isDefined(play, `${i}`);
			if (tl.clips?.length) {
				assert.isAtMost(from, tl.clips[0]!);
				assert.isAbove(play!.actionStart, tl.clips[0]!);
			}
			// (As it plays: the dead stretches in it run through fast.)
			let wall = 0;
			for (let t = from; t < to; t += 50) {
				wall += 50 / fastAt(tl, t);
			}
			assert.isBelow(wall, 16_000, `${i}`);
			assert.isAbove(to, play!.actionStart);
			clips += 1;
		}
		assert.isAbove(clips, 10);
	}, 120_000);
});
