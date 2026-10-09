import { assert, describe, test } from "vitest";
import {
	countPlayerHighlights,
	filterPlayerHighlights,
} from "../../../util/filterPlayerHighlights.ts";
import { clipDipAt, compileCourt } from "./director.ts";
import { evalBall, evalPlayer } from "./evaluate.ts";
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
});
