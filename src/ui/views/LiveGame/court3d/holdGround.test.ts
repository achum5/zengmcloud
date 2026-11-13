import { assert, describe, test } from "vitest";
import { evalPlayer } from "./evaluate.ts";
import { JUMPER } from "./poses.ts";
import { compile } from "./testGame.ts";

// HIS MAN GOING UP WITH IT, A DEFENDER DOES NOT BACK OFF HIM. However much
// room the shot is to have (see ROOM_FT in director.ts), a man already
// nearer the shooter than that as he goes up stays where he is and gets a
// hand to it late - he does not step away from a man rising in front of him
// to leave him open.

describe("3D director", () => {
	test("a defender up on a jump shooter holds his ground as it goes up", () => {
		let near = 0;
		const backed: string[] = [];
		for (const seed of ["a", "b", "c"]) {
			const { tl } = compile(seed, 150);
			const tracks = [...tl.tracks.values()].filter((tr) => tr.pid >= 0);
			for (const tr of tracks) {
				for (const a of tr.acts) {
					// (Straight up and down: not stepping back or fading off him.)
					if (!a.anim.startsWith("shoot")) {
						continue;
					}
					const release = a.t0 + (a.t1 - a.t0) * JUMPER.release;
					const s0 = evalPlayer(tl, tr.pid, a.t0);
					const s1 = evalPlayer(tl, tr.pid, release);
					if (!s0.shown || Math.hypot(s1.x - s0.x, s1.y - s0.y) > 1) {
						continue;
					}
					let him: number | undefined;
					let gap = 4;
					for (const o of tracks) {
						if (o.team === tr.team) {
							continue;
						}
						const st = evalPlayer(tl, o.pid, a.t0);
						const g = Math.hypot(st.x - s0.x, st.y - s0.y);
						if (st.shown && g < gap) {
							gap = g;
							him = o.pid;
						}
					}
					if (him === undefined) {
						continue;
					}
					near += 1;
					const st = evalPlayer(tl, him, release);
					const then = Math.hypot(st.x - s1.x, st.y - s1.y);
					if (then - gap > 1.5) {
						backed.push(
							`${seed} ${tr.pid} at ${Math.round(a.t0)}: ${gap.toFixed(1)} to ${then.toFixed(1)}`,
						);
					}
				}
			}
		}
		assert.isAbove(near, 40);
		assert.isAtMost(backed.length, near * 0.08, backed.join("; "));
	}, 960_000);
});
