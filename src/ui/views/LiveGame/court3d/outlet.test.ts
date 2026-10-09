import { assert, describe, test } from "vitest";
import { evalBall, evalPlayer } from "./evaluate.ts";
import { COURT_W } from "./geometry.ts";
import { bodyOf } from "./poses.ts";
import { compile } from "./testGame.ts";

const body = bodyOf();

describe("3D outlets", () => {
	test(
		"a defensive rebound by a man who isn't a ball handler is given up to one",
		{ timeout: 120_000 },
		() => {
			let boards = 0;
			let carried = 0;
			for (const seed of ["b1", "b2", "b3"]) {
				const { tl, events, players } = compile(seed, 160);
				const handles = new Map(
					players.map((p) => [p.pid, p.skills?.includes("B") ?? false]),
				);
				for (const beat of tl.beats) {
					const e = events[beat.i]!;
					if (e.type !== "drb" || handles.get(e.pid as number)) {
						continue;
					}
					boards += 1;
					// Who has it as it goes over half court.
					let prevX: number | undefined;
					for (
						let t = beat.actionStart;
						t < beat.actionStart + 15000;
						t += 100
					) {
						const b = evalBall(tl, t, () => body);
						if (b.holder === undefined) {
							prevX = undefined;
							continue;
						}
						const x = evalPlayer(tl, b.holder, t).x;
						if (
							prevX !== undefined &&
							(prevX - COURT_W / 2) * (x - COURT_W / 2) < 0
						) {
							if (b.holder === e.pid) {
								carried += 1;
							}
							break;
						}
						prevX = x;
					}
				}
			}
			assert.isAtLeast(boards, 40);
			assert.isAtMost(carried / boards, 0.1, `${carried} of ${boards}`);
		},
	);
});
