import { assert, describe, test } from "vitest";
import { compileCourt } from "../director.ts";
import {
	bodyPoint,
	evalPlayer,
	fastAt,
	poseOf,
	stepInchesOf,
	withBody,
} from "../evaluate.ts";
import { bodyOf, skeleton } from "../poses.ts";
import { fakeGame, gidOf } from "../testGame.ts";
import { stepsPerSecond } from "./gait.ts";

// A game staged by the new possession sim, each man getting about his own
// way (see gait.ts): his feet, as they are drawn.
describe("3D possession sim: his own stride", () => {
	test("running, his feet come down as often as a man with his legs' do at the pace", () => {
		const { events, players } = fakeGame("strides", 16);
		// (Each with ratings, so each gets about his own way: point guards
		// small and quick, centers big and slow.)
		const rated = players.map((p, i) => {
			const big = ["PG", "SG", "SF", "PF", "C"].indexOf(p.pos ?? "SF") / 4;
			return {
				...p,
				ratings: {
					hgt: 20 + 65 * big,
					spd: 80 - 40 * big + (i % 3) * 5,
					jmp: 70 - 30 * big,
					stre: 40 + 35 * big,
					diq: 45 + (i % 4) * 8,
					oiq: 45 + (i % 5) * 6,
				},
			};
		});
		const tl = compileCourt({
			events,
			players: rated,
			gid: gidOf("strides"),
			step: true,
			possession: true,
		});
		assert.isAbove(tl.gaits?.size ?? 0, 9);
		const DT = 1000 / 60;
		const end = Math.max(
			...[...tl.tracks.values()].flatMap((tr) => tr.moves.map((m) => m.t1)),
		);
		let got = 0;
		let want = 0;
		for (const [pid, own] of tl.gaits!) {
			const body = bodyOf(own.inches);
			let prev: { x: number; y: number; down: boolean[] } | undefined;
			for (let t = 0; t <= end; t += DT) {
				const st = evalPlayer(tl, pid, t);
				if (
					!st.shown ||
					fastAt(tl, t) > 1.001 ||
					(st.anim !== "jog" && st.anim !== "run" && st.anim !== "sprint") ||
					(st.from?.w ?? 0) > 0
				) {
					prev = undefined;
					continue;
				}
				const me = withBody(st, body);
				const sk = skeleton(body, poseOf(me));
				// (A foot down: its ankle where it sits on the floor.)
				const down = [sk.legR, sk.legL].map(
					(leg) => bodyPoint(me, leg.end).z - body.ankleH < 0.08,
				);
				if (prev) {
					const v = (Math.hypot(st.x - prev.x, st.y - prev.y) * 1000) / DT;
					if (v >= 9 && v <= 20) {
						got += down.filter((d, i) => d && !prev!.down[i]).length;
						want +=
							(stepsPerSecond("run", v, stepInchesOf(own.inches), own.sig) *
								DT) /
							1000;
					}
				}
				prev = { x: st.x, y: st.y, down };
			}
		}
		// (Enough running to count - and each step as long as the pace and
		// his beat make it: not patting along at a full stride's beat, nor
		// loping.)
		assert.isAbove(want, 60);
		assert.closeTo(got / want, 1, 0.15);
	}, 600_000);
});
