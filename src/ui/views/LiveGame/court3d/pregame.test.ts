import { assert, describe, test } from "vitest";
import type { CourtPlayer } from "./director.ts";
import { COURT_H, COURT_W } from "./geometry.ts";
import { bodyOf } from "./poses.ts";
import { stageWarmup, WARMUP_MS, warmupMoment } from "./pregame.ts";

const POS = [
	"PG",
	"SG",
	"SF",
	"PF",
	"C",
	"G",
	"F",
	"FC",
	"GF",
	"SG",
	"C",
	"PG",
	"SF",
];
// Two whole rosters, the way a real game has them - a hurt man among them.
const rosters = (perTeam: number): CourtPlayer[] =>
	([0, 1] as const).flatMap((team) =>
		Array.from({ length: perTeam }, (_, i) => ({
			pid: team * 100 + i + 1,
			team,
			pos: POS[i % POS.length],
			...(i === 2 ? { injury: { type: "Sprained Ankle", games: 3 } } : {}),
		})),
	);

const body = bodyOf();

describe("the warm-ups", () => {
	test("whole rosters: each team at its own basket, the hurt man sitting it out", () => {
		for (const n of [5, 8, 13, 15]) {
			const w = stageWarmup(rosters(n), 7);
			const m = warmupMoment(w, 4000, () => body);
			assert.isAbove(m.players.length, 7, `${n}`);
			assert.isTrue(m.warm);
			for (const st of m.players) {
				assert.notStrictEqual(st.pid % 100, 3, "hurt");
				assert.isTrue(
					st.x > -1 && st.x < COURT_W + 1 && st.y > -1 && st.y < COURT_H + 1,
					`${st.pid} at ${st.x}, ${st.y}`,
				);
				// On his own team's half.
				const team = st.pid > 100 ? 1 : 0;
				assert.isTrue(
					team === 0 ? st.x < COURT_W / 2 + 4 : st.x > COURT_W / 2 - 4,
					`${st.pid} at ${st.x}`,
				);
			}
		}
	});

	test("ball after ball goes in, every one out of the net to the next man", () => {
		const w = stageWarmup(rosters(13), 11);
		let made = 0;
		for (const part of w.parts) {
			// Its ball's path in order, nothing left over from before.
			for (let i = 1; i < part.ball.length; i++) {
				assert.isAtLeast(part.ball[i]!.t0, part.ball[i - 1]!.t0);
			}
			made += part.fx.filter((f) => f.kind === "swish").length;
		}
		// Two shootarounds a team, a make every four seconds or so each.
		assert.isAbove(made, ((4 * WARMUP_MS) / 1000 / 5) * 0.8);
		// Never two balls through the same net together.
		for (const team of [0, 1] as const) {
			const at = w.tl.fx
				.filter((f) => f.kind === "swish" && f.rim === team)
				.map((f) => f.t);
			for (let i = 1; i < at.length; i++) {
				assert.isAbove(at[i]! - at[i - 1]!, 150);
			}
		}
		// Every ball somewhere it could be, all the way through.
		for (let t = 0; t < WARMUP_MS; t += 97) {
			const m = warmupMoment(w, t, () => body);
			for (const b of [m.ball, ...(m.balls ?? [])]) {
				assert.isTrue(
					Number.isFinite(b.x) && b.z > -0.5 && b.z < 25,
					`${t}: ${b.x}, ${b.y}, ${b.z}`,
				);
			}
		}
	});

	test("the same warm-up every time it is shown", () => {
		const a = stageWarmup(rosters(13), 5);
		const b = stageWarmup(rosters(13), 5);
		const at = (w: typeof a) =>
			warmupMoment(w, 9000, () => body).players.map((p) => [p.x, p.y]);
		assert.deepEqual(at(a), at(b));
	});
});
