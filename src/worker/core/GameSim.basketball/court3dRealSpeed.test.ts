import { assert, beforeAll, describe, test } from "vitest";
import {
	compileCourt,
	type CourtTimeline,
} from "../../../ui/views/LiveGame/court3d/director.ts";
import {
	evalPlayer,
	fastAt,
} from "../../../ui/views/LiveGame/court3d/evaluate.ts";
import { COURT_H, rimX } from "../../../ui/views/LiveGame/court3d/geometry.ts";
import { simRealGame } from "./realGameForTests.ts";

// A game the real sim played, as the 3D court plays it: at real speed (not
// the lead-in to a play, run through fast), where the evaluator has each man
// every tenth of a second.
const STEP = 100;
const RUNS = new Set(["run", "jog", "sprint", "walk"]);
// A dart: this much quicker or slower (feet a second) from one tenth of a
// second to the next - and no more of them than this in the game. (37,
// when a closeout put in off a man he was shuffling along with could go
// five feet in a third of a second from next to standing.)
const DART = 10;
const DARTS_MOST = 33;

let tl: CourtTimeline;
beforeAll(async () => {
	const { events, players } = await simRealGame(22);
	tl = compileCourt({ events, players, gid: 1 });
}, 120_000);

describe("3D court, a real game", () => {
	test("at real speed nobody jumps from one spot to another, or runs backward at a run", () => {
		const jumps: string[] = [];
		const backward: string[] = [];
		// (And darts - from next to standing to a run, or back, in a tenth
		// of a second - are few.)
		const darts: string[] = [];
		const pace = new Map<number, number>();
		let frames = 0;
		const tracks = [...tl.tracks.values()].filter((t) => t.pid >= 0);
		const end = tl.beats.at(-1)!.end;
		const prev = new Map<number, { x: number; y: number } | undefined>();
		const since = new Map<number, number>();
		for (let t = 0; t < end; t += STEP) {
			if (fastAt(tl, t) > 1.2) {
				prev.clear();
				since.clear();
				pace.clear();
				continue;
			}
			const cut = tl.cuts.some((c) => c > t - STEP - 1 && c <= t + 1);
			// (A dart is over two steps: either side of a cut, neither.)
			const nearCut = tl.cuts.some((c) => c > t - 2 * STEP - 1 && c <= t + 1);
			for (const tr of tracks) {
				const q = evalPlayer(tl, tr.pid, t);
				const p = prev.get(tr.pid);
				prev.set(tr.pid, q.shown ? q : undefined);
				if (!q.shown || !p || cut) {
					since.delete(tr.pid);
					pace.delete(tr.pid);
					continue;
				}
				frames += 1;
				const vx = ((q.x - p.x) * 1000) / STEP;
				const vy = ((q.y - p.y) * 1000) / STEP;
				const v = Math.hypot(vx, vy);
				if (v > 30) {
					jumps.push(`${tr.pid} ${(v / 10).toFixed(1)} ft at ${t}`);
				}
				const was = pace.get(tr.pid);
				pace.set(tr.pid, v);
				if (was !== undefined && !nearCut && Math.abs(v - was) > DART) {
					darts.push(`${tr.pid} ${was.toFixed(0)} to ${v.toFixed(0)} at ${t}`);
				}
				const back =
					v > 4 &&
					RUNS.has(q.anim) &&
					(vx * Math.cos(q.yaw) + vy * Math.sin(q.yaw)) / v < -0.5;
				if (!back) {
					since.delete(tr.pid);
					continue;
				}
				const s = since.get(tr.pid) ?? t;
				since.set(tr.pid, s);
				if (t - s === 400) {
					backward.push(`${tr.pid} (${q.anim}) at ${t}`);
				}
			}
		}
		assert.isAbove(frames, 100_000);
		assert.deepEqual(jumps, []);
		assert.deepEqual(backward, []);
		assert.isAtMost(darts.length, DARTS_MOST, darts.join("; "));
	}, 900_000);

	// Backed his man down on the block, a set's drop step or up-and-under to
	// the rim: round him or under him - not turned round to face him and
	// walked in at him, dribbling, into a dunk through him.
	test("off a back-down, a finish at the rim comes off a move on his man", () => {
		const bad: string[] = [];
		let posted = 0;
		for (const tr of tl.tracks.values()) {
			for (const a of tr.acts) {
				if (!AT_RIM.has(a.anim)) {
					continue;
				}
				const down = tr.moves.findLast(
					(m) => m.anim === "post" && m.t1 <= a.t0 && m.t1 > a.t0 - 2500,
				);
				if (!down) {
					continue;
				}
				posted += 1;
				const walkedIn = tr.moves.some(
					(m) =>
						m.t0 >= down.t1 &&
						m.t1 <= a.t0 &&
						(m.anim === "dribble" || m.anim === "dribbleWalk"),
				);
				const moved = tr.acts.some(
					(b) =>
						(b.anim === "jab" || b.anim === "shotFake") &&
						b.t0 >= down.t1 - 100 &&
						b.t0 < a.t0,
				);
				if (walkedIn || !moved) {
					bad.push(`${tr.pid} at ${Math.round(a.t0)} (${a.anim})`);
				}
			}
		}
		assert.isAbove(posted, 0);
		assert.deepEqual(bad, []);
	}, 900_000);

	// Stood waiting on a pass he shoots straight off, his feet are already
	// turned to the rim: not square to the passer and swung right round on
	// it once it is in his hands.
	test("a spot-up shooter waits on the pass turned to the rim", () => {
		const turns: number[] = [];
		for (let k = 1; k < tl.ball.length; k++) {
			const s = tl.ball[k]!;
			if (
				s.kind !== "fly" ||
				!("pid" in s.from) ||
				"pid" in s.to ||
				s.from.pid < 0
			) {
				continue;
			}
			const pid = s.from.pid;
			let j = k - 1;
			while (j > 0) {
				const h = tl.ball[j]!;
				if (h.kind !== "hold" || h.pid !== pid) {
					break;
				}
				j -= 1;
			}
			const pass = tl.ball[j]!;
			if (
				j === k - 1 ||
				pass.kind !== "fly" ||
				!("pid" in pass.to) ||
				pass.to.pid !== pid
			) {
				continue;
			}
			const caught = tl.ball[j + 1]!.t0;
			const team = tl.tracks.get(pid)!.team;
			const R = evalPlayer(tl, pid, s.t0);
			if (
				s.t0 - caught > 1200 ||
				Math.hypot(R.x - rimX(team), R.y - COURT_H / 2) < 15 ||
				evalPlayer(tl, pid, caught - 300).anim !== "ready"
			) {
				continue;
			}
			const a = R.yaw - evalPlayer(tl, pid, caught).yaw;
			turns.push(
				(Math.abs(a - 2 * Math.PI * Math.round(a / (2 * Math.PI))) * 180) /
					Math.PI,
			);
		}
		assert.isAtLeast(turns.length, 5);
		const mean = turns.reduce((x, y) => x + y, 0) / turns.length;
		assert.isBelow(mean, 35, turns.map((x) => x.toFixed(0)).join(" "));
	}, 900_000);
});

const AT_RIM = new Set([
	"dunk",
	"dunk1",
	"tomahawk",
	"windmill",
	"layup",
	"powerLayup",
	"reverseLayup",
	"fingerRoll",
	"scoop",
]);
