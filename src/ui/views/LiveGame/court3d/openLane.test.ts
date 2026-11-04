import { assert, describe, test } from "vitest";
import { evalPlayer } from "./evaluate.ts";
import { COURT_H, rimX } from "./geometry.ts";
import { compile } from "./testGame.ts";

// What "nothing between him and the rim" means, as he lets it go: nobody of
// theirs in his way to it, and nobody right on him.
const LANE = 2.2;
const ON = 5;
const SHOT_PLAYS = /^(fg|miss|tp|ast|tov|stl|pf|blk|orb|drb)/;

describe("3D director", () => {
	test("a man with the ball pulling up, floating it or passing out of the lane has somebody between him and the rim", () => {
		let decisions = 0;
		const open: string[] = [];
		for (const seed of ["lane-a", "lane-b", "lane-c"]) {
			const { tl } = compile(seed, 90);
			const tracks = [...tl.tracks.values()];
			for (let k = 0; k < tl.ball.length; k++) {
				const b = tl.ball[k]!;
				if (b.kind !== "fly" || !("pid" in b.from) || b.from.pid < 0) {
					continue;
				}
				const h = b.from.pid;
				const me = tl.tracks.get(h);
				if (!me) {
					continue;
				}
				const pass =
					"pid" in b.to &&
					b.to.pid !== h &&
					tl.tracks.get(b.to.pid)?.team === me.team;
				const shot = !("pid" in b.to);
				if (!pass && !shot) {
					continue;
				}
				const release = b.t0;
				const beat = tl.beats.findLast((x) => x.preStart <= release);
				if (!beat || /ft$/i.test(beat.type) || !SHOT_PLAYS.test(beat.type)) {
					continue;
				}
				// He has had it a while.
				let since = release;
				for (let j = k - 1; j >= 0; j--) {
					const s = tl.ball[j]!;
					if (s.kind !== "hold" || s.pid !== h) {
						break;
					}
					since = s.t0;
				}
				if (release - since < 700) {
					continue;
				}
				const t = release - 150;
				const P = evalPlayer(tl, h, t);
				const rim = { x: rimX(me.team), y: COURT_H / 2 };
				const d = Math.hypot(rim.x - P.x, rim.y - P.y);
				if (d < 6 || d > 24) {
					continue;
				}
				decisions += 1;
				const u = { x: (rim.x - P.x) / d, y: (rim.y - P.y) / d };
				let blocked = false;
				let near = Infinity;
				let nearAhead = 0;
				for (const o of tracks) {
					if (o.team === me.team || o.pid < 0) {
						continue;
					}
					const q = evalPlayer(tl, o.pid, t);
					if (!q.shown) {
						continue;
					}
					const along = (q.x - P.x) * u.x + (q.y - P.y) * u.y;
					const off = Math.abs((q.x - P.x) * u.y - (q.y - P.y) * u.x);
					if (along > 0.5 && along < d + 2 && off < LANE + along * 0.08) {
						blocked = true;
					}
					const dd = Math.hypot(q.x - P.x, q.y - P.y);
					if (dd < near) {
						near = dd;
						nearAhead = along;
					}
				}
				if (!blocked && near > ON) {
					open.push(
						`${seed}: ${h} ${shot ? "shot" : "pass"} at ${Math.round(release)} (${beat.type}, ${Math.round(d)} ft)`,
					);
				} else if (!blocked && nearAhead < -0.5) {
					open.push(
						`${seed}: ${h} ${shot ? "shot" : "pass"} at ${Math.round(release)} past his man (${beat.type})`,
					);
				}
			}
		}
		assert.isAbove(decisions, 150);
		assert.isAtMost(open.length / decisions, 0.02, open.join("\n"));
	}, 900_000);
});
